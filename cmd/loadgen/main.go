// Command loadgen runs a bank workload against a cluster through the
// public SDK and writes every transaction attempt to a history file for the
// chaos runner's checkers (package history):
//
//	loadgen -addrs 127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003 -duration 2m -out history.jsonl
//
// It opens the accounts in one transaction, then runs -clients concurrent
// clients doing transfers between two random accounts and, with
// -audit-percent probability, audits that read every account. The first
// -short-clients clients give each transaction a deadline shorter than a
// failover (-short-deadline), so faults leave some commits with an unknown
// outcome. It imports no internal package of this module.
//
// Benchmark mode (docs/benchmarks.md) is turned on by any of -keys,
// -profile, -read-ratio, -cross-shard-ratio or -warmup. It still runs bank
// transfers, so the same checkers apply, but over a configurable key space
// and mix: -keys accounts (opened in batches), a uniform (-profile low) or
// hot-set (-profile high) key choice, a fraction of read-only transactions
// (-read-ratio) and of transactions whose two accounts are on different
// shards (-cross-shard-ratio). The first -warmup of the run is excluded
// from the measurements, which go to -report as JSON: committed
// transactions per second, commit latency percentiles, abort rate (with
// the servers' abort reasons if -metrics-addrs is set) and error counts.
// The history file still holds every attempt, warmup included.
//
// With -timeline, benchmark mode also writes one JSON line per finished
// transaction with its wall-clock end time, end-to-end latency and
// outcome, after a header line with the wall-clock start and measured
// window, and prints a one-line summary every second to stderr. A
// failover test (scripts/aws-failover-load.sh) cuts the run into phases
// around a kill with it.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Shrutij516/paxos-txn-store/api"
	"github.com/Shrutij516/paxos-txn-store/client"
	"github.com/Shrutij516/paxos-txn-store/history"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatalf("loadgen: %v", err)
	}
}

type config struct {
	addrs         []string
	clients       int
	short         int
	duration      time.Duration
	accounts      int
	initial       int
	auditPercent  int
	shortDeadline time.Duration
	longDeadline  time.Duration
	seed          int64
	out           string

	// Benchmark mode (bench is true if any of these is set).
	bench        bool
	warmup       time.Duration
	profile      string  // "" (legacy uniform), "low" or "high"
	hotKeys      int     // -profile high: size of the hot set
	hotFraction  float64 // -profile high: share of key picks from the hot set
	readRatio    float64 // fraction of read-only transactions
	crossRatio   float64 // fraction of cross-shard transactions; < 0 means unset
	report       string  // JSON results; "" means stdout
	timeline     string  // per-transaction JSON lines; "" means none
	metricsAddrs []string
	shards       int // learned at setup
}

func run(args []string) error {
	fs := flag.NewFlagSet("loadgen", flag.ContinueOnError)
	addrs := fs.String("addrs", "127.0.0.1:7001,127.0.0.1:7002,127.0.0.1:7003", "cluster node addresses, comma separated")
	var c config
	fs.IntVar(&c.clients, "clients", 10, "concurrent clients")
	fs.IntVar(&c.short, "short-clients", 2, "how many of the clients use -short-deadline per transaction")
	fs.DurationVar(&c.duration, "duration", time.Minute, "how long to run transfers after opening the accounts")
	fs.IntVar(&c.accounts, "accounts", 12, "number of accounts")
	fs.IntVar(&c.initial, "initial", 100, "opening balance of each account")
	fs.IntVar(&c.auditPercent, "audit-percent", 10, "percent of transactions that audit every account")
	fs.DurationVar(&c.shortDeadline, "short-deadline", 150*time.Millisecond, "per-transaction deadline of the short clients")
	fs.DurationVar(&c.longDeadline, "long-deadline", 10*time.Second, "per-transaction deadline of the other clients")
	fs.Int64Var(&c.seed, "seed", 1, "random seed for the workload")
	fs.StringVar(&c.out, "out", "history.jsonl", "history file to write")
	keys := fs.Int("keys", 0, "benchmark: key space size (number of accounts); overrides -accounts")
	fs.DurationVar(&c.warmup, "warmup", 0, "benchmark: run this long first and exclude it from the measurements")
	fs.StringVar(&c.profile, "profile", "", "benchmark: key choice, low (uniform over -keys) or high (most picks from a small hot set)")
	fs.IntVar(&c.hotKeys, "hot-keys", 16, "benchmark: size of the hot set for -profile high")
	fs.Float64Var(&c.hotFraction, "hot-fraction", 0.9, "benchmark: share of key picks that go to the hot set for -profile high")
	fs.Float64Var(&c.readRatio, "read-ratio", 0, "benchmark: fraction of transactions that only read their two accounts")
	fs.Float64Var(&c.crossRatio, "cross-shard-ratio", -1, "benchmark: fraction of transactions whose two accounts are on different shards (unset: whatever the random pair gives)")
	fs.StringVar(&c.report, "report", "", "benchmark: file for the JSON results (default stdout)")
	metrics := fs.String("metrics-addrs", "", "benchmark: node /metrics addresses, comma separated, for abort reasons")
	fs.StringVar(&c.timeline, "timeline", "", "benchmark: file for one JSON line per finished transaction (wall-clock end, latency, outcome); also prints a summary every second")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c.addrs = strings.Split(*addrs, ",")
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "keys", "warmup", "profile", "read-ratio", "cross-shard-ratio", "hot-keys", "hot-fraction", "report", "metrics-addrs", "timeline":
			c.bench = true
		}
	})
	if *keys > 0 {
		c.accounts = *keys
	}
	if *metrics != "" {
		c.metricsAddrs = strings.Split(*metrics, ",")
	}
	switch {
	case c.clients < 1 || c.short < 0 || c.short > c.clients:
		return errors.New("-clients must be at least 1 and -short-clients between 0 and -clients")
	case c.accounts < 2 || c.initial < 1:
		return errors.New("-accounts must be at least 2 and -initial at least 1")
	case c.duration <= 0:
		return errors.New("-duration must be positive")
	case c.warmup < 0:
		return errors.New("-warmup must not be negative")
	case c.profile != "" && c.profile != "low" && c.profile != "high":
		return errors.New("-profile must be low or high")
	case c.readRatio < 0 || c.readRatio > 1 || c.crossRatio > 1 || c.hotFraction < 0 || c.hotFraction > 1:
		return errors.New("-read-ratio, -cross-shard-ratio and -hot-fraction must be between 0 and 1")
	case c.profile == "high" && (c.hotKeys < 2 || c.hotKeys > c.accounts):
		return errors.New("-hot-keys must be between 2 and the number of accounts")
	}
	f, err := os.Create(c.out)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	var tl *timeline
	if c.timeline != "" {
		tf, err := os.Create(c.timeline)
		if err != nil {
			return err
		}
		defer func() { _ = tf.Close() }()
		tl = newTimeline(tf, os.Stderr)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	s, err := runLoad(ctx, c, f, tl)
	if err != nil {
		return err
	}
	log.Printf("loadgen: %d attempts in %v: %d committed (%.0f/s), %d aborted, %d unknown, %d failed before commit",
		s.attempts.Load(), c.warmup+c.duration, s.committed.Load(), float64(s.committed.Load())/(c.warmup+c.duration).Seconds(),
		s.aborted.Load(), s.unknown.Load(), s.failed.Load())
	if c.bench {
		return writeReport(c, s)
	}
	return nil
}

type stats struct {
	attempts, committed, aborted, unknown, failed atomic.Int64

	// Benchmark measurements, for attempts that began and ended inside the
	// measured window [winStart, winEnd) (nanoseconds since the start).
	winStart, winEnd int64
	mu               sync.Mutex
	win              counts
	latency          []float64 // ms, Run start to commit ack, committed txns
	attemptLatency   []float64 // ms, first operation to commit ack, committed attempts
	abortsBefore     map[string]float64
	abortsAfter      map[string]float64
}

type counts struct{ attempts, committed, aborted, unknown, failed int }

func (s *stats) inWindow(begin, end int64) bool { return begin >= s.winStart && end < s.winEnd }

// runLoad opens the accounts and runs the clients until c.duration passes
// or ctx ends, writing every attempt to w.
func runLoad(ctx context.Context, c config, w *os.File, tl *timeline) (*stats, error) {
	hw, err := history.NewWriter(w, history.Meta{Accounts: c.accounts, Initial: c.initial, Clients: c.clients, Seed: c.seed})
	if err != nil {
		return nil, err
	}
	defer func() { _ = hw.Flush() }()
	start := time.Now()
	since := func() int64 { return int64(time.Since(start)) }
	st := &stats{}

	// Open the accounts; the cluster may still be electing leaders.
	setup := &recorder{hw: hw, since: since, st: st, setup: true}
	sc, err := client.New(c.addrs, client.Options{MaxAttempts: 1000, OnAttempt: setup.onAttempt})
	if err != nil {
		return nil, err
	}
	defer func() { _ = sc.Close() }()
	sctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	// One transaction for the default 12 accounts, as before; a large key
	// space opens in batches so no single transaction holds thousands of
	// locks.
	const batch = 500
	for lo := 0; lo < c.accounts; lo += batch {
		hi := min(lo+batch, c.accounts)
		if err := sc.Run(sctx, func(ctx context.Context, t *client.Txn) error {
			setup.begin = since()
			for i := lo; i < hi; i++ {
				if err := t.Write(ctx, history.Account(i), strconv.Itoa(c.initial)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("opening the accounts: %w", err)
		}
	}
	c.shards = sc.Shards()
	var pick *picker
	if c.bench {
		pick = newPicker(c)
	}

	run := c.warmup + c.duration
	st.winStart = since() + int64(c.warmup)
	st.winEnd = st.winStart + int64(c.duration)
	runCtx, stopRun := context.WithTimeout(ctx, run)
	defer stopRun()
	if tl != nil {
		now := time.Now()
		if err := tl.header(start, now.Add(c.warmup), now.Add(run)); err != nil {
			return nil, err
		}
		go tl.tick(runCtx)
		defer tl.close()
	}
	if c.bench && len(c.metricsAddrs) > 0 {
		go func() {
			select {
			case <-time.After(c.warmup):
				before := scrapeAborts(c.metricsAddrs)
				st.mu.Lock()
				st.abortsBefore = before
				st.mu.Unlock()
			case <-runCtx.Done():
			}
		}()
	}
	var wg sync.WaitGroup
	for i := 0; i < c.clients; i++ {
		r := &recorder{hw: hw, since: since, st: st}
		cl, err := client.New(c.addrs, client.Options{AttemptTimeout: 500 * time.Millisecond, MaxAttempts: 20, OnAttempt: r.onAttempt})
		if err != nil {
			return nil, err
		}
		defer func() { _ = cl.Close() }()
		r.cl = cl
		deadline := c.longDeadline
		if i < c.short {
			deadline = c.shortDeadline
		}
		rng := rand.New(rand.NewPCG(uint64(c.seed), uint64(i)))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for runCtx.Err() == nil {
				tctx, cancel := context.WithTimeout(context.Background(), deadline)
				body := r.body(c, rng)
				if pick != nil {
					body = r.benchBody(c, rng, pick)
				}
				runStart := since()
				err := cl.Run(tctx, body) // every attempt is recorded by onAttempt
				cancel()
				if tl != nil {
					tl.add(time.Now(), float64(since()-runStart)/1e6, err)
				}
				if c.bench && err == nil {
					if end := since(); st.inWindow(runStart, end) {
						st.mu.Lock()
						st.latency = append(st.latency, float64(end-runStart)/1e6)
						st.mu.Unlock()
					}
				}
			}
		}()
	}
	wg.Wait()
	if c.bench && len(c.metricsAddrs) > 0 {
		after := scrapeAborts(c.metricsAddrs)
		st.mu.Lock()
		st.abortsAfter = after
		st.mu.Unlock()
	}
	return st, hw.Flush()
}

// recorder is one client's state for its current attempt.
type recorder struct {
	hw      *history.Writer
	cl      *client.Client
	since   func() int64
	st      *stats
	setup   bool
	audit   bool
	begin   int64
	planned []int
}

func (r *recorder) onAttempt(t *client.Txn, err error) {
	a := history.Attempt{
		ID: uint64(t.ID()), Setup: r.setup, Audit: r.audit, Reads: t.ReadVersions(), Values: t.ReadValues(),
		Shards: t.Shards(), Planned: r.planned, Begin: r.begin, End: -1, Finish: r.since(),
	}
	r.st.attempts.Add(1)
	switch {
	case err == nil:
		a.Outcome, a.End = history.Committed, a.Finish
		r.st.committed.Add(1)
	case errors.Is(err, client.ErrUnknown):
		a.Outcome = history.Unknown
		r.st.unknown.Add(1)
	case errors.Is(err, client.ErrAborted):
		a.Outcome = history.Aborted
		r.st.aborted.Add(1)
	default:
		a.Outcome = history.Failed
		r.st.failed.Add(1)
	}
	if !r.setup && r.st.inWindow(a.Begin, a.Finish) {
		r.st.mu.Lock()
		w := &r.st.win
		w.attempts++
		switch a.Outcome {
		case history.Committed:
			w.committed++
			r.st.attemptLatency = append(r.st.attemptLatency, float64(a.Finish-a.Begin)/1e6)
		case history.Unknown:
			w.unknown++
		case history.Aborted:
			w.aborted++
		default:
			w.failed++
		}
		r.st.mu.Unlock()
	}
	if err := r.hw.Add(a); err != nil {
		log.Printf("loadgen: writing history: %v", err)
	}
}

// body is one bank transaction: a transfer between two accounts, or an
// audit that reads them all.
func (r *recorder) body(c config, rng *rand.Rand) func(ctx context.Context, t *client.Txn) error {
	r.audit = rng.IntN(100) < c.auditPercent
	x := rng.IntN(c.accounts)
	y := (x + 1 + rng.IntN(c.accounts-1)) % c.accounts
	amt := 1 + rng.IntN(10)
	return func(ctx context.Context, t *client.Txn) error {
		r.begin = r.since()
		r.planned = nil
		if r.audit {
			for i := 0; i < c.accounts; i++ {
				if _, err := t.Read(ctx, history.Account(i)); err != nil {
					return err
				}
			}
			return nil
		}
		r.planned = plannedShards(r.cl.Shards(), x, y) // known after Begin
		va, err := t.Read(ctx, history.Account(x))
		if err != nil {
			return err
		}
		vb, err := t.Read(ctx, history.Account(y))
		if err != nil {
			return err
		}
		na, _ := strconv.Atoi(va)
		nb, _ := strconv.Atoi(vb)
		if na < amt {
			return nil // read-only: nothing to move
		}
		if err := t.Write(ctx, history.Account(x), strconv.Itoa(na-amt)); err != nil {
			return err
		}
		return t.Write(ctx, history.Account(y), strconv.Itoa(nb+amt))
	}
}

// plannedShards returns the shards of the two accounts if they differ.
func plannedShards(n, x, y int) []int {
	sx, sy := api.ShardOf(history.Account(x), n), api.ShardOf(history.Account(y), n)
	if sx == sy {
		return nil
	}
	return []int{min(sx, sy), max(sx, sy)}
}

// ---- Benchmark mode ----

// picker chooses accounts: uniformly (-profile low or unset), or with
// -hot-fraction of picks from the first -hot-keys accounts (-profile high).
type picker struct {
	c       config
	byShard [][]int // accounts on each shard
	shardOf []int
}

func newPicker(c config) *picker {
	p := &picker{c: c, byShard: make([][]int, c.shards), shardOf: make([]int, c.accounts)}
	for i := 0; i < c.accounts; i++ {
		sh := api.ShardOf(history.Account(i), c.shards)
		p.shardOf[i] = sh
		p.byShard[sh] = append(p.byShard[sh], i)
	}
	return p
}

func (p *picker) one(rng *rand.Rand) int {
	if p.c.profile == "high" && rng.Float64() < p.c.hotFraction {
		return rng.IntN(p.c.hotKeys)
	}
	return rng.IntN(p.c.accounts)
}

// pair picks two distinct accounts on the same shard (cross false) or on
// different shards (cross true), from the profile's distribution where it
// can: it retries a few draws, then picks uniformly among the accounts
// that qualify.
func (p *picker) pair(rng *rand.Rand, cross bool) (int, int) {
	x := p.one(rng)
	ok := func(y int) bool { return y != x && (p.shardOf[y] != p.shardOf[x]) == cross }
	for range 32 {
		if y := p.one(rng); ok(y) {
			return x, y
		}
	}
	var cands []int
	for sh, ks := range p.byShard {
		if (sh != p.shardOf[x]) == cross {
			cands = append(cands, ks...)
		}
	}
	cands = slices.DeleteFunc(cands, func(y int) bool { return y == x })
	if len(cands) == 0 {
		return x, (x + 1) % p.c.accounts
	}
	return x, cands[rng.IntN(len(cands))]
}

// benchBody is one benchmark transaction: a transfer between two accounts,
// a read-only transaction reading two accounts (-read-ratio), or an audit
// (-audit-percent).
func (r *recorder) benchBody(c config, rng *rand.Rand, p *picker) func(ctx context.Context, t *client.Txn) error {
	r.audit = rng.IntN(100) < c.auditPercent
	readOnly := rng.Float64() < c.readRatio
	cross := rng.IntN(2) == 0
	if c.crossRatio >= 0 {
		cross = rng.Float64() < c.crossRatio
	}
	x, y := p.pair(rng, cross)
	amt := 1 + rng.IntN(10)
	return func(ctx context.Context, t *client.Txn) error {
		r.begin = r.since()
		r.planned = nil
		if r.audit {
			for i := 0; i < c.accounts; i++ {
				if _, err := t.Read(ctx, history.Account(i)); err != nil {
					return err
				}
			}
			return nil
		}
		r.planned = plannedShards(c.shards, x, y)
		va, err := t.Read(ctx, history.Account(x))
		if err != nil {
			return err
		}
		vb, err := t.Read(ctx, history.Account(y))
		if err != nil {
			return err
		}
		na, _ := strconv.Atoi(va)
		nb, _ := strconv.Atoi(vb)
		if readOnly || na < amt {
			return nil
		}
		if err := t.Write(ctx, history.Account(x), strconv.Itoa(na-amt)); err != nil {
			return err
		}
		return t.Write(ctx, history.Account(y), strconv.Itoa(nb+amt))
	}
}

// scrapeAborts sums txn_aborts_total by reason over the nodes' /metrics.
// Nodes that do not answer are skipped.
func scrapeAborts(addrs []string) map[string]float64 {
	out := map[string]float64{}
	hc := http.Client{Timeout: 2 * time.Second}
	for _, a := range addrs {
		resp, err := hc.Get("http://" + a + "/metrics")
		if err != nil {
			continue
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		for _, line := range strings.Split(string(b), "\n") {
			if !strings.HasPrefix(line, "txn_aborts_total{") {
				continue
			}
			i := strings.Index(line, `reason="`)
			j := strings.LastIndexByte(line, ' ')
			if i < 0 || j < 0 {
				continue
			}
			reason := line[i+len(`reason="`):]
			reason = reason[:strings.IndexByte(reason, '"')]
			v, err := strconv.ParseFloat(line[j+1:], 64)
			if err == nil {
				out[reason] += v
			}
		}
	}
	return out
}

// report is the JSON written by benchmark mode.
type report struct {
	Config struct {
		Clients         int     `json:"clients"`
		Keys            int     `json:"keys"`
		Profile         string  `json:"profile"`
		HotKeys         int     `json:"hot_keys,omitempty"`
		HotFraction     float64 `json:"hot_fraction,omitempty"`
		ReadRatio       float64 `json:"read_ratio"`
		CrossShardRatio float64 `json:"cross_shard_ratio"`
		AuditPercent    int     `json:"audit_percent"`
		WarmupS         float64 `json:"warmup_s"`
		DurationS       float64 `json:"duration_s"`
		Seed            int64   `json:"seed"`
	} `json:"config"`
	CommittedPerSec float64 `json:"committed_per_sec"`
	// Latency is measured per committed transaction from the start of
	// client.Run (Begin RPC included) to the commit acknowledgement,
	// including any aborted attempts and backoff before it (end to end).
	// AttemptLatency is per committed attempt, from its first operation to
	// the commit acknowledgement. Both are exact: every sample is kept and
	// sorted, and a percentile is the nearest-rank value.
	LatencyMs        latencySummary     `json:"latency_ms"`
	AttemptLatencyMs latencySummary     `json:"attempt_latency_ms"`
	Attempts         int                `json:"attempts"`
	Committed        int                `json:"committed"`
	Aborted          int                `json:"aborted"`
	AbortRate        float64            `json:"abort_rate"` // aborted / attempts in the window
	AbortsByReason   map[string]float64 `json:"aborts_by_reason,omitempty"`
	Unknown          int                `json:"unknown"`
	Failed           int                `json:"failed"`
	Errors           int                `json:"errors"` // unknown + failed
}

type latencySummary struct {
	Count int     `json:"count"`
	P50   float64 `json:"p50"`
	P90   float64 `json:"p90"`
	P99   float64 `json:"p99"`
	Max   float64 `json:"max"`
}

func summarize(xs []float64) latencySummary {
	if len(xs) == 0 {
		return latencySummary{}
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	rank := func(q float64) float64 { return s[max(0, int(q*float64(len(s))+0.999999)-1)] }
	return latencySummary{Count: len(s), P50: rank(0.50), P90: rank(0.90), P99: rank(0.99), Max: s[len(s)-1]}
}

func writeReport(c config, s *stats) error {
	var r report
	r.Config.Clients, r.Config.Keys, r.Config.Profile = c.clients, c.accounts, c.profile
	if c.profile == "high" {
		r.Config.HotKeys, r.Config.HotFraction = c.hotKeys, c.hotFraction
	}
	r.Config.ReadRatio, r.Config.CrossShardRatio, r.Config.AuditPercent = c.readRatio, c.crossRatio, c.auditPercent
	r.Config.WarmupS, r.Config.DurationS, r.Config.Seed = c.warmup.Seconds(), c.duration.Seconds(), c.seed
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.win
	r.CommittedPerSec = float64(w.committed) / c.duration.Seconds()
	r.LatencyMs, r.AttemptLatencyMs = summarize(s.latency), summarize(s.attemptLatency)
	r.Attempts, r.Committed, r.Aborted, r.Unknown, r.Failed = w.attempts, w.committed, w.aborted, w.unknown, w.failed
	r.Errors = w.unknown + w.failed
	if w.attempts > 0 {
		r.AbortRate = float64(w.aborted) / float64(w.attempts)
	}
	if s.abortsAfter != nil {
		r.AbortsByReason = map[string]float64{}
		for k, v := range s.abortsAfter {
			if d := v - s.abortsBefore[k]; d > 0 {
				r.AbortsByReason[k] = d
			}
		}
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if c.report == "" {
		_, err = os.Stdout.Write(b)
		return err
	}
	return os.WriteFile(c.report, b, 0o644)
}

// ---- Timeline ----

// timeline writes one JSON line per finished transaction (client.Run, all
// its attempts) and prints a summary of each second.
type timeline struct {
	mu   sync.Mutex
	w    *bufio.Writer
	err  error
	out  io.Writer
	sec  []float64 // latencies (ms) of transactions committed this second
	errs int       // transactions that did not commit this second
	n    int       // seconds printed
}

// timelineHeader is the first line: wall-clock times in Unix milliseconds.
type timelineHeader struct {
	StartMs       int64 `json:"start_unix_ms"`
	WindowStartMs int64 `json:"window_start_unix_ms"`
	WindowEndMs   int64 `json:"window_end_unix_ms"`
}

// timelineEntry is one finished transaction. Outcome is committed,
// aborted (every attempt aborted), unknown or error (as in the history).
type timelineEntry struct {
	EndMs     int64   `json:"t"`
	LatencyMs float64 `json:"ms"`
	Outcome   string  `json:"outcome"`
}

func newTimeline(w, out io.Writer) *timeline {
	return &timeline{w: bufio.NewWriter(w), out: out}
}

func (tl *timeline) header(start, winStart, winEnd time.Time) error {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	b, _ := json.Marshal(timelineHeader{start.UnixMilli(), winStart.UnixMilli(), winEnd.UnixMilli()})
	_, err := tl.w.Write(append(b, '\n'))
	return err
}

func (tl *timeline) add(end time.Time, ms float64, err error) {
	e := timelineEntry{EndMs: end.UnixMilli(), LatencyMs: ms, Outcome: history.Committed}
	switch {
	case err == nil:
	case errors.Is(err, client.ErrUnknown):
		e.Outcome = history.Unknown
	case errors.Is(err, client.ErrAborted):
		e.Outcome = history.Aborted
	default:
		e.Outcome = history.Failed
	}
	b, _ := json.Marshal(e)
	tl.mu.Lock()
	defer tl.mu.Unlock()
	if _, werr := tl.w.Write(append(b, '\n')); werr != nil && tl.err == nil {
		tl.err = werr
	}
	if err == nil {
		tl.sec = append(tl.sec, ms)
	} else {
		tl.errs++
	}
}

// tick prints the last second's summary every second until ctx ends.
func (tl *timeline) tick(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tl.mu.Lock()
			tl.n++
			l := summarize(tl.sec)
			_, _ = fmt.Fprintf(tl.out, "loadgen: +%3ds %4d committed/s p50 %6.1f ms p99 %6.1f ms, %d not committed\n", tl.n, l.Count, l.P50, l.P99, tl.errs)
			tl.sec, tl.errs = tl.sec[:0], 0
			_ = tl.w.Flush()
			tl.mu.Unlock()
		}
	}
}

func (tl *timeline) close() {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	if err := tl.w.Flush(); err != nil && tl.err == nil {
		tl.err = err
	}
	if tl.err != nil {
		log.Printf("loadgen: writing timeline: %v", tl.err)
	}
}

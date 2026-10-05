package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"

	"github.com/Shrutij516/paxos-txn-store/api"
	"github.com/Shrutij516/paxos-txn-store/client"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/server"
	"github.com/Shrutij516/paxos-txn-store/internal/storage"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
)

const (
	numNodes       = 3
	numShards      = 3
	numClients     = 10 // concurrent bank clients
	numShort       = 2  // of them, with a per-transaction deadline shorter than a failover
	numAccounts    = 12
	initialBalance = 100
	auditPercent   = 10
	tick           = 10 * time.Millisecond
	// shortDeadline is below the shortest election timeout (20 ticks), so a
	// short client whose commit is in flight at a killed coordinator cannot
	// learn the outcome in time.
	shortDeadline = 150 * time.Millisecond
	longDeadline  = 10 * time.Second
	phase         = 1500 * time.Millisecond // between kills and restarts
	// inDoubtWait is passed to every process (-in-doubt-wait). A txn left
	// prepared by a kill keeps its locks until its participants ask the
	// coordinator, this long after the new leader takes over.
	inDoubtWait = server.DefaultInDoubtWait
	// traceSample is the nodes' -trace-sample. The clients do not trace, so
	// each Txn RPC a node receives starts a trace with this probability.
	traceSample = "0.2"
)

// coverDirEnv, when set to a directory, makes the test build paxosd with
// coverage instrumentation and has every child process write its coverage
// data there (GOCOVERDIR). make cover merges it into the report.
const coverDirEnv = "PAXOSD_GOCOVERDIR"

// keepLogsEnv, when set to a directory, makes a failed run copy its node
// logs, config and data directories there (t.TempDir is removed after the
// test), so the SQLite files can be inspected afterwards.
const keepLogsEnv = "PAXOSD_KEEP_LOGS"

func account(i int) string { return fmt.Sprintf("acct%d", i) }

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

// proc is one paxosd process.
type proc struct {
	id     paxos.NodeID
	addr   string
	maddr  string // its /metrics endpoint
	dir    string
	args   []string
	log    string
	cmd    *exec.Cmd
	exited chan struct{} // closed when the current process has exited
	err    error         // its Wait result, valid after exited is closed
}

func (p *proc) start(t *testing.T, bin string) {
	t.Helper()
	f, err := os.OpenFile(p.log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	p.cmd = exec.Command(bin, p.args...)
	p.cmd.Stdout, p.cmd.Stderr = f, f
	if dir := os.Getenv(coverDirEnv); dir != "" {
		p.cmd.Env = append(os.Environ(), "GOCOVERDIR="+dir)
	}
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p.exited = make(chan struct{})
	go func() {
		p.err = p.cmd.Wait()
		_ = f.Close()
		close(p.exited)
	}()
}

// wait waits for the current process to exit.
func (p *proc) wait(d time.Duration) (bool, error) {
	select {
	case <-p.exited:
		return true, p.err
	case <-time.After(d):
		return false, nil
	}
}

func (p *proc) kill(t *testing.T) {
	t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if ok, _ := p.wait(5 * time.Second); !ok {
		t.Fatalf("node %d did not exit after SIGKILL", p.id)
	}
}

// stop sends SIGTERM and requires a clean exit.
func (p *proc) stop(t *testing.T) {
	t.Helper()
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	if ok, err := p.wait(10 * time.Second); !ok || err != nil {
		t.Fatalf("node %d did not exit cleanly on SIGTERM (exited=%v): %v", p.id, ok, err)
	}
}

func buildPaxosd(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "paxosd")
	args := []string{"build", "-o", bin}
	if os.Getenv(coverDirEnv) != "" {
		args = append(args, "-cover", "-covermode=set", "-coverpkg=github.com/Shrutij516/paxos-txn-store/...")
	}
	if out, err := exec.Command("go", append(args, ".")...).CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	return bin
}

// TestMultiProcessTransactions builds paxosd and runs three real processes,
// each hosting one replica of each of three shards. Ten concurrent SDK
// clients run bank transfers (and some audits) across shards. Mid-run the
// test SIGKILLs the leader of shard 2 while a cross-shard commit touching
// shard 2 is in flight, and restarts it. Then it SIGKILLs the node leading
// the coordinator shard (shard 0, the lowest shard of every transaction
// that touches it) while a short client's commit and a cross-shard commit
// are in flight there, and restarts it too. Afterwards it rebuilds every
// shard from the processes' SQLite files and runs the Phase 5 checkers on
// the real history: atomicity, the bank invariant, and strict
// serializability (which also rejects G1a and G1b). A transaction whose
// commit outcome the client never learned counts as whatever the shards'
// logs say. The test fails if no transaction ended with an unknown
// outcome, or if for either kill no cross-shard transaction touching a
// shard the killed node led was in flight across it. 3 iterations; 1
// under -short.
func TestMultiProcessTransactions(t *testing.T) {
	iters := 3
	if testing.Short() {
		iters = 1
	}
	bin := buildPaxosd(t)
	var all []runResult
	for it := 0; it < iters; it++ {
		t.Run(fmt.Sprintf("iter%d", it), func(t *testing.T) {
			all = append(all, runBank(t, bin, uint64(it)+1))
		})
	}
	var commits, attempts, aborts, cross, unknown, unknownCommitted int
	var secs float64
	var fo []string
	for _, r := range all {
		commits += r.committed
		attempts += r.attempts
		aborts += r.aborted
		cross += r.cross
		unknown += r.unknown
		unknownCommitted += r.unknownCommitted
		secs += r.duration.Seconds()
		for _, k := range r.kills {
			fo = append(fo, fmt.Sprintf("%dms", k.failover.Milliseconds()))
		}
	}
	if len(all) > 0 && commits > 0 {
		t.Logf("%d runs: %.0f committed txns/s, abort rate %.1f%% of %d attempts, cross-shard %.0f%% of %d commits, %d unknown outcomes (%d of them committed), failover per kill %v",
			len(all), float64(commits)/secs, 100*float64(aborts)/float64(attempts), attempts,
			100*float64(cross)/float64(commits), commits, unknown, unknownCommitted, fo)
	}
}

// attempt is what a client recorded about one transaction attempt.
type attempt struct {
	id      txn.ID
	reads   map[string]uint64
	vals    map[string]string
	shards  []int // shards it read or wrote
	planned []int // shards of the accounts it set out to use, if more than one
	audit   bool
	short   bool
	setup   bool
	begin   int64 // ns since start, taken before the first read
	end     int64 // when Commit returned success, -1 otherwise
	finish  int64 // when the attempt ended, whatever the outcome
	outcome string
}

const (
	outCommitted = "committed"
	outAborted   = "aborted"
	outUnknown   = "unknown"
	outError     = "error" // failed before commit (deadline): never committed
)

type kill struct {
	node     paxos.NodeID
	at       int64 // ns since start
	led      []txn.ShardID
	failover time.Duration
}

type runResult struct {
	duration                            time.Duration
	attempts, aborted, committed, cross int
	unknown, unknownCommitted           int
	kills                               []kill
}

// recorder collects attempts from all clients.
type recorder struct {
	start time.Time
	mu    sync.Mutex
	all   []*attempt
	// shortCoord counts short clients whose commit is in flight with
	// shard 0 as coordinator; cross[s] counts cross-shard transactions in
	// flight (from their first read) that touch shard s. The kills wait
	// for them, so each kill hits transactions in flight.
	shortCoord atomic.Int64
	cross      [numShards]atomic.Int64
}

func (r *recorder) since() int64 { return int64(time.Since(r.start)) }

func (r *recorder) add(a *attempt) {
	r.mu.Lock()
	r.all = append(r.all, a)
	r.mu.Unlock()
}

// bankClient is one client goroutine's state for its current attempt.
type bankClient struct {
	r       *recorder
	short   bool
	setup   bool // opens the accounts; not part of the measured workload
	audit   bool
	begin   int64
	coord   bool  // this attempt is a short client's commit coordinated by shard 0
	pending []int // shards counted in recorder.cross for this attempt
}

func (b *bankClient) onAttempt(t *client.Txn, err error) {
	if b.coord {
		b.r.shortCoord.Add(-1)
		b.coord = false
	}
	planned := b.pending
	for _, sh := range b.pending {
		b.r.cross[sh].Add(-1)
	}
	b.pending = nil
	a := &attempt{id: txn.ID(t.ID()), reads: t.ReadVersions(), vals: t.ReadValues(), shards: t.Shards(), planned: planned,
		audit: b.audit, short: b.short, setup: b.setup, begin: b.begin, end: -1, finish: b.r.since()}
	switch {
	case err == nil:
		a.outcome, a.end = outCommitted, a.finish
	case errors.Is(err, client.ErrUnknown):
		a.outcome = outUnknown
	case errors.Is(err, client.ErrAborted):
		a.outcome = outAborted
	default:
		a.outcome = outError
	}
	b.r.add(a)
}

// body is one bank transaction: a transfer between two accounts, or an
// audit that reads them all.
func (b *bankClient) body(rng *rand.Rand) func(ctx context.Context, t *client.Txn) error {
	b.audit = rng.IntN(100) < auditPercent
	x := rng.IntN(numAccounts)
	y := (x + 1 + rng.IntN(numAccounts-1)) % numAccounts
	amt := 1 + rng.IntN(10)
	// The shards this transaction will touch are known up front; count it
	// in recorder.cross from its start, so a kill waiting for one in flight
	// has the whole transaction as its window, not just the commit.
	planned := map[int]bool{}
	for i := 0; i < numAccounts; i++ {
		if b.audit || i == x || i == y {
			planned[api.ShardOf(account(i), numShards)] = true
		}
	}
	return func(ctx context.Context, t *client.Txn) error {
		b.begin = b.r.since()
		if len(planned) > 1 {
			for sh := range planned {
				b.pending = append(b.pending, sh)
				b.r.cross[sh].Add(1)
			}
		}
		if b.audit {
			for i := 0; i < numAccounts; i++ {
				if _, err := t.Read(ctx, account(i)); err != nil {
					return err
				}
			}
		} else {
			va, err := t.Read(ctx, account(x))
			if err != nil {
				return err
			}
			vb, err := t.Read(ctx, account(y))
			if err != nil {
				return err
			}
			na, _ := strconv.Atoi(va)
			nb, _ := strconv.Atoi(vb)
			if na >= amt {
				if err := t.Write(ctx, account(x), strconv.Itoa(na-amt)); err != nil {
					return err
				}
				if err := t.Write(ctx, account(y), strconv.Itoa(nb+amt)); err != nil {
					return err
				}
			}
		}
		// Run calls Commit next; onAttempt undoes the counts.
		if sh := t.Shards(); b.short && len(sh) > 0 && sh[0] == 0 {
			b.coord = true
			b.r.shortCoord.Add(1)
		}
		return nil
	}
}

func writeConfig(t *testing.T, dir string, procs []*proc) string {
	t.Helper()
	cl := server.Cluster{Shards: numShards}
	for _, p := range procs {
		cl.Nodes = append(cl.Nodes, server.NodeConfig{ID: p.id, Addr: p.addr})
	}
	b, err := json.MarshalIndent(cl, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "cluster.json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// leadersNow scrapes every node and returns the scrape and, per shard, the
// node whose paxos_is_leader gauge is 1. It retries briefly while shard sh
// has no leader, or two (a deposed leader that has not noticed yet).
func leadersNow(t *testing.T, procs []*proc, sh txn.ShardID) (map[paxos.NodeID]map[string]*dto.MetricFamily, map[txn.ShardID]paxos.NodeID) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		fams := scrapeAll(t, procs)
		count := map[txn.ShardID]int{}
		out := map[txn.ShardID]paxos.NodeID{}
		for id, f := range fams {
			for _, m := range f["paxos_is_leader"].GetMetric() {
				if m.GetGauge().GetValue() != 1 {
					continue
				}
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "shard" {
						n, _ := strconv.Atoi(lp.GetValue())
						count[txn.ShardID(n)]++
						out[txn.ShardID(n)] = id
					}
				}
			}
		}
		if count[sh] == 1 {
			return fams, out
		}
		if time.Now().After(deadline) {
			t.Fatalf("shard %d has %d nodes reporting paxos_is_leader 1", sh, count[sh])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func runBank(t *testing.T, bin string, seed uint64) runResult {
	dir := t.TempDir()
	t.Cleanup(func() {
		keep := os.Getenv(keepLogsEnv)
		if !t.Failed() || keep == "" {
			return
		}
		dst := filepath.Join(keep, fmt.Sprintf("%s-%d", filepath.Base(dir), time.Now().UnixNano()))
		if err := os.CopyFS(dst, os.DirFS(dir)); err != nil {
			t.Logf("keeping logs: %v", err)
			return
		}
		t.Logf("logs and data kept in %s", dst)
	})
	procs := make([]*proc, numNodes)
	var addrs []string
	for i := range procs {
		id := paxos.NodeID(i + 1)
		procs[i] = &proc{id: id, addr: freeAddr(t), maddr: freeAddr(t), dir: filepath.Join(dir, fmt.Sprintf("n%d", id)), log: filepath.Join(dir, fmt.Sprintf("n%d.log", id))}
		addrs = append(addrs, procs[i].addr)
	}
	cfg := writeConfig(t, dir, procs)
	col, otlp := startCollector(t)
	for _, p := range procs {
		p.args = []string{"-config", cfg, "-id", fmt.Sprint(p.id), "-data-dir", p.dir, "-tick", tick.String(), "-in-doubt-wait", inDoubtWait.String(),
			"-metrics-listen", p.maddr, "-otlp-endpoint", otlp, "-trace-sample", traceSample}
		p.start(t, bin)
	}
	defer func() {
		for _, p := range procs {
			_ = p.cmd.Process.Kill() // no-op if already exited
			_, _ = p.wait(5 * time.Second)
		}
	}()
	byID := func(id paxos.NodeID) *proc { return procs[id-1] }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	rec := &recorder{start: time.Now()}
	// Open the accounts in one transaction across all shards.
	setup := &bankClient{r: rec, setup: true}
	sc, err := client.New(addrs, client.Options{MaxAttempts: 100, OnAttempt: setup.onAttempt})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sc.Close() }()
	if err := sc.Run(ctx, func(ctx context.Context, t *client.Txn) error {
		setup.begin = rec.since()
		for i := 0; i < numAccounts; i++ {
			if err := t.Write(ctx, account(i), strconv.Itoa(initialBalance)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("cluster never accepted the setup transaction: %v (logs in %s)", err, dir)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	workStart := time.Now()
	for ci := 0; ci < numClients; ci++ {
		b := &bankClient{r: rec, short: ci < numShort}
		c, err := client.New(addrs, client.Options{AttemptTimeout: 500 * time.Millisecond, MaxAttempts: 20, OnAttempt: b.onAttempt})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, uint64(ci)))
			deadline := longDeadline
			if b.short {
				deadline = shortDeadline
			}
			for {
				select {
				case <-stop:
					return
				default:
				}
				tctx, tcancel := context.WithTimeout(ctx, deadline)
				_ = c.Run(tctx, b.body(rng)) // every attempt is recorded by onAttempt
				tcancel()
			}
		}()
	}

	var kills []kill
	doKill := func(p *proc, led map[txn.ShardID]paxos.NodeID) {
		k := kill{node: p.id}
		for sh := txn.ShardID(0); sh < numShards; sh++ {
			if led[sh] == p.id {
				k.led = append(k.led, sh)
			}
		}
		k.at = rec.since() // the moment of the signal, not of the exit
		p.kill(t)
		kills = append(kills, k)
	}
	// awaitInFlight waits (up to 10 s) until cond holds; the kill follows.
	awaitInFlight := func(cond func() bool) {
		for wait := time.Now(); !cond() && time.Since(wait) < 10*time.Second; {
			time.Sleep(200 * time.Microsecond)
		}
	}
	// killLeader SIGKILLs the leader of shard sh once cond holds, and
	// returns it with a scrape of every node taken just before. The leader
	// is read from the paxos_is_leader gauges; if cond took a while,
	// leadership may have moved, so it reads them again.
	killLeader := func(sh txn.ShardID, cond func() bool) (*proc, map[paxos.NodeID]map[string]*dto.MetricFamily) {
		for try := 0; ; try++ {
			before, l := leadersNow(t, procs, sh)
			start := time.Now()
			awaitInFlight(cond)
			if time.Since(start) < 300*time.Millisecond || try == 5 {
				v := byID(l[sh])
				doKill(v, l)
				return v, before
			}
		}
	}
	// Kill 1: the leader of shard 2, while a cross-shard transaction
	// touching shard 2 is in flight.
	time.Sleep(phase)
	v1, before := killLeader(2, func() bool { return rec.cross[2].Load() > 0 })
	time.Sleep(phase)
	electionsRose(t, procs, v1, before)
	v1.start(t, bin)
	time.Sleep(phase)
	// Kill 2: the leader of shard 0, the coordinator shard, while a short
	// client's commit and a cross-shard transaction are in flight there.
	v2, before := killLeader(0, func() bool { return rec.shortCoord.Load() > 0 && rec.cross[0].Load() > 0 })
	time.Sleep(phase)
	electionsRose(t, procs, v2, before)
	v2.start(t, bin)
	time.Sleep(phase)
	close(stop)
	wg.Wait()
	workTime := time.Since(workStart)

	sms := quiesce(t, procs)
	col.checkExported(t)
	return check(t, rec, sms, kills, workTime, dir)
}

// quiesce waits until every node reports no transaction in doubt
// (txn_in_doubt is 0 for every shard), checks that the commit latency
// histograms have samples, stops every process cleanly, and rebuilds each
// shard from the SQLite files.
func quiesce(t *testing.T, procs []*proc) map[txn.ShardID]*txn.SM {
	t.Helper()
	// Long enough for an election plus an outcome query plus a coordinator
	// timeout, for a transaction left prepared by the last kill.
	deadline := time.Now().Add(10*time.Second + 2*inDoubtWait)
	for {
		inDoubt := 0.0
		for _, p := range procs {
			inDoubt += sum(scrape(t, p), "txn_in_doubt")
		}
		if inDoubt == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("txn_in_doubt still sums to %v across the cluster after the workload settled", inDoubt)
		}
		time.Sleep(100 * time.Millisecond)
	}
	samples := 0.0
	for _, p := range procs {
		samples += histCount(scrape(t, p), "paxos_commit_latency_seconds")
	}
	if samples == 0 {
		t.Fatal("paxos_commit_latency_seconds has no samples on any node")
	}
	for _, p := range procs {
		p.stop(t)
	}
	return loadShards(t, procs)
}

// ---- Traces ----

// collector is a minimal OTLP/gRPC trace receiver: it counts the spans the
// nodes export, by name, and the nodes they came from.
type collector struct {
	coltracepb.UnimplementedTraceServiceServer
	mu    sync.Mutex
	names map[string]int
	nodes map[int64]bool
}

func (c *collector) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rs := range req.GetResourceSpans() {
		for _, a := range rs.GetResource().GetAttributes() {
			if a.GetKey() == "node" {
				c.nodes[a.GetValue().GetIntValue()] = true
			}
		}
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				c.names[sp.GetName()]++
			}
		}
	}
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func startCollector(t *testing.T) (*collector, string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	c := &collector{names: map[string]int{}, nodes: map[int64]bool{}}
	srv := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(srv, c)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return c, lis.Addr().String()
}

// checkExported: the nodes exported spans over OTLP (flushed on SIGTERM),
// including two-phase commit phases and Paxos rounds, from several nodes.
func (c *collector) checkExported(t *testing.T) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, name := range []string{"txn.v1.Txn/Commit", "2pc.prepare", "2pc.decide", "2pc.participant.prepare", "paxos.round"} {
		if c.names[name] == 0 {
			t.Fatalf("no %q span exported over OTLP; got %v", name, c.names)
		}
	}
	if len(c.nodes) < 2 {
		t.Fatalf("spans exported by nodes %v, want at least 2", c.nodes)
	}
	t.Logf("OTLP collector received spans from nodes %v: %d 2pc.prepare, %d paxos.round", c.nodes, c.names["2pc.prepare"], c.names["paxos.round"])
}

// ---- Metrics ----

// scrape fetches and parses a node's /metrics.
func scrape(t *testing.T, p *proc) map[string]*dto.MetricFamily {
	t.Helper()
	resp, err := http.Get("http://" + p.maddr + "/metrics")
	if err != nil {
		t.Fatalf("scraping node %d: %v", p.id, err)
	}
	defer func() { _ = resp.Body.Close() }()
	parser := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		t.Fatalf("parsing node %d's metrics: %v", p.id, err)
	}
	return fams
}

func scrapeAll(t *testing.T, procs []*proc) map[paxos.NodeID]map[string]*dto.MetricFamily {
	t.Helper()
	out := map[paxos.NodeID]map[string]*dto.MetricFamily{}
	for _, p := range procs {
		out[p.id] = scrape(t, p)
	}
	return out
}

// sum adds a counter's or gauge's value over every shard.
func sum(fams map[string]*dto.MetricFamily, name string) float64 {
	total := 0.0
	for _, m := range fams[name].GetMetric() {
		total += m.GetCounter().GetValue() + m.GetGauge().GetValue()
	}
	return total
}

// histCount adds a histogram's sample count over every shard.
func histCount(fams map[string]*dto.MetricFamily, name string) float64 {
	total := 0.0
	for _, m := range fams[name].GetMetric() {
		total += float64(m.GetHistogram().GetSampleCount())
	}
	return total
}

// electionsRose checks that the surviving nodes won elections after the
// victim was killed: someone took over the shards it led.
func electionsRose(t *testing.T, procs []*proc, victim *proc, before map[paxos.NodeID]map[string]*dto.MetricFamily) {
	t.Helper()
	was, now := 0.0, 0.0
	for _, p := range procs {
		if p == victim {
			continue
		}
		was += sum(before[p.id], "paxos_leader_elections_total")
		now += sum(scrape(t, p), "paxos_leader_elections_total")
	}
	if now <= was {
		t.Fatalf("paxos_leader_elections_total on the survivors of node %d stayed at %v after the kill", victim.id, was)
	}
	t.Logf("paxos_leader_elections_total on the survivors of node %d: %v before the kill, %v after", victim.id, was, now)
}

// loadShards replays every node's committed log of every shard into a
// fresh state machine. All logs of a shard must agree on the slots they
// share (Paxos safety across real processes); the longest is the shard's
// state.
func loadShards(t *testing.T, procs []*proc) map[txn.ShardID]*txn.SM {
	t.Helper()
	out := map[txn.ShardID]*txn.SM{}
	for sh := txn.ShardID(0); sh < numShards; sh++ {
		var best []paxos.SlotEntry
		for _, p := range procs {
			db, err := storage.OpenSQLite(server.DBPath(p.dir, p.id, sh))
			if err != nil {
				t.Fatal(err)
			}
			log, err := db.LoadCommitted()
			_ = db.Close()
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < min(len(log), len(best)); i++ {
				if log[i] != best[i] {
					t.Fatalf("shard %d: node %d committed %+v at slot %d, another node %+v", sh, p.id, log[i], log[i].Slot, best[i])
				}
			}
			if len(log) > len(best) {
				best = log
			}
		}
		sm := txn.NewSM(nil)
		for _, se := range best {
			sm.Apply(se.Slot, se.Entry)
		}
		out[sh] = sm
	}
	return out
}

func check(t *testing.T, rec *recorder, sms map[txn.ShardID]*txn.SM, kills []kill, workTime time.Duration, dir string) runResult {
	t.Helper()
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf(format+" (logs in %s)", append(args, dir)...)
	}
	if err := txn.CheckAtomicity(sms); err != nil {
		fail("%v", err)
	}
	attempts := map[txn.ID]*txn.HTxn{}
	audits := map[txn.ID]map[string]string{}
	for _, a := range rec.all {
		attempts[a.id] = &txn.HTxn{Reads: a.reads, Begin: a.begin, End: a.end}
		if a.audit {
			audits[a.id] = a.vals
		}
	}
	if err := txn.CheckBank(sms, numAccounts*initialBalance, audits); err != nil {
		fail("%v", err)
	}
	h, err := txn.BuildHistory(sms, attempts)
	if err != nil {
		fail("%v", err)
	}
	if err := txn.CheckStrict(h); err != nil {
		fail("%v", err)
	}
	// What each client was told must match the logs. An unknown outcome
	// may have gone either way.
	committed := txn.Committed(sms)
	var r runResult
	r.duration = workTime
	for _, a := range rec.all {
		if a.setup {
			continue
		}
		r.attempts++
		switch a.outcome {
		case outCommitted:
			if !committed[a.id] {
				fail("client was told txn %d committed, but no shard committed it", a.id)
			}
		case outAborted, outError:
			if committed[a.id] {
				fail("client was told txn %d did not commit (%s), but it did", a.id, a.outcome)
			}
			if a.outcome == outAborted {
				r.aborted++
			}
		case outUnknown:
			r.unknown++
			if committed[a.id] {
				r.unknownCommitted++
			}
		}
		if committed[a.id] {
			r.committed++
			if len(a.shards) > 1 {
				r.cross++
			}
		}
	}
	if r.unknown == 0 {
		fail("no transaction ended with an unknown commit outcome; the kills proved nothing about it")
	}
	for i, k := range kills {
		spans := 0
		first := int64(-1)
		touches := func(shards []int) bool {
			for _, sh := range shards {
				if slices.Contains(k.led, txn.ShardID(sh)) {
					return true
				}
			}
			return false
		}
		for _, a := range rec.all {
			// A cross-shard transaction in flight across the kill on a
			// shard the victim led. Judged by the shards it set out to
			// use: one cut off mid-read by the kill counts too.
			if touches(a.planned) && a.begin <= k.at && a.finish >= k.at {
				spans++
			}
			if !touches(a.shards) {
				continue
			}
			if a.begin >= k.at && committed[a.id] && a.end >= 0 && (first < 0 || a.end < first) {
				first = a.end
			}
		}
		if spans == 0 {
			fail("kill %d (node %d, leading shards %v): no cross-shard txn was in flight across it", i+1, k.node, k.led)
		}
		if first < 0 {
			fail("kill %d (node %d, leading shards %v): no txn on those shards committed afterwards", i+1, k.node, k.led)
		}
		kills[i].failover = time.Duration(first - k.at)
		t.Logf("kill %d: node %d leading shards %v; %d cross-shard txns in flight across it; first txn on those shards begun after it committed %v later",
			i+1, k.node, k.led, spans, kills[i].failover.Round(time.Millisecond))
	}
	r.kills = kills
	t.Logf("%d attempts in %v: %d committed (%.0f/s), %d aborted, %d cross-shard commits, %d unknown outcomes (%d committed); history of %d committed txns is strictly serializable",
		r.attempts, workTime.Round(time.Millisecond), r.committed, float64(r.committed)/workTime.Seconds(), r.aborted, r.cross, r.unknown, r.unknownCommitted, len(h.Txns))
	return r
}

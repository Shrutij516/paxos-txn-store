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
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
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
	if err := fs.Parse(args); err != nil {
		return err
	}
	c.addrs = strings.Split(*addrs, ",")
	switch {
	case c.clients < 1 || c.short < 0 || c.short > c.clients:
		return errors.New("-clients must be at least 1 and -short-clients between 0 and -clients")
	case c.accounts < 2 || c.initial < 1:
		return errors.New("-accounts must be at least 2 and -initial at least 1")
	case c.duration <= 0:
		return errors.New("-duration must be positive")
	}
	f, err := os.Create(c.out)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	s, err := runLoad(ctx, c, f)
	if err != nil {
		return err
	}
	log.Printf("loadgen: %d attempts in %v: %d committed (%.0f/s), %d aborted, %d unknown, %d failed before commit",
		s.attempts.Load(), c.duration, s.committed.Load(), float64(s.committed.Load())/c.duration.Seconds(),
		s.aborted.Load(), s.unknown.Load(), s.failed.Load())
	return nil
}

type stats struct{ attempts, committed, aborted, unknown, failed atomic.Int64 }

// runLoad opens the accounts and runs the clients until c.duration passes
// or ctx ends, writing every attempt to w.
func runLoad(ctx context.Context, c config, w *os.File) (*stats, error) {
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
	if err := sc.Run(sctx, func(ctx context.Context, t *client.Txn) error {
		setup.begin = since()
		for i := 0; i < c.accounts; i++ {
			if err := t.Write(ctx, history.Account(i), strconv.Itoa(c.initial)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("opening the accounts: %w", err)
	}

	runCtx, stopRun := context.WithTimeout(ctx, c.duration)
	defer stopRun()
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
				_ = cl.Run(tctx, r.body(c, rng)) // every attempt is recorded by onAttempt
				cancel()
			}
		}()
	}
	wg.Wait()
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

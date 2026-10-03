package main

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/Shrutij516/paxos-txn-store/client"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/server"
)

const (
	numClients      = 10 // clients with a generous deadline per operation
	numShortClients = 2  // clients with a deadline shorter than a failover
	numKeys         = 3
)

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

type kvIn struct {
	put        bool
	key, value string
}

type kvOut struct {
	value   string
	unknown bool
}

var kvModel = porcupine.Model{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		by := map[string][]porcupine.Operation{}
		for _, op := range h {
			by[op.Input.(kvIn).key] = append(by[op.Input.(kvIn).key], op)
		}
		var out [][]porcupine.Operation
		for _, k := range slices.Sorted(maps.Keys(by)) {
			out = append(out, by[k])
		}
		return out
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		in, out, st := input.(kvIn), output.(kvOut), state.(string)
		if in.put {
			return true, in.value
		}
		return out.unknown || out.value == st, st
	},
}

// coverDirEnv, when set to a directory, makes the test build paxosd with
// coverage instrumentation and has every child process write its coverage
// data there (GOCOVERDIR). make cover merges it into the report.
const coverDirEnv = "PAXOSD_GOCOVERDIR"

// TestMultiProcessFailover builds paxosd, runs three real processes, drives
// them with 10 concurrent SDK clients plus 2 clients with short deadlines,
// SIGKILLs the leader mid-run, restarts it, and checks the recorded history
// for linearizability. It runs at two tick lengths, which gives two election
// timeout ranges (the replica's timing is fixed in ticks: heartbeat every 4,
// election timeout 20 to 40). 3 iterations per setting; under -short, 1
// iteration at the default tick only.
func TestMultiProcessFailover(t *testing.T) {
	iters := 3
	ticks := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond}
	if testing.Short() {
		iters, ticks = 1, ticks[:1]
	}
	bin := filepath.Join(t.TempDir(), "paxosd")
	args := []string{"build", "-o", bin}
	if os.Getenv(coverDirEnv) != "" {
		args = append(args, "-cover", "-covermode=set", "-coverpkg=github.com/Shrutij516/paxos-txn-store/...")
	}
	build := exec.Command("go", append(args, ".")...)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	tm := paxos.DefaultLogTiming
	for _, tick := range ticks {
		var total, worst time.Duration
		var unknown int64
		for it := 0; it < iters; it++ {
			t.Run(fmt.Sprintf("tick%v/iter%d", tick, it), func(t *testing.T) {
				r := runFailover(t, bin, uint64(it)+1, tick)
				total += r.failover
				worst = max(worst, r.failover)
				unknown += r.unknown
			})
		}
		if unknown == 0 {
			t.Errorf("tick %v: no operation ended with an unknown outcome; the short clients prove nothing", tick)
		}
		t.Logf("tick %v: heartbeat %v, election timeout %v to %v, peer RPC timeout %v: failover mean %v, max %v over %d runs; %d ops with unknown outcome",
			tick, tick*time.Duration(tm.HeartbeatEvery), tick*time.Duration(tm.ElectionMin), tick*time.Duration(tm.ElectionMax),
			server.DefaultRPCTimeout, (total / time.Duration(iters)).Round(time.Millisecond), worst.Round(time.Millisecond), iters, unknown)
	}
}

type failoverResult struct {
	failover time.Duration
	done     int64
	unknown  int64
}

func runFailover(t *testing.T, bin string, seed uint64, tick time.Duration) failoverResult {
	dir := t.TempDir()
	addrs := []string{freeAddr(t), freeAddr(t), freeAddr(t)}
	var peerList []string
	for i, a := range addrs {
		peerList = append(peerList, fmt.Sprintf("%d=%s", i+1, a))
	}
	procs := make([]*proc, 3)
	for i := range procs {
		procs[i] = &proc{
			args: []string{"-id", fmt.Sprint(i + 1), "-peers", strings.Join(peerList, ","),
				"-data-dir", filepath.Join(dir, fmt.Sprintf("n%d", i+1)), "-tick", tick.String()},
			log: filepath.Join(dir, fmt.Sprintf("n%d.log", i+1)),
		}
		procs[i].start(t, bin)
	}
	defer func() {
		for _, p := range procs {
			_ = p.cmd.Process.Kill() // no-op if already exited
			_, _ = p.wait(5 * time.Second)
		}
	}()

	// Wait until the cluster accepts a write, and learn the leader.
	probe, err := client.New(addrs, client.Options{AttemptTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = probe.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := probe.Put(ctx, "ready", "1"); err != nil {
		t.Fatalf("cluster never became ready: %v", err)
	}

	start := time.Now()
	since := func() int64 { return int64(time.Since(start)) }
	var (
		mu       sync.Mutex
		history  []porcupine.Operation
		done     atomic.Int64
		failed   atomic.Int64
		killedAt atomic.Int64 // ns since start, 0 until the kill
		firstOK  atomic.Int64 // ns since start of the first success after the kill
		stop     = make(chan struct{})
		wg       sync.WaitGroup
	)
	for ci := 0; ci < numClients+numShortClients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			// Short clients give up after 300ms, so operations caught by the
			// failover end with an unknown outcome.
			attempt, deadline := 500*time.Millisecond, 5*time.Second
			if ci >= numClients {
				attempt, deadline = 100*time.Millisecond, 300*time.Millisecond
			}
			c, err := client.New(addrs, client.Options{AttemptTimeout: attempt})
			if err != nil {
				t.Error(err)
				return
			}
			defer func() { _ = c.Close() }()
			rng := rand.New(rand.NewPCG(seed, uint64(ci)))
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				in := kvIn{key: fmt.Sprintf("k%d", rng.IntN(numKeys))}
				if rng.IntN(2) == 0 {
					in.put, in.value = true, fmt.Sprintf("c%d-%d", ci, n)
				}
				opCtx, opCancel := context.WithTimeout(context.Background(), deadline)
				call := since()
				var out kvOut
				if in.put {
					err = c.Put(opCtx, in.key, in.value)
				} else {
					out.value, err = c.Get(opCtx, in.key)
				}
				ret := since()
				opCancel()
				op := porcupine.Operation{ClientId: ci, Input: in, Call: call}
				if err != nil {
					// Unknown outcome: it may or may not have taken effect.
					failed.Add(1)
					op.Output, op.Return = kvOut{unknown: true}, -1
				} else {
					done.Add(1)
					op.Output, op.Return = out, ret
					if k := killedAt.Load(); k != 0 && call >= k {
						firstOK.CompareAndSwap(0, ret)
					}
				}
				mu.Lock()
				history = append(history, op)
				mu.Unlock()
			}
		}(ci)
	}

	time.Sleep(1 * time.Second)
	// Learn the current leader and kill it.
	if err := probe.Put(ctx, "probe", "1"); err != nil {
		t.Fatal(err)
	}
	victim := slices.Index(addrs, probe.Leader())
	if victim < 0 {
		t.Fatalf("leader %q not among %v", probe.Leader(), addrs)
	}
	if err := procs[victim].cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	killedAt.Store(since())
	if ok, _ := procs[victim].wait(5 * time.Second); !ok {
		t.Fatal("killed process did not exit")
	}
	time.Sleep(1500 * time.Millisecond)
	procs[victim].start(t, bin) // restart from its data dir
	time.Sleep(1500 * time.Millisecond)
	close(stop)
	wg.Wait()
	end := since() + 1

	for i := range history {
		if history[i].Return < 0 {
			history[i].Return = end
		}
	}
	if done.Load() == 0 {
		t.Fatal("no operation completed")
	}
	if firstOK.Load() == 0 {
		t.Fatal("no operation completed after the leader was killed")
	}
	if !porcupine.CheckOperations(kvModel, history) {
		t.Fatalf("history of %d operations is not linearizable (logs in %s)", len(history), dir)
	}
	failover := time.Duration(firstOK.Load() - killedAt.Load())
	t.Logf("node %d (leader) SIGKILLed; %d ops completed, %d with unknown outcome; first op issued after the kill completed %v later",
		victim+1, done.Load(), failed.Load(), failover.Round(time.Millisecond))

	// Graceful shutdown on SIGTERM.
	for _, p := range procs {
		_ = p.cmd.Process.Signal(syscall.SIGTERM)
	}
	for i, p := range procs {
		if ok, err := p.wait(10 * time.Second); !ok || err != nil {
			t.Fatalf("node %d did not exit cleanly on SIGTERM (exited=%v): %v", i+1, ok, err)
		}
	}
	return failoverResult{failover: failover, done: done.Load(), unknown: failed.Load()}
}

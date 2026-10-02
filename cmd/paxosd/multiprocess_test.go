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
)

const (
	numClients = 10
	numKeys    = 3
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

// TestMultiProcessFailover builds paxosd, runs three real processes, drives
// them with 10 concurrent SDK clients, SIGKILLs the leader mid-run, restarts
// it, and checks the recorded history for linearizability. 3 iterations, 1
// under -short.
func TestMultiProcessFailover(t *testing.T) {
	iters := 3
	if testing.Short() {
		iters = 1
	}
	bin := filepath.Join(t.TempDir(), "paxosd")
	build := exec.Command("go", "build", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	for it := 0; it < iters; it++ {
		t.Run(fmt.Sprintf("iter%d", it), func(t *testing.T) { runFailover(t, bin, uint64(it)+1) })
	}
}

func runFailover(t *testing.T, bin string, seed uint64) {
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
				"-data-dir", filepath.Join(dir, fmt.Sprintf("n%d", i+1)), "-tick", "10ms"},
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
	for ci := 0; ci < numClients; ci++ {
		wg.Add(1)
		go func(ci int) {
			defer wg.Done()
			c, err := client.New(addrs, client.Options{AttemptTimeout: 500 * time.Millisecond})
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
				opCtx, opCancel := context.WithTimeout(context.Background(), 5*time.Second)
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
}

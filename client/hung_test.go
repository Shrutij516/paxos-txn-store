package client_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Shrutij516/paxos-txn-store/client"
	txnv1 "github.com/Shrutij516/paxos-txn-store/proto/txn/v1"
)

// fakeNode is a TxnServer for client tests. A hung node accepts every call
// and never answers, like a node whose container was killed behind
// Docker's NAT (its peers' and clients' connections are black-holed, not
// refused). A healthy node answers Begin, and answers Read as the leader
// of every shard.
type fakeNode struct {
	txnv1.UnimplementedTxnServer
	hung  bool
	ok    atomic.Bool // set by recover: a hung node answers from then on
	calls atomic.Int64
}

func (f *fakeNode) recover() { f.ok.Store(true) }

func (f *fakeNode) wait(ctx context.Context) error {
	f.calls.Add(1)
	if f.hung && !f.ok.Load() {
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (f *fakeNode) Begin(ctx context.Context, _ *txnv1.BeginRequest) (*txnv1.BeginResponse, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	return &txnv1.BeginResponse{Txn: &txnv1.TxnMeta{Id: uint64(f.calls.Load()), Ts: 1}, Shards: 3}, nil
}

func (f *fakeNode) Read(ctx context.Context, _ *txnv1.ReadRequest) (*txnv1.ReadResponse, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	return &txnv1.ReadResponse{Status: txnv1.Status_STATUS_OK, Value: "v", Version: 1}, nil
}

func (f *fakeNode) Abort(ctx context.Context, _ *txnv1.AbortRequest) (*txnv1.AbortResponse, error) {
	if err := f.wait(ctx); err != nil {
		return nil, err
	}
	return &txnv1.AbortResponse{}, nil
}

func serveFake(t *testing.T, f *fakeNode) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := grpc.NewServer()
	txnv1.RegisterTxnServer(s, f)
	go func() { _ = s.Serve(l) }()
	t.Cleanup(s.Stop)
	return l.Addr().String()
}

// TestHungNodeIsNotRetried: once an attempt at a node times out, the
// client does not send it more attempts while it stays silent: Begin
// sticks to a node that answers, and rotation for an unknown leader skips
// it (incident I19). Before, Begin rotated over every node on every call
// and each attempt at the hung one cost a whole AttemptTimeout.
func TestHungNodeIsNotRetried(t *testing.T) {
	hung, ok := &fakeNode{hung: true}, &fakeNode{}
	addrs := []string{serveFake(t, hung), serveFake(t, ok)}
	const attempt = 100 * time.Millisecond
	cl, err := client.New(addrs, client.Options{AttemptTimeout: attempt})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	const txns = 10
	for i := 0; i < txns; i++ {
		tx, err := cl.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// One key per shard, so each shard's leader is looked up.
		for _, k := range []string{keyOn(0, i), keyOn(1, i), keyOn(2, i)} {
			if _, err := tx.Read(ctx, k); err != nil {
				t.Fatal(err)
			}
		}
	}
	took := time.Since(start)
	t.Logf("%d transactions in %v; attempts at the hung node: %d", txns, took.Round(time.Millisecond), hung.calls.Load())
	if n := hung.calls.Load(); n > 1 {
		t.Fatalf("the hung node got %d attempts (each a %v timeout), want at most 1", n, attempt)
	}
}

// TestAllNodesAvoidedStillTried: when every node has failed within the
// avoid window, the client keeps trying them (oldest failure first) and
// succeeds as soon as one answers, without waiting for a window to end.
func TestAllNodesAvoidedStillTried(t *testing.T) {
	a, b := &fakeNode{hung: true}, &fakeNode{hung: true}
	addrs := []string{serveFake(t, a), serveFake(t, b)}
	const attempt = 100 * time.Millisecond // avoid window: max(2 x 100 ms, 1 s) = 1 s
	// Tiny backoff, so the time taken is the attempts' own: a client that
	// waited for an avoid window to end could not finish under 1 s.
	cl, err := client.New(addrs, client.Options{AttemptTimeout: attempt, BackoffBase: time.Millisecond, BackoffMax: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })

	// Both nodes hang until b has failed twice; then b answers.
	go func() {
		for b.calls.Load() < 2 {
			time.Sleep(5 * time.Millisecond)
		}
		b.recover()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	if _, err := cl.Begin(ctx); err != nil {
		t.Fatalf("Begin with every node avoided: %v", err)
	}
	took := time.Since(start)
	t.Logf("Begin succeeded after %v; attempts: a %d, b %d", took.Round(time.Millisecond), a.calls.Load(), b.calls.Load())
	if a.calls.Load() < 2 || b.calls.Load() < 3 {
		t.Fatalf("attempts a=%d b=%d: the client stopped retrying nodes in the avoid window", a.calls.Load(), b.calls.Load())
	}
	// About five 100 ms timeouts, well inside the 1 s avoid window of the
	// first failure: nothing waited for a window to end.
	if took > 900*time.Millisecond {
		t.Fatalf("Begin took %v: it waited instead of retrying the oldest failure", took)
	}
}

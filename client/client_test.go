package client

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Shrutij516/paxos-txn-store/internal/kv"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/server"
	"github.com/Shrutij516/paxos-txn-store/internal/testcluster"
	kvv1 "github.com/Shrutij516/paxos-txn-store/proto/kv/v1"
)

func bg(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// A client that only knows a follower first is redirected to the leader.
func TestRedirectToLeader(t *testing.T) {
	cl := testcluster.Start(t, 3, nil)
	leader, err := cl.Leader(bg(t))
	if err != nil {
		t.Fatal(err)
	}
	var addrs []string
	for _, id := range cl.IDs {
		if id != leader {
			addrs = append(addrs, cl.Addr(id)) // followers first
		}
	}
	addrs = append(addrs, cl.Addr(leader))
	// A committed write guarantees every follower has heard from the leader.
	warm, _ := New([]string{cl.Addr(leader)}, Options{})
	defer func() { _ = warm.Close() }()
	if err := warm.Put(bg(t), "warm", "up"); err != nil {
		t.Fatal(err)
	}
	c, err := New(addrs, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if err := c.Put(bg(t), "k", "v"); err != nil {
		t.Fatal(err)
	}
	if c.redirects < 1 {
		t.Fatalf("redirects = %d, want at least 1", c.redirects)
	}
	if c.Leader() != cl.Addr(leader) {
		t.Fatalf("client leader %q, want %q", c.Leader(), cl.Addr(leader))
	}
	if c.attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (follower, then leader)", c.attempts)
	}
}

// The first reply for key "slow" is held back past the client's attempt
// timeout, after the server has already committed and applied the Put. The
// client retries with the same sequence number, and the cluster applies the
// request exactly once.
func TestRetryAfterTimeoutAppliedOnce(t *testing.T) {
	var held atomic.Bool
	delay := grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		resp, err := h(ctx, req)
		if p, ok := req.(*kvv1.PutRequest); ok && p.GetKey() == "slow" && err == nil &&
			resp.(*kvv1.PutResponse).GetOk() && held.CompareAndSwap(false, true) {
			time.Sleep(600 * time.Millisecond) // reply lost, as far as the client knows
		}
		return resp, err
	})
	cl := testcluster.Start(t, 3, func(cfg *server.Config) { cfg.ServerOptions = []grpc.ServerOption{delay} })
	c, err := New(cl.Addrs, Options{AttemptTimeout: 300 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if err := c.Put(bg(t), "slow", "1"); err != nil {
		t.Fatal(err)
	}
	if !held.Load() {
		t.Fatal("interceptor never held a reply; the test proves nothing")
	}
	if c.retries < 1 {
		t.Fatalf("retries = %d, want at least 1", c.retries)
	}
	// Another client overwrites the key; a late duplicate of the first Put
	// must not resurrect "1".
	c2, _ := New(cl.Addrs, Options{})
	defer func() { _ = c2.Close() }()
	if err := c2.Put(bg(t), "slow", "2"); err != nil {
		t.Fatal(err)
	}
	if v, err := c.Get(bg(t), "slow"); err != nil || v != "2" {
		t.Fatalf("Get = %q, %v; want 2", v, err)
	}
	leader, _ := cl.Leader(bg(t))
	want := cl.Commit(leader)
	for _, id := range cl.IDs {
		deadline := time.Now().Add(10 * time.Second)
		for cl.Commit(id) < want && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		var execs int
		var v string
		cl.Nodes[id].Inspect(func(_ *paxos.Replica, s *kv.Store) { execs, v = s.MaxExecutions(), s.Value("slow") })
		if execs != 1 || v != "2" {
			t.Fatalf("node %d: max executions %d, slow=%q", id, execs, v)
		}
	}
}

// With no reachable server the client keeps retrying until its context ends.
func TestGivesUpWhenContextEnds(t *testing.T) {
	c, err := New([]string{"127.0.0.1:1"}, Options{AttemptTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := c.Get(ctx, "k"); err == nil {
		t.Fatal("expected an error")
	}
	if c.retries < 2 {
		t.Fatalf("retries = %d, want several", c.retries)
	}
}

func TestNewValidates(t *testing.T) {
	if _, err := New(nil, Options{}); err != ErrNoAddrs {
		t.Fatalf("New(nil) = %v", err)
	}
	c, _ := New([]string{"a:1"}, Options{ClientID: 42})
	if c.ID() != 42 {
		t.Fatalf("ID = %d", c.ID())
	}
	d, _ := New([]string{"a:1"}, Options{})
	if d.ID() == 0 || d.ID() == c.ID() || d.ID()>>63 != 0 {
		t.Fatalf("random ID %d", d.ID())
	}
}

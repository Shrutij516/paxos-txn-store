package client_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Shrutij516/paxos-txn-store/client"
	"github.com/Shrutij516/paxos-txn-store/internal/testcluster"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
)

const shards = 3

func keyOn(sh txn.ShardID, i int) string {
	for n := 0; ; n++ {
		k := fmt.Sprintf("k%d", n)
		if txn.ShardOf(k, shards) == sh {
			if i == 0 {
				return k
			}
			i--
		}
	}
}

func newClient(t *testing.T, c *testcluster.Cluster, opts client.Options) *client.Client {
	t.Helper()
	cl, err := client.New(c.Addrs, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cl.Close() })
	return cl
}

func TestNew(t *testing.T) {
	if _, err := client.New(nil, client.Options{}); err != client.ErrNoAddrs {
		t.Fatalf("New without addresses: %v", err)
	}
	cl, err := client.New([]string{"127.0.0.1:1"}, client.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if cl.Shards() != 0 || cl.Leader(0) != "" {
		t.Fatal("a new client knows nothing about the cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := cl.Begin(ctx); err == nil {
		t.Fatal("Begin against a dead address must fail when ctx ends")
	}
	if err := cl.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestLeaderCache: the client learns each shard's leader, and after a
// redirect from a stale cache it follows the hint.
func TestLeaderCache(t *testing.T) {
	c := testcluster.Start(t, 3, shards, nil)
	cl := newClient(t, c, client.Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for sh := txn.ShardID(0); sh < shards; sh++ {
		k := keyOn(sh, 0)
		if err := cl.Run(ctx, func(ctx context.Context, t *client.Txn) error { return t.Write(ctx, k, "v") }); err != nil {
			t.Fatal(err)
		}
		id, err := c.Leader(ctx, sh)
		if err != nil {
			t.Fatal(err)
		}
		if cl.Leader(int(sh)) != c.Addr(id) {
			t.Fatalf("shard %d: client caches %q, leader is %s", sh, cl.Leader(int(sh)), c.Addr(id))
		}
	}
	if cl.Shards() != shards {
		t.Fatalf("client learned %d shards", cl.Shards())
	}
}

// TestRunRetriesThenGivesUp checks Run's retry policy: an aborted attempt
// is retried with the same start timestamp, up to MaxAttempts.
func TestRunRetriesThenGivesUp(t *testing.T) {
	c := testcluster.Start(t, 3, shards, nil)
	var ids []uint64
	cl := newClient(t, c, client.Options{MaxAttempts: 3, BackoffBase: time.Millisecond, OnAttempt: func(tx *client.Txn, _ error) {
		ids = append(ids, tx.ID())
	}})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	calls := 0
	err := cl.Run(ctx, func(context.Context, *client.Txn) error {
		calls++
		return client.ErrAborted
	})
	if !errors.Is(err, client.ErrAborted) || calls != 3 || len(ids) != 3 || ids[0] == ids[1] {
		t.Fatalf("Run: err %v after %d calls, attempt ids %v", err, calls, ids)
	}
	// Any other error is returned at once.
	boom := errors.New("boom")
	calls = 0
	if err := cl.Run(ctx, func(context.Context, *client.Txn) error { calls++; return boom }); err != boom || calls != 1 {
		t.Fatalf("Run: err %v after %d calls, want boom after 1", err, calls)
	}
	// A transaction is finished after Commit.
	tx, err := cl.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("empty commit: %v", err)
	}
	if _, err := tx.Read(ctx, "x"); err != client.ErrDone {
		t.Fatalf("read after commit: %v", err)
	}
	if err := tx.Write(ctx, "x", "1"); err != client.ErrDone {
		t.Fatalf("write after commit: %v", err)
	}
	if err := tx.Commit(ctx); err != client.ErrDone {
		t.Fatalf("second commit: %v", err)
	}
}

// TestCommitUnknownOutcome: when every node goes away after the reads, the
// commit cannot learn its outcome and says so.
func TestCommitUnknownOutcome(t *testing.T) {
	c := testcluster.Start(t, 3, shards, nil)
	cl := newClient(t, c, client.Options{AttemptTimeout: 100 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	k0, k2 := keyOn(0, 0), keyOn(2, 0)
	tx, err := cl.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Read(ctx, k0); err != nil {
		t.Fatal(err)
	}
	if err := tx.Write(ctx, k2, "x"); err != nil {
		t.Fatal(err)
	}
	if got := tx.ReadVersions(); len(got) != 1 || got[k0] != 0 {
		t.Fatalf("read versions %v", got)
	}
	c.StopAll()
	cctx, ccancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer ccancel()
	if err := tx.Commit(cctx); !errors.Is(err, client.ErrUnknown) {
		t.Fatalf("commit with every node down: %v, want ErrUnknown", err)
	}
}

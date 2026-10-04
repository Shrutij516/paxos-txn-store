package server_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Shrutij516/paxos-txn-store/client"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/server"
	"github.com/Shrutij516/paxos-txn-store/internal/testcluster"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
)

const shards = 3

// keyOn returns the i-th key of the form "k<n>" that lives on shard sh.
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

func put(ctx context.Context, cl *client.Client, kv map[string]string) error {
	return cl.Run(ctx, func(ctx context.Context, t *client.Txn) error {
		for k, v := range kv {
			if err := t.Write(ctx, k, v); err != nil {
				return err
			}
		}
		return nil
	})
}

func get(ctx context.Context, cl *client.Client, keys ...string) (map[string]string, error) {
	out := map[string]string{}
	err := cl.Run(ctx, func(ctx context.Context, t *client.Txn) error {
		for _, k := range keys {
			v, err := t.Read(ctx, k)
			if err != nil {
				return err
			}
			out[k] = v
		}
		return nil
	})
	return out, err
}

// transfer moves amt from a to b and reports the shards it touched.
func transfer(ctx context.Context, cl *client.Client, a, b string, amt int) ([]int, error) {
	var touched []int
	err := cl.Run(ctx, func(ctx context.Context, t *client.Txn) error {
		va, err := t.Read(ctx, a)
		if err != nil {
			return err
		}
		vb, err := t.Read(ctx, b)
		if err != nil {
			return err
		}
		na, _ := strconv.Atoi(va)
		nb, _ := strconv.Atoi(vb)
		if err := t.Write(ctx, a, strconv.Itoa(na-amt)); err != nil {
			return err
		}
		if err := t.Write(ctx, b, strconv.Itoa(nb+amt)); err != nil {
			return err
		}
		touched = t.Shards()
		return nil
	})
	return touched, err
}

// replicaValue returns a node's committed value of key on its shard.
func replicaValue(n *server.Node, key string) string {
	var v string
	n.Inspect(txn.ShardOf(key, shards), func(_ *paxos.Replica, sm *txn.SM, _ *txn.Server) { v = sm.Get(key).Value })
	return v
}

// waitReplicas waits until every running node has applied want for key.
func waitReplicas(t *testing.T, c *testcluster.Cluster, key, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for _, id := range c.IDs {
		n := c.Nodes[id]
		if n == nil {
			continue
		}
		for replicaValue(n, key) != want {
			if time.Now().After(deadline) {
				t.Fatalf("node %d has %s=%q, want %q", id, key, replicaValue(n, key), want)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// TestSingleAndCrossShard runs single-shard and cross-shard transactions on
// a 3-node, 3-shard cluster and checks every replica of every shard.
func TestSingleAndCrossShard(t *testing.T) {
	c := testcluster.Start(t, 3, shards, nil)
	cl := newClient(t, c, client.Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	a0, a1, b, d := keyOn(0, 0), keyOn(0, 1), keyOn(1, 0), keyOn(2, 0)
	if err := put(ctx, cl, map[string]string{a0: "100", a1: "100", b: "100", d: "100"}); err != nil {
		t.Fatal(err)
	}
	// Single shard: both keys on shard 0, committed in one phase.
	if sh, err := transfer(ctx, cl, a0, a1, 10); err != nil || len(sh) != 1 {
		t.Fatalf("single-shard transfer: shards %v, err %v", sh, err)
	}
	// Cross shard: two-phase commit coordinated by shard 0 or 1.
	if sh, err := transfer(ctx, cl, a0, d, 5); err != nil || len(sh) != 2 {
		t.Fatalf("cross-shard transfer: shards %v, err %v", sh, err)
	}
	if sh, err := transfer(ctx, cl, d, b, 7); err != nil || len(sh) != 2 {
		t.Fatalf("cross-shard transfer: shards %v, err %v", sh, err)
	}
	got, err := get(ctx, cl, a0, a1, b, d)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{a0: "85", a1: "110", b: "107", d: "98"}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s = %q, want %q (all: %v)", k, got[k], v, got)
		}
		waitReplicas(t, c, k, v)
	}
	// Each shard's state lives only on its own replicas.
	for _, id := range c.IDs {
		c.Nodes[id].Inspect(1, func(_ *paxos.Replica, sm *txn.SM, _ *txn.Server) {
			if _, ok := sm.Data()[a0]; ok {
				t.Errorf("node %d: shard 1 holds %s, a shard 0 key", id, a0)
			}
		})
	}
}

// TestShardLeaderFailover stops the node leading shard 1. Transactions on
// shard 1, alone and with another shard, commit under the new leader, and
// the stopped node catches up after a restart.
func TestShardLeaderFailover(t *testing.T) {
	c := testcluster.Start(t, 3, shards, nil)
	cl := newClient(t, c, client.Options{AttemptTimeout: 300 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	k1, k1b, k2 := keyOn(1, 0), keyOn(1, 1), keyOn(2, 0)
	if err := put(ctx, cl, map[string]string{k1: "50", k1b: "50", k2: "50"}); err != nil {
		t.Fatal(err)
	}
	old, err := c.Leader(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if cl.Leader(1) != c.Addr(old) {
		t.Fatalf("client caches %q as shard 1's leader, want %q", cl.Leader(1), c.Addr(old))
	}
	c.Stop(old)
	start := time.Now()
	if _, err := transfer(ctx, cl, k1, k1b, 1); err != nil {
		t.Fatalf("single-shard txn after failover: %v", err)
	}
	failover := time.Since(start)
	if _, err := transfer(ctx, cl, k2, k1, 2); err != nil {
		t.Fatalf("cross-shard txn after failover: %v", err)
	}
	now, err := c.Leader(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if now == old || cl.Leader(1) != c.Addr(now) {
		t.Fatalf("shard 1 leader %d (client caches %q), old leader %d", now, cl.Leader(1), old)
	}
	t.Logf("shard 1 leader %d stopped; first txn on shard 1 committed after %v under leader %d", old, failover.Round(time.Millisecond), now)
	c.Restart(old)
	waitReplicas(t, c, k1, "51")
	waitReplicas(t, c, k1b, "51")
	waitReplicas(t, c, k2, "48")
}

// TestConcurrentIncrements runs conflicting read-modify-write transactions
// from several goroutines and clients. Wound-wait aborts some of them; Run
// retries until each commits, so no increment is lost.
func TestConcurrentIncrements(t *testing.T) {
	c := testcluster.Start(t, 3, shards, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ka, kb := keyOn(0, 0), keyOn(2, 0)
	const workers, each = 4, 5
	var aborted sync.Map
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		cl := newClient(t, c, client.Options{MaxAttempts: 50, OnAttempt: func(tx *client.Txn, err error) {
			if errors.Is(err, client.ErrAborted) {
				aborted.Store(tx.ID(), true)
			}
		}})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				err := cl.Run(ctx, func(ctx context.Context, t *client.Txn) error {
					for _, k := range []string{ka, kb} {
						v, err := t.Read(ctx, k)
						if err != nil {
							return err
						}
						n, _ := strconv.Atoi(v)
						if err := t.Write(ctx, k, strconv.Itoa(n+1)); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	cl := newClient(t, c, client.Options{})
	got, err := get(ctx, cl, ka, kb)
	if err != nil {
		t.Fatal(err)
	}
	want := strconv.Itoa(workers * each)
	if got[ka] != want || got[kb] != want {
		t.Fatalf("counters %v, want %s each", got, want)
	}
	n := 0
	aborted.Range(func(any, any) bool { n++; return true })
	t.Logf("%d increments committed, %d attempts aborted and retried", workers*each, n)
}

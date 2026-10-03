package server_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Shrutij516/paxos-txn-store/client"
	"github.com/Shrutij516/paxos-txn-store/internal/kv"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/server"
	"github.com/Shrutij516/paxos-txn-store/internal/testcluster"
)

func newClient(t *testing.T, addrs []string) *client.Client {
	t.Helper()
	c, err := client.New(addrs, client.Options{AttemptTimeout: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func ctx(t *testing.T, d time.Duration) context.Context {
	c, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return c
}

// Test 2a: basic Get and Put through the SDK on a 3-node cluster.
func TestClusterGetPut(t *testing.T) {
	cl := testcluster.Start(t, 3, nil)
	c := newClient(t, cl.Addrs)
	if v, err := c.Get(ctx(t, 10*time.Second), "missing"); err != nil || v != "" {
		t.Fatalf("Get(missing) = %q, %v", v, err)
	}
	for i := 0; i < 20; i++ {
		if err := c.Put(ctx(t, 5*time.Second), "k", fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
		v, err := c.Get(ctx(t, 5*time.Second), "k")
		if err != nil || v != fmt.Sprint(i) {
			t.Fatalf("Get(k) = %q, %v; want %d", v, err, i)
		}
	}
	// Every node applies the same state.
	leader, err := cl.Leader(ctx(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	want := cl.Commit(leader)
	for _, id := range cl.IDs {
		waitFor(t, func() bool { return cl.Commit(id) >= want })
		var v string
		cl.Nodes[id].Inspect(func(_ *paxos.Replica, s *kv.Store) { v = s.Value("k") })
		if v != "19" {
			t.Fatalf("node %d has k=%q", id, v)
		}
	}
}

// Test 2b: the leader stops; a new leader takes over and the SDK follows.
func TestClusterLeaderFailover(t *testing.T) {
	cl := testcluster.Start(t, 3, nil)
	c := newClient(t, cl.Addrs)
	if err := c.Put(ctx(t, 10*time.Second), "a", "1"); err != nil {
		t.Fatal(err)
	}
	old, err := cl.Leader(ctx(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	cl.Stop(old)
	start := time.Now()
	if err := c.Put(ctx(t, 10*time.Second), "a", "2"); err != nil {
		t.Fatalf("Put after failover: %v", err)
	}
	t.Logf("first write after leader %d stopped took %v", old, time.Since(start))
	nl, err := cl.Leader(ctx(t, 5*time.Second))
	if err != nil || nl == old {
		t.Fatalf("new leader %d, %v", nl, err)
	}
	if v, err := c.Get(ctx(t, 5*time.Second), "a"); err != nil || v != "2" {
		t.Fatalf("Get(a) = %q, %v", v, err)
	}
}

// Test 2c: a follower stops, misses writes, restarts from its SQLite file
// and catches up.
func TestClusterFollowerRestartFromDisk(t *testing.T) {
	cl := testcluster.Start(t, 3, nil)
	c := newClient(t, cl.Addrs)
	if err := c.Put(ctx(t, 10*time.Second), "x", "before"); err != nil {
		t.Fatal(err)
	}
	leader, err := cl.Leader(ctx(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var f paxos.NodeID
	for _, id := range cl.IDs {
		if id != leader {
			f = id
			break
		}
	}
	waitFor(t, func() bool { return cl.Commit(f) >= cl.Commit(leader) })
	before := cl.Commit(f)
	cl.Stop(f)
	for i := 0; i < 30; i++ {
		if err := c.Put(ctx(t, 5*time.Second), "x", fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	cl.Restart(f)
	// Recovered from disk before hearing from anyone.
	if got := cl.Commit(f); got < before {
		t.Fatalf("restarted commit %d, had %d before stopping", got, before)
	}
	waitFor(t, func() bool { return cl.Commit(f) >= cl.Commit(leader) })
	var v string
	cl.Nodes[f].Inspect(func(_ *paxos.Replica, s *kv.Store) { v = s.Value("x") })
	if v != "29" {
		t.Fatalf("restarted follower has x=%q, want 29", v)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached within 10s")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestParsePeers(t *testing.T) {
	p, err := server.ParsePeers("1=127.0.0.1:7001, 2=127.0.0.1:7002,3=h:3")
	if err != nil || len(p) != 3 || p[2] != "127.0.0.1:7002" {
		t.Fatalf("ParsePeers = %v, %v", p, err)
	}
	for _, bad := range []string{"", "1", "1=", "x=a:1", "0=a:1", "1=a:1,1=b:2"} {
		if _, err := server.ParsePeers(bad); err == nil {
			t.Errorf("ParsePeers(%q) should fail", bad)
		}
	}
}

func TestStartRejectsBadConfig(t *testing.T) {
	if _, err := server.Start(server.Config{ID: 4, Peers: map[paxos.NodeID]string{1: "x"}}); err == nil {
		t.Fatal("ID not in peers must fail")
	}
}

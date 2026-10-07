package server

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
	"github.com/Shrutij516/paxos-txn-store/internal/wire"
	paxosv1 "github.com/Shrutij516/paxos-txn-store/proto/paxos/v1"
	txnv1 "github.com/Shrutij516/paxos-txn-store/proto/txn/v1"
)

func TestParseCluster(t *testing.T) {
	good := `{"shards": 3, "nodes": [{"id": 2, "addr": "b:2"}, {"id": 1, "addr": "a:1"}]}`
	c, err := ParseCluster([]byte(good))
	if err != nil {
		t.Fatal(err)
	}
	if ids := c.IDs(); len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("IDs %v", ids)
	}
	if a := c.Addrs(); a[1] != "a:1" || a[2] != "b:2" {
		t.Fatalf("Addrs %v", a)
	}
	if n := c.ShardNodes(2); len(n) != 2 {
		t.Fatalf("ShardNodes %v", n)
	}
	for _, tc := range []struct{ in, want string }{
		{`{"shards": 0, "nodes": [{"id": 1, "addr": "a"}]}`, "shards must be"},
		{`{"shards": 5000, "nodes": [{"id": 1, "addr": "a"}]}`, "shards must be"},
		{`{"shards": 1, "nodes": []}`, "no nodes"},
		{`{"shards": 1, "nodes": [{"id": 0, "addr": "a"}]}`, "positive"},
		{`{"shards": 1, "nodes": [{"id": 1, "addr": "a"}, {"id": 1, "addr": "b"}]}`, "duplicate"},
		{`{"shards": 1, "nodes": [{"id": 1}]}`, "no addr"},
		{`{"shards": 1, "nodes": [{"id": 1, "addr": "a"}], "shard": 2}`, "unknown field"},
		{`{"shards": `, "unexpected EOF"},
	} {
		if _, err := ParseCluster([]byte(tc.in)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got %v, want an error containing %q", tc.in, err, tc.want)
		}
	}
	path := filepath.Join(t.TempDir(), "cluster.json")
	if err := os.WriteFile(path, []byte(good), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCluster(path); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCluster(path + ".missing"); err == nil {
		t.Fatal("missing file must fail")
	}
}

func TestStartErrors(t *testing.T) {
	cl := Cluster{Shards: 1, Nodes: []NodeConfig{{ID: 1, Addr: "127.0.0.1:0"}}}
	if _, err := Start(Config{ID: 2, Cluster: cl, DataDir: t.TempDir()}); err == nil {
		t.Fatal("a node missing from the config must not start")
	}
	if _, err := Start(Config{ID: 1, Cluster: Cluster{}, DataDir: t.TempDir()}); err == nil {
		t.Fatal("an invalid config must not start")
	}
	if _, err := Start(Config{ID: 1, Cluster: cl, DataDir: filepath.Join(t.TempDir(), "missing", "dir")}); err == nil {
		t.Fatal("an unusable data dir must fail")
	}
	cl.Nodes[0].Addr = "256.0.0.1:1"
	if _, err := Start(Config{ID: 1, Cluster: cl, DataDir: t.TempDir()}); err == nil {
		t.Fatal("an address that cannot be listened on must fail")
	}
}

// startCluster starts n nodes with the given shard count.
func startCluster(t *testing.T, n, shards int) []*Node {
	t.Helper()
	cl := Cluster{Shards: shards}
	var lis []net.Listener
	for i := 1; i <= n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lis = append(lis, l)
		cl.Nodes = append(cl.Nodes, NodeConfig{ID: paxos.NodeID(i), Addr: l.Addr().String()})
	}
	var nodes []*Node
	for i := range lis {
		node, err := Start(Config{ID: paxos.NodeID(i + 1), Cluster: cl, Listener: lis[i], DataDir: t.TempDir(), Tick: 5 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, node)
		t.Cleanup(node.Stop)
	}
	return nodes
}

// leaderOf waits for a node that leads shard sh.
func leaderOf(t *testing.T, nodes []*Node, sh txn.ShardID) *Node {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			var lead bool
			n.Inspect(sh, func(_ *paxos.Replica, _ *txn.SM, ts *txn.Server) { lead = ts.Leading() })
			if lead {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no leader for shard %d", sh)
	return nil
}

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func keyOn(sh txn.ShardID, shards int) string {
	for i := 0; ; i++ {
		if k := "k" + string(rune('a'+i)); txn.ShardOf(k, shards) == sh {
			return k
		}
	}
}

func TestRequestValidation(t *testing.T) {
	nodes := startCluster(t, 1, 2)
	leaderOf(t, nodes, 0)
	c := txnv1.NewTxnClient(dial(t, nodes[0].Addr()))
	ctx := context.Background()
	meta := &txnv1.TxnMeta{Id: 7, Ts: 1}
	k0, k1 := keyOn(0, 2), keyOn(1, 2)
	invalid := func(name string, err error) {
		t.Helper()
		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("%s: got %v, want InvalidArgument", name, err)
		}
	}
	_, err := c.Read(ctx, &txnv1.ReadRequest{Key: k0})
	invalid("read without txn", err)
	_, err = c.Write(ctx, &txnv1.WriteRequest{Key: k0})
	invalid("write without txn", err)
	_, err = c.Commit(ctx, &txnv1.CommitRequest{Parts: []*txnv1.Part{{Shard: 0}}})
	invalid("commit without txn", err)
	_, err = c.Commit(ctx, &txnv1.CommitRequest{Txn: meta})
	invalid("commit without parts", err)
	_, err = c.Commit(ctx, &txnv1.CommitRequest{Txn: meta, Parts: []*txnv1.Part{{Shard: 2}}})
	invalid("commit on a missing shard", err)
	_, err = c.Commit(ctx, &txnv1.CommitRequest{Txn: meta, Parts: []*txnv1.Part{{Shard: 0, Writes: map[string]string{k1: "x"}}}})
	invalid("write on the wrong shard", err)
	_, err = c.Commit(ctx, &txnv1.CommitRequest{Txn: meta, Parts: []*txnv1.Part{{Shard: 1, Reads: map[string]uint64{k0: 0}}}})
	invalid("read on the wrong shard", err)
	_, err = c.Commit(ctx, &txnv1.CommitRequest{Txn: meta, Parts: []*txnv1.Part{{Shard: 1}, {Shard: 1}}})
	invalid("shard twice", err)
	_, err = c.Abort(ctx, &txnv1.AbortRequest{Shard: 0})
	invalid("abort without txn", err)
	_, err = c.Abort(ctx, &txnv1.AbortRequest{Txn: 7, Shard: 9})
	invalid("abort on a missing shard", err)

	p := paxosv1.NewPeerClient(dial(t, nodes[0].Addr()))
	_, err = p.Send(ctx, &paxosv1.Envelope{From: 2, To: 1})
	invalid("envelope without body", err)
	hb := &paxosv1.Envelope_Heartbeat{Heartbeat: &paxosv1.Heartbeat{}}
	_, err = p.Send(ctx, &paxosv1.Envelope{From: 2, To: 3, Body: hb})
	invalid("envelope for another node", err)
	_, err = p.Send(ctx, &paxosv1.Envelope{From: 2, To: 1, Shard: 2, Body: hb})
	invalid("envelope for a missing shard", err)

	// Begin answers on any node and keeps a given start timestamp.
	b, err := c.Begin(ctx, &txnv1.BeginRequest{Ts: 42})
	if err != nil || b.GetTxn().GetTs() != 42 || b.GetTxn().GetId() == 0 || b.GetShards() != 2 {
		t.Fatalf("Begin: %v, %v", b, err)
	}
}

// TestNotLeader: a follower redirects every shard request to the leader.
func TestNotLeader(t *testing.T) {
	nodes := startCluster(t, 3, 1)
	leader := leaderOf(t, nodes, 0)
	var follower *Node
	for _, n := range nodes {
		if n != leader {
			follower = n
		}
	}
	// Wait until the follower has heard from the leader.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var known paxos.NodeID
		follower.Inspect(0, func(r *paxos.Replica, _ *txn.SM, _ *txn.Server) { known = r.Leader() })
		if known == leader.ID() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower knows leader %d, want %d", known, leader.ID())
		}
		time.Sleep(10 * time.Millisecond)
	}
	c := txnv1.NewTxnClient(dial(t, follower.Addr()))
	ctx := context.Background()
	meta := &txnv1.TxnMeta{Id: 7, Ts: 1}
	check := func(name string, st txnv1.Status, hint *txnv1.LeaderHint, err error) {
		t.Helper()
		if err != nil || st != txnv1.Status_STATUS_NOT_LEADER || hint.GetAddr() != leader.Addr() || hint.GetId() != int32(leader.ID()) {
			t.Errorf("%s: status %v, hint %v, err %v; want NOT_LEADER pointing at %s", name, st, hint, err, leader.Addr())
		}
	}
	r, err := c.Read(ctx, &txnv1.ReadRequest{Txn: meta, Key: "a"})
	check("read", r.GetStatus(), r.GetLeader(), err)
	w, err := c.Write(ctx, &txnv1.WriteRequest{Txn: meta, Key: "a"})
	check("write", w.GetStatus(), w.GetLeader(), err)
	cm, err := c.Commit(ctx, &txnv1.CommitRequest{Txn: meta, Parts: []*txnv1.Part{{Shard: 0}}})
	check("commit", cm.GetStatus(), cm.GetLeader(), err)
	a, err := c.Abort(ctx, &txnv1.AbortRequest{Txn: 7})
	check("abort", a.GetStatus(), a.GetLeader(), err)
}

func waiters(n *Node, sh txn.ShardID) (reads, commits int) {
	s := n.shards[sh]
	_ = s.run(func() { reads, commits = len(s.reads), len(s.commits) })
	return
}

func noWaiters(t *testing.T, n *Node, when string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		r, cm := waiters(n, 0)
		if r == 0 && cm == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %d read and %d commit waiters, want none", when, r, cm)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestWaiters: a commit that waits for a lock is forgotten when its caller
// gives up, and one still waiting when the shard's leadership changes is
// answered at once with an unknown outcome.
func TestWaiters(t *testing.T) {
	nodes := startCluster(t, 3, 1)
	leader := leaderOf(t, nodes, 0)
	c := txnv1.NewTxnClient(dial(t, leader.Addr()))
	ctx := context.Background()
	old := &txnv1.TxnMeta{Id: 1, Ts: 1}
	young := &txnv1.TxnMeta{Id: 2, Ts: 2}
	// The old txn holds a shared lock on k; the young one's write waits.
	if r, err := c.Read(ctx, &txnv1.ReadRequest{Txn: old, Key: "k"}); err != nil || r.GetStatus() != txnv1.Status_STATUS_OK {
		t.Fatalf("read: %v %v", r, err)
	}
	if w, err := c.Write(ctx, &txnv1.WriteRequest{Txn: young, Key: "k"}); err != nil || w.GetStatus() != txnv1.Status_STATUS_OK {
		t.Fatalf("write: %v %v", w, err)
	}
	commit := &txnv1.CommitRequest{Txn: young, Parts: []*txnv1.Part{{Shard: 0, Writes: map[string]string{"k": "v"}}}}
	cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	_, err := c.Commit(cctx, commit)
	cancel()
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("blocked commit: %v, want DeadlineExceeded", err)
	}
	// The handler sees the cancellation shortly after the caller does.
	noWaiters(t, leader, "after the caller gave up")

	// Wait again, then make the leader see a higher ballot: it steps down
	// and the waiting commit learns its outcome is unknown.
	errc := make(chan error, 1)
	go func() {
		_, err := c.Commit(ctx, commit)
		errc <- err
	}()
	for {
		if _, cm := waiters(leader, 0); cm == 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	env, err := wire.ToProto(paxos.Message{From: 2, To: leader.ID(), Body: paxos.Heartbeat{Ballot: paxos.Ballot{Round: 1 << 40, Node: 99}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := paxosv1.NewPeerClient(dial(t, leader.Addr())).Send(ctx, env); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		if status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "outcome unknown") {
			t.Fatalf("commit after step-down: %v, want Unavailable with an unknown outcome", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiting commit was not answered after the leader stepped down")
	}
	noWaiters(t, leader, "after step-down")
}

func TestStoppedNode(t *testing.T) {
	nodes := startCluster(t, 1, 1)
	n := nodes[0]
	n.Stop()
	if n.Inspect(0, func(*paxos.Replica, *txn.SM, *txn.Server) {}) {
		t.Fatal("Inspect on a stopped node ran")
	}
	if err := n.shards[0].run(func() {}); err != errStopped {
		t.Fatalf("run on a stopped node: %v", err)
	}
}

func TestInDoubtWaitInTicks(t *testing.T) {
	cl := Cluster{Shards: 1, Nodes: []NodeConfig{{ID: 1, Addr: "127.0.0.1:0"}}}
	for _, tc := range []struct {
		wait, tick time.Duration
		ticks      int
	}{{0, 10 * time.Millisecond, 60}, {250 * time.Millisecond, 10 * time.Millisecond, 25}, {25 * time.Millisecond, 10 * time.Millisecond, 3}} {
		n, err := Start(Config{ID: 1, Cluster: cl, DataDir: t.TempDir(), Tick: tc.tick, InDoubtWait: tc.wait})
		if err != nil {
			t.Fatal(err)
		}
		got := n.cfg.Txn.QueryAfter
		n.Stop()
		if got != tc.ticks {
			t.Errorf("in-doubt wait %v at tick %v: QueryAfter %d ticks, want %d", tc.wait, tc.tick, got, tc.ticks)
		}
	}
}

func TestReady(t *testing.T) {
	nodes := startCluster(t, 3, 2)
	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := nodes[0].Ready(ctx)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	// A lone node of a 3-node cluster never learns a leader.
	lone := Cluster{Shards: 1, Nodes: []NodeConfig{{ID: 1, Addr: "127.0.0.1:0"}, {ID: 2, Addr: "127.0.0.1:1"}, {ID: 3, Addr: "127.0.0.1:2"}}}
	n, err := Start(Config{ID: 1, Cluster: lone, DataDir: t.TempDir(), Listen: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Stop()
	time.Sleep(100 * time.Millisecond)
	if err := n.Ready(context.Background()); err == nil || !strings.Contains(err.Error(), "no known leader") {
		t.Fatalf("lone node: %v, want no known leader", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = n.Ready(ctx) // an expired context does not hang
	n.Stop()
	if err := n.Ready(context.Background()); err == nil {
		t.Fatal("stopped node is ready")
	}
}

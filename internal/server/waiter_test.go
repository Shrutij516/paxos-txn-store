package server

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Shrutij516/paxos-txn-store/internal/kv"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	kvv1 "github.com/Shrutij516/paxos-txn-store/proto/kv/v1"
)

// waitersLen returns the size of the waiter table, read on the event loop.
func (n *Node) waitersLen() int {
	var size int
	ran := make(chan struct{})
	n.inspect <- func() { size = len(n.waiters); close(ran) }
	<-ran
	return size
}

// A leader that has lost its followers accepts requests but can never
// commit them. Every handler times out (or its caller cancels), and the
// waiter table must be empty again afterwards.
func TestWaitersRemovedAfterTimeoutAndCancel(t *testing.T) {
	peers := map[paxos.NodeID]string{}
	lis := map[paxos.NodeID]net.Listener{}
	for id := paxos.NodeID(1); id <= 3; id++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lis[id], peers[id] = l, l.Addr().String()
	}
	nodes := map[paxos.NodeID]*Node{}
	for id := paxos.NodeID(1); id <= 3; id++ {
		n, err := Start(Config{ID: id, Peers: peers, Listener: lis[id], DataDir: t.TempDir(),
			Tick: 5 * time.Millisecond, ReqTimeout: 150 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = n
	}
	var leader *Node
	for deadline := time.Now().Add(10 * time.Second); leader == nil; {
		for _, n := range nodes {
			var is bool
			n.Inspect(func(r *paxos.Replica, _ *kv.Store) { is = r.IsLeader() })
			if is {
				leader = n
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no leader")
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer leader.Stop()
	for id, n := range nodes {
		if n != leader {
			n.Stop()
			delete(nodes, id)
		}
	}

	conn, err := grpc.NewClient(leader.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	api := kvv1.NewKVClient(conn)

	// Server-side timeouts (ReqTimeout = 150ms).
	for c := uint64(1); c <= 5; c++ {
		if _, err := api.Put(context.Background(), &kvv1.PutRequest{ClientId: c, Seq: 1, Key: "k", Value: "v"}); err == nil {
			t.Fatal("a leader without followers must not commit")
		}
	}
	// Caller-side deadlines shorter than ReqTimeout.
	for c := uint64(6); c <= 10; c++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		_, _ = api.Get(ctx, &kvv1.GetRequest{ClientId: c, Seq: 1, Key: "k"})
		cancel()
	}
	deadline := time.Now().Add(5 * time.Second)
	for leader.waitersLen() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("waiter table still holds %d entries", leader.waitersLen())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

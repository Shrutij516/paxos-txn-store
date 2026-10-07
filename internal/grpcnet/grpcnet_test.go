package grpcnet

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	paxosv1 "github.com/Shrutij516/paxos-txn-store/proto/paxos/v1"
)

type sink struct {
	paxosv1.UnimplementedPeerServer
	mu  sync.Mutex
	got []*paxosv1.Envelope
}

func (s *sink) Send(_ context.Context, e *paxosv1.Envelope) (*paxosv1.SendAck, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, e)
	return &paxosv1.SendAck{}, nil
}

func (s *sink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.got)
}

func TestTransport(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	sk := &sink{}
	paxosv1.RegisterPeerServer(srv, sk)
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	var local []paxos.Message
	var localShards []uint32
	addrs := map[paxos.NodeID]string{1: "unused", 2: lis.Addr().String(), 3: "127.0.0.1:1"}
	tr, err := New(1, addrs, func(sh uint32, m paxos.Message) {
		local = append(local, m)
		localShards = append(localShards, sh)
	}, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	hb := paxos.Heartbeat{Ballot: paxos.Ballot{Round: 3, Node: 1}, Commit: 7}
	tr.SendTraced(2, paxos.Message{From: 1, To: 2, Body: hb}, map[string]string{"traceparent": "x"})
	tr.Send(1, paxos.Message{From: 1, To: 1, Body: hb})  // self: local
	tr.Send(4, paxos.Message{From: 1, To: -1, Body: hb}) // not a peer: local
	// A dead peer must never block Send, even past its queue size.
	start := time.Now()
	for i := 0; i < 3*peerQueue; i++ {
		tr.Send(0, paxos.Message{From: 1, To: 3, Body: hb})
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Send to a dead peer blocked for %v", d)
	}
	deadline := time.Now().Add(5 * time.Second)
	for sk.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if sk.count() != 1 || sk.got[0].GetHeartbeat().GetCommit() != 7 || sk.got[0].GetTo() != 2 || sk.got[0].GetShard() != 2 || sk.got[0].GetTrace()["traceparent"] != "x" {
		t.Fatalf("peer received %v", sk.got)
	}
	if len(local) != 2 || local[0].To != 1 || local[1].To != -1 || localShards[0] != 1 || localShards[1] != 4 {
		t.Fatalf("local deliveries %v on shards %v", local, localShards)
	}
	// Unconvertible bodies are dropped, not sent.
	type bogus struct{ paxos.Payload }
	tr.Send(0, paxos.Message{From: 1, To: 2, Body: bogus{}})
	done := make(chan struct{})
	go func() { tr.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Close hung")
	}
}

// Package grpcnet carries Paxos and transaction messages between nodes over
// gRPC (the Peer service in proto/paxos/v1).
package grpcnet

import (
	"context"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/wire"
	paxosv1 "github.com/Shrutij516/paxos-txn-store/proto/paxos/v1"
)

// peerQueue is the number of outbound messages buffered per peer. When a
// peer is slow or down its queue fills and new messages are dropped, which
// Paxos tolerates; the caller never blocks.
const peerQueue = 1024

// Transport carries messages between nodes over gRPC. A node hosts one
// replica of every shard, and Send tags each message with the shard it is
// for. Send never blocks: messages for a peer go into that peer's queue
// (shared by all shards) and a per-peer goroutine delivers them with one
// unary RPC each, bounded by a deadline. Messages addressed to this node
// itself, or to any ID that is not a configured peer (such as a local
// client endpoint), are handed to the local callback instead.
type Transport struct {
	self    paxos.NodeID
	local   func(shard uint32, m paxos.Message)
	timeout time.Duration
	peers   map[paxos.NodeID]*peer
	wg      sync.WaitGroup
	ctx     context.Context // canceled by Close so queued sends fail fast
	cancel  context.CancelFunc
}

type peer struct {
	conn   *grpc.ClientConn
	client paxosv1.PeerClient
	q      chan *paxosv1.Envelope
}

// New connects (lazily) to every peer in addrs other than self. local
// receives messages for self and for non-peer IDs; it is called from Send,
// on the caller's goroutine. timeout bounds each RPC.
func New(self paxos.NodeID, addrs map[paxos.NodeID]string, local func(shard uint32, m paxos.Message), timeout time.Duration) (*Transport, error) {
	t := &Transport{self: self, local: local, timeout: timeout, peers: make(map[paxos.NodeID]*peer)}
	t.ctx, t.cancel = context.WithCancel(context.Background())
	for id, addr := range addrs {
		if id == self {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Close()
			return nil, err
		}
		p := &peer{conn: conn, client: paxosv1.NewPeerClient(conn), q: make(chan *paxosv1.Envelope, peerQueue)}
		t.peers[id] = p
		t.wg.Add(1)
		go t.deliver(p)
	}
	return t, nil
}

// Send delivers m to node m.To's replica of shard sh.
func (t *Transport) Send(sh uint32, m paxos.Message) {
	p, ok := t.peers[m.To]
	if !ok {
		t.local(sh, m)
		return
	}
	env, err := wire.ToProto(m)
	if err != nil {
		return
	}
	env.Shard = sh
	select {
	case p.q <- env:
	default: // queue full: drop, Paxos will retry
	}
}

func (t *Transport) deliver(p *peer) {
	defer t.wg.Done()
	for env := range p.q {
		ctx, cancel := context.WithTimeout(t.ctx, t.timeout)
		_, _ = p.client.Send(ctx, env) // errors are message loss
		cancel()
	}
}

// Close stops the delivery goroutines, dropping anything still queued, and
// closes the connections. Send must not be called after Close.
func (t *Transport) Close() {
	t.cancel()
	for _, p := range t.peers {
		close(p.q)
	}
	t.wg.Wait()
	for _, p := range t.peers {
		_ = p.conn.Close()
	}
}

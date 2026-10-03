// Package server runs one Multi-Paxos replica as a network node: a gRPC
// server for peers and clients, a gRPC transport to the other nodes, SQLite
// storage, and a single event-loop goroutine that owns the replica.
//
// Every access to the replica and its key-value store happens on the event
// loop. gRPC handlers and the ticker only send events to it over channels,
// so the Paxos code needs no locks.
package server

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Shrutij516/paxos-txn-store/internal/kv"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/storage"
	"github.com/Shrutij516/paxos-txn-store/internal/transport"
	"github.com/Shrutij516/paxos-txn-store/internal/wire"
	kvv1 "github.com/Shrutij516/paxos-txn-store/proto/kv/v1"
	paxosv1 "github.com/Shrutij516/paxos-txn-store/proto/paxos/v1"
)

// clientEndpoint is the NodeID the replica sees as the sender of client
// requests that arrive at this node. Replies addressed to it come back to
// the waiting gRPC handler. It is never a real node ID.
const clientEndpoint paxos.NodeID = -1

// Defaults for Config fields left zero.
const (
	DefaultTick       = 10 * time.Millisecond
	DefaultRPCTimeout = 200 * time.Millisecond
	DefaultReqTimeout = 2 * time.Second
	inboxSize         = 4096
)

// Config describes one node.
type Config struct {
	ID    paxos.NodeID
	Peers map[paxos.NodeID]string // every node's address, including this one
	// Listen is the address to listen on. Empty means Peers[ID].
	Listen string
	// Listener, if set, is used instead of listening on Listen.
	Listener net.Listener
	// DataDir holds the SQLite file node-<ID>.db.
	DataDir string
	// Tick is the wall-clock length of one logical Paxos tick.
	Tick time.Duration
	// RPCTimeout bounds each peer RPC.
	RPCTimeout time.Duration
	// ReqTimeout bounds how long a client request waits for its commit.
	ReqTimeout time.Duration
	// Timing overrides the replica's timing in ticks; zero means the default.
	Timing paxos.LogTiming
	// ServerOptions are extra gRPC server options (tests add interceptors).
	ServerOptions []grpc.ServerOption
}

type waiter struct {
	seq uint64
	ch  chan paxos.ClientReply
}

type submission struct {
	req paxos.ClientRequest
	ch  chan paxos.ClientReply
}

// Node is a running replica.
type Node struct {
	cfg  Config
	db   *storage.SQLite
	kv   *kv.Store
	rep  *paxos.Replica
	tr   *transport.GRPC
	srv  *grpc.Server
	lis  net.Listener
	addr string

	inbox   chan paxos.Message
	submit  chan submission
	abandon chan submission // a handler gave up waiting; drop its waiter
	inspect chan func()
	quit    chan struct{}
	done    chan struct{}

	// Owned by the event loop goroutine.
	selfQ   []paxos.Message
	waiters map[uint64]waiter
}

// Start opens storage, recovers the replica, starts the event loop and
// begins serving.
func Start(cfg Config) (*Node, error) {
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("server: node %d missing from peers", cfg.ID)
	}
	if cfg.Tick <= 0 {
		cfg.Tick = DefaultTick
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = DefaultRPCTimeout
	}
	if cfg.ReqTimeout <= 0 {
		cfg.ReqTimeout = DefaultReqTimeout
	}
	n := &Node{
		cfg:     cfg,
		inbox:   make(chan paxos.Message, inboxSize),
		submit:  make(chan submission),
		abandon: make(chan submission),
		inspect: make(chan func()),
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
		waiters: make(map[uint64]waiter),
	}
	db, err := storage.OpenSQLite(filepath.Join(cfg.DataDir, fmt.Sprintf("node-%d.db", cfg.ID)))
	if err != nil {
		return nil, err
	}
	n.db = db
	n.tr, err = transport.NewGRPC(cfg.ID, cfg.Peers, n.local, cfg.RPCTimeout)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	ids := make([]paxos.NodeID, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		ids = append(ids, id)
	}
	n.kv = kv.New()
	n.rep, err = paxos.NewReplica(paxos.ReplicaConfig{
		ID: cfg.ID, Peers: ids, Rand: rand.New(rand.NewPCG(seed(), seed())),
		Timing: cfg.Timing, StateMachine: n.kv,
	}, db, n.tr)
	if err != nil {
		n.tr.Close()
		_ = db.Close()
		return nil, err
	}
	n.lis = cfg.Listener
	if n.lis == nil {
		listen := cfg.Listen
		if listen == "" {
			listen = cfg.Peers[cfg.ID]
		}
		if n.lis, err = net.Listen("tcp", listen); err != nil {
			n.tr.Close()
			_ = db.Close()
			return nil, err
		}
	}
	n.addr = n.lis.Addr().String()
	n.srv = grpc.NewServer(cfg.ServerOptions...)
	paxosv1.RegisterPeerServer(n.srv, peerService{n: n})
	kvv1.RegisterKVServer(n.srv, kvService{n: n})
	go n.loop()
	go func() { _ = n.srv.Serve(n.lis) }()
	return n, nil
}

func seed() uint64 {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	return binary.LittleEndian.Uint64(b[:])
}

// Addr returns the address the node is serving on.
func (n *Node) Addr() string { return n.addr }

// ID returns the node's ID.
func (n *Node) ID() paxos.NodeID { return n.cfg.ID }

// Stop shuts the node down: it stops accepting RPCs (waiting briefly for
// in-flight ones), stops the event loop, and closes the transport and the
// database. Safe to call once.
func (n *Node) Stop() {
	stopped := make(chan struct{})
	go func() { n.srv.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		n.srv.Stop()
		<-stopped
	}
	close(n.quit)
	<-n.done
	n.tr.Close()
	_ = n.db.Close()
}

// Inspect runs f on the event loop with exclusive access to the replica and
// the key-value store. It returns false if the node has stopped.
func (n *Node) Inspect(f func(r *paxos.Replica, s *kv.Store)) bool {
	ran := make(chan struct{})
	select {
	case n.inspect <- func() { f(n.rep, n.kv); close(ran) }:
		<-ran
		return true
	case <-n.done:
		return false
	}
}

// loop is the only goroutine that touches the replica.
func (n *Node) loop() {
	defer close(n.done)
	tick := time.NewTicker(n.cfg.Tick)
	defer tick.Stop()
	for {
		select {
		case <-n.quit:
			return
		case <-tick.C:
			n.rep.Tick()
		case m := <-n.inbox:
			n.rep.Handle(m)
		case s := <-n.submit:
			n.waiters[s.req.ClientID] = waiter{seq: s.req.Seq, ch: s.ch}
			n.rep.Handle(paxos.Message{From: clientEndpoint, To: n.cfg.ID, Body: s.req})
		case s := <-n.abandon:
			// Only remove the waiter if it is still this handler's; a retry
			// may already have replaced it.
			if w, ok := n.waiters[s.req.ClientID]; ok && w.ch == s.ch {
				delete(n.waiters, s.req.ClientID)
			}
		case f := <-n.inspect:
			f()
		}
		// Deliver messages the replica sent to itself, in order.
		for len(n.selfQ) > 0 {
			m := n.selfQ[0]
			n.selfQ = n.selfQ[1:]
			n.rep.Handle(m)
		}
	}
}

// local receives messages the transport does not send over the network.
// It runs on the event loop (inside a replica call), so it only queues.
func (n *Node) local(m paxos.Message) {
	if m.To == n.cfg.ID {
		n.selfQ = append(n.selfQ, m)
		return
	}
	if m.To != clientEndpoint {
		return
	}
	r, ok := m.Body.(paxos.ClientReply)
	if !ok {
		return
	}
	w, ok := n.waiters[r.ClientID]
	if !ok || w.seq != r.Seq {
		return
	}
	delete(n.waiters, r.ClientID)
	w.ch <- r // buffered, never blocks
}

// do submits a client request to the event loop and waits for the reply.
func (n *Node) do(ctx context.Context, req paxos.ClientRequest) (paxos.ClientReply, error) {
	ctx, cancel := context.WithTimeout(ctx, n.cfg.ReqTimeout)
	defer cancel()
	ch := make(chan paxos.ClientReply, 1)
	select {
	case n.submit <- submission{req: req, ch: ch}:
	case <-ctx.Done():
		return paxos.ClientReply{}, status.FromContextError(ctx.Err()).Err()
	case <-n.done:
		return paxos.ClientReply{}, status.Error(codes.Unavailable, "node stopping")
	}
	select {
	case r := <-ch:
		return r, nil
	case <-ctx.Done():
		// Timed out or cancelled: tell the loop to forget this waiter now,
		// so the table only holds requests someone is still waiting for.
		select {
		case n.abandon <- submission{req: req, ch: ch}:
		case <-n.done:
		}
		return paxos.ClientReply{}, status.FromContextError(ctx.Err()).Err()
	case <-n.done:
		return paxos.ClientReply{}, status.Error(codes.Unavailable, "node stopping")
	}
}

func (n *Node) leaderAddr(id paxos.NodeID) string { return n.cfg.Peers[id] }

// ---- gRPC services ----

type peerService struct {
	paxosv1.UnimplementedPeerServer
	n *Node
}

func (s peerService) Send(_ context.Context, env *paxosv1.Envelope) (*paxosv1.SendAck, error) {
	m, err := wire.FromProto(env)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if m.To != s.n.cfg.ID {
		return nil, status.Error(codes.InvalidArgument, "message for another node")
	}
	select {
	case s.n.inbox <- m:
	default: // overloaded: drop, Paxos retries
	}
	return &paxosv1.SendAck{}, nil
}

type kvService struct {
	kvv1.UnimplementedKVServer
	n *Node
}

var errNoClientID = status.Error(codes.InvalidArgument, "client_id and seq must be non-zero")

func (s kvService) Get(ctx context.Context, r *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	if r.GetClientId() == 0 || r.GetSeq() == 0 {
		return nil, errNoClientID
	}
	rep, err := s.n.do(ctx, paxos.ClientRequest{ClientID: r.GetClientId(), Seq: r.GetSeq(), Cmd: kv.Get(r.GetKey())})
	if err != nil {
		return nil, err
	}
	return &kvv1.GetResponse{Ok: rep.OK, Value: string(rep.Result), LeaderId: int32(rep.Leader), LeaderAddr: s.n.leaderAddr(rep.Leader)}, nil
}

func (s kvService) Put(ctx context.Context, r *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	if r.GetClientId() == 0 || r.GetSeq() == 0 {
		return nil, errNoClientID
	}
	rep, err := s.n.do(ctx, paxos.ClientRequest{ClientID: r.GetClientId(), Seq: r.GetSeq(), Cmd: kv.Put(r.GetKey(), r.GetValue())})
	if err != nil {
		return nil, err
	}
	return &kvv1.PutResponse{Ok: rep.OK, LeaderId: int32(rep.Leader), LeaderAddr: s.n.leaderAddr(rep.Leader)}, nil
}

// ParsePeers parses "1=host:port,2=host:port" into a peer map.
func ParsePeers(s string) (map[paxos.NodeID]string, error) {
	out := make(map[paxos.NodeID]string)
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		idStr, addr, ok := strings.Cut(part, "=")
		if !ok || addr == "" {
			return nil, fmt.Errorf("server: bad peer %q, want id=host:port", part)
		}
		id, err := strconv.ParseInt(idStr, 10, 32)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("server: bad peer id in %q", part)
		}
		if _, dup := out[paxos.NodeID(id)]; dup {
			return nil, fmt.Errorf("server: duplicate peer id %d", id)
		}
		out[paxos.NodeID(id)] = addr
	}
	if len(out) == 0 {
		return nil, errors.New("server: no peers")
	}
	return out, nil
}

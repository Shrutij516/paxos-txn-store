// Package server runs one node of the sharded transaction store: one
// Multi-Paxos replica of every shard, each with its own SQLite file, its
// transaction layer (internal/txn) and its own event-loop goroutine; a
// gRPC server for peers and clients; and a gRPC transport to the other
// nodes.
//
// Every access to a shard's replica, state machine and transaction server
// happens on that shard's event loop. gRPC handlers, the ticker and other
// shards only send events to it over channels, so the Paxos and
// transaction code needs no locks.
package server

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"

	"github.com/Shrutij516/paxos-txn-store/internal/grpcnet"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/storage"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
	paxosv1 "github.com/Shrutij516/paxos-txn-store/proto/paxos/v1"
	txnv1 "github.com/Shrutij516/paxos-txn-store/proto/txn/v1"
)

// clientEndpoint is the NodeID the transaction servers see as the sender of
// client requests that arrive at this node. Replies addressed to it come
// back to the waiting gRPC handler. It is never a real node ID, and differs
// from the negative IDs a transaction server uses for its own proposals.
const clientEndpoint paxos.NodeID = math.MinInt32

// Defaults for Config fields left zero.
const (
	DefaultTick       = 10 * time.Millisecond
	DefaultRPCTimeout = 200 * time.Millisecond
	DefaultReqTimeout = 2 * time.Second
	// DefaultInDoubtWait is how long a prepared participant waits for its
	// coordinator's decision before asking for it.
	DefaultInDoubtWait = 600 * time.Millisecond
	inboxSize          = 4096
)

// Config describes one node.
type Config struct {
	ID      paxos.NodeID
	Cluster Cluster // shard count and every node's address, including this one
	// Listen is the address to listen on. Empty means this node's address
	// in Cluster.
	Listen string
	// Listener, if set, is used instead of listening on Listen.
	Listener net.Listener
	// DataDir holds one SQLite file per shard, node-<ID>-shard-<S>.db.
	DataDir string
	// Tick is the wall-clock length of one logical tick.
	Tick time.Duration
	// RPCTimeout bounds each peer RPC.
	RPCTimeout time.Duration
	// ReqTimeout bounds how long a client request waits on the server.
	ReqTimeout time.Duration
	// Timing overrides the replicas' timing in ticks; zero means the default.
	Timing paxos.LogTiming
	// InDoubtWait is how long a shard leader holding a prepared
	// transaction waits for the coordinator's decision before it asks the
	// coordinator shard for the outcome (txn.Config.QueryAfter, rounded up
	// to whole ticks). Until then the transaction keeps its locks, so after
	// a coordinator failure it bounds how long conflicting transactions
	// wait. Zero means DefaultInDoubtWait. Ignored if Txn.QueryAfter is set.
	InDoubtWait time.Duration
	// Txn overrides the transaction layer's timeouts in ticks; zero fields
	// take the defaults. Shard and ShardNodes are filled in per shard.
	Txn txn.Config
	// ServerOptions are extra gRPC server options (tests add interceptors).
	ServerOptions []grpc.ServerOption

	// TracerProvider produces the node's spans: gRPC server spans for the
	// Txn service, two-phase commit phases and Paxos rounds. Nil means no
	// tracing.
	TracerProvider trace.TracerProvider
	// Registry receives the node's metrics. Nil means a new registry;
	// Node.Registry returns it either way.
	Registry *prometheus.Registry
	// Logger receives the node's structured logs. Nil means none.
	Logger *slog.Logger
}

// Node is a running node.
type Node struct {
	cfg    Config
	addrs  map[paxos.NodeID]string
	shards []*shard
	tr     *grpcnet.Transport
	srv    *grpc.Server
	lis    net.Listener
	addr   string
	stop   sync.Once

	tp      trace.TracerProvider
	tracing bool // TracerProvider was given
	reg     *prometheus.Registry
	metrics *metrics
	log     *slog.Logger
}

// Registry returns the registry holding the node's metrics.
func (n *Node) Registry() *prometheus.Registry { return n.reg }

// DBPath returns the SQLite file of a node's replica of a shard.
func DBPath(dataDir string, id paxos.NodeID, sh txn.ShardID) string {
	return filepath.Join(dataDir, fmt.Sprintf("node-%d-shard-%d.db", id, sh))
}

// Start opens storage, recovers every shard's replica, starts the event
// loops and begins serving.
func Start(cfg Config) (*Node, error) {
	if err := cfg.Cluster.Validate(); err != nil {
		return nil, err
	}
	addrs := cfg.Cluster.Addrs()
	if _, ok := addrs[cfg.ID]; !ok {
		return nil, fmt.Errorf("server: node %d missing from the cluster config", cfg.ID)
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
	if cfg.InDoubtWait <= 0 {
		cfg.InDoubtWait = DefaultInDoubtWait
	}
	if cfg.Txn.QueryAfter <= 0 {
		cfg.Txn.QueryAfter = int((cfg.InDoubtWait + cfg.Tick - 1) / cfg.Tick)
	}
	n := &Node{cfg: cfg, addrs: addrs, tp: cfg.TracerProvider, tracing: cfg.TracerProvider != nil, reg: cfg.Registry, log: cfg.Logger}
	if n.tp == nil {
		n.tp = noop.NewTracerProvider()
	}
	if n.reg == nil {
		n.reg = prometheus.NewRegistry()
	}
	if n.log == nil {
		n.log = slog.New(slog.DiscardHandler)
	}
	n.log = n.log.With("node", int(cfg.ID))
	var err error
	if n.metrics, err = registerMetrics(n.reg); err != nil {
		return nil, err
	}
	// The servers never address a non-peer ID through the transport (they
	// deliver locally first), so its local callback has nothing to do.
	n.tr, err = grpcnet.New(cfg.ID, addrs, func(uint32, paxos.Message) {}, cfg.RPCTimeout)
	if err != nil {
		return nil, err
	}
	ids := cfg.Cluster.IDs()
	for sh := txn.ShardID(0); int(sh) < cfg.Cluster.Shards; sh++ {
		s, err := n.openShard(sh, ids)
		if err != nil {
			n.closeAll()
			return nil, err
		}
		n.shards = append(n.shards, s)
	}
	n.lis = cfg.Listener
	if n.lis == nil {
		listen := cfg.Listen
		if listen == "" {
			listen = addrs[cfg.ID]
		}
		if n.lis, err = net.Listen("tcp", listen); err != nil {
			n.closeAll()
			return nil, err
		}
	}
	n.addr = n.lis.Addr().String()
	n.srv = grpc.NewServer(append(n.serverOptions(), cfg.ServerOptions...)...)
	paxosv1.RegisterPeerServer(n.srv, peerService{n: n})
	txnv1.RegisterTxnServer(n.srv, txnService{n: n})
	for _, s := range n.shards {
		go s.loop()
	}
	go func() { _ = n.srv.Serve(n.lis) }()
	n.log.Info("serving", "addr", n.addr, "shards", len(n.shards), "tick", cfg.Tick.String())
	return n, nil
}

func (n *Node) openShard(sh txn.ShardID, ids []paxos.NodeID) (*shard, error) {
	db, err := storage.OpenSQLite(DBPath(n.cfg.DataDir, n.cfg.ID, sh))
	if err != nil {
		return nil, err
	}
	s := &shard{
		n: n, id: sh, db: db, log: n.log.With("shard", int(sh)),
		inbox:   make(chan inMsg, inboxSize),
		calls:   make(chan func()),
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
		reads:   make(map[readKey]chan readResult),
		commits: make(map[txn.ID]chan commitResult),
	}
	m := n.metrics.forShard(sh, n.cfg.ID)
	s.tel = newTelemetry(s, m, n.tp)
	s.sm = txn.NewSM(nil)
	s.rep, err = paxos.NewReplica(paxos.ReplicaConfig{
		ID: n.cfg.ID, Peers: ids, Rand: rand.New(rand.NewPCG(seed(), seed())),
		Timing: n.cfg.Timing, StateMachine: s.sm, Observer: s.tel,
	}, timedLog{LogStorage: db, h: m.fsync}, shardSender{s})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	tc := n.cfg.Txn
	tc.Shard = sh
	tc.ShardNodes = func(txn.ShardID) []paxos.NodeID { return ids }
	tc.Observer = s.tel
	if err := n.metrics.queueDepth(sh, n.cfg.ID, func() float64 {
		return float64(len(s.inbox)) + float64(s.waiting.Load())
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	s.ts = txn.NewServer(tc, n.cfg.ID, s.rep, s.sm, s.send)
	s.ts.After() // drop the events from replaying the log
	return s, nil
}

func seed() uint64 {
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	return binary.LittleEndian.Uint64(b[:])
}

// closeAll releases what Start opened before the loops run.
func (n *Node) closeAll() {
	n.tr.Close()
	for _, s := range n.shards {
		_ = s.db.Close()
	}
}

// Addr returns the address the node is serving on.
func (n *Node) Addr() string { return n.addr }

// ID returns the node's ID.
func (n *Node) ID() paxos.NodeID { return n.cfg.ID }

// Shards returns the number of shards.
func (n *Node) Shards() int { return len(n.shards) }

// Stop shuts the node down: it stops accepting RPCs (waiting briefly for
// in-flight ones), stops the event loops, and closes the transport and the
// databases. Calls after the first do nothing.
func (n *Node) Stop() { n.stop.Do(n.shutdown) }

func (n *Node) shutdown() {
	n.log.Info("stopping")
	stopped := make(chan struct{})
	go func() { n.srv.GracefulStop(); close(stopped) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		n.srv.Stop()
		<-stopped
	}
	for _, s := range n.shards {
		close(s.quit)
	}
	for _, s := range n.shards {
		<-s.done
	}
	n.tr.Close()
	for _, s := range n.shards {
		_ = s.db.Close()
	}
}

// Inspect runs f on shard sh's event loop with exclusive access to its
// replica, state machine and transaction server. It returns false if the
// node has stopped.
func (n *Node) Inspect(sh txn.ShardID, f func(r *paxos.Replica, sm *txn.SM, ts *txn.Server)) bool {
	s := n.shards[sh]
	return s.run(func() { f(s.rep, s.sm, s.ts) }) == nil
}

// Ready reports whether every shard replica on this node knows its shard's
// leader (or is it) and has applied everything it knows to be committed.
// It returns an error naming the first shard that is not, or ctx's error if
// a shard's event loop did not answer in time.
func (n *Node) Ready(ctx context.Context) error {
	for _, s := range n.shards {
		type state struct {
			leader paxos.NodeID
			lag    uint64
		}
		ch := make(chan state, 1)
		go func() {
			_ = s.run(func() { ch <- state{s.rep.Leader(), s.rep.Lag()} })
		}()
		select {
		case st := <-ch:
			switch {
			case st.leader == 0:
				return fmt.Errorf("shard %d: no known leader", s.id)
			case st.lag > 0:
				return fmt.Errorf("shard %d: %d committed slots not applied yet", s.id, st.lag)
			}
		case <-ctx.Done():
			return fmt.Errorf("shard %d: event loop did not answer: %w", s.id, ctx.Err())
		case <-s.done:
			return fmt.Errorf("shard %d: %w", s.id, errStopped)
		}
	}
	return nil
}

// errStopped means the shard's event loop has exited.
var errStopped = errors.New("server: node stopping")

// ---- One shard ----

type readKey struct {
	txn txn.ID
	key string
}

type readResult struct {
	resp      txn.ReadResp
	notLeader bool // leadership changed while the read waited
}

type commitResult struct {
	resp txn.CommitResp
	lost bool // leadership changed while the commit waited: outcome unknown
}

// shard is this node's replica of one shard.
type shard struct {
	n   *Node
	id  txn.ShardID
	db  *storage.SQLite
	sm  *txn.SM
	rep *paxos.Replica
	ts  *txn.Server
	tel *telemetry
	log *slog.Logger

	inbox chan inMsg  // from peers and from other shards on this node
	calls chan func() // client requests and Inspect
	// waiting counts callers blocked handing a function to calls, for the
	// queue depth gauge.
	waiting atomic.Int64
	quit    chan struct{}
	done    chan struct{}

	// Owned by the event loop goroutine.
	selfQ   []paxos.Message
	reads   map[readKey]chan readResult
	commits map[txn.ID]chan commitResult
	leading bool
	ballot  paxos.Ballot
}

// inMsg is a message for a shard, with the trace context it carried.
type inMsg struct {
	m     paxos.Message
	trace map[string]string
}

// shardSender is the replica's transport: it sends within the shard.
type shardSender struct{ s *shard }

func (x shardSender) Send(m paxos.Message) { x.s.send(x.s.id, m) }

// send delivers m to node m.To's replica of shard sh. It runs on this
// shard's event loop (inside a replica or transaction server call), so
// it never blocks: a message for this replica is queued and handled after
// the current call, one for another shard on this node goes to that
// shard's inbox (dropped if full, like a lost network message), and a
// reply to a client goes to the waiting handler.
func (s *shard) send(sh txn.ShardID, m paxos.Message) {
	switch {
	case m.To == clientEndpoint:
		s.reply(m)
	case m.To < 0:
		// Replies to a transaction server's own proposals: unused.
	case m.To != s.n.cfg.ID:
		s.n.tr.SendTraced(uint32(sh), m, s.carrier(m))
	case sh == s.id:
		s.selfQ = append(s.selfQ, m)
	case int(sh) < len(s.n.shards):
		select {
		case s.n.shards[sh].inbox <- inMsg{m: m, trace: s.carrier(m)}:
		default:
		}
	}
}

// carrier is the trace context to send with m, if tracing is on.
func (s *shard) carrier(m paxos.Message) map[string]string {
	if !s.n.tracing {
		return nil
	}
	return s.tel.inject(m)
}

// reply hands a transaction server's answer to the waiting client handler.
func (s *shard) reply(m paxos.Message) {
	ext, ok := m.Body.(paxos.Ext)
	if !ok {
		return
	}
	switch r := ext.Body.(type) {
	case txn.ReadResp:
		k := readKey{r.Txn, r.Key}
		if ch, ok := s.reads[k]; ok {
			delete(s.reads, k)
			ch <- readResult{resp: r} // buffered, never blocks
		}
	case txn.CommitResp:
		if ch, ok := s.commits[r.Txn]; ok {
			delete(s.commits, r.Txn)
			ch <- commitResult{resp: r}
		}
	}
}

// loop is the only goroutine that touches the shard's replica, state
// machine and transaction server.
func (s *shard) loop() {
	defer close(s.done)
	tick := time.NewTicker(s.n.cfg.Tick)
	defer tick.Stop()
	for {
		select {
		case <-s.quit:
			return
		case <-tick.C:
			s.rep.Tick()
			s.ts.Tick()
			s.tel.tick()
		case in := <-s.inbox:
			if s.n.tracing {
				s.tel.extract(in.m, in.trace)
			}
			s.dispatch(in.m)
		case f := <-s.calls:
			f()
		}
		s.settle()
	}
}

func (s *shard) dispatch(m paxos.Message) {
	if _, ok := m.Body.(paxos.Ext); ok {
		s.ts.Handle(m)
	} else {
		s.rep.Handle(m)
	}
}

// settle runs after every event: it lets the transaction server react,
// delivers messages the replica sent to itself, and fails the waiting
// client requests if leadership changed, since the new term's server has
// forgotten them.
func (s *shard) settle() {
	s.ts.After()
	for len(s.selfQ) > 0 {
		m := s.selfQ[0]
		s.selfQ = s.selfQ[1:]
		s.dispatch(m)
		s.ts.After()
	}
	leading, ballot := s.ts.Leading(), s.rep.Ballot()
	if leading == s.leading && ballot == s.ballot {
		return
	}
	if leading != s.leading {
		s.tel.leadership(leading)
	}
	s.leading, s.ballot = leading, ballot
	for k, ch := range s.reads {
		ch <- readResult{notLeader: true}
		delete(s.reads, k)
	}
	for id, ch := range s.commits {
		ch <- commitResult{lost: true}
		delete(s.commits, id)
	}
}

// run executes f on the event loop and waits for it.
func (s *shard) run(f func()) error {
	ran := make(chan struct{})
	s.waiting.Add(1)
	select {
	case s.calls <- func() { f(); close(ran) }:
		s.waiting.Add(-1) // handed over; the loop is running it
		<-ran
		return nil
	case <-s.done:
		s.waiting.Add(-1)
		return errStopped
	}
}

// leaderHint names the leader this replica believes in.
func (s *shard) leaderHint() *txnv1.LeaderHint {
	id := s.rep.Leader()
	return &txnv1.LeaderHint{Id: int32(id), Addr: s.n.addrs[id]}
}

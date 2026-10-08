package txn

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/storage"
	"github.com/Shrutij516/paxos-txn-store/internal/transport"
)

const (
	numAccounts    = 9
	initialBalance = 100
	clientBase     = 100
	readTimeout    = 30
	commitTimeout  = 40
	maxReadTries   = 6
)

func account(i int) string { return fmt.Sprintf("acct%d", i) }

// numSeeds returns n, or 100 under -short.
func numSeeds(n uint64) uint64 {
	if testing.Short() {
		return min(n, 100)
	}
	return n
}

type chaos struct {
	CrashProb, RestartProb, PartitionProb, HealProb float64
	// Gap-heavy mode, as in the Multi-Paxos tests (internal/paxos): during
	// chaos each LogAccept is dropped with probability AcceptDrop, and a
	// leader that sends the accepts for a one-phase, prepare or decide
	// record crashes 1 to 3 steps later with probability RecordCrash. New
	// leaders then often recover such records from promises and need
	// several retransmissions to commit them, while clients retry.
	AcceptDrop, RecordCrash float64
}

type worldOpts struct {
	Shards       int
	Clients      int
	OpsPerClient int
	AuditPercent int
	Faults       transport.Faults
	Chaos        chaos
	Server       Config // timing and broken modes; Shard/ShardNodes filled in
	Observe      bool   // attach a recording Observer to every server
	// NoCheckQuorum turns off CheckQuorum on every replica (see
	// paxos.ReplicaConfig.NoCheckQuorum).
	NoCheckQuorum bool
}

type node struct {
	id    paxos.NodeID
	shard ShardID
	rep   *paxos.Replica
	sm    *SM
	srv   *Server
}

// world is a sharded cluster plus transaction clients on one SimNet,
// driven by one seed on one goroutine.
type world struct {
	seed   uint64
	opts   worldOpts
	rng    *rand.Rand
	net    *transport.SimNet
	ids    []paxos.NodeID
	shard  map[paxos.NodeID]ShardID
	nodes  map[paxos.NodeID]*node // nil while crashed
	stores map[paxos.NodeID]*storage.Memory
	cli    []*client
	h      hash.Hash

	allServers []*Server // every incarnation, for stats
	observers  []*recObserver

	chaosOn bool                 // gap-heavy faults apply only during chaos
	crashIn map[paxos.NodeID]int // steps until a scheduled gap-heavy crash
	gapRng  *rand.Rand
	// recordCrashes counts gap-heavy crashes scheduled after a record.
	recordCrashes int

	onDecide   func(n *node, id ID)
	onPrepared func(n *node, id ID)

	// unloggedDecision records the first Decision message a node sent
	// without that decision being in its own shard's log.
	unloggedDecision error
}

// observe checks every Decision on the wire: the sender must already have
// the decision applied from its shard's Paxos log, with the same outcome.
func (w *world) observe(m paxos.Message) {
	ext, ok := m.Body.(paxos.Ext)
	if !ok {
		return
	}
	d, ok := ext.Body.(Decision)
	if !ok || w.unloggedDecision != nil {
		return
	}
	n := w.nodes[m.From]
	if n == nil {
		return // a crashed node's sends are dropped anyway
	}
	if c, known := n.sm.Decided(d.Txn); !known || c != d.Commit {
		w.unloggedDecision = fmt.Errorf("node %d sent Decision{txn %d, commit %v} without that decision in its log", m.From, d.Txn, d.Commit)
	}
}

func shardNodes(sh ShardID) []paxos.NodeID {
	base := paxos.NodeID(int(sh)*10 + 1)
	return []paxos.NodeID{base, base + 1, base + 2}
}

func newWorld(seed uint64, o worldOpts) *world {
	if o.Shards == 0 {
		o.Shards = DefaultShards
	}
	w := &world{
		seed: seed, opts: o,
		rng:     rand.New(rand.NewPCG(seed, 0x7a)),
		net:     transport.NewSimNet(seed),
		shard:   map[paxos.NodeID]ShardID{},
		nodes:   map[paxos.NodeID]*node{},
		stores:  map[paxos.NodeID]*storage.Memory{},
		crashIn: map[paxos.NodeID]int{},
		gapRng:  rand.New(rand.NewPCG(seed, 0x9a9)),
		h:       sha256.New(),
	}
	w.net.SetFaults(o.Faults)
	w.net.Trace = w.trace
	w.net.OnSend = w.observe
	for sh := ShardID(0); int(sh) < o.Shards; sh++ {
		for _, id := range shardNodes(sh) {
			w.ids = append(w.ids, id)
			w.shard[id] = sh
			w.stores[id] = storage.NewMemory()
		}
	}
	for _, id := range w.ids {
		w.start(id)
	}
	for i := 0; i < o.Clients; i++ {
		c := &client{w: w, idx: i, id: paxos.NodeID(clientBase + i), ops: o.OpsPerClient,
			rng: rand.New(rand.NewPCG(w.rng.Uint64(), uint64(i))), attempts: map[ID]*attempt{}, leaders: map[ShardID]paxos.NodeID{}}
		c.think = 1 + c.rng.IntN(10)
		w.cli = append(w.cli, c)
		w.net.Register(c.id, c.handle)
	}
	return w
}

func (w *world) trace(line string) { w.h.Write([]byte(line + "\n")) }

func (w *world) traceHash() string { return hex.EncodeToString(w.h.Sum(nil)) }

func (w *world) initial(sh ShardID) map[string]string {
	out := map[string]string{}
	for i := 0; i < numAccounts; i++ {
		if ShardOf(account(i), w.opts.Shards) == sh {
			out[account(i)] = strconv.Itoa(initialBalance)
		}
	}
	return out
}

func (w *world) start(id paxos.NodeID) {
	sh := w.shard[id]
	sm := NewSM(w.initial(sh))
	// Set before the replica replays its log: every replica must validate
	// with the same rules or their states diverge.
	sm.noLockCheck = w.opts.Server.ReleaseLocksAtPrepare
	rep, err := paxos.NewReplica(paxos.ReplicaConfig{
		ID: id, Peers: shardNodes(sh), StateMachine: sm,
		Rand:          rand.New(rand.NewPCG(w.rng.Uint64(), uint64(id))),
		NoCheckQuorum: w.opts.NoCheckQuorum,
	}, w.stores[id], gapNet{w})
	if err != nil {
		panic(err)
	}
	cfg := w.opts.Server
	cfg.Shard, cfg.ShardNodes = sh, shardNodes
	if w.opts.Observe {
		o := &recObserver{shard: sh, finished: map[ID]bool{}}
		w.observers = append(w.observers, o)
		cfg.Observer = o
	}
	n := &node{id: id, shard: sh, rep: rep, sm: sm}
	n.srv = NewServer(cfg, id, rep, sm, func(_ ShardID, m paxos.Message) { w.net.Send(m) })
	n.srv.OnDecide = func(t ID) {
		if w.onDecide != nil {
			w.onDecide(n, t)
		}
	}
	n.srv.OnPrepared = func(t ID) {
		if w.onPrepared != nil {
			w.onPrepared(n, t)
		}
	}
	n.srv.After() // drop events from replaying the log
	w.allServers = append(w.allServers, n.srv)
	w.nodes[id] = n
	w.net.Register(id, func(m paxos.Message) {
		if _, ok := m.Body.(paxos.Ext); ok {
			n.srv.Handle(m)
		} else {
			n.rep.Handle(m)
		}
		n.srv.After()
	})
}

func (w *world) crash(id paxos.NodeID) {
	if w.nodes[id] == nil {
		return
	}
	w.nodes[id] = nil
	w.net.Crash(id)
}

func (w *world) restart(id paxos.NodeID) {
	if w.nodes[id] != nil {
		return
	}
	w.trace(fmt.Sprintf("restart %d", id))
	w.start(id)
}

func (w *world) injectChaos() {
	ch := w.opts.Chaos
	var live, down []paxos.NodeID
	for _, id := range w.ids {
		if w.nodes[id] != nil {
			live = append(live, id)
		} else {
			down = append(down, id)
		}
	}
	if len(live) > 0 && w.rng.Float64() < ch.CrashProb {
		w.crash(live[w.rng.IntN(len(live))])
	}
	if len(down) > 0 && w.rng.Float64() < ch.RestartProb {
		w.restart(down[w.rng.IntN(len(down))])
	}
	if w.rng.Float64() < ch.PartitionProb {
		perm := w.rng.Perm(len(w.ids))
		cut := 1 + w.rng.IntN(len(w.ids)-1)
		var a, b []paxos.NodeID
		for i, p := range perm {
			if i < cut {
				a = append(a, w.ids[p])
			} else {
				b = append(b, w.ids[p])
			}
		}
		w.net.Partition(a, b)
	}
	if w.rng.Float64() < ch.HealProb {
		w.net.Heal()
	}
}

func (w *world) step(withChaos bool) {
	w.chaosOn = withChaos
	if withChaos {
		w.injectChaos()
	}
	for _, id := range w.ids {
		if n, ok := w.crashIn[id]; ok {
			if n <= 1 {
				delete(w.crashIn, id)
				w.crash(id)
			} else {
				w.crashIn[id] = n - 1
			}
		}
	}
	w.net.Step()
	for _, id := range w.ids {
		if n := w.nodes[id]; n != nil {
			n.rep.Tick()
			n.srv.Tick()
			n.srv.After()
		}
	}
	for _, c := range w.cli {
		c.tick()
	}
}

func (w *world) run(steps int, withChaos bool) {
	for range steps {
		w.step(withChaos)
	}
}

func (w *world) calm() {
	clear(w.crashIn)
	w.net.SetFaults(transport.Faults{MaxDelay: 2})
	w.net.Heal()
	for _, id := range w.ids {
		w.restart(id)
	}
}

func (w *world) runUntil(limit int, cond func() bool) (int, bool) {
	for i := 0; i < limit; i++ {
		if cond() {
			return i, true
		}
		w.step(false)
	}
	return limit, cond()
}

func (w *world) clientsDone() bool {
	for _, c := range w.cli {
		if !c.done() {
			return false
		}
	}
	return true
}

// leader returns the live leader of a shard with the highest ballot.
func (w *world) leader(sh ShardID) *node {
	var best *node
	for _, id := range shardNodes(sh) {
		n := w.nodes[id]
		if n != nil && n.rep.IsLeader() && (best == nil || best.rep.Ballot().Less(n.rep.Ballot())) {
			best = n
		}
	}
	return best
}

// leadersUp reports whether every shard has a leader whose server has
// taken over (directed tests wait for it before starting clients, so
// their timing does not depend on how long the first elections take).
func (w *world) leadersUp() bool {
	for sh := ShardID(0); int(sh) < w.opts.Shards; sh++ {
		if l := w.leader(sh); l == nil || !l.srv.Leading() {
			return false
		}
	}
	return true
}

// canonical returns, per shard, the live replica that has applied the most.
// Paxos log safety (tested elsewhere) makes its state the shard's state.
func (w *world) canonical() map[ShardID]*SM {
	out := map[ShardID]*SM{}
	for sh := ShardID(0); int(sh) < w.opts.Shards; sh++ {
		var best *node
		for _, id := range shardNodes(sh) {
			n := w.nodes[id]
			if n != nil && (best == nil || n.rep.Commit() > best.rep.Commit()) {
				best = n
			}
		}
		out[sh] = best.sm
	}
	return out
}

// quiescent reports whether every client is done and no shard leader holds
// a prepared transaction.
func (w *world) quiescent() bool {
	if !w.clientsDone() {
		return false
	}
	for sh, sm := range w.canonical() {
		if len(sm.PreparedTxns()) > 0 {
			return false
		}
		if w.leader(sh) == nil {
			return false
		}
	}
	return true
}

// ---- Clients ----

type attempt struct {
	reads  map[string]uint64
	vals   map[string]string
	writes map[string]string
	audit  bool
	begin  int64 // step the attempt began
	ack    int64 // step the client learned it committed, -1 if never
	// toldAbort: a CommitResp said the attempt did not commit. The logs
	// must agree (checkAtomicity).
	toldAbort bool
}

const (
	phIdle = iota
	phReading
	phCommitting
	phBackoff
	phHeld // reads done, commit held back by the test
)

// client runs interactive transactions one at a time: transfers between
// two accounts, and audits that read every account.
type client struct {
	w        *world
	idx      int
	id       paxos.NodeID
	rng      *rand.Rand
	ops      int
	think    int
	ts       uint64
	counter  uint64
	meta     Meta
	phase    int
	keys     []string
	write    string // key a fixed plan writes, "" for random transfers/audits
	plan     func(c *client) ([]string, string)
	audit    bool
	next     int
	cur      *attempt
	timer    int
	tries    int
	backoff  int
	attempts map[ID]*attempt
	commits  int
	aborts   int
	hold     bool                     // stop before committing; release() continues
	leaders  map[ShardID]paxos.NodeID // learned from replies, as the SDK caches them
	rr       int                      // round-robin position when a leader is unknown
}

// release lets a held client send its commit request.
func (c *client) release() {
	c.hold = false
	if c.phase == phHeld {
		c.phase = phCommitting
		c.resend()
	}
}

func (c *client) done() bool { return c.ops == 0 && c.phase == phIdle }

func (c *client) shardOf(k string) ShardID { return ShardOf(k, c.w.opts.Shards) }

// commitTarget picks the node for a commit request the way the SDK does:
// the shard's cached leader, or the next replica round-robin when it is
// unknown. A retry after a timeout forgets the cached leader first, so
// the request moves on to another replica, which may be the new leader.
func (c *client) commitTarget(sh ShardID, retry bool) paxos.NodeID {
	if retry {
		delete(c.leaders, sh)
	}
	if n, ok := c.leaders[sh]; ok {
		return n
	}
	nodes := shardNodes(sh)
	c.rr++
	return nodes[c.rr%len(nodes)]
}

func (c *client) toShard(sh ShardID, body any) {
	for _, n := range shardNodes(sh) {
		c.w.net.Send(paxos.Message{From: c.id, To: n, Body: paxos.Ext{Body: body}})
	}
}

func (c *client) tick() {
	switch c.phase {
	case phIdle:
		if c.ops == 0 {
			return
		}
		if c.think--; c.think > 0 {
			return
		}
		c.newOp()
	case phReading, phCommitting:
		if c.timer--; c.timer > 0 {
			return
		}
		c.tries++
		if c.phase == phReading && c.tries > maxReadTries {
			c.abort()
			return
		}
		c.resend()
	case phBackoff:
		if c.timer--; c.timer <= 0 {
			c.begin()
		}
	}
}

func (c *client) newOp() {
	c.ts = c.w.net.Now()<<8 | uint64(c.idx)
	c.backoff = 4
	switch {
	case c.plan != nil:
		c.keys, c.write = c.plan(c)
		c.audit = false
	case c.rng.IntN(100) < c.w.opts.AuditPercent:
		c.keys, c.write, c.audit = nil, "", true
		for i := 0; i < numAccounts; i++ {
			c.keys = append(c.keys, account(i))
		}
	default:
		a := c.rng.IntN(numAccounts)
		b := (a + 1 + c.rng.IntN(numAccounts-1)) % numAccounts
		c.keys, c.write, c.audit = []string{account(a), account(b)}, "", false
	}
	c.begin()
}

// begin starts a new attempt of the current operation, keeping its start
// timestamp so wound-wait eventually lets it through.
func (c *client) begin() {
	c.counter++
	c.meta = Meta{ID: ID(uint64(c.idx+1)<<32 | c.counter), TS: c.ts}
	c.cur = &attempt{reads: map[string]uint64{}, vals: map[string]string{}, audit: c.audit,
		begin: int64(c.w.net.Now()), ack: -1}
	c.attempts[c.meta.ID] = c.cur
	c.next, c.tries = 0, 0
	c.phase = phReading
	c.resend()
}

func (c *client) resend() {
	switch c.phase {
	case phReading:
		k := c.keys[c.next]
		c.timer = readTimeout
		c.toShard(c.shardOf(k), ReadReq{Txn: c.meta, Key: k, Client: c.id})
	case phCommitting:
		c.timer = commitTimeout
		parts := c.parts()
		to := c.commitTarget(parts[0].Shard, c.tries > 0)
		c.w.net.Send(paxos.Message{From: c.id, To: to, Body: paxos.Ext{Body: CommitReq{Txn: c.meta, Coord: parts[0].Shard, Parts: parts, Client: c.id}}})
	}
}

func (c *client) parts() []Part {
	by := map[ShardID]*Part{}
	get := func(k string) *Part {
		sh := c.shardOf(k)
		if by[sh] == nil {
			by[sh] = &Part{Shard: sh, Reads: map[string]uint64{}, Writes: map[string]string{}}
		}
		return by[sh]
	}
	for k, v := range c.cur.reads {
		get(k).Reads[k] = v
	}
	for k, v := range c.cur.writes {
		get(k).Writes[k] = v
	}
	var out []Part
	for sh := ShardID(0); int(sh) < c.w.opts.Shards; sh++ {
		if by[sh] != nil {
			out = append(out, *by[sh])
		}
	}
	return out
}

func (c *client) handle(m paxos.Message) {
	ext, ok := m.Body.(paxos.Ext)
	if !ok {
		return
	}
	switch r := ext.Body.(type) {
	case ReadResp:
		if c.phase != phReading || r.Txn != c.meta.ID || r.Key != c.keys[c.next] {
			return
		}
		c.leaders[c.w.shard[m.From]] = m.From
		if r.Aborted {
			c.abort()
			return
		}
		c.cur.reads[r.Key], c.cur.vals[r.Key] = r.Version, r.Value
		c.next, c.tries = c.next+1, 0
		if c.next < len(c.keys) {
			c.resend()
			return
		}
		c.cur.writes = c.compute()
		if c.hold {
			c.phase = phHeld
			return
		}
		c.phase = phCommitting
		c.resend()
	case CommitResp:
		if c.phase != phCommitting || r.Txn != c.meta.ID {
			return
		}
		c.leaders[c.w.shard[m.From]] = m.From
		if !r.Committed {
			c.cur.toldAbort = true
			c.abort()
			return
		}
		c.cur.ack = int64(c.w.net.Now())
		c.commits++
		c.ops--
		c.phase, c.think = phIdle, 1+c.rng.IntN(10)
	}
}

func (c *client) compute() map[string]string {
	w := map[string]string{}
	if c.audit {
		return w
	}
	if c.write != "" { // fixed plan: increment the written key
		v, _ := strconv.Atoi(c.cur.vals[c.write])
		w[c.write] = strconv.Itoa(v + 1)
		return w
	}
	a, b := c.keys[0], c.keys[1]
	ba, _ := strconv.Atoi(c.cur.vals[a])
	bb, _ := strconv.Atoi(c.cur.vals[b])
	amt := 1 + c.rng.IntN(10)
	if ba >= amt {
		w[a], w[b] = strconv.Itoa(ba-amt), strconv.Itoa(bb+amt)
	}
	return w
}

func (c *client) abort() {
	c.aborts++
	touched := map[ShardID]bool{}
	for _, k := range c.keys {
		touched[c.shardOf(k)] = true
	}
	for sh := ShardID(0); int(sh) < c.w.opts.Shards; sh++ {
		if touched[sh] {
			c.toShard(sh, AbortReq{Txn: c.meta.ID})
		}
	}
	c.phase = phBackoff
	c.timer = 1 + c.rng.IntN(c.backoff)
	c.backoff = min(c.backoff*2, 64)
}

// gapNet is the transport replicas use. Outside gap-heavy mode (or outside
// chaos) it passes every message straight to the SimNet.
type gapNet struct{ w *world }

func (g gapNet) Send(m paxos.Message) {
	w := g.w
	ch := w.opts.Chaos
	if acc, ok := m.Body.(paxos.LogAccept); ok && w.chaosOn && (ch.AcceptDrop > 0 || ch.RecordCrash > 0) {
		if r, err := decode(acc.Entry.Cmd); err == nil && !acc.Entry.Noop &&
			(r.Kind == KindOnePhase || r.Kind == KindPrepare || r.Kind == KindDecide) {
			if _, pending := w.crashIn[m.From]; !pending && w.gapRng.Float64() < ch.RecordCrash {
				w.crashIn[m.From] = 1 + w.gapRng.IntN(3)
				w.recordCrashes++
			}
		}
		if w.gapRng.Float64() < ch.AcceptDrop {
			w.trace(fmt.Sprintf("t=%d gapdrop %v", w.net.Now(), m))
			return
		}
	}
	w.net.Send(m)
}

// recObserver records a server's Observer events.
type recObserver struct {
	shard    ShardID
	finished map[ID]bool // outcome reported by Finished
	reasons  map[AbortReason]int
	dup      error
	events   int
}

func (o *recObserver) Finished(id ID, commit, _ bool, reason AbortReason) {
	if _, ok := o.finished[id]; ok && o.dup == nil {
		o.dup = fmt.Errorf("shard %d reported txn %d twice", o.shard, id)
	}
	o.finished[id] = commit
	if !commit {
		if o.reasons == nil {
			o.reasons = map[AbortReason]int{}
		}
		o.reasons[reason]++
	}
}
func (o *recObserver) CoordinationStarted(ID)    { o.events++ }
func (o *recObserver) DecisionProposed(ID, bool) { o.events++ }
func (o *recObserver) PrepareProposed(ID)        { o.events++ }
func (o *recObserver) PrepareApplied(ID, bool)   { o.events++ }
func (o *recObserver) QuerySent(ID)              { o.events++ }
func (o *recObserver) Resolved(ID, bool)         { o.events++ }
func (o *recObserver) Wounded(ID)                { o.events++ }
func (o *recObserver) LockWait(ID)               { o.events++ }

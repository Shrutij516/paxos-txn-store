package paxos_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"maps"
	"math/rand/v2"
	"slices"

	"github.com/anishathalye/porcupine"

	"github.com/Shrutij516/paxos-txn-store/internal/kv"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/storage"
	"github.com/Shrutij516/paxos-txn-store/internal/transport"
)

const (
	clientBase    = 100 // client node IDs start above replica IDs
	clientTimeout = 40
)

type mpOpts struct {
	N            int
	Clients      int
	OpsPerClient int
	Keys         int
	Faults       transport.Faults
	Chaos        chaos

	// Broken modes for the negative tests.
	SkipPrepare    bool
	IgnorePromised bool
	NoDedup        bool
}

// applied is one entry a state machine applied, with its result.
type applied struct {
	slot   uint64
	entry  paxos.Entry
	result paxos.Value
}

// recSM wraps the kv store of one replica incarnation and records every
// entry it applies.
type recSM struct {
	c    *mpCluster
	node paxos.NodeID
	kv   *kv.Store
	log  []applied
}

func (r *recSM) Apply(slot uint64, e paxos.Entry) paxos.Value {
	res := r.kv.Apply(slot, e)
	if want := uint64(len(r.log) + 1); slot != want && r.c.violation == nil {
		r.c.violation = fmt.Errorf("node %d applied slot %d, expected %d", r.node, slot, want)
	}
	r.log = append(r.log, applied{slot, e, res})
	r.c.noteCommit(r.node, slot, e)
	r.c.trace(fmt.Sprintf("apply %d slot=%d %+v -> %q", r.node, slot, e, res))
	return res
}

type oracleKey struct {
	slot   uint64
	ballot paxos.Ballot
	entry  paxos.Entry
}

// mpCluster runs replicas and clients on one SimNet from one seed.
type mpCluster struct {
	seed  uint64
	opts  mpOpts
	rng   *rand.Rand
	net   *transport.SimNet
	ids   []paxos.NodeID
	reps  map[paxos.NodeID]*paxos.Replica // nil means crashed
	store map[paxos.NodeID]*storage.Memory
	sms   []*recSM // every incarnation ever
	cli   []*simClient

	committed map[uint64]paxos.Entry // first entry any node applied per slot
	oracle    map[oracleKey]map[paxos.NodeID]bool
	chosenAt  map[uint64]paxos.Entry
	violation error
	h         hash.Hash
}

func newMPCluster(seed uint64, o mpOpts) *mpCluster {
	c := &mpCluster{
		seed:      seed,
		opts:      o,
		rng:       rand.New(rand.NewPCG(seed, 0xfeed)),
		net:       transport.NewSimNet(seed),
		reps:      map[paxos.NodeID]*paxos.Replica{},
		store:     map[paxos.NodeID]*storage.Memory{},
		committed: map[uint64]paxos.Entry{},
		oracle:    map[oracleKey]map[paxos.NodeID]bool{},
		chosenAt:  map[uint64]paxos.Entry{},
		h:         sha256.New(),
	}
	c.net.SetFaults(o.Faults)
	c.net.Trace = c.trace
	c.net.OnSend = c.observe
	for i := 1; i <= o.N; i++ {
		c.ids = append(c.ids, paxos.NodeID(i))
	}
	for _, id := range c.ids {
		c.store[id] = storage.NewMemory()
		c.start(id)
	}
	for i := 0; i < o.Clients; i++ {
		cl := &simClient{
			c: c, idx: i, id: paxos.NodeID(clientBase + i), cid: uint64(i + 1),
			rng:    rand.New(rand.NewPCG(c.rng.Uint64(), uint64(i))),
			opsDue: o.OpsPerClient,
		}
		cl.think = 1 + cl.rng.IntN(10)
		cl.target = c.ids[cl.rng.IntN(len(c.ids))]
		c.cli = append(c.cli, cl)
		c.net.Register(cl.id, cl.handle)
	}
	return c
}

func (c *mpCluster) trace(line string) {
	c.h.Write([]byte(line))
	c.h.Write([]byte{'\n'})
}

func (c *mpCluster) TraceHash() string { return hex.EncodeToString(c.h.Sum(nil)) }

func (c *mpCluster) start(id paxos.NodeID) {
	var store *kv.Store
	if c.opts.NoDedup {
		store = kv.NewWithoutDedup()
	} else {
		store = kv.New()
	}
	sm := &recSM{c: c, node: id, kv: store}
	c.sms = append(c.sms, sm)
	r, err := paxos.NewReplica(paxos.ReplicaConfig{
		ID: id, Peers: c.ids, StateMachine: sm,
		Rand: rand.New(rand.NewPCG(c.rng.Uint64(), uint64(id))),
	}, c.store[id], c.net)
	if err != nil {
		panic(err)
	}
	if c.opts.SkipPrepare {
		r.SkipPrepareOnTakeover()
	}
	if c.opts.IgnorePromised {
		r.IgnorePromisedValues()
	}
	c.reps[id] = r
	c.net.Register(id, r.Handle)
}

// noteCommit checks log safety: every node must apply the same entry at a
// given slot, across all nodes and all restarts.
func (c *mpCluster) noteCommit(node paxos.NodeID, slot uint64, e paxos.Entry) {
	prev, ok := c.committed[slot]
	if !ok {
		c.committed[slot] = e
		return
	}
	if prev != e && c.violation == nil {
		c.violation = fmt.Errorf("log safety: slot %d committed as %+v and as %+v (node %d)", slot, prev, e, node)
	}
}

// observe is the global oracle: an entry is chosen at a slot once a
// majority of acceptors sent LogAccepted for it in one ballot.
func (c *mpCluster) observe(m paxos.Message) {
	a, ok := m.Body.(paxos.LogAccepted)
	if !ok {
		return
	}
	k := oracleKey{a.Slot, a.Ballot, a.Entry}
	v := c.oracle[k]
	if v == nil {
		v = map[paxos.NodeID]bool{}
		c.oracle[k] = v
	}
	v[m.From] = true
	if len(v) <= c.opts.N/2 {
		return
	}
	prev, seen := c.chosenAt[a.Slot]
	if !seen {
		c.chosenAt[a.Slot] = a.Entry
		return
	}
	if prev != a.Entry && c.violation == nil {
		c.violation = fmt.Errorf("log safety: two entries chosen at slot %d: %+v and %+v", a.Slot, prev, a.Entry)
	}
}

func (c *mpCluster) crash(id paxos.NodeID) {
	if c.reps[id] == nil {
		return
	}
	c.reps[id] = nil
	c.net.Crash(id)
}

func (c *mpCluster) restart(id paxos.NodeID) {
	if c.reps[id] != nil {
		return
	}
	c.trace(fmt.Sprintf("restart %d", id))
	c.start(id)
}

func (c *mpCluster) live() []paxos.NodeID {
	var out []paxos.NodeID
	for _, id := range c.ids {
		if c.reps[id] != nil {
			out = append(out, id)
		}
	}
	return out
}

func (c *mpCluster) injectChaos() {
	ch := c.opts.Chaos
	if l := c.live(); len(l) > 0 && c.rng.Float64() < ch.CrashProb {
		c.crash(l[c.rng.IntN(len(l))])
	}
	if c.rng.Float64() < ch.RestartProb {
		for _, id := range c.ids {
			if c.reps[id] == nil {
				c.restart(id)
				break
			}
		}
	}
	if c.rng.Float64() < ch.PartitionProb {
		perm := c.rng.Perm(len(c.ids))
		cut := 1 + c.rng.IntN(len(c.ids)-1)
		var a, b []paxos.NodeID
		for i, p := range perm {
			if i < cut {
				a = append(a, c.ids[p])
			} else {
				b = append(b, c.ids[p])
			}
		}
		c.net.Partition(a, b)
	}
	if c.rng.Float64() < ch.HealProb {
		c.net.Heal()
	}
}

func (c *mpCluster) step(withChaos bool) {
	if withChaos {
		c.injectChaos()
	}
	c.net.Step()
	for _, id := range c.ids {
		if r := c.reps[id]; r != nil {
			r.Tick()
		}
	}
	for _, cl := range c.cli {
		cl.tick()
	}
}

func (c *mpCluster) run(steps int, withChaos bool) {
	for range steps {
		c.step(withChaos)
	}
}

// calm stops all faults, heals partitions and restarts every crashed node.
func (c *mpCluster) calm() {
	c.net.SetFaults(transport.Faults{MaxDelay: 2})
	c.net.Heal()
	for _, id := range c.ids {
		c.restart(id)
	}
}

// runUntil steps without chaos until cond holds or limit steps pass.
func (c *mpCluster) runUntil(limit int, cond func() bool) (int, bool) {
	for i := 0; i < limit; i++ {
		if cond() {
			return i, true
		}
		c.step(false)
	}
	return limit, cond()
}

func (c *mpCluster) clientsDone() bool {
	for _, cl := range c.cli {
		if cl.opsDue > 0 || cl.waiting {
			return false
		}
	}
	return true
}

// leader returns the live replica that leads with the highest ballot.
func (c *mpCluster) leader() *paxos.Replica {
	var best *paxos.Replica
	for _, id := range c.ids {
		r := c.reps[id]
		if r != nil && r.IsLeader() && (best == nil || best.Ballot().Less(r.Ballot())) {
			best = r
		}
	}
	return best
}

// checkSafety checks log safety, state machine safety and exactly-once.
func (c *mpCluster) checkSafety() error {
	if c.violation != nil {
		return c.violation
	}
	for i, a := range c.sms {
		for _, b := range c.sms[i+1:] {
			n := min(len(a.log), len(b.log))
			for k := 0; k < n; k++ {
				if a.log[k] != b.log[k] {
					return fmt.Errorf("state machine safety: node %d and node %d differ at applied #%d: %+v vs %+v",
						a.node, b.node, k+1, a.log[k], b.log[k])
				}
			}
		}
		if n := a.kv.MaxExecutions(); n > 1 {
			return fmt.Errorf("exactly-once: node %d executed one request %d times", a.node, n)
		}
	}
	return nil
}

// ---- Clients and linearizability ----

type kvInput struct {
	put        bool
	key, value string
}

type kvOutput struct {
	value   string
	unknown bool // the operation never returned
}

type simClient struct {
	c      *mpCluster
	idx    int
	id     paxos.NodeID
	cid    uint64
	rng    *rand.Rand
	seq    uint64
	opsDue int
	think  int

	waiting bool
	cur     paxos.ClientRequest
	in      kvInput
	callAt  uint64
	target  paxos.NodeID
	timer   int

	history []porcupine.Operation
}

func (cl *simClient) send() {
	cl.timer = clientTimeout
	cl.c.net.Send(paxos.Message{From: cl.id, To: cl.target, Body: cl.cur})
}

func (cl *simClient) tick() {
	if cl.waiting {
		cl.timer--
		if cl.timer <= 0 {
			cl.target = cl.c.ids[cl.rng.IntN(len(cl.c.ids))]
			cl.send()
		}
		return
	}
	if cl.opsDue == 0 {
		return
	}
	cl.think--
	if cl.think > 0 {
		return
	}
	cl.opsDue--
	cl.seq++
	key := fmt.Sprintf("k%d", cl.rng.IntN(max(cl.c.opts.Keys, 1)))
	if cl.rng.IntN(2) == 0 {
		v := fmt.Sprintf("c%d-%d", cl.cid, cl.seq)
		cl.in = kvInput{put: true, key: key, value: v}
		cl.cur = paxos.ClientRequest{ClientID: cl.cid, Seq: cl.seq, Cmd: kv.Put(key, v)}
	} else {
		cl.in = kvInput{key: key}
		cl.cur = paxos.ClientRequest{ClientID: cl.cid, Seq: cl.seq, Cmd: kv.Get(key)}
	}
	cl.waiting = true
	cl.callAt = cl.c.net.Now()
	cl.c.trace(fmt.Sprintf("call client=%d seq=%d %+v", cl.cid, cl.seq, cl.in))
	cl.send()
}

func (cl *simClient) handle(m paxos.Message) {
	r, ok := m.Body.(paxos.ClientReply)
	if !ok || !cl.waiting || r.Seq != cl.seq {
		return
	}
	if !r.OK {
		if r.Leader != 0 && r.Leader != cl.target {
			cl.target = r.Leader
			cl.send()
		}
		return
	}
	cl.waiting = false
	cl.think = 1 + cl.rng.IntN(10)
	cl.history = append(cl.history, porcupine.Operation{
		ClientId: cl.idx, Input: cl.in, Call: int64(cl.callAt),
		Output: kvOutput{value: string(r.Result)}, Return: int64(cl.c.net.Now()),
	})
	cl.c.trace(fmt.Sprintf("return client=%d seq=%d %q", cl.cid, cl.seq, r.Result))
}

// history returns every client operation. Operations still outstanding get
// an unknown output and a return time after everything else.
func (c *mpCluster) history() []porcupine.Operation {
	var ops []porcupine.Operation
	end := int64(c.net.Now()) + 1
	for _, cl := range c.cli {
		ops = append(ops, cl.history...)
		if cl.waiting {
			ops = append(ops, porcupine.Operation{
				ClientId: cl.idx, Input: cl.in, Call: int64(cl.callAt),
				Output: kvOutput{unknown: true}, Return: end,
			})
		}
	}
	return ops
}

// kvModel is a single-register-per-key model, partitioned by key.
var kvModel = porcupine.Model{
	Partition: func(h []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range h {
			k := op.Input.(kvInput).key
			byKey[k] = append(byKey[k], op)
		}
		var out [][]porcupine.Operation
		for _, k := range slices.Sorted(maps.Keys(byKey)) {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() any { return "" },
	Step: func(state, input, output any) (bool, any) {
		in, out, st := input.(kvInput), output.(kvOutput), state.(string)
		if in.put {
			return true, in.value
		}
		return out.unknown || out.value == st, st
	},
}

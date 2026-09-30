package paxos_test

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"math/rand/v2"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/storage"
	"github.com/Shrutij516/paxos-txn-store/internal/transport"
)

// chaos is the per-step probability of each node-level fault.
type chaos struct {
	CrashProb     float64
	RestartProb   float64
	PartitionProb float64
	HealProb      float64
}

type clusterOpts struct {
	N         int
	Proposers int // nodes 1..Proposers propose at step 0
	Faults    transport.Faults
	Chaos     chaos

	// Broken modes for the negative tests.
	ForgetOnRestart    bool
	NoPromiseRule      bool
	NoAcceptRule       bool
	ReproposeOnRestart bool
}

// cluster is a test harness: nodes on a SimNet, a chaos driver, a global
// oracle of chosen values, and a running trace hash. Everything is driven by
// one seed and runs on one goroutine.
type cluster struct {
	seed   uint64
	opts   clusterOpts
	rng    *rand.Rand
	net    *transport.SimNet
	ids    []paxos.NodeID
	nodes  map[paxos.NodeID]*paxos.Node // nil entry means crashed
	stores map[paxos.NodeID]*storage.Memory
	own    map[paxos.NodeID]paxos.Value // value each node is proposing

	proposed map[paxos.Value]bool
	decided  map[paxos.NodeID]paxos.Value // first value each node learned
	decideAt map[paxos.NodeID]uint64
	oracle   map[paxos.Accepted]map[paxos.NodeID]bool
	chosen   map[paxos.Value]bool
	nextVal  int
	conflict error

	h hash.Hash
}

func newCluster(seed uint64, o clusterOpts) *cluster {
	c := &cluster{
		seed:     seed,
		opts:     o,
		rng:      rand.New(rand.NewPCG(seed, 0xc0ffee)),
		net:      transport.NewSimNet(seed),
		nodes:    map[paxos.NodeID]*paxos.Node{},
		stores:   map[paxos.NodeID]*storage.Memory{},
		own:      map[paxos.NodeID]paxos.Value{},
		proposed: map[paxos.Value]bool{},
		decided:  map[paxos.NodeID]paxos.Value{},
		decideAt: map[paxos.NodeID]uint64{},
		oracle:   map[paxos.Accepted]map[paxos.NodeID]bool{},
		chosen:   map[paxos.Value]bool{},
		h:        sha256.New(),
	}
	c.net.SetFaults(o.Faults)
	c.net.Trace = c.trace
	c.net.OnSend = c.observe
	for i := 1; i <= o.N; i++ {
		c.ids = append(c.ids, paxos.NodeID(i))
	}
	for _, id := range c.ids {
		c.stores[id] = storage.NewMemory()
		c.start(id)
	}
	for _, id := range c.ids[:o.Proposers] {
		c.propose(id)
	}
	return c
}

func (c *cluster) trace(line string) {
	c.h.Write([]byte(line))
	c.h.Write([]byte{'\n'})
}

// TraceHash is a digest of every event so far.
func (c *cluster) TraceHash() string { return hex.EncodeToString(c.h.Sum(nil)) }

// observe is the oracle. It sees every Accepted an acceptor emits, even ones
// the network later drops, and records a value as chosen once a majority of
// acceptors accepted it in the same ballot. This is the textbook definition
// of "chosen", independent of what any learner happened to hear.
func (c *cluster) observe(m paxos.Message) {
	a, ok := m.Body.(paxos.Accepted)
	if !ok {
		return
	}
	voters := c.oracle[a]
	if voters == nil {
		voters = map[paxos.NodeID]bool{}
		c.oracle[a] = voters
	}
	voters[m.From] = true
	if len(voters) > c.opts.N/2 && !c.chosen[a.Value] {
		c.chosen[a.Value] = true
		c.trace(fmt.Sprintf("chosen %v %q", a.Ballot, a.Value))
	}
}

func (c *cluster) start(id paxos.NodeID) {
	n, err := paxos.NewNode(paxos.Config{
		ID:    id,
		Peers: c.ids,
		Rand:  rand.New(rand.NewPCG(c.rng.Uint64(), uint64(id))),
	}, c.stores[id], c.net)
	if err != nil {
		panic(err)
	}
	if c.opts.NoPromiseRule {
		n.DisablePromiseRule()
	}
	if c.opts.NoAcceptRule {
		n.DisableAcceptRule()
	}
	c.nodes[id] = n
	c.net.Register(id, func(m paxos.Message) {
		n.Handle(m)
		c.noteDecision(id)
	})
}

func (c *cluster) noteDecision(id paxos.NodeID) {
	n := c.nodes[id]
	if n == nil {
		return
	}
	v, ok := n.Decided()
	if !ok {
		return
	}
	if _, seen := c.decided[id]; !seen {
		c.decided[id] = v
		c.decideAt[id] = c.net.Now()
		c.trace(fmt.Sprintf("decide %d %q", id, v))
	}
	if prev := c.decided[id]; prev != v && c.conflict == nil {
		// A restarted learner learns afresh; it must learn the same thing.
		c.conflict = fmt.Errorf("agreement: node %d decided %q, then after restart %q", id, prev, v)
	}
}

func (c *cluster) propose(id paxos.NodeID) {
	n := c.nodes[id]
	if n == nil {
		return
	}
	v, ok := c.own[id]
	if !ok {
		c.nextVal++
		v = paxos.Value(fmt.Sprintf("v%d-n%d", c.nextVal, id))
		c.own[id] = v
	}
	if err := n.Propose(v); err == nil {
		c.proposed[v] = true
		c.trace(fmt.Sprintf("propose %d %q", id, v))
	}
}

func (c *cluster) crash(id paxos.NodeID) {
	if c.nodes[id] == nil {
		return
	}
	c.nodes[id] = nil
	c.net.Crash(id)
}

func (c *cluster) restart(id paxos.NodeID) {
	if c.nodes[id] != nil {
		return
	}
	if c.opts.ForgetOnRestart {
		c.stores[id].ForgetAcceptor()
	}
	c.trace(fmt.Sprintf("restart %d", id))
	c.start(id)
	if c.opts.ReproposeOnRestart {
		if _, was := c.own[id]; was {
			c.propose(id)
		}
	}
}

func (c *cluster) live() []paxos.NodeID {
	var out []paxos.NodeID
	for _, id := range c.ids {
		if c.nodes[id] != nil {
			out = append(out, id)
		}
	}
	return out
}

func (c *cluster) down() []paxos.NodeID {
	var out []paxos.NodeID
	for _, id := range c.ids {
		if c.nodes[id] == nil {
			out = append(out, id)
		}
	}
	return out
}

func (c *cluster) injectChaos() {
	ch := c.opts.Chaos
	if l := c.live(); len(l) > 0 && c.rng.Float64() < ch.CrashProb {
		c.crash(l[c.rng.IntN(len(l))])
	}
	if d := c.down(); len(d) > 0 && c.rng.Float64() < ch.RestartProb {
		c.restart(d[c.rng.IntN(len(d))])
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

// step runs one simulator step: chaos, message delivery, then a tick for
// every live node in ID order.
func (c *cluster) step(withChaos bool) {
	if withChaos {
		c.injectChaos()
	}
	c.net.Step()
	for _, id := range c.ids {
		if n := c.nodes[id]; n != nil {
			n.Tick()
			c.noteDecision(id)
		}
	}
}

// run executes steps with chaos on.
func (c *cluster) run(steps int) {
	for range steps {
		c.step(true)
	}
}

// calm stops all faults, heals partitions, leaves exactly a bare majority
// alive, and asks every live undecided node to propose. It returns the live
// nodes.
func (c *cluster) calm() []paxos.NodeID {
	c.net.SetFaults(transport.Faults{MaxDelay: 1})
	c.net.Heal()
	for _, id := range c.down() {
		c.restart(id)
	}
	majority := c.opts.N/2 + 1
	for _, id := range c.rng.Perm(len(c.ids))[:c.opts.N-majority] {
		c.crash(c.ids[id])
	}
	live := c.live()
	for _, id := range live {
		if _, ok := c.nodes[id].Decided(); !ok {
			c.propose(id)
		}
	}
	return live
}

// runUntilDecided steps without chaos until every node in ids has decided or
// limit steps pass. It returns the number of steps used.
func (c *cluster) runUntilDecided(ids []paxos.NodeID, limit int) (int, bool) {
	for i := 0; i < limit; i++ {
		if c.allDecided(ids) {
			return i, true
		}
		c.step(false)
	}
	return limit, c.allDecided(ids)
}

func (c *cluster) allDecided(ids []paxos.NodeID) bool {
	for _, id := range ids {
		if _, ok := c.nodes[id].Decided(); !ok {
			return false
		}
	}
	return true
}

// checkSafety verifies agreement and validity over everything seen so far.
func (c *cluster) checkSafety() error {
	if c.conflict != nil {
		return c.conflict
	}
	var first paxos.Value
	var firstID paxos.NodeID
	for _, id := range c.ids {
		v, ok := c.decided[id]
		if !ok {
			continue
		}
		if !c.proposed[v] {
			return fmt.Errorf("validity: node %d decided %q which nobody proposed", id, v)
		}
		if firstID == 0 {
			first, firstID = v, id
			continue
		}
		if v != first {
			return fmt.Errorf("agreement: node %d decided %q but node %d decided %q", firstID, first, id, v)
		}
	}
	if len(c.chosen) > 1 {
		return fmt.Errorf("agreement: more than one value chosen by a majority: %v", c.chosen)
	}
	for v := range c.chosen {
		if !c.proposed[v] {
			return fmt.Errorf("validity: chosen value %q was never proposed", v)
		}
		if firstID != 0 && v != first {
			return fmt.Errorf("agreement: learners decided %q but %q was chosen", first, v)
		}
	}
	return nil
}

// randomOpts draws a fault-heavy schedule from rng.
func randomOpts(rng *rand.Rand) clusterOpts {
	n := []int{3, 5}[rng.IntN(2)]
	return clusterOpts{
		N:         n,
		Proposers: 2 + rng.IntN(n-1),
		Faults: transport.Faults{
			DropProb: rng.Float64() * 0.3,
			DupProb:  rng.Float64() * 0.2,
			MaxDelay: 1 + rng.IntN(10),
		},
		Chaos: chaos{
			CrashProb:     rng.Float64() * 0.02,
			RestartProb:   0.05,
			PartitionProb: rng.Float64() * 0.02,
			HealProb:      0.05,
		},
		ReproposeOnRestart: true,
	}
}

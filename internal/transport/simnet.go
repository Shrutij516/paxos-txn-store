// Package transport holds implementations of paxos.Transport: SimNet, a
// deterministic in-memory network for tests, and GRPC for real nodes.
package transport

import (
	"fmt"
	"math/rand/v2"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
)

// Faults configures what SimNet does to each message.
type Faults struct {
	DropProb float64 // chance a message is lost
	DupProb  float64 // chance a message is delivered twice
	MaxDelay int     // each copy is delayed 1..MaxDelay steps; values < 1 mean 1
}

// Handler receives a delivered message.
type Handler func(paxos.Message)

type envelope struct {
	at  uint64
	msg paxos.Message
}

// SimNet is a single-threaded, step-driven network. Nothing moves until the
// caller calls Step. All choices come from one seeded random source, so a
// given seed and a given sequence of calls always produce the same run.
//
// Reordering falls out of random per-message delays plus a shuffle of all
// messages that become due in the same step.
type SimNet struct {
	rng      *rand.Rand
	now      uint64
	faults   Faults
	queue    []envelope
	handlers map[paxos.NodeID]Handler
	down     map[paxos.NodeID]bool
	group    map[paxos.NodeID]int // partition group; nodes talk only within a group
	slow     map[paxos.NodeID]Slow

	// OnSend, if set, sees every message handed to Send before any fault is
	// applied. Tests use it to watch what acceptors actually did.
	OnSend func(paxos.Message)
	// Trace, if set, receives one line per network event.
	Trace func(string)
}

var _ paxos.Transport = (*SimNet)(nil)

// NewSimNet returns a network seeded with seed.
func NewSimNet(seed uint64) *SimNet {
	return &SimNet{
		rng:      rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		handlers: make(map[paxos.NodeID]Handler),
		down:     make(map[paxos.NodeID]bool),
		group:    make(map[paxos.NodeID]int),
		slow:     make(map[paxos.NodeID]Slow),
	}
}

// Slow describes a slow node: a starved CPU or a slow disk. Every message
// the node sends waits Persist extra steps (the write it makes durable
// before replying), and every message it receives waits Handle extra steps
// before the node gets to it. Messages a node sends to itself pay both.
type Slow struct {
	Persist, Handle int
}

// SetSlow makes id slow for messages sent from now on; the zero Slow makes
// it normal again.
func (n *SimNet) SetSlow(id paxos.NodeID, s Slow) {
	if s == (Slow{}) {
		delete(n.slow, id)
	} else {
		n.slow[id] = s
	}
	n.trace("slow %d %+v", id, s)
}

// Now returns the current step.
func (n *SimNet) Now() uint64 { return n.now }

// SetFaults replaces the fault configuration for messages sent from now on.
func (n *SimNet) SetFaults(f Faults) { n.faults = f }

// Register installs the handler for id and marks it up.
func (n *SimNet) Register(id paxos.NodeID, h Handler) {
	n.handlers[id] = h
	n.down[id] = false
}

// Crash marks id down. Messages to it, and new messages from it, are dropped
// until Register is called again. Messages it sent before crashing may still
// arrive, as on a real network.
func (n *SimNet) Crash(id paxos.NodeID) {
	n.down[id] = true
	n.trace("crash %d", id)
}

// Partition splits nodes into groups. Nodes not listed end up in group 0
// together with the first group.
func (n *SimNet) Partition(groups ...[]paxos.NodeID) {
	clear(n.group)
	for i, g := range groups {
		for _, id := range g {
			n.group[id] = i
		}
	}
	n.trace("partition %v", groups)
}

// Heal removes all partitions.
func (n *SimNet) Heal() {
	clear(n.group)
	n.trace("heal")
}

// Pending returns the number of messages in flight.
func (n *SimNet) Pending() int { return len(n.queue) }

// Send implements paxos.Transport.
func (n *SimNet) Send(m paxos.Message) {
	if n.OnSend != nil {
		n.OnSend(m)
	}
	if n.down[m.From] {
		return
	}
	if n.rng.Float64() < n.faults.DropProb {
		n.trace("drop %v", m)
		return
	}
	n.enqueue(m)
	if n.rng.Float64() < n.faults.DupProb {
		n.trace("dup %v", m)
		n.enqueue(m)
	}
}

func (n *SimNet) enqueue(m paxos.Message) {
	d := 1
	if n.faults.MaxDelay > 1 {
		d += n.rng.IntN(n.faults.MaxDelay)
	}
	d += n.slow[m.From].Persist + n.slow[m.To].Handle
	n.queue = append(n.queue, envelope{at: n.now + uint64(d), msg: m})
}

// Step advances time by one and delivers every message now due, in random
// order. Messages sent during delivery are due no earlier than the next step.
func (n *SimNet) Step() {
	n.now++
	var due []envelope
	keep := n.queue[:0]
	for _, e := range n.queue {
		if e.at <= n.now {
			due = append(due, e)
		} else {
			keep = append(keep, e)
		}
	}
	n.queue = keep
	// The queue is kept in send order, so the shuffle input is deterministic.
	n.rng.Shuffle(len(due), func(i, j int) { due[i], due[j] = due[j], due[i] })
	for _, e := range due {
		n.deliver(e.msg)
	}
}

func (n *SimNet) deliver(m paxos.Message) {
	h := n.handlers[m.To]
	switch {
	case h == nil || n.down[m.To]:
		n.trace("lost %v", m)
	case n.group[m.From] != n.group[m.To]:
		n.trace("cut %v", m)
	default:
		n.trace("deliver %v", m)
		h(m)
	}
}

func (n *SimNet) trace(format string, args ...any) {
	if n.Trace != nil {
		n.Trace(fmt.Sprintf("t=%d ", n.now) + fmt.Sprintf(format, args...))
	}
}

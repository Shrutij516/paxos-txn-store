package paxos

import (
	"errors"
	"slices"
)

// Config describes one node.
type Config struct {
	ID     NodeID
	Peers  []NodeID // every node in the cluster, including ID
	Rand   Rand
	Timing Timing // zero value means DefaultTiming
}

// Node bundles a proposer, an acceptor and a learner that share one
// Transport and one Storage. The caller feeds it messages with Handle and
// advances its clock with Tick. Node is not safe for concurrent use; the
// caller serializes all calls.
type Node struct {
	id       NodeID
	proposer *Proposer
	acceptor *Acceptor
	learner  *Learner
}

// NewNode builds a node and restores acceptor and proposer state from store.
func NewNode(cfg Config, store Storage, tr Transport) (*Node, error) {
	if !slices.Contains(cfg.Peers, cfg.ID) {
		return nil, errors.New("paxos: Peers must include ID")
	}
	if cfg.Rand == nil {
		return nil, errors.New("paxos: Rand is required")
	}
	if cfg.Timing == (Timing{}) {
		cfg.Timing = DefaultTiming
	}
	peers := slices.Clone(cfg.Peers)
	slices.Sort(peers)
	acc, err := newAcceptor(cfg.ID, peers, store, tr)
	if err != nil {
		return nil, err
	}
	prop, err := newProposer(cfg.ID, peers, store, tr, cfg.Rand, cfg.Timing)
	if err != nil {
		return nil, err
	}
	return &Node{id: cfg.ID, proposer: prop, acceptor: acc, learner: newLearner(len(peers)/2 + 1)}, nil
}

// ID returns the node's ID.
func (n *Node) ID() NodeID { return n.id }

// Propose asks this node's proposer to get v chosen. The value that is
// eventually decided may be another node's value.
func (n *Node) Propose(v Value) error {
	if _, ok := n.learner.Decided(); ok {
		return ErrDecided
	}
	return n.proposer.propose(v)
}

// Decided returns the value this node has learned, if any.
func (n *Node) Decided() (Value, bool) { return n.learner.Decided() }

// AcceptorState exposes the acceptor's state for inspection.
func (n *Node) AcceptorState() AcceptorState { return n.acceptor.State() }

// Handle processes one incoming message.
func (n *Node) Handle(m Message) {
	switch b := m.Body.(type) {
	case Prepare:
		n.acceptor.handlePrepare(m.From, b)
	case Accept:
		n.acceptor.handleAccept(m.From, b)
	case Promise:
		n.proposer.handlePromise(m.From, b)
	case Nack:
		n.proposer.handleNack(b)
	case Accepted:
		n.proposer.observe(b.Ballot)
		if n.learner.handleAccepted(m.From, b) {
			n.proposer.stop()
		}
	}
}

// Tick advances the node's logical clock by one step.
func (n *Node) Tick() { n.proposer.tick() }

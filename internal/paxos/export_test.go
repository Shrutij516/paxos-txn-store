package paxos

// Hooks for the negative tests. This file is compiled only by "go test", so
// the broken modes can never be switched on in a real binary.

// PromiseLowerBallots makes the acceptor promise, and record, a ballot lower
// than the one it already promised, instead of answering with a Nack.
func (n *Node) PromiseLowerBallots() { n.acceptor.promiseLower = true }

// OmitAcceptedFromPromise makes the acceptor leave its previously accepted
// proposal out of every Promise reply.
func (n *Node) OmitAcceptedFromPromise() { n.acceptor.omitAccepted = true }

// DisableAcceptRule makes the acceptor accept every Accept regardless of
// what it has promised.
func (n *Node) DisableAcceptRule() { n.acceptor.skipAcceptRule = true }

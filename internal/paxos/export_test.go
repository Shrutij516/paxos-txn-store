package paxos

// Hooks for the negative tests. This file is compiled only by "go test", so
// the broken modes can never be switched on in a real binary.

// DisablePromiseRule makes the acceptor answer every Prepare with an empty
// Promise without recording it.
func (n *Node) DisablePromiseRule() { n.acceptor.skipPromiseRule = true }

// DisableAcceptRule makes the acceptor accept every Accept regardless of
// what it has promised.
func (n *Node) DisableAcceptRule() { n.acceptor.skipAcceptRule = true }

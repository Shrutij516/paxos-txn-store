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

// SkipPrepareOnTakeover makes the replica start leading right after its
// election timeout, without running Prepare.
func (r *Replica) SkipPrepareOnTakeover() { r.skipPrepare = true }

// IgnorePromisedValues makes a new leader disregard the accepted entries
// reported in promises, as if every slot above its commit index were empty.
func (r *Replica) IgnorePromisedValues() { r.ignorePromised = true }

// TakeoverStats reports how often this replica, on becoming leader, filled
// a gap with a no-op, re-proposed a value reported in promises, and had to
// pick between different entries reported for the same slot.
func (r *Replica) TakeoverStats() (noops, recovered, contested int) {
	return r.statNoops, r.statRecovered, r.statContested
}

// ReplyBeforePersist makes the replica send Promise and Accepted replies
// before writing the state they vouch for.
func (r *Replica) ReplyBeforePersist() { r.replyFirst = true }

// DisableLeaderStickiness turns off PreVote and the refusal of Prepares
// while a leader is live, as before Phase 8b.
func (r *Replica) DisableLeaderStickiness() { r.noSticky = true }

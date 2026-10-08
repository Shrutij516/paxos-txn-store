package paxos

// Multi-Paxos types. A Replica keeps a log of numbered slots, starting at 1.
// One stable leader runs Prepare once for every slot above its commit index
// and then sends one Accept per slot. See docs/multipaxos.md.

// Entry is one log record: a client command or a no-op that fills a gap.
type Entry struct {
	Noop     bool
	ClientID uint64
	Seq      uint64
	Cmd      Value
}

// SlotEntry is an entry accepted at Slot in Ballot. In catch-up replies the
// Ballot is zero because the entry is already known to be chosen.
type SlotEntry struct {
	Slot   uint64
	Ballot Ballot
	Entry  Entry
}

// StateMachine consumes committed entries strictly in slot order. It must
// be deterministic: the same entries must produce the same results on every
// replica. It also sees no-ops so it can observe every slot.
type StateMachine interface {
	Apply(slot uint64, e Entry) Value
}

// LogStorage is stable storage for a Multi-Paxos replica. The round methods
// are the same as in Storage, so one implementation can serve both.
type LogStorage interface {
	LoadRound() (uint64, error)
	SaveRound(uint64) error
	LoadPromised() (Ballot, error)
	SavePromised(Ballot) error
	// LoadAccepted returns every accepted entry, sorted by slot.
	LoadAccepted() ([]SlotEntry, error)
	// SaveAccept durably stores e as the accepted entry for its slot and
	// sets the promise to promised, in one atomic write. promised is the
	// acceptor's promise after accepting e: at least e.Ballot and at least
	// the previous promise.
	SaveAccept(promised Ballot, e SlotEntry) error
	// LoadCommitted returns the committed prefix of the log, slots 1..n in
	// order, where n is the stored commit index.
	LoadCommitted() ([]SlotEntry, error)
	// AppendCommitted durably appends entries that extend the committed
	// prefix (the first must be at commit index + 1, the rest contiguous)
	// and advances the commit index to the last one, all in one atomic step.
	AppendCommitted([]SlotEntry) error
}

// LogPrepare asks acceptors to promise Ballot for every slot above Commit,
// the sender's commit index.
type LogPrepare struct {
	Ballot Ballot
	Commit uint64
}

// LogPromise carries every entry the acceptor accepted above the Commit in
// the matching LogPrepare.
type LogPromise struct {
	Ballot  Ballot
	Entries []SlotEntry
}

// LogAccept asks acceptors to accept Entry at Slot in Ballot.
type LogAccept struct {
	Ballot Ballot
	Slot   uint64
	Entry  Entry
}

// LogAccepted is the acceptor's vote, sent back to the leader.
type LogAccepted struct {
	Ballot Ballot
	Slot   uint64
	Entry  Entry
}

// LogNack rejects Ballot because the acceptor promised Promised.
type LogNack struct {
	Ballot   Ballot
	Promised Ballot
}

// Heartbeat asserts leadership and carries the leader's commit index.
type Heartbeat struct {
	Ballot Ballot
	Commit uint64
}

// PreVote asks whether the receiver would vote for a candidate at Ballot
// if it ran a real election now. Answering changes no state on the
// receiver: nothing is promised or persisted.
type PreVote struct{ Ballot Ballot }

// PreVoteReply answers a PreVote for Ballot. Granted is false while the
// receiver hears from a live leader. Promised is the receiver's promise,
// so a candidate that goes ahead picks a ballot above it.
type PreVoteReply struct {
	Ballot   Ballot
	Granted  bool
	Promised Ballot
}

// HeartbeatAck tells the leader at Ballot that a follower accepted its
// heartbeat. Leaders use it for CheckQuorum.
type HeartbeatAck struct{ Ballot Ballot }

// CatchupRequest asks for committed entries starting at From.
type CatchupRequest struct{ From uint64 }

// CatchupReply carries committed entries in slot order.
type CatchupReply struct{ Entries []SlotEntry }

// ClientRequest submits Cmd. ClientID and Seq identify the request so a
// retry is recognized and applied at most once.
type ClientRequest struct {
	ClientID uint64
	Seq      uint64
	Cmd      Value
}

// ClientReply answers a ClientRequest. If OK is false the replica is not
// the leader and Leader names the leader it knows of, or 0 if none.
type ClientReply struct {
	ClientID uint64
	Seq      uint64
	OK       bool
	Result   Value
	Leader   NodeID
}

func (LogPrepare) isPayload()     {}
func (LogPromise) isPayload()     {}
func (LogAccept) isPayload()      {}
func (LogAccepted) isPayload()    {}
func (LogNack) isPayload()        {}
func (Heartbeat) isPayload()      {}
func (PreVote) isPayload()        {}
func (PreVoteReply) isPayload()   {}
func (HeartbeatAck) isPayload()   {}
func (CatchupRequest) isPayload() {}
func (CatchupReply) isPayload()   {}
func (ClientRequest) isPayload()  {}
func (ClientReply) isPayload()    {}

// LogTiming holds replica timeouts in ticks.
type LogTiming struct {
	HeartbeatEvery int // leader sends heartbeats (and retransmits) this often
	ElectionMin    int // follower election timeout is drawn from
	ElectionMax    int // [ElectionMin, ElectionMax]
	CatchupBatch   int // max entries per CatchupReply
	// ElectionBackoff caps the election backoff: each time a replica's own
	// election times out, its next timeout range is doubled, up to
	// ElectionBackoff times [ElectionMin, ElectionMax]. Hearing from a
	// leader resets it. Values < 1 mean DefaultElectionBackoff.
	ElectionBackoff int
}

// DefaultElectionBackoff is the default cap on the election backoff.
const DefaultElectionBackoff = 8

// DefaultLogTiming suits the simulator's default delays.
var DefaultLogTiming = LogTiming{HeartbeatEvery: 4, ElectionMin: 20, ElectionMax: 40, CatchupBatch: 64,
	ElectionBackoff: DefaultElectionBackoff}

// ReplicaConfig describes one replica.
type ReplicaConfig struct {
	ID           NodeID
	Peers        []NodeID // every replica, including ID
	Rand         Rand
	Timing       LogTiming // zero value means DefaultLogTiming
	StateMachine StateMachine
	// Observer receives the replica's events; nil means none.
	Observer Observer
}

// Observer receives a Replica's events so the layer above can turn them
// into metrics and traces. The replica calls it synchronously from Handle
// and Tick. It must not call back into the replica, and nothing it does
// affects the replica, so a replica behaves the same with or without one
// (the simulator runs without). Any wall-clock timing is the observer's
// business.
type Observer interface {
	// BecameLeader: this replica won an election at ballot b.
	BecameLeader(b Ballot)
	// SteppedDown: this replica stopped leading because it saw a higher
	// ballot.
	SteppedDown(b Ballot)
	// Proposed: as leader, this replica proposed e at slot (a new entry,
	// or one re-proposed while taking over). Retransmissions do not count.
	Proposed(slot uint64, e Entry)
	// Applied: e was committed at slot and applied to the state machine.
	Applied(slot uint64, e Entry)
}

// NoObserver ignores every event.
type NoObserver struct{}

// BecameLeader does nothing.
func (NoObserver) BecameLeader(Ballot) {}

// SteppedDown does nothing.
func (NoObserver) SteppedDown(Ballot) {}

// Proposed does nothing.
func (NoObserver) Proposed(uint64, Entry) {}

// Applied does nothing.
func (NoObserver) Applied(uint64, Entry) {}

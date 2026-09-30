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
	SaveAccepted(SlotEntry) error
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
}

// DefaultLogTiming suits the simulator's default delays.
var DefaultLogTiming = LogTiming{HeartbeatEvery: 4, ElectionMin: 20, ElectionMax: 40, CatchupBatch: 64}

// ReplicaConfig describes one replica.
type ReplicaConfig struct {
	ID           NodeID
	Peers        []NodeID // every replica, including ID
	Rand         Rand
	Timing       LogTiming // zero value means DefaultLogTiming
	StateMachine StateMachine
}

package txn

// AbortReason says why a transaction was aborted. It is a small fixed set,
// so the server can count aborts per reason.
type AbortReason string

// Abort reasons.
const (
	// AbortWounded: an older transaction needed its locks (wound-wait).
	AbortWounded AbortReason = "wounded"
	// AbortLockLost: a read lock was gone at commit time (lease expiry or
	// a leader change), so the read may be stale.
	AbortLockLost AbortReason = "lock_lost"
	// AbortValidation: the state machine rejected a one-phase record (a
	// read version changed or a prepared transaction conflicts).
	AbortValidation AbortReason = "validation"
	// AbortVoteNo: a participant voted no.
	AbortVoteNo AbortReason = "vote_no"
	// AbortTimeout: the coordinator gave up waiting for votes.
	AbortTimeout AbortReason = "timeout"
	// AbortPresumed: a participant asked for the outcome and nobody was
	// coordinating the transaction (presumed abort).
	AbortPresumed AbortReason = "presumed"
	// AbortUnknown: the abort was decided under an earlier leader.
	AbortUnknown AbortReason = "unknown"
)

// AbortReasons lists every reason.
var AbortReasons = []AbortReason{AbortWounded, AbortLockLost, AbortValidation, AbortVoteNo, AbortTimeout, AbortPresumed, AbortUnknown}

// Observer receives a shard leader's transaction events so the server can
// turn them into metrics and trace spans. The Server calls it synchronously
// and only while leading. Like paxos.Observer it only watches: it must not
// call back into the Server, and the Server behaves the same without one
// (the simulator runs without). Timing is the observer's business.
type Observer interface {
	// Finished: the outcome of a transaction this shard decides was
	// applied here: a one-phase record, a logged one-phase abort, or a
	// coordinator decision (crossShard). reason is set for aborts.
	Finished(id ID, commit, crossShard bool, reason AbortReason)
	// CoordinationStarted: as coordinator, the shard began collecting
	// votes for a cross-shard transaction.
	CoordinationStarted(id ID)
	// DecisionProposed: the coordinator proposed its decision.
	DecisionProposed(id ID, commit bool)
	// PrepareProposed and PrepareApplied: as participant, the shard
	// proposed a prepare record, and it was applied (ok is the vote).
	PrepareProposed(id ID)
	PrepareApplied(id ID, ok bool)
	// QuerySent: a prepared participant asked the coordinator shard for
	// the outcome.
	QuerySent(id ID)
	// Resolved: as participant, the shard applied the transaction's
	// commit or abort record; it is no longer in doubt here.
	Resolved(id ID, commit bool)
	// Wounded: the transaction lost its locks to an older one.
	Wounded(id ID)
	// LockWait: a lock request of the transaction had to wait.
	LockWait(id ID)
}

// NoObserver ignores every event.
type NoObserver struct{}

// Finished does nothing.
func (NoObserver) Finished(ID, bool, bool, AbortReason) {}

// CoordinationStarted does nothing.
func (NoObserver) CoordinationStarted(ID) {}

// DecisionProposed does nothing.
func (NoObserver) DecisionProposed(ID, bool) {}

// PrepareProposed does nothing.
func (NoObserver) PrepareProposed(ID) {}

// PrepareApplied does nothing.
func (NoObserver) PrepareApplied(ID, bool) {}

// QuerySent does nothing.
func (NoObserver) QuerySent(ID) {}

// Resolved does nothing.
func (NoObserver) Resolved(ID, bool) {}

// Wounded does nothing.
func (NoObserver) Wounded(ID) {}

// LockWait does nothing.
func (NoObserver) LockWait(ID) {}

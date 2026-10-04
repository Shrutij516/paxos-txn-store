package txn

import (
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
)

// Version is the committed value of a key. Ver is the log slot that wrote
// it (0 for the initial value), so versions of a key are totally ordered.
type Version struct {
	Value  string
	Ver    uint64
	Writer ID
}

// WriteRec is one committed write, in apply order, for history checkers.
type WriteRec struct {
	Key    string
	Ver    uint64
	Writer ID
}

// Event tells the shard's server what an applied record did.
type Event struct {
	Kind string
	Rec  Record
	OK   bool // prepare/onephase accepted, or decision/outcome is commit
}

// SM is a shard's replicated transaction state machine. Every replica of
// the shard applies the same records in the same order and so holds the
// same data, prepared transactions and outcomes. All validation that
// decides safety happens here, deterministically, at apply time.
type SM struct {
	data     map[string]Version
	prepared map[ID]*Record   // durable locks: reads and writes of prepared txns
	outcome  map[ID]bool      // final outcome at this shard
	decision map[ID]bool      // coordinator decisions recorded at this shard
	parts    map[ID][]ShardID // participants of each decided txn
	rejected map[ID]bool      // prepares refused by validation
	onePhase map[ID]bool      // single-shard txns (committed or not) seen here

	writes []WriteRec // committed writes in apply order
	// commitsWithoutPrepare counts commit records applied for a transaction
	// that never prepared here: its writes are lost. Correct 2PC never does
	// this; the atomicity checker treats it as a violation.
	commitsWithoutPrepare map[ID]bool
	events                []Event

	// noLockCheck disables the conflict check against prepared transactions
	// (negative test: participant releases locks at prepare).
	noLockCheck bool
}

var _ paxos.StateMachine = (*SM)(nil)

// NewSM returns a state machine holding the given initial values at
// version 0.
func NewSM(initial map[string]string) *SM {
	s := &SM{
		data:                  make(map[string]Version),
		prepared:              make(map[ID]*Record),
		outcome:               make(map[ID]bool),
		decision:              make(map[ID]bool),
		parts:                 make(map[ID][]ShardID),
		rejected:              make(map[ID]bool),
		onePhase:              make(map[ID]bool),
		commitsWithoutPrepare: make(map[ID]bool),
	}
	for k, v := range initial {
		s.data[k] = Version{Value: v}
	}
	return s
}

// Get returns the committed version of key.
func (s *SM) Get(key string) Version { return s.data[key] }

// Outcome returns the final outcome of txn at this shard, if known.
func (s *SM) Outcome(id ID) (commit, known bool) {
	commit, known = s.outcome[id]
	return
}

// Decided returns the coordinator decision for txn, if recorded here.
func (s *SM) Decided(id ID) (commit, known bool) {
	commit, known = s.decision[id]
	return
}

// Decisions returns every decided txn with its participants, sorted by ID.
func (s *SM) Decisions() []ID { return sortedTxns(s.decision) }

// DecidedParticipants returns the participants named in txn's decision.
func (s *SM) DecidedParticipants(id ID) []ShardID { return s.parts[id] }

// Outcomes returns every txn with a final outcome here, sorted by ID.
func (s *SM) Outcomes() []ID { return sortedTxns(s.outcome) }

// IsOnePhase reports whether txn was a single-shard txn at this shard.
func (s *SM) IsOnePhase(id ID) bool { return s.onePhase[id] }

// Prepared returns the prepared record of txn, or nil.
func (s *SM) Prepared(id ID) *Record { return s.prepared[id] }

// PreparedTxns returns the IDs of all prepared transactions, sorted.
func (s *SM) PreparedTxns() []ID { return sortedTxns(s.prepared) }

// Writes returns the committed writes in apply order.
func (s *SM) Writes() []WriteRec { return s.writes }

// CommitsWithoutPrepare returns transactions committed here without ever
// preparing (their writes are missing).
func (s *SM) CommitsWithoutPrepare() []ID { return sortedTxns(s.commitsWithoutPrepare) }

// Data returns a copy of all committed values.
func (s *SM) Data() map[string]Version {
	out := make(map[string]Version, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

func (s *SM) drain() []Event {
	ev := s.events
	s.events = nil
	return ev
}

// Apply implements paxos.StateMachine.
func (s *SM) Apply(slot uint64, e paxos.Entry) paxos.Value {
	if e.Noop {
		return ""
	}
	r, err := decode(e.Cmd)
	if err != nil {
		return ""
	}
	id := r.Txn.ID
	switch r.Kind {
	case KindPrepare:
		ok := s.prepare(&r)
		s.events = append(s.events, Event{Kind: KindPrepare, Rec: r, OK: ok})
	case KindOnePhase:
		ok, done := s.outcome[id]
		s.onePhase[id] = true
		if !done {
			ok = s.validate(&r)
			if ok {
				s.applyWrites(id, r.Writes, slot)
			}
			s.outcome[id] = ok
		}
		s.events = append(s.events, Event{Kind: KindOnePhase, Rec: r, OK: ok})
	case KindDecide:
		commit, done := s.decision[id]
		if !done {
			commit = r.Commit
			s.decision[id] = commit
			s.parts[id] = r.Participants
			s.finish(&r, commit, slot) // the coordinator is also a participant
		}
		s.events = append(s.events, Event{Kind: KindDecide, Rec: r, OK: commit})
	case KindCommit, KindAbort:
		s.finish(&r, r.Kind == KindCommit, slot)
		s.events = append(s.events, Event{Kind: r.Kind, Rec: r, OK: s.outcome[id]})
	}
	return ""
}

// prepare validates and records a prepare. Duplicates are idempotent.
func (s *SM) prepare(r *Record) bool {
	id := r.Txn.ID
	if commit, done := s.outcome[id]; done {
		return commit
	}
	if s.prepared[id] != nil {
		return true
	}
	if s.rejected[id] {
		return false
	}
	if !s.validate(r) {
		s.rejected[id] = true
		return false
	}
	cp := *r
	s.prepared[id] = &cp
	return true
}

// validate checks that every read is still the latest version and that the
// record does not conflict with another prepared transaction's locks:
// its writes against their reads and writes, its reads against their
// writes.
func (s *SM) validate(r *Record) bool {
	for _, k := range sortedKeys(r.Reads) {
		if s.data[k].Ver != r.Reads[k] {
			return false
		}
	}
	if s.noLockCheck {
		return true
	}
	for _, pid := range sortedTxns(s.prepared) {
		p := s.prepared[pid]
		if pid == r.Txn.ID {
			continue
		}
		for k := range r.Writes {
			if _, ok := p.Writes[k]; ok {
				return false
			}
			if _, ok := p.Reads[k]; ok {
				return false
			}
		}
		for k := range r.Reads {
			if _, ok := p.Writes[k]; ok {
				return false
			}
		}
	}
	return true
}

// finish records the outcome at this shard. Commit applies the prepared
// writes. A commit or decide record may carry writes itself (only in the
// negative test where prepares are not replicated); without a prepare and
// without writes, the commit has nothing to apply and is flagged.
func (s *SM) finish(r *Record, commit bool, slot uint64) {
	id := r.Txn.ID
	if _, done := s.outcome[id]; done {
		return
	}
	if commit {
		switch {
		case s.prepared[id] != nil:
			s.applyWrites(id, s.prepared[id].Writes, slot)
		case r.Writes != nil:
			s.applyWrites(id, r.Writes, slot)
		default:
			s.commitsWithoutPrepare[id] = true
		}
	}
	s.outcome[id] = commit
	delete(s.prepared, id)
}

func (s *SM) applyWrites(id ID, w map[string]string, slot uint64) {
	for _, k := range sortedKeys(w) {
		s.data[k] = Version{Value: w[k], Ver: slot, Writer: id}
		s.writes = append(s.writes, WriteRec{Key: k, Ver: slot, Writer: id})
	}
}

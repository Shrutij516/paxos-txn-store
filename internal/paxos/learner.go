package paxos

// Learner decides a value once a majority of acceptors report Accepted for
// the same ballot. Duplicate reports from one acceptor count once.
type Learner struct {
	quorum  int
	votes   map[Accepted]map[NodeID]struct{}
	decided bool
	value   Value
}

func newLearner(quorum int) *Learner {
	return &Learner{quorum: quorum, votes: make(map[Accepted]map[NodeID]struct{})}
}

// handleAccepted records a vote and reports whether this call caused the
// learner to decide.
func (l *Learner) handleAccepted(from NodeID, m Accepted) bool {
	if l.decided {
		return false
	}
	voters := l.votes[m]
	if voters == nil {
		voters = make(map[NodeID]struct{})
		l.votes[m] = voters
	}
	voters[from] = struct{}{}
	if len(voters) < l.quorum {
		return false
	}
	l.decided = true
	l.value = m.Value
	l.votes = nil
	return true
}

// Decided returns the decided value, if any.
func (l *Learner) Decided() (Value, bool) { return l.value, l.decided }

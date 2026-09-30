package paxos

// Acceptor implements the two acceptor rules. See docs/paxos.md.
//
// Promise rule: on Prepare(b), if b >= promised, set promised = b, persist,
// then reply Promise carrying the highest accepted proposal. Otherwise Nack.
//
// Accept rule: on Accept(b, v), if b >= promised, set promised = b and
// accepted = (b, v), persist, then broadcast Accepted. Otherwise Nack.
//
// State is always written to Storage before any reply leaves the node. If
// the write fails the acceptor stays silent and keeps its old state.
type Acceptor struct {
	id    NodeID
	peers []NodeID
	store Storage
	tr    Transport
	state AcceptorState

	// Test-only switches that break the rules on purpose. They are set only
	// from export_test.go so the negative tests can prove the checker works.
	skipPromiseRule bool
	skipAcceptRule  bool
}

func newAcceptor(id NodeID, peers []NodeID, store Storage, tr Transport) (*Acceptor, error) {
	st, err := store.LoadAcceptor()
	if err != nil {
		return nil, err
	}
	return &Acceptor{id: id, peers: peers, store: store, tr: tr, state: st}, nil
}

// State returns a copy of the acceptor's current state.
func (a *Acceptor) State() AcceptorState { return a.state }

func (a *Acceptor) handlePrepare(from NodeID, m Prepare) {
	if a.skipPromiseRule {
		// Broken on purpose: promise anything, remember nothing, and hide
		// what was accepted before.
		a.tr.Send(Message{From: a.id, To: from, Body: Promise{Ballot: m.Ballot}})
		return
	}
	if m.Ballot.Less(a.state.Promised) {
		a.tr.Send(Message{From: a.id, To: from, Body: Nack{Ballot: m.Ballot, Promised: a.state.Promised}})
		return
	}
	next := a.state
	next.Promised = m.Ballot
	if !a.persist(next) {
		return
	}
	a.tr.Send(Message{From: a.id, To: from, Body: Promise{
		Ballot:   m.Ballot,
		Accepted: a.state.Accepted,
		Value:    a.state.Value,
	}})
}

func (a *Acceptor) handleAccept(from NodeID, m Accept) {
	if !a.skipAcceptRule && m.Ballot.Less(a.state.Promised) {
		a.tr.Send(Message{From: a.id, To: from, Body: Nack{Ballot: m.Ballot, Promised: a.state.Promised}})
		return
	}
	next := a.state
	if a.state.Promised.Less(m.Ballot) {
		next.Promised = m.Ballot
	}
	next.Accepted = m.Ballot
	next.Value = m.Value
	if !a.persist(next) {
		return
	}
	for _, p := range a.peers {
		a.tr.Send(Message{From: a.id, To: p, Body: Accepted(m)})
	}
}

// persist writes next to storage and adopts it only if the write succeeded.
func (a *Acceptor) persist(next AcceptorState) bool {
	if next == a.state {
		return true
	}
	if err := a.store.SaveAcceptor(next); err != nil {
		return false
	}
	a.state = next
	return true
}

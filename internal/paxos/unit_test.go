package paxos_test

import (
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/storage"
)

// outbox is a Transport that just records what was sent.
type outbox struct{ sent []paxos.Message }

func (o *outbox) Send(m paxos.Message) { o.sent = append(o.sent, m) }

func (o *outbox) take() []paxos.Message {
	s := o.sent
	o.sent = nil
	return s
}

// flaky wraps Memory and fails writes on demand.
type flaky struct {
	*storage.Memory
	failAcceptor, failRound, failLoad bool
}

var errDisk = errors.New("disk on fire")

func (f *flaky) SaveAcceptor(s paxos.AcceptorState) error {
	if f.failAcceptor {
		return errDisk
	}
	return f.Memory.SaveAcceptor(s)
}

func (f *flaky) SaveRound(r uint64) error {
	if f.failRound {
		return errDisk
	}
	return f.Memory.SaveRound(r)
}

func (f *flaky) LoadAcceptor() (paxos.AcceptorState, error) {
	if f.failLoad {
		return paxos.AcceptorState{}, errDisk
	}
	return f.Memory.LoadAcceptor()
}

func (f *flaky) LoadRound() (uint64, error) {
	if f.failLoad {
		return 0, errDisk
	}
	return f.Memory.LoadRound()
}

var peers3 = []paxos.NodeID{1, 2, 3}

func b(r uint64, n paxos.NodeID) paxos.Ballot { return paxos.Ballot{Round: r, Node: n} }

func newTestNode(t *testing.T, id paxos.NodeID, st paxos.Storage, tr paxos.Transport) *paxos.Node {
	t.Helper()
	n, err := paxos.NewNode(paxos.Config{
		ID: id, Peers: peers3, Rand: rand.New(rand.NewPCG(1, 2)),
		Timing: paxos.Timing{PhaseTimeout: 3, BackoffBase: 1, BackoffMax: 4},
	}, st, tr)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestBallotOrder(t *testing.T) {
	cases := []struct {
		a, b paxos.Ballot
		less bool
	}{
		{b(1, 1), b(2, 1), true},
		{b(2, 1), b(1, 9), false},
		{b(1, 1), b(1, 2), true},
		{b(1, 2), b(1, 1), false},
		{b(1, 1), b(1, 1), false},
		{paxos.Ballot{}, b(1, 1), true},
	}
	for _, c := range cases {
		if got := c.a.Less(c.b); got != c.less {
			t.Errorf("%v.Less(%v) = %v, want %v", c.a, c.b, got, c.less)
		}
	}
	if s := b(3, 2).String(); s != "(3,2)" {
		t.Errorf("String = %q", s)
	}
}

func TestAcceptorPromiseRule(t *testing.T) {
	tr := &outbox{}
	st := storage.NewMemory()
	n := newTestNode(t, 1, st, tr)

	n.Handle(paxos.Message{From: 2, To: 1, Body: paxos.Prepare{Ballot: b(5, 2)}})
	got := tr.take()
	if len(got) != 1 || got[0].Body != (paxos.Promise{Ballot: b(5, 2)}) || got[0].To != 2 {
		t.Fatalf("want empty Promise to 2, got %v", got)
	}
	if s, _ := st.LoadAcceptor(); s.Promised != b(5, 2) {
		t.Fatalf("promise not persisted before reply: %+v", s)
	}

	// A lower ballot is refused with the ballot we promised.
	n.Handle(paxos.Message{From: 3, To: 1, Body: paxos.Prepare{Ballot: b(4, 3)}})
	got = tr.take()
	if len(got) != 1 || got[0].Body != (paxos.Nack{Ballot: b(4, 3), Promised: b(5, 2)}) {
		t.Fatalf("want Nack, got %v", got)
	}

	// A duplicate of the promised Prepare is answered again, idempotently.
	saves := st.Saves
	n.Handle(paxos.Message{From: 2, To: 1, Body: paxos.Prepare{Ballot: b(5, 2)}})
	if got = tr.take(); len(got) != 1 || st.Saves != saves {
		t.Fatalf("duplicate prepare: sent %v, saves %d->%d", got, saves, st.Saves)
	}
}

func TestAcceptorAcceptRule(t *testing.T) {
	tr := &outbox{}
	st := storage.NewMemory()
	n := newTestNode(t, 1, st, tr)
	n.Handle(paxos.Message{From: 2, To: 1, Body: paxos.Prepare{Ballot: b(5, 2)}})
	tr.take()

	// Below the promise: refused.
	n.Handle(paxos.Message{From: 3, To: 1, Body: paxos.Accept{Ballot: b(4, 3), Value: "old"}})
	if got := tr.take(); len(got) != 1 || got[0].Body != (paxos.Nack{Ballot: b(4, 3), Promised: b(5, 2)}) {
		t.Fatalf("want Nack, got %v", got)
	}

	// At or above the promise: accepted, persisted, broadcast to all peers.
	n.Handle(paxos.Message{From: 3, To: 1, Body: paxos.Accept{Ballot: b(6, 3), Value: "x"}})
	got := tr.take()
	if len(got) != 3 {
		t.Fatalf("want Accepted to 3 peers, got %v", got)
	}
	for i, m := range got {
		if m.To != peers3[i] || m.Body != (paxos.Accepted{Ballot: b(6, 3), Value: "x"}) {
			t.Fatalf("bad Accepted %v", m)
		}
	}
	want := paxos.AcceptorState{Promised: b(6, 3), Accepted: b(6, 3), Value: "x"}
	if s := n.AcceptorState(); s != want {
		t.Fatalf("state %+v, want %+v", s, want)
	}

	// A later Prepare reports what was accepted.
	n.Handle(paxos.Message{From: 2, To: 1, Body: paxos.Prepare{Ballot: b(7, 2)}})
	if got := tr.take(); got[0].Body != (paxos.Promise{Ballot: b(7, 2), Accepted: b(6, 3), Value: "x"}) {
		t.Fatalf("promise must carry accepted value, got %v", got)
	}

	// A restarted acceptor on the same storage remembers everything.
	n2 := newTestNode(t, 1, st, tr)
	if s := n2.AcceptorState(); s.Promised != b(7, 2) || s.Value != "x" {
		t.Fatalf("restart lost state: %+v", s)
	}
}

func TestAcceptorSilentWhenStorageFails(t *testing.T) {
	tr := &outbox{}
	st := &flaky{Memory: storage.NewMemory(), failAcceptor: true}
	n := newTestNode(t, 1, st, tr)
	n.Handle(paxos.Message{From: 2, To: 1, Body: paxos.Prepare{Ballot: b(1, 2)}})
	n.Handle(paxos.Message{From: 2, To: 1, Body: paxos.Accept{Ballot: b(1, 2), Value: "v"}})
	if got := tr.take(); len(got) != 0 {
		t.Fatalf("acceptor replied without persisting: %v", got)
	}
	if s := n.AcceptorState(); s != (paxos.AcceptorState{}) {
		t.Fatalf("state changed despite failed write: %+v", s)
	}
}

func TestNewNodeErrors(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 1))
	if _, err := paxos.NewNode(paxos.Config{ID: 9, Peers: peers3, Rand: r}, storage.NewMemory(), &outbox{}); err == nil {
		t.Error("want error when ID not in Peers")
	}
	if _, err := paxos.NewNode(paxos.Config{ID: 1, Peers: peers3}, storage.NewMemory(), &outbox{}); err == nil {
		t.Error("want error when Rand is nil")
	}
	bad := &flaky{Memory: storage.NewMemory(), failLoad: true}
	if _, err := paxos.NewNode(paxos.Config{ID: 1, Peers: peers3, Rand: r}, bad, &outbox{}); !errors.Is(err, errDisk) {
		t.Errorf("want load error, got %v", err)
	}
	n, err := paxos.NewNode(paxos.Config{ID: 2, Peers: []paxos.NodeID{3, 2, 1}, Rand: r}, storage.NewMemory(), &outbox{})
	if err != nil || n.ID() != 2 {
		t.Fatalf("NewNode = %v, %v", n, err)
	}
}

// drive plays a message through n and returns what n sent.
func drive(n *paxos.Node, tr *outbox, m paxos.Message) []paxos.Message {
	n.Handle(m)
	return tr.take()
}

func TestProposerLifecycle(t *testing.T) {
	tr := &outbox{}
	st := storage.NewMemory()
	n := newTestNode(t, 1, st, tr)

	if err := n.Propose("mine"); err != nil {
		t.Fatal(err)
	}
	if err := n.Propose("again"); !errors.Is(err, paxos.ErrBusy) {
		t.Fatalf("want ErrBusy, got %v", err)
	}
	prep := tr.take()
	if len(prep) != 3 || prep[0].Body != (paxos.Prepare{Ballot: b(1, 1)}) {
		t.Fatalf("want Prepare(1,1) x3, got %v", prep)
	}
	if r, _ := st.LoadRound(); r != 1 {
		t.Fatalf("round not persisted before Prepare: %d", r)
	}

	// Stale and duplicate promises do not count twice.
	drive(n, tr, paxos.Message{From: 2, To: 1, Body: paxos.Promise{Ballot: b(9, 9)}})
	drive(n, tr, paxos.Message{From: 2, To: 1, Body: paxos.Promise{Ballot: b(1, 1)}})
	if got := drive(n, tr, paxos.Message{From: 2, To: 1, Body: paxos.Promise{Ballot: b(1, 1)}}); len(got) != 0 {
		t.Fatalf("quorum reached on duplicate promise: %v", got)
	}
	// Node 3 reports an earlier accepted value; the proposer must adopt it.
	acc := drive(n, tr, paxos.Message{From: 3, To: 1, Body: paxos.Promise{Ballot: b(1, 1), Accepted: b(0, 3), Value: "theirs"}})
	if len(acc) != 3 || acc[0].Body != (paxos.Accept{Ballot: b(1, 1), Value: "theirs"}) {
		t.Fatalf("want Accept of adopted value, got %v", acc)
	}

	// Two Accepted votes make a majority; the node decides and stops.
	drive(n, tr, paxos.Message{From: 2, To: 1, Body: paxos.Accepted{Ballot: b(1, 1), Value: "theirs"}})
	drive(n, tr, paxos.Message{From: 2, To: 1, Body: paxos.Accepted{Ballot: b(1, 1), Value: "theirs"}})
	if _, ok := n.Decided(); ok {
		t.Fatal("duplicate vote counted twice")
	}
	drive(n, tr, paxos.Message{From: 3, To: 1, Body: paxos.Accepted{Ballot: b(1, 1), Value: "theirs"}})
	if v, ok := n.Decided(); !ok || v != "theirs" {
		t.Fatalf("Decided = %q, %v", v, ok)
	}
	drive(n, tr, paxos.Message{From: 1, To: 1, Body: paxos.Accepted{Ballot: b(1, 1), Value: "theirs"}})
	if err := n.Propose("late"); !errors.Is(err, paxos.ErrDecided) {
		t.Fatalf("want ErrDecided, got %v", err)
	}
	for range 50 {
		n.Tick()
	}
	if got := tr.take(); len(got) != 0 {
		t.Fatalf("decided proposer kept sending: %v", got)
	}
}

func TestProposerNackAndTimeoutRetryHigher(t *testing.T) {
	tr := &outbox{}
	n := newTestNode(t, 1, storage.NewMemory(), tr)
	_ = n.Propose("v")
	tr.take()

	// A Nack for an old ballot is ignored except for its round hint.
	drive(n, tr, paxos.Message{From: 2, To: 1, Body: paxos.Nack{Ballot: b(0, 1), Promised: b(4, 2)}})
	// A Nack for the current ballot triggers backoff, then a higher ballot.
	drive(n, tr, paxos.Message{From: 2, To: 1, Body: paxos.Nack{Ballot: b(1, 1), Promised: b(7, 3)}})
	var prep []paxos.Message
	for i := 0; i < 100 && len(prep) == 0; i++ {
		n.Tick()
		prep = tr.take()
	}
	if len(prep) == 0 || prep[0].Body != (paxos.Prepare{Ballot: b(8, 1)}) {
		t.Fatalf("want retry at (8,1), got %v", prep)
	}

	// No replies at all: the phase times out and a still higher ballot follows.
	for i := 0; i < 100; i++ {
		n.Tick()
		if got := tr.take(); len(got) > 0 {
			if got[0].Body != (paxos.Prepare{Ballot: b(9, 1)}) {
				t.Fatalf("want (9,1) after timeout, got %v", got)
			}
			return
		}
	}
	t.Fatal("no retry after timeout")
}

func TestProposerRoundSurvivesRestartAndStorageFailure(t *testing.T) {
	tr := &outbox{}
	st := &flaky{Memory: storage.NewMemory()}
	n := newTestNode(t, 1, st, tr)
	_ = n.Propose("v")
	tr.take()

	// Restart: the next ballot must be above the persisted round.
	n = newTestNode(t, 1, st, tr)
	_ = n.Propose("v")
	if got := tr.take(); got[0].Body != (paxos.Prepare{Ballot: b(2, 1)}) {
		t.Fatalf("restart reused a ballot: %v", got)
	}

	// If the round cannot be persisted, no Prepare may be sent.
	n = newTestNode(t, 1, st, tr)
	st.failRound = true
	_ = n.Propose("v")
	for range 30 {
		n.Tick()
	}
	if got := tr.take(); len(got) != 0 {
		t.Fatalf("sent Prepare without persisting round: %v", got)
	}
	st.failRound = false
	for range 30 {
		n.Tick()
	}
	if got := tr.take(); len(got) == 0 || got[0].Body != (paxos.Prepare{Ballot: b(3, 1)}) {
		t.Fatalf("want (3,1) once storage recovers, got %v", got)
	}
}

func TestProposerAcceptPhaseNack(t *testing.T) {
	tr := &outbox{}
	n := newTestNode(t, 1, storage.NewMemory(), tr)
	_ = n.Propose("v")
	tr.take()
	drive(n, tr, paxos.Message{From: 1, To: 1, Body: paxos.Promise{Ballot: b(1, 1)}})
	if got := drive(n, tr, paxos.Message{From: 2, To: 1, Body: paxos.Promise{Ballot: b(1, 1)}}); len(got) != 3 {
		t.Fatalf("want Accept broadcast, got %v", got)
	}
	drive(n, tr, paxos.Message{From: 3, To: 1, Body: paxos.Nack{Ballot: b(1, 1), Promised: b(2, 3)}})
	for i := 0; i < 100; i++ {
		n.Tick()
		if got := tr.take(); len(got) > 0 {
			if got[0].Body != (paxos.Prepare{Ballot: b(3, 1)}) {
				t.Fatalf("want (3,1), got %v", got)
			}
			return
		}
	}
	t.Fatal("no retry after accept-phase Nack")
}

func TestMessageString(t *testing.T) {
	m := paxos.Message{From: 1, To: 2, Body: paxos.Prepare{Ballot: b(3, 1)}}
	if s := m.String(); s != "1->2 paxos.Prepare{Ballot:(3,1)}" {
		t.Fatalf("String = %q", s)
	}
}

// The learner needs a majority of Accepted for the same ballot. Votes for
// the same value spread over different ballots must not add up.
func TestLearnerCountsPerBallotNotPerValue(t *testing.T) {
	tr := &outbox{}
	n := newTestNode(t, 1, storage.NewMemory(), tr)
	drive(n, tr, paxos.Message{From: 2, To: 1, Body: paxos.Accepted{Ballot: b(1, 2), Value: "v"}})
	drive(n, tr, paxos.Message{From: 3, To: 1, Body: paxos.Accepted{Ballot: b(2, 3), Value: "v"}})
	if v, ok := n.Decided(); ok {
		t.Fatalf("decided %q from votes in two different ballots", v)
	}
	drive(n, tr, paxos.Message{From: 1, To: 1, Body: paxos.Accepted{Ballot: b(2, 3), Value: "v"}})
	if v, ok := n.Decided(); !ok || v != "v" {
		t.Fatalf("majority in ballot (2,3) not decided: %q, %v", v, ok)
	}
}

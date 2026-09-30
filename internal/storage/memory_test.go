package storage

import (
	"testing"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
)

func TestMemoryRoundTrip(t *testing.T) {
	m := NewMemory()
	st := paxos.AcceptorState{
		Promised: paxos.Ballot{Round: 3, Node: 1},
		Accepted: paxos.Ballot{Round: 2, Node: 2},
		Value:    "x",
	}
	if err := m.SaveAcceptor(st); err != nil {
		t.Fatal(err)
	}
	if err := m.SaveRound(7); err != nil {
		t.Fatal(err)
	}
	got, err := m.LoadAcceptor()
	if err != nil || got != st {
		t.Fatalf("LoadAcceptor = %+v, %v; want %+v", got, err, st)
	}
	r, err := m.LoadRound()
	if err != nil || r != 7 {
		t.Fatalf("LoadRound = %d, %v; want 7", r, err)
	}
	m.ForgetAcceptor()
	got, _ = m.LoadAcceptor()
	if got != (paxos.AcceptorState{}) {
		t.Fatalf("after ForgetAcceptor got %+v", got)
	}
	if r, _ := m.LoadRound(); r != 7 {
		t.Fatalf("ForgetAcceptor must keep round, got %d", r)
	}
	if m.Saves != 2 {
		t.Fatalf("Saves = %d, want 2", m.Saves)
	}
}

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

func TestMemoryLog(t *testing.T) {
	m := NewMemory()
	b := paxos.Ballot{Round: 2, Node: 1}
	if err := m.SavePromised(b); err != nil {
		t.Fatal(err)
	}
	for _, s := range []uint64{3, 1, 2} {
		if err := m.SaveAccepted(paxos.SlotEntry{Slot: s, Ballot: b, Entry: paxos.Entry{Seq: s}}); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := m.LoadPromised(); got != b {
		t.Fatalf("LoadPromised = %v", got)
	}
	got, _ := m.LoadAccepted()
	if len(got) != 3 {
		t.Fatalf("LoadAccepted = %v", got)
	}
	for i, se := range got {
		if se.Slot != uint64(i+1) || se.Entry.Seq != se.Slot {
			t.Fatalf("LoadAccepted not sorted by slot: %v", got)
		}
	}
}

package transport

import (
	"testing"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
)

type recorder struct{ got []paxos.Message }

func (r *recorder) handle(m paxos.Message) { r.got = append(r.got, m) }

func msg(from, to paxos.NodeID, round uint64) paxos.Message {
	return paxos.Message{From: from, To: to, Body: paxos.Prepare{Ballot: paxos.Ballot{Round: round, Node: from}}}
}

func setup(seed uint64, f Faults) (*SimNet, map[paxos.NodeID]*recorder) {
	n := NewSimNet(seed)
	n.SetFaults(f)
	recs := map[paxos.NodeID]*recorder{}
	for id := paxos.NodeID(1); id <= 3; id++ {
		r := &recorder{}
		recs[id] = r
		n.Register(id, r.handle)
	}
	return n, recs
}

func TestDeliverAfterStepOnly(t *testing.T) {
	n, recs := setup(1, Faults{})
	var seen []paxos.Message
	n.OnSend = func(m paxos.Message) { seen = append(seen, m) }
	n.Send(msg(1, 2, 1))
	if len(recs[2].got) != 0 || n.Pending() != 1 || len(seen) != 1 {
		t.Fatal("message delivered before Step")
	}
	n.Step()
	if len(recs[2].got) != 1 || n.Now() != 1 {
		t.Fatalf("not delivered after one step: %v", recs[2].got)
	}
}

func TestDropAndDuplicate(t *testing.T) {
	n, recs := setup(1, Faults{DropProb: 1})
	n.Send(msg(1, 2, 1))
	n.Step()
	if len(recs[2].got) != 0 {
		t.Fatal("DropProb=1 delivered a message")
	}
	n.SetFaults(Faults{DupProb: 1})
	n.Send(msg(1, 2, 1))
	n.Step()
	if len(recs[2].got) != 2 {
		t.Fatalf("DupProb=1 delivered %d copies", len(recs[2].got))
	}
}

func TestDelayAndReorder(t *testing.T) {
	n, recs := setup(3, Faults{MaxDelay: 10})
	for i := uint64(1); i <= 50; i++ {
		n.Send(msg(1, 2, i))
	}
	for range 10 {
		n.Step()
	}
	got := recs[2].got
	if len(got) != 50 || n.Pending() != 0 {
		t.Fatalf("delivered %d of 50 within MaxDelay, %d pending", len(got), n.Pending())
	}
	inOrder := true
	for i := range got {
		if got[i].Body.(paxos.Prepare).Ballot.Round != uint64(i+1) {
			inOrder = false
		}
	}
	if inOrder {
		t.Fatal("50 delayed messages arrived in send order; no reordering happened")
	}
}

func TestPartitionAndHeal(t *testing.T) {
	n, recs := setup(1, Faults{})
	n.Partition([]paxos.NodeID{1}, []paxos.NodeID{2, 3})
	n.Send(msg(1, 2, 1))
	n.Send(msg(2, 3, 1))
	n.Step()
	if len(recs[2].got) != 0 || len(recs[3].got) != 1 {
		t.Fatal("partition not enforced")
	}
	n.Heal()
	n.Send(msg(1, 2, 2))
	n.Step()
	if len(recs[2].got) != 1 {
		t.Fatal("heal did not restore connectivity")
	}
}

func TestCrashAndRestart(t *testing.T) {
	n, recs := setup(1, Faults{})
	n.Send(msg(1, 2, 1)) // in flight when 1 crashes: still arrives
	n.Crash(1)
	n.Send(msg(1, 2, 2)) // sent by a dead node: never leaves
	n.Send(msg(2, 1, 1)) // to a dead node: lost
	n.Step()
	if len(recs[2].got) != 1 || len(recs[1].got) != 0 {
		t.Fatalf("crash semantics wrong: to2=%v to1=%v", recs[2].got, recs[1].got)
	}
	r := &recorder{}
	n.Register(1, r.handle)
	n.Send(msg(2, 1, 2))
	n.Step()
	if len(r.got) != 1 {
		t.Fatal("restarted node did not receive")
	}
	n.Send(msg(1, 9, 1)) // unknown destination
	n.Step()
}

func TestSameSeedSameSchedule(t *testing.T) {
	run := func(seed uint64) string {
		n, _ := setup(seed, Faults{DropProb: 0.2, DupProb: 0.2, MaxDelay: 5})
		var trace string
		n.Trace = func(s string) { trace += s + "\n" }
		n.Partition([]paxos.NodeID{1, 2}, []paxos.NodeID{3})
		for i := uint64(1); i <= 40; i++ {
			n.Send(msg(paxos.NodeID(1+i%3), paxos.NodeID(1+(i+1)%3), i))
			n.Step()
		}
		n.Heal()
		for range 10 {
			n.Step()
		}
		return trace
	}
	a, b, c := run(5), run(5), run(6)
	if a != b {
		t.Fatal("same seed produced different traces")
	}
	if a == c {
		t.Fatal("different seeds produced the same trace")
	}
}

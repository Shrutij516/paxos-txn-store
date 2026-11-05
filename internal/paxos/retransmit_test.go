package paxos_test

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/Shrutij516/paxos-txn-store/internal/kv"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/storage"
)

// Retransmission bounds of these tests (DECISIONS.md entry 45).
const (
	rtAfter = 4 // ticks before a peer's Accept may be resent
	rtMax   = 3 // Accepts resent to one peer per heartbeat
	rtSlots = 50
)

// newRetransmitLeader returns a leader of replicas 1..3 with rtSlots
// proposals whose Accepts were all lost: nobody has voted for anything.
// CheckQuorum is off, so it keeps leading without heartbeat acks.
func newRetransmitLeader(t *testing.T, all bool) (*paxos.Replica, *outbox) {
	t.Helper()
	tr := &outbox{}
	r, err := paxos.NewReplica(paxos.ReplicaConfig{
		ID: 1, Peers: peers3, Rand: rand.New(rand.NewPCG(1, 2)), StateMachine: kv.New(), NoCheckQuorum: true,
		Timing: paxos.LogTiming{HeartbeatEvery: 2, ElectionMin: 1, ElectionMax: 1, CatchupBatch: 2,
			RetransmitAfter: rtAfter, RetransmitMax: rtMax},
	}, storage.NewMemory(), tr)
	if err != nil {
		t.Fatal(err)
	}
	if all {
		r.RetransmitAll()
	}
	campaign(t, r, tr)
	tr.take()
	for _, from := range []paxos.NodeID{1, 2} {
		r.Handle(paxos.Message{From: from, To: 1, Body: paxos.LogPromise{Ballot: r.Ballot()}})
	}
	if !r.IsLeader() {
		t.Fatal("not leader")
	}
	for i := 0; i < rtSlots; i++ {
		r.Handle(paxos.Message{From: 100, To: 1, Body: paxos.ClientRequest{ClientID: uint64(i + 1), Seq: 1, Cmd: kv.Put("k", fmt.Sprint(i))}})
	}
	tr.take() // the first Accepts, all lost
	return r, tr
}

// checkRetransmit ticks a leader for steps ticks and checks the bounds:
// per peer and tick at most rtMax Accepts, the same slot to the same peer
// at most once per rtAfter ticks. It returns every (peer, slot) resent.
func checkRetransmit(r *paxos.Replica, tr *outbox, steps int) (map[paxos.NodeID]map[uint64]bool, error) {
	last := map[paxos.NodeID]map[uint64]int{}
	resent := map[paxos.NodeID]map[uint64]bool{}
	for tick := 1; tick <= steps; tick++ {
		r.Tick()
		perPeer := map[paxos.NodeID]int{}
		for _, m := range tr.take() {
			acc, ok := m.Body.(paxos.LogAccept)
			if !ok {
				continue
			}
			perPeer[m.To]++
			if last[m.To] == nil {
				last[m.To], resent[m.To] = map[uint64]int{}, map[uint64]bool{}
			}
			if t, ok := last[m.To][acc.Slot]; ok && tick-t < rtAfter {
				return resent, fmt.Errorf("tick %d: slot %d resent to %d after %d ticks, want at least %d", tick, acc.Slot, m.To, tick-t, rtAfter)
			}
			last[m.To][acc.Slot], resent[m.To][acc.Slot] = tick, true
		}
		for to, n := range perPeer {
			if n > rtMax {
				return resent, fmt.Errorf("tick %d: %d Accepts to %d in one heartbeat, want at most %d", tick, n, to, rtMax)
			}
		}
	}
	return resent, nil
}

// A leader whose Accepts are lost resends them within the bounds, least recently
// sent first, and eventually resends every slot to every peer (liveness). A
// peer that has voted for a slot is not sent it again.
func TestReplicaRetransmitBounded(t *testing.T) {
	r, tr := newRetransmitLeader(t, false)
	// Peer 3 votes for every slot; one vote is short of a majority with
	// the leader's own vote missing, so nothing is chosen yet.
	for s := uint64(1); s <= rtSlots; s++ {
		r.Handle(paxos.Message{From: 3, To: 1, Body: paxos.LogAccepted{Ballot: r.Ballot(), Slot: s}})
	}
	if r.Commit() != 0 {
		t.Fatalf("commit %d with one vote", r.Commit())
	}
	// Enough heartbeats for every slot: rtSlots/rtMax rounds of resends,
	// each round at most rtAfter ticks apart.
	steps := (rtSlots/rtMax + 2) * rtAfter * 2
	resent, err := checkRetransmit(r, tr, steps)
	if err != nil {
		t.Fatal(err)
	}
	for _, to := range []paxos.NodeID{1, 2} {
		if len(resent[to]) != rtSlots {
			t.Fatalf("peer %d: %d of %d slots resent within %d ticks", to, len(resent[to]), rtSlots, steps)
		}
	}
	if len(resent[3]) != 0 {
		t.Fatalf("resent %d slots to peer 3, which voted for all of them", len(resent[3]))
	}
	if n := r.Unvoted(3); n != 0 {
		t.Fatalf("Unvoted(3) = %d", n)
	}
	// The leader's own votes arrive: with peer 3's, every slot is chosen
	// and nothing is resent any more.
	for s := uint64(1); s <= rtSlots; s++ {
		r.Handle(paxos.Message{From: 1, To: 1, Body: paxos.LogAccepted{Ballot: r.Ballot(), Slot: s}})
	}
	if r.Commit() != rtSlots {
		t.Fatalf("commit = %d, want %d", r.Commit(), rtSlots)
	}
	if resent, err := checkRetransmit(r, tr, 4*rtAfter); err != nil || len(resent) != 0 {
		t.Fatalf("after commit: resent %v, %v", resent, err)
	}
}

// Negative: the old behaviour, resending every unchosen slot to every
// peer on every heartbeat, breaks the bound the check enforces.
func TestReplicaRetransmitAllCaughtByBound(t *testing.T) {
	r, tr := newRetransmitLeader(t, true)
	_, err := checkRetransmit(r, tr, 4*rtAfter)
	if err == nil {
		t.Fatal("the bound check accepted resending every slot on every heartbeat")
	}
	t.Logf("old behaviour caught as expected: %v", err)
}

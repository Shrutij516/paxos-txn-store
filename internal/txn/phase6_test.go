package txn

import (
	"fmt"
	"testing"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/transport"
)

// sameShardAccounts returns two accounts on one shard.
func sameShardAccounts() (string, string, ShardID) {
	for i := 0; i < numAccounts; i++ {
		for j := i + 1; j < numAccounts; j++ {
			if si := ShardOf(account(i), DefaultShards); si == ShardOf(account(j), DefaultShards) {
				return account(i), account(j), si
			}
		}
	}
	panic("no two accounts share a shard")
}

// A one-phase commit that fails at the leader is answered only after an
// abort record for it is in the shard's log. A retried commit can reach a
// new leader while an earlier leader's one-phase record for the same txn
// is still on its way into the log; the first record in the log decides,
// so the client is never told "aborted" for a txn that then commits.
func TestOnePhaseAbortIsLogged(t *testing.T) {
	a, b, sh := sameShardAccounts()
	w := newWorld(7, worldOpts{Clients: 1, OpsPerClient: 0, Faults: transport.Faults{MaxDelay: 2}})
	tc := w.cli[0]
	tc.plan = func(*client) ([]string, string) { return []string{a, b}, b }
	var told, unlogged error
	w.net.OnSend = func(m paxos.Message) {
		w.observe(m)
		ext, ok := m.Body.(paxos.Ext)
		if !ok {
			return
		}
		r, ok := ext.Body.(CommitResp)
		if !ok || r.Committed || unlogged != nil {
			return
		}
		told = fmt.Errorf("txn %d", r.Txn)
		if c, known := w.nodes[m.From].sm.Outcome(r.Txn); !known || c {
			unlogged = fmt.Errorf("node %d told the client txn %d aborted with outcome (%v, known %v) in its log", m.From, r.Txn, c, known)
		}
	}
	tc.hold, tc.ops, tc.think = true, 1, 1
	if _, ok := w.runUntil(2000, func() bool { return tc.phase == phHeld }); !ok {
		t.Fatal("T never finished its reads")
	}
	tid := tc.meta.ID
	for _, n := range shardNodes(sh) { // T is now doomed at the leader
		w.net.Send(paxos.Message{From: tc.id, To: n, Body: paxos.Ext{Body: AbortReq{Txn: tid}}})
	}
	w.run(20, false)
	tc.release()
	if _, ok := w.runUntil(calmLimit, w.quiescent); !ok {
		t.Fatal("not quiescent")
	}
	if told == nil {
		t.Fatal("the client was never told its one-phase commit aborted")
	}
	if unlogged != nil {
		t.Fatal(unlogged)
	}
	sm := w.canonical()[sh]
	if c, known := sm.Outcome(tid); !known || c || sm.IsOnePhase(tid) {
		t.Fatalf("T's outcome in the log: commit=%v known=%v onephase=%v, want a logged abort", c, known, sm.IsOnePhase(tid))
	}
	if tc.aborts == 0 || tc.commits != 1 {
		t.Fatalf("client: aborts=%d commits=%d, want an abort and a committed retry", tc.aborts, tc.commits)
	}
	if err := checkNoBank(w); err != nil {
		t.Fatal(err)
	}
}

// Touch (the Write RPC) succeeds only at the leader, for a txn that is
// neither doomed nor finished.
func TestTouch(t *testing.T) {
	a, b, sh := sameShardAccounts()
	w := newWorld(8, worldOpts{Clients: 1, OpsPerClient: 0, Faults: transport.Faults{MaxDelay: 2}})
	tc := w.cli[0]
	tc.plan = func(*client) ([]string, string) { return []string{a, b}, b }
	tc.ops, tc.think = 1, 1
	if _, ok := w.runUntil(calmLimit, w.quiescent); !ok {
		t.Fatal("not quiescent")
	}
	l := w.leader(sh)
	if !l.srv.Touch(12345) {
		t.Fatal("leader refused a fresh txn")
	}
	if l.srv.Touch(tc.meta.ID) {
		t.Fatal("leader accepted a write for a committed txn")
	}
	l.srv.Handle(paxos.Message{From: tc.id, To: l.id, Body: paxos.Ext{Body: AbortReq{Txn: 12345}}})
	if l.srv.Touch(12345) {
		t.Fatal("leader accepted a write for an aborted txn")
	}
	for _, id := range shardNodes(sh) {
		if n := w.nodes[id]; n != l && n.srv.Touch(999) {
			t.Fatalf("follower %d accepted a write", id)
		}
	}
}

// runRetryAtNewLeader is one seeded run of the case the logged one-phase
// abort exists for. A client's one-phase commit reaches the shard leader,
// which sends the record's accepts and crashes before anyone learns it is
// chosen. The next leader recovers the record from the promises, but is
// cut off from the remaining replica as soon as it takes over, so it
// cannot commit the record yet. The client's commit times out and, as the
// SDK does, retries at the next replicas until it reaches the new leader,
// whose read locks are gone. Then the partition heals and the recovered
// record commits. It returns the first check that fails.
func runRetryAtNewLeader(seed uint64, broken bool) error {
	a, b, sh := sameShardAccounts()
	// CheckQuorum is off: the test targets the commit-retry window, in
	// which the new leader is cut off from the third replica and must
	// still answer the client's retried commit. With CheckQuorum it steps
	// down after an election timeout and some retries get "not leader"
	// instead, so the window is hit in only about a third of the seeds.
	// Leader liveness is tested in internal/paxos (TestMPCheckQuorum).
	w := newWorld(seed, worldOpts{Clients: 1, OpsPerClient: 0, Faults: transport.Faults{MaxDelay: 3},
		Server: Config{ReplyAbortUnlogged: broken}, NoCheckQuorum: true})
	tc := w.cli[0]
	tc.plan = func(*client) ([]string, string) { return []string{a, b}, b }
	if _, ok := w.runUntil(2000, w.leadersUp); !ok {
		return fmt.Errorf("no leader for shard %d", sh)
	}
	// Let any duel between the first candidates end, so the leader does
	// not change while T reads (that would abort T for another reason).
	w.run(100, false)
	tc.hold, tc.ops, tc.think = true, 1, 1
	if _, ok := w.runUntil(2000, func() bool { return tc.phase == phHeld }); !ok {
		return fmt.Errorf("T never finished its reads")
	}
	tid := tc.meta.ID
	var old paxos.NodeID
	w.net.OnSend = func(m paxos.Message) {
		w.observe(m)
		if acc, ok := m.Body.(paxos.LogAccept); ok && old == 0 {
			if r, err := decode(acc.Entry.Cmd); err == nil && r.Kind == KindOnePhase && r.Txn.ID == tid {
				old = m.From
			}
		}
	}
	tc.release()
	if _, ok := w.runUntil(500, func() bool { return old != 0 }); !ok {
		return fmt.Errorf("the one-phase record was never proposed")
	}
	w.crash(old) // its accepts are already on the wire
	var nl *node
	if _, ok := w.runUntil(2000, func() bool { nl = w.leader(sh); return nl != nil }); !ok {
		return fmt.Errorf("no new leader")
	}
	var rest []paxos.NodeID
	for _, id := range w.ids {
		if id != nl.id {
			rest = append(rest, id)
		}
	}
	w.net.Partition([]paxos.NodeID{nl.id, tc.id}, rest)
	// The client gives up on the old leader and tries the others until
	// some replica answers, or the partition has lasted long enough.
	w.runUntil(1500, func() bool { return tc.phase != phCommitting })
	w.net.Heal()
	w.restart(old)
	if _, ok := w.runUntil(calmLimit, w.quiescent); !ok {
		return fmt.Errorf("not quiescent")
	}
	return checkNoBank(w)
}

// TestOnePhaseRetryAtNewLeader runs that case over seeded schedules. With
// the abort logged before the reply, every seed passes. With the old
// reply (ReplyAbortUnlogged) the client is told "aborted" for a txn that
// then commits, and the outcome check catches it.
func TestOnePhaseRetryAtNewLeader(t *testing.T) {
	seeds := numSeeds(txnSchedules)
	caught := 0
	for seed := uint64(1); seed <= seeds; seed++ {
		if err := runRetryAtNewLeader(seed, false); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if err := runRetryAtNewLeader(seed, true); err != nil {
			if caught == 0 {
				t.Logf("without the logged abort, seed=%d: %v", seed, err)
			}
			caught++
		}
	}
	if caught != int(seeds) {
		t.Fatalf("unlogged abort reply caught in only %d of %d schedules, want all", caught, seeds)
	}
	t.Logf("unlogged abort reply caught in %d of %d seeds; logged abort passes all", caught, seeds)
}

// gapHeavy turns on the gap-heavy faults during chaos (see chaos).
func gapHeavy(o *worldOpts) {
	o.Chaos.AcceptDrop = 0.7
	o.Chaos.RecordCrash = 0.05
	o.Chaos.RestartProb = 0.1
}

// TestTxnGapHeavy: every check holds across the random schedules in
// gap-heavy mode, where leaders crash right after proposing transaction
// records and new leaders must recover them.
func TestTxnGapHeavy(t *testing.T) {
	seeds := numSeeds(txnSchedules)
	crashes, withCrash := 0, 0
	for seed := uint64(1); seed <= seeds; seed++ {
		w, ok := runSchedule(seed, gapHeavy)
		if !ok {
			t.Fatalf("seed=%d: not quiescent within %d calm steps", seed, calmLimit)
		}
		if err := checkAll(w); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		crashes += w.recordCrashes
		if w.recordCrashes > 0 {
			withCrash++
		}
	}
	if withCrash == 0 {
		t.Fatal("no leader crashed after proposing a record; gap-heavy mode proves nothing")
	}
	t.Logf("%d gap-heavy schedules: %d crashes right after a record, in %d schedules", seeds, crashes, withCrash)
}

// TestTxnGapHeavyCatchesUnloggedAbort: the random gap-heavy schedules,
// with no directed setup, catch the old one-phase abort reply
// (ReplyAbortUnlogged) in at least 1% of seeds.
func TestTxnGapHeavyCatchesUnloggedAbort(t *testing.T) {
	seeds := numSeeds(txnSchedules)
	caught := 0
	for seed := uint64(1); seed <= seeds; seed++ {
		w, _ := runSchedule(seed, func(o *worldOpts) {
			gapHeavy(o)
			o.Server.ReplyAbortUnlogged = true
		})
		if err := checkAll(w); err != nil {
			if caught == 0 {
				t.Logf("caught as expected: seed=%d: %v", seed, err)
			}
			caught++
		}
	}
	if caught*100 < int(seeds) {
		t.Fatalf("unlogged abort reply caught in %d of %d gap-heavy schedules, want at least 1%%", caught, seeds)
	}
	t.Logf("unlogged abort reply caught in %d of %d gap-heavy schedules", caught, seeds)
}

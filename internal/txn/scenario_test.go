package txn

import (
	"testing"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/transport"
)

func quietOpts() worldOpts {
	return worldOpts{Clients: 3, OpsPerClient: 8, Faults: transport.Faults{MaxDelay: 2}}
}

// Test 4: the coordinator's leader crashes right after its commit decision
// is applied, before it tells anyone. The decision is in the coordinator
// shard's Paxos log, so a new coordinator leader answers the participants'
// outcome queries and the transaction commits everywhere.
func TestCoordinatorCrashAfterDecide(t *testing.T) {
	for seed := uint64(1); seed <= numSeeds(30); seed++ {
		w := newWorld(seed, quietOpts())
		var victim paxos.NodeID
		var txn ID
		w.onDecide = func(n *node, id ID) {
			if c, _ := n.sm.Decided(id); c && victim == 0 {
				victim, txn = n.id, id
				w.crash(n.id) // before any Decision message goes out
			}
		}
		if _, ok := w.runUntil(3000, func() bool { return victim != 0 }); !ok {
			t.Fatalf("seed=%d: no multi-shard commit decision happened", seed)
		}
		coord := w.shard[victim]
		var parts []ShardID
		for _, id := range shardNodes(coord) {
			if n := w.nodes[id]; n != nil {
				parts = n.sm.DecidedParticipants(txn)
			}
		}
		// The crashed node stays down: the outcome must reach every
		// participant through the coordinator shard's new leader.
		resolved := func() bool {
			for _, p := range parts {
				l := w.leader(p)
				if l == nil {
					return false
				}
				if c, known := l.sm.Outcome(txn); !known || !c {
					return false
				}
			}
			return true
		}
		if _, ok := w.runUntil(2000, resolved); !ok {
			t.Fatalf("seed=%d: txn %d never committed on all of %v after coordinator %d crashed", seed, txn, parts, victim)
		}
		w.onDecide = nil
		w.restart(victim)
		if _, ok := w.runUntil(calmLimit, w.quiescent); !ok {
			t.Fatalf("seed=%d: not quiescent", seed)
		}
		if err := checkAll(w); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
	}
}

// Test 5: a participant's leader crashes right after its prepare record is
// applied, before it votes. The new leader rebuilds the exclusive locks
// from the prepared record in its replicated state, holds them, votes when
// the coordinator retries, and finishes the transaction.
func TestParticipantCrashWhilePrepared(t *testing.T) {
	locksChecked := 0
	for seed := uint64(1); seed <= numSeeds(30); seed++ {
		w := newWorld(seed, quietOpts())
		var victim paxos.NodeID
		var txn ID
		var keys []string
		w.onPrepared = func(n *node, id ID) {
			r := n.sm.Prepared(id)
			if victim == 0 && r != nil && r.Coord != n.shard && len(r.Writes) > 0 {
				victim, txn, keys = n.id, id, sortedKeys(r.Writes)
				w.crash(n.id)
			}
		}
		if _, ok := w.runUntil(3000, func() bool { return victim != 0 }); !ok {
			t.Fatalf("seed=%d: no participant prepared a write", seed)
		}
		sh := w.shard[victim]
		// Wait for a new leader of the participant shard that has caught up
		// on the prepare record (it may need to re-propose it first).
		caughtUp := func() bool {
			l := w.leader(sh)
			if l == nil || !l.srv.Leading() {
				return false
			}
			_, done := l.sm.Outcome(txn)
			return done || l.sm.Prepared(txn) != nil
		}
		if _, ok := w.runUntil(2000, caughtUp); !ok {
			t.Fatalf("seed=%d: shard %d elected no new leader with the prepare record", seed, sh)
		}
		l := w.leader(sh)
		if l.sm.Prepared(txn) != nil {
			for _, k := range keys {
				if h, ok := l.srv.ExclusiveHolder(k); !ok || h != txn {
					t.Fatalf("seed=%d: new leader %d does not hold %s for prepared txn %d (holder %d, %v)", seed, l.id, k, txn, h, ok)
				}
			}
			locksChecked++
		}
		w.onPrepared = nil
		w.restart(victim)
		if _, ok := w.runUntil(calmLimit, w.quiescent); !ok {
			t.Fatalf("seed=%d: not quiescent", seed)
		}
		if _, known := w.canonical()[sh].Outcome(txn); !known {
			t.Fatalf("seed=%d: txn %d never finished on shard %d", seed, txn, sh)
		}
		if err := checkAll(w); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
	}
	if locksChecked == 0 {
		t.Fatal("the new leader never saw the txn still prepared; the lock check never ran")
	}
	t.Logf("new leader held the prepared txn's locks in %d of %d runs (the rest resolved before the check)", locksChecked, numSeeds(30))
}

// crossingKeys returns two accounts on different shards.
func crossingKeys() (string, string) {
	x := account(0)
	for i := 1; i < numAccounts; i++ {
		if ShardOf(account(i), DefaultShards) != ShardOf(x, DefaultShards) {
			return x, account(i)
		}
	}
	panic("all accounts on one shard")
}

const deadlockBound = 400

// runCrossing starts two transactions at once that read both keys in
// opposite order and each write the key the other read first. Under plain
// waiting they deadlock: each prepare needs an exclusive lock the other
// holds shared. Timeouts that would break the deadlock are set far beyond
// the step bound, so only wound-wait can make progress in time.
func runCrossing(seed uint64, noWoundWait bool) (int, bool) {
	x, y := crossingKeys()
	o := worldOpts{Clients: 2, OpsPerClient: 1, Faults: transport.Faults{MaxDelay: 3},
		Server: Config{CoordTimeout: 5000, LockLease: 5000, NoWoundWait: noWoundWait}}
	w := newWorld(seed, o)
	w.cli[0].plan = func(*client) ([]string, string) { return []string{x, y}, y }
	w.cli[1].plan = func(*client) ([]string, string) { return []string{y, x}, x }
	// Let leaders settle, then start both at the same step.
	w.runUntil(1000, func() bool { return w.leader(0) != nil && w.leader(1) != nil && w.leader(2) != nil })
	for _, c := range w.cli {
		c.think = 1
	}
	return w.runUntil(deadlockBound, w.clientsDone)
}

// Test 6: crossing transactions always make progress under wound-wait.
func TestDeadlockCrossingTxnsProgress(t *testing.T) {
	worst := 0
	for seed := uint64(1); seed <= numSeeds(200); seed++ {
		used, ok := runCrossing(seed, false)
		if !ok {
			t.Fatalf("seed=%d: crossing txns did not both commit within %d steps", seed, deadlockBound)
		}
		worst = max(worst, used)
	}
	t.Logf("worst steps for both crossing txns to commit: %d (bound %d)", worst, deadlockBound)
}

// ---- Negative tests ----

// findBroken runs schedules with a broken mode until a check fails.
func findBroken(t *testing.T, tweak func(*worldOpts)) {
	t.Helper()
	for seed := uint64(1); seed <= txnSchedules; seed++ {
		w, ok := runSchedule(seed, tweak)
		err := checkAll(w)
		if err != nil {
			t.Logf("violation caught as expected: seed=%d: %v", seed, err)
			return
		}
		_ = ok
	}
	t.Fatalf("no violation in %d schedules: the checkers or the schedule are too weak", txnSchedules)
}

// Test 7a: a coordinator that commits without all yes votes.
func TestNegativeCommitWithoutAllVotes(t *testing.T) {
	findBroken(t, func(o *worldOpts) { o.Server.CommitWithoutAllVotes = true })
}

// Test 7b: a participant that releases its locks once prepared.
func TestNegativeReleaseLocksAtPrepare(t *testing.T) {
	findBroken(t, func(o *worldOpts) { o.Server.ReleaseLocksAtPrepare = true })
}

// Test 7c: a prepare kept in leader memory instead of the Paxos log.
func TestNegativeVolatilePrepare(t *testing.T) {
	findBroken(t, func(o *worldOpts) { o.Server.VolatilePrepare = true })
}

// Test 7d: without wound-wait the crossing transactions deadlock and the
// liveness test must fail.
func TestNegativeNoWoundWait(t *testing.T) {
	for seed := uint64(1); seed <= txnSchedules; seed++ {
		if _, ok := runCrossing(seed, true); !ok {
			t.Logf("deadlock caught as expected: seed=%d: crossing txns stuck for %d steps", seed, deadlockBound)
			return
		}
	}
	t.Fatal("crossing txns always finished without wound-wait; the liveness test proves nothing")
}

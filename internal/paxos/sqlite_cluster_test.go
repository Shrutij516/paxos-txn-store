package paxos_test

import (
	"math/rand/v2"
	"testing"

	"github.com/anishathalye/porcupine"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/transport"
)

// sqliteSeeds is the seed count for SQLite-backed cluster runs: 100 in the
// full suite, 20 under -short. Every write is a real fsync, so these runs
// cost far more per seed than the in-memory ones.
func sqliteSeeds() uint64 {
	if testing.Short() {
		return 20
	}
	return 100
}

// runSQLite drives one seeded schedule on SQLite-backed replicas in a fresh
// temp dir: chaos with real crash and restart, then calm until every client
// is done. It checks safety and linearizability (mid-chaos and at the end)
// and returns the cluster for further checks.
func runSQLite(t *testing.T, seed uint64, tweak func(*mpOpts)) (*mpCluster, error) {
	t.Helper()
	o := randomMPOpts(rand.New(rand.NewPCG(seed, 2)))
	o.Dir = t.TempDir()
	if tweak != nil {
		tweak(&o)
	}
	c := newMPCluster(seed, o)
	defer c.closeAll()
	c.run(mpChaosSteps, true)
	cut := c.history()
	c.calm()
	_, done := c.runUntil(mpCalmLimit, c.clientsDone)
	if err := c.checkSafety(); err != nil {
		return c, err
	}
	if !done {
		t.Fatalf("seed=%d: clients did not finish within %d calm steps", seed, mpCalmLimit)
	}
	for name, h := range map[string][]porcupine.Operation{"after chaos": cut, "at end": c.history()} {
		if !porcupine.CheckOperations(kvModel, h) {
			t.Fatalf("seed=%d: history %s is not linearizable", seed, name)
		}
	}
	return c, nil
}

// Test 3: log safety, state machine safety and linearizability with SQLite
// storage and real crash/restart from disk.
func TestSQLiteClusterSafety(t *testing.T) {
	restarts := 0
	for seed := uint64(1); seed <= sqliteSeeds(); seed++ {
		c, err := runSQLite(t, seed, nil)
		if err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		restarts += len(c.sms) - c.opts.N
	}
	if restarts == 0 {
		t.Fatal("no node ever restarted from disk; the test proves nothing")
	}
	t.Logf("%d restarts from disk over %d seeds", restarts, sqliteSeeds())
}

// Test 4: crash right before a storage transaction commits, and right after
// it commits but before the reply goes out. Both must stay safe.
func TestSQLiteCrashPoints(t *testing.T) {
	for _, tc := range []struct {
		name   string
		before float64
		after  float64
	}{
		{"before-commit", 0.01, 0},
		{"after-commit", 0, 0.01},
	} {
		t.Run(tc.name, func(t *testing.T) {
			crashes := 0
			for seed := uint64(1); seed <= sqliteSeeds(); seed++ {
				c, err := runSQLite(t, seed, func(o *mpOpts) {
					o.CrashBeforeCommit, o.CrashAfterCommit = tc.before, tc.after
				})
				if err != nil {
					t.Fatalf("seed=%d: %v", seed, err)
				}
				crashes += c.dbCrashes[0] + c.dbCrashes[1]
			}
			if crashes == 0 {
				t.Fatal("no crash was injected at the commit point")
			}
			t.Logf("%d crashes injected %s over %d seeds", crashes, tc.name, sqliteSeeds())
		})
	}
}

// Test 5 (negative): an acceptor that replies before its transaction
// commits, then crashes before the commit, must be caught by the checker.
func TestSQLiteNegativeReplyBeforePersist(t *testing.T) {
	for seed := uint64(1); seed <= 100; seed++ {
		if _, err := runSQLite(t, seed, replyBeforePersist); err != nil {
			t.Logf("violation caught as expected: seed=%d: %v", seed, err)
			return
		}
	}
	t.Fatal("no violation in 100 schedules: the checker or the schedule is too weak")
}

func replyBeforePersist(o *mpOpts) {
	o.N = 3
	o.ReplyBeforePersist = true
	o.CrashBeforeCommit = 0.15
}

// A node restarted from its SQLite file rebuilds the KV store and dedup
// table from its durable committed log before it hears from anyone.
func TestSQLiteRestartReplaysCommittedLog(t *testing.T) {
	o := mpOpts{N: 3, Clients: 3, OpsPerClient: 40, Keys: 2, Dir: t.TempDir(), Faults: transport.Faults{MaxDelay: 2}}
	c := newMPCluster(7, o)
	defer c.closeAll()
	if _, ok := c.runUntil(3000, func() bool { l := c.leader(); return l != nil && l.Commit() >= 30 }); !ok {
		t.Fatal("cluster made no progress")
	}
	var id paxos.NodeID
	for _, n := range c.ids {
		if n != c.leader().ID() {
			id = n
			break
		}
	}
	before := c.reps[id].Commit()
	var old *recSM // the incarnation that is about to crash
	for _, sm := range c.sms {
		if sm.node == id {
			old = sm
		}
	}
	if len(old.log) != int(before) {
		t.Fatalf("node applied %d entries but commit is %d", len(old.log), before)
	}
	c.crash(id)
	c.closeDead()
	c.restart(id) // no step runs: everything below comes from disk
	r := c.reps[id]
	if r.Commit() != before {
		t.Fatalf("restored commit index %d, want %d", r.Commit(), before)
	}
	fresh := c.sms[len(c.sms)-1]
	if fresh.node != id || len(fresh.log) != int(before) {
		t.Fatalf("replayed %d entries, want %d", len(fresh.log), before)
	}
	for i := range fresh.log {
		if fresh.log[i] != old.log[i] {
			t.Fatalf("replayed entry %d differs: %+v vs %+v", i+1, fresh.log[i], old.log[i])
		}
	}
	for _, k := range []string{"k0", "k1"} {
		if fresh.kv.Value(k) != old.kv.Value(k) {
			t.Fatalf("key %s = %q after replay, was %q", k, fresh.kv.Value(k), old.kv.Value(k))
		}
	}
	if fresh.kv.MaxExecutions() > 1 {
		t.Fatal("replay executed a request twice")
	}
	c.runUntil(2000, c.clientsDone)
	if err := c.checkSafety(); err != nil {
		t.Fatal(err)
	}
}

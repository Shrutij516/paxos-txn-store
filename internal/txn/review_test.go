package txn

import (
	"testing"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/transport"
)

// twoShardKeys returns a key on the lower shard (which will coordinate a
// txn touching both) and a key on a higher shard.
func twoShardKeys() (coordKey, partKey string, coord, part ShardID) {
	x, y := crossingKeys()
	sx, sy := ShardOf(x, DefaultShards), ShardOf(y, DefaultShards)
	if sx < sy {
		return x, y, sx, sy
	}
	return y, x, sy, sx
}

func others(sh ShardID) []paxos.NodeID {
	var out []paxos.NodeID
	for s := ShardID(0); s < DefaultShards; s++ {
		if s != sh {
			out = append(out, shardNodes(s)...)
		}
	}
	return out
}

func committedAnywhere(w *world, id ID) bool {
	for _, sm := range w.canonical() {
		if c, _ := sm.Outcome(id); c {
			return true
		}
	}
	return false
}

// Review 1: a lease never expires an exclusive lock held by a prepared txn.
// P prepares a write of k on the participant shard, then the coordinator
// shard is cut off so P stays prepared for ten lease periods. A conflicting
// txn on k must not commit until P is resolved.
func TestLeaseNeverExpiresPreparedLock(t *testing.T) {
	const lease = 30
	ck, k, coord, part := twoShardKeys()
	o := worldOpts{Clients: 2, OpsPerClient: 0, Faults: transport.Faults{MaxDelay: 2},
		Server: Config{LockLease: lease, QueryAfter: 100000, CoordTimeout: 100000}}
	w := newWorld(1, o)
	p, q := w.cli[0], w.cli[1]
	p.plan = func(*client) ([]string, string) { return []string{ck, k}, k }
	q.plan = func(*client) ([]string, string) { return []string{k}, k }
	var pid ID
	w.onPrepared = func(n *node, id ID) {
		if pid == 0 && n.shard == part && id == p.meta.ID {
			pid = id
			w.net.Partition(others(coord), shardNodes(coord)) // the vote never arrives
		}
	}
	if _, ok := w.runUntil(2000, w.leadersUp); !ok {
		t.Fatal("no leader on some shard")
	}
	p.ops, p.think = 1, 1
	if _, ok := w.runUntil(2000, func() bool { return pid != 0 }); !ok {
		t.Fatal("P never prepared on the participant shard")
	}
	before := w.leader(part).sm.Get(k)
	q.ops, q.think = 1, 1
	w.run(10*lease, false)
	l := w.leader(part)
	if l == nil || l.sm.Prepared(pid) == nil {
		t.Fatal("P is no longer prepared; the test lost its premise")
	}
	if h, ok := l.srv.ExclusiveHolder(k); !ok || h != pid {
		t.Fatalf("after %d lease periods the holder of %s is %d (%v), want prepared txn %d", 10, k, h, ok, pid)
	}
	if q.commits != 0 || l.sm.Get(k) != before {
		t.Fatalf("conflicting txn committed while P was prepared: commits=%d, %s=%+v", q.commits, k, l.sm.Get(k))
	}
	w.net.Heal()
	if _, ok := w.runUntil(calmLimit, w.quiescent); !ok {
		t.Fatal("not quiescent after heal")
	}
	if q.commits != 1 {
		t.Fatalf("conflicting txn never committed after P resolved (commits=%d)", q.commits)
	}
	if err := checkNoBank(w); err != nil {
		t.Fatal(err)
	}
}

// Review 2: a shard the txn only reads from still validates its reads. T
// reads a on shard A and b on shard B and writes only b. Before T commits,
// A's leader changes and another txn overwrites a. T must abort; its retry
// may commit.
func TestReadOnlyParticipantValidatesAfterLeaderChange(t *testing.T) {
	a, b, sa, _ := twoShardKeys() // A is read-only for T
	w := newWorld(3, worldOpts{Clients: 2, OpsPerClient: 0, Faults: transport.Faults{MaxDelay: 2}})
	tc, wc := w.cli[0], w.cli[1]
	tc.plan = func(*client) ([]string, string) { return []string{a, b}, b }
	wc.plan = func(*client) ([]string, string) { return []string{a}, a }
	tc.hold, tc.ops, tc.think = true, 1, 1
	if _, ok := w.runUntil(2000, func() bool { return tc.phase == phHeld }); !ok {
		t.Fatal("T never finished its reads")
	}
	tid, readVer := tc.meta.ID, tc.cur.reads[a]
	old := w.leader(sa)
	w.crash(old.id)
	if _, ok := w.runUntil(2000, func() bool { return w.leader(sa) != nil }); !ok {
		t.Fatal("shard A elected no new leader")
	}
	wc.ops, wc.think = 1, 1
	if _, ok := w.runUntil(2000, func() bool { return wc.commits == 1 }); !ok {
		t.Fatal("the conflicting write on a never committed")
	}
	if w.leader(sa).sm.Get(a).Ver == readVer {
		t.Fatal("a was not overwritten; the test lost its premise")
	}
	tc.release()
	w.restart(old.id)
	if _, ok := w.runUntil(calmLimit, w.quiescent); !ok {
		t.Fatal("not quiescent")
	}
	if committedAnywhere(w, tid) {
		t.Fatalf("T (txn %d) committed after reading a stale version of %s", tid, a)
	}
	if tc.aborts == 0 || tc.commits != 1 {
		t.Fatalf("T: aborts=%d commits=%d, want at least one abort and a committed retry", tc.aborts, tc.commits)
	}
	if err := checkNoBank(w); err != nil {
		t.Fatal(err)
	}
}

// Review 3a: a participant's outcome query reaches the coordinator shard
// before any decision and while nobody is coordinating. The coordinator
// logs an abort decision before answering (checked for every Decision on
// the wire by world.observe), and once that is logged the txn can never
// commit, not even when the client's commit request arrives afterwards.
func TestQueryBeforeDecisionLogsAbortFirst(t *testing.T) {
	ck, k, coord, part := twoShardKeys()
	w := newWorld(5, worldOpts{Clients: 1, OpsPerClient: 0, Faults: transport.Faults{MaxDelay: 2}})
	tc := w.cli[0]
	tc.plan = func(*client) ([]string, string) { return []string{ck, k}, k }
	tc.hold, tc.ops, tc.think = true, 1, 1
	if _, ok := w.runUntil(2000, func() bool { return tc.phase == phHeld }); !ok {
		t.Fatal("T never finished its reads")
	}
	tid := tc.meta.ID
	from := w.leader(part).id
	for _, n := range shardNodes(coord) {
		w.net.Send(paxos.Message{From: from, To: n, Body: paxos.Ext{Body: QueryOutcome{Txn: tid, From: part, Participants: []ShardID{coord, part}}}})
	}
	if _, ok := w.runUntil(500, func() bool { _, known := w.leader(coord).sm.Decided(tid); return known }); !ok {
		t.Fatal("the coordinator never logged a decision for the queried txn")
	}
	if c, _ := w.leader(coord).sm.Decided(tid); c {
		t.Fatal("presumed abort logged a commit")
	}
	tc.release() // the commit request now arrives after the abort is logged
	if _, ok := w.runUntil(calmLimit, w.quiescent); !ok {
		t.Fatal("not quiescent")
	}
	if committedAnywhere(w, tid) {
		t.Fatalf("txn %d committed after its coordinator logged abort", tid)
	}
	if tc.commits != 1 || tc.aborts == 0 {
		t.Fatalf("client: commits=%d aborts=%d, want the first attempt aborted and a retry committed", tc.commits, tc.aborts)
	}
	if err := checkNoBank(w); err != nil {
		t.Fatal(err)
	}
}

// Review 3b: a query that arrives while the coordinator is still collecting
// votes is not answered with a guess; the txn goes on to commit.
func TestQueryWhileCoordinatingWaitsForDecision(t *testing.T) {
	ck, k, coord, part := twoShardKeys()
	w := newWorld(6, worldOpts{Clients: 1, OpsPerClient: 0, Faults: transport.Faults{MaxDelay: 2}})
	tc := w.cli[0]
	tc.plan = func(*client) ([]string, string) { return []string{ck, k}, k }
	var tid ID
	w.onPrepared = func(n *node, id ID) {
		if tid == 0 && n.shard == part {
			tid = id
			for _, c := range shardNodes(coord) { // query before even voting
				w.net.Send(paxos.Message{From: n.id, To: c, Body: paxos.Ext{Body: QueryOutcome{Txn: id, From: part, Participants: []ShardID{coord, part}}}})
			}
		}
	}
	tc.ops, tc.think = 1, 1
	if _, ok := w.runUntil(calmLimit, w.quiescent); !ok {
		t.Fatal("not quiescent")
	}
	if tid == 0 {
		t.Fatal("participant never prepared")
	}
	if c, known := w.canonical()[coord].Decided(tid); !known || !c {
		t.Fatalf("early query changed the outcome: decided=%v commit=%v", known, c)
	}
	if err := checkNoBank(w); err != nil {
		t.Fatal(err)
	}
}

// checkNoBank runs atomicity and strict serializability. Directed tests
// whose fixed plans increment keys instead of transferring between
// accounts skip the bank invariant, which only holds for transfers.
func checkNoBank(w *world) error {
	if err := w.checkAtomicity(); err != nil {
		return err
	}
	h, err := w.history()
	if err != nil {
		return err
	}
	return checkStrict(h)
}

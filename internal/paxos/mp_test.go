package paxos_test

import (
	"math/rand/v2"
	"testing"

	"github.com/anishathalye/porcupine"

	"github.com/Shrutij516/paxos-txn-store/internal/kv"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/transport"
)

const (
	mpSchedules  = 1000
	mpChaosSteps = 600
	mpCalmLimit  = 4000 // steps allowed for all clients to finish once faults stop
)

func randomMPOpts(rng *rand.Rand) mpOpts {
	return mpOpts{
		N:            []int{3, 5}[rng.IntN(2)],
		Clients:      2 + rng.IntN(3),
		OpsPerClient: 5 + rng.IntN(6),
		Keys:         2,
		Faults: transport.Faults{
			DropProb: rng.Float64() * 0.2,
			DupProb:  rng.Float64() * 0.1,
			MaxDelay: 1 + rng.IntN(5),
		},
		Chaos: chaos{
			CrashProb:     rng.Float64() * 0.01,
			RestartProb:   0.05,
			PartitionProb: rng.Float64() * 0.01,
			HealProb:      0.05,
		},
	}
}

// runMP drives one seeded schedule: chaos, then calm until every client
// has finished. It reports whether the clients finished in time.
func runMP(seed uint64, tweak func(*mpOpts)) (*mpCluster, bool) {
	o := randomMPOpts(rand.New(rand.NewPCG(seed, 2)))
	if tweak != nil {
		tweak(&o)
	}
	c := newMPCluster(seed, o)
	c.run(mpChaosSteps, true)
	c.calm()
	_, ok := c.runUntil(mpCalmLimit, c.clientsDone)
	return c, ok
}

// Tests 1 and 2: log safety, state machine safety (and exactly-once) over
// 1000 random schedules. Clients must also all finish once faults stop.
func TestMPLogAndStateMachineSafety(t *testing.T) {
	for seed := uint64(1); seed <= numSeeds(mpSchedules); seed++ {
		c, ok := runMP(seed, nil)
		if err := c.checkSafety(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if !ok {
			t.Fatalf("seed=%d: clients did not finish within %d calm steps", seed, mpCalmLimit)
		}
	}
}

// Test 3: client histories with retries are linearizable. Each schedule
// is checked twice: once cut off at the end of the chaos phase, when many
// operations are still outstanding (timed out, stuck behind a crashed
// leader), and once after every client finished.
func TestMPLinearizable(t *testing.T) {
	for seed := uint64(1); seed <= numSeeds(mpSchedules); seed++ {
		checkLinearizable(t, seed, nil)
	}
}

func checkLinearizable(t *testing.T, seed uint64, tweak func(*mpOpts)) {
	t.Helper()
	o := randomMPOpts(rand.New(rand.NewPCG(seed, 2)))
	if tweak != nil {
		tweak(&o)
	}
	c := newMPCluster(seed, o)
	c.run(mpChaosSteps, true)
	cut := c.history()
	c.calm()
	c.runUntil(mpCalmLimit, c.clientsDone)
	for name, h := range map[string][]porcupine.Operation{"after chaos": cut, "at end": c.history()} {
		if len(h) == 0 {
			t.Fatalf("seed=%d: empty history %s", seed, name)
		}
		if !porcupine.CheckOperations(kvModel, h) {
			t.Fatalf("seed=%d: history %s (%d operations) is not linearizable", seed, name, len(h))
		}
	}
}

// Operations with no response stay in the history with an unknown output
// and a return time after every completed operation; they are never dropped.
func TestMPHistoryKeepsOutstandingOperations(t *testing.T) {
	for seed := uint64(1); seed <= 50; seed++ {
		c := newMPCluster(seed, randomMPOpts(rand.New(rand.NewPCG(seed, 2))))
		c.run(mpChaosSteps, true)
		done, open := 0, 0
		for _, cl := range c.cli {
			done += len(cl.history)
			if cl.waiting {
				open++
			}
		}
		h := c.history()
		if len(h) != done+open {
			t.Fatalf("seed=%d: history has %d ops, want %d completed + %d outstanding", seed, len(h), done, open)
		}
		var lastReturn int64
		for _, op := range h {
			if !op.Output.(kvOutput).unknown {
				lastReturn = max(lastReturn, op.Return)
			}
		}
		for _, op := range h {
			if op.Output.(kvOutput).unknown && op.Return <= lastReturn {
				t.Fatalf("seed=%d: outstanding op returns at %d, not after every completed op (%d)", seed, op.Return, lastReturn)
			}
		}
		if open > 0 {
			return
		}
	}
	t.Fatal("no schedule left an operation outstanding; the test proves nothing")
}

// stable builds a fault-free cluster and waits for a leader that has
// committed at least minCommit slots.
func stable(t *testing.T, seed uint64, o mpOpts, minCommit uint64) *mpCluster {
	t.Helper()
	c := newMPCluster(seed, o)
	if _, ok := c.runUntil(1000, func() bool {
		l := c.leader()
		return l != nil && l.Commit() >= minCommit
	}); !ok {
		t.Fatalf("seed=%d: no leader with commit >= %d", seed, minCommit)
	}
	return c
}

// Test 4: after the leader crashes, a new leader is elected and commits a
// new entry within a fixed step bound.
func TestMPLeaderFailover(t *testing.T) {
	const bound = 300
	worst := 0
	for seed := uint64(1); seed <= numSeeds(200); seed++ {
		c := stable(t, seed, mpOpts{N: 5, Clients: 3, OpsPerClient: 100, Keys: 2, Faults: transport.Faults{MaxDelay: 2}}, 3)
		old := c.leader()
		before := old.Commit()
		c.crash(old.ID())
		used, ok := c.runUntil(bound, func() bool {
			l := c.leader()
			return l != nil && l.ID() != old.ID() && l.Commit() > before
		})
		if !ok {
			t.Fatalf("seed=%d: no new leader committed within %d steps after leader %d crashed", seed, bound, old.ID())
		}
		worst = max(worst, used)
		if err := c.checkSafety(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
	}
	t.Logf("worst steps from leader crash to new commit: %d (bound %d)", worst, bound)
}

// Test 5: a crashed follower restarts and catches up to the leader's
// committed log, including more entries than one catch-up batch.
func TestMPCatchUp(t *testing.T) {
	for seed := uint64(1); seed <= numSeeds(100); seed++ {
		c := stable(t, seed, mpOpts{N: 3, Clients: 3, OpsPerClient: 60, Keys: 2, Faults: transport.Faults{MaxDelay: 2}}, 2)
		var lag paxos.NodeID
		for _, id := range c.ids {
			if id != c.leader().ID() {
				lag = id
				break
			}
		}
		c.crash(lag)
		c.runUntil(3000, c.clientsDone)
		l := c.leader()
		if l == nil || l.Commit() < uint64(paxos.DefaultLogTiming.CatchupBatch) {
			t.Fatalf("seed=%d: leader did not commit enough while follower was down", seed)
		}
		c.restart(lag)
		r := c.reps[lag]
		if _, ok := c.runUntil(500, func() bool { return r.Commit() == c.leader().Commit() }); !ok {
			t.Fatalf("seed=%d: node %d at commit %d, leader at %d", seed, lag, r.Commit(), c.leader().Commit())
		}
		for s := uint64(1); s <= r.Commit(); s++ {
			a, _ := r.Committed(s)
			b, _ := c.leader().Committed(s)
			if a != b {
				t.Fatalf("seed=%d: slot %d differs after catch-up: %+v vs %+v", seed, s, a, b)
			}
		}
		if err := c.checkSafety(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
	}
}

// Test 6: a retried request is applied once, and the retry gets the
// original result even after another client changed the key.
func TestMPExactlyOnce(t *testing.T) {
	c := stable(t, 1, mpOpts{N: 3, Faults: transport.Faults{MaxDelay: 2}}, 0)
	var replies []paxos.ClientReply
	const fake = paxos.NodeID(200)
	c.net.Register(fake, func(m paxos.Message) {
		if r, ok := m.Body.(paxos.ClientReply); ok && r.OK {
			replies = append(replies, r)
		}
	})
	send := func(r paxos.ClientRequest) {
		c.net.Send(paxos.Message{From: fake, To: c.leader().ID(), Body: r})
		c.runUntil(50, func() bool { return false })
	}
	get := paxos.ClientRequest{ClientID: 9, Seq: 1, Cmd: kv.Get("a")}
	send(paxos.ClientRequest{ClientID: 8, Seq: 1, Cmd: kv.Put("a", "x")})
	send(get)
	send(get) // retry, as if the reply were lost
	send(paxos.ClientRequest{ClientID: 8, Seq: 2, Cmd: kv.Put("a", "y")})
	send(get) // late retry after the value changed
	if len(replies) != 5 {
		t.Fatalf("got %d replies, want 5", len(replies))
	}
	for _, i := range []int{1, 2, 4} {
		if replies[i].Result != "x" {
			t.Fatalf("reply %d = %q, want cached x", i, replies[i].Result)
		}
	}
	dups := 0
	for _, a := range c.sms[0].log {
		if a.entry.ClientID == 9 {
			dups++
		}
	}
	if dups != 3 {
		t.Fatalf("retried request appears %d times in the log, want 3", dups)
	}
	for _, sm := range c.sms {
		if sm.kv.MaxExecutions() != 1 || sm.kv.Value("a") != "y" {
			t.Fatalf("node %d: executions=%d a=%q", sm.node, sm.kv.MaxExecutions(), sm.kv.Value("a"))
		}
	}
}

// Test 7: the same seed replays to the same trace hash.
func TestMPReplay(t *testing.T) {
	for _, seed := range []uint64{3, 77} {
		a, _ := runMP(seed, nil)
		b, _ := runMP(seed, nil)
		if a.TraceHash() != b.TraceHash() {
			t.Fatalf("seed=%d: trace hashes differ", seed)
		}
	}
	a, _ := runMP(3, nil)
	b, _ := runMP(4, nil)
	if a.TraceHash() == b.TraceHash() {
		t.Fatal("different seeds produced identical traces")
	}
}

// findMPViolation runs broken schedules until the safety checker fires.
func findMPViolation(t *testing.T, tweak func(*mpOpts)) {
	t.Helper()
	for seed := uint64(1); seed <= mpSchedules; seed++ {
		c, _ := runMP(seed, tweak)
		if err := c.checkSafety(); err != nil {
			t.Logf("violation caught as expected: seed=%d: %v", seed, err)
			return
		}
	}
	t.Fatalf("no violation in %d schedules: the checker or the schedule is too weak", mpSchedules)
}

// Test 8a: a leader that skips Prepare after taking over breaks log safety.
func TestMPNegativeSkipPrepare(t *testing.T) {
	findMPViolation(t, func(o *mpOpts) { o.SkipPrepare = true })
}

// Test 8b: a new leader that ignores accepted values in promises breaks
// log safety.
func TestMPNegativeIgnorePromisedValues(t *testing.T) {
	findMPViolation(t, func(o *mpOpts) { o.IgnorePromised = true })
}

// Test 8c: without the dedup table, a retried request executes twice.
func TestMPNegativeNoDedup(t *testing.T) {
	findMPViolation(t, func(o *mpOpts) { o.NoDedup = true })
}

// The linearizability check is not vacuous: corrupting one completed Get in
// a real history must make porcupine reject it.
func TestMPLinearizabilityCheckerCatchesStaleRead(t *testing.T) {
	c, _ := runMP(1, nil)
	h := c.history()
	for i, op := range h {
		if in := op.Input.(kvInput); !in.put && !op.Output.(kvOutput).unknown {
			h[i].Output = kvOutput{value: "never-written"}
			if porcupine.CheckOperations(kvModel, h) {
				t.Fatal("porcupine accepted a Get that returned a value nobody wrote")
			}
			return
		}
	}
	t.Fatal("history has no completed Get")
}

// ---- Gap-heavy mode ----

// gapHeavy drops most Accepts during chaos and crashes leaders a few steps
// after they send Accepts, so new leaders regularly find gaps (filled with
// no-ops) and partially accepted slots (recovered by highest ballot).
func gapHeavy(o *mpOpts) {
	o.AcceptDrop = 0.7
	o.CrashAfterAccept = 0.03
	o.Chaos.RestartProb = 0.1
}

func TestMPGapHeavySafety(t *testing.T) {
	n := numSeeds(mpSchedules)
	noopSeeds, recoveredSeeds, contestedSeeds := 0, 0, 0
	for seed := uint64(1); seed <= n; seed++ {
		c, ok := runMP(seed, gapHeavy)
		if err := c.checkSafety(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		if !ok {
			t.Fatalf("seed=%d: clients did not finish within %d calm steps", seed, mpCalmLimit)
		}
		noops, recovered, contested := c.takeoverStats()
		if noops > 0 {
			noopSeeds++
		}
		if recovered > 0 {
			recoveredSeeds++
		}
		if contested > 0 {
			contestedSeeds++
		}
	}
	t.Logf("gap-heavy over %d seeds: no-op fill in %d, highest-ballot recovery in %d (with differing entries in %d)",
		n, noopSeeds, recoveredSeeds, contestedSeeds)
	if uint64(noopSeeds)*10 < n {
		t.Fatalf("no-op path hit in only %d of %d seeds; want at least 10%%", noopSeeds, n)
	}
}

func TestMPGapHeavyLinearizable(t *testing.T) {
	for seed := uint64(1); seed <= numSeeds(mpSchedules); seed++ {
		checkLinearizable(t, seed, gapHeavy)
	}
}

func TestMPGapHeavyNegativeSkipPrepare(t *testing.T) {
	findMPViolation(t, func(o *mpOpts) { gapHeavy(o); o.SkipPrepare = true })
}

func TestMPGapHeavyNegativeIgnorePromisedValues(t *testing.T) {
	findMPViolation(t, func(o *mpOpts) { gapHeavy(o); o.IgnorePromised = true })
}

// The Observer only watches: the same seed replays to the same trace hash
// with and without observers, and the events they see are consistent.
func TestMPObserver(t *testing.T) {
	var elections, proposed, applied int
	for seed := uint64(1); seed <= numSeeds(200); seed++ {
		plain, _ := runMP(seed, nil)
		obs, _ := runMP(seed, func(o *mpOpts) { o.Observe = true })
		if plain.TraceHash() != obs.TraceHash() {
			t.Fatalf("seed=%d: an observer changed the run", seed)
		}
		for _, o := range obs.observers {
			if o.err != nil {
				t.Fatalf("seed=%d: %v", seed, o.err)
			}
			if o.outOfTurn > 0 {
				t.Fatalf("seed=%d: replica %d proposed %d entries while not leading", seed, o.id, o.outOfTurn)
			}
			elections, proposed, applied = elections+o.elections, proposed+o.proposed, applied+o.applied
		}
	}
	if elections == 0 || proposed == 0 || applied == 0 {
		t.Fatalf("observers saw %d elections, %d proposals, %d applies", elections, proposed, applied)
	}
	t.Logf("observers saw %d elections, %d proposals, %d applies", elections, proposed, applied)
}

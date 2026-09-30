package paxos_test

import (
	"math/rand/v2"
	"testing"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/transport"
)

const (
	schedules    = 1000
	chaosSteps   = 400
	livenessStep = 1500 // fixed step budget once faults stop
)

// runSchedule drives one seeded random schedule: chaos, then calm, then a
// bounded run to completion. It returns the cluster and the live nodes.
func runSchedule(seed uint64) (*cluster, []paxos.NodeID, bool) {
	opts := randomOpts(rand.New(rand.NewPCG(seed, 1)))
	c := newCluster(seed, opts)
	c.run(chaosSteps)
	live := c.calm()
	_, ok := c.runUntilDecided(live, livenessStep)
	return c, live, ok
}

// Test 1 (agreement) and test 2 (validity) over 1000 random schedules.
func TestAgreementAndValidityRandomSchedules(t *testing.T) {
	for seed := uint64(1); seed <= schedules; seed++ {
		c, _, _ := runSchedule(seed)
		if err := c.checkSafety(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
	}
}

// Test 2 (validity) on its own: every decision is some node's proposal.
func TestValidity(t *testing.T) {
	for seed := uint64(1); seed <= 200; seed++ {
		c, live, ok := runSchedule(seed)
		if !ok {
			t.Fatalf("seed=%d: not decided", seed)
		}
		v, _ := c.nodes[live[0]].Decided()
		if !c.proposed[v] {
			t.Fatalf("seed=%d: decided %q, proposed %v", seed, v, c.proposed)
		}
	}
}

// Test 3 (liveness): after faults stop, with a bare majority alive, every
// live node decides within the fixed step budget.
func TestLivenessAfterFaultsStop(t *testing.T) {
	worst := 0
	for seed := uint64(1); seed <= schedules; seed++ {
		opts := randomOpts(rand.New(rand.NewPCG(seed, 1)))
		c := newCluster(seed, opts)
		c.run(chaosSteps)
		live := c.calm()
		used, ok := c.runUntilDecided(live, livenessStep)
		if !ok {
			t.Fatalf("seed=%d: live nodes %v undecided after %d calm steps", seed, live, livenessStep)
		}
		worst = max(worst, used)
	}
	t.Logf("worst steps to decide after calm: %d (budget %d)", worst, livenessStep)
}

// Test 4: every node proposes a different value at the same instant.
func TestCompetingProposers(t *testing.T) {
	for seed := uint64(1); seed <= 300; seed++ {
		n := 3 + 2*int(seed%2)
		c := newCluster(seed, clusterOpts{
			N:         n,
			Proposers: n,
			Faults:    transport.Faults{MaxDelay: 4, DupProb: 0.1},
		})
		if got := len(c.proposed); got != n {
			t.Fatalf("seed=%d: %d proposals, want %d", seed, got, n)
		}
		if _, ok := c.runUntilDecided(c.ids, livenessStep); !ok {
			t.Fatalf("seed=%d: competing proposers did not converge", seed)
		}
		if err := c.checkSafety(); err != nil {
			t.Fatalf("seed=%d: %v", seed, err)
		}
		want, _ := c.nodes[1].Decided()
		for _, id := range c.ids {
			if v, _ := c.nodes[id].Decided(); v != want {
				t.Fatalf("seed=%d: node %d decided %q, node 1 decided %q", seed, id, v, want)
			}
		}
	}
}

// Test 5 (replay): the same seed yields the same trace hash; a different
// seed yields a different one.
func TestReplaySameSeedSameTrace(t *testing.T) {
	for _, seed := range []uint64{7, 42, 999} {
		a, _, _ := runSchedule(seed)
		b, _, _ := runSchedule(seed)
		if a.TraceHash() != b.TraceHash() {
			t.Fatalf("seed=%d: trace hashes differ: %s vs %s", seed, a.TraceHash(), b.TraceHash())
		}
	}
	a, _, _ := runSchedule(7)
	b, _, _ := runSchedule(8)
	if a.TraceHash() == b.TraceHash() {
		t.Fatal("different seeds produced identical traces; trace is not capturing the run")
	}
}

// findViolation runs seeds until checkSafety fails. The negative tests use
// it to prove the checker actually catches broken Paxos.
func findViolation(t *testing.T, mk func(seed uint64) *cluster) {
	t.Helper()
	for seed := uint64(1); seed <= schedules; seed++ {
		c := mk(seed)
		if err := c.checkSafety(); err != nil {
			t.Logf("violation caught as expected: seed=%d: %v", seed, err)
			return
		}
	}
	t.Fatalf("no violation in %d schedules: the checker or the schedule is too weak", schedules)
}

// Test 6a: an acceptor that loses its state on restart breaks agreement.
func TestNegativeForgetfulAcceptor(t *testing.T) {
	findViolation(t, func(seed uint64) *cluster {
		c := newCluster(seed, clusterOpts{
			N:                  3,
			Proposers:          3,
			Faults:             transport.Faults{MaxDelay: 6, DropProb: 0.1},
			Chaos:              chaos{CrashProb: 0.1, RestartProb: 0.3},
			ForgetOnRestart:    true,
			ReproposeOnRestart: true,
		})
		c.run(chaosSteps)
		live := c.calm()
		c.runUntilDecided(live, livenessStep)
		return c
	})
}

// Test 6b, promise rule part 1: promising a ballot lower than the current
// promise breaks agreement.
func TestNegativePromiseLowerBallot(t *testing.T) {
	findViolation(t, func(seed uint64) *cluster {
		return brokenRun(seed, clusterOpts{PromiseLower: true})
	})
}

// Test 6b, promise rule part 2: hiding the previously accepted value in the
// Promise reply breaks agreement.
func TestNegativeOmitAcceptedFromPromise(t *testing.T) {
	findViolation(t, func(seed uint64) *cluster {
		return brokenRun(seed, clusterOpts{OmitAccepted: true})
	})
}

// Test 6b: without the accept rule, agreement fails.
func TestNegativeNoAcceptRule(t *testing.T) {
	findViolation(t, func(seed uint64) *cluster {
		return brokenRun(seed, clusterOpts{NoAcceptRule: true})
	})
}

func brokenRun(seed uint64, o clusterOpts) *cluster {
	o.N = 3
	o.Proposers = 3
	o.Faults = transport.Faults{MaxDelay: 6, DropProb: 0.1}
	o.Chaos = chaos{CrashProb: 0.02, RestartProb: 0.2}
	o.ReproposeOnRestart = true
	c := newCluster(seed, o)
	c.run(chaosSteps)
	live := c.calm()
	c.runUntilDecided(live, livenessStep)
	return c
}

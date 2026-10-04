package txn

import (
	"math/rand/v2"
	"strings"
	"sync"
	"testing"

	"github.com/Shrutij516/paxos-txn-store/internal/transport"
)

const (
	txnSchedules = 1000
	chaosSteps   = 800
	calmLimit    = 8000
)

func randomOpts(rng *rand.Rand) worldOpts {
	return worldOpts{
		Shards:       DefaultShards,
		Clients:      4,
		OpsPerClient: 4 + rng.IntN(4),
		AuditPercent: 20,
		Faults: transport.Faults{
			DropProb: rng.Float64() * 0.1,
			DupProb:  rng.Float64() * 0.05,
			MaxDelay: 1 + rng.IntN(4),
		},
		Chaos: chaos{
			CrashProb:     rng.Float64() * 0.01,
			RestartProb:   0.05,
			PartitionProb: rng.Float64() * 0.005,
			HealProb:      0.05,
		},
	}
}

// runSchedule drives one seeded schedule: chaos with crashes, partitions
// and leader failovers, then calm until every client is done and nothing is
// left prepared. It reports whether that happened within the step limit.
func runSchedule(seed uint64, tweak func(*worldOpts)) (*world, bool) {
	o := randomOpts(rand.New(rand.NewPCG(seed, 5)))
	if tweak != nil {
		tweak(&o)
	}
	w := newWorld(seed, o)
	w.run(chaosSteps, true)
	w.calm()
	_, ok := w.runUntil(calmLimit, w.quiescent)
	return w, ok
}

func TestTxnSmoke(t *testing.T) {
	w := newWorld(1, worldOpts{Clients: 3, OpsPerClient: 10, AuditPercent: 20, Faults: transport.Faults{MaxDelay: 2}})
	used, ok := w.runUntil(5000, w.quiescent)
	if !ok {
		for _, c := range w.cli {
			t.Logf("client %d: ops left %d phase %d commits %d aborts %d", c.idx, c.ops, c.phase, c.commits, c.aborts)
		}
		t.Fatal("not quiescent")
	}
	commits, aborts := 0, 0
	for _, c := range w.cli {
		commits += c.commits
		aborts += c.aborts
	}
	t.Logf("quiescent after %d steps: %d commits, %d aborts", used, commits, aborts)
	for _, f := range []func() error{w.checkAtomicity, w.checkBank} {
		if err := f(); err != nil {
			t.Fatal(err)
		}
	}
	h, err := w.history()
	if err != nil {
		t.Fatal(err)
	}
	if err := checkSerializable(h); err != nil {
		t.Fatal(err)
	}
}

func checkAll(w *world) error {
	if err := w.checkAtomicity(); err != nil {
		return err
	}
	if err := w.checkBank(); err != nil {
		return err
	}
	h, err := w.history()
	if err != nil {
		return err
	}
	return checkSerializable(h)
}

// scheduleResult is what one seeded schedule produced.
type scheduleResult struct {
	quiet                              bool
	atomic, bank, serializable, strict error
	commits, aborts                    int
	leaderChanges                      int
	multiShardCommits                  int
	stats                              Stats
}

var (
	resultsOnce sync.Once
	results     []scheduleResult
)

// schedules runs the seeded random schedules once and shares the results
// between the atomicity, bank and serializability tests.
func schedules() []scheduleResult {
	resultsOnce.Do(func() {
		for seed := uint64(1); seed <= numSeeds(txnSchedules); seed++ {
			w, ok := runSchedule(seed, nil)
			r := scheduleResult{quiet: ok, atomic: w.checkAtomicity(), bank: w.checkBank()}
			if h, err := w.history(); err != nil {
				r.serializable, r.strict = err, err
			} else {
				r.serializable, r.strict = checkSerializable(h), checkStrict(h)
			}
			for _, c := range w.cli {
				r.commits += c.commits
				r.aborts += c.aborts
			}
			for _, n := range w.allServers {
				r.leaderChanges += n.leaderTerm
				r.stats.Wounds += n.stats.Wounds
				r.stats.WoundReqs += n.stats.WoundReqs
				r.stats.Queries += n.stats.Queries
				r.stats.PresumedAborts += n.stats.PresumedAborts
				r.stats.RebuiltPrepared += n.stats.RebuiltPrepared
			}
			for _, sm := range w.canonical() {
				for _, id := range sm.Decisions() {
					if c, _ := sm.Decided(id); c {
						r.multiShardCommits++
					}
				}
			}
			results = append(results, r)
		}
	})
	return results
}

// Test 1: atomicity across seeded schedules with crashes, partitions and
// leader failovers. Clients must also all finish once faults stop.
func TestTxnAtomicity(t *testing.T) {
	var commits, aborts, multi int
	seedsWith := map[string]int{}
	for i, r := range schedules() {
		for name, v := range map[string]int{"wounds": r.stats.Wounds, "wound requests": r.stats.WoundReqs,
			"outcome queries": r.stats.Queries, "presumed aborts": r.stats.PresumedAborts,
			"prepared txns rebuilt by a new leader": r.stats.RebuiltPrepared} {
			if v > 0 {
				seedsWith[name]++
			}
		}
		if !r.quiet {
			t.Fatalf("seed=%d: not quiescent within %d calm steps", i+1, calmLimit)
		}
		if r.atomic != nil {
			t.Fatalf("seed=%d: %v", i+1, r.atomic)
		}
		commits, aborts, multi = commits+r.commits, aborts+r.aborts, multi+r.multiShardCommits
	}
	t.Logf("%d schedules: %d commits (%d multi-shard), %d aborts", len(schedules()), commits, multi, aborts)
	for _, name := range sortedKeys(seedsWith) {
		t.Logf("schedules with %s: %d", name, seedsWith[name])
	}
}

// Test 2: bank invariant across the same schedules.
func TestTxnBankInvariant(t *testing.T) {
	for i, r := range schedules() {
		if r.bank != nil {
			t.Fatalf("seed=%d: %v", i+1, r.bank)
		}
	}
}

// Test 3: the committed history of every schedule is serializable.
func TestTxnSerializable(t *testing.T) {
	for i, r := range schedules() {
		if r.serializable != nil {
			t.Fatalf("seed=%d: %v", i+1, r.serializable)
		}
	}
}

// Test 3, strict: adding real-time edges (a txn whose commit the client saw
// precedes every txn that began afterwards) keeps every history acyclic, so
// the system is strictly serializable in these runs.
func TestTxnStrictSerializable(t *testing.T) {
	for i, r := range schedules() {
		if r.strict != nil {
			t.Fatalf("seed=%d: %v", i+1, r.strict)
		}
	}
}

// Controls for the G1a, G1b and real-time checks: each tampered history
// must be rejected with the matching error.
func TestTxnCheckerControls(t *testing.T) {
	w, _ := runSchedule(1, nil)
	base, err := w.history()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		tamper func(*history) bool
		check  func(*history) error
		want   string
	}{
		{"G1a", tamperG1a, checkSerializable, "G1a"},
		{"G1b", tamperG1b, checkSerializable, "G1b"},
		{"real-time", tamperRealTime, checkStrict, "not strictly serializable"},
	} {
		h := cloneHistory(base)
		if !tc.tamper(h) {
			t.Fatalf("%s: no suitable txns to tamper with", tc.name)
		}
		err := tc.check(h)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: tampered history gave %v, want an error mentioning %q", tc.name, err, tc.want)
		}
	}
	if err := checkStrict(base); err != nil {
		t.Fatalf("untampered history rejected: %v", err)
	}
}

// Test 3 control: a real history with an injected write-skew cycle must be
// rejected, so the checker is not vacuous.
func TestTxnSerializabilityCheckerCatchesTamperedHistory(t *testing.T) {
	w, _ := runSchedule(1, nil)
	h, err := w.history()
	if err != nil {
		t.Fatal(err)
	}
	if err := checkSerializable(h); err != nil {
		t.Fatalf("untampered history rejected: %v", err)
	}
	if !tamper(h) {
		t.Fatal("history has no pair of writers to tamper with")
	}
	if err := checkSerializable(h); err == nil {
		t.Fatal("checker accepted a history with a write-skew cycle")
	}
}

func TestTxnReplay(t *testing.T) {
	a, _ := runSchedule(9, nil)
	b, _ := runSchedule(9, nil)
	c, _ := runSchedule(10, nil)
	if a.traceHash() != b.traceHash() {
		t.Fatal("same seed, different trace")
	}
	if a.traceHash() == c.traceHash() {
		t.Fatal("different seeds, same trace")
	}
}

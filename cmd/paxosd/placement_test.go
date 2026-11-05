package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
)

// placementEnv, when set, enables TestMultiProcessConcentratedLeadership.
// It takes a minute or two, so neither -short nor make cover runs it.
const placementEnv = "PAXOSD_PLACEMENT"

// paxosdBinEnv, if set, names a paxosd binary to test instead of building
// one: TestMultiProcessConcentratedLeadership with a binary built with the
// old retransmission fails (docs/benchmarks.md, DECISIONS.md I5).
const paxosdBinEnv = "PAXOSD_BIN"

// maxInFlight bounds paxos_accepts_in_flight, the Accepts a leader has
// outstanding to its slowest peer, during the run: a sanity bound against
// a backlog that grows without limit. With 32 closed-loop clients a shard
// has at most a few dozen proposals in flight, since every client waits
// for its records. Measured peaks: 26–46 with the fix, 44–68 with the old
// code. The closed loop caps the old code's backlog too, so this check
// does not tell the two apart; the drop check does.
const maxInFlight = 128

// TestMultiProcessConcentratedLeadership is the regression test for
// incident I5. It runs the benchmark load (loadgen, uniform keys, half the
// transactions cross-shard, 32 clients) against fresh three-node clusters
// with leadership concentrated (one node leads all three shards) and
// spread (every node leads one), alternating. It passes or fails only on
// deterministic signals of the feedback loop, checked in every run: no
// peer message dropped for any reason during the load
// (peer_messages_dropped_total), and paxos_accepts_in_flight never above
// maxInFlight, sampled every 100 ms. Throughput is machine-dependent, so
// the concentrated/spread ratio is reported, not checked. With PAXOSD_BIN
// set to a build of the old resend-everything heartbeat it fails on drops
// and in-flight.
//
// Every node runs with GOMAXPROCS=1. Whether the old code's queues
// overflowed depended on the host: on a fast one the leader node kept up
// and nothing was dropped (DECISIONS.md I7). One core per node makes the
// node leading every shard the bottleneck on any host, so the old code
// reliably drops and the check means the same everywhere.
func TestMultiProcessConcentratedLeadership(t *testing.T) {
	if os.Getenv(placementEnv) == "" {
		t.Skip("set PAXOSD_PLACEMENT=1 to run")
	}
	bin := os.Getenv(paxosdBinEnv)
	if bin == "" {
		bin = buildPaxosd(t)
	}
	loadgen := filepath.Join(t.TempDir(), "loadgen")
	if out, err := exec.Command("go", "build", "-o", loadgen, "../loadgen").CombinedOutput(); err != nil {
		t.Fatalf("go build loadgen: %v\n%s", err, out)
	}
	const runs = 2
	rate := map[bool]float64{}
	for i := 0; i < 2*runs; i++ {
		concentrated := i%2 == 0
		name := map[bool]string{false: "spread", true: "concentrated"}[concentrated]
		t.Run(fmt.Sprintf("%d-%s", i/2, name), func(t *testing.T) {
			r := placementRun(t, bin, loadgen, concentrated, uint64(i/2)+1)
			if r.drops > 0 {
				t.Errorf("%.0f peer messages dropped during the load (%v), want 0", r.drops, r.byReason)
			}
			if r.inFlight > maxInFlight {
				t.Errorf("paxos_accepts_in_flight reached %.0f, want at most %d", r.inFlight, maxInFlight)
			}
			rate[concentrated] += r.rate / runs
		})
	}
	// Reported, not checked: about 0.78 with the fix and 0.43 before it in
	// the 5-run benchmark (docs/benchmarks.md).
	t.Logf("committed txns/s: concentrated %.1f, spread %.1f (concentrated/spread %.2f)", rate[true], rate[false], rate[true]/rate[false])
}

// placementResult is what one run measured.
type placementResult struct {
	rate     float64            // committed transactions per second
	drops    float64            // peer messages dropped during the load, all reasons
	byReason map[string]float64 // the same by reason
	inFlight float64            // highest paxos_accepts_in_flight sampled
}

// dropsByReason sums peer_messages_dropped_total over the nodes by reason.
func dropsByReason(t *testing.T, procs []*proc) map[string]float64 {
	out := map[string]float64{}
	for _, f := range scrapeAll(t, procs) {
		for _, m := range f["peer_messages_dropped_total"].GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "reason" {
					out[lp.GetValue()] += m.GetCounter().GetValue()
				}
			}
		}
	}
	return out
}

// placementRun starts fresh clusters until leadership is concentrated or
// spread as asked, then runs the load and measures it.
func placementRun(t *testing.T, bin, loadgen string, concentrated bool, seed uint64) placementResult {
	for try := 1; try <= 60; try++ {
		dir := t.TempDir()
		procs := make([]*proc, numNodes)
		for i := range procs {
			id := paxos.NodeID(i + 1)
			procs[i] = &proc{id: id, addr: freeAddr(t), maddr: freeAddr(t), dir: filepath.Join(dir, fmt.Sprintf("n%d", id)), log: filepath.Join(dir, fmt.Sprintf("n%d.log", id))}
		}
		cfg := writeConfig(t, dir, procs)
		for _, p := range procs {
			p.args = []string{"-config", cfg, "-id", fmt.Sprint(p.id), "-data-dir", p.dir, "-metrics-listen", p.maddr, "-trace-sample", "0"}
			p.env = []string{"GOMAXPROCS=1"}
			p.start(t, bin)
		}
		stopAll := func() {
			for _, p := range procs {
				_ = p.cmd.Process.Kill()
				_, _ = p.wait(5 * time.Second)
			}
		}
		ready := true
		for _, p := range procs {
			if _, ok := awaitReady(t, p, 30*time.Second); !ok {
				b, _ := os.ReadFile(p.log)
				t.Logf("try %d: node %d not ready after 30s, starting a new cluster; its log ends:\n%s", try, p.id, b[max(0, len(b)-2000):])
				ready = false
				break
			}
		}
		if !ready {
			stopAll()
			continue
		}
		count := map[paxos.NodeID]int{}
		for sh := txn.ShardID(0); sh < numShards; sh++ {
			_, l := leadersNow(t, procs, sh)
			count[l[sh]]++
		}
		most := 0
		for _, n := range count {
			most = max(most, n)
		}
		if (concentrated && most != numShards) || (!concentrated && most != 1) {
			stopAll()
			continue
		}
		t.Logf("try %d: leaders per node %v", try, count)
		var addrs, maddrs []string
		for _, p := range procs {
			addrs, maddrs = append(addrs, p.addr), append(maddrs, p.maddr)
		}
		// Drops before the load (connection attempts while the nodes were
		// starting) do not count.
		before := dropsByReason(t, procs)
		report := filepath.Join(dir, "report.json")
		lg := exec.Command(loadgen, "-addrs", strings.Join(addrs, ","), "-metrics-addrs", strings.Join(maddrs, ","),
			"-clients", "32", "-short-clients", "0", "-audit-percent", "0", "-keys", "10000", "-profile", "low",
			"-cross-shard-ratio", "0.5", "-warmup", "5s", "-duration", "15s", "-seed", fmt.Sprint(seed),
			"-out", filepath.Join(dir, "history.jsonl"), "-report", report)
		var out strings.Builder
		lg.Stdout, lg.Stderr = &out, &out
		if err := lg.Start(); err != nil {
			stopAll()
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- lg.Wait() }()
		var res placementResult
		for sampling := true; sampling; {
			select {
			case err := <-done:
				if err != nil {
					stopAll()
					t.Fatalf("loadgen: %v\n%s", err, out.String())
				}
				sampling = false
			case <-time.After(100 * time.Millisecond):
				for _, f := range scrapeAll(t, procs) {
					for _, m := range f["paxos_accepts_in_flight"].GetMetric() {
						res.inFlight = max(res.inFlight, m.GetGauge().GetValue())
					}
				}
			}
		}
		res.byReason = map[string]float64{}
		for r, n := range dropsByReason(t, procs) {
			if d := n - before[r]; d > 0 {
				res.byReason[r] = d
				res.drops += d
			}
		}
		stopAll()
		b, err := os.ReadFile(report)
		if err != nil {
			t.Fatal(err)
		}
		var rep struct {
			CommittedPerSec float64 `json:"committed_per_sec"`
		}
		if err := json.Unmarshal(b, &rep); err != nil {
			t.Fatal(err)
		}
		res.rate = rep.CommittedPerSec
		t.Logf("%.1f committed txns/s; peer messages dropped during the load: %.0f %v; highest paxos_accepts_in_flight: %.0f",
			res.rate, res.drops, res.byReason, res.inFlight)
		return res
	}
	t.Fatalf("no cluster with the wanted leader placement in 60 tries")
	return placementResult{}
}

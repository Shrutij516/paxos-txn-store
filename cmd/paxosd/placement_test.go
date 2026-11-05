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
	"github.com/Shrutij516/paxos-txn-store/internal/server"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
)

// placementEnv, when set, enables TestMultiProcessConcentratedLeadership.
// It takes a minute or two, so neither -short nor make cover runs it.
const placementEnv = "PAXOSD_PLACEMENT"

// paxosdBinEnv, if set, names a paxosd binary to test instead of building
// one: TestMultiProcessConcentratedLeadership with a binary built with the
// old retransmission fails (docs/benchmarks.md, DECISIONS.md I5).
const paxosdBinEnv = "PAXOSD_BIN"

// TestMultiProcessConcentratedLeadership is the regression test for
// incident I5. It runs the benchmark load (loadgen, uniform keys, half the
// transactions cross-shard, 32 clients) against fresh three-node clusters
// with leadership spread (every node leads one shard) and concentrated
// (one node leads all three), alternating, and requires that the
// concentrated runs (a) drop no more than a handful of peer messages on
// full send queues, where the old code dropped ~15,000 a run and fed its
// own backlog, and (b) commit at least 80% of the spread runs' rate.
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
	var drops float64
	for i := 0; i < 2*runs; i++ {
		concentrated := i%2 == 1
		name := map[bool]string{false: "spread", true: "concentrated"}[concentrated]
		t.Run(fmt.Sprintf("%d-%s", i/2, name), func(t *testing.T) {
			r, d := placementRun(t, bin, loadgen, concentrated, uint64(i/2)+1)
			rate[concentrated] += r / runs
			if concentrated {
				drops += d
			}
		})
	}
	t.Logf("committed txns/s: spread %.1f, concentrated %.1f (%.0f%%); peer messages dropped on full send queues in the concentrated runs: %.0f",
		rate[false], rate[true], 100*rate[true]/rate[false], drops)
	if drops > 100*runs {
		t.Errorf("concentrated leadership dropped %.0f peer messages on full send queues, want at most %d", drops, 100*runs)
	}
	if rate[true] < 0.8*rate[false] {
		t.Errorf("concentrated leadership committed %.1f txns/s, under 80%% of spread (%.1f)", rate[true], rate[false])
	}
}

// placementRun starts fresh clusters until leadership is concentrated or
// spread as asked, runs the load, and returns the committed rate and the
// peer messages the cluster dropped on full send queues.
func placementRun(t *testing.T, bin, loadgen string, concentrated bool, seed uint64) (float64, float64) {
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
		report := filepath.Join(dir, "report.json")
		out, err := exec.Command(loadgen, "-addrs", strings.Join(addrs, ","), "-metrics-addrs", strings.Join(maddrs, ","),
			"-clients", "32", "-short-clients", "0", "-audit-percent", "0", "-keys", "10000", "-profile", "low",
			"-cross-shard-ratio", "0.5", "-warmup", "5s", "-duration", "15s", "-seed", fmt.Sprint(seed),
			"-out", filepath.Join(dir, "history.jsonl"), "-report", report).CombinedOutput()
		if err != nil {
			stopAll()
			t.Fatalf("loadgen: %v\n%s", err, out)
		}
		drops := 0.0
		for _, f := range scrapeAll(t, procs) {
			for _, m := range f["peer_messages_dropped_total"].GetMetric() {
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "reason" && lp.GetValue() == server.DropSendQueueFull {
						drops += m.GetCounter().GetValue()
					}
				}
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
		t.Logf("%.1f committed txns/s, %.0f peer messages dropped on full send queues", rep.CommittedPerSec, drops)
		return rep.CommittedPerSec, drops
	}
	t.Fatalf("no cluster with the wanted leader placement in 60 tries")
	return 0, 0
}

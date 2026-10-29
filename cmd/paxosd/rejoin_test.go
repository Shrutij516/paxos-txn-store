package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
)

// rejoinBound is how long a restarted node may take to become ready
// (/readyz 200) again: start-up, learning every shard's leader from its
// heartbeats, and catching up an idle log. Before peer connections had a
// capped reconnect backoff it took up to two minutes (DECISIONS.md I4).
const rejoinBound = 5 * time.Second

// rejoinDownEnv overrides the downtimes TestMultiProcessRejoin uses, as a
// comma-separated list of Go durations (for a quick run: "20s").
const rejoinDownEnv = "PAXOSD_REJOIN_DOWN"

// TestMultiProcessRejoin runs three real processes with no load, SIGKILLs
// the node leading the most shards, keeps it down for a while (60 s and
// 180 s, as in the first AWS run), restarts it with the same address and
// data, and requires /readyz to answer 200 within rejoinBound. A restarted
// node learns each shard's leader only from that leader's heartbeats, which
// travel over the leader's outbound connection to it, so this measures how
// fast the survivors reconnect to a peer that was gone.
func TestMultiProcessRejoin(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-process rejoin test (minutes of downtime)")
	}
	downs := []time.Duration{60 * time.Second, 180 * time.Second}
	if v := os.Getenv(rejoinDownEnv); v != "" {
		downs = nil
		for _, s := range strings.Split(v, ",") {
			d, err := time.ParseDuration(s)
			if err != nil {
				t.Fatalf("%s: %v", rejoinDownEnv, err)
			}
			downs = append(downs, d)
		}
	}
	bin := buildPaxosd(t)
	for _, down := range downs {
		t.Run(fmt.Sprintf("down=%v", down), func(t *testing.T) {
			t.Parallel()
			took := rejoin(t, bin, down)
			if took > rejoinBound {
				t.Fatalf("node took %v to become ready after %v down, want at most %v", took.Round(100*time.Millisecond), down, rejoinBound)
			}
		})
	}
}

// rejoin starts a cluster, kills and restarts a leader after down, and
// returns how long the restarted node took to become ready.
func rejoin(t *testing.T, bin string, down time.Duration) time.Duration {
	dir := t.TempDir()
	procs := make([]*proc, numNodes)
	for i := range procs {
		id := paxos.NodeID(i + 1)
		procs[i] = &proc{id: id, addr: freeAddr(t), maddr: freeAddr(t), dir: filepath.Join(dir, fmt.Sprintf("n%d", id)), log: filepath.Join(dir, fmt.Sprintf("n%d.log", id))}
	}
	cfg := writeConfig(t, dir, procs)
	for _, p := range procs {
		p.args = []string{"-config", cfg, "-id", fmt.Sprint(p.id), "-data-dir", p.dir, "-tick", tick.String(), "-metrics-listen", p.maddr, "-trace-sample", "0"}
		p.start(t, bin)
	}
	defer func() {
		for _, p := range procs {
			_ = p.cmd.Process.Kill() // no-op if already exited
			_, _ = p.wait(5 * time.Second)
		}
	}()
	for _, p := range procs {
		if _, ok := awaitReady(t, p, 30*time.Second); !ok {
			t.Fatalf("node %d never became ready", p.id)
		}
	}

	// The victim leads the most shards, as on AWS: the survivors must
	// elect new leaders and later reach it again.
	_, led := leadersNow(t, procs, 0)
	count := map[paxos.NodeID]int{}
	for sh := txn.ShardID(0); sh < numShards; sh++ {
		if _, l := leadersNow(t, procs, sh); l[sh] != 0 {
			led[sh] = l[sh]
		}
		count[led[sh]]++
	}
	victim := procs[0]
	for _, p := range procs {
		if count[p.id] > count[victim.id] {
			victim = p
		}
	}
	t.Logf("leaders before the kill: %v; killing node %d", led, victim.id)
	victim.kill(t)
	time.Sleep(down)
	var survivors []*proc
	for _, p := range procs {
		if p != victim {
			survivors = append(survivors, p)
		}
	}
	after := map[txn.ShardID]paxos.NodeID{}
	for sh := txn.ShardID(0); sh < numShards; sh++ {
		_, l := leadersNow(t, survivors, sh)
		after[sh] = l[sh]
	}
	t.Logf("leaders after %v down: %v; restarting node %d", down, after, victim.id)
	victim.start(t, bin)
	took, ok := awaitReady(t, victim, 5*time.Minute)
	if !ok {
		t.Fatalf("node %d not ready %v after its restart", victim.id, took)
	}
	t.Logf("node %d ready %v after its restart (down %v)", victim.id, took.Round(10*time.Millisecond), down)
	return took
}

// awaitReady polls p's /readyz every 100 ms until it answers 200 or limit
// passes, logging each change of the answer, and returns the time taken.
func awaitReady(t *testing.T, p *proc, limit time.Duration) (time.Duration, bool) {
	t.Helper()
	start := time.Now()
	last := ""
	for time.Since(start) < limit {
		answer := "no answer"
		resp, err := http.Get("http://" + p.maddr + "/readyz")
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return time.Since(start), true
			}
			answer = strings.TrimSpace(string(b))
		}
		if answer != last {
			t.Logf("node %d at +%v: %s", p.id, time.Since(start).Round(100*time.Millisecond), answer)
			last = answer
		}
		time.Sleep(100 * time.Millisecond)
	}
	return time.Since(start), false
}

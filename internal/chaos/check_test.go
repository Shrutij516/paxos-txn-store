package chaos

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Shrutij516/paxos-txn-store/history"
	"github.com/Shrutij516/paxos-txn-store/internal/testcluster"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
)

// runLoadgen builds and runs cmd/loadgen against a 3-node, 3-shard
// in-process cluster for a few seconds, stopping and restarting the leader
// of shard 0 halfway, and returns the history and the final shard state.
func runLoadgen(t *testing.T) (history.Meta, []history.Attempt, map[txn.ShardID]*txn.SM) {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "loadgen")
	if out, err := exec.Command("go", "build", "-o", bin, "../../cmd/loadgen").CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	c := testcluster.Start(t, 3, 3, nil)
	hist := filepath.Join(t.TempDir(), "history.jsonl")
	cmd := exec.Command(bin, "-addrs", strings.Join(c.Addrs, ","), "-duration", "4s", "-out", hist)
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	c.Stop(c.IDs[0])
	time.Sleep(time.Second)
	c.Restart(c.IDs[0])
	if err := cmd.Wait(); err != nil {
		t.Fatalf("loadgen: %v\n%s", err, logs.String())
	}
	t.Log(strings.TrimSpace(logs.String()))
	time.Sleep(2 * time.Second) // let prepared transactions resolve
	var dirs []string
	for _, id := range c.IDs {
		dirs = append(dirs, c.DataDir(id))
	}
	c.StopAll()
	f, err := os.Open(hist)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	meta, atts, err := history.Read(f)
	if err != nil {
		t.Fatal(err)
	}
	sms, err := LoadShards(dirs, 3)
	if err != nil {
		t.Fatal(err)
	}
	return meta, atts, sms
}

// TestCheckRealRun: a real loadgen history against a real cluster with a
// node stopped mid-run passes every check; tampered versions of it fail.
func TestCheckRealRun(t *testing.T) {
	meta, atts, sms := runLoadgen(t)
	r := Check(meta, atts, sms)
	if len(r.Violations) > 0 {
		t.Fatalf("violations in a correct run: %v", r.Violations)
	}
	if r.Committed == 0 || r.CheckedTxns == 0 || meta.Accounts != 12 {
		t.Fatalf("report %+v, meta %+v", r, meta)
	}
	t.Logf("report: %+v", r)

	// Controls: lie about one attempt's outcome, and about a read.
	for _, tc := range []struct {
		name   string
		tamper func([]history.Attempt) bool
		want   string
	}{
		{"aborted reported committed", func(as []history.Attempt) bool {
			for i := range as {
				if as[i].Outcome == history.Aborted {
					as[i].Outcome, as[i].End = history.Committed, as[i].Finish
					return true
				}
			}
			return false
		}, "no shard committed it"},
		{"committed reported aborted", func(as []history.Attempt) bool {
			for i := range as {
				if as[i].Outcome == history.Committed && !as[i].Setup {
					as[i].Outcome, as[i].End = history.Aborted, -1
					return true
				}
			}
			return false
		}, "but it did"},
		{"read of a version nobody wrote", func(as []history.Attempt) bool {
			for i := range as {
				if as[i].Outcome == history.Committed && len(as[i].Reads) > 0 {
					for k := range as[i].Reads {
						as[i].Reads[k] = 1 << 40
					}
					return true
				}
			}
			return false
		}, "G1a"},
	} {
		cp := make([]history.Attempt, len(atts))
		for i, a := range atts {
			cp[i] = a
			cp[i].Reads = map[string]uint64{}
			for k, v := range a.Reads {
				cp[i].Reads[k] = v
			}
		}
		if !tc.tamper(cp) {
			t.Fatalf("%s: nothing to tamper with", tc.name)
		}
		r := Check(meta, cp, sms)
		found := false
		for _, v := range r.Violations {
			found = found || strings.Contains(v, tc.want)
		}
		if !found {
			t.Fatalf("%s: violations %v, want one containing %q", tc.name, r.Violations, tc.want)
		}
	}
}

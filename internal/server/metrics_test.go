package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/Shrutij516/paxos-txn-store/client"
	"github.com/Shrutij516/paxos-txn-store/internal/testcluster"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
)

// wantMetrics is every metric of the store a node exports
// (docs/observability.md). The Go runtime and process collectors add the
// standard go_* and process_* metrics on top.
var wantMetrics = []string{
	"paxos_leader_elections_total", "paxos_is_leader", "paxos_commit_latency_seconds",
	"paxos_apply_lag_slots", "paxos_log_length_slots",
	"txn_commits_total", "txn_cross_shard_commits_total", "txn_aborts_total",
	"txn_prepare_phase_seconds", "txn_decide_phase_seconds", "txn_participant_prepare_seconds",
	"txn_outcome_query_seconds", "txn_in_doubt", "txn_wounds_total", "txn_lock_waits_total",
	"storage_fsync_seconds", "shard_loop_queue_depth",
}

// ownMetric reports whether a metric is the store's (not a runtime one).
func ownMetric(name string) bool {
	for _, p := range []string{"paxos_", "txn_", "storage_", "shard_"} {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// TestMetricLabels runs a contended workload with single-shard and
// cross-shard transactions, then checks every metric of every node,
// including the Go runtime and process ones. Only bounded labels exist:
// shard and node (from the cluster config), reason (txn.AbortReasons),
// quantile (the fixed GC duration quantiles) and version (the Go version).
// No transaction ID, key or client ever becomes a label. The store's own
// metrics carry shard and node, with one series per shard (per reason for
// aborts).
func TestMetricLabels(t *testing.T) {
	c := testcluster.Start(t, 3, shards, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	keys := []string{keyOn(0, 0), keyOn(0, 1), keyOn(1, 0), keyOn(2, 0)}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		cl := newClient(t, c, client.Options{MaxAttempts: 50})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				a, b := keys[(w+i)%len(keys)], keys[(w+i+1)%len(keys)]
				if _, err := transfer(ctx, cl, a, b, 1); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()

	var commits, aborts float64
	for _, id := range c.IDs {
		fams, err := c.Nodes[id].Registry().Gather()
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		own := 0
		for _, f := range fams {
			name := f.GetName()
			seen[name] = true
			want := 0
			if ownMetric(name) {
				own++
				want = shards
				if name == "txn_aborts_total" {
					want = shards * len(txn.AbortReasons)
				}
			}
			if want > 0 && len(f.GetMetric()) != want {
				t.Errorf("node %d: %s has %d series, want %d", id, name, len(f.GetMetric()), want)
			}
			for _, m := range f.GetMetric() {
				if err := checkLabels(name, m, id); err != nil {
					t.Errorf("node %d: %s: %v", id, name, err)
				}
				switch name {
				case "txn_commits_total":
					commits += m.GetCounter().GetValue()
				case "txn_aborts_total":
					aborts += m.GetCounter().GetValue()
				}
			}
		}
		for _, name := range append(wantMetrics, "go_goroutines", "go_info", "go_gc_duration_seconds", "process_cpu_seconds_total") {
			if !seen[name] {
				t.Errorf("node %d does not export %s", id, name)
			}
		}
		if own != len(wantMetrics) {
			t.Errorf("node %d exports %d store metrics, want %d (update wantMetrics and the docs)", id, own, len(wantMetrics))
		}
	}
	if commits < 40 {
		t.Fatalf("txn_commits_total sums to %v over the cluster, want at least the 40 transfers", commits)
	}
	t.Logf("cluster: %v commits, %v aborts", commits, aborts)
}

// checkLabels allows only bounded label names, each with a value from its
// fixed set; the store's metrics must have shard and node.
func checkLabels(name string, m *dto.Metric, self any) error {
	reasons := map[string]bool{}
	for _, r := range txn.AbortReasons {
		reasons[string(r)] = true
	}
	quantiles := map[string]bool{"0": true, "0.25": true, "0.5": true, "0.75": true, "1": true}
	got := map[string]bool{}
	for _, l := range m.GetLabel() {
		got[l.GetName()] = true
		v := l.GetValue()
		ok := false
		switch l.GetName() {
		case "shard":
			n, err := strconv.Atoi(v)
			ok = err == nil && n >= 0 && n < shards
		case "node":
			ok = v == fmt.Sprint(self)
		case "reason":
			ok = name == "txn_aborts_total" && reasons[v]
		case "quantile":
			ok = quantiles[v]
		case "version":
			ok = name == "go_info" && v == runtime.Version()
		default:
			return fmt.Errorf("label %q is not one of the bounded labels", l.GetName())
		}
		if !ok {
			return fmt.Errorf("label %s=%q is outside its fixed set", l.GetName(), v)
		}
	}
	if ownMetric(name) && (!got["shard"] || !got["node"]) {
		return fmt.Errorf("labels %v lack shard or node", m.GetLabel())
	}
	return nil
}

// TestDashboardUsesRealMetrics: every metric the Grafana dashboard queries
// is one a node exports, so a rename cannot silently empty a panel.
func TestDashboardUsesRealMetrics(t *testing.T) {
	raw, err := os.ReadFile("../../deploy/grafana/dashboard.json")
	if err != nil {
		t.Fatal(err)
	}
	var dash struct {
		Panels []struct {
			Title   string
			Targets []struct{ Expr string }
		}
	}
	if err := json.Unmarshal(raw, &dash); err != nil {
		t.Fatal(err)
	}
	known := map[string]bool{}
	for _, m := range wantMetrics {
		known[m] = true
	}
	name := regexp.MustCompile(`\b(?:paxos|txn|storage|shard)_[a-z_]+`)
	for _, p := range dash.Panels {
		for _, tg := range p.Targets {
			for _, m := range name.FindAllString(tg.Expr, -1) {
				m = strings.TrimSuffix(m, "_bucket")
				if !known[m] {
					t.Errorf("panel %q queries %s, which no node exports", p.Title, m)
				}
			}
		}
	}
	if len(dash.Panels) != 7 {
		t.Fatalf("dashboard has %d panels, want 7", len(dash.Panels))
	}
}

// The label check rejects unbounded labels and out-of-set values.
func TestCheckLabelsRejects(t *testing.T) {
	lp := func(n, v string) *dto.LabelPair { return &dto.LabelPair{Name: &n, Value: &v} }
	for _, tc := range []struct {
		name   string
		labels []*dto.LabelPair
	}{
		{"txn_commits_total", []*dto.LabelPair{lp("shard", "0"), lp("node", "1"), lp("txn_id", "42")}},
		{"txn_commits_total", []*dto.LabelPair{lp("shard", "0"), lp("node", "1"), lp("key", "alice")}},
		{"txn_commits_total", []*dto.LabelPair{lp("shard", "0"), lp("node", "1"), lp("client", "7")}},
		{"txn_commits_total", []*dto.LabelPair{lp("shard", "9"), lp("node", "1")}},
		{"txn_commits_total", []*dto.LabelPair{lp("shard", "0"), lp("node", "2")}},
		{"txn_commits_total", []*dto.LabelPair{lp("shard", "0")}},
		{"txn_aborts_total", []*dto.LabelPair{lp("shard", "0"), lp("node", "1"), lp("reason", "because")}},
		{"txn_commits_total", []*dto.LabelPair{lp("shard", "0"), lp("node", "1"), lp("reason", "wounded")}},
		{"go_gc_duration_seconds", []*dto.LabelPair{lp("quantile", "0.9")}},
	} {
		if err := checkLabels(tc.name, &dto.Metric{Label: tc.labels}, 1); err == nil {
			t.Errorf("%s %v accepted", tc.name, tc.labels)
		}
	}
	if err := checkLabels("txn_aborts_total", &dto.Metric{Label: []*dto.LabelPair{lp("shard", "0"), lp("node", "1"), lp("reason", "wounded")}}, 1); err != nil {
		t.Errorf("a valid abort series rejected: %v", err)
	}
}

package server_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/Shrutij516/paxos-txn-store/client"
	"github.com/Shrutij516/paxos-txn-store/internal/testcluster"
)

// wantMetrics is every metric a node exports (docs/observability.md).
var wantMetrics = []string{
	"paxos_leader_elections_total", "paxos_is_leader", "paxos_commit_latency_seconds",
	"paxos_apply_lag_slots", "paxos_log_length_slots",
	"txn_commits_total", "txn_cross_shard_commits_total",
	"txn_aborts_wounded_total", "txn_aborts_lock_lost_total", "txn_aborts_validation_total",
	"txn_aborts_vote_no_total", "txn_aborts_timeout_total", "txn_aborts_presumed_total", "txn_aborts_unknown_total",
	"txn_prepare_phase_seconds", "txn_decide_phase_seconds", "txn_participant_prepare_seconds",
	"txn_outcome_query_seconds", "txn_in_doubt", "txn_wounds_total", "txn_lock_waits_total",
	"storage_fsync_seconds",
}

// TestMetricLabels runs a contended workload with single-shard and
// cross-shard transactions, then checks every metric of every node: its
// only labels are shard and node, with values from the cluster config, and
// it has exactly one series per shard. No transaction ID or key ever
// becomes a label value.
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

	nodes := map[string]bool{}
	for _, id := range c.IDs {
		nodes[strconv.Itoa(int(id))] = true
	}
	var commits float64
	for _, id := range c.IDs {
		fams, err := c.Nodes[id].Registry().Gather()
		if err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for _, f := range fams {
			seen[f.GetName()] = true
			if len(f.GetMetric()) != shards {
				t.Errorf("node %d: %s has %d series, want one per shard", id, f.GetName(), len(f.GetMetric()))
			}
			for _, m := range f.GetMetric() {
				if err := checkLabels(m, nodes, id); err != nil {
					t.Errorf("node %d: %s: %v", id, f.GetName(), err)
				}
				if f.GetName() == "txn_commits_total" {
					commits += m.GetCounter().GetValue()
				}
			}
		}
		for _, name := range wantMetrics {
			if !seen[name] {
				t.Errorf("node %d does not export %s", id, name)
			}
		}
		if len(seen) != len(wantMetrics) {
			t.Errorf("node %d exports %d metrics, want %d (update wantMetrics and the docs)", id, len(seen), len(wantMetrics))
		}
	}
	if commits < 40 {
		t.Fatalf("txn_commits_total sums to %v over the cluster, want at least the 40 transfers", commits)
	}
}

func checkLabels(m *dto.Metric, nodes map[string]bool, self any) error {
	if len(m.GetLabel()) != 2 {
		return fmt.Errorf("labels %v, want exactly shard and node", m.GetLabel())
	}
	for _, l := range m.GetLabel() {
		switch l.GetName() {
		case "shard":
			if n, err := strconv.Atoi(l.GetValue()); err != nil || n < 0 || n >= shards {
				return fmt.Errorf("shard label %q is not a shard", l.GetValue())
			}
		case "node":
			if !nodes[l.GetValue()] || l.GetValue() != fmt.Sprint(self) {
				return fmt.Errorf("node label %q is not this node", l.GetValue())
			}
		default:
			return fmt.Errorf("label %q is not shard or node", l.GetName())
		}
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
	name := regexp.MustCompile(`\b(?:paxos|txn|storage)_[a-z_]+`)
	for _, p := range dash.Panels {
		for _, tg := range p.Targets {
			for _, m := range name.FindAllString(tg.Expr, -1) {
				m = strings.TrimSuffix(m, "_bucket")
				if m == "txn_aborts_" { // the regex matcher over every abort counter
					continue
				}
				if !known[m] {
					t.Errorf("panel %q queries %s, which no node exports", p.Title, m)
				}
			}
		}
	}
	if len(dash.Panels) != 6 {
		t.Fatalf("dashboard has %d panels, want 6", len(dash.Panels))
	}
}

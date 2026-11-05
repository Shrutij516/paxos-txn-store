package main

import (
	"math/rand/v2"
	"testing"

	"github.com/Shrutij516/paxos-txn-store/api"
	"github.com/Shrutij516/paxos-txn-store/history"
)

// Percentiles are exact nearest-rank values over every sample.
func TestSummarize(t *testing.T) {
	xs := make([]float64, 100)
	for i := range xs {
		xs[i] = float64(100 - i) // 100..1, unsorted
	}
	s := summarize(xs)
	if s.Count != 100 || s.P50 != 50 || s.P90 != 90 || s.P99 != 99 || s.Max != 100 {
		t.Fatalf("summarize = %+v", s)
	}
	if one := summarize([]float64{7}); one.P50 != 7 || one.P99 != 7 || one.Max != 7 {
		t.Fatalf("one sample: %+v", one)
	}
	if empty := summarize(nil); empty.Count != 0 {
		t.Fatalf("empty: %+v", empty)
	}
}

// pair honours the cross-shard choice, never returns the same account
// twice, and -profile high sends about -hot-fraction of picks to the hot set.
func TestPicker(t *testing.T) {
	for _, profile := range []string{"low", "high"} {
		c := config{accounts: 1000, shards: 3, profile: profile, hotKeys: 16, hotFraction: 0.9}
		p := newPicker(c)
		rng := rand.New(rand.NewPCG(1, 2))
		hot := 0
		const n = 4000
		for i := 0; i < n; i++ {
			cross := i%2 == 0
			x, y := p.pair(rng, cross)
			if x == y {
				t.Fatalf("%s: same account twice: %d", profile, x)
			}
			sx, sy := api.ShardOf(history.Account(x), 3), api.ShardOf(history.Account(y), 3)
			if (sx != sy) != cross {
				t.Fatalf("%s: pair (%d, %d) on shards %d, %d, want cross=%v", profile, x, y, sx, sy, cross)
			}
			if x < c.hotKeys {
				hot++
			}
		}
		share := float64(hot) / n
		switch {
		case profile == "low" && share > 0.05:
			t.Fatalf("low: %.2f of first picks in the first %d accounts", share, c.hotKeys)
		case profile == "high" && (share < 0.85 || share > 0.95):
			t.Fatalf("high: %.2f of first picks in the hot set, want about 0.9", share)
		}
	}
}

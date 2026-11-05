package main

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/Shrutij516/paxos-txn-store/api"
	"github.com/Shrutij516/paxos-txn-store/client"
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

func TestTimeline(t *testing.T) {
	var file, out strings.Builder
	tl := newTimeline(&file, &out)
	t0 := time.UnixMilli(1_700_000_000_000)
	if err := tl.header(t0, t0.Add(10*time.Second), t0.Add(130*time.Second)); err != nil {
		t.Fatal(err)
	}
	tl.add(t0.Add(11*time.Second), 12.5, nil)
	tl.add(t0.Add(12*time.Second), 300, client.ErrUnknown)
	tl.add(t0.Add(13*time.Second), 40, fmt.Errorf("run: %w", client.ErrAborted))
	tl.add(t0.Add(14*time.Second), 41, errors.New("deadline"))
	tl.close()
	want := `{"start_unix_ms":1700000000000,"window_start_unix_ms":1700000010000,"window_end_unix_ms":1700000130000}
{"t":1700000011000,"ms":12.5,"outcome":"committed"}
{"t":1700000012000,"ms":300,"outcome":"unknown"}
{"t":1700000013000,"ms":40,"outcome":"aborted"}
{"t":1700000014000,"ms":41,"outcome":"error"}
`
	if file.String() != want {
		t.Fatalf("timeline:\n%s\nwant:\n%s", file.String(), want)
	}
}

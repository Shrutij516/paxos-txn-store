package txn

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

// Tests for the real-time edges of CheckStrict, which go through barrier
// nodes (DECISIONS.md entry 39), against the pairwise version they replaced.

// checkHistoryPairwise is checkHistory as it was before the barrier nodes:
// one real-time edge for every pair a, b with b.Begin > a.End, O(N^2). It is
// the reference oracle for the equivalence test and must not be used
// outside tests.
func checkHistoryPairwise(h *History, realTime bool) error {
	edges := map[ID]map[ID]bool{}
	add := func(a, b ID) {
		if a == b {
			return
		}
		if edges[a] == nil {
			edges[a] = map[ID]bool{}
		}
		edges[a][b] = true
	}
	versions := map[string][]WriteRec{}
	index := map[string]map[uint64]int{}
	final := map[string]map[ID]uint64{} // key -> writer -> last version it wrote
	for _, k := range sortedKeys(h.Order) {
		vs := append([]WriteRec{{Key: k, Ver: 0, Writer: 0}}, h.Order[k]...)
		versions[k] = vs
		index[k] = map[uint64]int{}
		final[k] = map[ID]uint64{}
		for i, v := range vs {
			index[k][v.Ver] = i
			final[k][v.Writer] = v.Ver
			if i > 0 {
				add(vs[i-1].Writer, v.Writer) // ww
			}
		}
	}
	for _, id := range sortedTxns(h.Txns) {
		for _, k := range sortedKeys(h.Txns[id].Reads) {
			ver := h.Txns[id].Reads[k]
			vs := versions[k]
			if vs == nil {
				vs = []WriteRec{{Key: k}}
				versions[k], index[k] = vs, map[uint64]int{0: 0}
				final[k] = map[ID]uint64{0: 0}
			}
			i, ok := index[k][ver]
			if !ok {
				return fmt.Errorf("G1a (aborted read): txn %d read %s at version %d, which no committed txn installed", id, k, ver)
			}
			w := vs[i].Writer
			if final[k][w] != ver {
				return fmt.Errorf("G1b (intermediate read): txn %d read %s at version %d, but its writer %d later wrote version %d", id, k, ver, w, final[k][w])
			}
			add(w, id) // wr
			if i+1 < len(vs) {
				add(id, vs[i+1].Writer) // rw
			}
		}
	}
	if realTime {
		ids := sortedTxns(h.Txns)
		for _, a := range ids {
			ta := h.Txns[a]
			if ta.End < 0 {
				continue
			}
			for _, b := range ids {
				if a != b && h.Txns[b].Begin > ta.End {
					add(a, b) // a finished before b started
				}
			}
		}
	}
	// Kahn's algorithm; anything left over sits on a cycle.
	nodes := map[ID]bool{0: true}
	for id := range h.Txns {
		nodes[id] = true
	}
	indeg := map[ID]int{}
	for _, a := range sortedTxns(edges) {
		for b := range edges[a] {
			indeg[b]++
		}
	}
	var queue []ID
	for _, id := range sortedTxns(nodes) {
		if indeg[id] == 0 {
			queue = append(queue, id)
		}
	}
	seen := 0
	for len(queue) > 0 {
		a := queue[0]
		queue = queue[1:]
		seen++
		for _, b := range sortedTxns(edges[a]) {
			if indeg[b]--; indeg[b] == 0 {
				queue = append(queue, b)
			}
		}
	}
	if seen != len(nodes) {
		var stuck []ID
		for _, id := range sortedTxns(nodes) {
			if indeg[id] > 0 {
				stuck = append(stuck, id)
			}
		}
		kind := "G-cycle (not serializable)"
		if realTime {
			kind = "cycle with real-time edges (not strictly serializable)"
		}
		return fmt.Errorf("%s: dependency cycle among %d txns, e.g. %v", kind, len(stuck), stuck[:min(len(stuck), 4)])
	}
	return nil
}

// randomHistory builds a small committed history: txns run serially in a
// random order, each reading and writing a few of three keys, with Begin
// and End drawn from a narrow range so equal timestamps are common and the
// real-time order often contradicts the serial order. With tamper, one read
// is redirected to some other committed version, which can add G1b, wr/rw
// cycles or real-time violations.
func randomHistory(rng *rand.Rand, tamper bool) *History {
	h := &History{Txns: map[ID]*HTxn{}, Order: map[string][]WriteRec{}}
	keys := []string{"x", "y", "z"}
	n := 2 + rng.IntN(7)
	ids := make([]ID, n)
	used := map[ID]bool{0: true}
	for i := range ids {
		for used[ids[i]] {
			if rng.IntN(4) == 0 {
				ids[i] = ID(1<<40 + rng.IntN(1000)) // sparse, large IDs too
			} else {
				ids[i] = ID(1 + rng.IntN(3*n))
			}
		}
		used[ids[i]] = true
	}
	rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
	ver := map[string]uint64{}
	slot := uint64(0)
	for _, id := range ids {
		t := &HTxn{Reads: map[string]uint64{}, End: -1}
		for _, k := range keys {
			if rng.IntN(2) == 0 {
				t.Reads[k] = ver[k]
			}
		}
		for _, k := range keys {
			if rng.IntN(3) == 0 {
				slot++
				ver[k] = slot
				t.Writes = append(t.Writes, k)
				h.Order[k] = append(h.Order[k], WriteRec{Key: k, Ver: slot, Writer: id})
			}
		}
		t.Begin = int64(rng.IntN(8))
		if rng.IntN(6) != 0 {
			t.End = t.Begin + int64(rng.IntN(4))
		}
		h.Txns[id] = t
	}
	if tamper {
		var all []WriteRec
		for _, k := range sortedKeys(h.Order) {
			all = append(all, h.Order[k]...)
		}
		if len(all) > 0 {
			wr := all[rng.IntN(len(all))]
			id := ids[rng.IntN(len(ids))]
			h.Txns[id].Reads[wr.Key] = wr.Ver
		}
	}
	return h
}

// The barrier checker must give exactly the pairwise checker's verdict,
// error text included (the same real txns are left on or after a cycle).
func TestRealTimeBarriersMatchPairwise(t *testing.T) {
	const cases = 20000
	rng := rand.New(rand.NewPCG(39, 39))
	verdicts := map[string]int{}
	for i := 0; i < cases; i++ {
		h := randomHistory(rng, i%2 == 1)
		for _, rt := range []bool{false, true} {
			want, got := checkHistoryPairwise(h, rt), checkHistory(h, rt)
			if fmt.Sprint(want) != fmt.Sprint(got) {
				t.Fatalf("case %d realTime=%v: pairwise %v, barriers %v\nhistory: %s", i, rt, want, got, dumpHistory(h))
			}
			if rt {
				v := "ok"
				if got != nil {
					v, _, _ = strings.Cut(got.Error(), ":")
				}
				verdicts[v]++
			}
		}
	}
	t.Logf("%d histories, strict verdicts: %v", cases, verdicts)
	if verdicts["ok"] < cases/10 || verdicts["cycle with real-time edges (not strictly serializable)"] < cases/10 {
		t.Fatalf("too few accepted or real-time-rejected histories to compare: %v", verdicts)
	}
}

func dumpHistory(h *History) string {
	var b strings.Builder
	for _, id := range sortedTxns(h.Txns) {
		t := h.Txns[id]
		fmt.Fprintf(&b, "\n  txn %d [%d, %d] reads %v writes %v", id, t.Begin, t.End, t.Reads, t.Writes)
	}
	for _, k := range sortedKeys(h.Order) {
		fmt.Fprintf(&b, "\n  %s: %v", k, h.Order[k])
	}
	return b.String()
}

// Ties: b began at the very moment a ended, so a does not precede b in
// real time ("b.Begin > a.End" is strict). b wrote x, which a read, so
// the only order is b, a: accepted at a tie, rejected one tick later.
func TestRealTimeTies(t *testing.T) {
	for _, tc := range []struct {
		bBegin int64
		ok     bool
	}{{5, true}, {6, false}, {4, true}} {
		h := &History{
			Txns: map[ID]*HTxn{
				1: {Reads: map[string]uint64{"x": 10}, Begin: 0, End: 5}, // a
				2: {Reads: map[string]uint64{}, Writes: []string{"x"}, Begin: tc.bBegin, End: tc.bBegin + 1},
			},
			Order: map[string][]WriteRec{"x": {{Key: "x", Ver: 10, Writer: 2}}},
		}
		for name, check := range map[string]func(*History, bool) error{"pairwise": checkHistoryPairwise, "barriers": checkHistory} {
			err := check(h, true)
			if (err == nil) != tc.ok {
				t.Fatalf("%s: b.Begin=%d a.End=5: got %v, want ok=%v", name, tc.bBegin, err, tc.ok)
			}
		}
	}
	// Many txns sharing one timestamp: every Begin at 3 sorts before every
	// End at 3, so none of them precedes another.
	h := &History{Txns: map[ID]*HTxn{}, Order: map[string][]WriteRec{}}
	for id := ID(1); id <= 5; id++ {
		h.Txns[id] = &HTxn{Reads: map[string]uint64{"x": uint64(6 - id)}, Begin: 3, End: 3}
		h.Order["x"] = append(h.Order["x"], WriteRec{Key: "x", Ver: uint64(id), Writer: 6 - id})
	}
	h.Txns[5].Reads = map[string]uint64{}
	if err := CheckStrict(h); err != nil {
		t.Fatalf("txns that all begin and end at one instant were ordered: %v", err)
	}
	if err := checkHistoryPairwise(h, true); err != nil {
		t.Fatalf("oracle disagrees: %v", err)
	}
}

func TestRealTimeEndBeforeBeginRejected(t *testing.T) {
	h := &History{Txns: map[ID]*HTxn{1: {Begin: 5, End: 4}}, Order: map[string][]WriteRec{}}
	if err := CheckStrict(h); err == nil || !strings.Contains(err.Error(), "ends (4) before it begins (5)") {
		t.Fatalf("got %v", err)
	}
}

// ciHistory is a committed history at the nightly chaos run's rate (about
// 65 commits/s, each txn 20 to 150 ms, 12 accounts, 10% audits that read
// every account), run serially in ID order.
func ciHistory(n int) *History {
	rng := rand.New(rand.NewPCG(65, 65))
	h := &History{Txns: map[ID]*HTxn{}, Order: map[string][]WriteRec{}}
	keys := make([]string, 12)
	for i := range keys {
		keys[i] = fmt.Sprintf("acct%d", i)
	}
	ver := map[string]uint64{}
	gap := int64(time.Second) / 65
	for i := 1; i <= n; i++ {
		id := ID(i)
		at := int64(i) * gap
		t := &HTxn{Reads: map[string]uint64{}, Begin: at - int64(20+rng.IntN(130))*int64(time.Millisecond), End: at + int64(time.Millisecond)}
		if rng.IntN(10) == 0 {
			for _, k := range keys {
				t.Reads[k] = ver[k]
			}
		} else {
			a, b := rng.IntN(12), rng.IntN(11)
			if b >= a {
				b++
			}
			for _, k := range []string{keys[a], keys[b]} {
				t.Reads[k] = ver[k]
				ver[k] = uint64(i)<<1 | uint64(len(t.Writes))
				t.Writes = append(t.Writes, k)
				h.Order[k] = append(h.Order[k], WriteRec{Key: k, Ver: ver[k], Writer: id})
			}
		}
		h.Txns[id] = t
	}
	return h
}

// A 20-minute chaos run commits about 78,000 txns; the pairwise edges needed
// over 13 GB for 31,000. 100,000 must pass, and a real-time violation at
// that size must still be caught.
func TestCheckStrictScale(t *testing.T) {
	if testing.Short() {
		t.Skip("scale test")
	}
	const n = 100000
	h := ciHistory(n)
	start := time.Now()
	if err := CheckStrict(h); err != nil {
		t.Fatal(err)
	}
	t.Logf("CheckStrict on %d committed txns: %v", n, time.Since(start).Round(time.Millisecond))
	// An early audit (it writes nothing) now reads the final version of
	// acct0, written by a txn that began long after the audit ended. Only
	// the real-time order contradicts that: the audit has no other edges.
	audit := ID(1)
	for len(h.Txns[audit].Writes) > 0 {
		audit++
	}
	last := h.Order["acct0"][len(h.Order["acct0"])-1]
	if h.Txns[last.Writer].Begin <= h.Txns[audit].End {
		t.Fatalf("audit %d does not precede writer %d in real time", audit, last.Writer)
	}
	h.Txns[audit].Reads = map[string]uint64{"acct0": last.Ver}
	if err := CheckStrict(h); err == nil || !strings.Contains(err.Error(), "not strictly serializable") {
		t.Fatalf("real-time violation at scale: got %v", err)
	}
	if err := CheckSerializable(h); err != nil {
		t.Fatalf("the tampered history should still be serializable: %v", err)
	}
}

package txn

import "testing"

// The Observer only watches: a schedule replays to the same trace hash with
// observers attached, and every outcome they report matches the logs.
func TestObserverMatchesLogs(t *testing.T) {
	reasons := map[AbortReason]int{}
	reported, committed := 0, 0
	for seed := uint64(1); seed <= numSeeds(200); seed++ {
		plain, _ := runSchedule(seed, nil)
		w, ok := runSchedule(seed, func(o *worldOpts) { o.Observe = true })
		if !ok {
			t.Fatalf("seed=%d: not quiescent", seed)
		}
		if plain.traceHash() != w.traceHash() {
			t.Fatalf("seed=%d: observers changed the run", seed)
		}
		sms := w.canonical()
		for _, o := range w.observers {
			if o.dup != nil {
				t.Fatalf("seed=%d: %v", seed, o.dup)
			}
			for id, c := range o.finished {
				got, known := sms[o.shard].Outcome(id)
				if !known || got != c {
					t.Fatalf("seed=%d: shard %d reported txn %d commit=%v, log says %v (known %v)", seed, o.shard, id, c, got, known)
				}
				reported++
			}
			for r, n := range o.reasons {
				reasons[r] += n
			}
		}
		committed += len(Committed(sms))
	}
	if reported == 0 || len(reasons) < 3 {
		t.Fatalf("observers reported %d outcomes, abort reasons %v", reported, reasons)
	}
	t.Logf("observers reported %d outcomes (%d txns committed); aborts by reason %v", reported, committed, reasons)
}

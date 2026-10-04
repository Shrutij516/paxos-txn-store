package txn

import (
	"fmt"
	"slices"
)

// checkAtomicity adds the wire-level check for Decisions sent before they
// were logged to CheckAtomicity.
func (w *world) checkAtomicity() error {
	if w.unloggedDecision != nil {
		return fmt.Errorf("atomicity: %w", w.unloggedDecision)
	}
	if err := CheckAtomicity(w.canonical()); err != nil {
		return err
	}
	// What clients were told must match the logs.
	committed := Committed(w.canonical())
	for _, c := range w.cli {
		for _, id := range sortedTxns(c.attempts) {
			a := c.attempts[id]
			if a.toldAbort && committed[id] {
				return fmt.Errorf("atomicity: client %d was told txn %d aborted, but it committed", c.id, id)
			}
			if a.ack >= 0 && !committed[id] {
				return fmt.Errorf("atomicity: client %d was told txn %d committed, but it did not", c.id, id)
			}
		}
	}
	return nil
}

func (w *world) checkBank() error {
	audits := map[ID]map[string]string{}
	for _, c := range w.cli {
		for id, a := range c.attempts {
			if a.audit {
				audits[id] = a.vals
			}
		}
	}
	return CheckBank(w.canonical(), numAccounts*initialBalance, audits)
}

func (w *world) history() (*History, error) {
	attempts := map[ID]*HTxn{}
	for _, c := range w.cli {
		for id, a := range c.attempts {
			attempts[id] = &HTxn{Reads: a.reads, Begin: a.begin, End: a.ack}
		}
	}
	return BuildHistory(w.canonical(), attempts)
}

func checkSerializable(h *History) error { return CheckSerializable(h) }

func checkStrict(h *History) error { return CheckStrict(h) }

func cloneHistory(h *History) *History {
	out := &History{Txns: map[ID]*HTxn{}, Order: map[string][]WriteRec{}}
	for id, t := range h.Txns {
		cp := *t
		cp.Reads = map[string]uint64{}
		for k, v := range t.Reads {
			cp.Reads[k] = v
		}
		cp.Writes = append([]string(nil), t.Writes...)
		out.Txns[id] = &cp
	}
	for k, vs := range h.Order {
		out.Order[k] = append([]WriteRec(nil), vs...)
	}
	return out
}

// tamperG1a makes a committed txn read a version that only an aborted txn
// would have written (one no committed txn installed).
func tamperG1a(h *History) bool {
	for _, id := range sortedTxns(h.Txns) {
		for _, k := range sortedKeys(h.Txns[id].Reads) {
			h.Txns[id].Reads[k] = 1 << 40 // never a real log slot
			return true
		}
	}
	return false
}

// tamperG1b gives a committed writer an extra, earlier version of a key it
// wrote (as if it had written the key twice) and makes another committed
// txn read that intermediate version.
func tamperG1b(h *History) bool {
	for _, k := range sortedKeys(h.Order) {
		vs := h.Order[k]
		if len(vs) == 0 {
			continue
		}
		last := vs[len(vs)-1]
		mid := WriteRec{Key: k, Ver: last.Ver - 1, Writer: last.Writer}
		if mid.Ver == 0 || (len(vs) > 1 && vs[len(vs)-2].Ver >= mid.Ver) {
			continue
		}
		for _, id := range sortedTxns(h.Txns) {
			if id == last.Writer {
				continue
			}
			h.Order[k] = append(append(vs[:len(vs)-1:len(vs)-1], mid), last)
			h.Txns[id].Reads = map[string]uint64{k: mid.Ver}
			return true
		}
	}
	return false
}

// tamperRealTime finds committed A and B where A finished before B began
// and makes A read a version B wrote. That wr edge B -> A contradicts the
// real-time order A -> B, while the version graph alone stays acyclic.
func tamperRealTime(h *History) bool {
	writes := map[ID]WriteRec{}
	for _, k := range sortedKeys(h.Order) {
		for _, wr := range h.Order[k] {
			if _, ok := writes[wr.Writer]; !ok {
				writes[wr.Writer] = wr
			}
		}
	}
	ids := sortedTxns(h.Txns)
	for _, a := range ids {
		for _, b := range ids {
			wb, ok := writes[b]
			ta, tb := h.Txns[a], h.Txns[b]
			if !ok || a == b || ta.End < 0 || tb.Begin <= ta.End {
				continue
			}
			trial := cloneHistory(h)
			trial.Txns[a].Reads = map[string]uint64{wb.Key: wb.Ver}
			if checkSerializable(trial) == nil && checkStrict(trial) != nil {
				h.Txns[a].Reads = trial.Txns[a].Reads
				return true
			}
		}
	}
	return false
}

// tamper injects a write-skew cycle into a real history: two committed
// writers A (of key a) and B (of key b) are made to have read the version
// of the other's key just before the other's write. That gives A -rw-> B
// and B -rw-> A. It returns false if the history has no suitable pair.
func tamper(h *History) bool {
	type pos struct {
		key string
		i   int
	}
	writer := map[ID]pos{}
	for _, k := range sortedKeys(h.Order) {
		for i, wr := range h.Order[k] {
			if _, ok := writer[wr.Writer]; !ok {
				writer[wr.Writer] = pos{k, i}
			}
		}
	}
	ids := sortedTxns(writer)
	prevVer := func(p pos) uint64 {
		if p.i == 0 {
			return 0
		}
		return h.Order[p.key][p.i-1].Ver
	}
	for _, a := range ids {
		for _, b := range ids {
			pa, pb := writer[a], writer[b]
			if a == b || pa.key == pb.key || slices.Contains(h.Txns[a].Writes, pb.key) || slices.Contains(h.Txns[b].Writes, pa.key) {
				continue
			}
			h.Txns[a].Reads = map[string]uint64{pb.key: prevVer(pb)}
			h.Txns[b].Reads = map[string]uint64{pa.key: prevVer(pa)}
			return true
		}
	}
	return false
}

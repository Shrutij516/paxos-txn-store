package txn

import (
	"fmt"
	"slices"
	"strconv"
)

// checkAtomicity: every decided transaction has the same outcome on all of
// its participant shards, every multi-shard commit comes from a commit
// decision, no shard committed a transaction it never prepared, and nothing
// is left prepared once the system is quiet.
func (w *world) checkAtomicity() error {
	if w.unloggedDecision != nil {
		return fmt.Errorf("atomicity: %w", w.unloggedDecision)
	}
	sms := w.canonical()
	shards := make([]ShardID, 0, len(sms))
	for sh := range sms {
		shards = append(shards, sh)
	}
	slices.Sort(shards)
	decided := map[ID]bool{}
	for _, sh := range shards {
		sm := sms[sh]
		if ids := sm.CommitsWithoutPrepare(); len(ids) > 0 {
			return fmt.Errorf("atomicity: shard %d committed txn %d without a prepare record (its writes are lost)", sh, ids[0])
		}
		if ids := sm.PreparedTxns(); len(ids) > 0 {
			return fmt.Errorf("atomicity: shard %d still has txn %d prepared after quiescence", sh, ids[0])
		}
		for _, id := range sm.Decisions() {
			commit, _ := sm.Decided(id)
			decided[id] = commit
			for _, p := range sm.DecidedParticipants(id) {
				out, known := sms[p].Outcome(id)
				if !known {
					if commit {
						return fmt.Errorf("atomicity: txn %d committed by coordinator shard %d but unresolved on shard %d", id, sh, p)
					}
					continue // never prepared there: nothing to undo
				}
				if out != commit {
					return fmt.Errorf("atomicity: txn %d decided commit=%v on shard %d but outcome %v on shard %d", id, commit, sh, out, p)
				}
			}
		}
	}
	for _, sh := range shards {
		sm := sms[sh]
		for _, id := range sm.Outcomes() {
			if commit, _ := sm.Outcome(id); commit && !sm.IsOnePhase(id) && !decided[id] {
				return fmt.Errorf("atomicity: shard %d committed multi-shard txn %d without a commit decision", sh, id)
			}
		}
	}
	return nil
}

// checkBank: the total balance is unchanged in the final state and in every
// committed audit, which reads all accounts in one transaction.
func (w *world) checkBank() error {
	want := numAccounts * initialBalance
	total := 0
	for _, sm := range w.canonical() {
		for _, v := range sm.Data() {
			n, _ := strconv.Atoi(v.Value)
			total += n
		}
	}
	if total != want {
		return fmt.Errorf("bank: final total %d, want %d", total, want)
	}
	committed := w.committed()
	for _, c := range w.cli {
		for _, id := range sortedTxns(c.attempts) {
			a := c.attempts[id]
			if !a.audit || !committed[id] {
				continue
			}
			sum := 0
			for _, v := range a.vals {
				n, _ := strconv.Atoi(v)
				sum += n
			}
			if sum != want {
				return fmt.Errorf("bank: committed audit txn %d saw total %d, want %d", id, sum, want)
			}
		}
	}
	return nil
}

func (w *world) committed() map[ID]bool {
	out := map[ID]bool{}
	for _, sm := range w.canonical() {
		for _, id := range sm.Outcomes() {
			if c, _ := sm.Outcome(id); c {
				out[id] = true
			}
		}
	}
	return out
}

// htxn is one committed transaction in a history.
type htxn struct {
	reads  map[string]uint64 // key -> version read
	writes []string
	begin  int64 // step the client began it
	end    int64 // step the client learned it committed, -1 if it never did
}

// history is what the serializability checker sees: committed
// transactions with the versions they read, and each key's version order.
type history struct {
	txns  map[ID]*htxn
	order map[string][]WriteRec // versions after the initial one, in order
}

func (w *world) history() (*history, error) {
	h := &history{txns: map[ID]*htxn{}, order: map[string][]WriteRec{}}
	committed := w.committed()
	for _, c := range w.cli {
		for _, id := range sortedTxns(c.attempts) {
			if a := c.attempts[id]; committed[id] {
				h.txns[id] = &htxn{reads: a.reads, begin: a.begin, end: a.ack}
			}
		}
	}
	for _, sm := range w.canonical() {
		for _, wr := range sm.Writes() {
			t := h.txns[wr.Writer]
			if t == nil {
				return nil, fmt.Errorf("history: write by unknown txn %d", wr.Writer)
			}
			t.writes = append(t.writes, wr.Key)
			h.order[wr.Key] = append(h.order[wr.Key], wr)
		}
	}
	return h, nil
}

// checkSerializable checks Adya's G1a, G1b and G-cycle conditions over
// the committed transactions: the history is serializable.
func checkSerializable(h *history) error { return checkHistory(h, false) }

// checkStrict additionally adds real-time edges (T1 precedes T2 when the
// client learned T1 committed before T2 began), which makes it a check of
// strict serializability.
func checkStrict(h *history) error { return checkHistory(h, true) }

// checkHistory builds Adya's direct serialization graph. Version edges:
// ww (Ti wrote the version before Tj's), wr (Tj read Ti's version), rw (Ti
// read the version that Tj overwrote next). Transaction 0 is the initial
// state. Before looking for cycles it checks:
//
//	G1a (aborted read): every version read was installed by a committed
//	    transaction. Only committed writes ever become versions, so a read
//	    of anything else is a read of an aborted or never-committed write.
//	G1b (intermediate read): every version read is its writer's final
//	    version of that key.
func checkHistory(h *history, realTime bool) error {
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
	for _, k := range sortedKeys(h.order) {
		vs := append([]WriteRec{{Key: k, Ver: 0, Writer: 0}}, h.order[k]...)
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
	for _, id := range sortedTxns(h.txns) {
		for _, k := range sortedKeys(h.txns[id].reads) {
			ver := h.txns[id].reads[k]
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
		ids := sortedTxns(h.txns)
		for _, a := range ids {
			ta := h.txns[a]
			if ta.end < 0 {
				continue
			}
			for _, b := range ids {
				if a != b && h.txns[b].begin > ta.end {
					add(a, b) // a finished before b started
				}
			}
		}
	}
	// Kahn's algorithm; anything left over sits on a cycle.
	nodes := map[ID]bool{0: true}
	for id := range h.txns {
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

func cloneHistory(h *history) *history {
	out := &history{txns: map[ID]*htxn{}, order: map[string][]WriteRec{}}
	for id, t := range h.txns {
		cp := *t
		cp.reads = map[string]uint64{}
		for k, v := range t.reads {
			cp.reads[k] = v
		}
		cp.writes = append([]string(nil), t.writes...)
		out.txns[id] = &cp
	}
	for k, vs := range h.order {
		out.order[k] = append([]WriteRec(nil), vs...)
	}
	return out
}

// tamperG1a makes a committed txn read a version that only an aborted txn
// would have written (one no committed txn installed).
func tamperG1a(h *history) bool {
	for _, id := range sortedTxns(h.txns) {
		for _, k := range sortedKeys(h.txns[id].reads) {
			h.txns[id].reads[k] = 1 << 40 // never a real log slot
			return true
		}
	}
	return false
}

// tamperG1b gives a committed writer an extra, earlier version of a key it
// wrote (as if it had written the key twice) and makes another committed
// txn read that intermediate version.
func tamperG1b(h *history) bool {
	for _, k := range sortedKeys(h.order) {
		vs := h.order[k]
		if len(vs) == 0 {
			continue
		}
		last := vs[len(vs)-1]
		mid := WriteRec{Key: k, Ver: last.Ver - 1, Writer: last.Writer}
		if mid.Ver == 0 || (len(vs) > 1 && vs[len(vs)-2].Ver >= mid.Ver) {
			continue
		}
		for _, id := range sortedTxns(h.txns) {
			if id == last.Writer {
				continue
			}
			h.order[k] = append(append(vs[:len(vs)-1:len(vs)-1], mid), last)
			h.txns[id].reads = map[string]uint64{k: mid.Ver}
			return true
		}
	}
	return false
}

// tamperRealTime finds committed A and B where A finished before B began
// and makes A read a version B wrote. That wr edge B -> A contradicts the
// real-time order A -> B, while the version graph alone stays acyclic.
func tamperRealTime(h *history) bool {
	writes := map[ID]WriteRec{}
	for _, k := range sortedKeys(h.order) {
		for _, wr := range h.order[k] {
			if _, ok := writes[wr.Writer]; !ok {
				writes[wr.Writer] = wr
			}
		}
	}
	ids := sortedTxns(h.txns)
	for _, a := range ids {
		for _, b := range ids {
			wb, ok := writes[b]
			ta, tb := h.txns[a], h.txns[b]
			if !ok || a == b || ta.end < 0 || tb.begin <= ta.end {
				continue
			}
			trial := cloneHistory(h)
			trial.txns[a].reads = map[string]uint64{wb.Key: wb.Ver}
			if checkSerializable(trial) == nil && checkStrict(trial) != nil {
				h.txns[a].reads = trial.txns[a].reads
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
func tamper(h *history) bool {
	type pos struct {
		key string
		i   int
	}
	writer := map[ID]pos{}
	for _, k := range sortedKeys(h.order) {
		for i, wr := range h.order[k] {
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
		return h.order[p.key][p.i-1].Ver
	}
	for _, a := range ids {
		for _, b := range ids {
			pa, pb := writer[a], writer[b]
			if a == b || pa.key == pb.key || slices.Contains(h.txns[a].writes, pb.key) || slices.Contains(h.txns[b].writes, pa.key) {
				continue
			}
			h.txns[a].reads = map[string]uint64{pb.key: prevVer(pb)}
			h.txns[b].reads = map[string]uint64{pa.key: prevVer(pa)}
			return true
		}
	}
	return false
}

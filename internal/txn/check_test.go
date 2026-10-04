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
			if committed[id] {
				h.txns[id] = &htxn{reads: c.attempts[id].reads}
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

// checkSerializable builds Adya's direct serialization graph over the
// committed transactions and reports a cycle if there is one. Edges:
// ww (Ti wrote the version before Tj's), wr (Tj read Ti's version) and rw
// (Ti read the version that Tj overwrote next). Transaction 0 is the
// initial state.
func checkSerializable(h *history) error {
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
	for _, k := range sortedKeys(h.order) {
		vs := append([]WriteRec{{Key: k, Ver: 0, Writer: 0}}, h.order[k]...)
		versions[k] = vs
		index[k] = map[uint64]int{}
		for i, v := range vs {
			index[k][v.Ver] = i
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
			}
			i, ok := index[k][ver]
			if !ok {
				return fmt.Errorf("serializability: txn %d read %s at version %d, which was never committed", id, k, ver)
			}
			add(vs[i].Writer, id) // wr
			if i+1 < len(vs) {
				add(id, vs[i+1].Writer) // rw
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
		return fmt.Errorf("serializability: dependency cycle among %d txns, e.g. %v", len(stuck), stuck[:min(len(stuck), 4)])
	}
	return nil
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

package txn

import (
	"fmt"
	"slices"
	"strconv"
)

// History checkers. They read the final replicated state of every shard
// (one SM per shard, from a replica that has applied the whole log) plus
// what clients recorded, and are shared by the simulator tests and the
// multi-process test. A transaction whose commit outcome the client never
// learned needs no special treatment: the shards' logs say whether it
// committed, and only committed transactions enter the history.

// CheckAtomicity checks that every decided transaction has the same outcome
// on all of its participant shards, every multi-shard commit comes from a
// commit decision, no shard committed a transaction it never prepared, and
// nothing is left prepared once the system is quiet.
func CheckAtomicity(sms map[ShardID]*SM) error {
	shards := sortedShards(sms)
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
				psm := sms[p]
				if psm == nil {
					return fmt.Errorf("atomicity: txn %d names unknown participant shard %d", id, p)
				}
				out, known := psm.Outcome(id)
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

// Committed returns every transaction that committed on some shard.
func Committed(sms map[ShardID]*SM) map[ID]bool {
	out := map[ID]bool{}
	for _, sm := range sms {
		for _, id := range sm.Outcomes() {
			if c, _ := sm.Outcome(id); c {
				out[id] = true
			}
		}
	}
	return out
}

// CheckBank checks that the balances (every value, as an integer) sum to
// want in the final state, and in every committed audit. audits maps an
// audit transaction to the values it read.
func CheckBank(sms map[ShardID]*SM, want int, audits map[ID]map[string]string) error {
	total := 0
	for _, sm := range sms {
		for _, v := range sm.Data() {
			n, _ := strconv.Atoi(v.Value)
			total += n
		}
	}
	if total != want {
		return fmt.Errorf("bank: final total %d, want %d", total, want)
	}
	committed := Committed(sms)
	for _, id := range sortedTxns(audits) {
		if !committed[id] {
			continue
		}
		sum := 0
		for _, v := range audits[id] {
			n, _ := strconv.Atoi(v)
			sum += n
		}
		if sum != want {
			return fmt.Errorf("bank: committed audit txn %d saw total %d, want %d", id, sum, want)
		}
	}
	return nil
}

// HTxn is one transaction in a history.
type HTxn struct {
	Reads  map[string]uint64 // key -> version read
	Writes []string
	Begin  int64 // when the client began it
	End    int64 // when the client learned it committed, -1 if it never did
}

// History is what the serializability checker sees: committed
// transactions with the versions they read, and each key's version order.
type History struct {
	Txns  map[ID]*HTxn
	Order map[string][]WriteRec // versions after the initial one, in order
}

// BuildHistory keeps the attempts that committed according to sms and
// fills in their writes and every key's version order from the shards.
// attempts must hold every transaction attempt any client made (Reads,
// Begin and End set); a write by a transaction not in it is an error.
func BuildHistory(sms map[ShardID]*SM, attempts map[ID]*HTxn) (*History, error) {
	h := &History{Txns: map[ID]*HTxn{}, Order: map[string][]WriteRec{}}
	committed := Committed(sms)
	for _, id := range sortedTxns(attempts) {
		if committed[id] {
			a := *attempts[id]
			a.Writes = nil
			h.Txns[id] = &a
		}
	}
	for _, sh := range sortedShards(sms) {
		for _, wr := range sms[sh].Writes() {
			t := h.Txns[wr.Writer]
			if t == nil {
				return nil, fmt.Errorf("history: write by unknown txn %d", wr.Writer)
			}
			t.Writes = append(t.Writes, wr.Key)
			h.Order[wr.Key] = append(h.Order[wr.Key], wr)
		}
	}
	return h, nil
}

// CheckSerializable checks Adya's G1a, G1b and G-cycle conditions over the
// committed transactions: the history is serializable.
func CheckSerializable(h *History) error { return checkHistory(h, false) }

// CheckStrict additionally adds real-time edges (T1 precedes T2 when the
// client learned T1 committed before T2 began), which makes it a check of
// strict serializability.
func CheckStrict(h *History) error { return checkHistory(h, true) }

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
func checkHistory(h *History, realTime bool) error {
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

func sortedShards[V any](m map[ShardID]V) []ShardID {
	out := make([]ShardID, 0, len(m))
	for sh := range m {
		out = append(out, sh)
	}
	slices.Sort(out)
	return out
}

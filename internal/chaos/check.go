// Package chaos checks a cluster's final state against the history a
// workload recorded while faults were injected (cmd/chaos drives the faults
// against the compose stack; cmd/loadgen records the history).
package chaos

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"

	"github.com/Shrutij516/paxos-txn-store/history"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/storage"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
)

var dbName = regexp.MustCompile(`^node-(\d+)-shard-(\d+)\.db$`)

// LoadShards replays every node's committed log of every shard, found in
// the given data directories, into fresh state machines. All replicas of a
// shard must agree on the slots they share (Paxos safety across real
// processes; a disagreement is returned as an error); the longest log is
// the shard's state.
func LoadShards(dirs []string, shards int) (map[txn.ShardID]*txn.SM, error) {
	logs := map[txn.ShardID][][]paxos.SlotEntry{}
	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			m := dbName.FindStringSubmatch(e.Name())
			if m == nil {
				continue
			}
			sh, _ := strconv.Atoi(m[2])
			db, err := storage.OpenSQLite(filepath.Join(dir, e.Name()))
			if err != nil {
				return nil, err
			}
			log, err := db.LoadCommitted()
			_ = db.Close()
			if err != nil {
				return nil, fmt.Errorf("%s: %w", e.Name(), err)
			}
			logs[txn.ShardID(sh)] = append(logs[txn.ShardID(sh)], log)
		}
	}
	out := map[txn.ShardID]*txn.SM{}
	for sh := txn.ShardID(0); int(sh) < shards; sh++ {
		if len(logs[sh]) == 0 {
			return nil, fmt.Errorf("no replica of shard %d found", sh)
		}
		var best []paxos.SlotEntry
		for _, log := range logs[sh] {
			for i := 0; i < min(len(log), len(best)); i++ {
				if log[i] != best[i] {
					return nil, fmt.Errorf("shard %d: replicas committed different entries at slot %d", sh, log[i].Slot)
				}
			}
			if len(log) > len(best) {
				best = log
			}
		}
		sm := txn.NewSM(nil)
		for _, se := range best {
			sm.Apply(se.Slot, se.Entry)
		}
		out[sh] = sm
	}
	return out, nil
}

// Report is what Check found.
type Report struct {
	Attempts         int      `json:"attempts"` // excluding the setup transaction
	Committed        int      `json:"committed"`
	CrossShard       int      `json:"cross_shard_committed"`
	Aborted          int      `json:"aborted"`
	Unknown          int      `json:"unknown"`
	UnknownCommitted int      `json:"unknown_committed"`
	Failed           int      `json:"failed_before_commit"`
	CheckedTxns      int      `json:"checked_txns"` // committed txns in the serializability check
	Violations       []string `json:"violations"`
}

// Check runs the Phase 5 checkers on the shards' final state and the
// recorded history: atomicity, the bank invariant (final state and every
// committed audit) and strict serializability (which also rejects G1a and
// G1b), and checks that what every client was told matches the logs. A
// transaction whose outcome the client never learned counts as whatever
// the logs say.
func Check(meta history.Meta, atts []history.Attempt, sms map[txn.ShardID]*txn.SM) Report {
	var r Report
	fail := func(format string, args ...any) { r.Violations = append(r.Violations, fmt.Sprintf(format, args...)) }
	if err := txn.CheckAtomicity(sms); err != nil {
		fail("%v", err)
	}
	attempts := map[txn.ID]*txn.HTxn{}
	audits := map[txn.ID]map[string]string{}
	for _, a := range atts {
		id := txn.ID(a.ID)
		attempts[id] = &txn.HTxn{Reads: a.Reads, Begin: a.Begin, End: a.End}
		if a.Audit {
			audits[id] = a.Values
		}
	}
	if err := txn.CheckBank(sms, meta.Accounts*meta.Initial, audits); err != nil {
		fail("%v", err)
	}
	if h, err := txn.BuildHistory(sms, attempts); err != nil {
		fail("%v", err)
	} else {
		r.CheckedTxns = len(h.Txns)
		if err := txn.CheckStrict(h); err != nil {
			fail("%v", err)
		}
	}
	committed := txn.Committed(sms)
	for _, a := range atts {
		c := committed[txn.ID(a.ID)]
		if a.Setup {
			if a.Outcome == history.Committed && !c {
				fail("setup txn %d reported committed but is not in the logs", a.ID)
			}
			continue
		}
		r.Attempts++
		switch a.Outcome {
		case history.Committed:
			if !c {
				fail("client was told txn %d committed, but no shard committed it", a.ID)
			}
		case history.Aborted, history.Failed:
			if c {
				fail("client was told txn %d did not commit (%s), but it did", a.ID, a.Outcome)
			}
			if a.Outcome == history.Aborted {
				r.Aborted++
			} else {
				r.Failed++
			}
		case history.Unknown:
			r.Unknown++
			if c {
				r.UnknownCommitted++
			}
		default:
			fail("txn %d has outcome %q", a.ID, a.Outcome)
		}
		if c {
			r.Committed++
			if len(a.Shards) > 1 {
				r.CrossShard++
			}
		}
	}
	slices.Sort(r.Violations)
	return r
}

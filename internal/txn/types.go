// Package txn adds sharded, serializable transactions on top of the
// Multi-Paxos replicas: keys are statically sharded, each shard is one Paxos
// group, shard leaders run strict two-phase locking with wound-wait, and
// cross-shard transactions commit with Spanner-style two-phase commit whose
// prepare records and decisions are written through each shard's Paxos log.
// See docs/transactions.md.
//
// Everything here is deterministic and single-threaded so it runs inside
// the seeded simulator.
package txn

import (
	"encoding/json"
	"slices"

	"github.com/Shrutij516/paxos-txn-store/api"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
)

// ShardID names a shard, 0..N-1.
type ShardID int

// DefaultShards is the default number of shards.
const DefaultShards = 3

// ShardOf maps a key to its shard by hashing (api.ShardOf, shared with the
// SDK).
func ShardOf(key string, shards int) ShardID { return ShardID(api.ShardOf(key, shards)) }

// ID identifies one attempt of a transaction. A retried transaction gets
// a new ID but keeps its start timestamp.
type ID uint64

// Meta identifies a transaction attempt and its age for wound-wait.
type Meta struct {
	ID ID
	TS uint64 // start timestamp; smaller is older
}

// Older reports whether m started before o. Ties break on ID so the order
// is total.
func (m Meta) Older(o Meta) bool {
	if m.TS != o.TS {
		return m.TS < o.TS
	}
	return m.ID < o.ID
}

// Part is one shard's share of a transaction: what it read (with the
// version it saw) and what it wants to write.
type Part struct {
	Shard  ShardID
	Reads  map[string]uint64
	Writes map[string]string
}

// ---- Messages (carried as paxos.Ext bodies) ----

// ReadReq asks a shard leader for a key under a shared lock.
type ReadReq struct {
	Txn    Meta
	Key    string
	Client paxos.NodeID
}

// ReadResp answers a ReadReq. Aborted means the transaction was wounded or
// already finished and must abort.
type ReadResp struct {
	Txn     ID
	Key     string
	Value   string
	Version uint64
	Aborted bool
}

// CommitReq asks the coordinator shard to commit. A single part means a
// one-phase commit.
type CommitReq struct {
	Txn    Meta
	Coord  ShardID
	Parts  []Part
	Client paxos.NodeID
}

// CommitResp reports the outcome to the client.
type CommitResp struct {
	Txn       ID
	Committed bool
}

// AbortReq tells a shard leader the client gave up on the transaction
// before asking to commit; its shared locks can go.
type AbortReq struct{ Txn ID }

// PrepareReq is 2PC phase one, from the coordinator to a participant.
type PrepareReq struct {
	Txn          Meta
	Coord        ShardID
	Participants []ShardID
	Part         Part
}

// Vote is a participant's answer to PrepareReq.
type Vote struct {
	Txn   ID
	Shard ShardID
	Yes   bool
}

// Decision tells a participant the coordinator's outcome.
type Decision struct {
	Txn    ID
	Commit bool
}

// QueryOutcome asks the coordinator shard for the outcome of a transaction
// that has been prepared for too long.
type QueryOutcome struct {
	Txn          ID
	From         ShardID
	Participants []ShardID
}

// WoundReq asks the coordinator to abort a transaction that holds a lock an
// older transaction needs, if it has not decided yet.
type WoundReq struct{ Txn ID }

// ---- Log records ----

// Record kinds written through a shard's Paxos log.
const (
	KindPrepare  = "prepare"  // participant prepared: locks and writes are durable
	KindOnePhase = "onephase" // single-shard transaction commits in one step
	KindDecide   = "decide"   // coordinator's decision (first one wins)
	KindCommit   = "commit"   // participant applies its prepared writes
	KindAbort    = "abort"    // participant drops its prepared record
)

// Record is one transaction log record, stored as an Entry's command.
type Record struct {
	Kind         string
	Txn          Meta
	Coord        ShardID
	Participants []ShardID
	Reads        map[string]uint64
	Writes       map[string]string
	Commit       bool
}

func encode(r Record) paxos.Value {
	b, err := json.Marshal(r)
	if err != nil {
		panic(err) // plain data, cannot fail
	}
	return paxos.Value(b)
}

func decode(v paxos.Value) (Record, error) {
	var r Record
	err := json.Unmarshal([]byte(v), &r)
	return r, err
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func sortedTxns[V any](m map[ID]V) []ID {
	out := make([]ID, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

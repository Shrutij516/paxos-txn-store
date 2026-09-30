// Package kv is a minimal in-memory key-value state machine for the
// replicated log. It supports Get and Put and applies each client request at
// most once, using a per-client dedup table keyed by (clientID, seq).
package kv

import (
	"strings"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
)

const sep = "\x00"

// Put encodes a Put(key, value) command.
func Put(key, value string) paxos.Value { return paxos.Value("P" + sep + key + sep + value) }

// Get encodes a Get(key) command.
func Get(key string) paxos.Value { return paxos.Value("G" + sep + key) }

// Decode splits a command into its op ("P" or "G"), key and value.
func Decode(cmd paxos.Value) (op, key, value string) {
	parts := strings.SplitN(string(cmd), sep, 3)
	for len(parts) < 3 {
		parts = append(parts, "")
	}
	return parts[0], parts[1], parts[2]
}

type lastReply struct {
	seq    uint64
	result paxos.Value
}

// Store is the state machine. It is not safe for concurrent use.
type Store struct {
	data    map[string]string
	dedup   map[uint64]lastReply
	execs   map[[2]uint64]int
	maxExec int
	noDedup bool
}

var _ paxos.StateMachine = (*Store)(nil)

// New returns an empty store with deduplication on.
func New() *Store {
	return &Store{
		data:  make(map[string]string),
		dedup: make(map[uint64]lastReply),
		execs: make(map[[2]uint64]int),
	}
}

// NewWithoutDedup returns a store that executes every request, including
// retries. It exists only so negative tests can show that the exactly-once
// checker catches a missing dedup table. Never use it for real.
func NewWithoutDedup() *Store {
	s := New()
	s.noDedup = true
	return s
}

// Apply implements paxos.StateMachine. Results: Put returns "", Get returns
// the current value ("" if absent). A retry of the client's latest request
// returns the cached result without executing again; an older request than
// the latest is ignored, since the client has already moved on.
//
// Dedup assumes each client has at most one request outstanding and uses
// increasing sequence numbers. The table keeps only the latest (seq, result)
// per client, so a client that pipelined requests could have an earlier one
// silently skipped when a later one is applied first.
func (s *Store) Apply(_ uint64, e paxos.Entry) paxos.Value {
	if e.Noop {
		return ""
	}
	if !s.noDedup {
		if l, ok := s.dedup[e.ClientID]; ok && e.Seq <= l.seq {
			if e.Seq == l.seq {
				return l.result
			}
			return ""
		}
	}
	var res paxos.Value
	switch op, key, val := Decode(e.Cmd); op {
	case "P":
		s.data[key] = val
	case "G":
		res = paxos.Value(s.data[key])
	}
	k := [2]uint64{e.ClientID, e.Seq}
	s.execs[k]++
	s.maxExec = max(s.maxExec, s.execs[k])
	s.dedup[e.ClientID] = lastReply{seq: e.Seq, result: res}
	return res
}

// Value returns the current value of key.
func (s *Store) Value(key string) string { return s.data[key] }

// MaxExecutions returns the largest number of times any single
// (clientID, seq) request was executed. Exactly-once means it is at most 1.
func (s *Store) MaxExecutions() int { return s.maxExec }

package txn

import (
	"testing"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
)

func apply(s *SM, slot uint64, r Record) Event {
	s.Apply(slot, paxos.Entry{Cmd: encode(r)})
	ev := s.drain()
	return ev[len(ev)-1]
}

func TestSMValidationAndIdempotency(t *testing.T) {
	s := NewSM(map[string]string{"a": "1", "b": "2"})
	t1 := Meta{ID: 1, TS: 1}
	t2 := Meta{ID: 2, TS: 2}
	// T1 prepares: reads a@0, writes b.
	if e := apply(s, 1, Record{Kind: KindPrepare, Txn: t1, Coord: 0, Participants: []ShardID{0, 1},
		Reads: map[string]uint64{"a": 0}, Writes: map[string]string{"b": "3"}}); !e.OK {
		t.Fatal("first prepare rejected")
	}
	// Duplicate prepare is idempotent.
	if e := apply(s, 2, Record{Kind: KindPrepare, Txn: t1}); !e.OK {
		t.Fatal("duplicate prepare rejected")
	}
	// T2 conflicts: writes a, which T1 read under a durable lock.
	if e := apply(s, 3, Record{Kind: KindPrepare, Txn: t2, Writes: map[string]string{"a": "9"}}); e.OK {
		t.Fatal("write against a prepared read accepted")
	}
	// T2 is remembered as rejected even if its record shows up again.
	if e := apply(s, 4, Record{Kind: KindPrepare, Txn: t2}); e.OK {
		t.Fatal("rejected prepare later accepted")
	}
	// One-phase read of b conflicts with T1's prepared write of b.
	if e := apply(s, 5, Record{Kind: KindOnePhase, Txn: Meta{ID: 3}, Reads: map[string]uint64{"b": 0}}); e.OK {
		t.Fatal("read against a prepared write accepted")
	}
	// Decide commit applies T1's writes; a second decision does not win.
	if e := apply(s, 6, Record{Kind: KindDecide, Txn: t1, Participants: []ShardID{0, 1}, Commit: true}); !e.OK {
		t.Fatal("decision not commit")
	}
	if e := apply(s, 7, Record{Kind: KindDecide, Txn: t1, Commit: false}); !e.OK {
		t.Fatal("later decision overrode the first")
	}
	if v := s.Get("b"); v.Value != "3" || v.Ver != 6 || v.Writer != 1 {
		t.Fatalf("b = %+v", v)
	}
	// A stale read version fails validation.
	if e := apply(s, 8, Record{Kind: KindOnePhase, Txn: Meta{ID: 4}, Reads: map[string]uint64{"b": 0}, Writes: map[string]string{"a": "5"}}); e.OK {
		t.Fatal("stale read accepted")
	}
	// Abort before prepare blocks a late prepare.
	apply(s, 9, Record{Kind: KindAbort, Txn: Meta{ID: 5}})
	if e := apply(s, 10, Record{Kind: KindPrepare, Txn: Meta{ID: 5}, Writes: map[string]string{"a": "7"}}); e.OK {
		t.Fatal("prepare after abort accepted")
	}
	if c, known := s.Outcome(1); !c || !known {
		t.Fatal("T1 outcome lost")
	}
	if len(s.CommitsWithoutPrepare()) != 0 || len(s.PreparedTxns()) != 0 {
		t.Fatal("unexpected leftovers")
	}
	// Garbage and no-ops are ignored.
	s.Apply(11, paxos.Entry{Cmd: "not json"})
	s.Apply(12, paxos.Entry{Noop: true})
}

func TestShardOfIsStableAndSpread(t *testing.T) {
	seen := map[ShardID]bool{}
	for i := 0; i < numAccounts; i++ {
		sh := ShardOf(account(i), DefaultShards)
		if sh != ShardOf(account(i), DefaultShards) || sh < 0 || int(sh) >= DefaultShards {
			t.Fatalf("bad shard %d", sh)
		}
		seen[sh] = true
	}
	if len(seen) != DefaultShards {
		t.Fatalf("accounts land on %d shards, want %d", len(seen), DefaultShards)
	}
}

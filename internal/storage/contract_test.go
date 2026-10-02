package storage

import (
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
)

// store is what both backends implement.
type store interface {
	paxos.Storage
	paxos.LogStorage
}

// backend opens a fresh store and returns a reopen function that simulates
// a process restart: it drops the current handle and opens the same data.
type backend struct {
	name string
	open func(t *testing.T) (store, func() store)
}

func backends() []backend {
	return []backend{
		{"memory", func(*testing.T) (store, func() store) {
			m := NewMemory()
			return m, func() store { return m }
		}},
		{"sqlite", func(t *testing.T) (store, func() store) {
			path := filepath.Join(t.TempDir(), "node.db")
			s, err := OpenSQLite(path)
			if err != nil {
				t.Fatal(err)
			}
			cur := s
			t.Cleanup(func() { _ = cur.Close() })
			return s, func() store {
				_ = cur.Close()
				r, err := OpenSQLite(path)
				if err != nil {
					t.Fatal(err)
				}
				cur = r
				return r
			}
		}},
	}
}

func b(r uint64, n paxos.NodeID) paxos.Ballot { return paxos.Ballot{Round: r, Node: n} }

func entry(slot uint64, ballot paxos.Ballot, cmd string) paxos.SlotEntry {
	return paxos.SlotEntry{Slot: slot, Ballot: ballot, Entry: paxos.Entry{ClientID: 7, Seq: slot, Cmd: paxos.Value(cmd)}}
}

// snapshot reads everything a store holds.
type snapshot struct {
	acceptor  paxos.AcceptorState
	round     uint64
	promised  paxos.Ballot
	accepted  []paxos.SlotEntry
	committed []paxos.SlotEntry
}

func snap(t *testing.T, s store) snapshot {
	t.Helper()
	var sn snapshot
	var err error
	must := func(e error) {
		if e != nil {
			t.Fatal(e)
		}
	}
	sn.acceptor, err = s.LoadAcceptor()
	must(err)
	sn.round, err = s.LoadRound()
	must(err)
	sn.promised, err = s.LoadPromised()
	must(err)
	sn.accepted, err = s.LoadAccepted()
	must(err)
	sn.committed, err = s.LoadCommitted()
	must(err)
	return sn
}

// Test 1: the contract both backends must meet.
func TestStorageContract(t *testing.T) {
	for _, be := range backends() {
		t.Run(be.name, func(t *testing.T) {
			t.Run("empty", func(t *testing.T) {
				s, _ := be.open(t)
				sn := snap(t, s)
				if len(sn.accepted) != 0 || len(sn.committed) != 0 || sn.acceptor != (paxos.AcceptorState{}) ||
					sn.round != 0 || sn.promised != (paxos.Ballot{}) {
					t.Fatalf("fresh store not empty: %+v", sn)
				}
			})
			t.Run("acceptor and round", func(t *testing.T) {
				s, _ := be.open(t)
				st := paxos.AcceptorState{Promised: b(4, 2), Accepted: b(3, 1), Value: "v\x00w"}
				if err := s.SaveAcceptor(st); err != nil {
					t.Fatal(err)
				}
				st.Promised = b(5, 3)
				if err := s.SaveAcceptor(st); err != nil {
					t.Fatal(err)
				}
				if err := s.SaveRound(9); err != nil {
					t.Fatal(err)
				}
				sn := snap(t, s)
				if sn.acceptor != st || sn.round != 9 {
					t.Fatalf("got %+v round %d", sn.acceptor, sn.round)
				}
			})
			t.Run("promise and accepted", func(t *testing.T) {
				s, _ := be.open(t)
				for _, pb := range []paxos.Ballot{b(1, 1), b(2, 3)} {
					if err := s.SavePromised(pb); err != nil {
						t.Fatal(err)
					}
				}
				for _, se := range []paxos.SlotEntry{entry(3, b(1, 1), "c"), entry(1, b(1, 1), "a"), entry(3, b(2, 3), "c2"),
					{Slot: 2, Ballot: b(2, 3), Entry: paxos.Entry{Noop: true}}} {
					if err := s.SaveAccept(b(2, 3), se); err != nil {
						t.Fatal(err)
					}
				}
				sn := snap(t, s)
				want := []paxos.SlotEntry{entry(1, b(1, 1), "a"), {Slot: 2, Ballot: b(2, 3), Entry: paxos.Entry{Noop: true}}, entry(3, b(2, 3), "c2")}
				if sn.promised != b(2, 3) || !reflect.DeepEqual(sn.accepted, want) {
					t.Fatalf("promised %v accepted %+v", sn.promised, sn.accepted)
				}
			})
			t.Run("committed prefix", func(t *testing.T) {
				s, _ := be.open(t)
				one := []paxos.SlotEntry{{Slot: 1, Entry: paxos.Entry{Cmd: "a"}}}
				two := []paxos.SlotEntry{{Slot: 2, Entry: paxos.Entry{Noop: true}}, {Slot: 3, Entry: paxos.Entry{ClientID: 1, Seq: 2, Cmd: "c"}}}
				if err := s.AppendCommitted(one); err != nil {
					t.Fatal(err)
				}
				if err := s.AppendCommitted(two); err != nil {
					t.Fatal(err)
				}
				if err := s.AppendCommitted([]paxos.SlotEntry{{Slot: 5}}); err == nil {
					t.Fatal("append with a gap must fail")
				}
				if err := s.AppendCommitted([]paxos.SlotEntry{{Slot: 3}}); err == nil {
					t.Fatal("append overlapping the prefix must fail")
				}
				got, _ := s.LoadCommitted()
				want := append(slices.Clone(one), two...)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("committed %+v, want %+v", got, want)
				}
			})
			// Test 2: close, reopen from the same data, state matches exactly.
			t.Run("restart", func(t *testing.T) {
				s, reopen := be.open(t)
				_ = s.SaveAcceptor(paxos.AcceptorState{Promised: b(8, 1), Accepted: b(7, 2), Value: "x"})
				_ = s.SaveRound(12)
				_ = s.SavePromised(b(11, 3))
				_ = s.SaveAccept(b(11, 3), entry(1, b(11, 3), "a"))
				_ = s.SaveAccept(b(11, 3), entry(2, b(10, 2), "b"))
				_ = s.AppendCommitted([]paxos.SlotEntry{{Slot: 1, Entry: entry(1, b(11, 3), "a").Entry}})
				before := snap(t, s)
				after := snap(t, reopen())
				if !reflect.DeepEqual(before, after) {
					t.Fatalf("state changed across restart:\nbefore %+v\nafter  %+v", before, after)
				}
			})
		})
	}
}

func TestSQLitePragmasAndSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n.db")
	s, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if s.Path() != path {
		t.Fatal("Path mismatch")
	}
	if v, _ := s.Pragma("journal_mode"); v != "wal" {
		t.Fatalf("journal_mode = %q, want wal", v)
	}
	// synchronous: 0 OFF, 1 NORMAL, 2 FULL, 3 EXTRA.
	if v, _ := s.Pragma("synchronous"); v != "2" {
		t.Fatalf("synchronous = %q, want 2 (FULL)", v)
	}
	if v, _ := s.Version(); v != SchemaVersion {
		t.Fatalf("schema version %d, want %d", v, SchemaVersion)
	}
	n, err := OpenSQLiteSync(filepath.Join(t.TempDir(), "n.db"), SyncNormal)
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := n.Pragma("synchronous"); v != "1" {
		t.Fatalf("synchronous = %q, want 1 (NORMAL)", v)
	}
	_ = n.Close()
	if _, err := OpenSQLiteSync(path, "OFF"); err == nil {
		t.Fatal("OFF must be rejected")
	}
}

func TestSQLiteRejectsNewerSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n.db")
	s, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE schema_version SET version = ?`, SchemaVersion+1); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	if _, err := OpenSQLite(path); err == nil {
		t.Fatal("opening a newer schema must fail")
	}
	// Reopening a current schema is a no-op migration.
	p2 := filepath.Join(t.TempDir(), "m.db")
	s2, _ := OpenSQLite(p2)
	_ = s2.Close()
	s3, err := OpenSQLite(p2)
	if err != nil {
		t.Fatal(err)
	}
	_ = s3.Close()
}

var errCrash = errors.New("crash")

func TestSQLiteFaultHook(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n.db")
	s, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	_ = s.SavePromised(b(1, 1))

	// Before commit: the write is rolled back.
	s.fault = func(p faultPoint) error {
		if p == beforeCommit {
			return errCrash
		}
		return nil
	}
	if err := s.SavePromised(b(2, 2)); !errors.Is(err, errCrash) {
		t.Fatalf("want crash error, got %v", err)
	}
	if err := s.AppendCommitted([]paxos.SlotEntry{{Slot: 1}}); !errors.Is(err, errCrash) {
		t.Fatalf("want crash error, got %v", err)
	}
	if got, _ := s.LoadPromised(); got != b(1, 1) {
		t.Fatalf("rolled-back promise visible: %v", got)
	}
	if got, _ := s.LoadCommitted(); len(got) != 0 {
		t.Fatalf("rolled-back commit visible: %v", got)
	}

	// After commit: durable even though the caller sees an error.
	s.fault = func(p faultPoint) error {
		if p == afterCommit {
			return errCrash
		}
		return nil
	}
	if err := s.SavePromised(b(3, 3)); !errors.Is(err, errCrash) {
		t.Fatalf("want crash error, got %v", err)
	}
	s.fault = nil
	_ = s.Close()
	r, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if got, _ := r.LoadPromised(); got != b(3, 3) {
		t.Fatalf("committed promise lost: %v", got)
	}
}

func TestSQLiteClosedErrors(t *testing.T) {
	s, err := OpenSQLite(filepath.Join(t.TempDir(), "n.db"))
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	if err := s.SavePromised(b(1, 1)); err == nil {
		t.Error("write on closed db must fail")
	}
	if _, err := s.LoadAccepted(); err == nil {
		t.Error("LoadAccepted on closed db must fail")
	}
	if _, err := s.LoadCommitted(); err == nil {
		t.Error("LoadCommitted on closed db must fail")
	}
	if _, err := OpenSQLite(filepath.Join(t.TempDir(), "missing", "dir", "n.db")); err == nil {
		t.Error("open in a missing directory must fail")
	}
}

// ---- Benchmarks: persist latency with synchronous=FULL vs NORMAL ----

func benchStore(b *testing.B, sync Sync) *SQLite {
	b.Helper()
	s, err := OpenSQLiteSync(filepath.Join(b.TempDir(), "bench.db"), sync)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s
}

func BenchmarkPersistPromise(bm *testing.B) {
	for _, sync := range []Sync{SyncFull, SyncNormal} {
		bm.Run(string(sync), func(bm *testing.B) {
			s := benchStore(bm, sync)
			bm.ResetTimer()
			for i := 0; i < bm.N; i++ {
				if err := s.SavePromised(paxos.Ballot{Round: uint64(i + 1), Node: 1}); err != nil {
					bm.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPersistAccept(bm *testing.B) {
	for _, sync := range []Sync{SyncFull, SyncNormal} {
		bm.Run(string(sync), func(bm *testing.B) {
			s := benchStore(bm, sync)
			e := paxos.Entry{ClientID: 1, Seq: 1, Cmd: "P\x00key\x00value-of-moderate-length"}
			bm.ResetTimer()
			for i := 0; i < bm.N; i++ {
				// A fresh ballot each time, so every write raises the promise too.
				bal := paxos.Ballot{Round: uint64(i + 1), Node: 1}
				if err := s.SaveAccept(bal, paxos.SlotEntry{Slot: uint64(i + 1), Ballot: bal, Entry: e}); err != nil {
					bm.Fatal(err)
				}
			}
		})
	}
}

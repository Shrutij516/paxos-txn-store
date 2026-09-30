package kv

import (
	"testing"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
)

func req(client, seq uint64, cmd paxos.Value) paxos.Entry {
	return paxos.Entry{ClientID: client, Seq: seq, Cmd: cmd}
}

func TestPutGet(t *testing.T) {
	s := New()
	if got := s.Apply(1, req(1, 1, Get("a"))); got != "" {
		t.Fatalf("Get missing key = %q", got)
	}
	s.Apply(2, req(1, 2, Put("a", "x")))
	if got := s.Apply(3, req(2, 1, Get("a"))); got != "x" {
		t.Fatalf("Get = %q, want x", got)
	}
	if s.Value("a") != "x" {
		t.Fatal("Value mismatch")
	}
	if got := s.Apply(4, paxos.Entry{Noop: true}); got != "" {
		t.Fatalf("noop result %q", got)
	}
	s.Apply(5, req(3, 1, "garbage"))
}

func TestDedupReturnsCachedResult(t *testing.T) {
	s := New()
	s.Apply(1, req(1, 1, Put("a", "x")))
	if got := s.Apply(2, req(1, 2, Get("a"))); got != "x" {
		t.Fatalf("Get = %q", got)
	}
	s.Apply(3, req(2, 1, Put("a", "y")))
	// Retry of client 1's Get must return the cached "x", not "y".
	if got := s.Apply(4, req(1, 2, Get("a"))); got != "x" {
		t.Fatalf("retry returned %q, want cached x", got)
	}
	// A stale retry of an older request is not executed.
	s.Apply(5, req(1, 1, Put("a", "x")))
	if s.Value("a") != "y" {
		t.Fatalf("stale retry was executed, a=%q", s.Value("a"))
	}
	if s.MaxExecutions() != 1 {
		t.Fatalf("MaxExecutions = %d", s.MaxExecutions())
	}
}

func TestWithoutDedupExecutesTwice(t *testing.T) {
	s := NewWithoutDedup()
	s.Apply(1, req(1, 1, Put("a", "x")))
	s.Apply(2, req(1, 1, Put("a", "x")))
	if s.MaxExecutions() != 2 {
		t.Fatalf("MaxExecutions = %d, want 2", s.MaxExecutions())
	}
}

func TestDecode(t *testing.T) {
	op, k, v := Decode(Put("k", "a\x00b"))
	if op != "P" || k != "k" || v != "a\x00b" {
		t.Fatalf("Decode = %q %q %q", op, k, v)
	}
}

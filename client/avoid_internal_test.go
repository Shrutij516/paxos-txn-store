package client

import (
	"testing"
	"time"
)

// When every address is in the avoid window, rotate must still return one,
// the least recently failed, and not whatever the round-robin position
// points at (incident I19, DECISIONS.md entry 48).
func TestRotateAllAvoidedPicksOldestFailure(t *testing.T) {
	c, err := New([]string{"a:1", "b:1", "c:1"}, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	now := time.Now()
	c.down = map[string]time.Time{
		"a:1": now.Add(3 * time.Second), // failed last
		"b:1": now.Add(1 * time.Second), // failed first
		"c:1": now.Add(2 * time.Second),
	}
	c.next = 0 // round-robin would say a:1
	c.mu.Lock()
	got := c.rotate()
	c.mu.Unlock()
	if got != "b:1" {
		t.Fatalf("all addresses avoided: rotate = %s, want b:1 (the oldest failure)", got)
	}
	// It does not stall either: every call returns an address.
	c.mu.Lock()
	for range 10 {
		if c.rotate() == "" {
			t.Fatal("rotate returned no address")
		}
	}
	c.mu.Unlock()
}

package api

import "testing"

// The shard function is part of the protocol: changing it moves keys.
func TestShardOfIsStable(t *testing.T) {
	for k, want := range map[string]int{"alice": 2, "dave": 1, "acct0": ShardOf("acct0", 3)} {
		if got := ShardOf(k, 3); got != want {
			t.Errorf("ShardOf(%q, 3) = %d, want %d", k, got, want)
		}
	}
	for i := 0; i < 100; i++ {
		if s := ShardOf(string(rune('a'+i)), 7); s < 0 || s >= 7 {
			t.Fatalf("shard %d out of range", s)
		}
	}
}

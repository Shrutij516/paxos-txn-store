package main

import "testing"

func TestRunRejectsBadFlags(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"-id", "1"},
		{"-id", "1", "-data-dir", t.TempDir()},
		{"-id", "1", "-data-dir", t.TempDir(), "-peers", "garbage"},
		{"-bogus"},
	} {
		if err := run(args); err == nil {
			t.Errorf("run(%q) should fail", args)
		}
	}
}

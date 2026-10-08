package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestRunRejectsBadFlags(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte(`{"shards": 0}`), 0o644); err != nil {
		t.Fatal(err)
	}
	good := filepath.Join(dir, "good.json")
	if err := os.WriteFile(good, []byte(`{"shards": 1, "nodes": [{"id": 1, "addr": "127.0.0.1:0"}]}`), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{},
		{"-id", "1"},
		{"-id", "1", "-data-dir", t.TempDir()},
		{"-config", good, "-data-dir", t.TempDir()},
		{"-config", filepath.Join(dir, "missing.json"), "-id", "1", "-data-dir", t.TempDir()},
		{"-config", bad, "-id", "1", "-data-dir", t.TempDir()},
		{"-config", good, "-id", "2", "-data-dir", t.TempDir()}, // not in the config
		{"-config", good, "-id", "1", "-data-dir", t.TempDir(), "-in-doubt-wait", "0s"},
		{"-bogus"},
	} {
		if err := run(args); err == nil {
			t.Errorf("run(%q) should fail", args)
		}
	}
}

func TestProbe(t *testing.T) {
	code := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }))
	defer srv.Close()
	if err := run([]string{"-probe", srv.URL}); err != nil {
		t.Fatalf("probe of a 200: %v", err)
	}
	code = http.StatusServiceUnavailable
	if err := run([]string{"-probe", srv.URL}); err == nil {
		t.Fatal("probe of a 503 succeeded")
	}
	if err := run([]string{"-probe", "http://127.0.0.1:1/readyz"}); err == nil {
		t.Fatal("probe of a closed port succeeded")
	}
}

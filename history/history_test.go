package history

import (
	"bytes"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	m := Meta{Accounts: 12, Initial: 100, Clients: 3, Seed: 7}
	w, err := NewWriter(&buf, m)
	if err != nil {
		t.Fatal(err)
	}
	want := []Attempt{
		{ID: 1, Setup: true, Begin: 1, End: 5, Finish: 5, Outcome: Committed},
		{ID: 2, Reads: map[string]uint64{"acct1": 3}, Values: map[string]string{"acct1": "90"}, Shards: []int{0, 2}, Planned: []int{0, 2}, Begin: 6, End: -1, Finish: 9, Outcome: Unknown},
	}
	var wg sync.WaitGroup
	for _, a := range want {
		wg.Add(1)
		go func() { defer wg.Done(); _ = w.Add(a) }()
	}
	wg.Wait()
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	gotMeta, got, err := Read(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if gotMeta != m || len(got) != 2 {
		t.Fatalf("meta %+v, %d attempts", gotMeta, len(got))
	}
	if got[0].ID != 1 {
		got[0], got[1] = got[1], got[0]
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
	if Account(3) != "acct3" {
		t.Fatal(Account(3))
	}
}

func TestReadErrors(t *testing.T) {
	for _, in := range []string{"", `{"attempt":{"id":1}}`, `{"meta":{}}` + "\n{}", `{"meta":{}}` + "\n{bad"} {
		if _, _, err := Read(strings.NewReader(in)); err == nil {
			t.Errorf("Read(%q) succeeded", in)
		}
	}
}

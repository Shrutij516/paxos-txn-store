// Package history is the file format loadgen writes and the chaos runner
// checks: one JSON object per line. The first line describes the run
// (Meta); every other line is one transaction attempt (Attempt). Times are
// nanoseconds since the run started, on loadgen's clock, so real-time order
// between attempts is meaningful. Like the SDK, it imports nothing internal.
package history

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// Meta describes a bank workload: Accounts accounts named Account(i), each
// opened with Initial by the setup transaction, so the balances always sum
// to Accounts*Initial.
type Meta struct {
	Accounts int   `json:"accounts"`
	Initial  int   `json:"initial"`
	Clients  int   `json:"clients"`
	Seed     int64 `json:"seed"`
}

// Account names the i-th account.
func Account(i int) string { return fmt.Sprintf("acct%d", i) }

// Outcomes an attempt can end with, as the client saw it.
const (
	Committed = "committed" // Commit returned success
	Aborted   = "aborted"   // the transaction did not commit and never will
	Unknown   = "unknown"   // the commit outcome was not learned
	Failed    = "error"     // it failed before Commit (a deadline): it never committed
)

// Attempt is one transaction attempt.
type Attempt struct {
	ID      uint64            `json:"id"`
	Setup   bool              `json:"setup,omitempty"`  // the transaction that opened the accounts
	Audit   bool              `json:"audit,omitempty"`  // reads every account, writes nothing
	Reads   map[string]uint64 `json:"reads,omitempty"`  // key -> version read
	Values  map[string]string `json:"values,omitempty"` // key -> value read
	Shards  []int             `json:"shards,omitempty"` // shards it read or wrote
	Planned []int             `json:"planned,omitempty"`
	Begin   int64             `json:"begin"`  // before its first operation
	End     int64             `json:"end"`    // when Commit returned success, -1 otherwise
	Finish  int64             `json:"finish"` // when the attempt ended, whatever the outcome
	Outcome string            `json:"outcome"`
}

type line struct {
	Meta    *Meta    `json:"meta,omitempty"`
	Attempt *Attempt `json:"attempt,omitempty"`
}

// Writer appends a history. It is safe for concurrent use.
type Writer struct {
	mu  sync.Mutex
	w   *bufio.Writer
	enc *json.Encoder
}

// NewWriter starts a history on w with its Meta line.
func NewWriter(w io.Writer, m Meta) (*Writer, error) {
	bw := bufio.NewWriter(w)
	hw := &Writer{w: bw, enc: json.NewEncoder(bw)}
	if err := hw.enc.Encode(line{Meta: &m}); err != nil {
		return nil, err
	}
	return hw, nil
}

// Add appends one attempt.
func (w *Writer) Add(a Attempt) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enc.Encode(line{Attempt: &a})
}

// Flush writes buffered lines through.
func (w *Writer) Flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Flush()
}

// Read parses a history.
func Read(r io.Reader) (Meta, []Attempt, error) {
	dec := json.NewDecoder(r)
	var first line
	if err := dec.Decode(&first); err != nil {
		return Meta{}, nil, fmt.Errorf("history: reading meta: %w", err)
	}
	if first.Meta == nil {
		return Meta{}, nil, errors.New("history: first line is not meta")
	}
	var out []Attempt
	for {
		var l line
		if err := dec.Decode(&l); errors.Is(err, io.EOF) {
			return *first.Meta, out, nil
		} else if err != nil {
			return Meta{}, nil, fmt.Errorf("history: line %d: %w", len(out)+2, err)
		}
		if l.Attempt == nil {
			return Meta{}, nil, fmt.Errorf("history: line %d is not an attempt", len(out)+2)
		}
		out = append(out, *l.Attempt)
	}
}

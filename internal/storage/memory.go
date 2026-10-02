// Package storage holds implementations of paxos.Storage and
// paxos.LogStorage: an in-memory store for fast simulation and a durable
// SQLite store. Both pass the same contract tests.
package storage

import (
	"fmt"
	"maps"
	"slices"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
)

// Memory is an in-memory paxos.Storage and paxos.LogStorage. It survives a
// simulated crash because the simulator hands the same Memory to the
// restarted node, which is how a disk would behave. It is not safe for
// concurrent use.
type Memory struct {
	acceptor paxos.AcceptorState
	round    uint64
	promised paxos.Ballot
	log      map[uint64]paxos.SlotEntry
	commit   []paxos.SlotEntry

	// Saves counts successful writes, for tests.
	Saves int
}

var (
	_ paxos.Storage    = (*Memory)(nil)
	_ paxos.LogStorage = (*Memory)(nil)
)

// NewMemory returns an empty store.
func NewMemory() *Memory { return &Memory{log: make(map[uint64]paxos.SlotEntry)} }

// LoadAcceptor implements paxos.Storage.
func (m *Memory) LoadAcceptor() (paxos.AcceptorState, error) { return m.acceptor, nil }

// SaveAcceptor implements paxos.Storage.
func (m *Memory) SaveAcceptor(s paxos.AcceptorState) error {
	m.acceptor = s
	m.Saves++
	return nil
}

// LoadRound implements paxos.Storage.
func (m *Memory) LoadRound() (uint64, error) { return m.round, nil }

// SaveRound implements paxos.Storage.
func (m *Memory) SaveRound(r uint64) error {
	m.round = r
	m.Saves++
	return nil
}

// ForgetAcceptor wipes the acceptor state, simulating a disk that lost its
// contents. It exists only so tests can show that losing this state breaks
// safety.
func (m *Memory) ForgetAcceptor() { m.acceptor = paxos.AcceptorState{} }

// LoadPromised implements paxos.LogStorage.
func (m *Memory) LoadPromised() (paxos.Ballot, error) { return m.promised, nil }

// SavePromised implements paxos.LogStorage.
func (m *Memory) SavePromised(b paxos.Ballot) error {
	m.promised = b
	m.Saves++
	return nil
}

// LoadAccepted implements paxos.LogStorage.
func (m *Memory) LoadAccepted() ([]paxos.SlotEntry, error) {
	out := make([]paxos.SlotEntry, 0, len(m.log))
	for _, s := range slices.Sorted(maps.Keys(m.log)) {
		out = append(out, m.log[s])
	}
	return out, nil
}

// SaveAccepted implements paxos.LogStorage.
func (m *Memory) SaveAccepted(e paxos.SlotEntry) error {
	m.log[e.Slot] = e
	m.Saves++
	return nil
}

// LoadCommitted implements paxos.LogStorage.
func (m *Memory) LoadCommitted() ([]paxos.SlotEntry, error) {
	return slices.Clone(m.commit), nil
}

// AppendCommitted implements paxos.LogStorage.
func (m *Memory) AppendCommitted(es []paxos.SlotEntry) error {
	if err := checkAppend(uint64(len(m.commit)), es); err != nil {
		return err
	}
	m.commit = append(m.commit, es...)
	m.Saves++
	return nil
}

// checkAppend verifies that es extends a committed prefix ending at commit.
func checkAppend(commit uint64, es []paxos.SlotEntry) error {
	for i, e := range es {
		if want := commit + uint64(i) + 1; e.Slot != want {
			return fmt.Errorf("storage: committed append at slot %d, want %d", e.Slot, want)
		}
	}
	return nil
}

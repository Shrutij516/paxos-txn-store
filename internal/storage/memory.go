// Package storage holds implementations of paxos.Storage and
// paxos.LogStorage. For now there is only an in-memory store; SQLite arrives
// in a later phase behind the same interfaces.
package storage

import (
	"maps"
	"slices"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
)

// Memory is an in-memory paxos.Storage and paxos.LogStorage. It survives a
// simulated crash because the simulator hands the same Memory to the
// restarted node, which is how a disk would behave. It is not safe for concurrent use.
type Memory struct {
	acceptor paxos.AcceptorState
	round    uint64
	promised paxos.Ballot
	log      map[uint64]paxos.SlotEntry

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

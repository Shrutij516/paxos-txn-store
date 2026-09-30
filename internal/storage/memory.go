// Package storage holds implementations of paxos.Storage. Phase 1 ships an
// in-memory store; SQLite arrives in a later phase behind the same interface.
package storage

import "github.com/Shrutij516/paxos-txn-store/internal/paxos"

// Memory is an in-memory paxos.Storage. It survives a simulated crash because
// the simulator hands the same Memory to the restarted node, which is how a
// disk would behave. It is not safe for concurrent use.
type Memory struct {
	acceptor paxos.AcceptorState
	round    uint64

	// Saves counts successful writes, for tests.
	Saves int
}

var _ paxos.Storage = (*Memory)(nil)

// NewMemory returns an empty store.
func NewMemory() *Memory { return &Memory{} }

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

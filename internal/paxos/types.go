// Package paxos implements single-decree Paxos: proposer, acceptor and
// learner roles that talk only through a Transport and persist acceptor
// state through a Storage. The package never reads the wall clock and never
// sleeps; time advances only when the caller invokes Tick, and all
// randomness comes from an injected Rand. That keeps every run replayable
// from a seed.
package paxos

import (
	"errors"
	"fmt"
)

// NodeID identifies a node in the cluster. Every node runs all three roles.
type NodeID int32

// Value is the thing being agreed on.
type Value string

// Ballot orders proposals. Two proposers never share a ballot because the
// node ID is part of it and breaks ties between equal rounds. The zero
// Ballot means "none"; real ballots start at round 1.
type Ballot struct {
	Round uint64
	Node  NodeID
}

// Less reports whether b orders strictly before o.
func (b Ballot) Less(o Ballot) bool {
	if b.Round != o.Round {
		return b.Round < o.Round
	}
	return b.Node < o.Node
}

func (b Ballot) String() string { return fmt.Sprintf("(%d,%d)", b.Round, b.Node) }

// Payload is implemented by every Paxos message body. Messages are plain Go
// structs for now; the gRPC phase will map them to protobuf.
type Payload interface{ isPayload() }

// Prepare is phase 1a: a proposer asks acceptors to promise ballot Ballot.
type Prepare struct{ Ballot Ballot }

// Promise is phase 1b. Accepted and Value report the highest-ballot proposal
// this acceptor has accepted so far; Accepted is zero if it has none.
type Promise struct {
	Ballot   Ballot
	Accepted Ballot
	Value    Value
}

// Accept is phase 2a: a proposer asks acceptors to accept Value at Ballot.
type Accept struct {
	Ballot Ballot
	Value  Value
}

// Accepted is phase 2b. Acceptors send it to every node so learners can
// count votes.
type Accepted struct {
	Ballot Ballot
	Value  Value
}

// Nack tells a proposer its Ballot was rejected because the acceptor has
// already promised Promised.
type Nack struct {
	Ballot   Ballot
	Promised Ballot
}

func (Prepare) isPayload()  {}
func (Promise) isPayload()  {}
func (Accept) isPayload()   {}
func (Accepted) isPayload() {}
func (Nack) isPayload()     {}

// Message is the envelope that crosses the Transport.
type Message struct {
	From NodeID
	To   NodeID
	Body Payload
}

func (m Message) String() string {
	return fmt.Sprintf("%d->%d %T%+v", m.From, m.To, m.Body, m.Body)
}

// Transport delivers messages between nodes. Send must not block and may
// drop, delay, duplicate or reorder messages; Paxos tolerates all of that.
type Transport interface {
	Send(m Message)
}

// Rand is the only source of randomness the package uses.
// *math/rand/v2.Rand satisfies it.
type Rand interface {
	IntN(n int) int
}

// AcceptorState is what an acceptor must remember across crashes.
type AcceptorState struct {
	Promised Ballot // highest ballot promised
	Accepted Ballot // ballot of the accepted proposal, zero if none
	Value    Value  // value of the accepted proposal
}

// Storage is stable storage for one node. Save calls must be durable when
// they return nil. The in-memory implementation lives in internal/storage;
// SQLite plugs in behind the same interface later.
type Storage interface {
	LoadAcceptor() (AcceptorState, error)
	SaveAcceptor(AcceptorState) error
	// LoadRound and SaveRound persist the highest round this node's
	// proposer has used, so a restarted proposer never reuses a ballot.
	LoadRound() (uint64, error)
	SaveRound(uint64) error
}

// ErrBusy is returned by Propose when a proposal is already in flight.
var ErrBusy = errors.New("paxos: proposal already in progress")

// ErrDecided is returned by Propose when this node already knows the outcome.
var ErrDecided = errors.New("paxos: value already decided")

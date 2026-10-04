// Package wire converts between the Paxos core's plain Go messages
// (internal/paxos) and their protobuf form (proto/paxos/v1). It is the only
// place the two meet, so the Paxos core stays free of protobuf.
package wire

import (
	"errors"
	"fmt"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	paxosv1 "github.com/Shrutij516/paxos-txn-store/proto/paxos/v1"
)

// ErrNoBody is returned for an envelope without a message body.
var ErrNoBody = errors.New("wire: envelope has no body")

func ballotToProto(b paxos.Ballot) *paxosv1.Ballot {
	return &paxosv1.Ballot{Round: b.Round, Node: int32(b.Node)}
}

func ballotFromProto(b *paxosv1.Ballot) paxos.Ballot {
	return paxos.Ballot{Round: b.GetRound(), Node: paxos.NodeID(b.GetNode())}
}

func entryToProto(e paxos.Entry) *paxosv1.Entry {
	return &paxosv1.Entry{Noop: e.Noop, ClientId: e.ClientID, Seq: e.Seq, Cmd: []byte(e.Cmd)}
}

func entryFromProto(e *paxosv1.Entry) paxos.Entry {
	return paxos.Entry{Noop: e.GetNoop(), ClientID: e.GetClientId(), Seq: e.GetSeq(), Cmd: paxos.Value(e.GetCmd())}
}

func slotsToProto(es []paxos.SlotEntry) []*paxosv1.SlotEntry {
	if len(es) == 0 {
		return nil
	}
	out := make([]*paxosv1.SlotEntry, len(es))
	for i, e := range es {
		out[i] = &paxosv1.SlotEntry{Slot: e.Slot, Ballot: ballotToProto(e.Ballot), Entry: entryToProto(e.Entry)}
	}
	return out
}

func slotsFromProto(es []*paxosv1.SlotEntry) []paxos.SlotEntry {
	if len(es) == 0 {
		return nil
	}
	out := make([]paxos.SlotEntry, len(es))
	for i, e := range es {
		out[i] = paxos.SlotEntry{Slot: e.GetSlot(), Ballot: ballotFromProto(e.GetBallot()), Entry: entryFromProto(e.GetEntry())}
	}
	return out
}

// ToProto converts a Paxos message to its envelope.
func ToProto(m paxos.Message) (*paxosv1.Envelope, error) {
	env := &paxosv1.Envelope{From: int32(m.From), To: int32(m.To)}
	switch b := m.Body.(type) {
	case paxos.Prepare:
		env.Body = &paxosv1.Envelope_Prepare{Prepare: &paxosv1.Prepare{Ballot: ballotToProto(b.Ballot)}}
	case paxos.Promise:
		env.Body = &paxosv1.Envelope_Promise{Promise: &paxosv1.Promise{
			Ballot: ballotToProto(b.Ballot), Accepted: ballotToProto(b.Accepted), Value: []byte(b.Value)}}
	case paxos.Accept:
		env.Body = &paxosv1.Envelope_Accept{Accept: &paxosv1.Accept{Ballot: ballotToProto(b.Ballot), Value: []byte(b.Value)}}
	case paxos.Accepted:
		env.Body = &paxosv1.Envelope_Accepted{Accepted: &paxosv1.Accepted{Ballot: ballotToProto(b.Ballot), Value: []byte(b.Value)}}
	case paxos.Nack:
		env.Body = &paxosv1.Envelope_Nack{Nack: &paxosv1.Nack{Ballot: ballotToProto(b.Ballot), Promised: ballotToProto(b.Promised)}}
	case paxos.LogPrepare:
		env.Body = &paxosv1.Envelope_LogPrepare{LogPrepare: &paxosv1.LogPrepare{Ballot: ballotToProto(b.Ballot), Commit: b.Commit}}
	case paxos.LogPromise:
		env.Body = &paxosv1.Envelope_LogPromise{LogPromise: &paxosv1.LogPromise{
			Ballot: ballotToProto(b.Ballot), Entries: slotsToProto(b.Entries)}}
	case paxos.LogAccept:
		env.Body = &paxosv1.Envelope_LogAccept{LogAccept: &paxosv1.LogAccept{
			Ballot: ballotToProto(b.Ballot), Slot: b.Slot, Entry: entryToProto(b.Entry)}}
	case paxos.LogAccepted:
		env.Body = &paxosv1.Envelope_LogAccepted{LogAccepted: &paxosv1.LogAccepted{
			Ballot: ballotToProto(b.Ballot), Slot: b.Slot, Entry: entryToProto(b.Entry)}}
	case paxos.LogNack:
		env.Body = &paxosv1.Envelope_LogNack{LogNack: &paxosv1.LogNack{
			Ballot: ballotToProto(b.Ballot), Promised: ballotToProto(b.Promised)}}
	case paxos.Heartbeat:
		env.Body = &paxosv1.Envelope_Heartbeat{Heartbeat: &paxosv1.Heartbeat{Ballot: ballotToProto(b.Ballot), Commit: b.Commit}}
	case paxos.CatchupRequest:
		env.Body = &paxosv1.Envelope_CatchupRequest{CatchupRequest: &paxosv1.CatchupRequest{From: b.From}}
	case paxos.CatchupReply:
		env.Body = &paxosv1.Envelope_CatchupReply{CatchupReply: &paxosv1.CatchupReply{Entries: slotsToProto(b.Entries)}}
	case paxos.ClientRequest:
		env.Body = &paxosv1.Envelope_ClientRequest{ClientRequest: &paxosv1.ClientRequest{
			ClientId: b.ClientID, Seq: b.Seq, Cmd: []byte(b.Cmd)}}
	case paxos.ClientReply:
		env.Body = &paxosv1.Envelope_ClientReply{ClientReply: &paxosv1.ClientReply{
			ClientId: b.ClientID, Seq: b.Seq, Ok: b.OK, Result: []byte(b.Result), Leader: int32(b.Leader)}}
	default:
		return nil, fmt.Errorf("wire: unsupported message type %T", m.Body)
	}
	return env, nil
}

// FromProto converts an envelope back to a Paxos message. Missing nested
// fields decode as zero values, as protobuf intends.
func FromProto(env *paxosv1.Envelope) (paxos.Message, error) {
	m := paxos.Message{From: paxos.NodeID(env.GetFrom()), To: paxos.NodeID(env.GetTo())}
	switch b := env.GetBody().(type) {
	case *paxosv1.Envelope_Prepare:
		m.Body = paxos.Prepare{Ballot: ballotFromProto(b.Prepare.GetBallot())}
	case *paxosv1.Envelope_Promise:
		p := b.Promise
		m.Body = paxos.Promise{Ballot: ballotFromProto(p.GetBallot()), Accepted: ballotFromProto(p.GetAccepted()), Value: paxos.Value(p.GetValue())}
	case *paxosv1.Envelope_Accept:
		m.Body = paxos.Accept{Ballot: ballotFromProto(b.Accept.GetBallot()), Value: paxos.Value(b.Accept.GetValue())}
	case *paxosv1.Envelope_Accepted:
		m.Body = paxos.Accepted{Ballot: ballotFromProto(b.Accepted.GetBallot()), Value: paxos.Value(b.Accepted.GetValue())}
	case *paxosv1.Envelope_Nack:
		m.Body = paxos.Nack{Ballot: ballotFromProto(b.Nack.GetBallot()), Promised: ballotFromProto(b.Nack.GetPromised())}
	case *paxosv1.Envelope_LogPrepare:
		m.Body = paxos.LogPrepare{Ballot: ballotFromProto(b.LogPrepare.GetBallot()), Commit: b.LogPrepare.GetCommit()}
	case *paxosv1.Envelope_LogPromise:
		m.Body = paxos.LogPromise{Ballot: ballotFromProto(b.LogPromise.GetBallot()), Entries: slotsFromProto(b.LogPromise.GetEntries())}
	case *paxosv1.Envelope_LogAccept:
		a := b.LogAccept
		m.Body = paxos.LogAccept{Ballot: ballotFromProto(a.GetBallot()), Slot: a.GetSlot(), Entry: entryFromProto(a.GetEntry())}
	case *paxosv1.Envelope_LogAccepted:
		a := b.LogAccepted
		m.Body = paxos.LogAccepted{Ballot: ballotFromProto(a.GetBallot()), Slot: a.GetSlot(), Entry: entryFromProto(a.GetEntry())}
	case *paxosv1.Envelope_LogNack:
		m.Body = paxos.LogNack{Ballot: ballotFromProto(b.LogNack.GetBallot()), Promised: ballotFromProto(b.LogNack.GetPromised())}
	case *paxosv1.Envelope_Heartbeat:
		m.Body = paxos.Heartbeat{Ballot: ballotFromProto(b.Heartbeat.GetBallot()), Commit: b.Heartbeat.GetCommit()}
	case *paxosv1.Envelope_CatchupRequest:
		m.Body = paxos.CatchupRequest{From: b.CatchupRequest.GetFrom()}
	case *paxosv1.Envelope_CatchupReply:
		m.Body = paxos.CatchupReply{Entries: slotsFromProto(b.CatchupReply.GetEntries())}
	case *paxosv1.Envelope_ClientRequest:
		r := b.ClientRequest
		m.Body = paxos.ClientRequest{ClientID: r.GetClientId(), Seq: r.GetSeq(), Cmd: paxos.Value(r.GetCmd())}
	case *paxosv1.Envelope_ClientReply:
		r := b.ClientReply
		m.Body = paxos.ClientReply{ClientID: r.GetClientId(), Seq: r.GetSeq(), OK: r.GetOk(),
			Result: paxos.Value(r.GetResult()), Leader: paxos.NodeID(r.GetLeader())}
	default:
		return paxos.Message{}, ErrNoBody
	}
	return m, nil
}

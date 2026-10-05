// Package wire converts between the Paxos core's plain Go messages
// (internal/paxos), the transaction layer's messages (internal/txn, carried
// as paxos.Ext bodies) and their protobuf form (proto/paxos/v1,
// proto/txn/v1). It is the only place the two meet, so neither core
// package depends on protobuf.
package wire

import (
	"errors"
	"fmt"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
	paxosv1 "github.com/Shrutij516/paxos-txn-store/proto/paxos/v1"
	txnv1 "github.com/Shrutij516/paxos-txn-store/proto/txn/v1"
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
	case paxos.Ext:
		if err := extToProto(env, b.Body); err != nil {
			return nil, err
		}
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
	case *paxosv1.Envelope_PrepareReq:
		p := b.PrepareReq
		m.Body = paxos.Ext{Body: txn.PrepareReq{Txn: MetaFromProto(p.GetTxn()), Coord: txn.ShardID(p.GetCoord()),
			Participants: shardsFromProto(p.GetParticipants()), Part: PartFromProto(p.GetPart())}}
	case *paxosv1.Envelope_Vote:
		v := b.Vote
		m.Body = paxos.Ext{Body: txn.Vote{Txn: txn.ID(v.GetTxn()), Shard: txn.ShardID(v.GetShard()), Yes: v.GetYes()}}
	case *paxosv1.Envelope_Decision:
		m.Body = paxos.Ext{Body: txn.Decision{Txn: txn.ID(b.Decision.GetTxn()), Commit: b.Decision.GetCommit()}}
	case *paxosv1.Envelope_QueryOutcome:
		q := b.QueryOutcome
		m.Body = paxos.Ext{Body: txn.QueryOutcome{Txn: txn.ID(q.GetTxn()), From: txn.ShardID(q.GetFrom()),
			Participants: shardsFromProto(q.GetParticipants())}}
	case *paxosv1.Envelope_WoundReq:
		m.Body = paxos.Ext{Body: txn.WoundReq{Txn: txn.ID(b.WoundReq.GetTxn())}}
	default:
		return paxos.Message{}, ErrNoBody
	}
	return m, nil
}

// ---- Transaction messages (paxos.Ext bodies) ----

func shardsToProto(s []txn.ShardID) []uint32 {
	if len(s) == 0 {
		return nil
	}
	out := make([]uint32, len(s))
	for i, sh := range s {
		out[i] = uint32(sh)
	}
	return out
}

func shardsFromProto(s []uint32) []txn.ShardID {
	if len(s) == 0 {
		return nil
	}
	out := make([]txn.ShardID, len(s))
	for i, sh := range s {
		out[i] = txn.ShardID(sh)
	}
	return out
}

func mapOrNil[V any](m map[string]V) map[string]V {
	if len(m) == 0 {
		return nil
	}
	return m
}

// MetaToProto converts a transaction's identity.
func MetaToProto(m txn.Meta) *txnv1.TxnMeta { return &txnv1.TxnMeta{Id: uint64(m.ID), Ts: m.TS} }

// MetaFromProto converts a transaction's identity back.
func MetaFromProto(m *txnv1.TxnMeta) txn.Meta {
	return txn.Meta{ID: txn.ID(m.GetId()), TS: m.GetTs()}
}

// PartToProto converts one shard's share of a transaction. Empty maps are
// sent as absent, and come back as nil.
func PartToProto(p txn.Part) *txnv1.Part {
	return &txnv1.Part{Shard: uint32(p.Shard), Reads: mapOrNil(p.Reads), Writes: mapOrNil(p.Writes)}
}

// PartFromProto converts a part back.
func PartFromProto(p *txnv1.Part) txn.Part {
	return txn.Part{Shard: txn.ShardID(p.GetShard()), Reads: mapOrNil(p.GetReads()), Writes: mapOrNil(p.GetWrites())}
}

func extToProto(env *paxosv1.Envelope, body any) error {
	switch b := body.(type) {
	case txn.PrepareReq:
		env.Body = &paxosv1.Envelope_PrepareReq{PrepareReq: &txnv1.PrepareReq{
			Txn: MetaToProto(b.Txn), Coord: uint32(b.Coord), Participants: shardsToProto(b.Participants), Part: PartToProto(b.Part)}}
	case txn.Vote:
		env.Body = &paxosv1.Envelope_Vote{Vote: &txnv1.Vote{Txn: uint64(b.Txn), Shard: uint32(b.Shard), Yes: b.Yes}}
	case txn.Decision:
		env.Body = &paxosv1.Envelope_Decision{Decision: &txnv1.Decision{Txn: uint64(b.Txn), Commit: b.Commit}}
	case txn.QueryOutcome:
		env.Body = &paxosv1.Envelope_QueryOutcome{QueryOutcome: &txnv1.QueryOutcome{
			Txn: uint64(b.Txn), From: uint32(b.From), Participants: shardsToProto(b.Participants)}}
	case txn.WoundReq:
		env.Body = &paxosv1.Envelope_WoundReq{WoundReq: &txnv1.WoundReq{Txn: uint64(b.Txn)}}
	default:
		// Client requests and replies never cross the network: the Txn
		// service hands them to the local shard directly.
		return fmt.Errorf("wire: unsupported transaction message %T", body)
	}
	return nil
}

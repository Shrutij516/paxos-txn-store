package server

import (
	"context"
	"fmt"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
	"github.com/Shrutij516/paxos-txn-store/internal/wire"
	paxosv1 "github.com/Shrutij516/paxos-txn-store/proto/paxos/v1"
	txnv1 "github.com/Shrutij516/paxos-txn-store/proto/txn/v1"
)

// ---- Peer service ----

type peerService struct {
	paxosv1.UnimplementedPeerServer
	n *Node
}

func (p peerService) Send(_ context.Context, env *paxosv1.Envelope) (*paxosv1.SendAck, error) {
	m, err := wire.FromProto(env)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if m.To != p.n.cfg.ID {
		return nil, status.Error(codes.InvalidArgument, "message for another node")
	}
	if int(env.GetShard()) >= len(p.n.shards) {
		return nil, status.Errorf(codes.InvalidArgument, "no shard %d", env.GetShard())
	}
	select {
	case p.n.shards[env.GetShard()].inbox <- m:
	default: // overloaded: drop, Paxos and two-phase commit retry
	}
	return &paxosv1.SendAck{}, nil
}

// ---- Txn service ----

type txnService struct {
	txnv1.UnimplementedTxnServer
	n *Node
}

var errNoTxn = status.Error(codes.InvalidArgument, "txn id must be non-zero")

var errLost = status.Error(codes.Unavailable, "shard leadership changed while the commit waited; outcome unknown, retry the commit")

// await waits for a reply from the shard's event loop. If the caller's
// context ends first, forget runs on the loop to drop the waiter, so the
// tables only hold requests someone is still waiting for.
func await[T any](ctx context.Context, s *shard, ch chan T, forget func()) (T, error) {
	var zero T
	select {
	case r := <-ch:
		return r, nil
	case <-ctx.Done():
		_ = s.run(forget)
		return zero, status.FromContextError(ctx.Err()).Err()
	case <-s.done:
		return zero, status.Error(codes.Unavailable, errStopped.Error())
	}
}

func (n *Node) keyShard(key string) *shard { return n.shards[txn.ShardOf(key, len(n.shards))] }

func (n *Node) reqCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, n.cfg.ReqTimeout)
}

func (t txnService) Begin(_ context.Context, r *txnv1.BeginRequest) (*txnv1.BeginResponse, error) {
	// Random IDs: unique across nodes and restarts without coordination.
	id := txn.ID(seed())
	for id == 0 {
		id = txn.ID(seed())
	}
	ts := r.GetTs()
	if ts == 0 {
		ts = uint64(time.Now().UnixNano())
	}
	return &txnv1.BeginResponse{Txn: wire.MetaToProto(txn.Meta{ID: id, TS: ts}), Shards: uint32(len(t.n.shards))}, nil
}

func (t txnService) Read(ctx context.Context, r *txnv1.ReadRequest) (*txnv1.ReadResponse, error) {
	meta := wire.MetaFromProto(r.GetTxn())
	if meta.ID == 0 {
		return nil, errNoTxn
	}
	ctx, cancel := t.n.reqCtx(ctx)
	defer cancel()
	s := t.n.keyShard(r.GetKey())
	k := readKey{meta.ID, r.GetKey()}
	ch := make(chan readResult, 1)
	var hint *txnv1.LeaderHint
	if err := s.run(func() {
		if !s.ts.Leading() {
			hint = s.leaderHint()
			return
		}
		s.reads[k] = ch
		s.ts.Handle(paxos.Message{From: clientEndpoint, To: t.n.cfg.ID,
			Body: paxos.Ext{Body: txn.ReadReq{Txn: meta, Key: r.GetKey(), Client: clientEndpoint}}})
	}); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if hint != nil {
		return &txnv1.ReadResponse{Status: txnv1.Status_STATUS_NOT_LEADER, Leader: hint}, nil
	}
	res, err := await(ctx, s, ch, func() {
		if s.reads[k] == ch {
			delete(s.reads, k)
		}
	})
	switch {
	case err != nil:
		return nil, err
	case res.notLeader:
		return &txnv1.ReadResponse{Status: txnv1.Status_STATUS_NOT_LEADER}, nil
	case res.resp.Aborted:
		return &txnv1.ReadResponse{Status: txnv1.Status_STATUS_ABORTED}, nil
	}
	return &txnv1.ReadResponse{Status: txnv1.Status_STATUS_OK, Value: res.resp.Value, Version: res.resp.Version}, nil
}

func (t txnService) Write(_ context.Context, r *txnv1.WriteRequest) (*txnv1.WriteResponse, error) {
	meta := wire.MetaFromProto(r.GetTxn())
	if meta.ID == 0 {
		return nil, errNoTxn
	}
	s := t.n.keyShard(r.GetKey())
	out := &txnv1.WriteResponse{}
	if err := s.run(func() {
		switch {
		case !s.ts.Leading():
			out.Status, out.Leader = txnv1.Status_STATUS_NOT_LEADER, s.leaderHint()
		case s.ts.Touch(meta.ID):
			out.Status = txnv1.Status_STATUS_OK
		default:
			out.Status = txnv1.Status_STATUS_ABORTED
		}
	}); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return out, nil
}

// parts validates a commit request's parts and returns them sorted by
// shard: at least one, each on an existing shard, no shard twice, and
// every key on the shard of its part.
func (n *Node) parts(ps []*txnv1.Part) ([]txn.Part, error) {
	if len(ps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "commit needs at least one part")
	}
	out := make([]txn.Part, 0, len(ps))
	for _, p := range ps {
		part := wire.PartFromProto(p)
		if int(part.Shard) >= len(n.shards) {
			return nil, status.Errorf(codes.InvalidArgument, "no shard %d", part.Shard)
		}
		for k := range part.Reads {
			if err := n.checkKey(k, part.Shard); err != nil {
				return nil, err
			}
		}
		for k := range part.Writes {
			if err := n.checkKey(k, part.Shard); err != nil {
				return nil, err
			}
		}
		out = append(out, part)
	}
	slices.SortFunc(out, func(a, b txn.Part) int { return int(a.Shard) - int(b.Shard) })
	for i := 1; i < len(out); i++ {
		if out[i].Shard == out[i-1].Shard {
			return nil, status.Errorf(codes.InvalidArgument, "shard %d appears twice", out[i].Shard)
		}
	}
	return out, nil
}

func (n *Node) checkKey(k string, sh txn.ShardID) error {
	if got := txn.ShardOf(k, len(n.shards)); got != sh {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("key %q is on shard %d, not %d", k, got, sh))
	}
	return nil
}

func (t txnService) Commit(ctx context.Context, r *txnv1.CommitRequest) (*txnv1.CommitResponse, error) {
	meta := wire.MetaFromProto(r.GetTxn())
	if meta.ID == 0 {
		return nil, errNoTxn
	}
	parts, err := t.n.parts(r.GetParts())
	if err != nil {
		return nil, err
	}
	ctx, cancel := t.n.reqCtx(ctx)
	defer cancel()
	coord := parts[0].Shard
	s := t.n.shards[coord]
	ch := make(chan commitResult, 1)
	var hint *txnv1.LeaderHint
	if err := s.run(func() {
		if !s.ts.Leading() {
			hint = s.leaderHint()
			return
		}
		s.commits[meta.ID] = ch
		s.ts.Handle(paxos.Message{From: clientEndpoint, To: t.n.cfg.ID,
			Body: paxos.Ext{Body: txn.CommitReq{Txn: meta, Coord: coord, Parts: parts, Client: clientEndpoint}}})
	}); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	if hint != nil {
		return &txnv1.CommitResponse{Status: txnv1.Status_STATUS_NOT_LEADER, Leader: hint}, nil
	}
	res, err := await(ctx, s, ch, func() {
		if s.commits[meta.ID] == ch {
			delete(s.commits, meta.ID)
		}
	})
	switch {
	case err != nil:
		return nil, err
	case res.lost:
		return nil, errLost
	case res.resp.Committed:
		return &txnv1.CommitResponse{Status: txnv1.Status_STATUS_OK}, nil
	}
	return &txnv1.CommitResponse{Status: txnv1.Status_STATUS_ABORTED}, nil
}

func (t txnService) Abort(_ context.Context, r *txnv1.AbortRequest) (*txnv1.AbortResponse, error) {
	if r.GetTxn() == 0 {
		return nil, errNoTxn
	}
	if int(r.GetShard()) >= len(t.n.shards) {
		return nil, status.Errorf(codes.InvalidArgument, "no shard %d", r.GetShard())
	}
	s := t.n.shards[r.GetShard()]
	out := &txnv1.AbortResponse{}
	if err := s.run(func() {
		if !s.ts.Leading() {
			out.Status, out.Leader = txnv1.Status_STATUS_NOT_LEADER, s.leaderHint()
			return
		}
		s.ts.Handle(paxos.Message{From: clientEndpoint, To: t.n.cfg.ID, Body: paxos.Ext{Body: txn.AbortReq{Txn: txn.ID(r.GetTxn())}}})
		out.Status = txnv1.Status_STATUS_OK
	}); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return out, nil
}

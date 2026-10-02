package paxos_test

import (
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/Shrutij516/paxos-txn-store/internal/kv"
	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/storage"
)

// flakyLog wraps Memory and fails LogStorage calls on demand.
type flakyLog struct {
	*storage.Memory
	failPromised, failAccepted, failRound, failLoad bool
}

func (f *flakyLog) SavePromised(b paxos.Ballot) error {
	if f.failPromised {
		return errDisk
	}
	return f.Memory.SavePromised(b)
}

func (f *flakyLog) SaveAccepted(e paxos.SlotEntry) error {
	if f.failAccepted {
		return errDisk
	}
	return f.Memory.SaveAccepted(e)
}

func (f *flakyLog) SaveRound(r uint64) error {
	if f.failRound {
		return errDisk
	}
	return f.Memory.SaveRound(r)
}

func (f *flakyLog) LoadAccepted() ([]paxos.SlotEntry, error) {
	if f.failLoad {
		return nil, errDisk
	}
	return f.Memory.LoadAccepted()
}

// fastTiming makes the first Tick start an election.
var fastTiming = paxos.LogTiming{HeartbeatEvery: 2, ElectionMin: 1, ElectionMax: 1, CatchupBatch: 2}

func newTestReplica(t *testing.T, id paxos.NodeID, st paxos.LogStorage, tr paxos.Transport) *paxos.Replica {
	t.Helper()
	r, err := paxos.NewReplica(paxos.ReplicaConfig{
		ID: id, Peers: peers3, Rand: rand.New(rand.NewPCG(1, 2)),
		Timing: fastTiming, StateMachine: kv.New(),
	}, st, tr)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func bodies[T any](ms []paxos.Message) []T {
	var out []T
	for _, m := range ms {
		if b, ok := m.Body.(T); ok {
			out = append(out, b)
		}
	}
	return out
}

func put(k, v string) paxos.Entry { return paxos.Entry{ClientID: 1, Seq: 1, Cmd: kv.Put(k, v)} }

// A new leader re-proposes the highest-ballot value per slot and fills
// gaps with no-ops.
func TestReplicaTakeoverFillsGapsWithNoops(t *testing.T) {
	tr := &outbox{}
	st := storage.NewMemory()
	_ = st.SaveRound(5)
	r := newTestReplica(t, 1, st, tr)
	r.Tick()
	prep := bodies[paxos.LogPrepare](tr.take())
	if len(prep) != 3 || prep[0] != (paxos.LogPrepare{Ballot: b(6, 1), Commit: 0}) {
		t.Fatalf("want LogPrepare (6,1) x3, got %v", prep)
	}
	eA, eB, eC := put("k", "A"), put("k", "B"), put("k", "C")
	r.Handle(paxos.Message{From: 2, To: 1, Body: paxos.LogPromise{Ballot: b(6, 1), Entries: []paxos.SlotEntry{
		{Slot: 1, Ballot: b(2, 2), Entry: eA},
		{Slot: 3, Ballot: b(3, 2), Entry: eC},
	}}})
	r.Handle(paxos.Message{From: 2, To: 1, Body: paxos.LogPromise{Ballot: b(9, 9)}}) // stale ballot
	if r.IsLeader() {
		t.Fatal("leader after one promise")
	}
	r.Handle(paxos.Message{From: 3, To: 1, Body: paxos.LogPromise{Ballot: b(6, 1), Entries: []paxos.SlotEntry{
		{Slot: 1, Ballot: b(4, 3), Entry: eB},
	}}})
	if !r.IsLeader() || r.Ballot() != b(6, 1) {
		t.Fatal("not leader after majority of promises")
	}
	want := map[uint64]paxos.Entry{1: eB, 2: {Noop: true}, 3: eC}
	got := map[uint64]paxos.Entry{}
	for _, a := range bodies[paxos.LogAccept](tr.take()) {
		if a.Ballot != b(6, 1) {
			t.Fatalf("accept at wrong ballot: %+v", a)
		}
		got[a.Slot] = a.Entry
	}
	for s, e := range want {
		if got[s] != e {
			t.Fatalf("slot %d proposed %+v, want %+v", s, got[s], e)
		}
	}
	// The next client request goes after the recovered slots.
	r.Handle(paxos.Message{From: 100, To: 1, Body: paxos.ClientRequest{ClientID: 5, Seq: 1, Cmd: kv.Get("k")}})
	if acc := bodies[paxos.LogAccept](tr.take()); len(acc) != 3 || acc[0].Slot != 4 {
		t.Fatalf("client request not proposed at slot 4: %v", acc)
	}
}

func TestReplicaCommitApplyAndReply(t *testing.T) {
	tr := &outbox{}
	r := newTestReplica(t, 1, storage.NewMemory(), tr)
	r.Tick()
	tr.take()
	for _, from := range []paxos.NodeID{1, 2} {
		r.Handle(paxos.Message{From: from, To: 1, Body: paxos.LogPromise{Ballot: b(1, 1)}})
	}
	tr.take()
	r.Handle(paxos.Message{From: 100, To: 1, Body: paxos.ClientRequest{ClientID: 5, Seq: 1, Cmd: kv.Put("k", "v")}})
	acc := bodies[paxos.LogAccept](tr.take())[0]
	vote := paxos.LogAccepted(acc)
	r.Handle(paxos.Message{From: 1, To: 1, Body: vote})
	r.Handle(paxos.Message{From: 1, To: 1, Body: vote}) // duplicate vote
	r.Handle(paxos.Message{From: 2, To: 1, Body: paxos.LogAccepted{Ballot: b(0, 2), Slot: 1}})
	r.Handle(paxos.Message{From: 2, To: 1, Body: paxos.LogAccepted{Ballot: b(1, 1), Slot: 99}})
	if r.Commit() != 0 {
		t.Fatal("committed without a majority")
	}
	r.Handle(paxos.Message{From: 2, To: 1, Body: vote})
	if r.Commit() != 1 {
		t.Fatalf("commit = %d, want 1", r.Commit())
	}
	if e, ok := r.Committed(1); !ok || e != acc.Entry {
		t.Fatalf("Committed(1) = %+v, %v", e, ok)
	}
	if _, ok := r.Committed(0); ok {
		t.Fatal("slot 0 must not exist")
	}
	replies := bodies[paxos.ClientReply](tr.take())
	if len(replies) != 1 || !replies[0].OK || replies[0].Seq != 1 {
		t.Fatalf("want one OK reply, got %v", replies)
	}
	// Heartbeats retransmit nothing once everything is chosen.
	r.Tick()
	r.Tick()
	if acc := bodies[paxos.LogAccept](tr.take()); len(acc) != 0 {
		t.Fatalf("retransmitted chosen slot: %v", acc)
	}
}

func TestReplicaFollowerLearnsCommitAndCatchesUp(t *testing.T) {
	tr := &outbox{}
	r := newTestReplica(t, 3, storage.NewMemory(), tr)
	lb := b(4, 2)
	e1, e2, e3 := put("a", "1"), put("a", "2"), put("a", "3")
	r.Handle(paxos.Message{From: 2, To: 3, Body: paxos.LogAccept{Ballot: lb, Slot: 1, Entry: e1}})
	r.Handle(paxos.Message{From: 2, To: 3, Body: paxos.LogAccept{Ballot: lb, Slot: 1, Entry: e1}}) // duplicate
	if got := bodies[paxos.LogAccepted](tr.take()); len(got) != 2 {
		t.Fatalf("want 2 LogAccepted, got %v", got)
	}
	// Client requests to a follower are redirected to the leader.
	r.Handle(paxos.Message{From: 100, To: 3, Body: paxos.ClientRequest{ClientID: 1, Seq: 1}})
	if rep := bodies[paxos.ClientReply](tr.take()); len(rep) != 1 || rep[0].OK || rep[0].Leader != 2 {
		t.Fatalf("want redirect to 2, got %v", rep)
	}
	r.Handle(paxos.Message{From: 2, To: 3, Body: paxos.Heartbeat{Ballot: lb, Commit: 3}})
	if r.Commit() != 1 {
		t.Fatalf("commit = %d, want 1 from own accepted entry", r.Commit())
	}
	if req := bodies[paxos.CatchupRequest](tr.take()); len(req) != 1 || req[0].From != 2 {
		t.Fatalf("want CatchupRequest from 2, got %v", req)
	}
	r.Handle(paxos.Message{From: 2, To: 3, Body: paxos.CatchupReply{Entries: []paxos.SlotEntry{{Slot: 3, Entry: e3}, {Slot: 2, Entry: e2}, {Slot: 1, Entry: e1}}}})
	if r.Commit() != 3 {
		t.Fatalf("commit = %d after catch-up, want 3", r.Commit())
	}
	// Serving catch-up is batched.
	r.Handle(paxos.Message{From: 1, To: 3, Body: paxos.CatchupRequest{From: 1}})
	rep := bodies[paxos.CatchupReply](tr.take())
	if len(rep) != 1 || len(rep[0].Entries) != 2 || rep[0].Entries[1].Entry != e2 {
		t.Fatalf("want a batch of 2, got %v", rep)
	}
	r.Handle(paxos.Message{From: 1, To: 3, Body: paxos.CatchupRequest{From: 9}})
	if rep := tr.take(); len(rep) != 0 {
		t.Fatalf("nothing to send, got %v", rep)
	}
	// A stale leader is told about the newer promise.
	r.Handle(paxos.Message{From: 1, To: 3, Body: paxos.Heartbeat{Ballot: b(1, 1)}})
	if n := bodies[paxos.LogNack](tr.take()); len(n) != 1 || n[0].Promised != lb {
		t.Fatalf("want LogNack, got %v", n)
	}
}

func TestReplicaStepsDown(t *testing.T) {
	tr := &outbox{}
	r := newTestReplica(t, 1, storage.NewMemory(), tr)
	r.Tick()
	for _, from := range []paxos.NodeID{1, 2} {
		r.Handle(paxos.Message{From: from, To: 1, Body: paxos.LogPromise{Ballot: b(1, 1)}})
	}
	if !r.IsLeader() {
		t.Fatal("not leader")
	}
	r.Handle(paxos.Message{From: 2, To: 1, Body: paxos.LogNack{Ballot: b(1, 1), Promised: b(3, 2)}})
	if r.IsLeader() {
		t.Fatal("still leader after Nack with higher ballot")
	}
	// Next election outranks what the Nack revealed.
	r.Tick()
	if p := bodies[paxos.LogPrepare](tr.take()); len(p) == 0 || p[len(p)-1].Ballot != b(4, 1) {
		t.Fatalf("want Prepare (4,1), got %v", p)
	}
	// A higher Prepare from another node makes a candidate yield and Nack
	// lower ballots.
	r.Handle(paxos.Message{From: 3, To: 1, Body: paxos.LogPrepare{Ballot: b(7, 3)}})
	r.Handle(paxos.Message{From: 2, To: 1, Body: paxos.LogPrepare{Ballot: b(5, 2)}})
	r.Handle(paxos.Message{From: 2, To: 1, Body: paxos.LogAccept{Ballot: b(5, 2), Slot: 1}})
	if n := bodies[paxos.LogNack](tr.take()); len(n) != 2 {
		t.Fatalf("want 2 Nacks, got %v", n)
	}
	r.Handle(paxos.Message{From: 1, To: 1, Body: paxos.LogPromise{Ballot: b(4, 1)}})
	r.Handle(paxos.Message{From: 2, To: 1, Body: paxos.LogPromise{Ballot: b(4, 1)}})
	if r.IsLeader() {
		t.Fatal("became leader after yielding")
	}
}

func TestReplicaStorageFailures(t *testing.T) {
	tr := &outbox{}
	st := &flakyLog{Memory: storage.NewMemory(), failPromised: true, failAccepted: true, failRound: true}
	r := newTestReplica(t, 1, st, tr)
	r.Tick()
	r.Handle(paxos.Message{From: 2, To: 1, Body: paxos.LogPrepare{Ballot: b(1, 2)}})
	r.Handle(paxos.Message{From: 2, To: 1, Body: paxos.LogAccept{Ballot: b(1, 2), Slot: 1}})
	r.Handle(paxos.Message{From: 2, To: 1, Body: paxos.Heartbeat{Ballot: b(1, 2)}})
	if got := tr.take(); len(got) != 0 {
		t.Fatalf("replied without persisting: %v", got)
	}
	st.failPromised = false
	r.Handle(paxos.Message{From: 2, To: 1, Body: paxos.LogAccept{Ballot: b(1, 2), Slot: 1}})
	if got := tr.take(); len(got) != 0 {
		t.Fatalf("acked accept without persisting it: %v", got)
	}

	r0 := rand.New(rand.NewPCG(1, 1))
	cfg := paxos.ReplicaConfig{ID: 1, Peers: peers3, Rand: r0, StateMachine: kv.New()}
	if _, err := paxos.NewReplica(cfg, &flakyLog{Memory: storage.NewMemory(), failLoad: true}, tr); !errors.Is(err, errDisk) {
		t.Errorf("want load error, got %v", err)
	}
	bad := cfg
	bad.ID = 9
	if _, err := paxos.NewReplica(bad, storage.NewMemory(), tr); err == nil {
		t.Error("want error for ID outside Peers")
	}
	bad = cfg
	bad.StateMachine = nil
	if _, err := paxos.NewReplica(bad, storage.NewMemory(), tr); err == nil {
		t.Error("want error for nil StateMachine")
	}
	r2, err := paxos.NewReplica(cfg, storage.NewMemory(), tr)
	if err != nil || r2.ID() != 1 {
		t.Fatalf("NewReplica = %v, %v", r2, err)
	}
}

// A restarted replica reloads its promise and accepted entries.
func TestReplicaRestartKeepsAcceptorState(t *testing.T) {
	tr := &outbox{}
	st := storage.NewMemory()
	r := newTestReplica(t, 3, st, tr)
	e := put("x", "1")
	r.Handle(paxos.Message{From: 2, To: 3, Body: paxos.LogAccept{Ballot: b(4, 2), Slot: 2, Entry: e}})
	tr.take()
	r = newTestReplica(t, 3, st, tr)
	r.Handle(paxos.Message{From: 1, To: 3, Body: paxos.LogPrepare{Ballot: b(3, 1)}})
	if n := bodies[paxos.LogNack](tr.take()); len(n) != 1 {
		t.Fatal("restarted acceptor forgot its promise")
	}
	r.Handle(paxos.Message{From: 1, To: 3, Body: paxos.LogPrepare{Ballot: b(5, 1), Commit: 1}})
	p := bodies[paxos.LogPromise](tr.take())
	if len(p) != 1 || len(p[0].Entries) != 1 || p[0].Entries[0].Entry != e {
		t.Fatalf("restarted acceptor lost its accepted entry: %v", p)
	}
	r.Handle(paxos.Message{From: 1, To: 3, Body: paxos.LogPrepare{Ballot: b(6, 1), Commit: 2}})
	if p := bodies[paxos.LogPromise](tr.take()); len(p[0].Entries) != 0 {
		t.Fatalf("promise must only carry entries above the commit index: %v", p)
	}
}

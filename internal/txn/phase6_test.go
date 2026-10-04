package txn

import (
	"fmt"
	"testing"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/transport"
)

// sameShardAccounts returns two accounts on one shard.
func sameShardAccounts() (string, string, ShardID) {
	for i := 0; i < numAccounts; i++ {
		for j := i + 1; j < numAccounts; j++ {
			if si := ShardOf(account(i), DefaultShards); si == ShardOf(account(j), DefaultShards) {
				return account(i), account(j), si
			}
		}
	}
	panic("no two accounts share a shard")
}

// A one-phase commit that fails at the leader is answered only after an
// abort record for it is in the shard's log. A retried commit can reach a
// new leader while an earlier leader's one-phase record for the same txn
// is still on its way into the log; the first record in the log decides,
// so the client is never told "aborted" for a txn that then commits.
func TestOnePhaseAbortIsLogged(t *testing.T) {
	a, b, sh := sameShardAccounts()
	w := newWorld(7, worldOpts{Clients: 1, OpsPerClient: 0, Faults: transport.Faults{MaxDelay: 2}})
	tc := w.cli[0]
	tc.plan = func(*client) ([]string, string) { return []string{a, b}, b }
	var told, unlogged error
	w.net.OnSend = func(m paxos.Message) {
		w.observe(m)
		ext, ok := m.Body.(paxos.Ext)
		if !ok {
			return
		}
		r, ok := ext.Body.(CommitResp)
		if !ok || r.Committed || unlogged != nil {
			return
		}
		told = fmt.Errorf("txn %d", r.Txn)
		if c, known := w.nodes[m.From].sm.Outcome(r.Txn); !known || c {
			unlogged = fmt.Errorf("node %d told the client txn %d aborted with outcome (%v, known %v) in its log", m.From, r.Txn, c, known)
		}
	}
	tc.hold, tc.ops, tc.think = true, 1, 1
	if _, ok := w.runUntil(2000, func() bool { return tc.phase == phHeld }); !ok {
		t.Fatal("T never finished its reads")
	}
	tid := tc.meta.ID
	for _, n := range shardNodes(sh) { // T is now doomed at the leader
		w.net.Send(paxos.Message{From: tc.id, To: n, Body: paxos.Ext{Body: AbortReq{Txn: tid}}})
	}
	w.run(20, false)
	tc.release()
	if _, ok := w.runUntil(calmLimit, w.quiescent); !ok {
		t.Fatal("not quiescent")
	}
	if told == nil {
		t.Fatal("the client was never told its one-phase commit aborted")
	}
	if unlogged != nil {
		t.Fatal(unlogged)
	}
	sm := w.canonical()[sh]
	if c, known := sm.Outcome(tid); !known || c || sm.IsOnePhase(tid) {
		t.Fatalf("T's outcome in the log: commit=%v known=%v onephase=%v, want a logged abort", c, known, sm.IsOnePhase(tid))
	}
	if tc.aborts == 0 || tc.commits != 1 {
		t.Fatalf("client: aborts=%d commits=%d, want an abort and a committed retry", tc.aborts, tc.commits)
	}
	if err := checkNoBank(w); err != nil {
		t.Fatal(err)
	}
}

// Touch (the Write RPC) succeeds only at the leader, for a txn that is
// neither doomed nor finished.
func TestTouch(t *testing.T) {
	a, b, sh := sameShardAccounts()
	w := newWorld(8, worldOpts{Clients: 1, OpsPerClient: 0, Faults: transport.Faults{MaxDelay: 2}})
	tc := w.cli[0]
	tc.plan = func(*client) ([]string, string) { return []string{a, b}, b }
	tc.ops, tc.think = 1, 1
	if _, ok := w.runUntil(calmLimit, w.quiescent); !ok {
		t.Fatal("not quiescent")
	}
	l := w.leader(sh)
	if !l.srv.Touch(12345) {
		t.Fatal("leader refused a fresh txn")
	}
	if l.srv.Touch(tc.meta.ID) {
		t.Fatal("leader accepted a write for a committed txn")
	}
	l.srv.Handle(paxos.Message{From: tc.id, To: l.id, Body: paxos.Ext{Body: AbortReq{Txn: 12345}}})
	if l.srv.Touch(12345) {
		t.Fatal("leader accepted a write for an aborted txn")
	}
	for _, id := range shardNodes(sh) {
		if n := w.nodes[id]; n != l && n.srv.Touch(999) {
			t.Fatalf("follower %d accepted a write", id)
		}
	}
}

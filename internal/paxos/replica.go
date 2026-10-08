package paxos

import (
	"errors"
	"fmt"
	"maps"
	"slices"
)

type role int

const (
	follower role = iota
	candidate
	leader
)

type pendingReq struct {
	seq  uint64
	from NodeID
}

// Replica is one member of a Multi-Paxos group. It plays acceptor for every
// slot, becomes leader by winning a single Prepare that covers all slots
// above its commit index, and applies committed entries in order to its
// StateMachine. Like Node, it is driven only by Handle and Tick and is not
// safe for concurrent use.
type Replica struct {
	id     NodeID
	peers  []NodeID
	quorum int
	store  LogStorage
	tr     Transport
	rng    Rand
	t      LogTiming
	sm     StateMachine
	obs    Observer

	// Acceptor state, persisted before any reply.
	promised  Ballot
	accepted  map[uint64]SlotEntry
	lastRound uint64

	maxSeen uint64
	role    role
	ballot  Ballot // our ballot while candidate or leader
	leader  NodeID // leader we believe in, 0 if unknown
	timer   int    // election timer, or heartbeat timer while leader
	backoff int    // election timeout multiplier: 1, doubled per failed election
	ran     bool   // we started an election and no leader has held since
	ticks   int    // Tick calls so far
	steadyB Ballot // ballot of the leader we last heard from (or are)
	since   int    // ticks when we first heard from steadyB

	// Candidate state.
	promises map[NodeID]LogPromise

	// Leader state.
	nextSlot  uint64
	proposals map[uint64]Entry
	votes     map[uint64]map[NodeID]struct{}

	// Learned state, kept in memory and rebuilt by catch-up after restart.
	chosen  map[uint64]Entry
	commit  uint64 // every slot <= commit is in chosen
	applied uint64
	pending map[uint64]pendingReq // by client ID
	// leaderCommit is the highest commit index a leader reported in a
	// heartbeat. Only observed (Lag), never acted on.
	leaderCommit uint64

	// Test-only switches, set from export_test.go.
	skipPrepare    bool
	ignorePromised bool
	replyFirst     bool // reply to Prepare and Accept before persisting

	// Takeover counters, read only by tests through export_test.go.
	statNoops     int // gaps filled with a no-op
	statRecovered int // slots re-proposed from a value reported in promises
	statContested int // of those, slots where promises reported different entries
}

// NewReplica builds a replica and restores acceptor state from store.
func NewReplica(cfg ReplicaConfig, store LogStorage, tr Transport) (*Replica, error) {
	if !slices.Contains(cfg.Peers, cfg.ID) {
		return nil, errors.New("paxos: Peers must include ID")
	}
	if cfg.Rand == nil || cfg.StateMachine == nil {
		return nil, errors.New("paxos: Rand and StateMachine are required")
	}
	if cfg.Timing == (LogTiming{}) {
		cfg.Timing = DefaultLogTiming
	}
	if cfg.Timing.ElectionBackoff < 1 {
		cfg.Timing.ElectionBackoff = DefaultElectionBackoff
	}
	rnd, err := store.LoadRound()
	if err != nil {
		return nil, err
	}
	promised, err := store.LoadPromised()
	if err != nil {
		return nil, err
	}
	acc, err := store.LoadAccepted()
	if err != nil {
		return nil, err
	}
	if cfg.Observer == nil {
		cfg.Observer = NoObserver{}
	}
	peers := slices.Clone(cfg.Peers)
	slices.Sort(peers)
	r := &Replica{
		id: cfg.ID, peers: peers, quorum: len(peers)/2 + 1,
		store: store, tr: tr, rng: cfg.Rand, t: cfg.Timing, sm: cfg.StateMachine, obs: cfg.Observer,
		promised: promised, accepted: make(map[uint64]SlotEntry, len(acc)), lastRound: rnd,
		chosen: make(map[uint64]Entry), pending: make(map[uint64]pendingReq),
		backoff: 1,
	}
	for _, se := range acc {
		r.accepted[se.Slot] = se
	}
	// Rebuild the state machine (including its dedup table) by replaying the
	// durable committed prefix in order. Anything committed after the last
	// durable write is fetched again through catch-up.
	committed, err := store.LoadCommitted()
	if err != nil {
		return nil, err
	}
	for i, se := range committed {
		if se.Slot != uint64(i+1) {
			return nil, fmt.Errorf("paxos: committed log has slot %d at position %d", se.Slot, i+1)
		}
		r.chosen[se.Slot] = se.Entry
		r.commit = se.Slot
		r.applied = se.Slot
		r.sm.Apply(se.Slot, se.Entry)
	}
	r.resetElectionTimer()
	return r, nil
}

// ID returns the replica's ID.
func (r *Replica) ID() NodeID { return r.id }

// IsLeader reports whether this replica currently acts as leader.
func (r *Replica) IsLeader() bool { return r.role == leader }

// Lag returns how many slots this replica knows are committed (by itself
// or, from heartbeats, by the leader) but has not applied yet.
func (r *Replica) Lag() uint64 { return max(r.commit, r.leaderCommit) - r.applied }

// Leader returns the leader this replica believes in (itself while it
// leads), or 0 if it knows of none.
func (r *Replica) Leader() NodeID { return r.leader }

// Ballot returns the replica's own ballot as candidate or leader.
func (r *Replica) Ballot() Ballot { return r.ballot }

// Commit returns the commit index: every slot up to it is known chosen.
func (r *Replica) Commit() uint64 { return r.commit }

// Committed returns the chosen entry at slot, if it is at or below Commit.
func (r *Replica) Committed(slot uint64) (Entry, bool) {
	if slot == 0 || slot > r.commit {
		return Entry{}, false
	}
	return r.chosen[slot], true
}

// Tick advances the replica's logical clock by one step.
func (r *Replica) Tick() {
	r.ticks++
	r.timer--
	if r.timer > 0 {
		return
	}
	if r.role == leader {
		r.sendHeartbeats()
		return
	}
	if r.ran {
		// An election we started produced no leader, whether it timed out
		// while we were candidate or a rival's higher Prepare cut it short.
		// If a round trip (with its writes) takes longer than the timeout,
		// retrying at the same pace gives up on every round before its
		// promises arrive, so slow down.
		r.backoff = min(2*r.backoff, r.t.ElectionBackoff)
	}
	r.startElection()
}

// Handle processes one incoming message.
func (r *Replica) Handle(m Message) {
	switch b := m.Body.(type) {
	case LogPrepare:
		r.onPrepare(m.From, b)
	case LogPromise:
		r.onPromise(m.From, b)
	case LogAccept:
		r.onAccept(m.From, b)
	case LogAccepted:
		r.onAccepted(m.From, b)
	case LogNack:
		r.onNack(b)
	case Heartbeat:
		r.onHeartbeat(m.From, b)
	case CatchupRequest:
		r.onCatchupRequest(m.From, b)
	case CatchupReply:
		for _, se := range b.Entries {
			r.markChosen(se.Slot, se.Entry)
		}
	case ClientRequest:
		r.onClientRequest(m.From, b)
	}
}

func (r *Replica) send(to NodeID, p Payload) { r.tr.Send(Message{From: r.id, To: to, Body: p}) }

func (r *Replica) broadcast(p Payload) {
	for _, to := range r.peers {
		r.send(to, p)
	}
}

// resetElectionTimer draws the election timeout from [ElectionMin,
// ElectionMax] scaled by the current backoff.
func (r *Replica) resetElectionTimer() {
	r.timer = r.backoff * r.t.ElectionMin
	r.timer += r.rng.IntN(r.backoff*(r.t.ElectionMax-r.t.ElectionMin) + 1)
}

// following notes that the leader at ballot b (possibly us) is up and
// reachable. Once the same leader has held for a whole backed-off election
// timeout, rival elections started before it won are over and the election
// backoff ends, so a later failover is fast again. Resetting on the first
// heartbeat instead would let a leader deposed by those rivals send
// everyone back to colliding at the short timeout.
func (r *Replica) following(b Ballot) {
	switch {
	case b != r.steadyB:
		r.steadyB, r.since = b, r.ticks
	case r.ticks-r.since >= r.backoff*r.t.ElectionMax:
		r.backoff, r.ran = 1, false
	}
}

func (r *Replica) observe(b Ballot) {
	if b.Node != r.id && b.Round > r.maxSeen {
		r.maxSeen = b.Round
	}
}

// savePromised persists a promise for b if it raises the current one.
func (r *Replica) savePromised(b Ballot) bool {
	if !r.promised.Less(b) {
		return true
	}
	if err := r.store.SavePromised(b); err != nil {
		return false
	}
	r.promised = b
	return true
}

// yield steps down if someone else holds a higher ballot than ours.
func (r *Replica) yield(b Ballot) {
	if r.role != follower && r.ballot.Less(b) {
		if r.role == leader {
			r.obs.SteppedDown(b)
		}
		r.role = follower
		r.leader = 0
		r.promises, r.proposals, r.votes = nil, nil, nil
	}
}

// ---- Election ----

func (r *Replica) startElection() {
	rnd := max(r.lastRound, r.maxSeen) + 1
	r.ran = true
	r.resetElectionTimer()
	if err := r.store.SaveRound(rnd); err != nil {
		return
	}
	r.lastRound = rnd
	r.ballot = Ballot{Round: rnd, Node: r.id}
	r.leader = 0
	if r.skipPrepare {
		r.becomeLeader()
		return
	}
	r.role = candidate
	r.promises = make(map[NodeID]LogPromise, len(r.peers))
	r.broadcast(LogPrepare{Ballot: r.ballot, Commit: r.commit})
}

func (r *Replica) onPrepare(from NodeID, m LogPrepare) {
	r.observe(m.Ballot)
	if m.Ballot.Less(r.promised) {
		r.send(from, LogNack{Ballot: m.Ballot, Promised: r.promised})
		return
	}
	var es []SlotEntry
	for _, s := range slices.Sorted(maps.Keys(r.accepted)) {
		if s > m.Commit {
			es = append(es, r.accepted[s])
		}
	}
	reply := LogPromise{Ballot: m.Ballot, Entries: es}
	if r.replyFirst {
		r.send(from, reply) // broken on purpose: promise leaves before it is durable
	}
	if !r.savePromised(m.Ballot) {
		return
	}
	if from != r.id {
		r.yield(m.Ballot)
		r.resetElectionTimer()
	}
	if !r.replyFirst {
		r.send(from, reply)
	}
}

func (r *Replica) onPromise(from NodeID, m LogPromise) {
	if r.role != candidate || m.Ballot != r.ballot {
		return
	}
	r.promises[from] = m
	if len(r.promises) >= r.quorum {
		r.becomeLeader()
	}
}

// becomeLeader re-proposes, at our ballot, every slot above the commit
// index that any promiser reported, using the highest-ballot value per
// slot, and fills slots nobody reported with no-ops.
func (r *Replica) becomeLeader() {
	best := make(map[uint64]SlotEntry)
	contested := make(map[uint64]bool)
	if !r.ignorePromised {
		for _, id := range r.peers {
			for _, se := range r.promises[id].Entries {
				cur, ok := best[se.Slot]
				if ok && cur.Entry != se.Entry {
					contested[se.Slot] = true
				}
				if !ok || cur.Ballot.Less(se.Ballot) {
					best[se.Slot] = se
				}
			}
		}
	}
	top := r.commit
	for s := range best {
		top = max(top, s)
	}
	for s := range r.chosen {
		top = max(top, s)
	}
	r.role = leader
	r.leader = r.id
	r.following(r.ballot)
	r.obs.BecameLeader(r.ballot)
	r.promises = nil
	r.proposals = make(map[uint64]Entry)
	r.votes = make(map[uint64]map[NodeID]struct{})
	for s := r.commit + 1; s <= top; s++ {
		e, ok := r.chosen[s]
		if !ok {
			if se, found := best[s]; found {
				e = se.Entry
				r.statRecovered++
				if contested[s] {
					r.statContested++
				}
			} else {
				e = Entry{Noop: true}
				r.statNoops++
			}
		}
		r.propose(s, e)
	}
	r.nextSlot = top + 1
	r.sendHeartbeats()
}

// ---- Replication ----

func (r *Replica) propose(slot uint64, e Entry) {
	r.proposals[slot] = e
	r.votes[slot] = make(map[NodeID]struct{})
	r.obs.Proposed(slot, e)
	r.broadcast(LogAccept{Ballot: r.ballot, Slot: slot, Entry: e})
}

func (r *Replica) onClientRequest(from NodeID, m ClientRequest) {
	if r.role != leader {
		r.send(from, ClientReply{ClientID: m.ClientID, Seq: m.Seq, Leader: r.leader})
		return
	}
	r.pending[m.ClientID] = pendingReq{seq: m.Seq, from: from}
	slot := r.nextSlot
	r.nextSlot++
	r.propose(slot, Entry{ClientID: m.ClientID, Seq: m.Seq, Cmd: m.Cmd})
}

func (r *Replica) onAccept(from NodeID, m LogAccept) {
	r.observe(m.Ballot)
	if m.Ballot.Less(r.promised) {
		r.send(from, LogNack{Ballot: m.Ballot, Promised: r.promised})
		return
	}
	if r.replyFirst {
		r.send(from, LogAccepted(m)) // broken on purpose: vote leaves before it is durable
	}
	// Raising the promise and recording the entry are one durable write, so
	// a crash leaves either neither or both.
	promised := r.promised
	if promised.Less(m.Ballot) {
		promised = m.Ballot
	}
	se := SlotEntry{Slot: m.Slot, Ballot: m.Ballot, Entry: m.Entry}
	if promised != r.promised || r.accepted[m.Slot] != se {
		if err := r.store.SaveAccept(promised, se); err != nil {
			return
		}
		r.promised = promised
		r.accepted[m.Slot] = se
	}
	if from != r.id {
		r.yield(m.Ballot)
		r.leader = from
		r.following(m.Ballot)
		r.resetElectionTimer()
	}
	if !r.replyFirst {
		r.send(from, LogAccepted(m))
	}
}

func (r *Replica) onAccepted(from NodeID, m LogAccepted) {
	if r.role != leader || m.Ballot != r.ballot {
		return
	}
	v, ok := r.votes[m.Slot]
	if !ok {
		return
	}
	v[from] = struct{}{}
	if len(v) >= r.quorum {
		e := r.proposals[m.Slot]
		delete(r.votes, m.Slot)
		delete(r.proposals, m.Slot)
		r.markChosen(m.Slot, e)
	}
}

func (r *Replica) onNack(m LogNack) {
	r.observe(m.Promised)
	if r.role != follower && m.Ballot == r.ballot && r.ballot.Less(m.Promised) {
		r.yield(m.Promised)
		r.resetElectionTimer()
	}
}

// sendHeartbeats also retransmits Accepts for slots not yet chosen, so a
// dropped Accept or Accepted only delays progress.
func (r *Replica) sendHeartbeats() {
	r.timer = r.t.HeartbeatEvery
	r.following(r.ballot)
	for _, to := range r.peers {
		if to != r.id {
			r.send(to, Heartbeat{Ballot: r.ballot, Commit: r.commit})
		}
	}
	for s := r.commit + 1; s < r.nextSlot; s++ {
		if e, ok := r.proposals[s]; ok {
			r.broadcast(LogAccept{Ballot: r.ballot, Slot: s, Entry: e})
		}
	}
}

func (r *Replica) onHeartbeat(from NodeID, m Heartbeat) {
	r.observe(m.Ballot)
	if m.Ballot.Less(r.promised) {
		r.send(from, LogNack{Ballot: m.Ballot, Promised: r.promised})
		return
	}
	if !r.savePromised(m.Ballot) {
		return
	}
	r.yield(m.Ballot)
	r.leader = from
	r.leaderCommit = max(r.leaderCommit, m.Commit)
	r.following(m.Ballot)
	r.resetElectionTimer()
	// An entry we accepted in the leader's own ballot at a slot the leader
	// reports committed is the chosen one: a leader proposes only one entry
	// per slot per ballot.
	for s := r.commit + 1; s <= m.Commit; s++ {
		if se, ok := r.accepted[s]; ok && se.Ballot == m.Ballot {
			r.markChosen(s, se.Entry)
		}
	}
	if r.commit < m.Commit {
		r.send(from, CatchupRequest{From: r.commit + 1})
	}
}

// ---- Commit, catch-up and apply ----

func (r *Replica) onCatchupRequest(from NodeID, m CatchupRequest) {
	var es []SlotEntry
	for s := max(m.From, 1); s <= r.commit && len(es) < r.t.CatchupBatch; s++ {
		es = append(es, SlotEntry{Slot: s, Entry: r.chosen[s]})
	}
	if len(es) > 0 {
		r.send(from, CatchupReply{Entries: es})
	}
}

func (r *Replica) markChosen(slot uint64, e Entry) {
	if slot <= r.commit {
		return
	}
	if _, ok := r.chosen[slot]; ok {
		return
	}
	r.chosen[slot] = e
	// Make the newly contiguous prefix durable, with the new commit index, in
	// one atomic write before applying anything. If the write fails the
	// commit index stays put and the next markChosen retries.
	var batch []SlotEntry
	for s := r.commit + 1; ; s++ {
		ce, ok := r.chosen[s]
		if !ok {
			break
		}
		batch = append(batch, SlotEntry{Slot: s, Entry: ce})
	}
	if len(batch) == 0 {
		return
	}
	if err := r.store.AppendCommitted(batch); err != nil {
		return
	}
	r.commit = batch[len(batch)-1].Slot
	for r.applied < r.commit {
		r.applied++
		e := r.chosen[r.applied]
		res := r.sm.Apply(r.applied, e)
		r.obs.Applied(r.applied, e)
		if p, ok := r.pending[e.ClientID]; ok && !e.Noop && p.seq == e.Seq {
			delete(r.pending, e.ClientID)
			r.send(p.from, ClientReply{ClientID: e.ClientID, Seq: e.Seq, OK: true, Result: res, Leader: r.leader})
		}
	}
}

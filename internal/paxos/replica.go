package paxos

import (
	"errors"
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

	// Acceptor state, persisted before any reply.
	promised  Ballot
	accepted  map[uint64]SlotEntry
	lastRound uint64

	maxSeen uint64
	role    role
	ballot  Ballot // our ballot while candidate or leader
	leader  NodeID // leader we believe in, 0 if unknown
	timer   int    // election timer, or heartbeat timer while leader

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

	// Test-only switches, set from export_test.go.
	skipPrepare    bool
	ignorePromised bool

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
	peers := slices.Clone(cfg.Peers)
	slices.Sort(peers)
	r := &Replica{
		id: cfg.ID, peers: peers, quorum: len(peers)/2 + 1,
		store: store, tr: tr, rng: cfg.Rand, t: cfg.Timing, sm: cfg.StateMachine,
		promised: promised, accepted: make(map[uint64]SlotEntry, len(acc)), lastRound: rnd,
		chosen: make(map[uint64]Entry), pending: make(map[uint64]pendingReq),
	}
	for _, se := range acc {
		r.accepted[se.Slot] = se
	}
	r.resetElectionTimer()
	return r, nil
}

// ID returns the replica's ID.
func (r *Replica) ID() NodeID { return r.id }

// IsLeader reports whether this replica currently acts as leader.
func (r *Replica) IsLeader() bool { return r.role == leader }

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
	r.timer--
	if r.timer > 0 {
		return
	}
	if r.role == leader {
		r.sendHeartbeats()
		return
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

func (r *Replica) resetElectionTimer() {
	r.timer = r.t.ElectionMin + r.rng.IntN(r.t.ElectionMax-r.t.ElectionMin+1)
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
		r.role = follower
		r.leader = 0
		r.promises, r.proposals, r.votes = nil, nil, nil
	}
}

// ---- Election ----

func (r *Replica) startElection() {
	rnd := max(r.lastRound, r.maxSeen) + 1
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
	if !r.savePromised(m.Ballot) {
		return
	}
	if from != r.id {
		r.yield(m.Ballot)
		r.resetElectionTimer()
	}
	var es []SlotEntry
	for _, s := range slices.Sorted(maps.Keys(r.accepted)) {
		if s > m.Commit {
			es = append(es, r.accepted[s])
		}
	}
	r.send(from, LogPromise{Ballot: m.Ballot, Entries: es})
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
	if !r.savePromised(m.Ballot) {
		return
	}
	if from != r.id {
		r.yield(m.Ballot)
		r.leader = from
		r.resetElectionTimer()
	}
	se := SlotEntry{Slot: m.Slot, Ballot: m.Ballot, Entry: m.Entry}
	if r.accepted[m.Slot] != se {
		if err := r.store.SaveAccepted(se); err != nil {
			return
		}
		r.accepted[m.Slot] = se
	}
	r.send(from, LogAccepted(m))
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
	for {
		if _, ok := r.chosen[r.commit+1]; !ok {
			break
		}
		r.commit++
	}
	for r.applied < r.commit {
		r.applied++
		e := r.chosen[r.applied]
		res := r.sm.Apply(r.applied, e)
		if p, ok := r.pending[e.ClientID]; ok && !e.Noop && p.seq == e.Seq {
			delete(r.pending, e.ClientID)
			r.send(p.from, ClientReply{ClientID: e.ClientID, Seq: e.Seq, OK: true, Result: res, Leader: r.leader})
		}
	}
}

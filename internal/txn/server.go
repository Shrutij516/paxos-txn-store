package txn

import (
	"slices"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
)

// Config describes one shard server. Timeouts are in logical ticks.
type Config struct {
	Shard      ShardID
	ShardNodes func(ShardID) []paxos.NodeID // replicas of each shard

	QueryAfter   int // a prepared txn queries its coordinator after this long
	CoordTimeout int // a coordinator aborts a txn not fully prepared by then
	Resend       int // interval for resending prepares, queries, wounds
	LockLease    int // an idle shared lock of an unprepared txn expires

	// Broken modes for the negative tests. Never set them otherwise.
	CommitWithoutAllVotes bool // coordinator commits on the first vote
	ReleaseLocksAtPrepare bool // participant drops its locks once prepared (pair with SM.noLockCheck)
	VolatilePrepare       bool // prepare kept only in leader memory
	NoWoundWait           bool // conflicting requests always wait
	ReplyAbortUnlogged    bool // a failed one-phase commit is answered "aborted" without logging it
}

// Defaults for zero Config fields.
const (
	DefaultQueryAfter   = 60
	DefaultCoordTimeout = 300
	DefaultResend       = 20
	DefaultLockLease    = 200
)

const (
	reqRead = iota
	reqPrepare
	reqOnePhase
)

type lockReq struct {
	kind int
	meta Meta
	key  string       // reqRead
	from paxos.NodeID // client, for reqRead and reqOnePhase
	prep PrepareReq   // reqPrepare and reqOnePhase
}

type holder struct {
	meta      Meta
	woundable bool // only a shared lock of an unprepared txn
	coord     ShardID
	hasCoord  bool // multi-shard txn whose coordinator can abort it
}

type coordState struct {
	meta     Meta
	parts    []Part
	votes    map[ShardID]bool
	started  int
	lastSend int
}

type deciding struct {
	at     int
	commit bool
	parts  []ShardID
}

// Server is the transaction layer of one replica. It only acts while its
// replica leads the shard. Everything it knows that must survive a leader
// change (prepared records, decisions, outcomes, data) lives in the
// replicated SM; its own state is volatile and is dropped whenever
// leadership changes: shared locks, lock waiters, in-flight coordination.
type Server struct {
	cfg  Config
	id   paxos.NodeID
	rep  *paxos.Replica
	sm   *SM
	send func(ShardID, paxos.Message)
	now  int

	leading   bool
	ballot    paxos.Ballot
	proposals []Record

	shared     map[string]map[ID]Meta
	active     map[ID]int // last activity, for the shared lock lease
	pendingX   map[string]ID
	pending    map[ID]*PrepareReq
	volatile   map[ID]*Record // VolatilePrepare mode only
	wounded    map[ID]bool
	waiting    []*lockReq
	coord      map[ID]*coordState
	deciding   map[ID]*deciding
	clients    map[ID]paxos.NodeID
	seen       map[ID]int
	lastQuery  map[ID]int
	outcomeAt  map[ID]int
	woundSent  map[ID]int
	leaderTerm int // number of times this server became leader

	// Counters read by tests to show which paths a schedule exercised.
	stats Stats

	// OnDecide, if set, runs when a decision is applied at the leader, just
	// before it notifies participants. Tests use it to crash the node at
	// that exact point.
	OnDecide func(ID)
	// OnPrepared, if set, runs when a prepare record is applied at the
	// leader, just before it votes.
	OnPrepared func(ID)
}

// Stats counts protocol events at one server.
type Stats struct {
	Wounds          int // shared locks of younger txns broken by older ones
	WoundReqs       int // wound requests sent to a holder's coordinator
	Queries         int // outcome queries sent by prepared participants
	PresumedAborts  int // abort decisions taken because nobody was coordinating
	RebuiltPrepared int // prepared txns found in the log on becoming leader
}

// NewServer wires a server to its replica and state machine. send delivers
// a message to node m.To's replica of shard sh; for a reply to a client sh
// is this server's own shard.
func NewServer(cfg Config, id paxos.NodeID, rep *paxos.Replica, sm *SM, send func(sh ShardID, m paxos.Message)) *Server {
	if cfg.QueryAfter <= 0 {
		cfg.QueryAfter = DefaultQueryAfter
	}
	if cfg.CoordTimeout <= 0 {
		cfg.CoordTimeout = DefaultCoordTimeout
	}
	if cfg.Resend <= 0 {
		cfg.Resend = DefaultResend
	}
	if cfg.LockLease <= 0 {
		cfg.LockLease = DefaultLockLease
	}
	s := &Server{cfg: cfg, id: id, rep: rep, sm: sm, send: send}
	s.reset()
	return s
}

func (s *Server) reset() {
	s.proposals = nil
	s.shared = make(map[string]map[ID]Meta)
	s.active = make(map[ID]int)
	s.pendingX = make(map[string]ID)
	s.pending = make(map[ID]*PrepareReq)
	s.volatile = make(map[ID]*Record)
	s.wounded = make(map[ID]bool)
	s.waiting = nil
	s.coord = make(map[ID]*coordState)
	s.deciding = make(map[ID]*deciding)
	s.clients = make(map[ID]paxos.NodeID)
	s.seen = make(map[ID]int)
	s.lastQuery = make(map[ID]int)
	s.outcomeAt = make(map[ID]int)
	s.woundSent = make(map[ID]int)
}

// Leading reports whether this server is acting as its shard's leader.
func (s *Server) Leading() bool { return s.leading }

// Touch is a client's write to a key of this shard, before commit. The
// value itself travels in the commit request; Touch only renews the lease
// on the transaction's shared locks and reports whether it can still
// commit here (this server leads the shard, the transaction was not
// wounded and has no outcome yet). Write locks are taken at prepare.
func (s *Server) Touch(id ID) bool {
	if !s.leading || s.wounded[id] {
		return false
	}
	if _, done := s.sm.Outcome(id); done {
		return false
	}
	s.active[id] = s.now
	return true
}

// ExclusiveHolder returns the transaction holding the exclusive lock on key
// at this leader (a prepared txn from the SM, or one being prepared).
func (s *Server) ExclusiveHolder(key string) (ID, bool) {
	for _, id := range s.sm.PreparedTxns() {
		if _, ok := s.sm.prepared[id].Writes[key]; ok {
			return id, true
		}
	}
	id, ok := s.pendingX[key]
	return id, ok
}

// toNode replies to a client.
func (s *Server) toNode(to paxos.NodeID, body any) {
	s.send(s.cfg.Shard, paxos.Message{From: s.id, To: to, Body: paxos.Ext{Body: body}})
}

func (s *Server) toShard(sh ShardID, body any) {
	for _, n := range s.cfg.ShardNodes(sh) {
		s.send(sh, paxos.Message{From: s.id, To: n, Body: paxos.Ext{Body: body}})
	}
}

func (s *Server) propose(r Record) { s.proposals = append(s.proposals, r) }

// After must be called after every replica or server call on this node. It
// tracks leadership, reacts to newly applied records, retries lock waiters
// and submits queued proposals to the replica.
func (s *Server) After() {
	leading, ballot := s.rep.IsLeader(), s.rep.Ballot()
	if leading != s.leading || (leading && ballot != s.ballot) {
		// New term or lost leadership: every volatile promise is void.
		s.reset()
		s.leading, s.ballot = leading, ballot
		if leading {
			s.leaderTerm++
			s.stats.RebuiltPrepared += len(s.sm.PreparedTxns())
		}
	}
	events := s.sm.drain()
	if !s.leading {
		return
	}
	for _, e := range events {
		s.onEvent(e)
	}
	s.tryLocks()
	props := s.proposals
	s.proposals = nil
	for _, r := range props {
		s.rep.Handle(paxos.Message{From: -s.id, To: s.id, Body: paxos.ClientRequest{Cmd: encode(r)}})
	}
}

// Tick advances the server's clock and runs its timers.
func (s *Server) Tick() {
	s.now++
	if !s.leading {
		return
	}
	// Coordinator: resend prepares, give up on slow participants.
	for _, id := range sortedTxns(s.coord) {
		cs := s.coord[id]
		if s.deciding[id] != nil {
			continue
		}
		if s.now-cs.started > s.cfg.CoordTimeout {
			s.decide(id, false, partShards(cs.parts))
			continue
		}
		if s.now-cs.lastSend >= s.cfg.Resend {
			cs.lastSend = s.now
			s.sendPrepares(cs)
		}
	}
	// Re-propose decisions that did not get applied.
	for _, id := range sortedTxns(s.deciding) {
		d := s.deciding[id]
		if _, known := s.sm.Decided(id); !known && s.now-d.at >= s.cfg.Resend {
			d.at = s.now
			s.propose(Record{Kind: KindDecide, Txn: Meta{ID: id}, Participants: d.parts, Commit: d.commit})
		}
	}
	// Participant: a prepared txn that hears nothing asks its coordinator.
	for _, id := range s.sm.PreparedTxns() {
		s.maybeQuery(id, s.sm.prepared[id])
	}
	for _, id := range sortedTxns(s.volatile) {
		s.maybeQuery(id, s.volatile[id])
	}
	for _, id := range sortedTxns(s.outcomeAt) {
		if s.now-s.outcomeAt[id] >= s.cfg.Resend {
			delete(s.outcomeAt, id) // allow a retry if the record was lost
		}
	}
	// Shared lock lease for transactions that went quiet.
	for _, id := range sortedTxns(s.active) {
		if s.now-s.active[id] > s.cfg.LockLease && s.pending[id] == nil && s.sm.prepared[id] == nil && s.volatile[id] == nil {
			s.dropShared(id)
			delete(s.active, id)
		}
	}
}

func (s *Server) maybeQuery(id ID, r *Record) {
	if _, ok := s.seen[id]; !ok {
		s.seen[id] = s.now
	}
	if s.now-s.seen[id] >= s.cfg.QueryAfter && s.now-s.lastQuery[id] >= s.cfg.Resend {
		s.lastQuery[id] = s.now
		s.stats.Queries++
		s.toShard(r.Coord, QueryOutcome{Txn: id, From: s.cfg.Shard, Participants: r.Participants})
	}
}

// Handle processes a transaction-layer message. Only the leader acts;
// requests are broadcast to every replica of a shard.
func (s *Server) Handle(m paxos.Message) {
	ext, ok := m.Body.(paxos.Ext)
	if !ok || !s.leading {
		return
	}
	switch b := ext.Body.(type) {
	case ReadReq:
		s.onRead(b)
	case AbortReq:
		s.onAbort(b)
	case CommitReq:
		s.onCommit(b)
	case PrepareReq:
		s.onPrepare(b)
	case Vote:
		s.onVote(b)
	case Decision:
		s.onDecision(b)
	case QueryOutcome:
		s.onQuery(b)
	case WoundReq:
		s.onWound(b)
	}
}

// ---- Locks and wound-wait ----

func (s *Server) holdsShared(key string, id ID) bool {
	_, ok := s.shared[key][id]
	return ok
}

func (s *Server) dropShared(id ID) {
	for _, k := range sortedKeys(s.shared) {
		delete(s.shared[k], id)
		if len(s.shared[k]) == 0 {
			delete(s.shared, k)
		}
	}
}

func (s *Server) releasePending(id ID) {
	if p := s.pending[id]; p != nil {
		for k := range p.Part.Writes {
			if s.pendingX[k] == id {
				delete(s.pendingX, k)
			}
		}
		delete(s.pending, id)
	}
}

// exclusiveHolders lists transactions holding an exclusive lock on key,
// other than self.
func (s *Server) exclusiveHolders(key string, self ID, out []holder) []holder {
	if !s.cfg.ReleaseLocksAtPrepare {
		for _, id := range s.sm.PreparedTxns() {
			r := s.sm.prepared[id]
			if _, ok := r.Writes[key]; ok && id != self {
				out = append(out, holder{meta: r.Txn, coord: r.Coord, hasCoord: true})
			}
		}
	}
	if id, ok := s.pendingX[key]; ok && id != self {
		p := s.pending[id]
		out = append(out, holder{meta: p.Txn, coord: p.Coord, hasCoord: len(p.Participants) > 1})
	}
	for _, id := range sortedTxns(s.volatile) {
		r := s.volatile[id]
		if _, ok := r.Writes[key]; ok && id != self {
			out = append(out, holder{meta: r.Txn, coord: r.Coord, hasCoord: true})
		}
	}
	return out
}

func (s *Server) holders(q *lockReq) []holder {
	self := q.meta.ID
	var out []holder
	if q.kind == reqRead {
		return s.exclusiveHolders(q.key, self, out)
	}
	for _, k := range sortedKeys(q.prep.Part.Writes) {
		for _, id := range sortedTxns(s.shared[k]) {
			if id != self {
				out = append(out, holder{meta: s.shared[k][id], woundable: true})
			}
		}
		if !s.cfg.ReleaseLocksAtPrepare {
			for _, id := range s.sm.PreparedTxns() {
				r := s.sm.prepared[id]
				if _, ok := r.Reads[k]; ok && id != self {
					out = append(out, holder{meta: r.Txn, coord: r.Coord, hasCoord: true})
				}
			}
		}
		out = s.exclusiveHolders(k, self, out)
	}
	for _, k := range sortedKeys(q.prep.Part.Reads) {
		out = s.exclusiveHolders(k, self, out)
	}
	return out
}

// tryLocks grants waiting requests in age order. Under wound-wait an older
// requester wounds younger holders (a shared-lock holder is aborted on the
// spot; a prepared holder is aborted through its coordinator if undecided)
// and a younger requester waits.
func (s *Server) tryLocks() {
	slices.SortStableFunc(s.waiting, func(a, b *lockReq) int {
		switch {
		case a.meta.Older(b.meta):
			return -1
		case b.meta.Older(a.meta):
			return 1
		}
		return 0
	})
	reqs := s.waiting
	s.waiting = nil
	for _, q := range reqs {
		if s.wounded[q.meta.ID] {
			s.fail(q)
			continue
		}
		hs := s.holders(q)
		if len(hs) > 0 && !s.cfg.NoWoundWait {
			for _, h := range hs {
				if !q.meta.Older(h.meta) {
					continue
				}
				if h.woundable {
					s.wound(h.meta.ID)
				} else if h.hasCoord {
					s.sendWound(h)
				}
			}
			hs = s.holders(q)
		}
		if len(hs) == 0 {
			s.grant(q)
		} else {
			s.waiting = append(s.waiting, q)
		}
	}
}

func (s *Server) wound(id ID) {
	if !s.wounded[id] {
		s.stats.Wounds++
	}
	s.wounded[id] = true
	s.dropShared(id)
}

func (s *Server) sendWound(h holder) {
	if t, ok := s.woundSent[h.meta.ID]; ok && s.now-t < s.cfg.Resend {
		return
	}
	s.woundSent[h.meta.ID] = s.now
	s.stats.WoundReqs++
	s.toShard(h.coord, WoundReq{Txn: h.meta.ID})
}

func (s *Server) enqueue(q *lockReq) {
	for _, w := range s.waiting {
		if w.meta.ID == q.meta.ID && w.kind == q.kind && w.key == q.key {
			w.from = q.from
			return
		}
	}
	s.waiting = append(s.waiting, q)
}

func (s *Server) grant(q *lockReq) {
	id := q.meta.ID
	s.active[id] = s.now
	if q.kind == reqRead {
		if s.shared[q.key] == nil {
			s.shared[q.key] = make(map[ID]Meta)
		}
		s.shared[q.key][id] = q.meta
		v := s.sm.Get(q.key)
		s.toNode(q.from, ReadResp{Txn: id, Key: q.key, Value: v.Value, Version: v.Ver})
		return
	}
	// Strict 2PL: every read must still be covered by this leader's shared
	// lock. A lock lost to a wound, a lease expiry or a leader change means
	// the read may be stale, so the txn must abort.
	for _, k := range sortedKeys(q.prep.Part.Reads) {
		if !s.holdsShared(k, id) {
			s.fail(q)
			return
		}
	}
	p := q.prep
	rec := Record{Txn: p.Txn, Coord: p.Coord, Participants: p.Participants, Reads: p.Part.Reads, Writes: p.Part.Writes}
	if q.kind == reqOnePhase {
		rec.Kind = KindOnePhase
		s.clients[id] = q.from
	} else {
		rec.Kind = KindPrepare
		if s.cfg.VolatilePrepare {
			// Broken on purpose: prepared only in this leader's memory.
			for _, k := range sortedKeys(rec.Reads) {
				if s.sm.Get(k).Ver != rec.Reads[k] {
					s.vote(p.Coord, id, false)
					return
				}
			}
			s.volatile[id] = &rec
			s.vote(p.Coord, id, true)
			return
		}
	}
	for k := range p.Part.Writes {
		s.pendingX[k] = id
	}
	s.pending[id] = &p
	s.propose(rec)
}

func (s *Server) fail(q *lockReq) {
	id := q.meta.ID
	s.wound(id)
	switch q.kind {
	case reqRead:
		s.toNode(q.from, ReadResp{Txn: id, Key: q.key, Aborted: true})
	case reqPrepare:
		s.vote(q.prep.Coord, id, false)
	case reqOnePhase:
		// The abort goes through the log before the client hears it. An
		// earlier leader may have logged this txn's one-phase record
		// without anyone learning it yet (the client then retries here);
		// whichever record comes first in the log decides, and the client
		// is told that outcome.
		if s.cfg.ReplyAbortUnlogged {
			s.toNode(q.from, CommitResp{Txn: id, Committed: false}) // broken on purpose
			return
		}
		s.clients[id] = q.from
		s.propose(Record{Kind: KindAbort, Txn: q.meta})
	}
}

func (s *Server) vote(coord ShardID, id ID, yes bool) {
	s.toShard(coord, Vote{Txn: id, Shard: s.cfg.Shard, Yes: yes})
}

// ---- Message handlers ----

func (s *Server) onRead(r ReadReq) {
	id := r.Txn.ID
	if _, done := s.sm.Outcome(id); done || s.wounded[id] {
		s.toNode(r.Client, ReadResp{Txn: id, Key: r.Key, Aborted: true})
		return
	}
	s.active[id] = s.now
	if s.holdsShared(r.Key, id) {
		v := s.sm.Get(r.Key)
		s.toNode(r.Client, ReadResp{Txn: id, Key: r.Key, Value: v.Value, Version: v.Ver})
		return
	}
	s.enqueue(&lockReq{kind: reqRead, meta: r.Txn, key: r.Key, from: r.Client})
}

func (s *Server) onAbort(a AbortReq) {
	s.wounded[a.Txn] = true
	if s.pending[a.Txn] == nil && s.sm.prepared[a.Txn] == nil && s.volatile[a.Txn] == nil {
		s.dropShared(a.Txn)
	}
}

func (s *Server) inFlight(id ID) bool {
	if s.pending[id] != nil {
		return true
	}
	for _, w := range s.waiting {
		if w.meta.ID == id && w.kind != reqRead {
			return true
		}
	}
	return false
}

func (s *Server) onCommit(c CommitReq) {
	id := c.Txn.ID
	s.clients[id] = c.Client
	if len(c.Parts) == 1 {
		if commit, known := s.sm.Outcome(id); known {
			s.toNode(c.Client, CommitResp{Txn: id, Committed: commit})
			return
		}
		if s.inFlight(id) {
			return
		}
		s.enqueue(&lockReq{kind: reqOnePhase, meta: c.Txn, from: c.Client,
			prep: PrepareReq{Txn: c.Txn, Coord: s.cfg.Shard, Participants: []ShardID{s.cfg.Shard}, Part: c.Parts[0]}})
		return
	}
	if commit, known := s.sm.Decided(id); known {
		s.toNode(c.Client, CommitResp{Txn: id, Committed: commit})
		return
	}
	if s.coord[id] != nil || s.deciding[id] != nil {
		return
	}
	cs := &coordState{meta: c.Txn, parts: c.Parts, votes: make(map[ShardID]bool), started: s.now, lastSend: s.now}
	s.coord[id] = cs
	s.sendPrepares(cs)
}

func partShards(parts []Part) []ShardID {
	out := make([]ShardID, len(parts))
	for i, p := range parts {
		out[i] = p.Shard
	}
	return out
}

func (s *Server) sendPrepares(cs *coordState) {
	shards := partShards(cs.parts)
	for _, p := range cs.parts {
		if cs.votes[p.Shard] {
			continue
		}
		req := PrepareReq{Txn: cs.meta, Coord: s.cfg.Shard, Participants: shards, Part: p}
		if p.Shard == s.cfg.Shard {
			s.onPrepare(req) // the coordinator is also a participant
		} else {
			s.toShard(p.Shard, req)
		}
	}
}

func (s *Server) onPrepare(p PrepareReq) {
	id := p.Txn.ID
	if commit, known := s.sm.Outcome(id); known {
		s.vote(p.Coord, id, commit)
		return
	}
	if s.sm.prepared[id] != nil || s.volatile[id] != nil {
		s.vote(p.Coord, id, true)
		return
	}
	if s.sm.rejected[id] || s.wounded[id] {
		s.vote(p.Coord, id, false)
		return
	}
	if s.inFlight(id) {
		return
	}
	s.active[id] = s.now
	s.enqueue(&lockReq{kind: reqPrepare, meta: p.Txn, prep: p})
}

func (s *Server) onVote(v Vote) {
	cs := s.coord[v.Txn]
	if cs == nil || s.deciding[v.Txn] != nil {
		return
	}
	if s.cfg.CommitWithoutAllVotes {
		s.decide(v.Txn, true, partShards(cs.parts)) // broken on purpose
		return
	}
	if !v.Yes {
		s.decide(v.Txn, false, partShards(cs.parts))
		return
	}
	cs.votes[v.Shard] = true
	for _, p := range cs.parts {
		if !cs.votes[p.Shard] {
			return
		}
	}
	s.decide(v.Txn, true, partShards(cs.parts))
}

// decide writes the coordinator's decision through this shard's log. The
// first decision applied wins; the outcome reported to anyone is always the
// one in the SM.
func (s *Server) decide(id ID, commit bool, parts []ShardID) {
	if s.deciding[id] != nil {
		return
	}
	s.deciding[id] = &deciding{at: s.now, commit: commit, parts: parts}
	r := Record{Kind: KindDecide, Txn: Meta{ID: id}, Participants: parts, Commit: commit}
	if v := s.volatile[id]; v != nil && commit {
		r.Writes = v.Writes // VolatilePrepare mode: the only copy of the writes
	}
	s.propose(r)
}

func (s *Server) onDecision(d Decision) {
	id := d.Txn
	if _, known := s.sm.Outcome(id); known {
		return
	}
	if _, ok := s.outcomeAt[id]; ok {
		return
	}
	s.outcomeAt[id] = s.now
	r := Record{Kind: KindAbort, Txn: Meta{ID: id}}
	if d.Commit {
		r.Kind = KindCommit
		if v := s.volatile[id]; v != nil {
			r.Writes = v.Writes // VolatilePrepare mode: the only copy of the writes
		}
	}
	s.propose(r)
}

func (s *Server) onQuery(q QueryOutcome) {
	if commit, known := s.sm.Decided(q.Txn); known {
		s.toShard(q.From, Decision{Txn: q.Txn, Commit: commit})
		return
	}
	if cs := s.coord[q.Txn]; cs != nil && s.now-cs.started < s.cfg.CoordTimeout {
		return // still collecting votes
	}
	// No decision and nobody coordinating it here: presume abort. If an
	// earlier leader's commit decision is still in the log, it is applied
	// first and wins.
	if s.deciding[q.Txn] == nil {
		s.stats.PresumedAborts++
	}
	s.decide(q.Txn, false, q.Participants)
}

func (s *Server) onWound(w WoundReq) {
	if _, known := s.sm.Decided(w.Txn); known {
		return
	}
	if cs := s.coord[w.Txn]; cs != nil {
		s.decide(w.Txn, false, partShards(cs.parts))
		return
	}
	if r := s.sm.prepared[w.Txn]; r != nil && r.Coord == s.cfg.Shard {
		s.decide(w.Txn, false, r.Participants)
	}
}

// ---- Applied records ----

func (s *Server) onEvent(e Event) {
	id := e.Rec.Txn.ID
	switch e.Kind {
	case KindPrepare:
		s.releasePending(id)
		if s.OnPrepared != nil {
			s.OnPrepared(id) // a crash here drops everything sent below
		}
		if s.cfg.ReleaseLocksAtPrepare {
			s.dropShared(id)
		}
		if !e.OK {
			s.wound(id)
		}
		s.vote(e.Rec.Coord, id, e.OK)
	case KindOnePhase:
		s.releasePending(id)
		s.dropShared(id)
		if c, ok := s.clients[id]; ok {
			s.toNode(c, CommitResp{Txn: id, Committed: e.OK})
		}
	case KindDecide:
		if s.OnDecide != nil {
			s.OnDecide(id) // a crash here drops the notifications below
		}
		delete(s.coord, id)
		delete(s.deciding, id)
		for _, sh := range e.Rec.Participants {
			if sh != s.cfg.Shard {
				s.toShard(sh, Decision{Txn: id, Commit: e.OK})
			}
		}
		if c, ok := s.clients[id]; ok {
			s.toNode(c, CommitResp{Txn: id, Committed: e.OK})
		}
		s.dropShared(id)
		delete(s.volatile, id)
	case KindCommit, KindAbort:
		delete(s.outcomeAt, id)
		delete(s.volatile, id)
		s.dropShared(id)
		if c, ok := s.clients[id]; ok {
			s.toNode(c, CommitResp{Txn: id, Committed: e.OK}) // a logged one-phase abort
		}
	}
}

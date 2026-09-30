package paxos

type proposerPhase int

const (
	phaseIdle proposerPhase = iota
	phaseBackoff
	phasePreparing
	phaseAccepting
	phaseDone
)

// Proposer drives a value through phase 1 (prepare/promise) and phase 2
// (accept/accepted). If a phase stalls or is rejected it waits a randomized,
// exponentially growing number of ticks and retries with a higher ballot.
// The randomness breaks the symmetry that would otherwise let two proposers
// preempt each other forever.
type Proposer struct {
	id     NodeID
	peers  []NodeID
	quorum int
	store  Storage
	tr     Transport
	rng    Rand
	cfg    Timing

	phase    proposerPhase
	own      Value // value the client asked us to propose
	ballot   Ballot
	lastRnd  uint64 // highest round persisted by this proposer
	maxSeen  uint64 // highest round observed from any other ballot
	promises map[NodeID]Promise
	proposal Value // value chosen for phase 2
	timer    int
	attempt  int
}

// Timing controls proposer timeouts and backoff, measured in ticks.
type Timing struct {
	PhaseTimeout int // ticks to wait for a quorum before giving up on a phase
	BackoffBase  int // minimum backoff before a retry
	BackoffMax   int // cap on the random part of the backoff window
}

// DefaultTiming suits the simulator's default delays.
var DefaultTiming = Timing{PhaseTimeout: 20, BackoffBase: 2, BackoffMax: 64}

func newProposer(id NodeID, peers []NodeID, store Storage, tr Transport, rng Rand, t Timing) (*Proposer, error) {
	rnd, err := store.LoadRound()
	if err != nil {
		return nil, err
	}
	return &Proposer{
		id: id, peers: peers, quorum: len(peers)/2 + 1,
		store: store, tr: tr, rng: rng, cfg: t, lastRnd: rnd,
	}, nil
}

func (p *Proposer) propose(v Value) error {
	if p.phase != phaseIdle {
		return ErrBusy
	}
	p.own = v
	p.startPrepare()
	return nil
}

func (p *Proposer) startPrepare() {
	rnd := max(p.lastRnd, p.maxSeen) + 1
	if err := p.store.SaveRound(rnd); err != nil {
		p.backoff()
		return
	}
	p.lastRnd = rnd
	p.ballot = Ballot{Round: rnd, Node: p.id}
	p.promises = make(map[NodeID]Promise, len(p.peers))
	p.phase = phasePreparing
	p.timer = p.cfg.PhaseTimeout
	for _, to := range p.peers {
		p.tr.Send(Message{From: p.id, To: to, Body: Prepare{Ballot: p.ballot}})
	}
}

func (p *Proposer) handlePromise(from NodeID, m Promise) {
	if p.phase != phasePreparing || m.Ballot != p.ballot {
		return
	}
	p.promises[from] = m
	if len(p.promises) < p.quorum {
		return
	}
	// Adopt the value of the highest-ballot proposal any quorum member has
	// accepted. Only if none has accepted anything are we free to use our
	// own value. Iterating peers in order keeps this deterministic.
	p.proposal = p.own
	var best Ballot
	for _, id := range p.peers {
		pr, ok := p.promises[id]
		if ok && best.Less(pr.Accepted) {
			best, p.proposal = pr.Accepted, pr.Value
		}
	}
	p.phase = phaseAccepting
	p.timer = p.cfg.PhaseTimeout
	for _, to := range p.peers {
		p.tr.Send(Message{From: p.id, To: to, Body: Accept{Ballot: p.ballot, Value: p.proposal}})
	}
}

func (p *Proposer) handleNack(m Nack) {
	p.observe(m.Promised)
	if m.Ballot != p.ballot || (p.phase != phasePreparing && p.phase != phaseAccepting) {
		return
	}
	p.backoff()
}

// observe remembers the highest round seen so the next ballot outranks it.
func (p *Proposer) observe(b Ballot) {
	if b.Node != p.id && b.Round > p.maxSeen {
		p.maxSeen = b.Round
	}
}

func (p *Proposer) backoff() {
	p.attempt++
	window := p.cfg.BackoffBase << min(p.attempt, 16)
	if window > p.cfg.BackoffMax || window <= 0 {
		window = p.cfg.BackoffMax
	}
	p.phase = phaseBackoff
	p.timer = p.cfg.BackoffBase + p.rng.IntN(window+1)
}

func (p *Proposer) tick() {
	if p.phase == phaseIdle || p.phase == phaseDone {
		return
	}
	p.timer--
	if p.timer > 0 {
		return
	}
	if p.phase == phaseBackoff {
		p.startPrepare()
		return
	}
	p.backoff()
}

func (p *Proposer) stop() { p.phase = phaseDone }

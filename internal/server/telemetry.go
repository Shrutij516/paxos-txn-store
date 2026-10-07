package server

import (
	"context"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
)

// The Paxos and transaction cores only report events (paxos.Observer,
// txn.Observer) and never read the clock. telemetry turns those events,
// on the shard's event loop, into metrics, spans and logs: it stamps them
// with wall-clock time here, in the server layer.

// tracerName names the tracer of the server's spans.
const tracerName = "github.com/Shrutij516/paxos-txn-store/internal/server"

// propagator carries trace context in peer envelopes (W3C trace context).
var propagator = propagation.TraceContext{}

// traceTTL bounds how long a shard remembers a transaction's trace context.
// A transaction lives far shorter; the bound only keeps the table small.
const traceTTL = 30 * time.Second

type traced struct {
	sc trace.SpanContext
	at time.Time
}

type phase struct {
	at     time.Time
	span   trace.Span
	parent context.Context
}

// telemetry is one shard replica's observer. All of it is owned by the
// shard's event loop: the cores call it synchronously from that loop.
type telemetry struct {
	s      *shard
	m      shardMetrics
	tracer trace.Tracer
	attrs  []attribute.KeyValue

	tctx     map[txn.ID]traced // trace context of each transaction seen here
	proposed map[uint64]phase  // slot -> proposal, at the leader
	coord    map[txn.ID]phase  // coordinator: collecting votes
	deciding map[txn.ID]phase  // coordinator: decision proposed
	prep     map[txn.ID]phase  // participant: prepare proposed
	query    map[txn.ID]phase  // participant: outcome queried
	pruned   time.Time
}

func newTelemetry(s *shard, m shardMetrics, tp trace.TracerProvider) *telemetry {
	return &telemetry{
		s: s, m: m, tracer: tp.Tracer(tracerName),
		attrs: []attribute.KeyValue{attribute.Int("shard", int(s.id)), attribute.Int("node", int(s.n.cfg.ID))},
		tctx:  map[txn.ID]traced{}, proposed: map[uint64]phase{},
		coord: map[txn.ID]phase{}, deciding: map[txn.ID]phase{}, prep: map[txn.ID]phase{}, query: map[txn.ID]phase{},
	}
}

// ---- Trace context ----

// remember records the trace context a transaction arrived with, unless
// one is known already.
func (t *telemetry) remember(id txn.ID, sc trace.SpanContext) {
	if !sc.IsValid() {
		return
	}
	if _, ok := t.tctx[id]; !ok {
		t.tctx[id] = traced{sc: sc, at: time.Now()}
	}
}

// parent returns a context carrying the transaction's current span context,
// or nil if none is known.
func (t *telemetry) parent(id txn.ID) context.Context {
	tc, ok := t.tctx[id]
	if !ok {
		return nil
	}
	return trace.ContextWithSpanContext(context.Background(), tc.sc)
}

// startSpan begins a span for a transaction as a child of its current
// span (see startFrom). It returns nil if the transaction has no trace
// context: background work starts no root spans.
func (t *telemetry) startSpan(id txn.ID, name string) (trace.Span, context.Context) {
	parent := t.parent(id)
	if parent == nil {
		return nil, nil
	}
	return t.startFrom(parent, id, name)
}

// startFrom begins a span for a transaction as a child of parent, and makes
// it the transaction's current span, so the messages and log rounds that
// follow become its children.
func (t *telemetry) startFrom(parent context.Context, id txn.ID, name string) (trace.Span, context.Context) {
	_, span := t.tracer.Start(parent, name, trace.WithAttributes(append(t.attrs, attribute.String("txn.id", strconv.FormatUint(uint64(id), 10)))...))
	t.tctx[id] = traced{sc: span.SpanContext(), at: time.Now()}
	return span, parent
}

func end(span trace.Span, attrs ...attribute.KeyValue) {
	if span != nil {
		span.SetAttributes(attrs...)
		span.End()
	}
}

// inject returns the trace context to send with a transaction message, or
// nil.
func (t *telemetry) inject(m paxos.Message) map[string]string {
	id, ok := msgTxn(m)
	if !ok {
		return nil
	}
	ctx := t.parent(id)
	if ctx == nil {
		return nil
	}
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	return carrier
}

// extract records the trace context a transaction message arrived with.
func (t *telemetry) extract(m paxos.Message, carrier map[string]string) {
	if len(carrier) == 0 {
		return
	}
	if id, ok := msgTxn(m); ok {
		t.remember(id, trace.SpanContextFromContext(propagator.Extract(context.Background(), propagation.MapCarrier(carrier))))
	}
}

// msgTxn returns the transaction a two-phase commit message is about.
func msgTxn(m paxos.Message) (txn.ID, bool) {
	ext, ok := m.Body.(paxos.Ext)
	if !ok {
		return 0, false
	}
	switch b := ext.Body.(type) {
	case txn.PrepareReq:
		return b.Txn.ID, true
	case txn.Vote:
		return b.Txn, true
	case txn.Decision:
		return b.Txn, true
	case txn.QueryOutcome:
		return b.Txn, true
	case txn.WoundReq:
		return b.Txn, true
	}
	return 0, false
}

// tick updates the gauges and forgets old trace contexts.
func (t *telemetry) tick() {
	t.m.applyLag.Set(float64(t.s.rep.Lag()))
	t.m.logLength.Set(float64(t.s.rep.Commit()))
	t.m.inDoubt.Set(float64(t.s.sm.NumPrepared()))
	if now := time.Now(); now.Sub(t.pruned) > time.Second {
		t.pruned = now
		for id, tc := range t.tctx {
			if now.Sub(tc.at) > traceTTL {
				delete(t.tctx, id)
			}
		}
	}
}

// leadership is called when the shard's leadership changes. Phases this
// replica was timing as leader end; the new term's server forgot them.
func (t *telemetry) leadership(leading bool) {
	if leading {
		t.m.isLeader.Set(1)
	} else {
		t.m.isLeader.Set(0)
	}
	for _, ps := range []map[txn.ID]phase{t.coord, t.deciding, t.prep, t.query} {
		for id, p := range ps {
			if p.span != nil {
				p.span.SetStatus(codes.Error, "leadership changed")
				p.span.End()
			}
			delete(ps, id)
		}
	}
	for slot, p := range t.proposed {
		if p.span != nil {
			p.span.SetStatus(codes.Error, "leadership changed")
			p.span.End()
		}
		delete(t.proposed, slot)
	}
}

// ---- paxos.Observer ----

func (t *telemetry) BecameLeader(b paxos.Ballot) {
	t.m.elections.Inc()
	t.s.log.Info("became leader", "ballot", b.String())
}

func (t *telemetry) SteppedDown(b paxos.Ballot) {
	t.s.log.Info("stepped down", "higher_ballot", b.String())
}

func (t *telemetry) Proposed(slot uint64, e paxos.Entry) {
	p := phase{at: time.Now()}
	if !e.Noop {
		if r, err := txn.DecodeRecord(e.Cmd); err == nil {
			if parent := t.parent(r.Txn.ID); parent != nil {
				_, p.span = t.tracer.Start(parent, "paxos.round", trace.WithAttributes(append(t.attrs,
					attribute.Int64("slot", int64(slot)), attribute.String("record", r.Kind))...))
			}
		}
	}
	t.proposed[slot] = p
}

func (t *telemetry) Applied(slot uint64, _ paxos.Entry) {
	if p, ok := t.proposed[slot]; ok {
		delete(t.proposed, slot)
		t.m.commitLatency.Observe(time.Since(p.at).Seconds())
		end(p.span)
	}
}

// ---- txn.Observer ----

func (t *telemetry) Finished(id txn.ID, commit, cross bool, reason txn.AbortReason) {
	switch {
	case commit:
		t.m.commits.Inc()
		if cross {
			t.m.crossCommits.Inc()
		}
	default:
		if c, ok := t.m.aborts[reason]; ok {
			c.Inc()
		}
	}
	if p, ok := t.deciding[id]; ok {
		delete(t.deciding, id)
		t.m.decide.Observe(time.Since(p.at).Seconds())
		end(p.span, attribute.Bool("commit", commit))
	}
}

func (t *telemetry) CoordinationStarted(id txn.ID) {
	span, parent := t.startSpan(id, "2pc.prepare")
	t.coord[id] = phase{at: time.Now(), span: span, parent: parent}
}

func (t *telemetry) DecisionProposed(id txn.ID, commit bool) {
	var parent context.Context
	if p, ok := t.coord[id]; ok {
		delete(t.coord, id)
		t.m.prepare.Observe(time.Since(p.at).Seconds())
		end(p.span, attribute.Bool("commit", commit))
		parent = p.parent // the decide span is the prepare span's sibling
	}
	var span trace.Span
	if parent != nil {
		span, parent = t.startFrom(parent, id, "2pc.decide")
	} else {
		span, parent = t.startSpan(id, "2pc.decide")
	}
	t.deciding[id] = phase{at: time.Now(), span: span, parent: parent}
}

func (t *telemetry) PrepareProposed(id txn.ID) {
	span, parent := t.startSpan(id, "2pc.participant.prepare")
	t.prep[id] = phase{at: time.Now(), span: span, parent: parent}
}

func (t *telemetry) PrepareApplied(id txn.ID, ok bool) {
	if p, found := t.prep[id]; found {
		delete(t.prep, id)
		t.m.partPrepare.Observe(time.Since(p.at).Seconds())
		end(p.span, attribute.Bool("vote", ok))
	}
}

func (t *telemetry) QuerySent(id txn.ID) {
	if _, ok := t.query[id]; ok {
		return // a repeated query: keep timing from the first
	}
	span, parent := t.startSpan(id, "2pc.outcome_query")
	t.query[id] = phase{at: time.Now(), span: span, parent: parent}
}

func (t *telemetry) Resolved(id txn.ID, commit bool) {
	if p, ok := t.query[id]; ok {
		delete(t.query, id)
		t.m.query.Observe(time.Since(p.at).Seconds())
		end(p.span, attribute.Bool("commit", commit))
	}
}

func (t *telemetry) Wounded(txn.ID) { t.m.wounds.Inc() }

func (t *telemetry) LockWait(txn.ID) { t.m.lockWaits.Inc() }

// ---- Storage timing ----

// timedLog measures every durable write of a shard's log storage. Each is
// one SQLite transaction ending in an fsync, which dominates its time.
type timedLog struct {
	paxos.LogStorage
	h interface{ Observe(float64) }
}

func (l timedLog) time(f func() error) error {
	start := time.Now()
	err := f()
	l.h.Observe(time.Since(start).Seconds())
	return err
}

func (l timedLog) SaveRound(r uint64) error {
	return l.time(func() error { return l.LogStorage.SaveRound(r) })
}

func (l timedLog) SavePromised(b paxos.Ballot) error {
	return l.time(func() error { return l.LogStorage.SavePromised(b) })
}

func (l timedLog) SaveAccept(p paxos.Ballot, e paxos.SlotEntry) error {
	return l.time(func() error { return l.LogStorage.SaveAccept(p, e) })
}

func (l timedLog) AppendCommitted(es []paxos.SlotEntry) error {
	return l.time(func() error { return l.LogStorage.AppendCommitted(es) })
}

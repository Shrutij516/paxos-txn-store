package server

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Shrutij516/paxos-txn-store/internal/paxos"
	"github.com/Shrutij516/paxos-txn-store/internal/txn"
)

// Every metric has exactly two labels, shard and node, both bounded by the
// cluster config. Nothing that grows with traffic (a transaction ID, a
// key, a client) is ever a label; an abort reason, a small fixed set, is
// part of the metric name instead (DECISIONS.md entry 29).
var labels = []string{"shard", "node"}

// latencyBuckets span 100µs to about 3.3s.
var latencyBuckets = prometheus.ExponentialBuckets(0.0001, 2, 16)

// slowBuckets span 1ms to about 33s, for waits measured in election and
// in-doubt timeouts.
var slowBuckets = prometheus.ExponentialBuckets(0.001, 2, 16)

// metrics holds every metric of a node, registered on its registry.
type metrics struct {
	elections     *prometheus.CounterVec
	isLeader      *prometheus.GaugeVec
	commitLatency *prometheus.HistogramVec
	applyLag      *prometheus.GaugeVec
	logLength     *prometheus.GaugeVec

	commits      *prometheus.CounterVec
	crossCommits *prometheus.CounterVec
	aborts       map[txn.AbortReason]*prometheus.CounterVec
	prepare      *prometheus.HistogramVec
	decide       *prometheus.HistogramVec
	partPrepare  *prometheus.HistogramVec
	query        *prometheus.HistogramVec
	inDoubt      *prometheus.GaugeVec
	wounds       *prometheus.CounterVec
	lockWaits    *prometheus.CounterVec

	fsync *prometheus.HistogramVec
}

// registerMetrics creates the node's metrics on reg. It fails if reg
// already holds them (two nodes cannot share a registry).
func registerMetrics(reg prometheus.Registerer) (m *metrics, err error) {
	register := func(c prometheus.Collector) {
		if e := reg.Register(c); e != nil && err == nil {
			err = e
		}
	}
	counter := func(name, help string) *prometheus.CounterVec {
		v := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
		register(v)
		return v
	}
	gauge := func(name, help string) *prometheus.GaugeVec {
		v := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
		register(v)
		return v
	}
	histogram := func(name, help string, buckets []float64) *prometheus.HistogramVec {
		v := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
		register(v)
		return v
	}
	m = &metrics{
		elections:     counter("paxos_leader_elections_total", "Elections this replica won."),
		isLeader:      gauge("paxos_is_leader", "1 while this replica leads its shard, else 0."),
		commitLatency: histogram("paxos_commit_latency_seconds", "Leader: time from proposing a log entry to applying it.", latencyBuckets),
		applyLag:      gauge("paxos_apply_lag_slots", "Slots known to be committed (here or, from heartbeats, at the leader) but not yet applied here."),
		logLength:     gauge("paxos_log_length_slots", "Committed log entries (the commit index; the log is never compacted)."),

		commits:      counter("txn_commits_total", "Transactions committed, counted at the leader of the shard that decides them."),
		crossCommits: counter("txn_cross_shard_commits_total", "Of those, cross-shard transactions (counted at the coordinator shard)."),
		aborts:       map[txn.AbortReason]*prometheus.CounterVec{},
		prepare:      histogram("txn_prepare_phase_seconds", "Coordinator: time from starting two-phase commit to proposing the decision (collecting votes).", slowBuckets),
		decide:       histogram("txn_decide_phase_seconds", "Coordinator: time from proposing the decision to applying it from the log.", latencyBuckets),
		partPrepare:  histogram("txn_participant_prepare_seconds", "Participant: time from proposing a prepare record to applying it (then it votes).", latencyBuckets),
		query:        histogram("txn_outcome_query_seconds", "Participant: time from first asking the coordinator for an outcome to applying the outcome.", slowBuckets),
		inDoubt:      gauge("txn_in_doubt", "Transactions prepared on this replica of the shard whose outcome it has not applied yet."),
		wounds:       counter("txn_wounds_total", "Transactions wounded (aborted by an older one under wound-wait) at this leader."),
		lockWaits:    counter("txn_lock_waits_total", "Lock requests that had to wait at this leader."),

		fsync: histogram("storage_fsync_seconds", "Duration of each durable storage write (one SQLite transaction with an fsync).", latencyBuckets),
	}
	for _, r := range txn.AbortReasons {
		m.aborts[r] = counter("txn_aborts_"+string(r)+"_total", "Transactions aborted, reason "+string(r)+", counted at the leader of the deciding shard.")
	}
	return m, err
}

// shardMetrics are one shard replica's metrics, with the labels bound.
type shardMetrics struct {
	elections     prometheus.Counter
	isLeader      prometheus.Gauge
	commitLatency prometheus.Observer
	applyLag      prometheus.Gauge
	logLength     prometheus.Gauge
	commits       prometheus.Counter
	crossCommits  prometheus.Counter
	aborts        map[txn.AbortReason]prometheus.Counter
	prepare       prometheus.Observer
	decide        prometheus.Observer
	partPrepare   prometheus.Observer
	query         prometheus.Observer
	inDoubt       prometheus.Gauge
	wounds        prometheus.Counter
	lockWaits     prometheus.Counter
	fsync         prometheus.Observer
}

func (m *metrics) forShard(sh txn.ShardID, node paxos.NodeID) shardMetrics {
	lv := []string{strconv.Itoa(int(sh)), strconv.Itoa(int(node))}
	out := shardMetrics{
		elections:     m.elections.WithLabelValues(lv...),
		isLeader:      m.isLeader.WithLabelValues(lv...),
		commitLatency: m.commitLatency.WithLabelValues(lv...),
		applyLag:      m.applyLag.WithLabelValues(lv...),
		logLength:     m.logLength.WithLabelValues(lv...),
		commits:       m.commits.WithLabelValues(lv...),
		crossCommits:  m.crossCommits.WithLabelValues(lv...),
		aborts:        map[txn.AbortReason]prometheus.Counter{},
		prepare:       m.prepare.WithLabelValues(lv...),
		decide:        m.decide.WithLabelValues(lv...),
		partPrepare:   m.partPrepare.WithLabelValues(lv...),
		query:         m.query.WithLabelValues(lv...),
		inDoubt:       m.inDoubt.WithLabelValues(lv...),
		wounds:        m.wounds.WithLabelValues(lv...),
		lockWaits:     m.lockWaits.WithLabelValues(lv...),
		fsync:         m.fsync.WithLabelValues(lv...),
	}
	for r, v := range m.aborts {
		out.aborts[r] = v.WithLabelValues(lv...)
	}
	return out
}

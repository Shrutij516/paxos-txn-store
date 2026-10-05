# Observability

Phase 7 adds traces (OpenTelemetry), metrics (Prometheus) and structured logs (`log/slog`) to `paxosd` and the SDK.

## Where telemetry comes from

The Paxos replica (`internal/paxos`) and the transaction layer (`internal/txn`) never read the clock and never import OpenTelemetry or Prometheus. They report what happens through two small interfaces, `paxos.Observer` (became leader, stepped down, proposed an entry, applied an entry) and `txn.Observer` (outcome decided, two-phase commit phase boundaries, outcome query, wound, lock wait). The simulator passes no observer, and a test (`TestMPObserver`, `TestObserverMatchesLogs`) replays seeds with and without observers and checks the trace hashes are identical, so observing cannot change behavior.

The server (`internal/server/telemetry.go`) implements both interfaces once per shard replica. It runs on the shard's event loop, stamps events with wall-clock time and turns them into metrics, spans and log lines (DECISIONS.md entry 27).

## Running with telemetry

| Flag | Default | Meaning |
|---|---|---|
| `-metrics-listen` | empty (off) | address for the Prometheus `/metrics` endpoint, separate from the gRPC port, for example `127.0.0.1:9101` |
| `-otlp-endpoint` | empty (off) | OTLP/gRPC collector (`host:port`, plaintext) to export traces to |
| `-trace-sample` | `0.01` | fraction of new traces sampled; a trace a client started follows the client's decision (DECISIONS.md entry 28) |
| `-log-level` | `info` | `debug` adds one line per client request, with its trace ID |

`deploy/prometheus/prometheus.yml` scrapes a local cluster started with `-metrics-listen 127.0.0.1:9101`, `:9102` and `:9103`. `deploy/grafana/dashboard.json` is the dashboard described below; import it into Grafana with a Prometheus data source. Neither is run by tests yet.

The SDK takes a `TracerProvider` in `client.Options` (default: the global one, which does nothing unless the program installs one).

## Metrics

Every metric has exactly two labels, `shard` and `node`, and one series per shard on each node. No label value ever comes from a transaction ID, a key or a client; an abort reason is part of the metric name instead (DECISIONS.md entry 29). `TestMetricLabels` checks this for every metric after a contended workload. The Go runtime collectors are not registered, since `go_info` carries a `version` label.

"At the leader" means the counter moves only on the replica that leads the shard when the event happens, so summing over nodes counts each event once. Gauges are per replica.

### Paxos

| Metric | Type | Meaning |
|---|---|---|
| `paxos_leader_elections_total` | counter | Elections this replica won. A rise means a leader was lost or cut off; a steady climb means leaders keep losing their quorum. |
| `paxos_is_leader` | gauge | 1 while this replica leads its shard. Summed per shard it should be 1; 0 means no leader, 2 means a deposed leader has not noticed yet. |
| `paxos_commit_latency_seconds` | histogram | At the leader: from proposing a log entry to applying it, which is one round of Accept and Accepted messages plus the fsyncs on a majority. |
| `paxos_apply_lag_slots` | gauge | Slots known to be committed (by this replica, or by the leader according to its last heartbeat) but not applied here. Non-zero on a follower catching up. |
| `paxos_log_length_slots` | gauge | Committed log entries (the commit index). The log is never compacted, so this only grows. |

### Transactions

| Metric | Type | Meaning |
|---|---|---|
| `txn_commits_total` | counter | Transactions committed, at the leader of the shard that decides them: the one shard of a single-shard transaction, the coordinator (lowest shard) of a cross-shard one. |
| `txn_cross_shard_commits_total` | counter | Of those, the cross-shard ones. |
| `txn_aborts_wounded_total` | counter | Aborted because an older transaction needed its locks (wound-wait). |
| `txn_aborts_lock_lost_total` | counter | Aborted because a read lock was gone at commit (lease expiry or a leader change), so the read could be stale. |
| `txn_aborts_validation_total` | counter | A one-phase record rejected by the state machine (a read version changed, or a prepared transaction conflicts). |
| `txn_aborts_vote_no_total` | counter | A participant voted no. |
| `txn_aborts_timeout_total` | counter | The coordinator gave up waiting for votes. |
| `txn_aborts_presumed_total` | counter | A participant asked for the outcome and nobody was coordinating the transaction (presumed abort). |
| `txn_aborts_unknown_total` | counter | The abort was decided under an earlier leader, so this one does not know why. |
| `txn_prepare_phase_seconds` | histogram | Coordinator: from starting two-phase commit to proposing the decision. Mostly waiting for votes, which includes each participant's prepare round. |
| `txn_decide_phase_seconds` | histogram | Coordinator: from proposing the decision to applying it from the log. |
| `txn_participant_prepare_seconds` | histogram | Participant: from proposing a prepare record to applying it, after which it votes. It starts once the locks are granted, so lock waits are not included (see `txn_lock_waits_total`). |
| `txn_outcome_query_seconds` | histogram | Participant: from first asking the coordinator for an outcome to applying it. Only transactions left in doubt get here, so samples mean a coordinator was lost or slow. |
| `txn_in_doubt` | gauge | Transactions prepared on this replica whose outcome it has not applied. Normally 0 or a handful; a value that stays up means participants are holding locks waiting for a coordinator. |
| `txn_wounds_total` | counter | Transactions wounded at this leader. |
| `txn_lock_waits_total` | counter | Lock requests that could not be granted at once and waited (a younger transaction meeting an older one). |

Aborts only count transactions whose outcome a shard logged. A transaction the client gives up on during its reads (it was wounded, or its deadline passed) is counted in `txn_wounds_total` but has no outcome record, so it is not an abort here.

### Storage

| Metric | Type | Meaning |
|---|---|---|
| `storage_fsync_seconds` | histogram | Each durable write of a shard's log (a promise, an accepted entry, a commit batch, a proposer round): one SQLite transaction ending in an fsync, which dominates its time. Paxos waits for it before every reply, so it bounds commit latency. |

## Traces

A transaction is one trace across the client, every shard it touches and every node involved:

```
txn.run                                client, one per Run call
  txn                                  client, one per attempt (txn.id, txn.outcome, txn.shards)
    txn.v1.Txn/Begin, Read, Write      client and server spans from otelgrpc
    txn.v1.Txn/Commit                  client span
      txn.v1.Txn/Commit                server span at the coordinator shard's leader
        paxos.round                    one-phase: the record's propose to apply
        2pc.prepare                    cross-shard: collecting votes
          2pc.participant.prepare      each participant, the coordinator's own part included;
                                       another shard or node gets the context via the peer envelope
            paxos.round                the participant's prepare record
            paxos.round                later, its commit or abort record
            2pc.outcome_query          only when the participant had to ask for the outcome
        2pc.decide                     the coordinator's decision record
          paxos.round                  the decision's log round
```

Every server span carries `shard`, `node` and `txn.id` attributes; `paxos.round` adds `slot` and `record` (prepare, onephase, decide, commit, abort).

**How context crosses shards.** The SDK sends W3C trace context in gRPC metadata (otelgrpc). The coordinator's leader remembers it per transaction, and every two-phase commit message it sends (prepare, vote, decision, outcome query, wound) carries the transaction's current span in the peer `Envelope`'s `trace` field. A participant on another node extracts it from the envelope; one on the same node gets it through the shard inbox. `TestCrossShardTxnIsOneTrace` checks that a cross-shard transaction with its shards led by different nodes yields a single trace with spans from both shards and both nodes, including `2pc.prepare` and `2pc.decide`. It fails if the envelope field is dropped.

Peer RPCs (heartbeats, Paxos rounds, 2PC messages) get no gRPC spans of their own: they are far too frequent, and their work appears as the spans above. Background work with no transaction trace (a no-op filling a log gap, a presumed abort for a transaction the server never saw a trace for) starts no root spans.

**Overhead.** `BenchmarkTracingOverhead` (`make bench-trace`) runs eight workers doing conflict-free cross-shard transfers on an in-process 3-node cluster, with every node and client exporting through the SDK's batch processor to a discarding exporter. Three runs of 3 s each in the development sandbox:

| Tracing | Committed txns/s (3 runs) | Mean |
|---|---|---|
| off | 246, 243, 254 | 248 |
| sampling 0.1 | 256, 242, 220 | 239 |
| sampling 1.0 | 239, 234, 237 | 237 |

The cluster is bound by fsyncs (every replica of every shard shares one disk), so tracing costs about 4% at full sampling, and the 0.1 runs are within noise of tracing off. Exporting over the network to a real collector adds CPU for serialization on top.

## What the tests check

- `TestCrossShardTxnIsOneTrace` (in-memory exporter): one trace per cross-shard transaction, with spans from both shards and both nodes, including `2pc.prepare` and `2pc.decide`.
- `TestMetricLabels`: every metric has only the `shard` and `node` labels, with values from the cluster config, and every documented metric exists. `TestDashboardUsesRealMetrics`: every metric the dashboard queries exists.
- `TestRequestLogsCarryTraceID`: request logs are JSON with the transaction's trace ID.
- `TestMultiProcessTransactions` (real processes): picks each kill's victim from the `paxos_is_leader` gauges just before the kill, checks that `paxos_leader_elections_total` on the surviving nodes rises after it, waits for `txn_in_doubt` to return to 0 on every node before stopping the cluster, checks that `paxos_commit_latency_seconds` has samples, and receives the nodes' spans in a small OTLP receiver (with `-trace-sample 0.2`), requiring two-phase commit and Paxos round spans from at least two nodes.
- `TestMPObserver`, `TestObserverMatchesLogs`: the cores replay identically with observers attached, and the outcomes reported match the logs.

## Logs

`paxosd` logs JSON to stderr through `log/slog`. Every line has `node`; shard events also have `shard`.

- Info: `serving` (address, shard count, tick), `became leader` and `stepped down` (with ballots), `serving metrics`, `stopping`, `stopped`.
- Debug: `request` for every Txn RPC: `method`, `duration_ms`, gRPC `code` and `trace_id`, so a slow or failed request leads straight to its trace. `TestRequestLogsCarryTraceID` checks this.

## Reading the dashboard

`deploy/grafana/dashboard.json` has six panels. All are rates or quantiles over 1 minute.

- **Leader elections.** Elections won per shard per minute. It should be flat at 0. A spike on one shard is one failover; spikes on every shard at once mean a node died (each node leads replicas of every shard). Repeated spikes without a crash suggest timeouts too tight for the network or disk (DECISIONS.md entry 15).
- **Commit latency p50 / p99.** Per shard, from proposal to apply at the leader. p50 is about one network round trip plus a majority's fsync. A rising p99 with flat p50 usually follows the fsync panel; a gap during an election is expected, since nothing commits without a leader.
- **Throughput.** Committed transactions per second, all and cross-shard. A dip lines up with elections (transactions in flight on that shard abort and retry) and with in-doubt spikes (locks held).
- **Abort rate by reason.** One series per `txn_aborts_<reason>_total`. Under contention `vote_no` and `wounded` dominate (wound-wait at work; see docs/running.md on the contended workload). `lock_lost` rises after leader changes. `timeout` or `presumed` mean coordinators are failing or slow, and should be near zero otherwise.
- **In-doubt transactions.** Prepared transactions waiting for an outcome, per shard. Normally 0 or a few in flight. After a coordinator failure it rises and must fall back to 0 within about an election plus the in-doubt wait (`-in-doubt-wait`, docs/running.md). If it stays up, participants cannot reach the coordinator shard.
- **fsync latency p50 / p99.** Per node. This is the floor under commit latency; if both rise together, the disk is the bottleneck.

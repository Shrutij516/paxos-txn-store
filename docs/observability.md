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

Labels are only ever bounded (DECISIONS.md entry 29). The store's metrics carry `shard` and `node`, whose values come from the cluster config, so a node has one series per shard; `txn_aborts_total` adds `reason`, from the fixed set below. No label value ever comes from a transaction ID, a key or a client ID. The standard Go runtime and process collectors are registered too (`go_*`, `process_*`); their only labels are `quantile` (the fixed GC duration quantiles) and `version` (on `go_info`). `TestMetricLabels` checks every exported metric after a contended workload: only these label names exist, each value is in its fixed set, and every documented metric is there. `TestCheckLabelsRejects` shows the check refuses a `txn_id`, `key` or `client` label.

Latency histograms (`paxos_commit_latency_seconds`, `txn_prepare_phase_seconds`, `txn_decide_phase_seconds`, `txn_participant_prepare_seconds`, `storage_fsync_seconds`) share explicit buckets from 50µs to 2s, in seconds: 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2. That covers an fsync on a fast disk (tens of µs) up to a stall of an election timeout or two; slower samples land in `+Inf`. `txn_outcome_query_seconds` adds 5 and 10, since a query only starts after the in-doubt wait and can span an election.

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
| `txn_aborts_total` | counter | Transactions aborted, at the leader of the deciding shard, with a `reason` label: |

| `reason` | Meaning |
|---|---|
| `wounded` | An older transaction needed its locks (wound-wait). |
| `lock_lost` | A read lock was gone at commit (lease expiry or a leader change), so the read could be stale. |
| `validation` | The state machine rejected a one-phase record (a read version changed, or a prepared transaction conflicts). |
| `vote_no` | A participant voted no. |
| `timeout` | The coordinator gave up waiting for votes. |
| `presumed` | A participant asked for the outcome and nobody was coordinating the transaction (presumed abort). |
| `unknown` | The abort was decided under an earlier leader, so this one does not know why. |

| Metric | Type | Meaning |
|---|---|---|
| `txn_prepare_phase_seconds` | histogram | Coordinator: from starting two-phase commit to proposing the decision. Mostly waiting for votes, which includes each participant's prepare round. |
| `txn_decide_phase_seconds` | histogram | Coordinator: from proposing the decision to applying it from the log. |
| `txn_participant_prepare_seconds` | histogram | Participant: from proposing a prepare record to applying it, after which it votes. It starts once the locks are granted, so lock waits are not included (see `txn_lock_waits_total`). |
| `txn_outcome_query_seconds` | histogram | Participant: from first asking the coordinator for an outcome to applying it. Only transactions left in doubt get here, so samples mean a coordinator was lost or slow. |
| `txn_in_doubt` | gauge | Transactions prepared on this replica whose outcome it has not applied. Normally 0 or a handful; a value that stays up means participants are holding locks waiting for a coordinator. |
| `txn_wounds_total` | counter | Transactions wounded at this leader. |
| `txn_lock_waits_total` | counter | Lock requests that could not be granted at once and waited (a younger transaction meeting an older one). |

Aborts only count transactions whose outcome a shard logged. A transaction the client gives up on during its reads (it was wounded, or its deadline passed) is counted in `txn_wounds_total` but has no outcome record, so it is not an abort here.

### Storage and the event loop

| Metric | Type | Meaning |
|---|---|---|
| `storage_fsync_seconds` | histogram | Each durable write of a shard's log (a promise, an accepted entry, a commit batch, a proposer round): one SQLite transaction ending in an fsync, which dominates its time. Paxos waits for it before every reply, so it bounds commit latency. |
| `shard_loop_queue_depth` | gauge | Events waiting for the shard's event loop, read at scrape time: peer and cross-shard messages in its inbox plus client requests (and `Inspect` calls) waiting to be handed over. Near 0 when the loop keeps up. A sustained rise means the loop, usually its fsyncs, is the bottleneck; past 4096 queued messages new ones are dropped (Paxos and two-phase commit retry). |

### Go runtime and process

The standard collectors from `client_golang`: `go_goroutines`, `go_threads`, `go_gc_duration_seconds`, `go_memstats_*`, `go_info` and so on, and `process_cpu_seconds_total`, `process_resident_memory_bytes`, `process_open_fds` and the other `process_*` metrics. They describe the whole `paxosd` process, so they carry no `shard` or `node` label.

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
- `TestMetricLabels`: only bounded labels (`shard`, `node`, `reason`, `quantile`, `version`), each value in its fixed set, and every documented metric exists; `TestCheckLabelsRejects` is its negative control. `TestDashboardUsesRealMetrics`: every metric the dashboard queries exists.
- `TestRequestLogsCarryTraceID`: request logs are JSON with the transaction's trace ID.
- `TestMultiProcessTransactions` (real processes): picks each kill's victim from the `paxos_is_leader` gauges just before the kill, checks that `paxos_leader_elections_total` on the surviving nodes rises after it, waits for `txn_in_doubt` to return to 0 on every node before stopping the cluster, checks that `paxos_commit_latency_seconds` has samples, and receives the nodes' spans in a small OTLP receiver (with `-trace-sample 0.2`), requiring two-phase commit and Paxos round spans from at least two nodes.
- `TestMPObserver`, `TestObserverMatchesLogs`: the cores replay identically with observers attached, and the outcomes reported match the logs.

**A/B in real processes.** `TestTracingThroughputAB` (run with `PAXOSD_AB=1`) runs the multi-process workload of docs/running.md with the nodes at `-trace-sample 0` and no OTLP endpoint (off), and at `-trace-sample 0.2` exporting to an OTLP receiver (on), interleaving 5 runs of each. Two batches in the development sandbox, committed transactions per second:

| Batch | Off: mean (range) | On: mean (range) |
|---|---|---|
| 1 | 70.5 (58.4 to 91.7) | 59.3 (31.4 to 83.4) |
| 2 | 59.4 (40.8 to 79.5) | 65.0 (46.3 to 97.5) |
| Both, 10 runs each | 65.0 (40.8 to 91.7) | 62.2 (31.4 to 97.5) |

The difference between the means (about 4%) is far inside the run-to-run spread, and the batches disagree on its sign: the workload's contention and its two leader kills vary more from run to run than tracing costs. Tracing at 0.2 is not measurably slower here, consistent with the in-process benchmark above.

## Logs

`paxosd` logs JSON to stderr through `log/slog`. Every line has `node`; shard events also have `shard`.

- Info: `serving` (address, shard count, tick), `became leader` and `stepped down` (with ballots), `serving metrics`, `stopping`, `stopped`.
- Debug: `request` for every Txn RPC: `method`, `duration_ms`, gRPC `code` and `trace_id`, so a slow or failed request leads straight to its trace. `TestRequestLogsCarryTraceID` checks this.

## Reading the dashboard

`deploy/grafana/dashboard.json` has seven panels. All are rates or quantiles over 1 minute.

- **Leader elections.** Elections won per shard per minute. It should be flat at 0. A spike on one shard is one failover; spikes on every shard at once mean a node died (each node leads replicas of every shard). Repeated spikes without a crash suggest timeouts too tight for the network or disk (DECISIONS.md entry 15).
- **Commit latency p50 / p99.** Per shard, from proposal to apply at the leader. p50 is about one network round trip plus a majority's fsync. A rising p99 with flat p50 usually follows the fsync panel; a gap during an election is expected, since nothing commits without a leader.
- **Throughput.** Committed transactions per second, all and cross-shard. A dip lines up with elections (transactions in flight on that shard abort and retry) and with in-doubt spikes (locks held).
- **Abort rate by reason.** `txn_aborts_total` summed by `reason`, one series per reason. Under contention `vote_no` and `wounded` dominate (wound-wait at work; see docs/running.md on the contended workload). `lock_lost` rises after leader changes. `timeout` or `presumed` mean coordinators are failing or slow, and should be near zero otherwise.
- **In-doubt transactions.** Prepared transactions waiting for an outcome, per shard. Normally 0 or a few in flight. After a coordinator failure it rises and must fall back to 0 within about an election plus the in-doubt wait (`-in-doubt-wait`, docs/running.md). If it stays up, participants cannot reach the coordinator shard.
- **fsync latency p50 / p99.** Per node. This is the floor under commit latency; if both rise together, the disk is the bottleneck.
- **Event loop queue depth.** `shard_loop_queue_depth` per shard and node. Flat near 0 is healthy. A shard whose queue climbs while the others stay flat is the hot one; all shards on one node climbing together point at that node's disk or CPU. It usually rises together with fsync latency and commit latency.

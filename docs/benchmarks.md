# Benchmarks

Steady-state numbers for a 3-node, 3-shard cluster under a closed-loop bank workload. These replace nothing measured before: the earlier figures (about 55 to 78 committed transactions per second, docs/running.md and docs/chaos.md) came from chaos runs with kills on a deliberately contended 12-account workload and are not a benchmark. Design: DECISIONS.md entry 44.

## Method

- **Cluster.** A fresh cluster for every run: three native `paxosd` processes on one machine, empty data directories, default flags (10 ms tick, tracing off), loopback addresses. Every node hosts a replica of every shard; shard leaders are wherever the first elections put them.
- **Workload.** `cmd/loadgen` in benchmark mode:
  - **Transactions:** closed-loop clients, each running bank transfers through the public SDK (read two accounts, write both) over 10,000 accounts, opened in batches of 500 before the run. No audits, no short-deadline clients, no read-only transactions (`-read-ratio 0`).
  - **`-profile low`:** accounts are picked uniformly.
  - **`-profile high`:** 90% of picks go to a hot set of 16 accounts.
  - **`-cross-shard-ratio`:** 0 means both accounts are always on the same shard (one-phase commit); 0.5 means half the transactions span two shards (two-phase commit).
- **Run.** 10 s warmup, then 30 s measured. Attempts that start in the warmup or end after the window are excluded. Each config is run 5 times.
- **Metrics:**
  - **Throughput:** committed transactions per second in the window.
  - **Latency:** per committed transaction, from the start of `client.Run` (its `Begin` RPC included) to the commit acknowledgement, including earlier aborted attempts and their backoff. Percentiles are exact (every sample kept and sorted, nearest rank), not a histogram.
  - **Abort rate:** aborted attempts / attempts.
  - **Abort reasons:** the change in the nodes' `txn_aborts_total{reason}` over the window. They count only aborts decided at a shard leader, so they sum to less than the client-side aborts (a transaction wounded while it held only read locks fails at its next read or write and is not counted by reason).
  - **Errors:** unknown outcomes plus failures before commit.
- **Correctness.** The first run of every config keeps its history and data directories. After the run, `scripts/bench.sh` waits until every node is ready with no transaction in doubt, stops the nodes, and the chaos checkers are run offline (`cmd/chaos -check-history -check-data`): replica agreement, atomicity, the bank invariant, strict serializability, and client outcomes against the logs.

Reproduce:

```sh
scripts/bench.sh bench-out            # the full matrix below, about 30 minutes
RUNS=1 WARMUP=2s DURATION=5s scripts/bench.sh bench-smoke   # quick check
go build -o bin/chaos ./cmd/chaos
d=bench-out/runs/low-cross0.5-c32-r1
bin/chaos -cluster $d/cluster.json -check-history $d/history.jsonl -check-data $d/data/n1,$d/data/n2,$d/data/n3
```

## Machine

The development sandbox, 2026-11-05:
- **CPU:** a KVM guest with 4 vCPUs (Intel Xeon @ 2.80 GHz, 1 thread per core).
- **Memory:** 16 GB.
- **Disk:** one virtio disk.
- **OS:** Linux 6.18; Go 1.26.10.

The three nodes, their nine SQLite files and the load generator all share those 4 vCPUs and one disk. **These numbers include no network latency** (loopback only) and no cross-machine fsync parallelism; they are not a prediction for a multi-machine or multi-AZ deployment.

## Results

These are the first measurements, before the fix for incident I5. The fix and its before/after runs are in [Leader placement after the I5 fix](#leader-placement-after-the-i5-fix).

Each cell is the mean over 5 runs (min–max). Errors (unknown plus failed) were 0 in every run.

| Profile | Cross-shard | Clients | Committed txn/s | p50 ms | p90 ms | p99 ms | max ms | Abort % | Top abort reasons (per run) |
|---|---|---|---|---|---|---|---|---|---|
| low | 0 | 8 | **545** (533–553) | 13.7 (13.5–14.0) | 22.0 (21.7–22.7) | 31.7 (31.0–32.2) | 141 (70–390) | 0.1 | wounded 6 |
| low | 0 | 32 | **558** (399–645) ⚠ | 56.7 (48.5–79.7) | 89.7 (77.1–102) | 126 (107–150) | 234 (195–276) | 0.3 | wounded 24 |
| low | 0.5 | 8 | **244** (230–263) | 29.0 (27.5–31.4) | 58.3 (52.8–65.1) | 77.5 (69.9–86.2) | 116 (99–131) | 0.1 | wounded 2, vote_no 2 |
| low | 0.5 | 32 | **176** (45–236) ⚠ | 168 (36.7–678) | 541 (362–991) | 679 (434–1186) | 1243 (963–1809) | 0.3 | wounded 9, vote_no 6 |
| high | 0 | 8 | **379** (369–385) | 12.2 (12.0–12.4) | 44.4 (43.7–45.3) | 90.4 (87.4–93.3) | 276 (182–407) | 26.6 | wounded 2033 |
| high | 0 | 32 | **294** (276–317) | 48.2 (43.0–57.4) | 309 (257–342) | 658 (601–694) | 1373 (1090–1616) | 57.7 | wounded 5824 |
| high | 0.5 | 8 | **179** (177–181) | 32.7 (32.0–33.2) | 95.4 (94.2–96.9) | 178 (171–181) | 388 (318–583) | 36.3 | wounded 1097, vote_no 907 |
| high | 0.5 | 32 | **100** (90–113) | 236 (224–260) | 723 (626–807) | 1272 (1150–1387) | 1803 (1539–2027) | 72.4 | wounded 2474, vote_no 2074 |

What the table says:
- **Single-shard throughput:** single-shard transfers saturate at about 550 committed/s on this machine; going from 8 to 32 clients adds latency (p50 14 → 57 ms), not throughput.
- **Cross-shard cost:** two-phase commit costs more than half the throughput at 8 clients (545 → 244/s) and doubles p50. Each cross-shard commit writes about four log records across two shards (two prepares, a decision and a commit), against one for a single-shard commit.
- **Contention:** the hot set turns concurrency into aborts under wound-wait. At 32 clients, 58% (single-shard) and 72% (cross-shard) of attempts abort, and adding clients lowers throughput.

## Noisy configs (runs more than 20% apart)

| Config | Spread | Cause |
|---|---|---|
| low / 0 / 32 | txn/s 399–645 (44% of the mean); p50 and p99 too | Run 3: one node led all three shards (below) |
| low / 0.5 / 32 | txn/s 45–236 (108%); p50 37–678 ms | Run 3: one node led all three shards, 45 txn/s with no aborts |
| low / 0.5 / 8 | p99 70–86 ms (21%) | Tail noise only; throughput within ±8% |
| high / 0 / 32 | p50 43–57 ms (30%) | Contention: abort and retry patterns vary per run; throughput within ±8% |
| high / 0.5 / 32 | txn/s 90–113 (23%) | Same, at 72% aborts |

**Leader placement is the large effect** (incident I5). Leaders are not balanced: each shard's leader is whichever node won its first election. In 5 of the 40 runs one node led all three shards.
- **When it matters:** in the low-contention configs at 32 clients, which are bound by message and disk throughput, this cut throughput to 398 (vs 558–645) and to **45** (vs 173–236) transactions per second.
- **When it doesn't:** in contention-bound or 8-client configs it made no measurable difference.
- **Mechanism:** confirmed with drop counters on a throwaway build.
  1. With all leaders on one node, all Accepts and two-phase commit messages leave through that node's two per-peer queues, each drained by one serial RPC at a time.
  2. The queues overflow: about 14,700 dropped messages per 25 s run, against 0 with spread leaders.
  3. Every heartbeat then retransmits every unchosen slot, which refills the queues: congestion collapse.
  4. The leader node sent about 100 RPCs per committed transaction, against about 26 with spread leaders.

Excluding those runs would hide a real behaviour, so they stay in the table. The fix is below.

## Leader placement after the I5 fix

Bounded Accept retransmission (DECISIONS.md entry 45) removed the feedback loop. To measure it without waiting for chance placements, `scripts/bench.sh` now forces placement: `PLACEMENT=concentrated` restarts each run's cluster until one node leads all three shards, and `PLACEMENT=spread` until each node leads one. `PAXOSD_BIN` runs a given binary.
- **Before:** this branch built with the old resend-everything heartbeat switched back on. It differs from the original only in that, and it exports the new drop counter.
- **After:** the fix.

Low contention, 32 clients, 10 s warmup and 30 s measured, 5 runs per cell, the same machine on a later day. Cells are means (min–max). Queue drops are `peer_messages_dropped_total{reason="send_queue_full"}` summed over the nodes per run. Errors were 0 and every run settled.

| Cross-shard | Leaders | Build | Committed txn/s | p50 ms | p99 ms | Queue drops |
|---|---|---|---|---|---|---|
| 0 | one node | before | 369 (331–410) | 85.4 | 140 | 0 |
| 0 | one node | **after** | **571** (531–646) | 54.4 | 105 | 0 |
| 0 | spread | before | 455 (370–507) | 44.4 | 250 | 0 |
| 0 | spread | **after** | **573** (549–595) | 53.6 | 113 | 0 |
| 0.5 | one node | before | **37** (35–39) | 876 | 1583 | 58,862 (54,666–63,325) |
| 0.5 | one node | **after** | **192** (178–231) | 158 | 311 | **0** |
| 0.5 | spread | before | 86 (63–148) | 48.5 | 1245 | 6,802 (63–11,466) |
| 0.5 | spread | **after** | **247** (241–261) | 93.2 | 419 | 0 |

What it says:
- **The collapse is gone.** With one node leading every shard, cross-shard throughput went from 37 to 192/s, p99 from 1.6 s to 311 ms, and drops from about 59,000 a run to none.
- **Spread leadership was hit too.** On this day the old code also dropped messages and collapsed with spread leaders on cross-shard load (63–148/s). The exact pre-fix commit, fc53199, gave the same: 59, 126 and 59/s over 3 runs. The loop needed any overflowing queue, not one particular placement. The first measurements above caught it only in the concentrated runs, and this machine is noisy from day to day, so compare within a table, not across tables.
- **Placement still costs something on cross-shard load.** Concentrated leadership reaches 78% of spread (192 vs 247/s), short of a "within 20%" target. Single-shard is at parity (571 vs 573/s). In a concentrated run the leader node used about 1.4 CPU cores and each other node about 0.7. One node is doing every shard's leader work and every coordinator's two-phase commit, on a 4-vCPU machine it shares with the others and the load generator. That is uneven load, not a failure: there are no drops, and latency is bounded.
- **Leader balancing was not added.** It would close the remaining gap, but it means leader-transfer machinery in the protocol, for a 22% difference on one machine (DECISIONS.md entry 45, Rejected).
- **Regression test:** `PAXOSD_PLACEMENT=1 go test -run TestMultiProcessConcentratedLeadership ./cmd/paxosd` runs both placements and fails on drops, or on concentrated below 60% of spread. With `PAXOSD_BIN` set to the old build it fails: 47,410 drops, 51%.

Reproduce:

```sh
PLACEMENT=concentrated RUNS=5 PROFILES=low CLIENTS=32 CROSS="0 0.5" scripts/bench.sh bench-conc
PLACEMENT=spread       RUNS=5 PROFILES=low CLIENTS=32 CROSS="0 0.5" scripts/bench.sh bench-spread
```

## Correctness at speed

Ten histories were checked offline after settling: the first run of every config (8), one experiment run with all leaders on one node, and one run of the fixed script. All 10 passed with no violations, over 102,527 committed transactions in total:

| Run | Committed checked | Cross-shard | Result |
|---|---|---|---|
| low / 0 / 8 | 20,936 | 0 | no violations |
| low / 0 / 32 | 22,704 | 0 | no violations |
| low / 0.5 / 8 | 10,436 | 5,273 | no violations |
| low / 0.5 / 32 | 6,694 | 3,352 | no violations (after settling, see incident I6) |
| high / 0 / 8 | 15,381 | 0 | no violations |
| high / 0 / 32 | 11,669 | 0 | no violations |
| high / 0.5 / 8 | 6,954 | 3,467 | no violations |
| high / 0.5 / 32 | 3,814 | 1,901 | no violations |
| all leaders on one node, low / 0.5 / 32 | 1,448 | 708 | no violations (after settling) |
| re-run with the fixed script, low / 0.5 / 32 | 2,491 | 1,255 | no violations |

"Committed checked" includes the 20 setup transactions of each run.

## Caveats

- **One machine.** No network latency, a shared CPU and a shared disk; the load generator takes CPU from the nodes. Absolute numbers are machine-specific and are not a multi-machine prediction.
- **Closed loop.** A fixed number of clients, each waiting for its previous transaction; overload shows as latency, not as dropped requests, and there is no target arrival rate.
- **Leader placement** is uncontrolled in the first table, where it dominated the high-throughput configs. `PLACEMENT` controls it (see the section after the I5 fix).
- **One workload.** Two-account transfers. Read-only and audit mixes, larger transactions and blind writes are not in this matrix (`-read-ratio` and `-audit-percent` exist for them).
- **Fresh state.** Every run starts with an empty log. Long runs would add the unbounded log and memory growth (DECISIONS.md entry 9), which this does not measure.

# Chaos testing

`cmd/chaos` runs the bank workload (`cmd/loadgen`) against the compose stack (`deploy/compose.yaml`) while injecting faults into the nodes, then checks the result with the Phase 5 checkers. CI runs it for 2 minutes on every pull request and for 20 minutes every night (`.github/workflows/chaos.yml`).

```sh
docker build -t paxos-txn-store:dev .
go build -o bin/loadgen ./cmd/loadgen && go build -o bin/chaos ./cmd/chaos
./bin/chaos -duration 2m -loadgen ./bin/loadgen -out chaos-out
```

The runner first recreates the stack with empty volumes (`docker compose down -v`, then `up --wait`), so the history covers every transaction in the logs; `-fresh=false` reuses a running stack.

## The workload

`loadgen` uses only the public SDK. It opens 12 accounts with 100 each in one transaction, then runs 10 concurrent clients for the run's duration: transfers between two random accounts (about two thirds of them cross-shard) and, 10% of the time, audits that read every account. Two of the clients give each transaction 150 ms, less than a failover, so faults leave some commits with an unknown outcome. Every attempt goes to the history file (package `history`, one JSON object per line): its ID, the versions and values it read, the shards it touched, when it began and ended on loadgen's clock, and what the client was told (committed, aborted, unknown, or failed before commit).

## The faults

One fault at a time, on a node picked at random. After a pause of 2 to 6 seconds the runner injects a fault, holds it for 2 to 8 seconds, undoes it, and repeats until the run's time is up (`-min-gap`, `-max-gap`, `-min-hold`, `-max-hold`). The kinds are equally likely:

| Fault | How | Undone by |
|---|---|---|
| Kill | `docker kill -s KILL`: the process dies with no chance to clean up; the node restarts from its volume | `docker start` |
| Delay | `tc qdisc add dev eth0 root netem delay D J`, D from 50 to 300 ms, jitter D/2, on everything the node sends | `tc qdisc del dev eth0 root` |
| Loss | `netem loss P%`, P from 10 to 40 | the same |
| Partition | `netem loss 100%`: the node is cut off from every peer and client | the same |

`tc` runs in a short-lived sidecar container (`nicolaka/netshoot`) that joins the node's network namespace (`--net container:<node>`) and has the `NET_ADMIN` capability. The node image has no shell or tools, and the node itself never gets `NET_ADMIN`. With one node faulty at a time, every shard keeps a majority, so the cluster should stay available; leaders move whenever a fault hits the node leading a shard.

If the kernel lacks netem (the runner checks once at start, and logs `netem available: false`), delay and loss are skipped and a partition uses `iptables` DROP rules against the other nodes instead. GitHub's Ubuntu runners have netem; some sandboxes do not.

## What is checked

When the run ends, the runner undoes the last fault, waits for loadgen to finish, and waits (up to 2 minutes) until every node answers `/readyz` and `txn_in_doubt` is 0 everywhere; a cluster that does not settle is a violation. It then stops the nodes with SIGTERM, copies their data directories out, replays every replica's committed log of every shard, and checks:

- **Replica agreement:** all replicas of a shard committed the same entry at every slot they share.
- **Atomicity:** every decided transaction has the same outcome on all its shards, and nothing is left prepared.
- **Bank invariant:** the balances sum to 1200 in the final state and in every committed audit.
- **Strict serializability:** the committed transactions, with the versions they read and real-time order from loadgen's clock, form an acyclic dependency graph; this also rules out reads of aborted (G1a) or intermediate (G1b) versions.
- **Client outcomes:** every transaction a client was told committed is committed in the logs, and every one it was told did not commit is not. An unknown outcome may go either way and counts as whatever the logs say.

Any violation fails the run. The runner writes `report.json` (fault counts, attempts by outcome, transactions checked, violations), `faults.json` (each fault with its node and timing), `history.jsonl`, `loadgen.log` and `compose.log` (all node logs) to `-out`; CI uploads the directory, with the nodes' data, when a run fails. `internal/chaos/check_test.go` runs the checks on a real loadgen history from an in-process cluster with a node stopped mid-run, and shows that tampered histories (a false commit, a false abort, a read of a version nobody wrote) are caught.

## Results

A 60-second run in the development sandbox, which has no netem (kills and iptables partitions only):

| Faults | Attempts | Committed (cross-shard) | Aborted | Unknown (committed) | Failed before commit | Checked | Violations |
|---|---|---|---|---|---|---|---|
| 2 kills, 4 partitions | 7178 | 2567 (1773) | 4334 | 80 (26) | 223 | 2568 | 0 |

CI results with netem are in the workflow runs.

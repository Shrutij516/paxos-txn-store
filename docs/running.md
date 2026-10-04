# Running a cluster

`paxosd` is one node of the sharded transaction store. Every node hosts one replica of every shard (DECISIONS.md entry 22), so a 3-node, 3-shard cluster is three processes, each running three Multi-Paxos replicas. The `client` package is the Go SDK. There is no TLS yet.

## Build

```sh
go build -o bin/paxosd ./cmd/paxosd
```

## Describe the cluster

All nodes read the same JSON config file: the number of shards, and every node's ID and address. Save this as `cluster.json`:

```json
{
  "shards": 3,
  "nodes": [
    {"id": 1, "addr": "127.0.0.1:7001"},
    {"id": 2, "addr": "127.0.0.1:7002"},
    {"id": 3, "addr": "127.0.0.1:7003"}
  ]
}
```

Node IDs must be positive and unique. Unknown keys are rejected, so a typo fails at startup instead of being ignored. Keys are assigned to shards by hashing (`api.ShardOf`: FNV-1a modulo the shard count), so the shard count cannot change once data is written.

## Start three nodes locally

Run each command in its own terminal:

```sh
bin/paxosd -config cluster.json -id 1 -data-dir data/1
bin/paxosd -config cluster.json -id 2 -data-dir data/2
bin/paxosd -config cluster.json -id 3 -data-dir data/3
```

Each shard elects its own leader within a few hundred milliseconds, so the leaders of different shards are often on different nodes. Each node serves the peer service (Paxos and two-phase commit messages, tagged with their shard) and the client `Txn` service on its address. A node keeps one SQLite file per shard in its data directory, `node-<id>-shard-<s>.db` (DECISIONS.md entry 23).

| Flag | Default | Meaning |
|---|---|---|
| `-config` | required | the cluster config file, identical on all nodes |
| `-id` | required | this node's ID, must appear in the config |
| `-data-dir` | required | where the node's SQLite files live |
| `-listen` | the node's `addr` in the config | address to bind, if different (for example `0.0.0.0:7001`) |
| `-tick` | `10ms` | wall-clock length of one logical tick |
| `-rpc-timeout` | `200ms` | deadline for each peer RPC |
| `-in-doubt-wait` | `600ms` | how long a prepared transaction waits for its coordinator's decision before its shard leader asks for it (rounded up to whole ticks); see below |

With the default tick, a shard leader sends a heartbeat every 40 ms and a follower starts an election after 200 to 400 ms without one (DECISIONS.md entry 15). The transaction layer's timeouts are also in ticks: a prepared participant asks its coordinator for the outcome after `-in-doubt-wait` (0.6 s), a coordinator gives up on missing votes after 3 s, and an idle read lock expires after 2 s.

Stop a node with Ctrl-C or `kill <pid>` (SIGTERM). It stops accepting requests, waits up to 2 seconds for in-flight ones, stops its event loops and closes its databases. Start it again with the same flags and every shard replica recovers from its file (docs/storage.md) and catches up from its shard's leader.

## Run a transaction with the SDK

This program opens two accounts, then moves 30 from one to the other. With 3 shards, `alice` is on shard 2 and `dave` on shard 1, so the transfer commits with two-phase commit.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/Shrutij516/paxos-txn-store/client"
)

func main() {
	c, err := client.New([]string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"}, client.Options{})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Open both accounts in one transaction.
	err = c.Run(ctx, func(ctx context.Context, t *client.Txn) error {
		if err := t.Write(ctx, "alice", "100"); err != nil {
			return err
		}
		return t.Write(ctx, "dave", "100")
	})
	if err != nil {
		log.Fatal(err)
	}

	// Transfer 30. Run retries the function if the transaction aborts.
	err = c.Run(ctx, func(ctx context.Context, t *client.Txn) error {
		a, err := t.Read(ctx, "alice")
		if err != nil {
			return err
		}
		d, err := t.Read(ctx, "dave")
		if err != nil {
			return err
		}
		na, _ := strconv.Atoi(a)
		nd, _ := strconv.Atoi(d)
		if na < 30 {
			return errors.New("insufficient funds") // aborts, not retried
		}
		if err := t.Write(ctx, "alice", strconv.Itoa(na-30)); err != nil {
			return err
		}
		return t.Write(ctx, "dave", strconv.Itoa(nd+30))
	})
	switch {
	case errors.Is(err, client.ErrUnknown):
		log.Fatal("the transfer may or may not have committed; read the balances to find out")
	case err != nil:
		log.Fatal(err)
	}
	fmt.Println("transferred 30 from alice to dave")
}
```

The SDK (`client`) and the types it exposes (`api`: transaction IDs, the shard function, the outcome errors) import none of this module's internal packages, so the program builds in any module that requires this one. CI checks that (`make sdk-check`). `client/example_test.go` holds the same transfer as a compiled example.

What the SDK does:

- **Routing to shard leaders.** `Read` and `Write` go to the leader of the key's shard, and `Commit` goes to the leader of the lowest shard the transaction touched, which coordinates it. The client caches one leader per shard. A node that does not lead the shard answers "not leader" with the leader it knows of, and the client retries there at once.
- **Retries of one call.** Each RPC attempt has a deadline (`AttemptTimeout`, default 1 s). After a failure the client forgets that leader, tries another node, and waits a randomized, exponentially growing backoff (20 ms up to 500 ms), until the call's context ends.
- **Retries of aborted transactions.** `Run` runs the function and commits. If the transaction aborts (it was wounded by an older one, or a lock was lost to a leader change), `Run` waits a randomized backoff and runs the function again as a new attempt that keeps the first attempt's start timestamp, so it wins conflicts eventually. It gives up after `MaxAttempts` (default 10). Any other error from the function aborts the transaction and is returned as is.
- **Unknown outcomes.** `Commit` retries the same request until it gets an answer or its context ends; the servers answer a repeated commit from the log, so it never commits twice. If the context ends after a request may have reached a coordinator, `Commit` returns an error wrapping `client.ErrUnknown`: the transaction may or may not have committed. `Run` returns that to the caller and does not retry, since running a transfer again could apply it twice (DECISIONS.md entry 25).
- **Reads.** A read takes a shared lock at the shard leader and returns the latest committed value. Reading a key twice, or reading a key the transaction already wrote, is answered from the client.
- **Aborts.** `Txn.Abort` (and `Run` when the function fails) asks the leaders of the touched shards to drop the transaction's locks, even if the caller's context has already ended. Locks of a client that disappears expire after the lock lease.
- A `Client` is safe for concurrent use; a `Txn` is not.

Transactions are strictly serializable (docs/transactions.md).

## Multi-process test

`TestMultiProcessTransactions` (in `cmd/paxosd`) starts three real `paxosd` processes with three shards and runs 10 concurrent SDK clients doing bank transfers between 12 accounts, plus 10% audits that read every account. Eight clients give each transaction 10 s; two give it 150 ms, less than an election timeout. During the run the test SIGKILLs the leader of shard 2 and restarts it 1.5 s later. Then it waits until a short client's commit is in flight at shard 0 (the coordinator of every transaction that touches it), SIGKILLs the node leading shard 0 and restarts it 1.5 s later. Afterwards it stops every process with SIGTERM, rebuilds every shard from the SQLite files (checking that all replicas' logs agree), and runs the Phase 5 checkers on the real history: atomicity, the bank invariant (final state and every committed audit), and strict serializability, which also rejects G1a and G1b. It also checks that every transaction the SDK reported committed is committed in the logs, and every one reported aborted is not. A transaction with an unknown outcome counts as whatever the logs say. The test fails if no transaction ended with an unknown outcome, or if no cross-shard transaction was in flight across each kill. It runs 3 times in full CI, once under `-short`. Set `PAXOSD_KEEP_LOGS` to a directory to keep a failed run's node logs, config and SQLite files there.

Numbers from one full run of 3 iterations in the development sandbox:

| Metric | Value |
|---|---|
| Committed transactions per second | 78 (73 to 83 per iteration) |
| Aborted attempts | 53.5% of 4580 |
| Cross-shard share of commits | 69% |
| Transactions with unknown outcome | 28 (10 of them committed) |
| Failover, first kill (shard 2 leader) | 273, 243, 240 ms |
| Failover, second kill (shard 0 leader, commits in flight) | 263, 402, 219 ms |

They vary a lot between runs on a shared machine: another full run gave 26 to 59 committed transactions per second and one second-kill failover of 1.36 s. Failover is the time from the SIGKILL to the commit of the first transaction that began after it and touched a shard the killed node led. It is at least an election timeout (200 to 400 ms), and more when that transaction needs a key locked by a transaction left prepared by the kill: such a transaction stays prepared until its participants ask the new coordinator leader (after `-in-doubt-wait`) and learn the outcome. That is the blocking window two-phase commit keeps even with replicated coordinators, now bounded by the in-doubt wait instead of by a coordinator restart.

### The in-doubt wait and failover

`-in-doubt-wait` sets how long a shard leader holding a prepared transaction waits for the coordinator's decision before asking the coordinator shard for it. Normally the decision arrives within milliseconds and the wait never expires. It matters when the coordinator shard's leader dies between collecting votes and telling the participants: the participants keep the transaction's locks for an election plus up to the in-doubt wait plus one round trip. Any transaction that needs one of those keys waits that long, or is wounded and retried. A participant whose leader changes starts the wait again under the new leader.

- Shorter (down to a few ticks): in-doubt locks are released sooner after a coordinator failure. The cost is more outcome queries. A coordinator that is still collecting votes just lets a query wait, but a query that reaches a new coordinator leader before the client's commit retry does finds no decision and no coordination in progress, so it logs a presumed abort, aborting a transaction that could still have committed.
- Longer: fewer needless queries, but locks left by a coordinator failure are held longer, so throughput drops for longer after such a failure.

Measured with the multi-process test (3 iterations each, same machine): the failover metric above stayed at 193 to 456 ms for 150 ms, 600 ms and 2 s, because the first transaction to commit after a kill rarely needs a key that an in-doubt transaction holds. Throughput was 66, 63 and 59 committed transactions per second, within the run-to-run noise noted above. The wait shows in the transactions that do touch in-doubt keys: they wait at least the election timeout plus the in-doubt wait.

The workload is deliberately contended: 10 clients on 12 accounts, where two transfers that read the same account both need to upgrade to a write lock and the younger one is wounded, and audits hold read locks on every account. Uncontended, a single-shard transfer takes about 3 ms and a cross-shard one about 7 ms on the same machine.

## Regenerating protobuf code

The generated files in `proto/` are committed, and CI regenerates them with pinned tools (`make proto-check`) and fails if anything differs. After editing a `.proto` file, install `protoc` 29.3, then:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
make proto
```

## How it fits together

- `proto/paxos/v1`: peer messages. One `Envelope` with a shard number and a `oneof` per message type: the Paxos messages and the two-phase commit messages (prepare, vote, decision, outcome query, wound). The `Peer` service carries them.
- `proto/txn/v1`: the client `Txn` service and the transaction messages.
- `internal/wire`: converts between the protobuf types and the plain Go structs in `internal/paxos` and `internal/txn`. Neither core package sees protobuf. Fuzz tests round-trip every message type.
- `internal/grpcnet`: the node-to-node transport. Each peer has a bounded queue, shared by all shards, drained by one goroutine that sends one unary RPC per message under a deadline. A full queue drops messages, so a slow or dead peer never blocks the node.
- `internal/server`: one node. Each shard has its own event-loop goroutine that owns its replica, transaction state machine and transaction server; gRPC handlers, the ticker and the other shards only send it events over channels.
- `cmd/paxosd`: flags, config file, signal handling.
- `client`: the SDK.

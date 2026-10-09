# Decisions

Each entry records What we chose, Why, what we Rejected, and the Tradeoff we accepted.

## 1. Go as the implementation language

- **What:** The whole system is written in Go.
- **Why:** Goroutines and channels fit a networked service with many concurrent RPCs. The standard library covers most needs, the race detector is built in, and gRPC, OpenTelemetry and SQLite all have mature Go libraries. Single static binaries make chaos testing with many processes easy.
- **Rejected:** Rust (stronger safety guarantees, but slower iteration and a steeper learning curve for contributors); Java (mature ecosystem, but heavier runtime and GC tuning); C++ (maximum control, too much footgun surface for a consensus codebase).
- **Tradeoff:** We accept GC pauses and less compile-time control over memory and aliasing. Go's simplicity also means fewer ways to encode invariants in types, so tests carry more of that weight.

## 2. gRPC with Protobuf for node-to-node and client traffic

- **What:** In a later phase, nodes talk over gRPC with Protobuf-defined messages.
- **Why:** Schema-first contracts, generated code, HTTP/2 multiplexing, deadlines and cancellation, and first-class OpenTelemetry instrumentation. Protobuf evolves safely with field numbers.
- **Rejected:** Hand-rolled TCP framing (full control, but we would rebuild deadlines, retries and codegen); JSON over HTTP (easy to debug, but slower, untyped, and schema drift is easy); Cap'n Proto (fast, smaller ecosystem in Go).
- **Tradeoff:** Extra build step (protoc), a larger dependency tree, and messages that are less human-readable on the wire.

## 3. SQLite instead of RocksDB for persistence

- **What:** Acceptor state, the Paxos log and the key-value data will live in SQLite.
- **Why:** One file per node, ACID transactions, simple to inspect with the sqlite3 shell, and a pure-Go driver is available so we avoid cgo. Our write rate is bounded by consensus round trips, not by the storage engine, so SQLite's throughput is enough. Transactions make "persist promise and accepted state atomically before replying" straightforward.
- **Rejected:** RocksDB (much higher write throughput and LSM compaction, but cgo, many tuning knobs, and harder to inspect); BoltDB/bbolt (pure Go and simple, but a single writer with a B+tree that is slower for our append-heavy log and less familiar tooling); a custom WAL (smallest footprint, but durability bugs are easy to write).
- **Tradeoff:** Lower peak write throughput and a single-writer lock per database. If profiling later shows storage is the bottleneck, the `paxos.Storage` interface lets us swap engines.

## 4. Deterministic simulated network for tests

- **What:** Consensus tests run on `transport.SimNet`: an in-memory, single-threaded, step-driven network whose every choice (drop, delay, duplicate, reorder, partition, crash) comes from one seeded random source. Paxos code never calls `time.Now` or `time.Sleep`; time is a `Tick` call.
- **Why:** Consensus bugs hide in rare interleavings. A deterministic simulator lets us run thousands of adversarial schedules per second and replay any failure exactly from its seed. Tests are fast and never flaky.
- **Rejected:** Real sockets with goroutines and sleeps (realistic, but slow, flaky, and failures cannot be replayed); Jepsen-style external testing only (excellent for the final system, but too slow and coarse for the inner loop); model checking with TLA+ alone (proves the design, not the code).
- **Tradeoff:** The simulator is not the real network. Bugs in gRPC wiring, real concurrency, or clock behavior are out of its reach, so later phases add chaos tests on real processes. Code must also be written in an event-driven style so it can be driven step by step.

## 5. Paxos messages as plain Go structs until the gRPC phase

- **What:** `Prepare`, `Promise`, `Accept`, `Accepted` and `Nack` are ordinary Go structs passed through a `Transport` interface. Protobuf definitions come in the gRPC phase.
- **Why:** Keeps Phase 1 focused on algorithm correctness. Structs are comparable, easy to construct in tests, and print readably in traces. The `Transport` interface means the gRPC transport can be added without touching the proposer, acceptor or learner.
- **Rejected:** Defining Protobuf messages from day one (earlier wire contract, but codegen churn while the algorithm is still changing, and generated types are not comparable with `==`).
- **Tradeoff:** A mapping layer between structs and Protobuf is needed later, and wire compatibility is not exercised until then.

## 6. Leader election via heartbeats and randomized timeouts

- **What:** The Multi-Paxos leader sends a heartbeat every few ticks. Each follower has an election timer, measured in simulator ticks, reset to a random value in a range whenever it hears from the leader. When it expires, the follower runs Prepare with a higher ballot.
- **Why:** Simple, well understood (the same idea as Raft), and deterministic under the simulator because time is a tick count and the randomness comes from the seeded source. Randomized timeouts make split votes rare without any coordination. Heartbeats double as the channel for the commit index and as the retransmission timer for lost Accepts.
- **Rejected:** A fixed leader (no fault tolerance); a separate failure detector or leader-election service (another component to run and test); pure Paxos preemption without heartbeats (proposers keep stealing leadership and livelock is more likely); fixed timeouts (repeated split votes).
- **Tradeoff:** Leader failure costs at least one election timeout before progress resumes. Timeouts are in ticks now, and will need tuning in real time once the gRPC transport exists. A partitioned old leader can believe it leads until it hears a higher ballot; safety does not depend on that belief, but it wastes client retries.

## 7. Reads through the log instead of leader leases

- **What:** A Get is a log entry. It is answered only after it is committed and applied in order.
- **Why:** Linearizable with no timing assumptions. Committing the read proves the leader still holds its ballot at that point, so a deposed leader cannot return stale data.
- **Rejected:** Leader leases (local reads, much lower latency, but correctness depends on bounded clock drift and careful lease handoff); read-index style reads with a heartbeat round (cheaper than a log write, but more protocol to get right in this phase); follower reads (stale).
- **Tradeoff:** Every read costs a consensus round trip and a log slot. Leases or read-index can be added later behind the same client API.

## 8. Porcupine for linearizability checking

- **What:** Test clients record every operation's call and return step and output. `github.com/anishathalye/porcupine` checks the history against a sequential key-value model, partitioned by key.
- **Why:** Linearizability is the correctness contract clients rely on, and it cannot be checked by looking at the log alone (it involves real-time ordering of client calls and replies). Porcupine is a small, well-known, pure Go checker, used by MIT 6.5840 labs, and fast when the history is partitioned by key.
- **Rejected:** Writing our own checker (easy to get subtly wrong); Jepsen/Knossos (JVM, heavier, better for later real-process chaos testing); checking only state machine equality (misses stale reads and lost acknowledgements).
- **Tradeoff:** A third-party test dependency. Checking time can grow exponentially with highly concurrent histories, so test histories stay small (a few clients, a few keys). Operations that never return are modeled with an unknown output and a return time after everything else.

## 9. Snapshots and log compaction deferred

- **What:** Replicas keep the full log in memory and in storage. There are no snapshots, and catch-up always replays entries from the log.
- **Why:** Keeps Phase 2 focused on the replication protocol. Snapshots interact with catch-up, restart, and the dedup table (which must be part of the snapshot), and are easier to add once the storage layer (SQLite) exists.
- **Rejected:** Snapshotting now (more code and more states to test before the core is proven); truncating the log without snapshots (a lagging replica could never catch up).
- **Tradeoff:** Memory and storage grow without bound, and a restarted replica replays the whole history. Fine for tests; must be fixed before long-running deployments.

## 10. CI runs the race detector on short seed counts and the full seeds without it

- **What / Why:** The race detector made the 1000-seed suites about 9x slower (about 160 s versus 17 s for internal/paxos) even though the simulator is single-threaded, so CI runs `go test -race -short` (100 seeds) and the full 1000 seeds without `-race` as parallel jobs, keeping both race coverage of all code paths and the full seed sweep at a fraction of the wall time.

## 11. modernc.org/sqlite instead of mattn/go-sqlite3

- **What:** The SQLite backend uses `modernc.org/sqlite`, a pure Go translation of SQLite, now at v1.60.1 (it was held at v1.44.3, the newest release that built with Go 1.24, until the move to Go 1.26.8).
- **Why:** No cgo: builds and cross-compiles with the plain Go toolchain, works with `-race` and in minimal CI images without a C compiler, and keeps static binaries simple for later chaos tests on real processes. It is the same SQLite code base, so file format, pragmas and durability semantics match.
- **Rejected:** `mattn/go-sqlite3` (the most widely used driver and somewhat faster, but needs cgo and a C toolchain everywhere, slows builds, and complicates cross-compiling); `ncruces/go-sqlite3` (pure Go via WebAssembly, promising but younger).
- **Tradeoff:** modernc is slower on CPU-heavy queries and adds a large dependency tree. Our workload is a few small writes per consensus step, dominated by fsync, so CPU cost does not matter yet. Newer modernc releases need a newer Go, so upgrading the driver will mean upgrading Go.

## 12. synchronous=FULL, not NORMAL, with WAL

- **What:** Every database opens with `journal_mode=WAL` and `synchronous=FULL`. Each state change (a promise, an accepted entry, a batch of committed entries plus the commit index, the proposer round) is one transaction, and the node replies only after that transaction commits.
- **Why:** A Paxos promise or vote is a statement to other nodes about the acceptor's future behavior. Once sent, it must survive any crash, including power loss. In WAL mode, `synchronous=NORMAL` does not fsync the WAL on every commit, only at checkpoints, so after power loss the most recent commits can vanish even though the node already replied. A node could then forget a promise or an accepted value it reported, which is exactly the "forgetful acceptor" that breaks agreement (see the Phase 1 and Phase 3 negative tests). FULL fsyncs the WAL on every commit, so a reply is never ahead of the disk.
- **Rejected:** `synchronous=NORMAL` (about 4x to 9x faster per write in our benchmark, durable across process crashes but not power loss); `synchronous=EXTRA` (also syncs the directory after deleting a rollback journal; adds nothing in WAL mode); rollback journal mode (also safe with FULL, but more fsyncs per transaction and readers block writers).
- **Tradeoff:** Each persisted change costs an fsync, about 0.3 ms in our sandbox benchmark versus about 0.04 to 0.07 ms with NORMAL, and far more on slow disks. That bounds per-node throughput until writes are batched (group commit), which is left for later.

## 13. One event-loop goroutine per replica instead of mutexes

- **What:** Each node runs its replica on a single goroutine. gRPC handlers, the ticker and incoming peer messages send events to it over channels; client handlers wait on a reply channel. The Paxos code has no locks.
- **Why:** The replica was written as a single-threaded state machine for the simulator, and this keeps it that way in production: the same code, the same order of events, no data races by construction. Reasoning about Paxos invariants is much easier when no other goroutine can change state halfway through a handler. The race detector stays quiet because nothing is shared.
- **Rejected:** A mutex around the replica (works, but every handler must remember to lock, long operations such as fsync hold the lock while other events queue up anyway, and it is easy to call back into the replica while holding the lock); fine-grained locks per role (more concurrency than the protocol needs and many more ways to deadlock or break invariants).
- **Tradeoff:** All work for a node, including each fsync, is serialized on one goroutine, which caps per-node throughput. Batching (group commit) can be added inside the loop later without changing the model. A slow event (a large catch-up) delays heartbeats for its duration.

## 14. Protobuf kept out of the Paxos core

- **What:** `internal/paxos` still uses plain Go structs. `internal/wire` converts them to and from `proto/paxos/v1` at the transport boundary.
- **Why:** The core stays easy to test (comparable structs, no generated code, deterministic simulation unchanged) and the wire format can evolve without touching consensus logic. A fuzz test round-trips every message type through bytes, so the conversion cannot silently drop a field.
- **Rejected:** Using the generated protobuf types directly in the core (no conversion code, but generated structs are not comparable with `==`, carry internal state, and would tie the simulator and every test to protobuf).
- **Tradeoff:** One conversion per message in each direction, and a second definition of every message to keep in sync. The fuzz test and the check that every `oneof` case is covered guard the sync.

## 15. Tick duration of 10 ms by default

- **What:** The node binary advances one logical Paxos tick every 10 ms (`-tick`). With the replica's timing in ticks, that means a heartbeat every 40 ms, an election timeout drawn from 200 to 400 ms, and a peer RPC deadline of 200 ms.
- **Why:** The election timeout must be comfortably above heartbeat interval plus network round trip plus an fsync (about 0.3 ms here), or followers start needless elections. It should also be short enough that failover is quick: in the multi-process test, operations issued after the leader is killed complete about 0.3 to 0.4 s later. Keeping timing in ticks means the same values are used by the simulator and by real nodes, and only the tick length changes per deployment.
- **Rejected:** 1 ms ticks (faster failover, but the ticker wakes the loop 1000 times a second and timeouts approach scheduling noise on a busy machine); 100 ms ticks (calmer, but a leader failure stalls writes for 2 to 4 seconds).
- **Tradeoff:** Across data centers or on slow disks the default is too aggressive and must be raised with `-tick`. Tick length is not adaptive.

## 16. Insecure gRPC for now

- **What:** Peers and clients use plaintext gRPC (`insecure.NewCredentials()`), with no authentication.
- **Why:** Keeps Phase 4 focused on the transport, the event loop and the SDK. Everything runs on localhost in tests.
- **Rejected:** TLS from the start (certificate generation and rotation in every test and in local runs, for no benefit yet).
- **Tradeoff:** Anyone who can reach a node's port can send it Paxos messages or client requests, and traffic is readable on the network. Not deployable outside a trusted network until mTLS lands (see Future work).

## 17. Spanner-style two-phase commit over Paxos groups

- **What:** Cross-shard transactions use two-phase commit in which one participant shard acts as coordinator, every prepare record is written through its participant shard's Paxos log before the participant votes, and the coordinator's decision is written through the coordinator shard's log before anyone is told. Participants that stay prepared too long query the coordinator shard; a coordinator shard with no decision and no coordination in progress presumes abort.
- **Why:** Plain 2PC blocks when the coordinator fails after collecting votes. With every piece of 2PC state replicated, a single machine failure loses nothing: a new leader of the same shard picks up from the log. Using a participant as coordinator avoids a separate coordinator service and saves a round trip for the coordinator's own part.
- **Rejected:** Plain 2PC with a single-node coordinator (blocking); three-phase commit (non-blocking only under synchronous network assumptions, and more rounds); Paxos Commit with a separate acceptor set per transaction (elegant, but more messages and more code than reusing the shard groups we already have); Calvin-style deterministic ordering (no 2PC at all, but needs read and write sets up front, which rules out interactive transactions).
- **Tradeoff:** Each cross-shard commit costs a Paxos round on every participant plus one on the coordinator, on top of the 2PC messages. A transaction can still wait while a shard has no leader, but no longer than an election.

## 18. Wound-wait for deadlocks

- **What:** Lock conflicts are resolved by start timestamp: an older requester wounds (aborts) a younger holder, a younger requester waits. A wounded transaction retries with its original timestamp. A prepared holder is wounded by asking its coordinator to abort it if undecided.
- **Why:** Prevents deadlock without detecting it: waits only point from younger to older, so no cycle can form, and a retried transaction keeps its age, so it cannot starve. It works across shards with no global view.
- **Rejected:** Wait-die (younger requesters die instead of waiting; also deadlock-free, but aborts more often, because a young transaction dies every time it meets an older lock even when it would have been released soon); deadlock detection with a waits-for graph (fewest aborts, but needs a global graph across shards or periodic probing, which is a distributed protocol of its own); timeouts only (simple, but either slow to resolve deadlocks or aborts transactions that were merely slow).
- **Tradeoff:** A younger transaction may be wounded even when no deadlock exists. Long-running transactions that start early are favored. Wounding a prepared transaction needs an extra message to its coordinator.

## 19. Interactive transactions instead of one-shot

- **What:** Clients run Begin, any number of Reads, buffered Writes, then Commit or Abort. Reads take shared locks as they go.
- **Why:** This is the programming model people expect: read a balance, decide, then write. Write sets need not be known in advance.
- **Rejected:** One-shot transactions with read and write sets declared up front (one round trip, and they allow deterministic scheduling such as Calvin, but they push complexity to the application and cannot express read-then-decide logic).
- **Tradeoff:** Locks are held across client round trips, so slow clients hold locks longer, and a client that disappears holds them until the lock lease expires. More round trips per transaction.

## 20. Leader-only read locks

- **What:** Shared locks taken by reads before prepare are kept only in the shard leader's memory, not written to the log. At prepare time they become durable as part of the prepare record. If the leader that granted them is replaced, they are lost and the transaction aborts when it tries to prepare. Separately, the state machine re-validates every read version at apply time, so a lost lock can never let a stale read commit.
- **Why:** Writing every read lock through Paxos would cost a consensus round per read. Reads are frequent and leader changes are rare.
- **Rejected:** Replicating read locks through the log (survives failover, but makes every read as expensive as a write); optimistic reads without locks (no lock overhead, but conflicts are found only at commit time, so long transactions under contention abort much more).
- **Tradeoff:** Every transaction in flight during a leader change aborts and must retry. A deposed leader that does not know it was replaced can still grant read locks, which is why the version check in the state machine is required for safety.

## 21. Static sharding; resharding deferred

- **What:** A key's shard is its hash modulo the number of shards, fixed at startup.
- **Why:** Simple and deterministic, and enough to build and test cross-shard transactions.
- **Rejected:** Range sharding with a shard map service (supports splits, moves and range scans, but needs a replicated directory and a protocol to move data while transactions run); consistent hashing with virtual nodes (cheaper rebalancing, same need for data movement).
- **Tradeoff:** The number of shards cannot change without rewriting every key's location, hot keys cannot be split off, and range scans touch every shard. See Future work.

## 22. One replica of every shard per process

- **What:** Each `paxosd` process hosts one replica of every shard listed in the cluster config file. With 3 nodes and 3 shards, every shard is a Paxos group of the same 3 processes, and each process runs 3 replicas. Each shard replica has its own event-loop goroutine (entry 13 applied per shard) and its own transaction server. All shards share one gRPC server and one connection per peer; every peer message carries the shard it belongs to. Two-phase commit messages between shards on the same process skip the network and go to the other shard's inbox.
- **Why:** It is the simplest layout that still has independent leaders per shard: elections are per shard, so leadership spreads across processes and one slow shard does not stall another. Three processes give every shard a majority-of-3 group, so killing any one process leaves every shard available. The config file stays small (shard count plus node addresses) and every process has the same view.
- **Rejected:** One process per shard replica (9 processes for 3 shards: more ports, more connections, more to deploy, no benefit on one machine); a subset of nodes per shard (placement is what lets a cluster grow beyond 3 machines, but it needs a placement policy and a richer config, and it belongs with resharding, see Future work); one goroutine for all shards on a node (fewer goroutines, but an fsync on one shard would delay every other shard).
- **Tradeoff:** Killing one process takes a replica out of every shard at once, and often the leaders of several shards (the multi-process test shows a single kill moving up to all three). Load per process grows with the number of shards. Adding a node means every shard grows by a replica.

## 23. One SQLite file per shard

- **What:** A node keeps each shard's Paxos state (promises, accepted entries, the committed log) in its own SQLite file, `node-<ID>-shard-<S>.db`, in the node's data directory. The schema and the durability settings are those of entries 11 and 12.
- **Why:** Shards are independent Paxos groups, and with separate files they share nothing on disk: each shard's event loop owns its own connection, so the "one connection per database" rule (writes serialized, pragmas always applied) still holds without a shared lock, and the storage code is unchanged from Phase 3. A shard can be inspected, copied or deleted on its own. Each file fsyncs independently, so a busy shard does not queue behind another's writes inside SQLite.
- **Rejected:** One file with a shard column in every table (one fsync could cover several shards, but the shards' loops would contend for one writer connection, and a schema change would touch every query); one connection per node with each shard's file attached (separate files, but the single connection serializes every shard's writes again).
- **Tradeoff:** More open files and more fsyncs per process: a transaction that touches 3 shards fsyncs 3 files on every replica. Group commit across shards is not possible.

## 24. A Txn service with leader routing in the SDK, replacing the KV service

- **What:** The client API is the `Txn` gRPC service: `Begin`, `Read`, `Write`, `Commit`, `Abort`. `Begin` (any node) returns a random 64-bit transaction ID, a start timestamp and the shard count. `Read` and `Write` go to the leader of the key's shard, and `Commit` goes to the leader of the lowest shard in the transaction, which coordinates. The SDK caches one leader per shard, follows "not leader" hints, and backs off between failed attempts. Write values are buffered in the SDK and sent with `Commit`; the `Write` RPC only tells the shard leader, which renews the transaction's locks and reports if it was already wounded. The Phase 4 `KV` service (`Get`, `Put`) and its SDK are removed.
- **Why:** Routing in the SDK saves a forwarding hop on every request. Sending the whole read and write set with `Commit` keeps the commit request self-contained, so a retry to a new leader works without any state from the old one; that is what makes retrying a commit after a timeout safe. Random IDs are unique across nodes and restarts without coordination. The KV service ran one Paxos group with a key-value state machine; with every group now running the transaction state machine, a separate KV path would be a second store. A single-key read or write is simply a one-shard transaction.
- **Rejected:** Server-side routing, where any node forwards to the leader (simpler SDK, but an extra hop and more server state); buffering writes at the shard leaders (the commit request would be smaller, but a leader change would lose them and the request would not be self-contained); keeping the KV service as one-key transactions on the server (more code for no new capability).
- **Tradeoff:** A transfer costs 5 client round trips (2 reads, 2 writes, commit) where 3 would do; the `Write` RPC buys only an early abort. The SDK needs the shard function (`api.ShardOf`, public so programs outside the module can use the SDK), so changing it means updating clients. Phase 4's single-group linearizability test is replaced by the strict serializability check of the multi-process transaction test.

## 25. Unknown commit outcomes are reported, never guessed

- **What:** `Commit` returns committed, aborted, or unknown. The SDK retries the same commit request (same transaction ID) after errors and at new leaders, and the servers answer a repeated commit from the log, so a retry never commits twice. If the caller's context ends while some attempt may have reached a coordinator, `Commit` returns `ErrUnknown`. `Run` retries aborted transactions with backoff up to a maximum number of attempts, but returns an unknown outcome to the caller instead of retrying. On the server, every "aborted" answer comes from the log: a coordinator reports its logged decision, and a one-phase commit that cannot get its locks writes an abort record before replying (docs/transactions.md, One-phase commit).
- **Why:** After a timeout the client cannot tell whether the commit happened, and guessing either way is wrong: reporting "aborted" and re-running would apply a transfer twice, and reporting "committed" could lose a write. Only the log knows. The multi-process test checks this end to end: every committed answer is committed in the shards' logs, every aborted answer is not, and unknown ones are counted as whatever the logs say.
- **Rejected:** Blocking until the outcome is known (no unknowns, but a client could wait through any number of failovers, and deadlines are part of the API); retrying unknown transactions inside `Run` (only safe if the transaction is idempotent, which the SDK cannot know); a per-client dedup table for transactions, as the KV service had (unnecessary, since the transaction ID already identifies the commit).
- **Tradeoff:** Callers must handle a third outcome, for example by reading back what they wrote. A one-phase transaction that fails at the leader costs a log write for its abort record.

## 26. OpenTelemetry for traces, Prometheus for metrics, slog for logs

- **What:** Traces use the OpenTelemetry Go SDK with the OTLP/gRPC exporter and the otelgrpc instrumentation; metrics use the Prometheus client library with a `/metrics` endpoint on its own port; logs use the standard library's `log/slog` with a JSON handler. The SDK takes any OpenTelemetry `TracerProvider`.
- **Why:** OpenTelemetry is the vendor-neutral standard: OTLP is accepted by Jaeger, Tempo, Honeycomb, Datadog and the OpenTelemetry Collector, so the backend is a deployment choice, not a code change. W3C trace context is the propagation format everyone reads, which is what lets a client, every shard and every node add to one trace. Prometheus is the de facto pull format for metrics and what the Grafana dashboard reads; pulling also means a node does no work for metrics nobody scrapes. `slog` is in the standard library, structured, and cheap when a level is off.
- **Rejected:** Vendor SDKs (Datadog, New Relic and others: good tooling, but they tie the code to one backend and each propagates context its own way); OpenTelemetry for metrics too (one SDK for both, but the Prometheus client is simpler and more mature for a pull endpoint, and exporting OTel metrics to Prometheus adds a translation layer); zap or zerolog for logs (faster in benchmarks, but logging is not on the hot path here except at debug level, and they are extra dependencies).
- **Tradeoff:** Two telemetry libraries instead of one, and the module now depends on the OpenTelemetry and Prometheus packages (the SDK depends on OpenTelemetry's API and otelgrpc). OpenTelemetry's Go packages move quickly; versions are pinned in go.mod and are bumped together with Go when govulncheck flags them (as in the move to Go 1.26.8).

## 27. The cores emit events; only the server adds telemetry

- **What:** `internal/paxos` and `internal/txn` define `Observer` interfaces and call them at the points that matter (leader elected or stepped down, entry proposed or applied; transaction decided with a reason, two-phase commit phase boundaries, outcome query, wound, lock wait). The observer gets no time and returns nothing. The server implements both per shard replica, on the shard's event loop: it reads the clock, records histograms and counters, starts and ends spans, and logs. The simulator passes no observer.
- **Why:** The cores' determinism is what makes every seeded test replayable, and it rests on them never reading the clock or depending on anything outside their inputs. An observer that only receives calls cannot change their behavior, and a test replays seeds with observers attached and compares trace hashes to prove it. Keeping OpenTelemetry and Prometheus out of the cores also keeps them small and their tests fast, and lets the telemetry stack change without touching consensus code.
- **Rejected:** Instrumenting the cores directly with OpenTelemetry and Prometheus (fewer layers, but wall-clock reads and global registries inside deterministic code, and every simulator test would carry the libraries); deriving everything in the server from outside the cores, for example by diffing state after each event (no interface to maintain, but the server cannot see why an abort happened, or when a phase began, without re-implementing the protocol).
- **Tradeoff:** Each new metric that needs a new kind of event changes a core interface. Timings are taken when the event reaches the observer on the event loop, so they include time the event spent queued behind other work on that loop.

## 28. Tracing off by default; 1% sampling for new traces when on

- **What:** `paxosd` traces only with `-otlp-endpoint` set. It then samples with ParentBased(TraceIDRatioBased(`-trace-sample`)), default 0.01. A trace the client started follows the client's decision, so the SDK's sampler decides for every transaction it runs. Only Txn RPCs get gRPC spans; peer RPCs (heartbeats, Paxos, two-phase commit messages) do not.
- **Why:** Off by default because a tracer with nowhere to send spans only costs CPU. When on, parent-based sampling keeps traces whole: a transaction is either traced on the client, every shard and every node, or nowhere, and the decision is made once, where the transaction starts. 1% of new traces bounds export volume at high throughput while still catching slow requests over minutes. The benchmark shows about 4% throughput cost at full sampling on a disk-bound cluster (docs/observability.md), so 1.0 is affordable for debugging. Peer messages are excluded because each transaction causes dozens of them; their work appears as the transaction's two-phase commit and Paxos round spans.
- **Rejected:** Always-on full sampling (complete data, but export volume grows with throughput); tail-based sampling, which keeps slow or failed traces after the fact (better at catching outliers, but needs a collector that buffers whole traces, which is a deployment concern for later); per-node independent sampling (simpler, but a transaction would be traced on some nodes and not others).
- **Tradeoff:** At the default rate most transactions leave no trace, so a rare slow one may be missed unless the client samples more. Server-side root spans only exist for clients that do not send trace context.

## 29. Metric labels are bounded: no transaction ID, key or client ID

- **What:** A metric label may only take values from a small fixed set. The store's metrics carry `shard` and `node` (values from the cluster config); `txn_aborts_total` adds `reason` (the fixed set `txn.AbortReasons`). The standard Go runtime and process collectors are registered, adding only `quantile` (fixed GC duration quantiles) and `version` (on `go_info`). Values that grow with traffic, such as a transaction ID, a key or a client ID, are never labels; they go on spans as attributes. `TestMetricLabels` checks every exported metric against this allowlist, and `TestCheckLabelsRejects` shows it refuses the unbounded ones.
- **Why:** Each distinct label combination is a separate time series in Prometheus, kept in memory and on disk. One transaction ID label would add a series per transaction and take down the monitoring system long before the store. The danger is unboundedness, not label count: a reason label with seven values costs seven series per shard and node, and is the usual Prometheus way to express a category, so one `txn_aborts_total` with `sum by (reason)` replaces a counter per reason. An allowlist with fixed value sets, enforced by a test, keeps that from creeping.
- **Rejected:** Only `shard` and `node`, with a metric per abort reason (the Phase 7 first draft: strictly simpler, but unconventional, a new reason was a new metric, and it ruled out the standard runtime collectors because of `go_info`'s `version`); free-form labels reviewed case by case (flexible, but cardinality problems are found in production, not in review); exemplars carrying trace IDs on histograms (they link a latency bucket to a trace without adding series, and are worth adding when a backend that shows them is in use).
- **Tradeoff:** A new label, or a new value for one, means updating the allowlist in the test. Questions about a single transaction or key cannot be answered from metrics; that is what traces and logs are for.

## 30. Distroless static image, nonroot

- **What:** The `paxosd` image is a static binary (`CGO_ENABLED=0`, possible because the SQLite driver is pure Go) on `gcr.io/distroless/static-debian12:nonroot`, built in a multi-stage Dockerfile. It runs as UID 65532, keeps its SQLite files on a volume at `/data`, and stops on SIGTERM. The healthcheck is `paxosd -probe`, because the image has no shell or curl.
- **Why:** The runtime image holds only the binary, CA certificates and a passwd entry: about 33 MB unpacked, almost all of it the binary. No shell, package manager or libc means far less to patch and nothing for an attacker who gets code execution to use. Running as nonroot limits what a compromised process can do to the volume and the host. Static linking removes any libc version coupling between build and runtime images.
- **Rejected:** Alpine (small and has a shell for debugging, but musl and a package manager to keep patched, and a shell is the first thing an attacker wants); `scratch` (smaller still, but no CA bundle, no `/etc/passwd` for a nonroot user and no timezone data, all of which distroless provides); a Debian slim base (familiar tools, ten times the size and the patch surface).
- **Tradeoff:** No shell in the container: debugging uses `docker cp`, the metrics and health endpoints, or an ephemeral sidecar (as the chaos runner does for `tc`). The image cannot be built with cgo if a future dependency needs it.

## 31. tc netem in a sidecar for network faults, not Toxiproxy

- **What:** The chaos runner injects delay, loss and partitions with Linux `tc netem` on a node's own network interface, from a throwaway sidecar container that shares the node's network namespace and has `NET_ADMIN`. Partitions are 100% loss. Where the kernel lacks netem, partitions fall back to `iptables` and delay and loss are skipped.
- **Why:** netem acts on every packet the node sends, to peers and clients alike, below the application, so the system cannot tell an injected fault from a real one, and no address or port in the cluster config changes. The sidecar keeps `NET_ADMIN` and the tools out of the production image. It needs nothing but Docker and a stock kernel, which GitHub's runners have.
- **Rejected:** Toxiproxy (a TCP proxy between nodes: precise per-link faults and an HTTP API, but every peer address must point at a proxy, so the test cluster differs from the real one; it only sees TCP streams, so it cannot drop individual packets; and it is one more moving part that can itself fail); Pumba (wraps the same netem, but adds a dependency for little the runner does not do in a few lines); iptables only (fine for partitions, but cannot add delay or random loss).
- **Tradeoff:** Faults are per node, not per link, so asymmetric partitions (A hears B but not C) are not covered yet. netem shapes only outgoing packets, so delay is one way per node. It needs Linux and a kernel with `sch_netem`, which some sandboxes, including the development one, lack.

## 32. GitHub Container Registry, tagged by commit SHA

- **What:** On every push to main, `.github/workflows/release.yml` builds the image and pushes it to `ghcr.io/<owner>/paxos-txn-store:<commit SHA>`, logging in with the workflow's own `GITHUB_TOKEN`.
- **Why:** The registry lives next to the code: no extra account or long-lived credential, since the token is scoped to the run and granted `packages: write` only in that workflow; the package links back to the repository and inherits its access. An immutable SHA tag says exactly which commit an image is, so a deploy or a bug report can name it and nobody can retag it later.
- **Rejected:** Docker Hub (the default for public images, but a separate account, a stored access token, and pull rate limits); a cloud registry such as Amazon ECR (it will make sense once there is a cloud deployment, which is the next phase, not this one); a mutable `latest` tag (convenient, but it hides which build is running).
- **Tradeoff:** Only main is published, so a branch image has to be built locally. SHA tags are not human-friendly; release tags (semver) can be added later on top.

## 33. A 2-minute chaos run on pull requests, 20 minutes nightly

- **What:** The chaos workflow runs for 2 minutes on every pull request and for 20 minutes every night at 03:17 UTC; it can also be started by hand with any duration. Both runs use the same faults and checks.
- **Why:** Two minutes is about 15 to 25 faults, enough to hit every fault kind and several leader failovers, and keeps the pull request job (image build, stack start, run, checks) under about 5 minutes, so it is not skipped or resented. Rarer interleavings need more faults than any one review can wait for; the nightly run gives ten times as many, on the current main, every day. A failure in either uploads the history, the node logs and the data for a replay.
- **Rejected:** Chaos only nightly (cheap, but a regression is found a day late and is no longer tied to a pull request); a long run on every pull request (more coverage per change, but tens of minutes per push); a fixed fault schedule (reproducible, but it explores nothing new; the runner logs its seed instead, and `-seed` reruns the same sequence of faults, nodes and durations, though not the same interleaving with the workload).
- **Tradeoff:** A short random run can miss a bug that the nightly run then finds, after merge. Random schedules also mean a failure may not reproduce on rerun; the uploaded history and data are what makes it debuggable.

## 34. govulncheck stays blocking, with one tracked exclusion

- **Update (Phase 8b):** the exclusion is gone. On 2026-10-08 govulncheck stopped reporting GO-2026-6443 for grpc v1.84.0 (no new grpc release; the vulnerability database entry changed), the script failed as designed, and `ALLOWED` is now empty. The script stays, so a future exclusion gets the same tripwire. The record below is kept as it was.
- **Update (Phase 8b, later):** on 2026-10-08 the Go project published eleven standard library advisories (GO-2026-6599 to 6617, fixed in go1.26.9; four of them, 6603, 6611, 6612 and 6617, also affect golang.org/x/net/http2 and needed x/net v0.60.0). PR #9 passed govulncheck on its push run and failed on its pull_request run five minutes later with no code change, because the advisories appeared in between. The fix was the Go 1.26.9 upgrade plus x/net v0.60.0. To keep such surprises off unrelated pull requests: `vulncheck.yml` runs the same check on main daily, so a new advisory against merged code shows up as a failed scheduled run, and `.github/dependabot.yml` opens weekly pull requests for Go modules, GitHub Actions, and the Dockerfile and compose images (the Dockerfile's `FROM` lines are now literal so Dependabot can read them). The govulncheck job in `ci` stays blocking.
- **What:** CI runs govulncheck through `scripts/govulncheck.sh`, which fails on any vulnerability reachable from our code except GO-2026-6443, and also fails if GO-2026-6443 stops being reported. GO-2026-6443: a grpc server panics on a request that carries neither an `:authority` nor a `Host` header; our server reaches it through `server.Start` and `grpc.Server.Serve`. It is fixed only on grpc master (v1.85.0-dev.0.20260825072537-93e31b48545e); v1.84.0, the newest release, still has it. **Remove when grpc v1.85.0 ships:** bump grpc, delete the ID from the script and delete this entry.
- **Why:** The risk is low. Every caller of our servers is a grpc-go client (peers through `internal/grpcnet`, users through the SDK), and grpc-go always sends `:authority`, so only a hand-crafted HTTP/2 request can trigger the panic. Nodes are not publicly reachable: the compose stack publishes its ports on 127.0.0.1 only, and a deployment keeps the gRPC ports on a private network. Excluding exactly one ID keeps the check blocking for everything else, and failing once it disappears means the exclusion cannot outlive the fix.
- **Rejected:** Pinning grpc to the unreleased master commit (passes the check with no exclusion, but runs untested, unreleased code on the consensus path); leaving govulncheck red or making it advisory until v1.85.0 (a red check that everyone learns to ignore hides the next finding); a general ignore list (exclusions would accumulate without anyone noticing they became stale).
- **Tradeoff:** Until grpc v1.85.0 a node can be crashed by anyone who can open an HTTP/2 connection to its gRPC port; a crash is a fault the protocol already tolerates (a restart, as in the chaos runs), not a safety problem, but it is an availability risk if a port is ever exposed. The script parses govulncheck's JSON output, so a change to that format would need a script change.

## 35. Election backoff: double the timeout after an election that produced no leader

- **What:** When a replica's election timer runs out and an election it started since it last saw a steady leader produced none (it timed out as candidate, or a rival's higher Prepare cut it short), it doubles its next election timeout range, keeping the randomization: `backoff` times `[ElectionMin, ElectionMax]`, with `backoff` capped at `ElectionBackoff` (default 8, so 1.6 to 3.2 s at the 10 ms tick). The backoff returns to 1 once the same leader ballot has held for a whole backed-off election timeout, checked on each heartbeat a follower accepts and each one a leader sends.
- **Why:** Election timeouts are fixed in ticks, but an election round costs a round trip plus an fsync on the candidate and on each acceptor. When the machine is slow enough that a round takes longer than the timeout (a starved CPU under `-race` in CI, a stalled disk), every candidate gives up and starts a higher ballot before its promises arrive, and the group never elects a leader. This was found while chasing a CI failure of `TestConcurrentIncrements` (a commit stalled for 60 s under `-race`; its last errors were "does not lead shard"), but it is not that failure's whole cause: six race-instrumented copies of that test on one CPU still fail with the backoff, also with the cap raised to 64, and pass with a longer test-cluster tick, so that overload is driven by the tick (entry 36). The simulator now has a slow node fault (`transport.Slow`), and `TestMPSlowNodesElectLeader` reproduces the livelock deterministically (no leader in 3000 steps on the old code) and passes with the backoff (every client done within 805 steps over 200 seeds). Doubling finds a timeout longer than the round in a few elections whatever the slowdown (up to the cap), and costs nothing in steady state, where elections succeed at the first try. The reset waits for a steady leader instead of its first heartbeat because Prepares sent by rivals before it won still depose it; resetting at once sent every replica back to the short timeout and the livelock continued (seed 3 never elected a leader that held).
- **Rejected:** Longer fixed timeouts (slower failover for everyone to protect against an occasional slow period, and still a livelock past the new bound); a timeout measured in wall-clock time from observed round trips (adaptive, but the replica has no clock and the simulator would need one; ticks keep it deterministic); resetting the backoff on any leader contact (simplest, and what the first version did, but it does not end the livelock, as above); Raft-style pre-vote (avoids disruptions by partitioned nodes, a different problem, and does not help when every round is too slow).
- **Tradeoff:** After a slow period, failover is slower until the backoff ends: up to `ElectionBackoff` times the normal timeout, and a leader that crashes before it has held for a backed-off timeout leaves its followers waiting that long. A round slower than the cap times `ElectionMax` still livelocks; the cap bounds how long a healthy cluster can wait instead. Steady-state behavior is unchanged: the multi-process failover times at 10 ms and 20 ms ticks are within run-to-run noise of the old code (docs/running.md, below the failover table).

## 36. A longer test-cluster tick under -race

- **What:** `internal/testcluster` uses a 5 ms tick normally and 60 ms when built with `-race` (a `race` build-tag constant picks it). Production defaults (`server.DefaultTick`, 10 ms) are unchanged. `internal/chaos`'s real-cluster test is skipped under `-short`; the full suite and the chaos workflow cover it.
- **Why:** `TestConcurrentIncrements` failed in race-short on CI (2 of the last 3 runs on the Phase 8a branch), with shards repeatedly left without a leader. Six race-instrumented copies of it pinned to one CPU reproduce that every time at 5 ms and at 10 ms. The election backoff (entry 35) and a backoff cap of 64 do not help, and a 2 s peer RPC deadline does not either, while a longer tick does: 20 ms passed 1 of 18 runs, 35 ms 11 of 18, 50 ms 17 of 18, and 60 ms 36 of 36 (75 ms 18 of 18). Under `-race` every node runs several times slower, and with a short tick the heartbeats, retransmissions and timers of nine replicas are themselves enough work to starve the runner, so heartbeats arrive late and followers depose leaders that were working. Phase 8a made this more likely by adding a race-instrumented real-cluster test that runs in parallel with the others.
- **Update (Phase 8b): the repro is throughput-bound, not a liveness failure.** With leader stickiness (entry 37), leaders stay in place under the repro. At a 20 ms race tick, the samples in which some shard had no known leader fell from 34 of 115 (30%) to 5 of 114 (4%), and elections per run from 4.9 to 3.6 (3 is the startup minimum). It still fails at short ticks because each Paxos round takes about 1 s on the starved CPU: committed transactions per run were 2.4 before and 1.5 after, far from the 20 contended cross-shard increments the test needs in 60 s. That is overload, which no election rule can fix, so the race tick stays at 60 ms.
- **Rejected:** Fewer parallel packages in race-short (`-p 2`: reduces contention for everyone, but slower CI and the test stays one busy neighbor away from failing); a longer tick in every build (slows the non-race suites, which do not need it); retrying the test (hides the problem).
- **Tradeoff:** Race-short exercises the servers at a coarser tick than production, so a timing bug that only shows at short ticks can slip past it; the non-race suites still run at 5 ms. The servers' race-short tests take longer (internal/server about 26 s instead of 9 s locally). The underlying weakness, followers deposing a working leader whose heartbeats are late, is addressed by entry 37, but the repro still needs the longer tick.

## 37. Leader stickiness: PreVote, refusing Prepares while a leader is live, and CheckQuorum

- **What:**
  - **PreVote:** when its timer runs out, a replica first sends `PreVote` with the ballot it would use and runs a real Prepare only after a majority (itself included) grants it. A replica answers a PreVote without changing or persisting anything.
  - **Stickiness:** a replica refuses a PreVote, and ignores a Prepare from another replica, while it leads or has heard from a leader within `ElectionMin` ticks.
  - **CheckQuorum:** followers acknowledge heartbeats (`HeartbeatAck`). A leader that has not heard from a majority (heartbeat acks or Accepted votes) within its election timeout, `ElectionMax` times the backoff it was elected at, steps down.
  - **Backoff:** a pre-vote that times out unanswered still counts as a failed election for the backoff (entry 35); one refused by a peer that still hears a leader does not, since that leader may have just failed.
- **Why:** Before this, any replica whose timer ran out raised its ballot and could depose a working leader: a follower that missed a few heartbeats under load, or one that sat in a partition raising its ballot every timeout and then came back. Each such election costs a round of writes and a burst of client retries, which adds load and makes the next one more likely.
  - **Rejoin:** a follower cut off for 400 steps and let back in now causes 0 elections over 200 seeds (`TestMPRejoinDoesNotDepose`). Without stickiness it deposes the leader in 200 of 200 (`TestMPRejoinDeposesWithoutStickiness`).
  - **Election counts:** over 200 seeds, the random chaos schedules (crashes, partitions, slow nodes) need 390 elections instead of 602, and the all-slow schedule needs 200 (one per seed) instead of 588 (`TestMPStickinessElectionCounts`).
  - **CheckQuorum:** a leader cut off from every other replica steps down within 80 steps, against a bound of 90 (`TestMPCheckQuorum`).
  - **Safety:** none of this is needed for safety. These rules only decide when a replica runs Prepare or stops leading, and Paxos stays safe even if every replica runs Prepare at any moment. The 1000-seed safety, linearizability and transaction suites and every negative test still pass.
- **Measured on the stress repro (entry 36):** six race-instrumented copies of `TestConcurrentIncrements` on one CPU.
  - **Leader churn:** at a 20 ms race tick, samples in which some shard had no known leader fell from 34 of 115 to 5 of 114, and elections per run from 4.9 to 3.6 (3 is the minimum).
  - **Pass rate:** not improved. 5 ms: 0 of 18 before and after. 20 ms: 1 of 18 before, 0 of 18 after. 35 ms: 11 of 18 before, 8 of 18 after.
  - **Why it still fails:** with leaders now stable, it is limited by throughput. Each Paxos round takes about a second on the starved CPU, so the 20 contended cross-shard increments do not finish in 60 s. The race tick stays at 60 ms.
- **Rejected:**
  - **Only PreVote:** it stops a returning partitioned replica, but a replica that passes the pre-vote among followers who miss heartbeats can still depose a leader that the others hear.
  - **Only CheckQuorum:** it makes a cut-off leader step down, but does nothing about disruptive candidates.
  - **Leader leases with clock bounds:** these would also allow local reads, but depend on bounded clock drift (Multi-Paxos docs, reads).
  - **Granting a pre-vote only to candidates whose log is at least as long as one's own,** as Raft does: in Paxos, Prepare recovers any accepted entry from the promises, so a candidate with a shorter log is safe, and this restriction would only add a way to refuse.
- **Tradeoff:**
  - **Extra delay:** a candidate needs one more round trip (the pre-vote) before an election, and a replica that heard the old leader slightly later than the candidate refuses until its own `ElectionMin` has passed. Measured back to back on one machine (9 iterations each, 10 ms tick), multi-process failover was 213 to 1002 ms before and 207 to 690 ms after, within run-to-run noise.
  - **Asymmetric partitions:** a leader can be kept in place by followers that hear it even if some clients or replicas cannot reach it; CheckQuorum only removes it when a majority cannot.
  - **Sticky directed tests:** two directed transaction tests depended on how fast the first elections ran. They now wait for every shard to have a settled leader before starting.
  - **One directed test runs without CheckQuorum:** the one-phase retry test cuts a new leader off on purpose and needs it to keep answering the client's retried commit. With CheckQuorum that leader stepped down first in about two thirds of the seeds, and the broken mode was caught in only 353 of 1000. `ReplicaConfig.NoCheckQuorum` turns it off for that test alone, which again catches the broken mode in 1000 of 1000 seeds and passes 1000 of 1000 in the correct mode. Leader liveness is tested separately (`TestMPCheckQuorum`).

## Future work

- **Power-loss testing with LazyFS.** The simulator and the SIGKILL test cover process crashes, where the OS page cache survives. They cannot show what happens when unsynced data in the page cache is lost. LazyFS (a FUSE file system from the Jepsen project that keeps writes in its own cache until fsync and can drop everything unsynced on command) would let a test cut "power" at arbitrary points and check that every acknowledged promise, vote and commit survives. That would test the argument in entry 12 instead of relying on it, and would catch a regression to `synchronous=NORMAL`.
- **Mutual TLS between nodes and for clients.** Each node gets a certificate signed by a cluster CA; peers verify each other's node ID from the certificate, and clients verify the server (optionally with client certificates). Replaces the insecure credentials from entry 16.
- **Resharding.** Move from hash sharding to range sharding with a replicated shard map, and split or move ranges while transactions keep running (for example by preparing a move as a transaction on both the old and new owner).

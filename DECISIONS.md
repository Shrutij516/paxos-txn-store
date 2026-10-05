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

- **What:** The SQLite backend uses `modernc.org/sqlite`, a pure Go translation of SQLite, pinned to v1.44.3 (the newest release that still builds with Go 1.24).
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
- **Tradeoff:** Two telemetry libraries instead of one, and the module now depends on the OpenTelemetry and Prometheus packages (the SDK depends on OpenTelemetry's API and otelgrpc). OpenTelemetry's Go packages move quickly; versions are pinned to ones that build with Go 1.24.

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

## 29. Metric labels are only shard and node

- **What:** Every metric has exactly the labels `shard` and `node`, whose values come from the cluster config. Values that grow with traffic, such as a transaction ID, a key or a client ID, are never labels; they go on spans as attributes. Bounded categories that would otherwise be a label, such as the abort reason, become part of the metric name (`txn_aborts_wounded_total` and so on). `TestMetricLabels` checks every exported metric. The Go runtime collectors are not registered, because `go_info` has a `version` label.
- **Why:** Each distinct label combination is a separate time series in Prometheus, kept in memory and on disk. One transaction ID label would add a series per transaction and take down the monitoring system long before the store. A fixed rule that a test enforces is easier to keep than case-by-case judgment. Shard and node are what an operator slices by: which shard is slow, which node is unhealthy.
- **Rejected:** A `reason` label on one `txn_aborts_total` (bounded too, and the more usual Prometheus style, but it would make the rule "shard, node and sometimes others", which is how cardinality creep starts; the dashboard selects the per-reason counters with one regex instead); exemplars carrying trace IDs on histograms (they link a latency bucket to a trace without adding series, and are worth adding when a backend that shows them is in use).
- **Tradeoff:** Per-reason metric names are less conventional, and a new reason is a new metric. Questions about a single transaction or key cannot be answered from metrics; that is what traces and logs are for.

## Future work

- **Power-loss testing with LazyFS.** The simulator and the SIGKILL test cover process crashes, where the OS page cache survives. They cannot show what happens when unsynced data in the page cache is lost. LazyFS (a FUSE file system from the Jepsen project that keeps writes in its own cache until fsync and can drop everything unsynced on command) would let a test cut "power" at arbitrary points and check that every acknowledged promise, vote and commit survives. That would test the argument in entry 12 instead of relying on it, and would catch a regression to `synchronous=NORMAL`.
- **Mutual TLS between nodes and for clients.** Each node gets a certificate signed by a cluster CA; peers verify each other's node ID from the certificate, and clients verify the server (optionally with client certificates). Replaces the insecure credentials from entry 16.
- **Resharding.** Move from hash sharding to range sharding with a replicated shard map, and split or move ranges while transactions keep running (for example by preparing a move as a transaction on both the old and new owner).

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

## Future work

- **Power-loss testing with LazyFS.** The simulator and the SIGKILL test cover process crashes, where the OS page cache survives. They cannot show what happens when unsynced data in the page cache is lost. LazyFS (a FUSE file system from the Jepsen project that keeps writes in its own cache until fsync and can drop everything unsynced on command) would let a test cut "power" at arbitrary points and check that every acknowledged promise, vote and commit survives. That would test the argument in entry 12 instead of relying on it, and would catch a regression to `synchronous=NORMAL`.

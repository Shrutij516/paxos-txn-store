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

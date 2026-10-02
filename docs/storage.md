# Storage and crash recovery

Phase 3 puts each replica's durable state in a SQLite file (`internal/storage/sqlite.go`). The in-memory store (`internal/storage/memory.go`) implements the same interfaces and passes the same contract tests (`internal/storage/contract_test.go`), so the consensus code does not know which one it is using.

## Settings

- Driver: `modernc.org/sqlite` (pure Go, no cgo). See DECISIONS.md entry 11.
- `journal_mode=WAL`, `synchronous=FULL`, `busy_timeout=5000`. See DECISIONS.md entry 12 for why FULL.
- One connection per database, so the pragmas always apply and writes are serialized.

## Schema (version 1)

| Table | Rows | Holds |
|---|---|---|
| `schema_version` | 1 | the schema version of this file |
| `acceptor` | 0 or 1 | single-decree acceptor state (Phase 1 `Node`): promised ballot, accepted ballot and value |
| `proposer` | 0 or 1 | the highest round this node's proposer has used |
| `log_promise` | 0 or 1 | the Multi-Paxos promise: one ballot that covers every slot |
| `log_accepted` | one per slot | the last entry this acceptor accepted at each slot, with its ballot |
| `log_committed` | one per slot | entries known to be chosen, slots 1 to the commit index |
| `log_commit` | 0 or 1 | the commit index |

Ballots are stored as two integer columns (round, node). Commands are stored as BLOBs because they contain zero bytes.

On open, the store reads `schema_version`. An empty file is migrated to the current version inside a transaction. A file with a newer version than the code knows is refused rather than misread. Future schema changes append a migration step; old files are upgraded in place on open.

## What is written, and when

Every row below is written in its own transaction, and the node sends the matching message only after the transaction commits.

| Event | Written | Then the node may |
|---|---|---|
| Proposer starts an election or a new ballot | `proposer.round` | send Prepare with that ballot |
| Acceptor promises a higher ballot on Prepare or Heartbeat | `log_promise` | send Promise, or follow the new leader |
| Acceptor accepts an entry | `log_accepted` row for the slot, and `log_promise` if the Accept raises the promise, in one transaction | send Accepted |
| Slots become committed (contiguous prefix extends) | new `log_committed` rows and `log_commit`, in one transaction | apply them to the KV store and reply to clients |
| Single-decree acceptor changes state | `acceptor` | send Promise or Accepted |

The commit index never runs ahead of the disk: if the committed write fails, nothing is applied and the next attempt retries the whole batch.

## Recovery on restart

1. Open the file, check the schema version, migrate if needed.
2. Load the proposer round, so the next ballot is higher than any this node used before the crash.
3. Load the promise and every accepted entry, so the acceptor keeps every promise and vote it ever made.
4. Load the committed prefix (slots 1 to the commit index) and replay it, in order, into a fresh KV store. This rebuilds both the data and the dedup table, so a client retry that arrives after the restart still gets its cached result and is not executed twice.
5. Join as a follower. Anything committed after the last durable write arrives through heartbeats and catch-up, as in Phase 2.

There are no snapshots yet, so step 4 replays the whole log. `TestSQLiteRestartReplaysCommittedLog` checks that a node restarted from its file, before receiving any message, has the same commit index, the same applied entries and the same key values as before the crash.

## Why the reply must wait for the durable write

Paxos safety rests on acceptors keeping their word. A Promise says "I will never accept a lower ballot, and here is everything I accepted". An Accepted says "this entry is in my log at this ballot". Other nodes act on those messages immediately: a leader decides an entry is chosen once a majority said Accepted, and a new leader trusts the promises it collected to show every possibly chosen entry.

If a node replies first and crashes before the write reaches disk, it comes back without the promise or vote it already gave. That is the same as the "forgetful acceptor" from Phase 1. A leader can then count a majority for an entry that, after the crash, only a minority remembers, and a later leader can choose a different entry for the same slot.

The tests show both sides:

- `TestSQLiteCrashPoints` crashes nodes right before a write commits (nothing is written and nothing was sent) and right after it commits but before the reply goes out (the state is durable and the reply is lost, which looks like a dropped message). Both stay safe over 100 seeds each, with about 200 injected crashes per mode. In the cluster tests these crashes are injected by a storage wrapper in the test harness (`crashStore`); inside the SQLite store an unexported hook, set only by the storage package's own tests, checks that a fault before commit really rolls the transaction back.
- `TestSQLiteSurvivesSIGKILL` crashes a real process. The test binary re-executes itself as a child that writes in a loop (an accept with its promise, then a committed entry with the commit index) and prints each index only after both writes returned. The parent kills it with SIGKILL at a random point, reopens the file, runs `PRAGMA integrity_check`, and checks that every confirmed index is on disk. It runs 20 times in the full suite and 3 times under `-short`.
- `TestSQLiteNegativeReplyBeforePersist` makes acceptors send Promise and Accepted before writing, with crashes before commit. The checker catches it: 28 of 100 seeds violate log safety, while the same schedules without the bug have 0 violations.
- The `synchronous` setting matters for the same reason. With NORMAL in WAL mode a commit can be lost on power loss after the reply was sent, which is the same failure in a different place. The simulator cannot cut power, so this is argued in DECISIONS.md entry 12 rather than tested.

## Benchmarks

`go test -run xxx -bench . ./internal/storage/` measures one persisted write (one transaction plus fsync) in the sandbox:

| Write | synchronous=FULL | synchronous=NORMAL |
|---|---|---|
| Promise (`SavePromised`) | about 0.33 to 0.42 ms | about 0.038 ms |
| Accept, raising the promise, one transaction (`SaveAccept`) | about 0.28 to 0.32 ms | about 0.065 to 0.072 ms |

Ranges come from three runs of 1000 writes each. Numbers depend heavily on the disk; they are shown here only to make the cost of FULL visible.

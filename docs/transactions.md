# Transactions across shards

Phase 5 adds serializable, interactive transactions across shards. It runs in the deterministic simulator only; Phase 6 will wire it into `paxosd` and the SDK. The code is in `internal/txn`.

## The pieces

- **Shards.** Keys are split across N shards (default 3) by hashing the key. Each shard is its own Multi-Paxos group of 3 replicas, exactly like Phase 2. Resharding is not supported yet (DECISIONS.md, Future work).
- **The shard state machine** (`sm.go`). Each shard's Paxos log holds transaction records instead of plain Gets and Puts: prepare, one-phase commit, coordinator decision, commit, abort. Every replica applies them in the same order, so every replica knows the same committed data, the same prepared transactions and the same outcomes.
- **The shard server** (`server.go`). It runs next to each replica and only acts while that replica leads its shard. It holds locks, runs two-phase commit, and turns decisions into log records.
- **Clients.** A transaction is interactive: `Begin`, then `Read`s that go to the shard leaders one at a time, `Write`s that are only buffered on the client, then `Commit` or `Abort`. Each attempt has a unique transaction ID and a start timestamp. A retried transaction gets a new ID but keeps its original timestamp.

## Two-phase commit, and why plain 2PC blocks

A transaction that touched several shards must commit on all of them or on none. Two-phase commit does that with a coordinator:

1. **Prepare.** The coordinator asks every participant "can you commit?". A participant that says yes promises to be able to commit no matter what: it locks what the transaction read and wrote and records its writes durably.
2. **Decide.** If every participant said yes, the coordinator decides commit; if anyone said no or did not answer in time, it decides abort. Then it tells everyone.

The weakness is the gap between the two steps. A participant that has voted yes may not commit or abort on its own: it does not know whether the others voted yes too. If the coordinator crashes after collecting the votes, the prepared participants are stuck holding their locks until the coordinator comes back. Worse, if the coordinator's disk is lost, nobody may ever know the decision. That is why plain 2PC is called blocking.

## How Paxos groups make it non-blocking

Here, the coordinator is not a single machine but one of the participant shards (the lowest-numbered one), and each shard is a Paxos group. Everything 2PC needs to remember goes through Paxos logs:

- A participant's **prepare record** (what it read, with the versions it saw, and what it will write) is written to its shard's log before it votes yes.
- The coordinator's **decision** is written to the coordinator shard's log before anyone is told.

A single replica crashing therefore loses nothing. If the coordinator shard's leader crashes right after the decision is in the log, the next leader has the same log. Participants that have been prepared for a while (`QueryAfter` ticks) ask the coordinator shard for the outcome, and whichever replica now leads answers from the log (`TestCoordinatorCrashAfterDecide`). If the coordinator shard has no decision and is not coordinating the transaction any more (for example its leader changed mid-vote), it decides abort, which is always safe before a commit decision exists. Because decisions go through the log and the first one applied wins, a slow old leader's commit decision and a new leader's presumed abort can never both take effect.

As long as a majority of each shard is up, every prepared transaction eventually learns its outcome.

## Two-phase locking

Each shard leader keeps a lock table and runs strict two-phase locking:

- A **read** takes a shared lock on the key and returns the latest committed value with its version (the log slot that wrote it).
- **Prepare** takes exclusive locks on the keys the transaction will write, then writes the prepare record. From then on the record itself is the lock: every replica knows a prepared transaction holds its read and write keys until its commit or abort record is applied.
- Locks are released only when the transaction commits or aborts.

Shared read locks taken before prepare live only in the leader's memory. If that leader fails or is replaced, they are gone, and the transaction aborts when it tries to prepare (DECISIONS.md explains why that is acceptable). To make this safe even when a deposed leader does not yet know it was replaced, the state machine checks every prepare and one-phase record when it applies it: every key read must still be at the version the client saw, and the record must not overlap another prepared transaction's locks (its writes against their reads and writes, its reads against their writes). Losing a lock can only cause an abort, never a non-serializable commit.

## Deadlocks and wound-wait

Two transactions can each hold a lock the other needs: T1 read x and wants to write y, T2 read y and wants to write x. Neither can prepare. Wound-wait breaks this using the start timestamps:

- If the requester is **older** than the holder, it **wounds** the holder. A holder that only has a shared lock is aborted on the spot (its lock is dropped and its later reads or prepare fail). A holder that is already prepared cannot be aborted by a participant, so the older transaction asks the holder's coordinator to abort it if it has not decided yet.
- If the requester is **younger**, it **waits**.

Waits only ever go from younger to older, so there can be no cycle, and a wounded transaction retries with its original timestamp, so it eventually becomes the oldest and gets through. `TestDeadlockCrossingTxnsProgress` runs exactly the crossing pattern above with every timeout set far beyond its step bound, so only wound-wait can resolve it: both transactions commit within 112 steps in every seed. With wound-wait turned off, the same test gets stuck in 859 of 1000 seeds.

## One-phase commit

A transaction that only touched one shard skips 2PC: the shard leader takes the locks and writes a single one-phase record that validates the reads and applies the writes in one step.

## Recovery of prepared transactions

When a replica becomes leader, its volatile state is empty: no shared locks, no waiters, no coordination in progress. What it does have is the replicated state:

- **Locks.** Every prepared transaction in the state machine still holds its keys. The new leader's lock table is computed from those records, so a transaction prepared under the old leader keeps its locks (`TestParticipantCrashWhilePrepared` checks the new leader reports the prepared transaction as the holder of its write keys).
- **Votes.** If the old leader crashed before voting, the coordinator's retried prepare finds the record already there and gets a yes.
- **Outcomes.** Prepared transactions query the coordinator shard until they learn the decision, then write a commit or abort record. Commit, abort and decide records are idempotent by transaction ID: duplicates do nothing.

## What the tests check

| Property | Test | Seeds |
|---|---|---|
| Every txn commits on all its shards or none, with crashes, partitions and leader failovers | `TestTxnAtomicity` | 1000 (100 under -short) |
| Total balance never changes, in the final state and in every committed audit | `TestTxnBankInvariant` | same schedules |
| Committed history has no cycle in its dependency graph (ww, wr, rw) | `TestTxnSerializable` | same schedules |
| A history with an injected write-skew cycle is rejected | `TestTxnSerializabilityCheckerCatchesTamperedHistory` | 1 |
| Coordinator leader crashes after deciding, before notifying | `TestCoordinatorCrashAfterDecide` | 30 |
| Participant leader crashes while prepared | `TestParticipantCrashWhilePrepared` | 30 |
| Crossing transactions make progress under wound-wait | `TestDeadlockCrossingTxnsProgress` | 200 |
| Same seed, same trace | `TestTxnReplay` | 2 |

Negative tests, each run over seeds until a checker fails (seeds caught out of 1000):

| Broken mode | Caught | By |
|---|---|---|
| Coordinator commits without all yes votes | 1000 | atomicity |
| Participant releases its locks at prepare | 683 | bank invariant, serializability |
| Prepare record kept in leader memory instead of the Paxos log | 411 | atomicity |
| Wound-wait disabled | 859 stuck | liveness bound in the crossing test |

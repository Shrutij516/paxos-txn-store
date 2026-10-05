# Transactions across shards

Phase 5 added strictly serializable, interactive transactions across shards, tested in the deterministic simulator. Phase 6 runs the same code in `paxosd` over gRPC, with one replica of every shard in each process, and exposes it through the SDK (see docs/running.md). The code is in `internal/txn`.

## The pieces

- **Shards.** Keys are split across N shards (default 3) by hashing the key. Each shard is its own Multi-Paxos group of 3 replicas, exactly like Phase 2. Resharding is not supported yet (DECISIONS.md, Future work).
- **The shard state machine** (`sm.go`). Each shard's Paxos log holds transaction records instead of plain Gets and Puts: prepare, one-phase commit, coordinator decision, commit, abort. Every replica applies them in the same order, so every replica knows the same committed data, the same prepared transactions and the same outcomes.
- **The shard server** (`server.go`). It runs next to each replica and only acts while that replica leads its shard. It holds locks, runs two-phase commit, and turns decisions into log records.
- **Clients.** A transaction is interactive: `Begin`, then `Read`s that go to the shard leaders one at a time, `Write`s whose values are buffered on the client, then `Commit` or `Abort`. Each attempt has a unique transaction ID and a start timestamp. A retried transaction gets a new ID but keeps its original timestamp. (Over gRPC, `Write` also tells the shard leader, which renews the transaction's locks and fails fast if it was already wounded; see DECISIONS.md entry 24.)

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

## Concurrency control: two-phase locking plus validation at apply time

Two mechanisms work together. Strict two-phase locking at each shard leader keeps conflicting transactions apart while they run. Validation in the replicated state machine guarantees safety even when a leader's locks are lost.

**Locks at the leader.** Each shard leader keeps a lock table:

- A **read** takes a shared lock on the key and returns the latest committed value with its version (the log slot that wrote it). A read waits while another transaction holds an exclusive lock on the key, so it never sees a prepared but uncommitted write.
- **Prepare** first checks that the transaction still holds a shared lock on every key it read at this shard, then takes exclusive locks on the keys it will write, then proposes the prepare record. Between proposing and applying, the leader holds those exclusive locks in memory.
- Once the prepare record is applied, the record itself is the lock: every replica knows that a prepared transaction holds its read and write keys. The leader's lock table includes all prepared records, so a new leader has them too.
- Locks are released only when the transaction's commit or abort record is applied (strict 2PL). A one-phase transaction takes its exclusive locks, writes one record and releases everything when that record is applied.

Shared locks taken before prepare live only in the leader's memory. They are lost if the leader changes, and an idle transaction's shared locks expire after a lease (`LockLease`). The lease never touches a prepared transaction: its locks come from the replicated record, not from the lease-managed table (`TestLeaseNeverExpiresPreparedLock` keeps a transaction prepared for ten lease periods and checks a conflicting transaction still cannot commit). When a shared lock is lost, the transaction fails the "still holds its shared locks" check at prepare and aborts.

**Validation when records are applied.** A prepare or one-phase record carries the versions the transaction read at that shard, including shards it only read from. When a replica applies the record, it checks:

1. every key read is still at the version the transaction saw, and
2. the record does not overlap another prepared transaction: its writes against their reads and writes, its reads against their writes.

If either check fails, the record is rejected on every replica, deterministically, and the participant votes no. This matters because a leader that has been replaced may not know it yet and can still grant locks and serve reads. Its records may even be committed later by the new leader's recovery. Validation at apply time makes those cases harmless: a stale read or a lost lock can cause an abort, but never a non-serializable commit. `TestReadOnlyParticipantValidatesAfterLeaderChange` covers this: a transaction reads from shard A and only writes to shard B, A's leader changes, another transaction overwrites the key read on A, and the first transaction aborts.

So the protocol is 2PL for liveness and low abort rates, with an optimistic version check at the replicated state machine as the safety guarantee across leader changes.

## Deadlocks and wound-wait

Two transactions can each hold a lock the other needs: T1 read x and wants to write y, T2 read y and wants to write x. Neither can prepare. Wound-wait breaks this using the start timestamps:

- If the requester is **older** than the holder, it **wounds** the holder. A holder that only has a shared lock is aborted on the spot (its lock is dropped and its later reads or prepare fail). A holder that is already prepared cannot be aborted by a participant, so the older transaction asks the holder's coordinator to abort it if it has not decided yet.
- If the requester is **younger**, it **waits**.

Waits only ever go from younger to older, so there can be no cycle, and a wounded transaction retries with its original timestamp, so it eventually becomes the oldest and gets through. `TestDeadlockCrossingTxnsProgress` runs exactly the crossing pattern above with every timeout set far beyond its step bound, so only wound-wait can resolve it: both transactions commit within 112 steps in every seed. With wound-wait turned off, the same test gets stuck in 859 of 1000 seeds.

## One-phase commit

A transaction that only touched one shard skips 2PC: the shard leader takes the locks and writes a single one-phase record that validates the reads and applies the writes in one step.

If the leader cannot grant the locks (the transaction was wounded, or a read lock was lost to a leader change), it does not simply reply "aborted". It writes an abort record for the transaction and replies once that record is applied, with whatever outcome the log then holds. A client whose commit timed out retries the same request at the new leader; meanwhile the old leader's one-phase record may still be on its way into the log. The first of the two records in the log decides, so the client is never told "aborted" for a transaction that then commits (`TestOnePhaseAbortIsLogged`).

The simulated clients retry a commit the way the SDK does: they send it to the shard leader they last heard from, and after a timeout forget that leader and try the next replica. Every schedule also checks that what each client was told matches the logs: no transaction reported aborted committed, and every one reported committed did. `TestOnePhaseRetryAtNewLeader` runs the case above in 1000 seeded schedules: the leader crashes right after sending the accepts for a one-phase record, the next leader recovers the record but is cut off from the third replica before it can commit it, and the client's retried commit reaches that leader. With the abort logged first, all 1000 seeds pass. With the old direct reply (the `ReplyAbortUnlogged` broken mode), all 1000 tell the client "aborted" for a transaction that then commits. The 1000 ordinary random chaos schedules never hit this window (0 of 1000 catch the broken mode). A gap-heavy mode, as in the Multi-Paxos tests, widens it: during chaos 70% of Accepts are dropped and a leader that sends the accepts for a one-phase, prepare or decide record crashes 1 to 3 steps later with probability 5%, so new leaders often recover such records and need several retransmissions to commit them while clients retry. Every check holds in 1000 gap-heavy schedules (`TestTxnGapHeavy`), and they catch the broken mode in 40 of 1000 (4%, `TestTxnGapHeavyCatchesUnloggedAbort`, which requires at least 1%). The directed test stays, since it catches the bug in every seed.

## Recovery of prepared transactions

When a replica becomes leader, its volatile state is empty: no shared locks, no waiters, no coordination in progress. What it does have is the replicated state:

- **Locks.** Every prepared transaction in the state machine still holds its keys. The new leader's lock table is computed from those records, so a transaction prepared under the old leader keeps its locks (`TestParticipantCrashWhilePrepared` checks the new leader reports the prepared transaction as the holder of its write keys).
- **Votes.** If the old leader crashed before voting, the coordinator's retried prepare finds the record already there and gets a yes.
- **Outcomes.** Prepared transactions query the coordinator shard until they learn the decision, then write a commit or abort record. Commit, abort and decide records are idempotent by transaction ID: duplicates do nothing.

## Isolation level

The tests show **strict serializability** for committed transactions. The checker builds Adya's direct serialization graph over every committed transaction in each schedule:

- **G1a, aborted reads:** every version a committed transaction read was installed by a committed transaction.
- **G1b, intermediate reads:** every version read is its writer's final version of that key.
- **G-cycle:** the graph of write-write, write-read and read-write (anti-dependency) edges has no cycle. Passing this means the history is serializable.
- **Real-time order:** an edge from T1 to T2 whenever the client learned that T1 committed before T2 began. With these edges added the graph is still acyclic in all 1000 schedules, so the serial order also respects real time.

Each of these checks has a control that tampers with a real history and must be rejected (`TestTxnCheckerControls`, `TestTxnSerializabilityCheckerCatchesTamperedHistory`). The claim covers committed transactions only. Reads of a transaction that later aborts may be stale (for example, served by a deposed leader); the abort is what keeps them out of the history.

## Presumed abort is logged before it is answered

When a participant asks the coordinator shard about a transaction and no decision is in the log, the coordinator shard does one of two things. If it is still collecting votes for that transaction, it does not answer, and the participant asks again later (`TestQueryWhileCoordinatingWaitsForDecision`). Otherwise it proposes an abort decision and answers only after that decision has been applied from its log. Since the first decision applied wins, a transaction whose abort has been logged can never commit, even if the client's commit request arrives afterwards (`TestQueryBeforeDecisionLogsAbortFirst`). Every schedule also checks that no node ever sends a Decision message that is not already in its own log.

## What the tests check

| Property | Test | Seeds |
|---|---|---|
| Every txn commits on all its shards or none, with crashes, partitions and leader failovers | `TestTxnAtomicity` | 1000 (100 under -short) |
| Total balance never changes, in the final state and in every committed audit | `TestTxnBankInvariant` | same schedules |
| No G1a or G1b, and no cycle in the dependency graph (ww, wr, rw): serializable | `TestTxnSerializable` | same schedules |
| Still acyclic with real-time edges: strictly serializable | `TestTxnStrictSerializable` | same schedules |
| Tampered histories are rejected: write-skew cycle, G1a, G1b, real-time violation | `TestTxnSerializabilityCheckerCatchesTamperedHistory`, `TestTxnCheckerControls` | 1 each |
| A Decision is only sent once it is in the sender's log | every schedule | 1000 |
| A lease never expires a prepared transaction's locks | `TestLeaseNeverExpiresPreparedLock` | 1 |
| A read-only participant validates its reads after a leader change | `TestReadOnlyParticipantValidatesAfterLeaderChange` | 1 |
| Presumed abort is logged before it is answered, and wins | `TestQueryBeforeDecisionLogsAbortFirst` | 1 |
| A query during coordination does not abort the txn | `TestQueryWhileCoordinatingWaitsForDecision` | 1 |
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

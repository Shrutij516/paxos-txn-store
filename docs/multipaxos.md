# Multi-Paxos replicated log (one shard)

Phase 2 turns single-decree Paxos into a replicated log. Each log position ("slot") is its own Paxos instance, but a stable leader runs the expensive first phase once for all of them. The code is `internal/paxos/replica.go`; the key-value state machine is `internal/kv`.

## The shape of it

- Every replica is an acceptor for every slot. It stores one `promised` ballot (for all slots) and, per slot, the last entry it accepted and in which ballot. Both are written to storage before any reply.
- One replica at a time acts as leader. Clients send requests to it; it gives each request the next free slot and asks the acceptors to accept it there.
- Once a majority accepts an entry in the leader's ballot, the entry is chosen. The leader advances its **commit index** (the highest slot such that every slot up to it is chosen) and applies entries in order to the key-value store.

## Why Prepare runs once per leader

In single-decree Paxos every value needs two round trips: Prepare, then Accept. Prepare exists so a proposer can learn what might already be chosen and lock out older ballots. None of that is slot specific: a promise "ignore every ballot below mine" can cover all slots at once.

So a new leader sends one `LogPrepare(ballot, commit)`, where `commit` is its own commit index. Each acceptor promises the ballot for every slot and replies with every entry it has accepted above that commit index. With a majority of those replies, the leader knows everything that could possibly be chosen in the slots it does not already know. From then on, each new client request costs only the Accept round trip. The leader keeps that advantage until someone with a higher ballot takes over.

A leader that skipped this step would propose into slots that may already hold chosen entries, and overwrite them. `TestMPNegativeSkipPrepare` shows the checker catching exactly that.

## Re-proposing, gaps and no-ops

After winning Prepare, the new leader walks every slot above its commit index up to the highest slot anyone reported:

1. If it already knows the slot is chosen, it re-proposes that entry.
2. Otherwise, if any promise reported an accepted entry for the slot, it re-proposes the one with the **highest ballot**. This is the same rule as in single-decree Paxos, applied per slot, and it is what keeps a possibly-chosen entry from being replaced. `TestMPNegativeIgnorePromisedValues` shows what happens without it: two different entries get chosen at the same slot.
3. If nobody in the majority accepted anything there, the slot is a **gap**. Nothing can have been chosen in it (any chosen entry would appear in at least one reply from the majority), so the leader fills it with a **no-op**.

Gaps happen when an old leader handed out slots 5 and 6, the Accepts for 5 were lost, and it crashed. Slot 6 may be chosen, but the log cannot be applied past slot 5 until something occupies it. The no-op is that something: the state machine skips it, and the log moves on. `TestReplicaTakeoverFillsGapsWithNoops` checks both the highest-ballot rule and the no-op fill.

These takeover paths are rare under ordinary faults, so the tests also run a **gap-heavy** mode: during chaos, 70% of Accept messages are dropped, and a replica that sends Accepts is crashed 1 to 3 steps later with probability 3% per Accept. Over 1000 seeds, new leaders fill a gap with a no-op in 344 seeds and recover a value by highest ballot in 998 (in 147 of those, the promises reported different entries for the same slot, so the highest-ballot rule decided). Log safety, state machine safety and linearizability all hold in this mode (`TestMPGapHeavy*`). Without Prepare the checker fails in 960 of 1000 gap-heavy seeds, and when promised values are ignored it fails in 965.

## Leader election

There is no wall clock. Every replica has an election timer counted in simulator ticks, reset to a random value in `[ElectionMin, ElectionMax]` whenever it hears from a leader (a heartbeat or an Accept) or grants a promise to a candidate. The leader sends a heartbeat every `HeartbeatEvery` ticks, carrying its ballot and commit index. If a follower's timer runs out, it picks a ballot above every ballot it has seen, persists the round, and runs Prepare. The randomized timeout makes it unlikely that two replicas start an election at the same moment, and if they do, the one with the lower ballot gets a Nack and steps down.

Anyone who is not the leader answers a client with "not leader" plus the ID of the leader it last heard from. The client retries there.

## How followers learn commits, and how catch-up works

A heartbeat carries the leader's commit index. For each slot up to that index, if the follower accepted an entry **in the leader's current ballot**, that entry is the chosen one, because a leader proposes only one entry per slot per ballot. The follower marks it chosen and applies it.

If that still leaves the follower behind (it was partitioned, crashed, or accepted an older ballot's entry), it sends `CatchupRequest(from = commit + 1)`. Any replica that has those slots committed replies with a batch of chosen entries (`CatchupBatch`, default 64). The next heartbeat triggers the next batch until the follower is level.

A restarted replica keeps its acceptor state (promise and accepted entries) because that is on disk, but its commit index and key-value store are in memory and start empty. It rebuilds them from slot 1 through catch-up and re-applies every entry in order, so its store ends up identical to everyone else's. `TestMPCatchUp` restarts a follower that has missed more than one batch and checks that its committed log matches the leader's slot by slot.

Snapshots and log compaction are deferred (see DECISIONS.md), so the leader keeps the whole log.

## Why reads go through the log

A `Get` is a log entry like any `Put`. It returns the value as of its slot, after every earlier slot has been applied.

The tempting shortcut is for the leader to answer reads from its own store. That is wrong when the leader has been replaced without knowing it: an old leader on the minority side of a partition still believes it leads and would serve stale values while the majority commits new writes. Putting the read in the log forces a majority to accept it in the leader's current ballot, which proves the leader is still the leader at that moment. The cost is a round trip per read. Leader leases would remove that cost but depend on bounded clock drift, so they are deferred.

## Exactly-once

Every request carries `(clientID, seq)`. A client retries a request with the same `seq` if it times out, and moves to `seq + 1` only after getting an answer.

**Assumption: each client has at most one request outstanding.** The dedup table relies on it. It keeps only the latest `seq` and result per client, not a history, so a client that pipelined several requests could have an earlier one skipped (treated as stale) if a later one reached the log first. The simulated clients obey this rule; a real client library must too. Retries can land in the log several times. The key-value store keeps a dedup table: for each client, the last `seq` it executed and the result. A repeat of that `seq` returns the cached result without executing it again. An older `seq` is ignored, since the client has already moved on. `TestMPExactlyOnce` sends a Get, retries it, has another client overwrite the key, and retries again: every retry returns the original result and the request executes once. `TestMPNegativeNoDedup` shows the checker catching the double execution when the table is off.

## How the linearizability check treats unanswered operations

Test clients record each operation's call step, return step and output. An operation that never got a response (its client is still retrying after a timeout, or its leader crashed, or the test stopped) is **not dropped**: it goes into the history with an unknown output and a return time after every other operation. Porcupine may then place it anywhere after its call, or treat it as taking effect at the very end, which matches reality: the request may or may not have been applied. Dropping it would be unsound, because a Get elsewhere might have observed its write. The check runs twice per schedule: once cut off at the end of the chaos phase, when many operations are still open, and once after every client has finished.

## Test run length

Under `go test -short` (what `make test` runs) the seeded suites use 100 seeds. CI and `make test-full` run the full 1000.

## What the tests check

| Property | Test |
|---|---|
| No two nodes commit different entries at a slot (plus a global oracle that no two entries are chosen at a slot) | `TestMPLogAndStateMachineSafety` |
| Every node's applied sequence (entries and results) is a prefix of every other's, across restarts | `TestMPLogAndStateMachineSafety` |
| Client histories with retries are linearizable (porcupine), checked both mid-chaos and at the end | `TestMPLinearizable`, `TestMPGapHeavyLinearizable`, `TestMPLinearizabilityCheckerCatchesStaleRead` |
| Operations that never got a response stay in the history, open-ended | `TestMPHistoryKeepsOutstandingOperations` |
| A new leader commits within a bound after the old one crashes | `TestMPLeaderFailover` |
| A restarted node catches up to the leader's committed log | `TestMPCatchUp` |
| A retried request is applied once | `TestMPExactlyOnce` |
| Same seed, same trace | `TestMPReplay` |
| Leader skips Prepare / ignores promised values / dedup off: each must be caught | `TestMPNegative*`, `TestMPGapHeavyNegative*` |
| Gap-heavy schedules hit the no-op path in at least 10% of seeds | `TestMPGapHeavySafety` |

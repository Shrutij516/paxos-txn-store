# Single-decree Paxos in this repo

This note explains, in plain language, how the acceptor in `internal/paxos` keeps the cluster safe, and what goes wrong if you take its rules away. Every claim about breakage is backed by a negative test that must fail the safety checker.

## The pieces

- **Proposer** wants a value chosen. It picks a ballot, runs two phases, and retries with a higher ballot if it is rejected or hears nothing.
- **Acceptor** votes. It is the only role with state that matters for safety, and it writes that state to storage before it answers anyone.
- **Learner** watches `Accepted` messages and decides a value once a majority of acceptors accepted it in the same ballot. Votes for the same value in different ballots never add up (`TestLearnerCountsPerBallotNotPerValue`).

Every node runs all three. Nodes only talk through a `Transport`, which may lose, delay, duplicate or reorder messages.

## Ballots

A ballot is a pair `(round, nodeID)`, compared by round first and node ID second. Because the node ID is part of it, two proposers can never use the same ballot. A proposer also writes its latest round to storage before sending `Prepare`, so a restarted proposer never reuses a ballot it already used with a different value.

## The two acceptor rules

Each acceptor remembers three things: `promised` (the highest ballot it has promised), and `accepted` plus `value` (the last proposal it accepted, if any).

**Promise rule.** When an acceptor gets `Prepare(b)`:
- if `b` is at least `promised`, it sets `promised = b`, saves that to storage, and replies `Promise(b)` together with its `accepted` ballot and `value`;
- otherwise it replies `Nack` and changes nothing.

**Accept rule.** When an acceptor gets `Accept(b, v)`:
- if `b` is at least `promised`, it records `accepted = b, value = v` (and raises `promised` to `b`), saves that, and tells everyone `Accepted(b, v)`;
- otherwise it replies `Nack` and changes nothing.

On the proposer side there is one matching rule: after it collects promises from a majority, it must propose the value with the highest `accepted` ballot among those promises. Only if none of them has accepted anything may it propose its own value.

## Why this is safe

A value is **chosen** once a majority of acceptors have accepted it in the same ballot `b`. We need every later ballot to propose that same value.

Take any later ballot `b2 > b`. To get to phase 2, its proposer needs promises from a majority. Any two majorities overlap, so at least one acceptor in that majority also accepted `(b, v)`.

- Because of the **promise rule**, that acceptor reports what it accepted, so the proposer of `b2` sees `v` (or something accepted in a ballot between `b` and `b2`, which by the same argument applied earlier is also `v`).
- Because of the **accept rule**, once that acceptor promised `b2` it will refuse any `Accept` from a ballot lower than `b2`, so an old proposer cannot sneak a different value in behind the new promise.
- Because state is **saved before replying**, a crash and restart cannot make the acceptor forget a promise or a vote it already sent.

So the proposer of `b2` is forced to pick `v`, and by induction every higher ballot does too. Different values can never both be chosen.

Liveness is not guaranteed by these rules (two proposers can keep preempting each other). Proposers use randomized, exponentially growing backoff so that in practice one of them gets a quiet window and finishes.

## What breaks without each rule

The tests live in `internal/paxos/paxos_test.go`. The seed counts come from running each broken mode over seeds 1 to 1000; the tests themselves stop at the first violation. Each negative test runs seeded random schedules with a deliberately broken acceptor and **passes only if the safety checker catches a violation**. If the checker ever stopped catching these, the negative tests would fail, which tells us the positive tests had become vacuous.

| Removed | What goes wrong | Test |
|---|---|---|
| Promise rule, part 1: refuse lower ballots | The acceptor promises, and records, a ballot lower than the one it already promised. A delayed `Prepare` from an old proposer drags `promised` back down, so that proposer's stale `Accept` is now allowed through and a second value can be chosen in the old ballot after `v` was chosen in a newer one. | `TestNegativePromiseLowerBallot` (196 of 1000 seeds violate) |
| Promise rule, part 2: report what was accepted | The acceptor promises correctly but leaves its accepted proposal out of the reply. A new proposer collects a majority that looks "clean" even though `v` was already chosen, proposes its own value, and that also gets chosen. | `TestNegativeOmitAcceptedFromPromise` (989 of 1000 seeds violate) |
| Accept rule | The acceptor accepts from any ballot, even one lower than it promised. A slow proposer with an old ballot can get a different value accepted by a majority after a newer ballot already chose `v`. | `TestNegativeNoAcceptRule` (440 of 1000 seeds violate) |
| Durable state | The acceptor loses its promises and votes on restart. After `v` is chosen by acceptors A and B, B restarts empty, and a new proposer that hears from B and C sees no accepted value, so it gets a second value chosen. | `TestNegativeForgetfulAcceptor` (965 of 1000 seeds violate) |

In the tests, the rules are switched off through hooks in `internal/paxos/export_test.go`, which is compiled only under `go test`, so a real binary cannot run with a broken acceptor.

## How the safety checker decides

The checker in `internal/paxos/cluster_test.go` flags a run as unsafe if any of these hold:

1. Two nodes decided different values, or a restarted node learned a different value than it did before (agreement).
2. The global oracle saw a majority of acceptors accept two different values, each within a single ballot (agreement, independent of which messages learners happened to receive).
3. A decided or chosen value was never proposed (validity).

On any failure the test prints the seed. Because the simulator is deterministic, rerunning with that seed reproduces the exact run.

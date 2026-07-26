# Distributed keyed state Cycle 54 review

- **Date:** 2026-07-26
- **Scope:** validation-model subset of step 4 in the Cycle 50 delivery-evidence sequence: a
  synchronous fake transactional writer around the frozen authority and bytes
- **Cycle outcome:** marker-before-data, bounded transaction planning, checked admission sequences,
  deterministic abort retry, terminal ambiguity, and confirmed-predecessor replay are executable
- **Runtime/backend outcome:** unchanged; the model is unit-test-only and no connector, Kafka,
  state backend, endpoint, metric, capability, or admission guard changed
- **Production verdict:** **NO-GO**; durable interval uniqueness, complete partition inventory,
  broker fencing/visibility, lifecycle wiring, and the independent soak remain absent

## Result and exact boundary

The root-workspace-excluded independent tool now has a `#[cfg(test)]` synchronous writer model. It
consumes the Cycle 53 prepared authority and Cycle 52 marker/data encoders. Its only states are
uninitialized, marker pending, data open, transaction in flight at begin/send, and terminal poison.
Initialization never opens data. One marker value is encoded once and associated with every member
of the supplied canonical affected-partition slice in one unsplittable simulated transaction. Only
a confirmed marker commit opens data; marker and first data are separate transactions.

The affected-partition slice is an input hypothesis. The fake validates nonempty sorted unique
nonnegative values, but it has no broker topology inventory and cannot prove that the slice is
complete. That proof remains at the real broker boundary.

Data execution accepts exactly one bounded slice. A separate pure planner reports maximal ordered
ranges by explicit record-count and modeled-byte limits; it never hides multiple transactions
behind one all-or-nothing result. Modeled bytes are only `66-byte header + payload length`, excluding
Kafka keys, framing, compression, requests, and broker limits. Preflight validates the whole slice,
including partitions and operation IDs, before sequence preview or allocation. Headers are inline
fixed arrays. Owned payload/header transcript storage exists only in tests and only for confirmed
simulation results; it is not allocator or latency evidence.

One counter spans every partition in a `(shard, interval)`. A transaction previews one contiguous
range, derives each row as `start + index`, and advances only after a confirmed commit. Confirmed
abort attempts retry the same borrowed slice and range inside the same scripted call. `Option<u64>`
represents exhaustion, so a range ending at `u64::MAX` is legal once. Ambiguous initialize, begin,
send, or commit poisons the writer permanently. An unchanged confirmed-only transcript after an
ambiguous marker or data commit is no visibility verdict.

A first model interval requires no predecessor. A successor consumes a token from a confirmed
predecessor, preserves the structured `(deployment, pipeline incarnation, sink, shard)` scope,
names that exact predecessor, uses a distinct current ID, and restarts sequence zero. This proves
only immediate linkage. A separately constructed chain can reuse an ID, and `A -> B -> A` is not
rejected across writers. Durable uniqueness for confirmed, aborted, ambiguous, and retired interval
IDs is still missing. After an ambiguous marker, Cycle 55 must also reconcile read-committed broker
visibility before it can choose either the ambiguous interval or the last visible interval as the
successor predecessor.

## Independent review corrections

Pre-implementation audits rejected an exclusive-end sequence counter because it would make wire-
valid `u64::MAX` impossible. They required an explicit exhausted sentinel, one global counter rather
than per-partition counters, provisional ranges across confirmed-abort retry, an unsplit marker, and
an explicit chunk planner instead of a convenience method that could hide a committed prefix. They
also identified the ambiguous-marker predecessor branch before implementation, so it remains an
open broker-reconciliation requirement rather than a guessed rule.

Implementation review found that invalid late records initially reached sequence preview and batch
allocation before partition/operation-ID validation. A complete first validation pass now precedes
both. A second review rejected three overclaims: immediate predecessor checking is not durable
non-reuse; supplied marker partitions are not a proven complete topology; and absence from a
confirmed-only transcript after ambiguity is not proof of broker absence. The ADR, plans, report,
charter, API name, and tests now state those limits. Reviewers approved correctness and hot-path/
scope after the corrections; the adversarial documentation/test verdict is recorded below.

## AI slop review

**Pass.** The model has one concrete writer protocol and three injected outcomes. It adds no generic
broker trait, async runtime, Kafka error taxonomy, retry/backoff policy, UUID allocator, interval
registry, connector adapter, metrics framework, or speculative window/join protocol. The ambiguous
marker branch is left unresolved instead of being filled with invented behavior.

## Overengineering and hot-path review

**Pass for a test-only model.** Transaction count is capped by the existing 65,536-header bound;
modeled byte limits are explicit inputs rather than invented production constants. Marker length
multiplication and every data-size/sequence calculation are checked. Data preparation uses one
batch of inline headers, one shard-local counter, borrowed input payloads, and no lock, atomic,
per-row reservation, payload hash, or provenance expansion. The confirmed transcript allocation is
test bookkeeping. No throughput, allocator, broker-limit, or latency conclusion is drawn from it.

## Unused-code review

**Pass.** The module is compiled only for unit tests. Every state, phase, outcome, bound, trace,
scope token, and transition helper is exercised. Nothing is exported by the standalone CLI or root
workspace, and there is no dormant runtime feature, backend dependency, endpoint, or metric.

## Production-readiness review

**NO-GO.** The fake does not prove a complete sink-partition inventory, stable transactional-ID
serialization, producer-epoch fencing, old-producer rejection/abort, read-committed visibility,
ambiguous outcome reconciliation, durable interval non-reuse, header placement, broker limits, or
latency. The runtime still lacks pipeline-incarnation and interval lifecycles, canonical key/count
handoff, supported assignment/checkpoint evidence, transactional sink wiring, and a qualified state
backend. Source cursor, managed state, and Kafka are not one atomic commit, so this remains an
at-least-once design rather than exactly-once. Cluster keyed aggregates, windows, and stateful joins
remain fail-closed. No independent release-binary soak ran.

## Documentation review

**Pass.** ADR-008 is the sole normative state-machine definition. The implementation and phase
plans, validation report, and soak charter link or summarize only the evidence boundary and retain
**NO-GO**. The earlier blanket marker-failure rule now distinguishes a provider-proved rejection or
confirmed abort from an unproved outcome. Backend research and the selected-but-stopped TidesDB line
are unaffected; no research document became obsolete in this cycle, so none was removed.

## Test review

**Pass for the validation-model subset.** Final checks cover:

- 64 standalone library tests and 6 CLI tests, plus doc tests;
- first and successor markers, exact stable producer scope, supplied fanout count/byte bounds, and
  marker-before-data;
- direct begun/staged transition legality, confirmed abort at begin/send/commit, ambiguity at
  initialize/begin/send/commit, poison inertia, and confirmed-only visibility;
- count- and byte-driven ordered chunk ranges, exact limits, oversized singleton, arithmetic
  overflow, invalid partition/operation ID, and no preflight mutation;
- global interleaved-partition sequences, confirmed-abort retry, `u64::MAX` once, exhaustion, and
  overflow-before-begin;
- confirmed-prefix retention when a later explicit chunk is ambiguous, and replay with stable
  operation/payload bytes but a new interval and sequence zero;
- all-target standalone Clippy with warnings denied, standalone/root formatting, dependency
  isolation, local Markdown links, and `git diff --check`; and
- independent state/sequence, hot-path/scope, and adversarial claim/test reviews.

These are deterministic model tests, not Kafka integration, production benchmarks, backend runs,
distributed fault tests, or certification evidence.

## Cycle 55 review plan

Use a disposable real Kafka/Redpanda environment, preferring the available Docker-on-WSL path, to
test only step 5 before any runtime connector wiring.

1. **AI slop:** freeze the smallest stable transactional-ID scope/encoding and broker test driver;
   do not copy the fake into a generic connector framework.
2. **Overengineering/hot path:** exercise explicit broker/request/header limits and bounded batches;
   measure broker operations separately from production latency and avoid per-row futures.
3. **Unused code:** keep the driver outside the root workspace and require every fault/control hook
   in a deterministic real-broker case.
4. **Production readiness:** prove producer-epoch fencing, forced old-writer rejection, confirmed
   all-supplied-partition markers, complete topic inventory, read-committed atomic visibility,
   aborted predecessor invisibility, and ambiguous-marker reconciliation. Retain **NO-GO**.
5. **Documentation:** record actual broker/version/configuration and evidence limits once; make no
   exactly-once, backend, runtime, latency, or soak claim.
6. **Tests:** bracket initialization, every marker send/commit boundary, open predecessor data,
   ambiguous data begin/send/commit before and after possible broker application, successor fencing,
   `__ldb` header placement, null marker key/empty non-null value, untouched data payload, and read-
   committed/read-uncommitted observations. Keep the independent release-binary soak as a later
   mandatory gate.

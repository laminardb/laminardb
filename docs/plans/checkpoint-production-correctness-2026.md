# Checkpoint Production Correctness — 2026 Order

**Status:** code-complete for currently admitted runtime modes on
`feat/checkpoint-correctness-2026`; fault-injection and soak execution remain
**Decision date:** 2026-07-11

This plan supersedes the older implementation order that treated connector phase-2 commits and
per-sink status fields as the checkpoint commit point. LaminarDB now follows a decision-led
protocol: an exact immutable state seal plus one exact durable decision commits the recovery cut;
external publication is re-driven asynchronously from that decided inventory.

The ordering follows the production model used by modern barrier-snapshot systems and the
separation of fast alignment from durable completion described in the current
[Apache Flink task lifecycle](https://nightlies.apache.org/flink/flink-docs-stable/docs/internals/task_lifecycle/),
[Flink fault-tolerance model](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/),
and [Flink sink contract](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/sinks/).
The latency design also incorporates recent work on decoupling the pipeline pause from remote
checkpoint persistence ([arXiv:2403.13629](https://arxiv.org/abs/2403.13629)).

## Required implementation order

1. **Admission and durability contracts.** Resolve source/sink consistency and topology before
   connector I/O. Reject a delivery guarantee that the active runtime mode, state backend, or
   connector cannot prove.
2. **Exact attempt identity.** Durably reserve `(epoch, checkpoint_id)` and bind it to pipeline
   identity plus a create-once deployment incarnation. Failed attempts are abandoned, never
   retried under the same identity.
3. **Aligned capture.** One streaming coordinator admits barriers, fences prior sink writes,
   aligns source/shuffle input, and captures source offsets with operator state.
4. **Immutable prepare.** Persist the Prepared manifest, source handoff, vnode partials, and every
   coordinated sink participant marker under the exact attempt. Writer generation and payload
   digests make stale or conflicting partials unsealable.
5. **Restorable seal.** Publish one canonical seal only after the complete vnode/descriptor
   inventory validates. Presence without exact provenance is not sufficient.
6. **Durable decision.** Record the exact decision after the seal. Starting this write is the
   irrevocable boundary: timeout or transport failure is ambiguous and must never trigger live
   rollback.
7. **Completion and source acknowledgement.** Finalize the manifest and deliver the exact
   completion. Sources acknowledge only that validated attempt. A successor-epoch failure is a
   continuation fault, not a retroactive failure of the committed cut.
8. **External publication.** A designated committer consumes decided sealed inventories in order,
   validates the exact predecessor cursor, and commits Delta/Iceberg outside pipeline-stall
   latency. It re-reads external cursors on every pass and fails closed on rollback or overlap.
9. **Recovery.** Select the highest exact durable decision, require its bound manifest/seal and
   deployment identity, finalize a matching Prepared manifest, and re-drive external publication.
   An undecided Prepared attempt is force-rolled back.
10. **Retention and shutdown.** Retention is one coalescing maintenance owner pinned by the
    external-commit floor and newest decision. Shutdown first settles/cancels durable tails, then
    tears down sources/sinks and finally releases deployment/control-plane ownership.

## Configuration dimensions

Research and the fault model do not justify independently tunable pre-commit, persist, commit,
rollback, enqueue, actor, and acknowledgement budgets. Resetting those budgets makes the actual
checkpoint bound unknowable. The production model is:

- one end-to-end checkpoint-attempt deadline;
- connector write timeout as a connector health limit, carried as one absolute enqueue-to-ack
  deadline;
- a private bounded cleanup budget after failure;
- public resource bounds: in-flight epochs, staged bytes, and retention;
- optional cluster delta-chain bounds; alignment derives from the one attempt deadline;
- a private runtime-owned safety cap for externally uncommitted epochs.

Polling intervals, sidecar thresholds, per-sink commit statuses, explicit writer IDs, store-local
retention counts, and incremental-query emission policy are implementation details, obsolete
dimensions, or belong outside checkpoint configuration.

## Runtime-mode policy

| Runtime | Supported production semantics | Required durability/fencing |
|---|---|---|
| Embedded/local | Best effort, at least once, and coordinated exactly once | Node-durable state/checkpoint storage and exclusive deployment lock for exactly once |
| Single-node server | Same protocol as embedded/local | Same node-durable/exclusive-ownership requirements |
| Cluster | At least once only | Cluster-shared state/checkpoint storage, durable membership/lease during tail settlement |
| Cluster exactly once | **Rejected (`LDB-0013`)** | Remains rejected until leader term is atomically consumed by both decision creation and external sink cursor commit |

The rejection is intentional correctness, not a missing fallback: a renewable leader lease alone
cannot prevent an expired leader from completing a separate object-store or catalog transaction.

## Exit gates

- All-target compilation for core, connectors, DB (local and `cluster`), and server.
- Deterministic tests for decision ambiguity, exact seal inventory, cursor rollback/overlap,
  successor-epoch failure, total attempt deadline, retention coalescing, and shutdown ownership.
- Fault-injection/soak runs for process death before/after seal, during decision creation, during
  external commit, and during shutdown.
- Latency verification separates pipeline stall, restorable-gate wait, durable completion, and
  external-commit lag; retention work must never appear on the source-ack critical path.
- Dead-code and configuration audit after every feature cycle.

Code gates completed on 2026-07-11: strict all-target Clippy passes for core/connectors, DB in
local and cluster feature builds, and server; connectors also pass with no default features;
workspace rustfmt and `git diff --check` pass. Test targets compile through those all-target gates.
Runtime test execution remains outstanding in this Windows environment because linking the test
binaries exhausts the configured paging file; run the fault-injection and soak matrix on the Linux
production CI runners before release.

Remaining deliberate gap: cluster exactly once requires a term-fenced decision/external-commit
protocol. Until that exists, admitting it would be less correct than failing configuration.

The file source now checkpoints one exact, unbounded processed-file inventory plus a hash-verified
partial-file row cursor; it never truncates correctness state or treats a probabilistic membership
result as authoritative. In-memory inventory inserts are `O(log N)`, and immutable source-offset
snapshots/clones are `O(1)` through a structurally shared serialized-fragment tree. The full
`O(processed files)` string is built only when DB converts the source position into the durable
checkpoint manifest (or during explicit compatibility/recovery access), without retaining a
second materialized copy in the source snapshot. Durable storage is still a full payload, not an
append log. A later cycle may introduce a checksummed durable inventory log and paged compaction
whose exact root/cursor is sealed by the checkpoint manifest; it must not reintroduce eviction or
false-positive data loss.

The full durable source-offset map is materialized on a deadline-bounded blocking worker in the
leader/follower durable tail, after the callback has released the pipeline and before the
checkpoint coordinator mutex is acquired. Connector startup follows the same single-budget model:
all sink opens in one stage and all source starts in one stage share the checkpoint-derived
absolute deadline, while failed stages use one private shared cleanup deadline.

MySQL CDC now advertises only behavior it executes: one fully qualified table, explicit unique
`server.id`, strict GTID selection, and real buffer/backpressure controls. It remains an honest
ephemeral/best-effort source until a certified snapshot plus replay/resume protocol is implemented;
reader task or stream failure is terminal rather than silently appearing as an empty poll.

NATS source behavior is likewise aligned with its ephemeral contract. JetStream messages are
acknowledged asynchronously only after successful deserialization through an owned, bounded worker
with capped concurrency and private I/O deadlines; no checkpoint-owned ack queue can grow forever
when checkpointing is disabled. Queue saturation or ack failure leaves messages eligible for
broker redelivery, and reader termination becomes a terminal source error after queued data drains.

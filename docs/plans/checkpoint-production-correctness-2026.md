# Checkpoint Production Correctness — 2026 Order

**Status:** corrective fault cycle active on `feat/checkpoint-correctness-2026`. Earlier embedded
and coordinated leader/follower hard-kill legs passed; the rotating three-node leg exposed further
source-reconciliation, restore-activation, retention, and decision-ambiguity faults. Corrective
code is not certified until the current deterministic gates and the full fault matrix both pass.
**Decision date:** 2026-07-12

This plan supersedes the older implementation order that treated connector phase-2 commits and
per-sink status fields as the checkpoint commit point. LaminarDB now follows a decision-led
protocol: an exact immutable state seal plus one exact durable decision commits the recovery cut;
external publication is re-driven asynchronously from that decided inventory.

The ordering follows the production model used by modern barrier-snapshot systems and the
separation of fast alignment from durable completion described in the current
[Apache Flink task lifecycle](https://nightlies.apache.org/flink/flink-docs-stable/docs/internals/task_lifecycle/),
[Flink fault-tolerance model](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/),
and [Flink sink contract](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/sinks/).
Flink's asynchronous snapshot model is the basis for keeping remote persistence off the pipeline
pause. [CheckMate](https://arxiv.org/abs/2403.13629) supports coordinated checkpoints as the
production baseline under uniform load while showing that protocol choice is workload-sensitive;
it does not by itself justify a universal latency claim.
The release gates treat guarantees as empirical contracts, following the 2025 PVLDB work on
end-to-end processing-guarantee validation under injected process and network faults
([PGVal](https://www.vldb.org/pvldb/vol18/p585-tahir.pdf)); a clean unit-test model is not a
substitute for recovery-output validation.

## Required implementation order

1. **Admission and durability contracts.** Resolve source/sink consistency and topology before
   connector I/O. Reject a delivery guarantee that the active runtime mode, state backend, or
   connector cannot prove.
2. **Exact attempt identity.** Durably reserve `(epoch, checkpoint_id)` and bind it to pipeline
   identity plus a create-once deployment incarnation. Failed attempts are abandoned, never
   retried under the same identity.
3. **Aligned capture.** One streaming coordinator admits barriers, fences prior sink writes,
   aligns source/shuffle input, and captures source offsets with operator state.
4. **Immutable participant prepare.** Each capture participant persists its Prepared manifest,
   source handoff, vnode partials, coordinated-sink marker, and one final readiness attestation
   under the exact attempt. The readiness key is required even for a zero-vnode/idle participant.
   Writer generation and payload digests make stale or conflicting artifacts unsealable.
5. **Restorable seal.** Publish one canonical seal only after the complete vnode, descriptor, and
   participant-readiness inventory decodes and validates against one assignment generation.
   Presence without exact provenance or participant completeness is not sufficient.
6. **Write-ahead decision intent, then durable decision.** After the seal, first CAS-create and
   await an immutable intent containing the complete canonical decision. Only then CAS-create the
   commit marker with the same bytes. An unmatched intent is explicitly in-doubt and blocks
   ordinary inventory reads; startup idempotently completes that exact marker before recovery or
   reconciliation rollback. This ordering makes a cancelled or timed-out commit-marker write
   recoverably resolvable instead of invisible. A participant absent from the completed decision
   must roll back its late prepare.
7. **Completion and source acknowledgement.** Finalize the manifest and deliver the exact
   completion. Sources acknowledge only that validated attempt. A successor-epoch failure is a
   continuation fault, not a retroactive failure of the committed cut.
8. **External publication.** A designated committer consumes decided sealed inventories in order,
   validates the exact predecessor cursor, and commits Delta/Iceberg outside pipeline-stall
   latency. It re-reads external cursors on every pass and fails closed on rollback or overlap.
9. **Recovery.** Select the highest exact durable decision, require its bound manifest, seal,
   participant set, assignment generation, and deployment identity, finalize a matching Prepared
   manifest, and re-drive external publication. An undecided or excluded Prepared attempt is
   force-rolled back. Until a canonical cluster recovery capsule exists, a replacement participant
   may borrow only a metadata-only peer manifest; participant-local operators, tables, or
   watermarks fail closed rather than being guessed from a donor.
10. **Retention and shutdown.** Retention is one coalescing maintenance owner pinned by the
    external-commit floor and newest decision. Before deleting any decision, manifest, or state
    artifact, it audits every intent and publishes an immutable, deployment-scoped durable GC
    floor. That floor embeds the full canonical decision immediately below the retained window as
    its continuity anchor; readers treat every raw record below the floor as tombstoned even if a
    stale writer recreates it. Shutdown retains ownership of every issued decision task and waits
    for it to reach a terminal client-side state. A timeout leaves teardown retryable; it never
    cancels or detaches the task and releases lifecycle fences behind it. Only after owned durable
    tails settle does shutdown tear down sources/sinks and release deployment/control-plane
    ownership.

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
dimensions, or belong outside checkpoint configuration. Event-time policy has one SQL surface,
`WATERMARK FOR`; Kafka-specific event-time and out-of-orderness aliases are removed.

## Runtime-mode policy

| Runtime | Admitted semantics | Required durability/fencing |
|---|---|---|
| Embedded/local | Best effort, at least once, and connector-eligible coordinated exactly once | For exactly once: node-durable state plus the built-in local checkpoint/decision store under an exclusive OS deployment lock |
| Single-node server | Same protocol as embedded/local | Same built-in local checkpoint/decision provenance and exclusive-ownership requirements |
| Local exactly once with a configured checkpoint/object-store URL or injected decision store | **Rejected (`LDB-0014`)** | Provenance-erasing stores remain rejected until a deployment lease term fences decisions and external commits end to end |
| Cluster | At least once only | Cluster-shared state/checkpoint storage, durable membership/lease during tail settlement |
| Cluster exactly once | **Rejected (`LDB-0013`)** | Remains rejected until leader term is atomically consumed by both decision creation and external sink cursor commit |

These rejections are intentional correctness, not missing fallbacks. A renewable leader lease
alone cannot prevent an expired leader from completing a separate object-store or catalog
transaction, and an arbitrary local object-store or injected decision-store handle erases the
provenance needed to prove that the built-in OS lock fences every decision writer.

## Effective exactly-once scope and next order

The contract audit on 2026-07-12 found a narrower reachable matrix than the runtime table alone
suggests:

- local Kafka sources use engine-owned assignment for guaranteed delivery. `earliest` captures the
  full explicit topic inventory; specific offsets bind exactly the configured partition set. Both
  persist numeric next-to-read baselines before the first record and fail recovery on inventory,
  configuration, or retention drift. Patterns, broker group cursors, moving latest, and timestamp
  starts remain rejected for guaranteed delivery;
- the Kafka sink is intentionally `DurableAtLeastOnce`, never `CheckpointCommittable`:
  idempotent production and `acks=all` do not make its output transaction recoverable from an
  exact LaminarDB decision;
- commit-coupled CDC sources cannot yet align a checkpoint at an external transaction boundary;
- only coordinated append-mode Delta Lake and Iceberg are checkpoint-committable sinks; and
- Iceberg has no direct at-least-once flush path, so it is also unreachable under the default
  at-least-once setting.

Consequently, append-mode Delta Lake and Iceberg are implemented/admitted local exactly-once
candidates, not production-certified paths. Even those candidates are admitted only with the
built-in local checkpoint/decision store; a configured remote or `file://` checkpoint URL and an
injected decision store fail closed with `[LDB-0014]`. Their current connector tests do not replace
a full engine process-death/output-oracle matrix. A local Kafka-to-Delta/Iceberg path is reachable
in the implementation but remains **uncertified** until its pre-first-record, process-death,
restart, and partition-topology cases pass. Kafka-to-Kafka remains at-least-once because the Kafka
sink is not `CheckpointCommittable`. Kafka append is multi-writer; compacted upsert is singleton
until writer-generation ordering is fenced across handoff.

Kafka source group offsets are progress telemetry, not recovery authority. After a LaminarDB
checkpoint commits, the source enqueues the corresponding broker offset asynchronously and records
the eventual callback outcome as a metric. A broker-progress failure must not restart or invalidate
the already-decided engine cut; recovery always uses the sealed source checkpoint. This follows the
current Flink Kafka source contract, which likewise treats broker commits as monitoring progress
rather than fault-tolerance state
([Kafka source offset committing](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/kafka/#consumer-offset-committing)).

Do not restore the former inline Kafka transaction path. A process can die after the engine's
durable decision but before `commit_transaction`; a new producer with the same transactional ID
fences the old session and Kafka aborts its unresolved transaction, but LaminarDB has no durable
committable from which to reproduce output that was aborted before publication. Kafka's
transaction protocol requires stable transactional IDs and
atomic commit markers ([KIP-98](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/66854913/KIP-98%2B-%2BExactly%2BOnce%2BDelivery%2Band%2BTransactional%2BMessaging)); moving partition ownership also
requires group-generation fencing ([KIP-447](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/103093950/KIP-447%2BProducer%2Bscalability%2Bfor%2Bexactly%2Bonce%2Bsemantics)). Flink likewise makes
Kafka transactions checkpoint-owned, requires a stable transactional-ID prefix, and documents
checkpoint-bounded visibility and transaction-timeout constraints.

Implement the remaining work in this order:

1. **Certify the current corrective protocol.** Prove write-ahead decision ambiguity, follower
   assignment-cut validation, fail-closed vnode rehydration, batch/cursor rotation fencing, and
   latest-pointer corruption before another feature is admitted. Run process-death output oracles,
   not only unit models.
2. **Make the existing matrix truthful and useful.** Use the typed `Embedded | Cluster` server
   boundary and remove the unused Raft coordination settings; embedded and standalone API hosting
   share the local checkpoint protocol. Keep consistency, topology, and input mode derived from
   connector contracts. Certify local Kafka initial, pre-first-record, restart, and topology-drift
   behavior. Give Iceberg a real direct-append at-least-once path (or prove that its coordinated
   protocol is safe under an at-least-once request).
3. **Build the canonical cluster recovery capsule.** This is the first remaining availability
   blocker because cluster at-least-once already admits stateful pipelines, while a replacement
   participant cannot safely reconstruct participant-local operator, table, or watermark state.
   Seal a decision-bound global recovery image, not an arbitrary participant's local manifest.
   It must bind the assignment, every participant readiness digest, source-offset union,
   cluster-min watermark, operator/table-state roots, and any materialized-view metadata needed
   after ownership changes. Keep the current metadata-only peer bootstrap as a narrow
   optimization; reject non-portable donor state until this capsule is implemented and
   fault-tested.
4. **Certify source transaction cuts.** Add a deadline-bounded async barrier-prepare hook for
   commit-coupled CDC. PostgreSQL CDC must either drain through the current database transaction
   before emitting its barrier or persist the transaction identifier, event ordinal, and in-flight
   payload. Merely removing its admission rejection can replay a partially emitted transaction.
5. **Add recoverable Kafka sink committables, local first.** Stage encoded record segments durably
   before the seal and keep participant descriptors small and checksummed. After the exact engine
   decision, a designated committer opens a fresh Kafka transaction, publishes the staged records
   plus a namespaced exact cursor marker atomically, and resolves ambiguous commits by reading that
   cursor with `read_committed`. Derive the transactional ID from pipeline identity, deployment,
   sink, and participant; do not add a public writer-ID dimension. Reject a non-transactional DLQ
   under exactly-once and validate transaction timeout against checkpoint plus recovery bounds.
   Preserve the current direct producer path for low-latency at-least-once operation.
6. **Build a linearizable cluster decision authority.** Complete AD-0 from
   `cluster-production-readiness.md`: use the authoritative Postgres control store to insert the
   seal-bound decision in a transaction conditioned on the current owner and fencing term. The
   removed legacy `strategy = "raft"`/`raft_port` settings never constituted a Raft implementation
   and cannot justify removing `LDB-0013`.
7. **Fence and certify every external committer.** External cursors must carry the exact
   predecessor, deployment namespace, and decision fence. Run stale-leader, network-partition,
   rebalance, process-death-before/after-decision, ambiguous Kafka commit, mixed-sink, and
   transaction-timeout matrices before admitting cluster exactly-once.

Keep consistency, topology, and input mode as derived connector contract dimensions rather than
user options. The public delivery setting remains the requested end-to-end minimum; diagnostics
and status APIs should also expose the effective source/state/sink guarantee so a narrower matrix
cannot be mistaken for broad exactly-once support.

## Exit gates

- All-target compilation for core, connectors, DB (local and `cluster`), and server.
- Deterministic tests for decision ambiguity, exact seal inventory, cursor rollback/overlap,
  successor-epoch failure, total attempt deadline, decision-floor monotonicity/continuity,
  retention coalescing, and owned-task shutdown quiescence.
- Fault-injection/soak runs for process death before/after seal, during decision creation, during
  external commit, and during shutdown.
- Latency verification separates pipeline stall, restorable-gate wait, durable completion, and
  external-commit lag; retention work must never appear on the source-ack critical path.
- Dead-code and configuration audit after every feature cycle.

The previous local gate counts are intentionally not reused as certification evidence: the current
cycle changes decision persistence, follower preparation, source rotation, and rehydration. Strict
Clippy, all-feature compilation, complete local suites, public cluster adoption, and the full Linux
fault matrix must be rerun on the final commit and recorded with that exact SHA.
The first Linux fault matrix (`29169226798`) completed four embedded kill-9 rounds with aggregate
state continuity, 908 demotions, and 31,025 cold-state fetches. Its cluster legs exposed an
unaligned rkyv vnode payload returned from object storage after the first fault; object-store reads
now normalize that buffer once and the decoder also protects custom backends. The deliberately
misaligned regressions pass locally; the production matrix must pass on the corrective commit
before release.

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

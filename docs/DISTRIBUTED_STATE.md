# Distributed state

- **Status date:** 2026-08-04
- **Checkpoint core:** implemented
- **Cluster state matrix:** incomplete and fail-closed
- **Production validation:** pending the final soak matrix

## State and durability

All deployment tiers use the same in-memory state machinery. Keyed aggregates and supported
interval joins use concrete per-vnode maps; embedded and single-node runtimes own every vnode,
while clusters spread those vnodes across nodes. There is no state-backend trait, embedded state
database, local durable state, spill tier, or compatibility path.

The only durable recovery authority is an immutable committed checkpoint plus replayable source
offsets. Each participant writes one node-data object per checkpoint. The manifest records exact
byte ranges and SHA-256 digests for complete dirty-vnode frames and directly references unchanged
frames in older objects. The node-data object is created first; the immutable manifest is created
last as the participant-readiness marker. PUT count therefore scales with participant count, not
vnode count.

The committed index binds the deployment, pipeline ABI, checkpoint attempt, participant manifests,
source offsets, per-channel watermarks and idle flags, assignment fence, and predecessor index.
Local recovery selects it from one authoritative decision head; cluster recovery uses the
leader-fenced authority chain. Recovery verifies each manifest and range, restores only the local
participant frames for an unchanged assignment, and restores target-owned ranges from their
committed donors for a direct successor. Successor recovery requires the authority's exact pinned
handoff cut; skipped generations fail closed. Sources then replay from the committed offsets. It
never scans checkpoint objects to infer state references.

Retention keeps only the authoritative latest recovery cut because its manifests are a complete
logical inventory and directly reference every older live chunk. A bounded two-phase cursor
retires one exact predecessor at a time: data first, then manifests and the committed index. Local
mode updates a dedicated durable head under its namespace lease; shared-store updates use native
conditional writes, and cluster mode records every transition in the leader-fenced authority
sequence. Restart or takeover resumes the durable phase without LIST or a pre-scan of checkpoint
history; the O(1) cursor advances toward the prior durable floor or genesis. Validated per-manifest
frame counts determine chunk liveness. Committed drains and failure-recovery assignments bind
their exact state cut in the same authority sequence. Retention pins that cut across leader
takeover until a complete checkpoint commits under the target assignment.

Checkpoint storage is provider-neutral through the `object_store` crate. Configuration selects
local filesystem, S3 (including R2/MinIO through S3 endpoint options), GCS, or Azure Blob by URL.
LaminarDB does not add provider-specific retry, multipart, backoff, or CAS code. Startup probes the
conditional-write primitive required by checkpoint publication and fails closed when the store
cannot prove it.

ALO and EO use identical operator state. Their difference is the admitted source replay and sink
commit contract; checkpoint-committable sinks publish only after the committed index is durable.

## Current capability boundary

The runtime checkpoints aggregate accumulators, window/session state, bounded stream-join buffers,
managed temporal ASOF history, timers, channel progress, source offsets, reference-table history,
and materialized-view images. Keyed aggregation and supported stream joins route by stable vnode
in local and cluster execution. Live assignment changes range-read the exact committed donor
frames behind the rotation fence and publish the prepared operator transition only after the
target assignment and shuffle authority match. Reference-table and materialized-view images remain
whole-node frames; cluster plans requiring them fail closed until they have assignment-fenced vnode
ownership and restore.

The following remain production gaps, not alternate state implementations:

- Durable Aborts reclaim exact prepared objects, including after restart. An attempt that crashes
  before publishing a terminal decision can still leave unreferenced checkpoint metadata.
- Checkpoint capture still performs state-sized snapshot work synchronously; its tail latency is
  not certified.
- Temporal ASOF has vnode-keyed final-only execution, compact per-source-partition replay
  frontiers, checkpoint, recovery, and rescale machinery. SQL admission remains closed until the
  managed graph path, source-role contracts, execution limits, and output schema are certified.
- Mutable update/merge joins, materialized-view joins, unbounded retention policy, and the complete
  cluster window/MV matrix still need planner/runtime certification.
- Cluster EO remains connector-gated; unsupported source/sink compositions are rejected before I/O.
- Production readiness remains unclaimed until correctness, recovery, skew, and latency gates pass.

## Validation gate

Final certification requires real-connector soaks for single-node ALO, single-node EO, three-node
cluster ALO, and three-node cluster EO. The matrix uses two replayable inputs, Zipfian keys,
checkpoint/restart and process-kill faults, an independent result oracle, checkpoint-progress
checks, memory bounds, and explicit p50/p95/p99 latency gates. ALO may replay output but may not lose
admitted records or invent results. EO may neither lose nor duplicate externally visible output.

[ADR-008](architecture-decisions/ADR-008-managed-vnode-keyed-state.md) records the decision and its
research basis.

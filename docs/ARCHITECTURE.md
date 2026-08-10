# Architecture

LaminarDB is an Arrow-native streaming SQL engine that runs embedded, as a standalone node, or as
a cluster. Every deployment tier uses the same operator graph and vnode-partitioned in-memory
state. Deployment changes process ownership and transport, not state semantics.

## Runtime

The runtime has three planes:

- The record path runs one `StreamingCoordinator` task on a dedicated current-thread Tokio
  runtime. It aligns source batches and barriers, executes compiled projections or cached
  DataFusion plans, updates operator state, and emits batches.
- Background I/O tasks run connectors, checkpoint persistence, sink publication, and retention on
  the main work-stealing runtime. Object storage is never consulted for a record-path state lookup.
- The control plane exposes lifecycle, DDL, metrics, HTTP/pgwire administration, discovery, leader
  fencing, and vnode assignment.

Source connectors deliver `RecordBatch` values through bounded channels. The coordinator applies
event-time extraction and watermark progress, routes rows to the owning vnode, executes operators,
then sends output to named streams and sinks. Cluster shuffles use local channels when the owner is
in process and fenced remote transport when it is not.

The main execution tiers are compiled single-source projections, incremental keyed aggregation,
specialized window state, bounded interval joins, and cached DataFusion plans. Unsupported
unbounded physical operators are rejected before execution.

## Stateful execution

Keyed aggregates and supported interval joins use concrete in-memory maps and buffers partitioned
by stable vnodes (256 by default). Vnodes are their unit of routing, ownership, checkpoint framing,
restore, and rescale. Whole-node materialized-view and reference-table frames are not admitted in
cluster plans that require distributed ownership.
There is no state-backend trait, local state database, disk spill, or alternate cluster state
machinery. State exceeding configured memory/work limits fails explicitly.

Embedded and single-node runtimes own every vnode. Cluster nodes own the vnodes in the current
assignment fence. Exact committed-assignment recovery completes before graph launch. Live
reassignment range-reads the exact committed donor frames behind the rotation fence and publishes
the prepared operator transition only after acquired and revoked vnode state, replay/frontier state,
the target assignment, and shuffle authority form one validated cut. Drain and failure-recovery
decisions retain that cut until the target assignment completes a checkpoint.

The detailed implemented boundary and remaining SQL gaps are in
[Distributed state](DISTRIBUTED_STATE.md). The accepted design and research basis are in
[ADR-008](architecture-decisions/ADR-008-managed-vnode-keyed-state.md).

## Checkpoints and recovery

Checkpoint barriers establish one exact attempt across source positions, operator state, channel
watermarks/idle flags, timers, and sink pre-commit state. Each participant writes one immutable
node-data object. Its manifest indexes complete state frames by byte range and digest and directly
references unchanged frames in older node objects. Node data is created first; the manifest is
created last and is the participant-readiness marker.

After all required participant manifests are verified, the coordinator creates one immutable
committed checkpoint index and records the terminal outcome. The index binds:

- deployment and pipeline ABI identity;
- canonical epoch/checkpoint identity;
- local or cluster assignment and participant roster;
- exact participant manifest and node-object digests;
- source offsets and per-channel event-time progress;
- the prior committed index used by retention.

Recovery selects an exact committed outcome/index, verifies participant manifests and requested
ranges, restores the local runtime image, then starts sources at the stored offsets. It does not
fall back to a different checkpoint when validation fails.

Retention keeps the authoritative latest cut and follows exact predecessor references through a
crash-resumable two-phase cursor. It deletes unreferenced data before deleting each retired cut's
manifests and committed index. Local cursor updates use a dedicated durable head under the local
namespace lease; shared-store updates use native conditional writes and cluster updates are
leader-fenced authority records. No phase discovers liveness by listing objects or materializes
the predecessor chain; one O(1) cursor advances to the prior floor or genesis.

Checkpoint URLs are built through the `object_store` crate for local files, S3 (including
S3-compatible stores through endpoint options), GCS, and Azure Blob. Startup probes the
conditional-write operation required by durable publication and fails closed if the configured
store cannot prove it. Provider retries, multipart, backoff, and conditional operations remain
library-owned.

## Delivery contracts

Sources and sinks declare typed consistency, topology, input-mode, replay, and commit contracts.
Admission validates the complete pipeline before connector I/O.

- Best-effort may lose accepted input across a failure.
- At-least-once restores the committed state and replays source offsets; downstream output may be
  repeated.
- Exactly-once uses the same operator state but requires exact-certified replayable sources and a
  checkpoint-committable sink. The sink stages an epoch, records an immutable prepared descriptor,
  and publishes only after the committed index is durable. Recovery completes a committed but
  unpublished external cursor before admitting new input.

Exactly-once is connector-composition-specific, not a blanket property of a connector type.
Unsupported combinations fail before external I/O. Current connector certification is documented
in the repository README and tested by the final real-connector soak matrix.

## Cluster control

Cluster mode adds:

- static or Chitchat discovery;
- a renewable shared-store leader lease;
- CAS-published vnode assignments with process-incarnation fencing;
- assignment-fenced local/remote row shuffle;
- direct leader/follower checkpoint barrier RPCs;
- a shared checkpoint object store visible to every participant.

LaminarDB does not embed a consensus service. Durable shared-store leases and conditional updates
fence leader-only mutations; process incarnation and assignment digests fence data-plane work.

## Crates

- `laminar-core`: channels, event time, checkpoint formats/stores, cluster control and shuffle.
- `laminar-sql`: parser, streaming extensions, planning, validation, and DataFusion integration.
- `laminar-connectors`: source/sink implementations, formats, schemas, and connector contracts.
- `laminar-db`: database facade, operator graph, coordinator, state, checkpointing, and recovery.
- `laminar-server`: standalone configuration, lifecycle, HTTP, pgwire, and metrics.
- `laminar-derive`: record and connector proc macros.

## Production gate

The architecture is implemented, but production readiness is not claimed until the final
single-node/cluster by ALO/EO soak matrix passes correctness, recovery, Zipfian-skew, memory, and
latency gates. See [Distributed state](DISTRIBUTED_STATE.md#validation-gate).

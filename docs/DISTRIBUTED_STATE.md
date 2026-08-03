# Distributed state

- **Status date:** 2026-08-03
- **Decision:** accepted
- **Implementation:** coordinator cutover in progress
- **Production validation:** pending the full soak matrix

## Current implementation

Operator working state is in memory, and the public state-backend selector has been removed. The
standalone server derives its temporary checkpoint-artifact backend from the single checkpoint URL.
Aggregate and window hot maps use FxHashMap where their current layouts permit it.

The coordinator still writes the version 6 manifest, operator sidecars, and legacy per-vnode
StateBackend artifacts. Some operator layouts and local/cluster paths are not yet unified. Those
are migration code, not the production target. No release should claim the target checkpoint shape
or production readiness until that code is deleted and the final soaks pass.

## Settled target contract

Every deployment uses the same state machinery. Embedded and single-node runtimes are one node
owning every vnode and use local shuffle channels. A cluster runs the same operators with vnodes
spread across nodes and remote shuffles only when ownership crosses a node boundary.

Vnodes are the unit of ownership, routing, checkpointing, restore, and rescale for joins,
aggregations, windows, sessions, timers, temporal history, and materialized views. Authoritative
working state is a concrete per-vnode FxHashMap; there is no hot-state trait, local state database,
spill tier, or runtime backend selector.

A checkpoint contains window panes and open windows, session intervals, aggregation accumulators,
join buffers and retained ASOF history, per-channel watermarks and idle flags, timers, source
offsets, and connector recovery metadata.

Each node writes one immutable data object per checkpoint. Dirty-vnode frames are concatenated and
the manifest records their byte ranges and digests, so restore reads only assigned vnodes and PUT
count scales with nodes rather than vnode count. Committed manifests carry explicit chunk reference
counts for direct garbage collection; listing is never used to infer live references.

Checkpoint storage is selected by URL and built through the object_store crate. The target path
supports local files, S3 and S3-compatible R2/MinIO, GCS, and Azure Blob. Provider retry, backoff,
multipart, and conditional-write behavior remain in object_store. Manifest publication uses a
conditional put, and startup must fail unless a bounded capability probe proves the required Create
or Update semantics.

Recovery restores the latest committed checkpoint, then replays each source from offsets in that
checkpoint. ALO and EO share operator state; their difference is the source, checkpoint-publication,
and sink-commit contract.

## Current capability boundary

The distributed join currently admits INNER, LEFT, RIGHT, FULL, LEFT/RIGHT SEMI, and LEFT/RIGHT
ANTI over its supported bounded append-only shape. Mutable relation state, resumable hot-key
fanout, temporal joins, cluster windows, and materialized views remain gaps.

Cluster EO remains connector-gated. Exact-certified Kafka input with coordinated direct S3/S3A
Delta publication is the admitted cluster composition. Kafka and Iceberg sinks remain ALO; other
EO combinations fail before connector I/O. Single-node replay-capable delivery currently requires
the fenced local filesystem checkpoint path; remote single-node writer fencing is not yet admitted.

## Remaining cutover

1. Atomically replace the legacy manifest writer and reader with one node object, vnode byte ranges,
   complete vnode frames, per-channel event-time state, and direct range restore.
2. Add manifest-led refcount GC, then delete LIST-based artifact GC, StateBackend, its
   implementations, configuration type, integration tests, and builder plumbing.
3. Finish the remaining operator and local/cluster topology migrations without alternate state
   machinery.
4. Add term-fenced remote single-node checkpoint publication without a second state path.
5. Run deterministic correction oracles and the single-node/cluster × ALO/EO Zipfian fault and
   latency soak matrix.

## Evidence gate

Production certification requires real-connector validity soaks for single-node ALO, single-node
EO, three-node cluster ALO, and three-node cluster EO. Each run uses two Kafka inputs, Zipfian keys,
process kills and recovery, an independent output oracle, checkpoint-progress checks, and explicit
latency gates. ALO may duplicate replayed output but may not lose admitted records or invent
results. EO may neither lose nor duplicate externally visible output.

[ADR-008](architecture-decisions/ADR-008-managed-vnode-keyed-state.md) records the decision and its
research basis.

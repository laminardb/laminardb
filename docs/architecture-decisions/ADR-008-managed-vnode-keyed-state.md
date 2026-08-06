# ADR-008: In-memory vnode state with object-store checkpoints

- **Status:** accepted; production validation pending
- **Updated:** 2026-08-04
- **Current status:** [distributed state](../DISTRIBUTED_STATE.md)

## Decision

LaminarDB keeps authoritative working state in concrete per-vnode `FxHashMap` layouts. Vnodes are
the unit of routing, ownership, checkpointing, restore, and rescale for joins, aggregations,
windows, sessions, timers, temporal history, and materialized views. Keyed aggregates, supported
interval joins, and managed SQL temporal ASOF plans implement this layout now. Temporal production
certification and conversion of remaining whole-node materialized-view and reference frames are
still required.

Embedded and single-node deployments use one node that owns every vnode and routes shuffles over
local channels. Cluster deployments run the same stateful operators with vnodes spread across nodes.
There is no storage trait for working state, embedded state database, local durable state, spill
tier, or runtime selector.

The only durability mechanism is a checkpoint written through the `object_store` crate. Each node
concatenates its dirty-vnode chunks into one checkpoint data object. The manifest indexes every
vnode by byte range and records the state required for exact recovery: window panes and open
windows, sessions, aggregate accumulators, bounded join buffers, temporal ASOF history, versioned
reference-table history, bounded per-source-partition replay frontiers, channel watermarks, idle
flags, timers, and source offsets.

Node data is created before its immutable manifest; the manifest is created last as the participant
readiness marker. Recovery restores the latest committed checkpoint and replays sources from its
offsets. Range GETs load only the vnodes assigned to the recovering node. After an assignment
handoff, restart may use only the exact authority-pinned predecessor cut and only for its direct
successor; assignment chains fail closed. Retention keeps that one complete recovery cut and
retires exact predecessors through a crash-resumable data-then-metadata cursor. Validated manifest
chunk counts and frame references drive deletion; object listing is never used to infer references.

Checkpoint configuration is a provider-neutral URL. S3, GCS, Azure Blob, S3-compatible R2 and
MinIO, and local filesystem storage use the same `object_store` path. LaminarDB does not add
provider-specific retry, backoff, multipart, or CAS implementations. Manifest-last publication uses
a conditional create, and startup fails unless a capability probe proves that the configured store
honors it.

At-least-once and exactly-once use identical operator state. Their difference is the composition of
source replay, checkpoint publication, and sink commit contracts.

## Rationale

- [Megaphone](https://www.vldb.org/pvldb/vol12/p1002-hoffmann.pdf) shows that stable fine-grained
  key bins let state move incrementally without stop-the-world migration.
- [Styx (2026)](https://link.springer.com/article/10.1007/s00778-026-00971-x) reinforces
  fine-grained key-set migration and asynchronous snapshots as tail-latency controls.
- [Flink 2 disaggregated state](https://www.vldb.org/pvldb/vol18/p4846-mei.pdf) demonstrates that
  remote hot state needs asynchronous access and caching to recover its I/O cost. LaminarDB keeps
  object storage off the record path instead of adding that machinery before measurements require
  it.
- Flink's [SharedStateRegistry](https://nightlies.apache.org/flink/flink-docs-release-1.15/api/java/org/apache/flink/runtime/state/SharedStateRegistry.html)
  provides the production precedent for explicit shared-checkpoint reference tracking.

## Consequences

- Record-path state lookup remains an in-memory hash lookup with no storage I/O.
- Object-store PUT count scales with node count and checkpoint frequency, not vnode count.
- One vnode representation serves local execution, cluster execution, and rescaling.
- Operators that have not adopted vnode ownership remain unavailable in cluster mode.
- State larger than available memory is an explicit admission or capacity failure, not a silent
  spill or alternate execution mode.
- A committed checkpoint plus replayable source offsets is the complete recovery authority.
- Production readiness remains unclaimed until the four-mode real-connector soak matrix passes.

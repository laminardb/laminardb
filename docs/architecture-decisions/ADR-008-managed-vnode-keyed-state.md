# ADR-008: One managed vnode-state lifecycle

- **Status:** accepted; production validation pending
- **Updated:** 2026-07-31
- **Working state target:** one worker-local TidesDB instance
- **Current status:** [distributed state](../DISTRIBUTED_STATE.md)

## Decision

LaminarDB uses one lifecycle for stateful operators: stable identity, vnode ownership, capture,
bounded restore, off-side preparation, fenced publication, revocation, accounting, and recovery.
Every stateful operator consumes the same signed relation changes and stores its keyed indexes in
that lifecycle. Operator-specific key and value encodings are allowed, but alternate state engines,
ownership paths, checkpoint paths, and compatibility formats are not.

`StateBackend` is the durable checkpoint-artifact boundary. Its in-process, local-disk, and object-
store implementations change artifact placement only; they do not serve row-path lookups. Committed
seals and recovery capsules remain authoritative.

Working state uses one TidesDB database and one retained column family per worker. TidesDB owns
memory-resident memtables, the shared block cache, WAL, flushing, compaction, and local-disk spill;
LaminarDB does not duplicate those functions. Logical prefixes identify pipeline, operator, state
table, vnode, ownership generation, and operator key. A processing batch changes all of its indexes
in one transaction.

Production configuration supplies an explicit engine memory budget and local-disk reserve. Writes
flush from bounded memtables into local SSTables, and frequently read blocks stay in the block
cache. Auto memory sizing, one database or column family per vnode, engine object-store mode, and a
second Laminar-managed RAM/spill tier are rejected.

TidesDB local data is disposable and is not the EO authority. `StateBackend` continues to store
portable committed checkpoint artifacts through the existing provider-neutral object-store path.
An uncertain local transaction outcome fail-stops the worker attempt; recovery restores the last
committed vnode state and replays its source input.

TidesDB is the sole selected embedded engine, with no RocksDB fallback or runtime selector. Native
9.3.15 has the required incomplete-batch rejection fix. Integration is gated on publication and
qualification of the matching official Rust package; LaminarDB does not ship a fork, git dependency,
or vendored native copy while that release is unavailable.

## Supported distributed operators

The distributed join is a bounded, append-only directional event-time equi join over one or more
ordered `BIGINT`/`VARCHAR` keys. `INNER`, left/right/full outer, left/right semi, and left/right anti
joins use the same vnode state in single-node and cluster mode. Each side must be a directly
watermarked source, and every join has a positive finite time bound.

Join state is held per vnode. Checkpoints capture the exact owned roster, restore validates identity
and lineage before decoding, and rebalance prepares replacement vnodes before an infallible fenced
swap. Watermark advancement expires idle as well as active vnodes. Cross, as-of, general non-equi,
unbounded, multiway, and intermediate-input joins remain rejected.

Keyed grouped aggregates may consume a named join pipeline and use the same lifecycle. A fused
`JOIN ... GROUP BY` stage and cluster event-time window aggregates remain rejected.

## Delivery composition

State correctness does not by itself provide exactly-once output. Runtime admission composes source,
state, checkpoint decision, and sink contracts:

- at-least-once requires replayable input and a durably acknowledged sink;
- exactly-once requires exact-certified input and a checkpoint-committable sink; and
- cluster exactly-once additionally requires cluster-certified source handoff and immutable external
  publication.

The admitted cluster exact composition is Kafka source input to direct S3/S3A append-mode Delta
Lake; production certification remains gated on the full soak matrix.
Kafka output remains at-least-once. Azure/GCS Delta require provider fault soaks before cluster-EO
certification. Iceberg remains durable at-least-once because format-level atomic commits do not
supply LaminarDB's checkpoint committer/cursor. Other exact combinations fail admission before I/O.

## Resource and failure rules

The graph accounts logical live, prepared, and retired state, including overlap during ownership
transition. Record processing updates only touched state. Join materialization is capped by rows and
bytes per cycle; exceeding a cap causes a terminal controlled failure instead of an unbounded
allocation.

Validation completes before owned decode or publication. A failure after stateful mutation begins
requires recovery and withholds affected output. Logical accounting does not claim allocator, RSS,
transport-buffer, or storage-client coverage; the soak matrix measures those operational effects.

## Consequences

- Single-node and cluster joins share one state and recovery implementation.
- ALO and EO differ by connector and commit contracts, not operator state machinery.
- Checkpoint storage can vary without changing the single TidesDB hot-state path.
- Unsupported shapes fail closed instead of entering legacy formats or fallback operators.
- Production readiness remains unclaimed until the four-mode real-connector soak matrix passes.

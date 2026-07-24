# Working-state placement and backend scope — Cycle 20

- **Date:** 2026-07-24
- **Question:** does cluster keyed state require RocksDB, Fjall, or another database?
- **Answer:** a managed mutable working-state layer is required; no named engine and no LSM is
  intrinsically required
- **Recommended broad-state profile:** one qualified worker-local embedded store plus portable
  cluster-shared checkpoints
- **Additional profile to prove:** hard-bounded in-memory working state plus the same portable
  checkpoints
- **Production backend selected:** none
- **Evidence:** code inspection and current primary-source review; no candidate or product run
- **Admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`

## Decision

LaminarDB needs the behavior of a state engine, not RocksDB or Fjall by name. Keyed aggregates,
windows, joins, timers, output bookkeeping and checkpoint generations need byte-governed point and
ordered-range access, atomic batch mutation, consistent generation freeze, deterministic export and
restore, vnode cleanup, and pressure/error observation. An LSM is one implementation of that
contract; a B-tree or a strictly bounded in-memory implementation can satisfy it too.

A durable local database is not what makes distributed state correct. The existing
`laminar_core::state::StateBackend` writes immutable checkpoint-attempt vnode artifacts and seals an
exact attempt; it deliberately has no live get, scan, timer or mutation-batch API. Shared object
storage, checkpoint decisions and ownership epochs remain recovery authority. Local working state
is disposable after a committed cut.

The production choice is therefore profile-scoped:

1. **Bounded-memory:** viable only when admission and runtime reservations hard-bound live state,
   timers, join rows, indexes/output bookkeeping, skew, active and frozen generations, and restore/
   replay scratch within an approved worker-memory envelope. Node loss means remote restore plus
   source replay, so source retention and RTO are gates. There is no silent eviction or automatic
   fallback after exhaustion.
2. **Local-spill:** required for the intended general profile where live state can exceed its RAM
   budget. A qualified embedded store on worker-local NVMe is the recommended first architecture.
   Calls are coalesced per Arrow batch and blocking/cold work runs on bounded non-event-loop lanes.
3. **Remote-primary:** possible, but not a drop-in backend. It needs asynchronous state access,
   local caches and write buffers, version/lease authority, compaction and garbage collection,
   network-tail isolation, and portable export. It remains a separate future ADR.

This means a bounded Phase 1 lifecycle slice need not wait for a disk-engine winner after its
applicable contract gate: the common service and its in-memory conformance implementation can prove
vnode state, timers, generation freeze, artifacts, restore and fencing while Phase 0 backend
qualification remains open. It neither completes Phase 1 nor changes admission. Cluster production
admission still requires either a separately qualified bounded-memory profile or a qualified local-
spill backend. The broad profile must not ship on the memory implementation by default.

## Authority boundaries

| Concern | Required authority | What a local engine does not provide |
|---|---|---|
| Live operator state | `ManagedWorkingState` scoped by pipeline/operator/table/vnode and ownership epoch | Cluster ownership or a durable checkpoint decision |
| Recovery state | Immutable portable vnode artifacts and exact-attempt seal in cluster-shared storage | A local engine directory is never the only node-loss or upgrade artifact |
| Rebalance | Controller assignment/process fence plus restore-before-activate and revoke-before-delete | Local rows cannot authorize their owner or epoch |
| Delivery | Certified source cursor/handoff, checkpoint decision and sink commit/fencing | Local `fsync`, WAL or transaction cannot atomically commit an external source and sink |

End-to-end exactly-once remains a composition claim:

```text
replay-stable, fenced source position
  + one state/checkpoint cut and durable decision
  + transactional or idempotent, fenced sink publication
```

Memory-only state can participate in exactly-once after total process loss when the sealed artifact
is complete and the source is replayable. Conversely, an immediately durable RocksDB/Fjall/redb
commit cannot create exactly-once for an uncertified source or sink. A non-replayable source needs a
durable ingress log or stays outside the claim.

## Placement matrix

| Architecture | Hot-path and capacity consequence | Recovery/rebalance consequence | Disposition |
|---|---|---|---|
| Hard-bounded in-memory + shared checkpoints | Lowest local access overhead, but all charged live/frozen/scratch state and worst-case skew must fit RAM; allocator retention and snapshot-copy tails still count | Cold restore and source replay after every process loss; full/delta chains and source retention must meet RTO | **Prove as a separate small-state profile.** The current flat operator maps are not this service. |
| Embedded local store + shared checkpoints | Local cache/memtable for hot state and disk capacity for cold state; compaction, page faults, writer contention, sync and disk pressure enter p99.9 | Local files may accelerate restart but are disposable; portable vnode restore remains authoritative | **Recommended general profile**, after one engine passes the common campaign |
| Object-store-primary embedded engine | Cold reads and durable writes are async and inherit remote latency/cost; requires caching, batching and failure isolation | Shared immutable files can reduce restore copying, but version ownership, fencing, compaction and GC become a subsystem | **Deferred architecture**, not an extra current candidate |
| Remote transactional KV/database | Network/service queueing enters every miss/range path; client cache introduces coherence and version questions | Needs a snapshot/version that composes with Laminar's cut and a portable export; still needs vnode epoch fencing | **Reject as generic initial backend**; evaluate only a named service after measured need |
| Kafka state changelog | Fast local state is still required; mutation traffic and recovery become Kafka-coupled | Coherent when source partitions, tasks, changelog and transactions all use Kafka; creates dual authority for other connectors | **Reject as connector-neutral authority** |
| Custom Laminar state engine/service | Full control at the cost of owning transactions, storage formats, compaction, recovery, corruption and operations | Adds another distributed failure domain and long-lived database-team obligation | **Reject initially**; revisit only from measured strategic need |

[Flink 2.3's state-backend documentation](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/state/state_backends/)
explicitly separates heap or embedded RocksDB/ForSt working state from checkpoint storage. The
[Arroyo concepts documentation](https://doc.arroyo.dev/concepts/) is a Rust precedent for in-memory
operator state with remote Parquet checkpoints, while explicitly documenting worker memory as its
capacity bound. [SlateDB](https://github.com/slatedb/slatedb) shows a current Rust object-store LSM,
but also documents the remote latency/API-cost tradeoff and uses asynchronous durability, batching
and local caching. These are evidence that each placement is viable in principle, not LaminarDB
qualification evidence.

## Embedded-engine screen

The current active work should not expand into a many-engine benchmark. Candidate count is not risk
coverage: every engine still has to close the same atomicity, hot-writer/victim tail, byte/resource,
persistence, export/restore, fault and endurance obligations.

| Engine or family | Useful fit | Blocking concern | Cycle 20 disposition |
|---|---|---|---|
| RocksDB 10.4.2 via `rocksdb` 0.24.0 | Mature ordered LSM, atomic batches, snapshots, range operations, checkpoints and broad controls | Native/C++ build and memory accounting; the pinned binding still lacks the complete reviewed maintenance/error/stall observation | **Continue only the already-scoped source-closure track; not selected** |
| Fjall 3.1.8 | Rust-native LSM with atomic cross-keyspace batches, snapshots and ordered ranges | Stock scheduler/lifecycle, global pressure control and stable maintenance/error/stall surfaces do not close DKS-Q2-006 | **Re-enter only with explicit fork/upstream ownership; not selected** |
| redb 4.1.0 | Rust-native transactional B-tree, ordered ranges, MVCC snapshots and no LSM workers | Exactly one blocking database writer; commit/reclamation/resize/close and recovery tails need the already-designed native prescreen | **Keep the approved bounded prescreen; not selected** |
| `heed` 0.22.1 / LMDB | Mature ACID mmap B-tree, named databases, ordered cursors, consistent reads and no maintenance workers | One writer, fixed-map/resize coordination, reader-pinned pages, mmap cold faults, C/unsafe-open boundary and vnode export adapter | **Conditional prescreen after redb**, only if redb fails or mature LMDB behavior offers a stated decision benefit |
| SQLite WAL / `rusqlite` | Very mature transactions, B-tree ranges, recovery and checkpoint controls | One writer; SQL/VDBE/row overhead, `SQLITE_BUSY` modes and reader-starved WAL checkpoint tails duplicate rather than remove the B-tree risk | **Paper fallback**, not a current candidate |
| libMDBX | Strong ordered/transaction/snapshot/backup surface and better geometry controls than LMDB | One writer, long-reader retention and mmap/sync tails; current public-source/binding governance would create native-fork ownership | **Hold**, no prescreen now |
| SlateDB 0.13.x | Active Rust object-store-native LSM with ranges, transactions, caching and asynchronous durability | Remote state changes the execution and checkpoint model; API compatibility is not yet guaranteed | **Future remote-primary research**, not local-backend bake-off |
| sled, ParityDB, Persy, Nebari, Tonbo, SurrealKV | Each supplies some Rust-native tree/log/storage primitives | Upstream beta/experimental/alpha status, workload mismatch, unresolved crash/snapshot/lifecycle issues, or already-proved pinned-source defect | **Reject or watch only**; no implementation prescreen now |

Primary evidence for the new alternatives is the [LMDB API](https://www.lmdb.tech/doc/group__mdb.html),
[`heed` API and environment constraints](https://docs.rs/heed/latest/heed/struct.EnvOpenOptions.html),
[SQLite WAL concurrency/checkpoint rules](https://www.sqlite.org/wal.html),
[libMDBX limitations](https://github.com/Mithril-mine/libmdbx),
[sled's own production warnings](https://github.com/spacejam/sled),
[ParityDB's workload and durability description](https://github.com/paritytech/parity-db), and
[Tonbo's alpha status](https://github.com/tonbo-io/tonbo). None supplies Laminar's vnode checkpoint,
ownership, delivery or independent-soak lifecycle.

DKS-Q2-006 is therefore not a demand for RocksDB metrics. For an LSM it requires truthful bounded
pressure, maintenance progress/failure and exact foreground-stall evidence. An engine with no
background worker may prove that maintenance arm not applicable, but sole-writer wait, commit/sync,
page/file growth, snapshot retention, terminal errors and recovery remain common vetoes. A memory
profile replaces engine-maintenance evidence with hard reservation, allocator/RSS, frozen-
generation, checkpoint-copy and controlled-exhaustion evidence; it does not skip common gates.

## Recommended implementation and decision order

1. Freeze the smallest placement-neutral `ManagedWorkingState` contract and portable vnode artifact
   bridge. Do not extend the immutable `StateBackend` into the hot-path API.
2. Implement the already-required in-memory semantic backend as the first lifecycle consumer. Use it
   to prove atomic pre-mutation reads/batch writes, ordered timers/ranges, generation freeze, whole-
   graph acquire/revoke and the external oracle. This is implementation evidence, not production
   admission.
3. In parallel, finish the approved redb prescreen and the owner-gated RocksDB/Fjall mechanism
   decisions. Do not add `heed` unless the redb result leaves a specific unanswered B-tree question.
4. Choose whether to certify a bounded-memory product profile. If chosen, freeze its hard byte/
   cardinality/window/join/source-retention/RTO limits and fail-closed behavior before the run.
5. Select one embedded backend for the general local-spill profile only from common native-host
   evidence. Remove losing spike adapters; keep portable state format independent of the winner.
6. Run a separately chartered independent release-candidate product soak for every profile claimed
   production-ready. The memory profile must force near-cap skew, timer/join growth, overlapping
   generations, allocator fragmentation, object-store faults, repeated process loss and rebalances.
   The spill profile additionally forces cold cache, maintenance stalls, disk fill/corruption and
   local-disk loss. A backend endurance run is not this soak.

## Research hygiene

The tracked Cycle 16–19 reports remain relevant provenance: later documents supersede their next
action without replacing exact source identities, rejected mechanisms or construction evidence.
Deleting them would weaken auditability. Two ignored local February 2026 schema-research drafts were
removed because the validation report already identifies their registries, DDL and performance
claims as removed or unwired. Stale local Claude audit prompts were corrected so they discover the
active store/cache path instead of assuming redb and foyer are runtime architecture.

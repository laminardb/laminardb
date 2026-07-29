# Working-state placement and backend scope — Cycle 20

- **Date:** 2026-07-24
- **Question:** does cluster keyed state require RocksDB, Fjall, or another database?
- **Answer:** a managed mutable working-state layer is required; no named engine and no LSM is
  intrinsically required
- **Recommended broad-state profile:** one qualified worker-local embedded store plus portable
  cluster-shared checkpoints
- **Bounded-memory outcome:** reference/conformance-only under the current ADR and plan; no cluster
  product profile or production-soak matrix
- **Current worker-local result:** released `rocksdb` 0.24.0 failed the bounded adapter-entry
  cleanup/lifecycle source gate; no backend is selected or production-qualified and admission
  remains closed
- **Evidence:** code inspection and current primary-source review; no candidate or product run
- **Admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`

**2026-07-29 source-closure reconciliation:** the engine-neutral placement conclusion remains
authoritative. The [official-release selection](official-release-state-backend-selection-2026-07-29.md)
carried RocksDB into one bounded source gate; the subsequent
[adapter-entry closure](rocksdb-0.24.0-adapter-entry-source-closure-2026-07-29.md) stopped it before
dependency or adapter. Current RocksDB, TidesDB, Fjall, and redb releases are not live alternatives.
The matrix below is retained as research provenance, not a current multi-engine queue.

**2026-07-28 historical direction:** the engine-neutral placement conclusion remains authoritative.
The bounded stock-Fjall 3.1.8
[source closure](fjall-3.1.8-adapter-entry-source-closure-2026-07-28.md) stops before dependency,
adapter, or execution. Fatal worker exit can leave database destruction waiting indefinitely on
private worker accounting; the frozen v2 background-failure mapping also cannot be completed from
stock public facts. The Cycle 40/41 TidesDB design and T0 stop remain historical evidence, not a
current package selection. The owner's stated next direction is a separately bounded current
official Rust/native TidesDB source re-entry. This is not qualification, runtime admission, or a production
claim. Every later `current`, `continue`, `keep`, or `should` imperative in the dated Cycle 20 engine
screen is historical and superseded by the current implementation order below.

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

1. **Bounded-memory:** viable in principle only when admission and runtime reservations hard-bound live state,
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

Engine selection is not an architectural prerequisite for proving the common service with its
in-memory conformance implementation. It remains a governance prerequisite under the current plan:
ADR-008 requires the Phase 0 review gate, and Cycle 20 neither completes nor splits that gate. No
runtime Phase 1 work is authorized by this report. A later accepted ADR/plan amendment may define a
smaller owner-approved lifecycle-entry gate, but until then Phase 1 stays blocked. Cluster
production admission still requires the currently intended qualified local-spill backend. Cycle 21
keeps bounded memory reference/conformance-only. It receives no implementation or admission
schedule unless a future ADR amendment explicitly reopens its separate support and certification
cost.

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
| Hard-bounded in-memory + shared checkpoints | Lowest local access overhead, but all charged live/frozen/scratch state and worst-case skew must fit RAM; allocator retention and snapshot-copy tails still count | Cold restore and source replay after every process loss; full/delta chains and source retention must meet RTO | **Reference/conformance-only:** no current product profile or admission evidence. The current flat operator maps are not this service. |
| Embedded local store + shared checkpoints | Local cache/memtable for hot state and disk capacity for cold state; compaction, page faults, writer contention, sync and disk pressure enter p99.9 | Local files are disposable; any later engine profile must use portable vnode restore into a fresh generation/root | **Recommended general profile**, after integration and absolute qualification pass; no engine is currently selected |
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

## Historical Cycle 20 embedded-engine screen

At Cycle 20, active work was not to expand into a many-engine benchmark. Candidate count is not risk
coverage: every engine still has to close the same atomicity, hot-writer/victim tail, byte/resource,
persistence, export/restore, fault and endurance obligations. The dispositions below are dated
evidence and are not the Cycle 38 work order.

| Engine or family | Useful fit | Blocking concern | Cycle 20 disposition |
|---|---|---|---|
| RocksDB 10.4.2 via `rocksdb` 0.24.0 | Mature ordered LSM, atomic batches, snapshots, range operations, checkpoints and broad controls | Native/C++ build and memory accounting; the pinned binding still lacks the complete reviewed maintenance/error/stall observation | **Continue only the already-scoped source-closure track; not selected** |
| Fjall 3.1.8 | Rust-native LSM with atomic cross-keyspace batches, snapshots and ordered ranges | Stock scheduler/lifecycle, global pressure control and stable maintenance/error/stall surfaces do not close DKS-Q2-006 | **Re-enter only with explicit fork/upstream ownership; not selected** |
| redb 4.1.0 | Rust-native transactional B-tree, ordered ranges, MVCC snapshots and no LSM workers | Exactly one blocking database writer; commit/reclamation/resize/close and recovery tails need the already-designed native prescreen | **Keep the validation-only prescreen protocol track; execution and candidate admission remain unapproved** |
| `heed` 0.22.1 / LMDB | Mature ACID mmap B-tree, named databases, ordered cursors, consistent reads and no maintenance workers | One writer, fixed-map/resize coordination, reader-pinned pages, mmap cold faults, C/unsafe-open boundary and vnode export adapter | **Conditional only after a redb-specific failure**; do not repeat a failed single-writer architecture test |
| SQLite WAL / `rusqlite` | Very mature transactions, B-tree ranges, recovery and checkpoint controls | One writer; SQL/VDBE/row overhead, `SQLITE_BUSY` modes and reader-starved WAL checkpoint tails duplicate rather than remove the B-tree risk | **Paper fallback**, not a current candidate |
| libMDBX | Strong ordered/transaction/snapshot/backup surface and better geometry controls than LMDB | One writer, long-reader retention and mmap/sync tails; the C engine and separate Rust binding pins, safe surface and observability have not been reviewed | **Hold**, no prescreen now |
| [SlateDB 0.14.1](https://github.com/slatedb/slatedb/releases/tag/v0.14.1) | Active Rust object-store-native LSM with ranges, transactions, caching and asynchronous durability | Remote state changes the execution and checkpoint model; API compatibility is not yet guaranteed | **Future remote-primary research**, not local-backend bake-off |
| sled | Rust-native ordered tree and atomic operations | Upstream calls it beta/unstable, recommends SQLite when reliability dominates, warns of format migration and describes a storage rewrite | **Reject for production qualification** |
| ParityDB | Rust-native atomic columns and serialized batch writes | Upstream calls it experimental and blockchain/read-heavy; B-tree support is TODO and a returned commit may still be lost before background persistence | **Reject for this workload** |
| Tonbo 0.4.0-a1 | Rust/Arrow/Parquet, MVCC and async object-store design | Upstream calls it alpha, recommends non-critical use and still lists remote compaction in progress | **Research reference only** |
| SurrealKV 0.21.2 | Rust-native transactional KV surface | The pinned-source audit proves snapshot-registration bookkeeping and lifecycle defects | **Rejected unmodified**; reconsider only after a correctness fix and new source audit |

Primary evidence for the new alternatives is the [LMDB API](https://www.lmdb.tech/doc/group__mdb.html),
[`heed` API and environment constraints](https://docs.rs/heed/latest/heed/struct.EnvOpenOptions.html),
[SQLite WAL concurrency/checkpoint rules](https://www.sqlite.org/wal.html),
[libMDBX limitations](https://github.com/Mithril-mine/libmdbx),
[sled's own production warnings](https://github.com/spacejam/sled),
[ParityDB's workload and durability description](https://github.com/paritytech/parity-db), and
[Tonbo's alpha status](https://github.com/tonbo-io/tonbo). The existing
[pinned-source audit](state-backend-static-audit-2026-07-23.md) supplies the SurrealKV finding. None
supplies Laminar's vnode checkpoint, ownership, delivery or independent-soak lifecycle.

DKS-Q2-006 is therefore not a demand for RocksDB metrics. For an LSM it requires truthful bounded
pressure, maintenance progress/failure and exact foreground-stall evidence. An engine with no
background worker may prove that maintenance arm not applicable, but sole-writer wait, commit/sync,
page/file growth, snapshot retention, terminal errors and recovery remain common vetoes. A memory
profile would need a separately versioned and owner-approved applicability contract for hard
reservation, allocator/RSS, frozen-generation, checkpoint-copy and controlled-exhaustion evidence.
It must not reuse or reinterpret the current disk-oriented v1 or the v2 contract accepted for
validation-only implementation.

## Current implementation and qualification order

1. Retain the Cycle 40 TidesDB design and Cycle 41 `STOP_WAIT_FOR_UPSTREAM` only as historical
   evidence; the stale subject grants no dependency, adapter, or qualification authority.
2. Record stock official Fjall 3.1.8 as `OBSERVED_DESIGN_UNSUPPORTED_IN_STOCK_SOURCE`. Its bounded
   one-engineer-day, zero-candidate-machine-hour closure stopped on worker-exit/drop lifecycle and
   maintenance-health contract defects; do not fork, add, adapt, or execute it.
3. Continue the backend-neutral complete/failed-before-apply/unknown-poison publication,
   fresh-root/fencing, resource-admission, and health-capability contracts plus fake-backend fault
   tests. Cycle 42 completes the current aggregate classification/publication slice: an error after
   aggregate state application may begin requires coordinated recovery, and the recovery path
   excludes output and a due checkpoint. The backend-owned sticky native-root poison and remaining
   admission contracts stay open. Do not add Fjall in those core slices.
4. Under the owner-stated pivot, separately scope a short, source-first re-entry for the current
   official TidesDB Rust/native package. Verify exact identity and stop on the first atomicity,
   failure-lifecycle, resource, or public-health veto; do not couple local state to its S3 path or add
   an adapter in the source slice.
5. Only after a source pass, seek separate implementation authority for the smallest all-mode adapter/
   conformance vertical. Remove it on a correctness or resource-containment failure; do not add
   alternative adapters or a generic runtime backend framework.
6. Qualify the exact admitted subject using the actual uniform/Zipf aggregate, window/timer and join
   operations plus checkpoint, cleanup, rebalance, crash, corruption, disk-pressure, N/N-1 and
   p99/p99.9/max gates. A hard failure or required fork disqualifies it and returns alternatives to
   an explicit owner decision.
7. Use the already-required in-memory semantic backend as the first lifecycle consumer and the
   qualified local-spill backend behind the same contract. Use the memory
   implementation to prove atomic pre-mutation reads/batch writes, ordered timers/ranges, generation
   freeze, whole-graph acquire/revoke and the external oracle. This is implementation evidence, not
   production admission.
8. Keep bounded memory as the semantic reference only. Any later product proposal must amend the ADR
   and restart applicability, hard-limit, recovery/RTO, and independent-soak approval before a run.
9. After existing failover, ALO, and EO-eligible regressions and final cleanup pass, run a separately chartered
   independent release-candidate product soak for the intended local-spill production profile once
   a backend is selected and qualified. It forces cold cache, maintenance stalls, disk fill/corruption,
   local-disk loss, object-store faults, repeated process loss and rebalances. A backend endurance
   run is not this soak. A future memory product proposal must first amend the ADR and charter with
   its own approved matrix; this reference-only decision defines no memory soak obligation.

## Research hygiene

The tracked Cycle 16–19 reports remain relevant provenance: later documents supersede their next
action without replacing exact source identities, rejected mechanisms or construction evidence.
Deleting them would weaken auditability. The ignored, untracked local drafts
`docs/research/extensible-schema-traits.md` and `docs/research/schema-inference-design.md` were
removed from this workspace because the validation report already identifies their registry, DDL
and performance claims as removed or unwired. The ignored local prompts
`.claude/agents/lookup-table-validator.md` and `.claude/agents/state-auditor.md` were corrected to
discover the active store/cache rather than assume redb and foyer. Because `.gitignore` excludes
those private paths, this paragraph records workspace hygiene; commit history cannot carry their
content change.

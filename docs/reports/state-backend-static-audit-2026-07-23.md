# Fjall 3.1.8, RocksDB 10.4.2, redb 4.1.0, and SurrealKV 0.21.2 static backend audit

- **Date:** 2026-07-23
- **Scope:** exact-source API, durability, pressure, restore, and hot-path review
- **Evidence class:** static design evidence only; no candidate was built or run
- **Selection verdict:** **BLOCK**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` are unchanged

## Result

Fjall and RocksDB expose the primitives from which a backend-neutral semantic adapter can be
tested, but neither retained candidate is ready to enter a gate-bearing campaign unchanged. redb
4.1.0 can implement the bounded C1 semantics. Its database-wide blocking writer and not-yet-approved
durability, cache, and telemetry mappings need a preliminary writer/commit/recovery microprobe and
a reviewed contract mapping before deciding whether to expand the bake-off. It is deferred, not
failed or selected against by unmeasured tail latency.

SurrealKV 0.21.2 is rejected unmodified. Its exact source has a snapshot-registration invariant
violation on a compaction-critical path, plus unresolved background-wakeup liveness, durability,
and observability gaps. It is Apache-2.0 and patchable, but the required work is a correctness fork
before a telemetry patch, not a low-cost third candidate.

| Candidate | Required primitives | Governance/telemetry | Static disposition |
|---|---|---|---|
| Fjall 3.1.8 | Cross-keyspace atomic batch, consistent snapshot, ordered range/prefix iteration, and explicit journal persistence are present. | No stable compaction-debt counter, complete write-stall duration/counter, enforceable global write-buffer cap, or complete cache/pinned-memory accounting. | **FAIL DKS-Q2-006 as published.** Patch/upstream the missing stable signals and controls or remove Fjall from the campaign. |
| `rocksdb` 0.24.0 / RocksDB 10.4.2 | Cross-column-family `WriteBatch`, snapshots, bounded iterators, multi-get, WAL flush, checkpoint, and SST ingest are present. | Pending-compaction and pressure properties exist, but the exposed stall ticker omits verified write-buffer-manager/database-scope paths. | **BLOCK DKS-Q2-006.** Supply a proven complete signal or narrow it only with pre-approved evidence that no uncovered stall class is possible. |
| redb 4.1.0 | Atomic cross-table write transaction, snapshot reads, ordered ranges, an open-time cache budget, and immediate-durability commit are present. | A single database-wide writer blocks without a timeout/cancel API; storage statistics traverse the trees while holding that writer; the current contract has no approved non-LSM debt/stall mapping. | **DEFER.** Freeze that mapping and run a non-gating writer/commit/recovery microprobe before deciding whether to add a third C2/C3 candidate. |
| SurrealKV 0.21.2 | One-tree atomic transactions, snapshot-filtered point/range reads, WAL sync, and internal LSM pressure machinery are present. | Snapshot registrations are not reference-counted and a temporary range snapshot unregisters a real sequence; public debt/stall telemetry is absent and background notification liveness is unproved. | **REJECT unmodified.** A pinned fork/upstream must fix and prescreen correctness/liveness before telemetry work or candidate admission. |

This does not select RocksDB by elimination. Fjall remains smaller and safe-Rust, while RocksDB has
broader operational controls at the cost of synchronous FFI, native-memory/build provenance, and
shared database write-control risk. redb's Rust-native, no-C++-engine implementation and
copy-on-write B-trees are attractive, but adding a third adapter before its cheaper risk screen
passes would expand scope without selection evidence. Only equivalent C1/C2, C3, fault, endurance,
and restore evidence may select between candidates admitted to the bake-off. The losing adapter is
then removed. SurrealKV does not enter that comparison merely because it is Rust-native.

## Provenance and current-tree correction

The candidate profile declares Fjall `=3.1.8`, `rocksdb =0.24.0`, and bundled RocksDB `10.4.2`.
Those strings are comparison inputs, not project pins. `cargo metadata`, the root lockfile, and the
qualification-tool lockfile contain no Fjall, RocksDB, `librocksdb-sys`, redb, or SurrealKV package,
and no backend adapter exists. The current tree therefore does not “use Fjall”; the former cold tier
remains removed. redb and SurrealKV are not in the profile, dependency graph, or lockfiles; their
audits are requested static alternative screens, not silently added candidates.

The audit read cached crate archives and the adjacent expanded registry sources. The hashes below
identify the archives; they do not by themselves authenticate a subsequently mutable extraction:

| Package | SHA-256 of exact `.crate` bytes |
|---|---|
| `fjall-3.1.8` | `420a84699b8ccbb1ed573e38e88f4f23637b45beab6432066452f834be469c57` |
| `lsm-tree-3.1.8` | `055a908d502129cf63bedae52f2db222e4436d2da32a69df9b84ac9fb9147761` |
| `rocksdb-0.24.0` | `ddb7af00d2b17dbd07d82c0063e25411959748ff03e8d4f96134c2ff41fce34f` |
| `librocksdb-sys-0.17.3+10.4.2` | `cef2a00ee60fe526157c9023edab23943fae1ce2ab6f4abb2a807c1746835de9` |
| `redb-4.1.0` | `8e925444704b5f17d32bf42f5b6e2df050bceebc3dcd6e71cc73dafe8092e839` |
| `surrealkv-0.21.2` | `b0672cbbe282723a62ccee14b95232ca0b4c9bf44ea22fb520c43e7c517fb8ec` |

The RocksDB wrapper and sys crate record VCS revision
`bb7d2168eab1bc7849f23adbcb825e3aba1bd2f4`; the bundled
`include/rocksdb/version.h` identifies 10.4.2. Fjall's manifest accepts `lsm-tree ~3.1.8`, so even
an exact top-level Fjall constraint is not a complete build identity. redb's packaged VCS record
and upstream `v4.1.0` tag identify `6ed1f981ba4deab0b2adbdd7bccb46ec409b2191`; its declared MSRV is
1.89. A future candidate needs an isolated exact lockfile, package archive, SBOM, feature set,
target, toolchain, and build flags. SurrealKV's annotated `v0.21.2` tag peels to
`d7e85669f59493c9501adcf0289e497ee206ffae`; its manifest declares Apache-2.0 and Rust 1.86. This
Windows source audit did not exercise the target Linux/XFS/NVMe mappings.

Primary redb provenance is the [crates.io 4.1.0 release](https://crates.io/crates/redb/4.1.0),
[versioned API documentation](https://docs.rs/redb/4.1.0/redb/), and the
[matching upstream revision](https://github.com/cberner/redb/tree/6ed1f981ba4deab0b2adbdd7bccb46ec409b2191).
Primary SurrealKV provenance is the [crates.io 0.21.2 release](https://crates.io/crates/surrealkv/0.21.2)
and its [pinned upstream revision](https://github.com/surrealdb/surrealkv/tree/d7e85669f59493c9501adcf0289e497ee206ffae).

## Common semantic boundary

Plain atomic batches are the qualification baseline. C2 requires one serialized foreground state
worker, and the production adapter must add the per-vnode lifecycle guard; together those contracts
supply an unambiguous order. Fjall transactions add a global writer lock or conflict bookkeeping,
and RocksDB transactions add locking, retry, and expiry behavior without fixing distributed
ownership. C3 must prove shared-database concurrency before a backend can be selected.

Both adapters must:

- use one database and the fixed four-keyspace/column-family layout, never one tree per vnode;
- copy/detach request-local keys and values into the bounded result slot, then promptly drop pinned
  slices and request iterators before the service interval ends; export snapshots instead have an
  explicit bounded lifetime and remain charged until export completion;
- perform bounded half-open scans with independent row and byte limits plus one lookahead, returning
  canonical bytewise order;
- issue one atomic cross-keyspace/column-family mutation after all serialized reads and validation;
- fatal-latch any write, persistence, timeout, or unknown-commit-point error and recover from a
  committed cut rather than retrying blindly; and
- keep portable logical export/restore authoritative. Native directory checkpoints, backups, or
  SST ingestion are optimizations and diagnostics, not vnode artifacts.

### Fjall semantics and risks

`fjall/src/batch/mod.rs` writes one checksummed journal batch, installs all items at one sequence,
then publishes visibility. `Database::snapshot`, `Keyspace::range`, and `Keyspace::prefix` provide a
cross-keyspace sequence cut and ordered iteration. Returned disk slices may retain a complete cache
block, so borrowing them across the service boundary violates the memory model.

Fjall has no native multi-get, range delete, physical checkpoint, or backup API. The candidate
adapter must therefore use bounded repeated gets for aggregate reads and resumable scans plus point
tombstones for vnode/generation cleanup. Long snapshots retain MVCC versions. Sorted ingestion
accepts only strictly increasing, duplicate-free keys. More importantly, `Ingestion::finish` holds
Fjall's global journal mutex while the underlying engine flushes the active memtable, finishes and
syncs SSTs, registers tables, and persists a version. It is not an assumed online-rebalance hot
path; use it only for offline setup or restore until C3 proves otherwise. The production adapter
must perform online acquisition through rate-limited generation-qualified batches followed by
lifecycle-pointer publication.

`WriteBatch::commit` does not poison the database when the journal write itself returns an error.
The adapter must therefore latch every mutation/fence error as fatal even if Fjall remains callable;
continuing could place a later acknowledgement behind an incomplete record that recovery truncates.

### RocksDB semantics and risks

`WriteBatch` covers puts and deletes across column families atomically. DB-wide snapshots, bounded
`ReadOptions`, same-CF batched multi-get, raw iterators, external SST writing/ingest, and physical
checkpoints exist. The ordinary iterator and ordinary multi-get paths allocate/copy; the candidate
hot path should qualify raw bounded iterators and same-CF batched multi-get while releasing pinned
values promptly.

All calls are synchronous FFI and foreground calls are not cancellable. `PerfContext`, detailed
statistics, and broad ticker collection can alter the workload; gate-bearing settings require
paired observer-control trials and no measured overhead subtraction. RocksDB's rate limiter covers
background work, not WAL durability or vnode QoS. Checkpoint may flush all column families, and SST
ingest may require a blocking flush. Both need foreground-tail evidence. Database-level write
queues/controllers mean column-family isolation cannot be assumed.

### redb semantics and static deferral

redb stores multiple named tables in one database file using copy-on-write B-trees. One write
transaction can atomically mutate all four logical tables, while MVCC read transactions and ordered
ranges supply the basic C1 adapter semantics. Guard-backed keys and values still have to be copied
into bounded result slots. There is no native multi-get or range-tombstone primitive; ranged
removal scans entries through `extract_from_if`/`retain_in`. A read transaction can drive portable
export. There is no native physical checkpoint, online backup, or bulk/sorted-ingest primitive to
promote into the vnode format.

The primary risk is the writer contract. `Database::begin_write` permits one database-wide
writer and blocks until the existing writer completes. The API exposes no try, timeout, or
cancellation form. That is workable for C2's single serialized foreground worker. In C3,
independent disjoint-vnode lanes would share this acquisition point, but the frozen contract
requires measured victim/hot-writer tails rather than assuming that serialization fails them. An
adapter-owned queue can measure/reject before `begin_write`; it cannot create write parallelism or
bound engine work after acquisition.

`WriteTransaction::stats` traverses the data and system B-trees and counts allocated/free pages;
using it for periodic 96-GiB resource sampling would occupy the sole writer and contaminate the
latency population, so a redb mapping must prohibit it during measured trials. The returned
allocated, leaf, branch, stored, metadata, and fragmented-byte figures remain useful offline.
External cgroup/device/quota counters, adapter queue/service timing, and optional cache hit/miss/
eviction/used-byte counters may form a mapping, but that mapping has not been frozen or reviewed.

`Database::compact` requires exclusive mutable database access, rejects live readers/savepoints,
and performs foreground relocation through repeated write transactions. It is an offline
maintenance path, not background debt management or an online vnode-rebalance primitive.
Savepoints retain otherwise reclaimable pages and are not portable vnode artifacts. These
properties make global-writer, durable-commit, crash-open, and compaction behavior the correct
cheap microprobe targets before implementing a full third adapter.

### SurrealKV correctness screen

SurrealKV has one byte-key `Tree`; Laminar's four logical keyspaces would need disjoint prefixes in
that one tree because separate trees do not provide the required cross-keyspace atomic batch. Its
transactions provide snapshot isolation with write-write conflict detection, not general
serializability, so the C2 adapter would retain one serialized foreground writer. WAL append and
sequence publication are serialized, while up to seven commit bodies can be in flight and memtable
application occurs outside the write mutex. Synchronous filesystem work runs directly in async task
paths, so Tokio scheduling and blocking tails would require explicit qualification.

The exact v0.21.2 source violates the snapshot-retention invariant before any performance question:

- [`SnapshotTracker`](https://github.com/surrealdb/surrealkv/blob/d7e85669f59493c9501adcf0289e497ee206ffae/src/snapshot.rs#L42-L103)
  stores sequence numbers in a `SkipSet<u64>`. Two live snapshots at the same visible sequence
  collapse to one element; dropping either removes that sequence while the other is still live.
- [`SnapshotIterator::new_from`](https://github.com/surrealdb/surrealkv/blob/d7e85669f59493c9501adcf0289e497ee206ffae/src/snapshot.rs#L918-L928)
  constructs a temporary `Snapshot` without registering it. Its ordinary `Drop` unregisters the
  enclosing transaction's real sequence, so merely creating a range iterator can remove retention
  protection.
- The [compactor consumes this tracker](https://github.com/surrealdb/surrealkv/blob/d7e85669f59493c9501adcf0289e497ee206ffae/src/compaction/compactor.rs#L167-L194)
  when deciding which versions to preserve. This is a source-proved bookkeeping invariant failure
  and a credible stale-snapshot/data-visibility risk; it must be fixed and exercised under forced
  compaction before C1 semantic conformance.

The background scheduler also needs a deterministic liveness test. Notifications are suppressed
while a task is marked running, and the level worker performs one compaction per notification. The
memtable worker drains what it sees, but concurrent rotations can race with its final empty check
and flag clear. That is a plausible lost-wakeup/residual-debt path, not yet an empirical failure;
the safe fix must clear-and-recheck or use non-lossy notification and prove drain-to-stability
([task source](https://github.com/surrealdb/surrealkv/blob/d7e85669f59493c9501adcf0289e497ee206ffae/src/task.rs#L45-L176)).

Other prescreen blockers are concrete:

- an oversized batch is appended to WAL before the memtable rejects that it cannot fit a fresh
  arena, while recovery can then fail on the retained record; admission must reject it before WAL;
- `Tree` clones can each spawn close work from `Drop`; an adapter could avoid cloning the tree, but
  safe last-owner shutdown should be fixed upstream;
- default tolerant WAL repair is not an authoritative corruption policy; an incomplete final crash
  tail must be distinguished from checksum corruption in the middle; and
- exact level/run debt, immutable bytes, task state, stall episodes, cache/pinned memory, and applied
  options are private or absent from the public API.

The minimum acceptable fork first adds reference-counted/uniquely owned snapshot guards and forced-
compaction regressions, fixes wakeup/drain and oversized-batch ordering, and tightens close/recovery.
Only then should it expose a stable cheap stats snapshot and complete normalized stall episodes.
Until that bounded correctness prescreen passes, SurrealKV is rejected unmodified rather than added
to the comparison profile.

## Persistence mapping: static proposal, not durability evidence

| Contract boundary | Fjall 3.1.8 | RocksDB wrapper 0.24.0 | Status |
|---|---|---|---|
| `buffered_batch` | `batch.durability(Some(PersistMode::Buffer)).commit()` | WAL enabled, `manual_wal_flush=false`, `WriteOptions.sync=false`, then `write_opt` | API mapping only; process-death test required. |
| `persist_data` | `Database::persist(PersistMode::SyncData)` | `DB::flush_wal(true)` | Statically credible; physical cache-loss test required. |
| `persist_all` | `Database::persist(PersistMode::SyncAll)` is a candidate mapping. | No audited single safe-wrapper primitive. | Unsupported until an engine/file/directory sequence passes the physical truth table. |

The RocksDB wrapper has no separate `sync_wal` method: `flush_wal(true)` reaches engine
`FlushWAL(true)`/`SyncWAL`. `use_fsync` is an open-time configuration, so `fdatasync` and `fsync`
variants have distinct candidate identities. Recovery mode is also explicit; `SkipAnyCorruptedRecord`
is ineligible for authoritative state.

Fjall `Buffer` flushes its userspace `BufWriter`; `SyncData` and `SyncAll` call the corresponding
file methods on the active journal. Rotation and table/version paths include file and directory
sync operations, but static ordering cannot prove the target filesystem/device cache-loss
contract. Neither retained candidate's fence publishes a Laminar checkpoint, seals a source-offset
cut, or commits a sink transaction.

redb is deferred before persistence qualification. `Durability::None` is documented to remain
unpersisted until a later `Immediate` commit, so it cannot satisfy this contract's process-death
guarantee for `buffered_batch`. The default `Durability::Immediate` one-phase commit flushes the
userspace write cache and calls `sync_data`; optional two-phase commit performs two syncs. Using
`Immediate` for every batch may satisfy a stronger durability boundary, but collapses
`buffered_batch` and `persist_data` onto the same durability operation, eliminating the intended
latency distinction. Quick repair persists allocator state
and forces two-phase commit, trading slower commits for fast crash reopen. Without usable allocator
state, an unclean open can perform extensive tree/allocator repair. There is no distinct standard
`persist_all`/directory-sync primitive. None of these source-level mappings satisfies Laminar's
cache-loss or failover gates without the physical truth-table testing required of retained
candidates.

SurrealKV's default/Eventual commit only appends to a 32-KiB userspace `BufWriter`; it is not the
contract's process-death-safe `buffered_batch`. A forked adapter would need Eventual commit followed
by public `flush_wal(false)` inside the acknowledged service interval, or a dedicated upstream
flush-without-sync durability mode. `flush_wal(true)` is the provisional `persist_data` mapping and
reaches WAL `sync_all`; no public online `persist_all`/flush-all primitive exists. Immediate commit
also syncs the WAL but collapses the buffered/persist-data latency distinction. All mappings remain
unapproved until snapshot correctness, strict recovery, and the physical truth table pass
([transaction durability](https://github.com/surrealdb/surrealkv/blob/d7e85669f59493c9501adcf0289e497ee206ffae/src/transaction.rs#L47-L90),
[WAL flush API](https://github.com/surrealdb/surrealkv/blob/d7e85669f59493c9501adcf0289e497ee206ffae/src/lsm.rs#L1682-L1695)).

## Pressure and resource audit

Common authoritative observations remain external: a fresh candidate-leaf cgroup for CPU and
memory, cgroup/device I/O, XFS project quota for every candidate path, `/proc` PSS/RSS/FDs, and
adapter-owned exact snapshot/iterator lifetimes. Approximate engine memory or filesystem logical
sizes are diagnostics.

### Fjall static failure

Fjall's deprecated `max_write_buffer_size` stores a configuration field that no enforcement path
reads. Per-keyspace pressure is post-commit and private: L0 levels trigger a busy loop or fixed
sleeps, and sealed memtables trigger fixed sleeps. There is no stable public compaction-debt byte
counter or stall interval/time counter. The optional, explicitly experimental metrics cover
read/cache/filter observations rather than debt, flush/compaction writes, or stalls. Cache capacity
is approximate and excludes allocation overhead and pinned index/filter memory. Background queues
have fixed bounds and lossy notification paths, with no compaction I/O rate limiter.

Unmodified Fjall 3.1.8 therefore cannot populate the v2 mapping's applicable
`background_maintenance_debt` byte observations and `engine_pressure_stalls` interval observations
honestly. A qualifying patch/upstream release needs stable applied-option,
level/run bytes, active/sealed/queued memtable bytes, pending compaction input/output/debt, active
and cumulative stalls by reason, compaction I/O, cache/pinned index/filter, journal, and retained
version/snapshot observations. The production adapter must still supply its own pre-write
reservation, disk/memory governor, and bounded in-flight limit.

### RocksDB binding gaps

With level compaction frozen, `estimate-pending-compaction-bytes` can supply debt per column family.
Current delayed rate, stopped state, pending flush/compaction, cache usage/pinned usage,
write-buffer-manager usage, snapshot counts, and approximate memory builders are available.

`Ticker::StallMicros` is not a complete stall total. The 10.4.2 write-controller path records its
normal delay and stopped interval, but the separate write-buffer-manager block path has no matching
ticker update. Structured stall maps exist inside the engine, while the safe wrapper exposes no
stable structured source that proves complete coverage; parsing formatted strings is not gate
evidence. A listener alone is not assumed to cover the write-buffer-manager path. Adapter counters
must track live iterators, while cgroup/PSS remains authoritative for native memory.

The safe wrapper also omits optional engine controls such as the C API's strict-capacity LRU
constructor and hard-space manager. Those are useful defense in depth, not required substitutes for
the authoritative adapter reservation, cgroup memory limit, and XFS project quota. Their absence is
not the stall-accounting blocker.

DKS-Q2-006 therefore stays blocked for RocksDB until a reviewed source exposes complete stall
events/time, or the approved configuration and fault evidence prove that every uncovered path is
impossible. One-second polling cannot prove absence of short stalls.

### SurrealKV static failure

SurrealKV's leveled scheduler privately computes file-count/byte scores from exact table sizes, so a
fork could define background-maintenance debt. Its private stall controller waits on immutable-
memtable and L0-file thresholds and returns per-wait information, but the commit caller discards the
result; there is no public cumulative union duration or episode stream. Cache counters are test-only,
and configured cache/memtable capacities omit allocator, key, ownership, and pinned-block overhead.
Memtables eagerly allocate their configured arenas and the count-based stall threshold is not a
global byte reservation.

Unmodified 0.21.2 therefore fails candidate-specific DKS-Q2-006 in addition to its correctness
rejection. A qualifying fork needs a versioned cheap observation snapshot, complete debt formula,
active/cumulative stall episodes by reason, background errors/jobs and I/O, cache/pinned accounting,
and an applied configuration record. External cgroup/XFS/device/process metrics and adapter-owned
snapshot/iterator counters remain authoritative.

### redb mapping gap

redb has no background LSM compaction, so LSM debt cannot simply be demanded from it or silently
encoded as an unsupported zero. The amended runner contract now separates mandatory common
resource-v2 observations from candidate-specific `observed | not_applicable` background-maintenance
and engine-pressure arms. That corrects the former LSM-shaped contract; it does not approve redb's
mapping. redb still needs exact source/configuration proof plus a bounded probe for both proposed N/A
arms, authoritative external cgroup/device/quota observations, adapter-owned queue and service
timing, and a prohibition on full-tree statistics during measurement.

Version 4.1.0 does not expose internal read/write bytes or pinned-snapshot bytes, but the contract
does not make candidate-internal versions of those observations common mandatory fields. The
candidate remains **DEFER** pending the mechanism-map schema, reviewed redb mapping, and
writer/commit/recovery microprobe; no telemetry or latency failure has been measured.

The proposed [bounded redb 4.1.0 prescreen](../testing/state-backend-redb-prescreen-v1.md) specifies
that microprobe's non-gating decision boundary, exact pin, single-writer matrix, atomicity/recovery
split, hard caps, and Docker/WSL smoke-only subset. It still requires strict schemas/harness, a
detached pre-run approval by named owners, and review; it does not add redb to any manifest,
lockfile, profile, adapter, or qualification population.

## Configuration, restore, and decision gates

Neither retained library exposes a complete applied-options dump. Each adapter must emit one closed
canonical configuration record and archive engine files needed to cross-check it. For RocksDB this
includes the OPTIONS files plus cache/WBM, thread pools, rate limiter, paths, recovery mode,
compression/table
format, statistics level, allocator, native features, and compiler flags. For Fjall it includes
database/keyspace options, feature flags, and non-configurable pressure constants.

Fjall sorted ingest and RocksDB SST ingest/checkpoint are not assumed safe during live traffic.
The production design requires portable export through a bounded cross-keyspace snapshot; restore
must build an unservable reserved generation, verify it, then atomically publish the lifecycle
pointer. Snapshot retention, ingest/cleanup latency, write amplification, disk/RSS growth, and
hot-writer/victim tails remain C2 and C3 gates.

The static disposition is:

- DKS-Q2-006: **FAIL for unmodified Fjall; BLOCK for the current RocksDB binding**;
- DKS-Q2-007: **BLOCK for both** until exact candidate locks/builds/options, approval/completion
  records, cache-loss truth-table evidence, and N/N-1 recovery exist;
- redb 4.1.0: **DEFER before candidate-specific DKS-Q2-006/007 implementation**; first approve its
  typed mechanism, cache, and persistence mapping and a bounded non-gating writer/commit/recovery
  microprobe. Do not add it to the profile or dependency graph yet;
- SurrealKV 0.21.2: **REJECT unmodified before C1/C2**; a pinned fork/upstream must first repair and
  prescreen snapshot retention, background drain, oversized-batch ordering, close/recovery, and then
  DKS-Q2-006 observability. Do not add it to the profile or dependency graph;
- candidate execution: still blocked on the complete workload/runner approval set;
- backend selection: still additionally blocked on C3 shared-database concurrency; and
- production: still blocked on the vnode lifecycle, checkpoint/source/sink delivery protocol,
  fault/endurance work, and independently operated release soak.

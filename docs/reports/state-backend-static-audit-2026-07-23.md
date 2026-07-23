# Fjall 3.1.8 and RocksDB 10.4.2 static backend audit

- **Date:** 2026-07-23
- **Scope:** exact-source API, durability, pressure, restore, and hot-path review
- **Evidence class:** static design evidence only; no candidate was built or run
- **Selection verdict:** **BLOCK**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` are unchanged

## Result

Both candidates expose the primitives from which a backend-neutral semantic adapter can be tested,
but neither audited source is ready to enter a gate-bearing campaign unchanged.

| Candidate | Required primitives | Governance/telemetry | Static disposition |
|---|---|---|---|
| Fjall 3.1.8 | Cross-keyspace atomic batch, consistent snapshot, ordered range/prefix iteration, and explicit journal persistence are present. | No stable compaction-debt counter, complete write-stall duration/counter, enforceable global write-buffer cap, or complete cache/pinned-memory accounting. | **FAIL DKS-Q2-006 as published.** Patch/upstream the missing stable signals and controls or remove Fjall from the campaign. |
| `rocksdb` 0.24.0 / RocksDB 10.4.2 | Cross-column-family `WriteBatch`, snapshots, bounded iterators, multi-get, WAL flush, checkpoint, and SST ingest are present. | Pending-compaction and pressure properties exist, but the exposed stall ticker omits verified write-buffer-manager/database-scope paths. | **BLOCK DKS-Q2-006.** Supply a proven complete signal or narrow it only with pre-approved evidence that no uncovered stall class is possible. |

This does not select RocksDB by elimination. Fjall remains smaller and safe-Rust, while RocksDB has
broader operational controls at the cost of synchronous FFI, native-memory/build provenance, and
shared database write-control risk. Only equivalent C1/C2, C3, fault, endurance, and restore
evidence may select one. The losing adapter is then removed.

## Provenance and current-tree correction

The candidate profile declares Fjall `=3.1.8`, `rocksdb =0.24.0`, and bundled RocksDB `10.4.2`.
Those strings are comparison inputs, not project pins. `cargo metadata`, the root lockfile, and the
qualification-tool lockfile contain no Fjall, RocksDB, or `librocksdb-sys` package, and no backend
adapter exists. The current tree therefore does not “use Fjall”; the former cold tier remains
removed.

The audit read cached crate archives and the adjacent expanded registry sources. The hashes below
identify the archives; they do not by themselves authenticate a subsequently mutable extraction:

| Package | SHA-256 of exact `.crate` bytes |
|---|---|
| `fjall-3.1.8` | `420a84699b8ccbb1ed573e38e88f4f23637b45beab6432066452f834be469c57` |
| `lsm-tree-3.1.8` | `055a908d502129cf63bedae52f2db222e4436d2da32a69df9b84ac9fb9147761` |
| `rocksdb-0.24.0` | `ddb7af00d2b17dbd07d82c0063e25411959748ff03e8d4f96134c2ff41fce34f` |
| `librocksdb-sys-0.17.3+10.4.2` | `cef2a00ee60fe526157c9023edab23943fae1ce2ab6f4abb2a807c1746835de9` |

The RocksDB wrapper and sys crate record VCS revision
`bb7d2168eab1bc7849f23adbcb825e3aba1bd2f4`; the bundled
`include/rocksdb/version.h` identifies 10.4.2. Fjall's manifest accepts `lsm-tree ~3.1.8`, so even
an exact top-level Fjall constraint is not a complete build identity. A future candidate needs an
isolated exact lockfile, package archive, SBOM, feature set, target, toolchain, and build flags.
This Windows source audit did not exercise the target Linux/XFS/NVMe mappings.

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
contract. Neither candidate's fence publishes a Laminar checkpoint, seals a source-offset cut, or
commits a sink transaction.

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

Unmodified Fjall 3.1.8 therefore cannot populate the runner's required `compaction_debt_bytes` and
`write_stall_total_ns` honestly. A qualifying patch/upstream release needs stable applied-option,
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

## Configuration, restore, and decision gates

Neither library exposes a complete applied-options dump. Each adapter must emit one closed canonical
configuration record and archive engine files needed to cross-check it. For RocksDB this includes
the OPTIONS files plus cache/WBM, thread pools, rate limiter, paths, recovery mode, compression/table
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
- candidate execution: still blocked on the complete workload/runner approval set;
- backend selection: still additionally blocked on C3 shared-database concurrency; and
- production: still blocked on the vnode lifecycle, checkpoint/source/sink delivery protocol,
  fault/endurance work, and independently operated release soak.

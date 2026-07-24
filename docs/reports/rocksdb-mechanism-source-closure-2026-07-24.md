# RocksDB mechanism-source closure — Cycle 17

- **Date:** 2026-07-24
- **Decision scope:** the RocksDB-specific part of DKS-Q2-006 only
- **Exact subject:** `rocksdb =0.24.0`, `librocksdb-sys =0.17.3+10.4.2`, bundled RocksDB 10.4.2
- **Outcome under the published v1 contract:** `CLOSURE_STOPPED_AT_STAGE_0`
- **Meaning of that outcome:** the unmodified pinned source remains blocked; this is neither an
  owner rejection of RocksDB nor a backend-selection result
- **Production backend selected:** none
- **Cluster admission:** unchanged; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Decision

Do not build the proposed stall-only C++/C/Rust patch within this bounded attempt. The exact-source
audit confirmed that the stall gap is real and apparently patchable, but also found that the
current report's maintenance-debt premise is incompatible with the normative contract.
`rocksdb.estimate-pending-compaction-bytes` is explicitly an estimate of level-rewrite work. The v1
mapping requires a complete, pairwise-disjoint, direct unsigned-byte population and rejects an
estimate unless source proves it represents that population. RocksDB's projection does not.
Unmodified RocksDB therefore cannot populate either required observed arm today, and a stall-only
patch would still leave DKS-Q2-006 blocked.

Closing the debt arm requires either exact configuration proof that eliminates every uncovered
mechanism or transition-consistent bookkeeping for every enabled mechanism across flush,
compaction scheduling/execution, and purge. One likely implementation is new native bookkeeping,
but its feasibility and overhead are unproved. That is not the narrow source/binding change scoped
by Cycle 16's recommendation. The user's Cycle 17 continuation authorized this read-only
source/configuration audit, not a fork or candidate construction. The Stage-0 stop condition
therefore fired before a fork, adapter dependency, native build, forced-path test, or
qualification-shaped artifact was created.

There is also a contract-fitness question. The documented production-monitoring guidance from
Flink, RisingWave, Materialize, and Kafka Streams relies on semantically named estimates,
queue/count/time signals, common resource limits, and end-to-end latency/backpressure evidence. It
does not demonstrate that one engine-neutral exact byte population is a prerequisite for
production state. The v1 debt rule structurally favours engines with no background worker, whose
arm may be N/A. That is an AI-overengineering risk to review, not a reason to silently weaken the
rule for RocksDB.

The next owner decision is between retaining v1 and explicitly funding broader engine
instrumentation, or approving an additive DKS-Q2-006 successor. The successor proposal would keep
objective common latency, resource-tail, disk-growth, write-amplification, device-I/O, memory, and
pressure gates, while replacing the universal exact-debt scalar with reviewed candidate-specific
health signals whose units, estimation error, scope, overhead, configuration, and thresholds are
explicit. Only after that decision should an apparently bounded RocksDB stall observer be funded.
redb must not inherit a pass merely because its background-maintenance arm could be N/A; its global
writer, synchronous reclamation, and durability risks remain independently gate-bearing.

## Stage-0 stop

Cycle 16 recommended a bounded source/binding closure subject to a future owner decision and
numeric cap. The user's Cycle 17 continuation authorized the read-only source/configuration audit,
not construction, a backend fork, or a qualification run. The audit had to prove the complete
source/configuration populations before construction. It found a second unsupported v1 arm whose
closure is materially broader than listener and binding glue, so the attempt stopped. Any resumed
construction needs an approved contract choice, explicit scope and stop limits, and independent
review. It still would not be a DKS-Q2-006 pass. The common
plan/attempt validator, XFS project quota, cgroup dirty/writeback/device I/O, process-tree
PSS/RSS/FD, adapter lifecycle, pressure observations, target trace, C1/C2/C3, fault/endurance,
delivery, and independent product soak remain absent.

## Exact identity and freshness screen

| Component | Bound identity |
|---|---|
| Rust wrapper archive | `rocksdb-0.24.0.crate`, SHA-256 `ddb7af00d2b17dbd07d82c0063e25411959748ff03e8d4f96134c2ff41fce34f` |
| Native sys archive | `librocksdb-sys-0.17.3+10.4.2.crate`, SHA-256 `cef2a00ee60fe526157c9023edab23943fae1ce2ab6f4abb2a807c1746835de9` |
| Wrapper/sys VCS | [`bb7d2168eab1bc7849f23adbcb825e3aba1bd2f4`](https://github.com/rust-rocksdb/rust-rocksdb/tree/bb7d2168eab1bc7849f23adbcb825e3aba1bd2f4) |
| Bundled engine | [`410c5623195ecbe4699b9b5a5f622c7325cec6fe`](https://github.com/facebook/rocksdb/tree/410c5623195ecbe4699b9b5a5f622c7325cec6fe), tag `v10.4.2` |

Nineteen relevant extracted files were compared byte-for-byte with those official commits; no
mismatch was found. Neither archive is present in a LaminarDB root manifest or lockfile.

[RocksDB 11.1.2](https://github.com/facebook/rocksdb/releases/tag/v11.1.2) is the current upstream
release as of this audit. Its WBM still blocks through the separate
`ShouldStall`/`WriteBufferManagerStallWrites` path while `STALL_MICROS` is updated only by
`DelayWrite`; see the current
[foreground check](https://github.com/facebook/rocksdb/blob/v11.1.2/db/db_impl/db_impl_write.cc#L1558-L1573),
[controller statistic](https://github.com/facebook/rocksdb/blob/v11.1.2/db/db_impl/db_impl_write.cc#L2278-L2283),
and [WBM wait](https://github.com/facebook/rocksdb/blob/v11.1.2/db/db_impl/db_impl_write.cc#L2305-L2325).
Its public properties also still describe memtable sizes as
[approximate](https://github.com/facebook/rocksdb/blob/v11.1.2/include/rocksdb/db.h#L1191-L1197),
exclude files already scheduled for deletion from
[obsolete-SST bytes](https://github.com/facebook/rocksdb/blob/v11.1.2/include/rocksdb/db.h#L1277-L1284),
and retain the relevant pending-compaction byte property as an
[estimated level-rewrite quantity](https://github.com/facebook/rocksdb/blob/v11.1.2/include/rocksdb/db.h#L1294-L1298)
implemented by the same
[level projection](https://github.com/facebook/rocksdb/blob/v11.1.2/db/version_set.cc#L3608-L3691).
Other metadata and job observations exist, but there is no public complete, transition-consistent
union. Version 11 also adds
[optional asynchronous file opening](https://github.com/facebook/rocksdb/blob/v11.1.2/include/rocksdb/options.h#L803),
which a future upgrade must freeze off or include in its source inventory. Upgrading the engine
alone therefore closes neither source. The latest released
[`rocksdb` crate is 0.24.0](https://github.com/rust-rocksdb/rust-rocksdb/releases/tag/v0.24.0) and
exposes no event-listener surface. This freshness screen does not change the exact candidate
identity.

## Stall-path proof

RocksDB 10.4.2 has three classified column-family causes—memtable limit, L0 file-count limit, and
pending-compaction-byte limit—each of which may delay or stop the database-wide `WriteController`.
It also has a separate WBM path which may stall every database sharing that WBM. The relevant exact
sources are the [CF predicates and token creation](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/db/column_family.cc#L984-L1193),
[foreground checks](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/db/db_impl/db_impl_write.cc#L1545-L1573),
[controller waits](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/db/db_impl/db_impl_write.cc#L2203-L2284),
[WBM waits](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/db/db_impl/db_impl_write.cc#L2305-L2325),
[low-priority throttling](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/db/db_impl/db_impl_write.cc#L2328-L2359),
and the [WBM queue lifecycle](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/memtable/write_buffer_manager.cc#L118-L164).

| Enabled path | Existing evidence | Why v1 remains unsupported |
|---|---|---|
| CF controller delay/stop | `STALL_MICROS`, `WRITE_STALL`, condition/count properties | Cumulative after-the-fact scalar; no interval boundaries, boundary-open state, overlap union, or loss detection |
| WBM barrier | Stop count and WBM usage/limit | Separate blocking path has no duration or interval and no safe-wrapper state/listener |
| Low-priority throttle | No qualifying interval from this path | Rate-limiter wait also activates on compaction-pressure tokens and passes no statistics object |
| `no_slowdown` pressure rejection | Generic `Incomplete` status | No pressure-end interval or typed retry cause; wrapper error kind is derived from text |
| Administrative `LockWAL` | Controller stopped property | Not engine pressure; must be source-proved unreachable rather than counted |

The native CF listener is not a substitute: it excludes WBM, carries no source timestamp or cause,
and is queued until `JobContext::Clean`. The C API exposes opaque current/previous condition pointers
without a condition-value accessor, and wrapper 0.24.0 exposes no listener. RocksDB's default POSIX
`NowNanos` uses `CLOCK_MONOTONIC`, while the attempt contract currently requires one
`CLOCK_MONOTONIC_RAW` origin.

An apparently bounded future stall observer is coherent if owners first approve its contract and
configuration. A blocking prototype can freeze `no_slowdown=false`, `low_pri=false`, exclude
raw/native `LockWAL`, and bind the exact approved CF/WBM topology. It can add synchronous typed
begin/end callbacks around the actual slow regions in `DBImpl::DelayWrite` and
`DBImpl::WriteBufferManagerStallWrites`, plus C and safe-Rust bindings. A preallocated, nonblocking,
non-unwinding Rust collector can timestamp those synchronous callbacks from the attempt's raw
monotonic clock and invalidate on overflow, gaps, duplicates, regressions, or unmatched episodes.
Size, correctness, loss handling, and overhead remain unproved. This design observes actual
foreground admission/progress stalls, not merely time during which a pressure condition is idle.

`allow_stall=false` makes `ShouldStall()` false and disables WBM admission blocking while flush
triggering/accounting remains. Whether that is an acceptable production policy depends on the
separate memory and admission-control evidence; it cannot be chosen only to simplify
instrumentation. `no_slowdown=true` would instead require a typed rejection/retry protocol and
pressure-end source; that protocol interacts with exactly-once replay and must not be inferred from
`Incomplete`.

## Maintenance-debt proof and stop reason

The published contract defines background maintenance broadly: asynchronous flush, compaction,
garbage collection, vacuum, or any work that can remain after a request returns. Each mapped gauge
must be a direct unsigned byte quantity; populations must be pairwise disjoint; an estimate is
unsupported unless its source proves it represents that reviewed quantity. The exact RocksDB
sources do not expose such a population:

| Work class | Available source | Contract mismatch |
|---|---|---|
| Immutable memtable flush | approximate active/all-memtable memory, pending/running counts | No public exact immutable/picked/running byte gauge; separate reads can cross a flush-to-SST transition |
| Leveled compaction | `estimate-pending-compaction-bytes` | Policy projection of rewrite-to-target work, not an identifiable queued/running file set; it may include files already being compacted, so adding running-input bytes can double-count |
| Other compaction triggers | pending/running counts, file metadata | The level projection does not identify independent marked-file, bottommost, TTL/periodic, or blob-triggered selections; each must be eliminated by exact configuration proof or mapped |
| Running jobs | DB-wide counts; internal selected inputs | Selected inputs exist internally before execution, but public/event-visible exact bytes begin only after job preparation, omit CFs merely queued for picking, and are not disjoint from the level projection |
| Obsolete file cleanup | obsolete-SST byte property | Excludes files once scheduled/grabbed for deletion; purge queue transitions discard size and omit other file classes |

The estimate's implementation walks current level sizes, applies targets and fan-out projections,
and explicitly handles only level compaction; see
[`VersionStorageInfo::EstimateCompactionBytesNeeded`](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/db/version_set.cc#L3298-L3381).
The public contract describes it as an estimated rewrite quantity, not an exact queue; see the
[property documentation](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/include/rocksdb/db.h#L1331-L1335).

The source [separately builds other compaction candidates](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/db/version_set.cc#L3647-L3659),
including marked-file, bottommost, TTL/periodic, and forced-blob lists. Suppressing those mechanisms
is not automatically semantics neutral: for example, a very long
[bottommost-compaction delay](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/include/rocksdb/advanced_options.h#L1095-L1105)
changes reclamation semantics and does not prove the complete-process N/A arm. The
[obsolete-SST property excludes scheduled deletion](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/include/rocksdb/db.h#L1314-L1321),
and purge transitions
[grab metadata](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/db/version_set.cc#L7298-L7339),
[queue only file identity/type](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/db/db_impl/db_impl_compaction_flush.cc#L3068-L3076),
then [remove the queue entry before deletion completes](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/db/db_impl/db_impl.cc#L1964-L2005).
RocksDB also
[registers periodic info-log flushing](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/db/db_impl/db_impl.cc#L789-L819)
in this pinned source, illustrating why v1's literal “any work after return” definition needs an
explicit scope/configuration proof and cannot be inferred from one compaction property.

A literal v1 closure needs either exact configuration elimination or, at minimum,
transition-consistent, bracket-compatible native observations with retained sizes for unflushed
memtables, scheduled/running compactions, and scheduled/running purge. Those observations must
cover every CF once plus DB-wide cleanup once, and bridge flush→SST→compaction→obsolete→queued→delete
transitions. The safe wrapper reads properties individually with no common snapshot; querying a
DB-wide property through each CF can duplicate it. Enabled purge work also includes non-SST files
and deferred WAL-writer/SuperVersion release, so the three categories above are not a complete
implementation inventory.

Configuration elimination is equally substantive. The pinned
[`ttl` and `periodic_compaction_seconds` defaults](https://github.com/facebook/rocksdb/blob/410c5623195ecbe4699b9b5a5f622c7325cec6fe/include/rocksdb/advanced_options.h#L739-L819)
use an auto-selection sentinel rather than zero and must be explicitly set to zero. Blob/GC and
mempurge paths, temperature moves, external/custom compaction paths, manual/ingest/refit operations,
WAL retention/recycling, background iterator/file purge, delete scheduling, stats persistence, and
background-error recovery must each be disabled by exact source-bound configuration or mapped. The
observer must not change picking/purge order or hold scheduler locks materially longer. The likely
native instrumentation is engine-level work, not a wrapper binding; its feasibility, hot-path cost,
and semantic risk are unproved and outside this bounded closure.

This corrects the earlier statement in the static audit that the estimate could supply the debt arm.
That statement confused a useful operations estimate with the stricter v1 wire semantics.

## Contract-fitness finding

The source failure above is real under v1. It does not establish that v1 is the right production
gate:

- [Flink 2.3 exposes](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/config/#rocksdb-native-metrics)
  RocksDB's estimated pending-compaction bytes, approximate memtable sizes, pending booleans, job
  counts, and stopped/delayed state selectively, and warns that native metrics can affect
  performance.
- [RisingWave monitors](https://docs.risingwave.com/performance/metrics#lsm-tree-compact-pending-bytes)
  compaction pending bytes as a time-averaged operational signal and separately documents
  write-stop and barrier latency.
- [Materialize exposes](https://materialize.com/docs/manage/monitor/appendix-metrics/)
  compaction queue time, request/outcome counts, in-progress/noncompact batches, GC steps, usage
  categories, and backpressure signals with their native units.
- [Kafka Streams 4.3 exposes](https://kafka.apache.org/43/operations/monitoring/#rocksdb-metrics)
  cumulative/average write-stall duration, compaction and flush time/rates, and current RocksDB
  properties; statistics are sampled every minute and explicitly carry overhead concerns.

These documented monitoring models do not lower LaminarDB's correctness, latency, delivery, or
soak requirements. Taken together, they are evidence—not proof—that a universal exact byte scalar
is not a demonstrated prerequisite for production state.
The candidate-neutral comparison should come from common external facts; candidate-native signals
should prove observability, alertability, and pressure diagnosis under typed, source-bound semantics.

If owners choose an additive successor, it should:

1. keep common end-to-end/queue/service latency, offered-load, resource-tail, cgroup/PSS, XFS,
   disk-growth, write-amplification, device-I/O, pressure, snapshot, restore, and error gates;
2. replace the universal debt maximum with a required candidate-health inventory whose individual
   gauges may be exact, approximate, estimated, count, rate, or duration only when their source
   semantics and error/overhead bounds are explicit;
3. freeze candidate-specific thresholds before a run and forbid comparing unlike gauges as one
   ranking score;
4. retain background-error, sustained-growth, failure-to-quiesce, pressure-stall, and tail-latency
   vetoes, so weaker telemetry cannot hide a production failure; and
5. retain N/A only for a complete-process source/configuration proof, while charging synchronous
   maintenance and lock waits to the common latency/resource gates.

This successor needs its own schema/profile identity and independent review. V1 artifacts must not
be reinterpreted.

## Backend consequence

| Track | Cycle-17 evidence | Carry decision now |
|---|---|---|
| RocksDB 10.4.2 / wrapper 0.24.0 | Mature primitives and controls; an apparently bounded slow-path stall observer is possible but unproved; current v1 debt source is not closable by a narrow binding | **PAUSE before code.** Carry into the owner contract-or-fork decision; resume construction only under the approved outcome. |
| redb 4.1.0 | Rust-native construction passes; no background LSM worker; one non-cancellable database-wide writer and synchronous maintenance remain unqualified | **Continue only the separately approved native prescreen.** N/A cannot select it. |
| Fjall 3.1.8 | Rust-native and smaller; stable complete pressure/debt/control surface absent | **No adapter.** Reconsider alongside RocksDB only after the owner contract choice defines the candidate-specific telemetry obligation. |
| SurrealKV 0.21.2 | Snapshot-retention correctness defect plus observability/liveness gaps | **Reject unmodified.** Contract correction does not waive correctness. |

No weighted score or selection by elimination is valid. If owners retain v1 unchanged, RocksDB and
Fjall both require explicitly funded native telemetry forks and the next cheaper decision is redb's
prescreen. If owners approve the additive health-signal model, RocksDB remains the primary mature
LSM candidate and the slow-path observer becomes the next bounded construction task. In either case,
source/sink delivery certification, checkpoint/rebalance authority, exactly-once composition, and an
independently operated product soak remain separate release vetoes.

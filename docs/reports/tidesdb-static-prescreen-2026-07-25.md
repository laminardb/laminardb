# TidesDB static backend prescreen and Zipf claim audit

- **Date:** 2026-07-25
- **Cycle:** 28
- **Evidence class:** exact-source and published-artifact review only
- **Native subject inspected:** TidesDB `v9.3.14`, commit
  `6fe1e83104b70255a694239d360a14bae51d0c70`
- **Rust subject inspected:** `tidesdb-rs v0.11.1`, commit
  `e2febbc548e7f0158d1c09ea487aa0bb7c343616`
- **Candidate installed, built, linked, or executed:** no
- **Runtime dependency or backend added:** no
- **Prescreen disposition:** `REJECT_CURRENT_OFFICIAL_RUST_PATH_RETAIN_NATIVE_RESEARCH_ONLY`
- **Replacement decision:** do not replace RocksDB, Fjall, or redb with TidesDB
- **Production and cluster admission:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain fail-closed
- **Bounded memory:** reference/conformance-only; no current product or soak profile
- **Maintenance-health v2:** direction approved; final contract and candidate mappings remain gated

## Decision

The reported TidesDB Zipf advantages are credible enough to preserve as a hypothesis, but they do
not support a backend choice. The strongest recent comparison disables durability, measures a
MariaDB/TideSQL stack rather than LaminarDB state access, emphasizes TPS and p95 transaction latency
without a consistent tail distribution, and does not exercise checkpoint, restore, rebalance,
source/sink composition, or sustained maintenance
under a bounded process envelope. A direct engine comparison also disables sync, relies heavily on
batching, and shows materially higher TidesDB memory in important phases. An older full-sync result
shows that the ordering can reverse for Zipf write-only and batch-one writes.

More importantly, static prescreening finds blockers before performance execution is justified:

1. the current Rust release builds or links exactly native `9.3.6`, not the current `9.3.14`, and
   therefore misses later acknowledged correctness and recovery fixes;
2. its safe API does not encode the parent lifetimes of column families, transactions, or iterators,
   although native children retain and dereference those pointers, and its commit-hook API permits
   an unsynchronized callback/context replacement race;
3. the native default per-column-family layout explicitly is not crash-atomic for a transaction
   spanning column families and can expose a partial multi-column-family apply before commit returns;
   the unified-WAL alternative has fail-open recovery and ambiguous acknowledgement paths;
4. the native online checkpoint omits unflushed shared state in unified mode and otherwise is not an
   atomic cross-column-family state cut, with unresolved lifetime, concurrent-flush,
   compaction-acquisition, publication, and retry concerns;
5. the public health surface does not close exact foreground stall episodes or general local
   background-maintenance failures; and
6. the current native memory-pressure budget is not a proved hard RSS cap, is based on host physical
   memory rather than Linux cgroup limits, and raises an explicit lower value to five percent of
   host RAM.

The current official Rust integration is therefore a hard **NO-GO**. Keep the native C engine as a
research-only option that could re-enter after a lifetime-safe, exact-version wrapper and bounded
source closures exist. Do not insert TidesDB into the frozen Fjall/RocksDB runner lineage, change an
ADR backend decision, add a dependency, or authorize a candidate run from this report.

## Exact subject boundary

The product name currently refers to three non-equivalent subjects:

| Subject | Exact identity | Relevance |
|---|---|---|
| Current native engine | TidesDB `v9.3.14`, `6fe1e83104b70255a694239d360a14bae51d0c70`, C engine, MPL-2.0 plus bundled permissive components | Static native API and implementation review only |
| Current official Rust release | `tidesdb-rs v0.11.1`, `e2febbc548e7f0158d1c09ea487aa0bb7c343616`, MPL-2.0 | The available Rust integration path |
| Rust default native payload | Cargo feature `v9_3_6` and build dependency `tidesdb-src-v9-3-6 = 0.1`; `build.rs` accepts only pkg-config version `9.3.6` or builds that archive | Not the current engine and not the recent benchmark engine |
| Recent SQL Zipf benchmark | TidesDB `9.3.11` through TideSQL `4.5.9`, versus MyRocks in MariaDB `11.8.6` | Vendor SQL-stack evidence, not embedded Rust evidence |
| Recent direct engine benchmark | TidesDB `9.2.1` versus RocksDB `11.1.1` | Vendor engine evidence for an older subject |

The Rust release text says it lines up with TidesDB `9.3.8+`, but its exact manifest, build script,
and default feature select `9.3.6`. The native releases after that payload include acknowledged
concurrency crash/corruption and recovery fixes in `9.3.7`, snapshot/lost-update fixes in `9.3.9`,
further write-conflict work in `9.3.10`, iterator/visibility/flush/stats fixes in `9.3.11`,
compaction correction in `9.3.13`, and cache-budget/checksum/iterator-cache corrections in
`9.3.14`. A newer native library cannot be substituted without changing the wrapper build and ABI
subject: its build script requests an exact version.

No archive or clone used for this review is a LaminarDB dependency or candidate identity. A future
subject would require immutable source archives and digests, an isolated exact lockfile, SBOM,
compiler/target/build flags, allocator, codec features, license/NOTICE review, and a reviewed native
ABI statement.

## Static findings: fact, inference, and unknown

### Rust integration safety — fail

The public `TidesDB`, `ColumnFamily`, `Transaction`, and `Iterator` wrappers contain raw native
pointers. Returned child objects have no Rust lifetime parameter or shared owner tying them to
their parent:

- safe Rust can drop `TidesDB` while a `ColumnFamily` or `Transaction` remains;
- safe Rust can drop a `Transaction` while its `Iterator` remains; and
- dropping a column family by name can leave another independently obtained safe Rust handle to the
  freed native column family; and
- the native iterator stores both `tidesdb_column_family_t *` and `tidesdb_txn_t *`, then
  dereferences them during iteration and read-set tracking.

This is a source-proved lifetime-contract hole, not an empirically triggered crash claim. The
wrapper also marks raw-pointer owners `Send`, and marks the database and column family `Sync`,
without making parent ownership explicit. A future wrapper must use borrow lifetimes or shared
ownership plus ordered shutdown, justify or remove each unsafe thread-safety trait, and pass Miri
plus native ASAN/TSAN/UBSAN coverage before it can be called safe.

There is a separate source-proved callback race. Safe Rust permits multiple `Send`/`Sync` column-
family handles and concurrent commit with hook clear, replacement, or handle drop. Native hook
installation is two unsynchronized pointer stores, while commit reads and calls those fields without
a lock; clear/replacement frees the boxed Rust callback context. That permits a native data race and
use-after-free through safe Rust. Comparator and commit-hook trampolines also invoke closures without
an explicit `catch_unwind` boundary, so there is no contained panic policy and a callback panic can
abort the process. Both defects must close before an adapter is considered.

### Atomic mutation and acknowledgement — conditional native fit, current path fail

TidesDB supplies point operations, ordered iterators, MVCC transactions, multiple column families,
and batched WAL records. Those are useful C1 ingredients. They do not by themselves establish the
LaminarDB mutation contract.

The current native header explicitly distinguishes two layouts:

- in default per-column-family mode, one transaction writes separate WALs sequentially; a crash or
  I/O/OOM failure after an earlier column family can leave a recovered prefix that is treated as
  committed; and
- in unified-memtable mode, one shared WAL batch is intended to make a cross-column-family
  transaction crash-atomic.

LaminarDB's four logical state/timer/join namespaces require the second property. Any future profile
must therefore pin unified mode and `TDB_SYNC_FULL`; both unified mode and its sync mode default to
off/`NONE`. An ordinary transaction commit, WAL write, or commit hook is not a distributed
checkpoint or an exactly-once receipt.

Default `READ_COMMITTED` does not make the first layout runtime-atomic. Commit publishes an
in-progress transaction before applying its column families sequentially, but point reads disable
commit-status visibility and iterators filter by sequence rather than that status. A concurrent
reader can therefore observe a partially applied multi-column-family transaction before commit
returns. This contradicts the native header's runtime-atomicity statement and is a source-proved
failure for LaminarDB's cross-namespace mutation contract.

The Rust commit-hook documentation calls the data fully durable before callback invocation, but
the native sync modes explicitly allow `NONE` and interval acknowledgement without a per-commit
stable-storage barrier. The hook is therefore not a durability receipt unless the exact FULL-sync
path and its failure boundary are separately proved.

Even in unified mode, the implementation appends and, in FULL mode, synchronizes the WAL before
applying the batch to the in-memory skip list. A later application allocation failure can return an
error even though restart will replay the already-written record. Some large-transaction fallback
branches skip a failed prefixed-key allocation, ignore individual insertion failures, and can return
success after a partial in-memory apply. Commit-error cleanup releases memtable references but does
not establish a reviewed abort/removal/reservation-release state transition. These are exact-source
concerns, not fault-injection results. Until upstream resolves the semantics and adversarial tests
prove them, every non-success after the WAL append must be a fatal unknown-commit-point event:
LaminarDB must stop, restore a committed cut, and reconcile; it must not retry blindly.

Unified-WAL recovery is also fail-open in inspected source. Its replay helper breaks on several
malformed/truncated-field and allocation paths, ignores skip-list insertion returns, and then returns
success unconditionally. Lower-generation recovery ignores that result and unlinks the WAL. The
engine can therefore accept a partial replay and delete its remaining recovery input. This is a
source-proved control-flow defect; no corruption or power-loss experiment was run. A strict
corruption/failure contract and retained forensic input are mandatory before this path can re-enter.

The source permits up to 256 column families and defines the operation limit as `INT_MAX`, which is
not a useful production bound. LaminarDB must impose smaller byte, row, key, value, result, and
service-time caps before native allocation or FFI entry.

### Snapshot, checkpoint, restore, and rebalance — native checkpoint fail

The v9.3.14 checkpoint implementation is unsuitable as LaminarDB recovery authority:

- in the required unified-memtable mode, it never rotates, drains, or copies `unified_mt.active`,
  `unified_mt.immutables`, or their WAL; its directly called force-flush helper examines only each
  per-column-family active memtable, which is intentionally unused in that mode, so acknowledged but
  unflushed state can be absent from a successful checkpoint;
- it processes column families sequentially without a global write or transaction barrier;
- writers can cross the individual column-family capture points;
- it copies a raw column-family pointer and releases the list lock before use, while concurrent
  column-family drop can remove and free that object without a checkpoint reference or epoch;
- after its force-flush stage, a concurrent writer can rotate and flush more state while checkpoint
  enumeration continues;
- a bounded loop attempts to acquire `is_compacting`, but after the loop the code does not verify
  that acquisition succeeded before proceeding and later clears the flag unconditionally;
- SST files are enumerated before the manifest file is copied, while concurrent flush publication
  remains possible, so the copied manifest/file set has no proved single cut;
- files are written directly into the destination, with no file/directory sync, sealed inventory,
  completion marker, atomic directory publication, checkpoint sequence, or safe cleanup contract;
- the copy helper treats some source-open failures, including `ENOENT` and `EACCES`, as success; and
- a failed partial destination is non-empty, while the API requires an empty destination for retry.

These are source-level consistency concerns requiring upstream confirmation and adversarial proof;
this review did not corrupt or open a produced checkpoint. The call also forces flush work and
holds compaction exclusion across file linking/copying, so its foreground and victim tail impact is
unknown.

Native checkpoint is optional for LaminarDB. The authoritative design remains a consistent logical
read cut followed by deterministic, portable vnode export, validation, upload, and sealed
publication. TidesDB transactions and ordered iterators make such a path plausible, but it still
needs one immutable cross-namespace snapshot, canonical unsigned-byte bounds, bounded lifetime,
atomic restore into an unservable generation, and fenced/idempotent cleanup proof. Native directory
checkpoint may be reconsidered only as an optimization after its own contract closes.

### Resource governance and bounded memory — block

The native engine exposes a useful `max_memory_usage` pressure/accounting setting, block-cache size,
write-buffer sizes, background-worker counts, and file limits. It is not proved to be a hard
process/RSS cap: the pressure scan counts selected structures and estimated compaction needs, not
allocator and page-cache headroom. Current source also derives total memory from Linux `sysinfo`,
not cgroup v1/v2, and enforces a minimum resolved limit of five percent of host physical RAM. An
explicit lower value is clamped upward with a warning. In a small container on a large host this can
exceed the cgroup budget. Auto mode uses 75 percent of host RAM in v9.3.14; the Rust wrapper
documentation still says 50 percent, which is another symptom of subject drift.

This blocks a container production profile until the engine honors the effective cgroup/process
limit without an unsafe host-derived minimum. External cgroup enforcement is still mandatory, but
an internal pressure valve configured above that hard limit is not acceptable. Qualification must
also account for native allocator arenas, transaction buffers, iterator/snapshot retention, block
and B-tree caches, page cache, WAL/SST construction, background concurrency, file descriptors,
disk quota, and write/space amplification. Published RSS is not bounded-memory proof.

### Maintenance health and DKS-Q2-006 — block

TidesDB has useful cheap candidate-native signals:

- database memory-pressure level, total memtable bytes, immutable count, flush pending count, and
  flush/compaction queue depth;
- per-column-family `is_flushing`/`is_compacting` flags that include queued and in-flight work,
  level file/byte populations, read amplification, tombstones, and cumulative
  WAL/flush/compaction/user bytes and counts; and
- object-store upload queue and retry-exhaustion/failure-attempt counts when that optional mechanism
  is enabled; terminal semantics remain unresolved because the reaper can retry parked work.

That is not a complete mapping under the approved maintenance-health-v2 direction. No reviewed
public Rust/native surface was found for exact foreground-pressure stall intervals and reasons, or
for general local flush/compaction/background errors. Logs and a retryable `BUSY` return do not
reconstruct lossless interval start/end/count/duration. Object-store upload failures cover only
that optional subsystem. Queue gauges and completed-byte counters cannot prove that every enabled
mechanism is healthy, nor distinguish slow progress from a swallowed local background failure.

The native backpressure path contains 10 ms polling stalls, graduated delays, active/unified
memtable ceiling stalls, global critical-memory stalls, and an approximately ten-second no-progress
budget before `BUSY`. A bounded slow-path observer could plausibly expose exact episodes without an
expensive per-row metric call, but it does not exist in the inspected public Rust subject. Any
future paper mapping must cover every enabled mechanism, backlog/in-flight pressure, background
failure, tail quiescence, and exact foreground stalls, with stable scope, reset/wrap, cadence,
atomicity, and observer-overhead proof. Missing is blocked, never zero.

### Hot path and low latency — unknown, not inherited from benchmarks

The engine is C, not Rust; the official Rust path adds synchronous FFI. Iterator keys and values are
copied into new Rust `Vec`s. There is no reviewed native multi-get in the Rust surface. None of
those facts is an automatic performance failure, but they constrain the adapter:

- state requests must be Arrow-batch coalesced rather than one FFI call, transaction, future, or
  task per row;
- every disk-capable, lock-waiting, flush, checkpoint, iterator, and commit call must run on bounded
  non-event-loop lanes with bounded admission and result buffers;
- engine-owned slices and iterators must not escape the service interval;
- no per-row metric read, allocation, log, fsync, or task spawn is allowed; and
- open-loop offered latency, service latency, queue delay, p50/p95/p99/p99.9/max, hot-writer/victim
  isolation, telemetry interference, and sustained-compaction tails must all be measured.

Throughput from a closed-loop storage or SQL benchmark cannot waive those requirements.

## Zipf performance claim audit

### July 2026 SQL-stack comparison

The current headline evidence compares TidesDB `9.3.11` through TideSQL `4.5.9` with MyRocks in
MariaDB `11.8.6`. It uses Zipf exponent `0.8` on an i7-11700K, 48 GiB host and consumer SATA SSD.
The article reports substantial TidesDB throughput and p95 advantages in several runs.

The same article also establishes why those results cannot select a LaminarDB backend:

- durability is disabled for both engines: binary logging is off, MyRocks uses
  `rocksdb_flush_log_at_trx_commit=0`, and TidesDB uses sync mode `NONE`;
- runs last 60–300 seconds, have no warmup, and are single executions;
- configurations change between runs, including compression and unified mode;
- the headline table exposes transaction TPS and p95, and one narrative run reports a maximum
  spike, but there is no consistent per-state-operation p99/p99.9/max distribution;
- the high-concurrency runs reach 512 threads, retry thousands of operations, and have hundreds of
  milliseconds to seconds of tail latency;
- the SQL transaction includes point, range, update, delete, and insert work through different
  storage plugins, not vnode-prefixed aggregate/window/join state; and
- it contains no crash/power-loss, checkpoint, restore, rebalance, bounded-memory, long maintenance
  tail, or independent-operator evidence.

The raw bundle is published with SHA-256
`40338a7256d9b8a75c882cca812d56a1e37ce6a4008b91f95e6b4340fbe11f61`. Publication is useful, but
it does not make the workload or durability boundary equivalent.

### Direct engine and full-sync comparisons

The May 2026 direct comparison covers TidesDB `9.2.1` and RocksDB `11.1.1`; the audited Zipf phases
use 16-byte keys, 100-byte values, five million operations, eight threads, and three executions
summarized by the median. Its published raw artifact has SHA-256
`b494461279cd43d97f0ce4e6c23da2596815054cd6f36479aabb1e4409f178f1`.

Static artifact review found the following Zipf medians:

| Phase | TidesDB | RocksDB | Ratio |
|---|---:|---:|---:|
| Write PUT | 1.213 M ops/s | 0.705 M ops/s | 1.72x |
| Mixed PUT | 1.211 M ops/s | 0.703 M ops/s | 1.72x |
| Mixed GET | 1.217 M ops/s | 1.191 M ops/s | 1.02x |
| Seek | 1.256 M ops/s | 0.934 M ops/s | 1.35x |

Sync is disabled and write/mixed operations use batches of 1,000. Several hot phases last only
about four to seven seconds. TidesDB uses approximately 2.5–2.7 times RocksDB peak RSS in the
write/mixed phases, and other read/iteration phases are mixed. The linked benchmark repository and
runner currently return 404, while the raw artifacts omit the exact Zipf theta/seed, observed
cardinality, full options, cache/reset procedure, and compaction reset. The batch p99 values are
batch latency, not LaminarDB event latency.

The January 2026 full-sync comparison is older—TidesDB `7.1.0` versus RocksDB `10.7.5`—but proves
the ordering is durability- and workload-sensitive. RocksDB wins Zipf write-only throughput
(214.8 K versus 181.8 K ops/s) and batch-one durable random put (4.04 K versus 2.50 K ops/s), while
TidesDB wins that report's mixed Zipf reads/writes and iteration. This is why “faster on Zipf” must
always name durability, batch size, operation mix, and tail metric.

The current native built-in benchmark has an additional generator warning: its `zipf_next` helper
returns `uint8_t` even when parameterized by a much larger operation count, so conversions wrap and
limit the observable generated keyspace to at most 256 values. There is no evidence that the missing
external comparison runner used this same helper, so this finding does not invalidate the published
artifacts. It does mean the built-in generator is ineligible for LaminarDB qualification.

### Fair future hypothesis test

If a later owner explicitly admits a repaired TidesDB subject, its comparison must use the same
candidate-neutral workload and resource envelope as the retained candidates:

1. exact immutable engine/wrapper/build/allocator/filesystem/device identities;
2. vnode-prefixed aggregate, window, timer, and join state with point reads, ordered bounded scans,
   tombstones, snapshots, checkpoint export, restore, and rebalance;
3. Zipf exponents such as `0`, `0.8`, `0.99`, and `1.1`, plus moving hotspots, with fixed seeds,
   observed cardinality, and rank-histogram receipts;
4. equal hard cgroup memory, CPU, I/O, page-cache policy, database cache/write-buffer, worker, FD,
   quota, and offered-load limits;
5. separate asynchronous and production-durable modes, with FULL/unified durability matching the
   actual checkpoint barrier;
6. open-loop latency including queueing, p99.9/max, stalls, recovery and checkpoint tails, CPU/op,
   PSS/RSS, device bytes, growth, and amplification;
7. process crash, cache loss/power-cut proxy, I/O error, ENOSPC, corruption, unknown acknowledgement,
   restore, rescale, and old-owner fencing arms; and
8. long steady state followed by separately governed 24/72-hour backend endurance. Backend
   endurance still does not replace the independent release-candidate product soak.

This list defines no execution authority.

## Mandatory gate matrix

Absolute gates are conjunctive. A weighted score is forbidden because faster throughput cannot
cancel memory unsafety, torn recovery, an unavailable state cut, or missing failure signals.

| Gate | Current evidence | Disposition before any run |
|---|---|---|
| Exact deployable subject | Rust `0.11.1` selects native `9.3.6`; current engine and benchmarks are different revisions | **FAIL** — produce a lifetime-safe wrapper pinned to the exact reviewed native release and complete provenance |
| Legal/build adoption | MPL-2.0 is identifiable; C toolchain, bundled codecs, ABI, SBOM, notices, target flags, and redistribution package are not frozen | **BLOCK** |
| C1 ordered KV primitives | Point operations, transactions, snapshots/isolation, ordered bidirectional iteration, seeks, deletes, and multiple CFs exist | **CONDITIONAL** — exact byte ordering, bounded scans, read-cut semantics, and canonical export/restore still need conformance |
| Cross-namespace atomicity | Per-CF mode is crash-nonatomic; default `READ_COMMITTED` point reads and iterator paths permit partial runtime visibility; unified mode has silent/ambiguous apply paths | **FAIL** — a repaired unified+FULL path needs fault proof |
| Rust memory and FFI safety | Parent/child lifetimes are not encoded; hook replacement can race callback use; callbacks have no panic boundary | **FAIL** |
| Portable checkpoint/restore | Native checkpoint omits shared unified state and has no atomic global cut or publication; logical export is only plausible | **FAIL native / BLOCK logical** |
| Rebalance lifecycle | No Laminar vnode epoch, inactive-generation restore, atomic publication, fencing, resumable cleanup, or retained-cut GC | **MISSING** — backend-independent ADR work remains required |
| DKS-Q2-006 maintenance health | Useful queues/pressure/progress stats; exact stalls and general local background-error coverage missing | **BLOCK** |
| Bounded memory and resource governance | Host-RAM minimum can exceed cgroup; full native/page-cache/temporary accounting unproved | **BLOCK** |
| Low-latency hot path | Vendor throughput only; synchronous FFI, copies, service lanes, C2/C3 tails, and observer overhead unmeasured | **UNKNOWN** |
| Durability and recovery | WAL and FULL sync exist, but unified replay fails open and stable acknowledgement, strict corruption, process/cache-loss truth table, N/N-1 and RTO are unproved | **FAIL current / BLOCK repaired** |
| Fault/endurance/upgrade | No Laminar-bound candidate evidence | **MISSING** |
| Source/sink delivery | A local store cannot supply source replay/fencing or sink publication semantics | **SEPARATE CLOSED GATE** |
| Independent production soak | Not run | **MISSING; production NO-GO** |

## Carry-forward decision matrix

This is the Cycle 28 decision input, not a ranking result:

| Candidate | Current role | Principal unresolved veto | Carry decision |
|---|---|---|---|
| RocksDB `10.4.2` through `rocksdb 0.24.0` | Mature operational LSM reference and one of the two frozen v4 comparison subjects | Exact complete pressure-stall source/binding, native memory, durable truth table, common C1/C2/C3/fault/endurance evidence | **Carry as reference/closure track; not selected.** TidesDB evidence does not justify removing it. |
| Fjall `3.1.8` | Rust-native LSM reference and the other frozen v4 comparison subject | Stable public pressure/progress/error/resource/stall surface and global controls remain insufficient | **Retain in frozen comparison lineage; no production admission.** |
| redb `4.1.0` | Rust-native B-tree/single-writer hedge under a separate prescreen | Global non-cancellable writer, durability/recovery/resource truth, and approved non-LSM health mapping | **Continue only its approved validation protocol; do not execute or pool it with v4.** |
| TidesDB native `9.3.14` | New C-engine research option with interesting skew/batch performance | No safe exact-current Rust path; checkpoint, resource governance, health, failure semantics, and Laminar evidence blocked | **Do not carry into a runner or replace RocksDB. Retain research-only pending upstream/fork closure.** |

SurrealKV `0.21.2` remains rejected unmodified by the earlier exact-source audit; adding TidesDB does
not reopen its snapshot-retention defect. No candidate is selected by elimination.

## Checkpoint, exactly-once, source, and sink boundary

A backend transaction and WAL solve only local working-state mutation. A later cluster exactly-once
certification still requires one composed recovery decision:

```text
exact-certified, replay-stable, assignment-fenced source position
+ one frozen and sealed vnode-state/timer/output cut
+ durable coordinator decision and ownership epoch
+ checkpoint-committable external sink transaction that atomically consumes the predecessor cursor
+ fencing by deployment, pipeline/sink namespace, checkpoint attempt, and live leader term
+ ambiguous-commit recovery
```

TidesDB's unified WAL does not provide source offsets, coordinator consensus, sink transactions,
old-owner fencing, or portable vnode artifacts. Native object-store features also do not inherit
LaminarDB's manifest, retention, restore-before-serve, or connector capability contracts.

The initial distributed-state release remains **at-least-once**. Its concrete source/sink profile
must still be certified: the source must be partitionable, replayable, assignment-fenced, and
checkpointable; the sink must support the output mode, multiple writers, durability, ambiguity
handling, and at-least-once delivery. Retraction/full-changelog remains fail-closed. End-to-end
exactly-once is a later certification per exact source/state/coordinator/sink combination and
requires all properties above. A local backend cannot provide any missing connector/provider commit,
fencing, or reconciliation operation, so no backend choice may upgrade delivery implicitly.

## Smallest honest re-entry path

TidesDB should be reconsidered only in this order:

1. upstream or maintain a Rust wrapper pinned to the current reviewed native revision, with encoded
   parent/duplicate-handle lifetimes, ordered shutdown, synchronized callback ownership, justified
   thread-safety traits, panic containment, and sanitizer/Miri evidence;
2. make the internal pressure budget cgroup-aware and safely below the external hard cgroup limit,
   prove allocator/page-cache/temporary headroom, and define a candidate unified-memtable plus
   FULL-sync profile subject to the remaining steps;
3. fix strict unified-WAL recovery, post-WAL acknowledgement, silent/ignored apply failures, and
   transaction cleanup; establish point/iterator commit-status visibility and immutable cross-column-
   family read-cut semantics; then prove them with corruption, allocation, I/O, crash and cache-loss
   faults;
4. obtain upstream answers or fixes for unified checkpoint omission, column-family lifetime and
   concurrent capture/publication; use logical export as authority only after the read-cut contract
   above closes, unless native checkpoint later proves a durable sealed atomic cut;
5. write a TidesDB maintenance-health-v2 paper mapping and close exact stall/background-error
   sources with bounded off-hot-path collection;
6. obtain a new owner decision for an additive TidesDB profile and candidate source construction;
   do not edit or reinterpret the current two-candidate v4 lineage;
7. only after all static and authorization gates close, run the common C1/C2/C3, durability,
   fault, Zipf/hot-victim, restore, rebalance, and 24/72-hour backend campaign; and
8. after one backend is selected and the complete feature is integrated, run the separately
   operated black-box production soak over the unchanged release artifact before any production
   claim.

A failure at any step stops the track. A pass funds the next step; it never retroactively creates
qualification or production evidence.

## Research hygiene and primary sources

No existing project research document became irrelevant because of this prescreen. The dated
RocksDB/Fjall/redb/SurrealKV reports remain decision history and are not deleted. This report
supersedes only the idea that a vendor Zipf result can justify substituting TidesDB for RocksDB.

Project contracts used for the decision:

- [maintenance-health v2 proposal](../architecture-decisions/state-backend-maintenance-health-v2-proposal.md)
- [qualification runner v2 freeze candidate](../architecture-decisions/state-backend-qualification-runner-v2-draft.md)
- [C1 qualification model](../architecture-decisions/state-backend-qualification-model-v1.md)
- [managed vnode-keyed state ADR](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md)
- [production soak charter](../testing/distributed-state-production-soak-charter.md)
- [prior exact-source backend audit](state-backend-static-audit-2026-07-23.md)

External primary sources:

- [TidesDB native v9.3.14 release](https://github.com/tidesdb/tidesdb/releases/tag/v9.3.14)
  and [exact source](https://github.com/tidesdb/tidesdb/tree/6fe1e83104b70255a694239d360a14bae51d0c70)
- [official Rust v0.11.1 release](https://github.com/tidesdb/tidesdb-rs/releases/tag/v0.11.1),
  [manifest](https://github.com/tidesdb/tidesdb-rs/blob/v0.11.1/Cargo.toml),
  [build script](https://github.com/tidesdb/tidesdb-rs/blob/v0.11.1/build.rs), and
  [exact wrapper source](https://github.com/tidesdb/tidesdb-rs/tree/e2febbc548e7f0158d1c09ea487aa0bb7c343616)
- [native v9.3.7](https://github.com/tidesdb/tidesdb/releases/tag/v9.3.7),
  [v9.3.9](https://github.com/tidesdb/tidesdb/releases/tag/v9.3.9),
  [v9.3.10](https://github.com/tidesdb/tidesdb/releases/tag/v9.3.10),
  [v9.3.11](https://github.com/tidesdb/tidesdb/releases/tag/v9.3.11), and
  [v9.3.13](https://github.com/tidesdb/tidesdb/releases/tag/v9.3.13) release notes
- [July 2026 sysbench Zipf comparison](https://tidesdb.com/articles/sysbench-rw-tidesdb-v9-3-11-tidesql-v4-5-9-rocksdb-myrocks-in-mariadb-v11-8-6/)
- [May 2026 direct engine comparison](https://tidesdb.com/articles/benchmark-analysis-tidesdb-v9-2-1-rocksdb-v11-1-1/)
- [January 2026 full-sync comparison](https://tidesdb.com/articles/benchmark-analysis-tidesdb-v7-1-0-rocksdb-v10-7-5-full-sync/)
- [TidesDB tuning reference](https://tidesdb.com/reference/tuning/)

All external performance sources above are produced by the TidesDB project. They are useful primary
artifacts, not independent LaminarDB qualification or soak evidence.

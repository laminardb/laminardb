# State-backend maintenance-health mapping designs — Cycle 19

- **Date:** 2026-07-24
- **Evidence class:** non-normative, read-only exact-source design
- **Contract basis:** [maintenance-health v2 direction](../architecture-decisions/state-backend-maintenance-health-v2-proposal.md),
  approved after this Stage 2 report
- **Mapping artifacts created:** none
- **Backend selected at Cycle 19:** none; Cycle 39 later selects TidesDB as the target
- **Candidate construction or execution authorized:** no
- **Cluster admission:** unchanged; `[LDB-4007]` and `[LDB-0013]` remain fail-closed
- **Production verdict:** **NO-GO**

**Cycle 39 current direction:** the mappings below remain frozen Fjall/RocksDB/redb vocabulary and
source-gap provenance; they are not the active product ranking. TidesDB is the selected worker-local
target, but its current official Rust path remains rejected. Its one-CF/fresh-restore
[design](../architecture-decisions/tidesdb-local-state-successor-design.md) requires successor
mapping/profile identities only after source closure. No candidate source, adapter, or execution is
authorized by this report.

## Historical Cycle 19 outcome

The proposed sampled-signal vocabulary can describe the continuous health obligations without a
generic metrics DSL or a cross-engine score. A blocking tail-wait command is not silently recast as
a sampled boolean; if owners later require one as gate evidence, it needs a separately typed outcome
and contract review. No unmodified candidate is ready to run:

| Candidate | Proposed arm | Stock-source result | Smallest next decision-producing work |
|---|---|---|---|
| RocksDB 10.4.2 / `rocksdb` 0.24.0 | `observed` | Flush and compaction expose a useful core set. Scheduled bottom-priority compaction, asynchronous purge, complete background-error/recovery observation, and exact foreground stalls remain unsupported. | Under a later approved v2 contract and separate source-closure authority, specify and adversarially prove the enumerated native/C/safe-Rust closure; effort and interference are unknown. |
| Fjall 3.1.8 / `lsm-tree` 3.1.8 | `observed` | Existing hidden gauges are diagnostics only. Scheduler correctness/liveness, complete background failure, physical cleanup tail, strategy pressure, resource control, and exact stalls remain unsupported. | Decide whether to fund a lossless scheduler/lifecycle repair plus stable maintenance/error/stall surface; effort and interference are unknown. |
| redb 4.1.0 | whole-arm `not_applicable` is source-plausible | The exact crate creates no runtime maintenance worker, but complete-process proof and a bounded native forced probe do not exist. | Cycle 19 proposed a separately governed native prescreen; redb is now parked and no probe is scheduled. |

At Cycle 19 this narrowed the next decisions without ranking unbuilt patches. RocksDB had a concrete multi-layer
native/C/safe-Rust closure to specify. Fjall has a different Rust scheduler/lifecycle and telemetry
closure whose cost is also unknown. redb has a separately bounded prescreen for its global writer,
durability, recovery and N/A premise. If native C++ ownership is prohibited, there is still no ready
Rust-only fallback: first obtain redb's prescreen result and an explicit Fjall fork-ownership
decision. No candidate may win because another candidate is blocked or because N/A produces fewer
veto signals.

## Authority and interpretation

This report is Stage 2 decision input from the v2 proposal. The project owner subsequently recorded
`APPROVE_MAINTENANCE_HEALTH_V2_DIRECTION` on 2026-07-24. That decision does not instantiate any
reserved v2 identity, freeze a schema or threshold, authorize source changes, or emit
`state-backend-mechanism-mapping/v2`. Signal names below remain paper-design names, not wire
identifiers. Predicate *kinds* are proposed; all numerical limits, cadence, cut-skew, hold time,
timeout, and observer-overhead limits must be frozen before any result exists. Cycle 38 carries these
inputs into the accepted
[consolidated runner v2 validation contract](../architecture-decisions/state-backend-qualification-runner-v2-draft.md).

The candidate-native arm is a veto only. The identical C1/C2/C3 correctness, open-loop queue/service/
end-to-end latency, throughput, memory, target-device I/O, disk growth, write/space amplification,
physical persistence, recovery, fault, and endurance gates remain the comparison surface. Exact
foreground pressure-stall intervals remain a separate mandatory arm. Source/sink delivery,
checkpoint/rebalance ownership, exactly-once composition, and independently operated product soak
remain later independent vetoes.

## Exact subjects and method

The source was read from the locally cached crates.io archives and compared with the existing
[static audit](state-backend-static-audit-2026-07-23.md),
[RocksDB source closure](rocksdb-mechanism-source-closure-2026-07-24.md), and
[redb mechanism note](redb-4.1.0-prescreen-mechanism-note-2026-07-23.md). No candidate was compiled,
modified, or run in this cycle.

| Subject | Exact source identity |
|---|---|
| Fjall | `fjall-3.1.8` archive SHA-256 `420a84699b8ccbb1ed573e38e88f4f23637b45beab6432066452f834be469c57`, packaged VCS `6debe706dbc53d6d0eb666aae5057671d5c1370f` |
| Fjall LSM | `lsm-tree-3.1.8` archive SHA-256 `055a908d502129cf63bedae52f2db222e4436d2da32a69df9b84ac9fb9147761`, packaged VCS `f09f4235c5e6735c54f99c0d425784602ce71975` |
| Rust RocksDB wrapper | `rocksdb-0.24.0` archive SHA-256 `ddb7af00d2b17dbd07d82c0063e25411959748ff03e8d4f96134c2ff41fce34f`, wrapper/sys VCS [`bb7d216`](https://github.com/rust-rocksdb/rust-rocksdb/tree/bb7d2168eab1bc7849f23adbcb825e3aba1bd2f4) |
| Native RocksDB | `librocksdb-sys-0.17.3+10.4.2` archive SHA-256 `cef2a00ee60fe526157c9023edab23943fae1ce2ab6f4abb2a807c1746835de9`, bundled engine [`410c562`](https://github.com/facebook/rocksdb/tree/410c5623195ecbe4699b9b5a5f622c7325cec6fe) |
| redb | `redb-4.1.0` archive SHA-256 `8e925444704b5f17d32bf42f5b6e2df050bceebc3dcd6e71cc73dafe8092e839`, packaged VCS [`6ed1f98`](https://github.com/cberner/redb/tree/6ed1f981ba4deab0b2adbdd7bccb46ec409b2191) |

The intended logical domain remains one database, four fixed physical keyspaces/column families and
256 logical vnodes. The proposed 8-GiB cache, 2-GiB write-buffer budget and six-worker controls apply
to the Fjall/RocksDB comparison subject only—and Fjall cannot currently enforce the global write
budget. redb's 8-GiB cache is dynamically partitioned, its write buffer may consume half, and it has
no independent 2-GiB control. A later redb candidate therefore needs an approved additive profile/
configuration translation and the same common cgroup, space and I/O gates; unsupported control must
not be presented as equivalence. Compression/blob optimizations remain disabled where applicable.
These are proposed inputs, not an approved v4 profile. Every candidate must freeze its exact
archive, transitive lock, features, target, toolchain, flags, adapter and configuration before a
real mapping exists.

For each asynchronous state-storage mechanism, the audit sought the smallest native observations
that jointly cover backlog or in-flight pressure, failure, and tail quiescence. A signal is retained
only with an exact scope, kind/unit, quality, reset behavior, bounded collection story, and a closed
predicate kind. A useful diagnostic is not promoted when it cannot close an objective cheaply.

## RocksDB paper mapping

### Configuration and mechanism boundary

The baseline keeps normal and WAL-pressure memtable flush, automatic level compaction, marked-file
and bottommost compaction, and low-latency asynchronous obsolete-file purge. The eventual mapping
must freeze every option and prohibit unrecorded `SetOptions` mutation.

| Mechanism or API | Required treatment in the exact mapping |
|---|---|
| Experimental mempurge | Set `experimental_mempurge_threshold=0`, or map separately. |
| Periodic and TTL compaction | Explicitly set `periodic_compaction_seconds=0` and `ttl=0`; defaults using an auto-selection sentinel are not proof of zero. |
| Integrated blob files/GC | Set both blob-enable and blob-GC options false. |
| External/remote compaction service | Require absent. |
| Manual compaction, `CompactFiles`, refit/promote, external ingest, and administrative `DeleteFilesInRange` | Prove unreachable from the adapter and qualification control plane, or map separately. Ordinary `DeleteRange` tombstones remain in normal compaction coverage. |
| Archived-WAL TTL/size reclamation | Freeze both archive limits to zero. Ordinary obsolete WAL lifecycle remains in purge coverage. |
| `SstFileManager` trash/deletion scheduler | Require immediate/default deletion, or include its independent queue and failure lifecycle. |
| Persistent engine statistics | Keep disabled; telemetry/log housekeeping is outside native health but remains charged to common resources. |
| Automatic background-error recovery | Either prove every retry/recovery path disabled, or map both `ErrorHandler` and `SstFileManager` recovery state/events. Setting `max_bgerror_resume_count=0` alone does not eliminate every NoSpace recovery path. |
| Obsolete file, WAL-writer, and SuperVersion purge | Keep for the proposed low-latency configuration and close the source gap below. |

### Smallest useful existing set

Every column-family value is flattened for the four frozen column families. DB-wide values are read
once through the default column family; querying them once per column family would duplicate the
same database state. None of these values is summed with another signal.

| Paper signal | Exact property/handler | Scope; kind/unit; quality | Objective and proposed predicate kind | Important semantics |
|---|---|---|---|---|
| `flush_unflushed_memtables.<cf>` | `rocksdb.num-immutable-mem-table`; `NumNotFlushed()` | CF; unsigned gauge/items; exact current count | Flush backlog; measurement maximum and tail upper bound | Stronger for this objective than the redundant flush-pending boolean. Reconstructed state may be nonzero after reopen. |
| `flush_jobs_running` | `rocksdb.num-running-flushes`; `num_running_flushes()` | DB; unsigned gauge/jobs; exact | Flush in-flight; measurement maximum and tail upper bound | Sample once, not per CF. |
| `compaction_rewrite_estimate.<cf>` | `rocksdb.estimate-pending-compaction-bytes`; `estimated_compaction_needed_bytes()` | CF; unsigned gauge/bytes; **estimated** and level-compaction-only | Level pressure; measurement maximum and tail upper bound; no-increase may be additional only | Never call it exact debt or compare it numerically across engines. A stable large backlog cannot pass merely because it stopped growing. |
| `compaction_pending.<cf>` | `rocksdb.compaction-pending`; configured picker's `NeedsCompaction()` | CF; boolean; exact at the individual read | Other picker reasons and tail health; required tail state | Covers current picker need, not a durable queue identity or atomic multi-CF cut. |
| `compaction_jobs_running` | `rocksdb.num-running-compactions`; `num_running_compactions_` | DB; unsigned gauge/jobs; exact | Compaction in-flight; measurement maximum and tail upper bound | Sample once. |
| `background_job_failures` | `rocksdb.background-errors`; default-CF `bg_error_count_` | DB in meaning; monotonic counter/operations; exact for counted flush/compaction failures | Background failure; counter-delta upper bound | Counter is stored/read via default-CF internal stats and restarts with a new DB instance. It is not complete purge/recovery evidence. |

Approximate memtable bytes, L0 file count, live-version count, SST sizes, stopped/delayed state and
obsolete-SST bytes remain diagnostics unless a reviewed objective requires one. In particular,
`rocksdb.obsolete-sst-files-size` excludes files after they are scheduled for deletion and omits
other purge work; it cannot close cleanup health.

Each integer property read holds RocksDB's DB mutex. The current safe wrapper calls the
string-returning C API, allocates, and parses a `u64`, even though the native C API has integer
property calls. Qualification therefore needs an allocation-free safe integer binding, fixed
off-event-loop cadence, per-signal brackets, and a cut-skew rejection rule. Sequential properties
are individually mutex-consistent but do not form one atomic multi-property/multi-CF cut.

### Source and binding gaps

A picked compaction can leave picker-visible pending state, be forwarded to the bottom-priority
pool, and remain scheduled before the running-compaction count rises again. The exact configuration
must either prove that forwarding path absent or expose the DB's scheduled low- and bottom-priority
compaction populations as separate exact gauges. Both need measurement maximum and tail upper-bound
predicates. This means the six existing signals are a core set, not a complete compaction mapping.

| Proposed source | Native population | Scope; kind/unit; quality | Proposed predicate |
|---|---|---|---|
| `compaction_jobs_scheduled_low` | `bg_compaction_scheduled_` | DB; unsigned gauge/jobs; exact only after its scheduled/running lifecycle is source-frozen | Measurement maximum and tail upper bound |
| `compaction_jobs_scheduled_bottom` | `bg_bottom_compaction_scheduled_` | DB; unsigned gauge/jobs; exact only after its forwarding lifecycle is source-frozen | Measurement maximum and tail upper bound |

With `avoid_unnecessary_blocking_io=true`, `BGWorkPurge` asynchronously drains obsolete-file,
WAL-writer, and SuperVersion queues. Flush/compaction workers can also call synchronous
`PurgeObsoleteFiles` before their running count is decremented. One allocation-free snapshot under
the existing DB mutex is a possible source, but it must keep the heterogeneous populations typed:

| Proposed source | Native population | Scope; kind/unit; quality | Proposed predicate |
|---|---|---|---|
| `purge_files_queued` | `purge_files_.size()` | DB; unsigned gauge/files; exact under the snapshot mutex | Measurement maximum and tail upper bound |
| `purge_wal_writers_queued` | `wals_to_free_queue_.size()` | DB; unsigned gauge/writers; exact under the snapshot mutex | Measurement maximum and tail upper bound |
| `purge_superversions_queued` | `superversions_to_free_queue_.size()` | DB; unsigned gauge/objects; exact under the snapshot mutex | Measurement maximum and tail upper bound |
| `purge_producers_active` | `pending_purge_obsolete_files_` | DB; unsigned gauge/operations; exact under the snapshot mutex | Measurement maximum and tail upper bound |
| `purge_callback_scheduled` | `bg_purge_scheduled_` | DB; boolean state; exact under the snapshot mutex | Required false tail state |
| `purge_failures` | Every unsuccessful state-bearing table/blob/WAL/manifest/options deletion branch in synchronous and deferred purge | DB; saturating monotonic counter/operations; exact only after complete branch proof | Counter-delta upper bound; saturation invalidates evidence |

The native `WaitForCompactOptions` can request purge waiting, but the pinned C API and safe Rust
wrapper do not expose that option. `rust-rocksdb` 0.24.0 does expose timed `DB::wait_for_compact`;
with its current default it covers queued/running flush, compaction and recovery, not purge. Even
native `wait_for_purge=true` checks scheduled purge work rather than every producer count, queue or
`SstFileManager` trash path. It may be an adversarial/debug aid only. The v2 health gate remains
continuous typed sampling with frozen producers/APIs and immediate `SstFileManager` deletion. A
future proposal to make the blocking command gate-bearing would need a new typed outcome with
invocation/return clocks, timeout/error/late-return semantics, common-tail charging and proof that
it does not trigger or mutate the condition it claims to observe.

`rocksdb.background-errors` counts qualifying flush/compaction failures. The pinned C API has an
`on_background_error` event-listener callback, but wrapper 0.24.0 has no safe listener surface, and
the C API does not expose all purge/deletion events needed here. The callback can run on a user-write
thread and receives mutable status capable of changing/suppressing the engine error. Any observer
must be read-only, nonblocking, non-unwinding and lifetime-safe. Only collector state can be
preallocated: the stock C shim allocates a status wrapper and invokes registered callbacks, so its
observer effect must be measured. If recovery remains enabled, map both recovery systems and their
outcomes; proving all recovery absent requires more than `max_bgerror_resume_count=0`.

The separate exact foreground stall source remains missing around `DBImpl::DelayWrite` and
`WriteBufferManagerStallWrites`. Sampled delayed/stopped properties and stall-condition callbacks do
not time all actual waits. A future mapping must also freeze low-priority, `no_slowdown`, `LockWAL`,
and WBM `allow_stall` reachability. The closure scope is now concrete, but its effort, completeness,
scheduler interference, callback cost, and shutdown/fault behavior remain unknown until an approved
adversarial construction proves them.

**RocksDB disposition:** `OBSERVED_DESIGN_INCOMPLETE`. Carry into source-closure specification only
after the final v2 contract; construction still needs separate approval. Do not build an adapter or
run qualification yet.

## Fjall paper mapping

### Mechanism and configuration boundary

Whole-arm N/A is false. The intended normal build starts workers that rotate/seal memtables, flush,
invoke strategy compaction, rotate and collect journals, perform MVCC/version-history maintenance,
and eventually delete stale SSTs. Recovery is synchronous before workers and stays in common
recovery/fault gates. KV separation must be explicitly disabled; otherwise blob reclamation also
belongs in this arm. Dynamic keyspace creation/deletion, ingestion, manual major compaction, and
temporary-database cleanup must be disabled/unreachable or separately mapped.

The mapping domain must freeze one DB/four keyspaces, exact `lsm-tree` resolution, features, worker
count greater than zero, the built-in Leveled strategy/options, memtable and journal triggers,
compression `None`, filter/cache settings, and KV separation off. Custom compaction strategies or
filters, `open_without_background_threads`, `worker_threads_unchecked(0)`, `Keyspace::clear`, and
unplanned control-plane maintenance must be unreachable. `Keyspace::clear` bypasses corresponding
global write-buffer release, so the current byte gauge is not exact if it is admitted.

`DatabaseBuilder::max_write_buffer_size` stores a deprecated value but no enforcement path reads it;
it is not a global memory control. `max_journaling_size` triggers rotation/flush attempts rather
than enforcing a hard disk cap. Adapter admission plus common cgroup/memory/disk gates therefore
remain unresolved resource-control obligations even if telemetry is added.

### Existing non-conforming diagnostics

No subset of the stock observations below satisfies the proposed arm. Some fields could be
stabilized into a future mapping, while others remain debug-only because of blind intervals or
wrapping semantics.

| API | Scope; kind/unit; quality | Lifecycle and collection cost | Proposed use | Fatal limitation |
|---|---|---|---|---|
| `Database::write_buffer_size` | DB; unsigned gauge/bytes; exact logical active+sealed allocation on audited, allowed paths | Atomic acquire load; O(1); reconstructed during open | Candidate for maximum and tail upper bound after stabilization | Hidden experimental API; excludes old-version/native memory; `Keyspace::clear` breaks the audited accounting; global cap is unenforced. |
| `Database::outstanding_flushes` | DB; gauge/jobs; exact queued flush tasks | Bounded-channel length; cheap but cost stability unproved | Diagnostic only | Task is dequeued before I/O, so zero does not exclude an active flush. |
| `Keyspace::sealed_memtable_count` | Fixed keyspace; gauge/items; exact latest-version sealed count | O(1) after version-history read lock | Per-keyspace maximum and tail bound | Hidden; misses pre-seal scheduler messages and other maintenance. |
| `Database::active_compactions` | DB; gauge/jobs; exact active attempts | Relaxed atomic; O(1); new instance starts at zero | Diagnostic only | Dequeue-to-increment race; no queued compaction state. |
| `Database::compactions_completed` | DB; wrapping counter/attempts; exact successful attempts | Relaxed `AtomicUsize::fetch_add`; new instance starts at zero | Debug progress only; not a v2 monotonic counter | Can wrap and a strategy attempt may do no work; completion does not prove pressure retired. |
| `Database::journal_count` | DB; gauge/files; exact sealed plus one active | Journal-manager read lock; O(1) after acquisition | Maximum and tail bound against approved baseline | Can contend; no byte pressure. `journal_disk_space()` also takes writer/file paths and is too intrusive for frequent sampling. |

`Keyspace::fragmented_blob_bytes` would be relevant only if KV separation were enabled. The hidden
L0 table count is not the stall source: foreground pressure uses private L0 *run* count. Optional
`lsm-tree` metrics cover read/cache/filter I/O rather than maintenance queues, failures or cleanup;
they remain debug observations.

### Why stock 3.1.8 is incomplete

- The private worker message queue is bounded at 1,000. Rotation requests and post-flush compaction
  requests use `try_send(...).ok()`, so notification loss is silent. The post-seal flush notification
  uses blocking `send(...).ok()` but still discards disconnect. Messages leave the queue before work,
  and no general queue, loss, in-flight, or per-keyspace pending state is public.
- One compaction call asks the configured strategy once. A source TODO says strategy feedback is
  absent, so queue and active counts can both be zero while eligible work remains.
- Propagated rotate/flush/compact/journal errors log, poison the DB, and terminate a worker, but the
  poison state, configured-versus-live worker count, and a stable error counter are not public.
  Panics and unexpected exits also need explicit liveness evidence. Version/manifest GC and deferred
  SST/blob unlink can warn and continue without poisoning, so log capture is not a failure veto.
- Version free-list, snapshot watermark/open population, scheduled deletion, delete retry, and
  cleanup-tail state are private or absent. A version free-list can reach zero while an obsolete
  file remains held by an iterator/snapshot `Arc`; last-drop unlink may happen outside a worker and
  an unlink failure is warning-only. Existing zeroes cannot prove quiescence.
- Foreground backpressure delays at L0-run thresholds and sleeps on hard L0/sealed-memtable
  pressure, but callers discard the returned boolean. There is no exact start/end/reason/duration
  population for the separate stall arm.

Telemetry alone cannot repair stranded work. A future source must replace lossy notifications with
lossless/coalesced per-keyspace pending state, treat channel disconnect as fatal, retain a
rotation-required state, and re-evaluate/requeue built-in Leveled compaction until a source-proven
no-work state. This is scheduler/lifecycle repair, not an observer convenience.

One generation-tagged, allocation-free snapshot is a possible observation surface. The following
fields are hypotheses requiring exact source semantics; they are not stock APIs or approved wire
signals. Comma-separated names below denote separate typed fields, not one aggregated signal:

| Proposed field | Scope; kind/unit; quality | Update/reset/atomicity | Proposed predicate and unresolved proof |
|---|---|---|---|
| `snapshot_epoch`, `snapshot_active_writers` | DB; saturating counter/transitions plus gauge/writers; exact observation-bracket metadata, not health signals | Every transition increments active writers before field updates, increments epoch after updates, then decrements active writers; sampler accepts only zero writers and unchanged epoch across its copy | Validates a sampled cut, not a health predicate. Multi-writer memory ordering, overflow and contention remain to be proved; saturation invalidates evidence. |
| `worker_pool_healthy` | DB; boolean; exact only with guarded start/exit/poison paths | True exactly when live workers equal frozen configured workers and the DB is not poisoned; reconstructed on process start | Required true in every sample. `workers_alive` may remain a diagnostic gauge. |
| `maintenance_jobs_active` | DB; gauge/jobs; exact full worker-message lifetime | Increment immediately after dequeue and decrement only after complete rotate/flush/compact return, including version/journal work and error exit | Measurement and tail upper bounds; prevents queue/pending false zero while any maintenance message executes. |
| `maintenance_error_total` | DB; saturating counter/errors; exact only after exhaustive path proof | Increment before every propagated worker error, panic/unexpected exit, channel disconnect, version/manifest GC failure and SST/blob/journal unlink failure | Absolute maximum zero and zero delta; any nonzero first/later sample fails. Saturation invalidates; path inventory remains unproved. |
| `scheduler_loss_total` | DB; saturating counter/events; exact impossible-event guard | Increment on rejected/disconnected notification even after lossless repair | Absolute maximum zero and zero delta; any event fails. It detects a violated invariant but does not replace progress repair. |
| `write_buffer_bytes` | DB; gauge/bytes; exact only for frozen allowed mutation APIs | Stabilized existing transition accounting; reconstructed from recovered state | Measurement and tail upper bounds; no-increase is additional only. `clear` must remain excluded. |
| `journal_files` | DB; gauge/files; exact active-plus-sealed set | Updated on journal create/delete in the coherent snapshot | Measurement/tail upper bound against an approved active-file baseline. The configured journal size remains only a trigger. |
| `flush_jobs_queued` | DB; gauge/jobs; exact | Updated on enqueue/dequeue under the observation protocol; general active gauge remains set through journal rotation, flush, post-flush scheduling and journal GC | Measurement and tail upper bounds; every handoff must avoid a false zero. |
| `rotation_pending.<ks>` | Fixed keyspace; boolean; exact required-work state | Set before/coincident with threshold crossing; clear only after the complete rotation/version/journal operation returns successfully or proof no longer required | Required false in the tail; source must prove no request can be stranded. |
| `sealed_memtables.<ks>` | Fixed keyspace; gauge/items; exact | Stabilized current count across successful flush registration | Measurement and tail upper bounds. |
| `l0_runs.<ks>` | Fixed keyspace; gauge/runs; exact current version | Updated with version install | Measurement and tail upper bounds; also diagnoses pressure but does not replace exact stalls. |
| `compaction_pending.<ks>` | Fixed keyspace; boolean; quality **unproved** until pinned-strategy/version semantics close | Coalesced pending set before notification; general active gauge covers the strategy call; clear only after re-evaluation proves no work | Required false tail state. Do not label the pressure projection exact before proof. |
| `version_free_list_items.<ks>` | Fixed keyspace; gauge/items; exact internal list | Updated with version retirement/release | Measurement and tail upper bounds; cannot replace physical-delete tracking. |
| `physical_deletions_pending.<ks>` | Fixed keyspace; gauge/files; exact from logical retirement through successful unlink/retry | Increment before retirement can escape; decrement only after successful unlink; retained across retry | Measurement and tail upper bounds; sticky failure/retry policy and crash reconstruction remain to be designed. |

All new counters must saturate rather than wrap; an unexpected restart or saturation invalidates the
attempt. The multi-writer observation bracket is accepted only when active writers are zero both
before and after copying and the epoch is unchanged; it is metadata, not a candidate-health field.
Sampling stays at fixed cadence off the event loop over four fixed keyspaces with no allocation,
scan or dynamic labels. Transition updates, memory ordering, coherent snapshot cost, retry
persistence and cached strategy-pressure work all need paired telemetry-on/off throughput,
p99/p99.9/maximum latency, CPU, memory and observer-cost evidence. A separate exact stall observer
must still report intervals and reasons.

**Fjall disposition:** `OBSERVED_DESIGN_UNSUPPORTED_IN_STOCK_SOURCE`. Retain as a Rust-native
alternative only if owners explicitly accept scheduler/lifecycle repair and long-term fork/upstream
ownership. Its cost relative to the RocksDB closure is unknown.

## redb paper mapping

### Whole-arm N/A premise

The exact default/no-feature Linux crate contains no async runtime, task framework, channel, or
redb-created runtime thread. Its only `std::thread::spawn` calls under `src/` are inside a
`#[cfg(test)]` cache test module. I1 immediate one-phase commit, I2 immediate two-phase commit, and
quick-repair commit alter synchronous work but not that execution model. This supports a *proposed*
complete maintenance-health N/A at crate scope; it is not complete-process proof.

The eventual claim must also freeze the release binary, Linux GNU target/toolchain/flags, no
features, default standard file backend, cache/config bytes, repair callback, adapter source/binary,
and process topology. A custom `StorageBackend`, feature, callback, adapter dispatch path, or target
is a different subject.

I1, I2 and quick-repair are prescreen alternatives, not composable evidence. Any admitted candidate
must choose one production durability/repair mode and run latency, C3, persistence, recovery, fault
and endurance under that same identity. I2 without quick repair still need not serialize allocator
state and can require full repair. Clean Drop performs its own quick-repair/maximum-shrink lifecycle
transaction regardless of the traffic-mode choice and remains separately measured.

### Work N/A does not waive

| Synchronous or deferred work | Required evidence outside native health |
|---|---|
| One database-wide writer blocks on a mutex/condition variable with no try, timeout, or cancellation API | Scheduled-to-dispatch, adapter queue, writer-acquisition, service, end-to-end, fairness/starvation, and C3 hot-writer/victim latency |
| Freed-page reclamation waits for MVCC readers and is paid by a later commit; reader release itself reclaims nothing | Pinned-reader lifetime, an explicit measured post-release trigger commit, its service tail, file growth, space amplification, resource tail, and endurance |
| Cache pressure can evict/write during mutation; final drain and `sync_data` occur inline | Entire mutation/service interval and commit phase, cgroup/device I/O, cache-loss and physical persistence truth table; `sync_data` is not assumed to prove power-loss durability |
| Growth and default shrink call resize on the caller; shrink follows the final flush without a same-path later sync | Latency, allocation/growth, crash correctness, and shrink durability fault tests |
| Open/repair and allocator reconstruction are synchronous | Recovery correctness, reopen RTO, corruption/torn-write and recovery resources |
| Explicit `Database::compact`, incomplete-transaction rollback, and clean close/trim are synchronous lifecycle work | Prohibit compact during measured traffic unless separately scheduled/timed; time abort, shutdown, close and reopen explicitly |

With `cache_metrics` disabled, zero-filled cache stats mean unsupported, not observed zero. Full
transaction statistics traverse trees while holding the sole writer and are prohibited during
measurement. Kernel writeback, filesystem and device/controller queues remain common external
evidence.

There is also an adapter-admission blocker independent of N/A: `Database` has no fallible close API.
Drop discards close errors, logging is disabled in the exact build, and the close path ignores an
initial storage-flush failure. The candidate needs an observable fail-closed lifecycle outcome or an
explicitly approved recovery/availability design; fault tests alone do not make production close
failure observable. Explicit abort should be used where its error is meaningful, while close/reopen/
fault evidence remains mandatory.

### Proof still required

Before formal N/A, complete the state-storage call graph for the whole candidate process, including
adapter queues/tasks, blocking pools, runtime task creation, repair callback, last-`Arc` drop and
shutdown. N/A requires that no enabled state-maintenance task/thread is ever created, queued,
executing or pending—not merely that none survives operation return. An awaited blocking offload is
foreground only while the exact operation remains open and its entire queue/acquisition/service cost
is charged. An unawaited adapter/runtime reaper, compact, reclaim, close or drop task defeats N/A.

Reuse the approval-bound prescreen executions rather than duplicate their I1/I2/quick-repair,
sole-writer, pinned-reader, cache, growth/shrink, close and recovery cases. Add only missing N/A
activation witnesses. Attach before child `exec` and trace the whole process tree: caller TIDs,
redb operation/correlation boundaries, `clone`/`clone3`, thread start/exit and state I/O. Because
OS-thread tracing cannot see short-lived or CPU-only coroutine work on an existing runtime worker,
the exact adapter/runtime also needs source-derived task enqueue/start/end proof or a bounded
loss-detecting task registry. Every claimed branch needs an activation witness; invoking a scenario
does not prove the source path ran.

Failure activation must use an external approval-bound syscall/device actuator against the exact
default-backend binary. A custom `StorageBackend` changes identity and is corroboration only. Cache
pressure must reach the exact configured cache's relevant threshold; actual resize and repair paths
need witnesses. Unclean recovery uses externally killed children with no Drop and cold-cache
controls, with I1/I2 full-repair evidence separate from quick-repair allocator-load evidence.
`Database::compact` is prohibited in the measured candidate; source proves it unreachable. An
isolated compact mechanism test, if retained, is non-gating.

The required finding is no candidate-process async state maintenance, with zero trace/registry loss
and complete source-derived reachability. Unexpected async work rejects N/A; an unforced path,
missing activation, drift, exhausted bound or unattributable activity defers it. A short quiet run
cannot prove absence. Docker/WSL may validate probe mechanics but cannot supply native XFS/device
evidence.

The separate foreground-stall N/A needs its own source/configuration/probe proof. It is plausible
only if that arm is frozen to explicit asynchronous-debt throttle/write-stop states. Synchronous
write-buffer overflow, inline eviction/flush, writer and lock waits stay in full service timing and
cannot inherit maintenance-health N/A.

**redb disposition:** `N_A_DESIGN_SOURCE_PLAUSIBLE_PROBE_MISSING`. Complete the separately governed
native prescreen first. An approved N/A would remove only candidate-native health samples; it would
not admit redb or compensate for its global-writer, durability, recovery, or close behavior.

## Historical Cycle 19 carry decision matrix

| Decision factor | RocksDB 10.4.2 | Fjall 3.1.8 | redb 4.1.0 |
|---|---|---|---|
| C1 primitive fit from static audit | Broadest: atomic cross-CF batch, snapshots, bounded iteration, multi-get, WAL controls, checkpoint and ingest | Adequate core atomic batch/snapshot/range/journal persistence; fewer native migration tools | Adequate atomic cross-table write and MVCC/range core; fewer bulk/checkpoint primitives |
| Native-health shape | Six useful but incomplete stock flush/compaction signals; scheduled-compaction, purge, error/recovery and stall closure is unimplemented | Six stock diagnostics; scheduler/lifecycle repair plus worker, error, cleanup, strategy and stall closure is unimplemented | Plausible whole-arm N/A only after complete-process source and task/thread probe proof |
| Required health closure | Scheduled low/bottom counts, typed purge snapshot/failure sources, safe listener/integer bindings and complete recovery policy | Lossless/coalesced scheduler, stable coherent snapshot, worker liveness/error and physical-delete tracking | No engine health patch if N/A proof succeeds; fail-closed lifecycle outcome still blocks adapter admission |
| Exact foreground stalls | Slow-path observer still required, including WBM; effort unknown | Exact interval/reason observer required; effort unknown | Separate explicit async-debt stall N/A is source-plausible; writer/lock/inline-I/O latency remains common |
| Hot-path/observer risk | Property polling stays off event loop; callbacks/counters touch slow/background paths but C++/FFI safety and DB-mutex cost require A/B proof | Transition-only counters are plausible, but scheduler repair and cached strategy-pressure changes may alter worker/hot-path behavior | No health observer; fundamental single-writer and synchronous commit/reclaim/resize tails are the risk |
| Native ownership shape | C++ engine plus C and safe-Rust binding ownership; operational controls are broad; closure cost unknown | Rust engine scheduler/lifecycle plus telemetry fork or upstream ownership; closure cost unknown | No health fork if N/A closes; adapter, task-registry and lifecycle observability ownership remains |
| Most important unmeasured veto | C3 tails/shared controls, native patch interference, persistence/fault behavior | Patch completeness/interference, resource controls, C3 and persistence | Global-writer C3 tail/fairness, sync/shrink durability, recovery and close errors |
| Next bounded decision | **Source-closure specification after v2 approval** | **Explicit scheduler/lifecycle fork-ownership decision** | **Native prescreen and N/A observation as the all-Rust hedge** |
| Selection status | Blocked, not selected | Blocked | Deferred, not selected |

The matrix supports carrying all three into different *decision stages*: RocksDB into an enumerated
source-closure specification if v2 is approved, redb into its separately bounded native prescreen,
and Fjall into an owner decision about accepting scheduler/lifecycle fork obligations. None has an
earned implementation-cost rank. No candidate should consume adapter work until its prior gate
closes. Only comparable measured correctness, latency, resources, persistence, fault and endurance
evidence may eventually choose a backend.

## Historical Cycle 19 contract feedback and next gates

The sampled gauge/counter/boolean vocabulary remains sufficient for the mappings as corrected. The
RocksDB blocking wait stays non-gating; it is not encoded as a boolean. If owners later require an
active terminal command, it needs a versioned outcome union—completed, timeout, engine error or
collector invalid—plus cardinality, clocks, cancellation/late-return, non-mutation, common-tail and
overhead rules. That review is still smaller than a generic expression language. No candidate
requires arithmetic across unlike signals, a weighted score or candidate-defined aggregation.

Before Cycle 38 accepted the validation contract, owners still had to decide and freeze:

1. the exact consolidated wording by which the approved v2 direction replaces v1's exact-debt arm;
2. exact signal and mechanism schemas, reset/restart rules and complete nominal/cut populations;
3. candidate-specific numerical health limits plus common cadence, skew, tail hold/deadline, and
   telemetry-overhead limits before any candidate result;
4. RocksDB background-error/recovery policy and the exact low-latency purge configuration;
5. whether Fjall scheduler/lifecycle and telemetry ownership is an accepted long-term obligation;
6. a redb-specific profile/control translation plus one non-cherry-picked production durability
   mode; and
7. the independent reviewers and source-closure stop limits.

Even a completed contract authorizes only validator work if its approval says so. Candidate source
construction, adapter construction, candidate execution, backend selection, runtime integration,
connector delivery certification, exactly-once certification, and independent product soak each
retain their separate authority and evidence gates.

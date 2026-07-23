# State backend qualification runner and evidence contract v1

- **Status:** provisional C2 engineering contract; no executable runner plan exists
- **Scope:** standalone `tools/state-backend-qual` qualification tooling only
- **Approval required before candidate execution:** workload owner and operations owner
- **Related decisions:** [ADR-008](ADR-008-managed-vnode-keyed-state.md),
  [state backend qualification model v1](state-backend-qualification-model-v1.md), and the
  [long-stream workload/identity v2](state-backend-workload-v2.md)
- **Pinned-source findings:** [Fjall/RocksDB static audit](../reports/state-backend-static-audit-2026-07-23.md)

## Decision and safety boundary

LaminarDB will define one candidate-neutral runner contract before adding a Fjall or RocksDB
performance adapter. It will produce strict plan, attempt, raw-sample, and evidence-manifest
artifacts. Validation code may land while the profile is unapproved, but it will expose no backend
execution command and accept no incomplete production-shaped runner plan.

The runner is an isolated backend bake-off, not the future runtime state API. It cannot establish
checkpoint correctness, source/sink delivery, exactly-once, rebalance safety, or production
readiness. Its evidence manifest is an input to a later reviewed selection report and never carries
its own qualification verdict. `[LDB-4007]` and `[LDB-0013]` remain unchanged.

Before approval, permitted candidate-specific work is limited to exact builds and semantic adapter
conformance against the C1 oracle. Performance, resource, physical-fault, endurance, and selection
execution is prohibited. Calling an unapproved run “diagnostic” does not make it permissible.

## Stable identities and artifacts

V1 reserves these printable identities:

| Item | Identity |
|---|---|
| runner contract | `state-backend-runner-contract/v1` |
| runner plan schema | `state-backend-runner-plan/v1` |
| evidence manifest schema | `state-backend-evidence-manifest/v1` |
| latency samples | `state-backend-latency-samples/v1` |
| resource samples | `state-backend-resource-samples/v1` |
| resource cuts | `state-backend-resource-cuts/v1` |
| resource formulas | `state-backend-resource-formulas/v1` |
| physical layout | `state-backend-physical-layout/v1` |
| physical faults | `state-backend-physical-faults/v1` |
| attempt classification | `state-backend-attempt-classification/v1` |
| qualification approval | `state-backend-qualification-approval/v1` |
| campaign completion root | `state-backend-campaign-completion/v1` |

The canonical latency stream begins with `LDB-SBQ-LATENCY-SAMPLES-V1\0`; the zero byte is part of
the wire domain and not the printable identity. Any incompatible encoding, formula, layout,
classification, or schedule change requires a new identity.

The plan binds the exact raw profile SHA-256, physical layout, and every policy identity above. C1
adapter-conformance entries additionally bind their model-input SHA-256 plus v1 generator/model
identities; those values are provenance and never a C2 generation input. Every C2 entry instead
binds the exact workload-v2 case wrapper/body IDs and, for each selected seed, immutable
expectations, preflight-provenance, and required independent-audit descriptors. It does not bind its
containing source revision.
A separate, detached approval record avoids that identity cycle by binding exact profile bytes,
plan bytes, runner source archive, lockfile, both candidate binaries/configurations, toolchain,
target, build flags, and environment image/package manifest. The approval record is excluded from
the source archive and candidate binaries that it binds. A later informational repository copy
cannot change the already approved source identity or become part of that archive. The record
carries workload and operations owner identities, UTC approval time, and signed/protected-review
provenance. The future execution command must verify it and structurally reject today's ineligible
profile; convention or a CLI flag is not sufficient. Line-ending or formatting changes therefore
change identity.

A `runner-plan/v1` instance always means a complete, executable, nonempty plan. The schema has no
`draft`, `executable=false`, placeholder, or empty-matrix form. Until the blockers at the end of
this ADR are resolved, repository tests use clearly synthetic fixtures and no real candidate plan.

## Campaign, pair, case, and attempt hierarchy

A campaign contains an exact ordered schedule. Each scheduled slot has one contiguous index plus
case, repetition, and candidate. Its seed is exactly `profile.fixed_seeds[repetition]`; pair and
order are derived from the frozen schedule rather than repeated as free-form identities. Arbitrary
retry IDs cannot replace a slot. The plan names exactly two candidate IDs, and one candidate is
compiled into each binary.

The case matrix is an explicit list, never a Cartesian product of profile vectors. Every case has:

- a stable slug, scenario, independent residency (`resident` or `spill`) and contention
  (`normal` or `hot_vnode`) dimensions, active vnode count, fixed- or deterministically
  variable-width policy, batch rows, and join fanout where applicable;
- exact setup/prefill, warmup, measured-stream, churn/retention, and persistence policy identities;
- low/target/high logical live-state bytes and cardinality/timer-density state-control bands, plus
  descriptors for separate per-seed expectations containing setup, post-warmup, and final state
  digests and exact derived counters; the plan does not copy those derived values into mutable case
  fields;
- exact warmup and measured request counts plus rational offered requests-per-second for each phase;
- the v1-required single foreground worker, maximum in-flight requests, queue byte/entry ceilings,
  drain deadline, and terminal timeout;
- explicit end-to-end, service, queue, throughput, and resource gate mappings; and
- expected request/row/mutation counters and applicable fault schedule IDs.

Each candidate starts from a fresh directory populated from the same candidate-neutral logical
setup stream. Setup is verified through the model digest before warmup. Warmup is excluded from
measurement but remains in live state and the oracle. Manual compaction, candidate-specific
quiescing, cache dropping, or reuse of the other candidate's mutated state is forbidden unless an
identical named policy explicitly requires it for both candidates.

The long-stream generator must deterministically update and expire aggregate, timer, and join
records so actual live bytes, cardinality, and timer density stay inside each case's approved band.
The runner checks the bands at setup, throughout warmup/measurement, and at the final digest. Leaving
the declared residency band is FAIL. At the exact warmup horizon, approved L0/run, memtable,
compaction-debt, and debt-slope conditions must hold; the runner neither waits longer nor forces
quiescence to improve a candidate's result. Failure to stabilize is FAIL, while unavailable evidence
needed to decide it is a conformance failure.

Five requests or five million requests within one attempt are not independent repetitions. Gate
decisions use independently reset paired repetitions. Every case/repetition/seed runs both
candidates in the frozen order. Absolute gates apply independently to every valid attempt; pooled
histograms and paired relative deltas are diagnostic and cannot hide a failing repetition.
The profile's per-repetition minimum applies to every candidate/case/repetition, and its total
minimum applies independently to each candidate and case across all required repetitions. The plan
freezes a balanced, seeded AB/BA order, predecessor-reset and cooldown schedule before results.

Before a plan is accepted, a bounded static preflight checks every planned ordinal after actual
deduplication. Canonical request bytes, point/range row and byte capacity, mutation charge, framing,
join fanout, target batch, and hard batch limits must all fit. A scalar multiplication is useful for
finding an impossible all-distinct shape but is not a substitute for that exact check.

## Open-loop schedule and conservation

The runner uses a real open-loop schedule. A case stores offered rate as positive reduced integers
`rate_numerator / rate_denominator` requests per second. Zero-based request `i` is due at:

```text
floor(i * 1_000_000_000 * rate_denominator / rate_numerator) nanoseconds
```

All products use checked `u128` and every final offset must fit `u64`. The exact request count is
authoritative. The exclusive scheduling horizon is:

```text
ceil(request_count * 1_000_000_000 * rate_denominator / rate_numerator)
```

Warmup and measurement have separate exact counts/rates. Their horizons must meet the profile's
time minima and their counts must meet its request minima. The measured elapsed interval is phase
start through `max(schedule_horizon, last_terminal_offset)`. Drain deadline is an exact duration
after the horizon. Throughput, CPU, and stall formulas use that conservative elapsed interval.

Scheduling does not wait for the preceding completion. A lightweight ordinal may enter only the
pre-reserved bounded queue, whose byte reservation is entries times the worst exact generated
request charge. Request payload generation occurs after dispatch into pre-sized, pre-touched worker
scratch. Queue exhaustion, timeout, process exit, or work remaining at the drain deadline is a
candidate failure, not an excuse to hide an arrival. A terminal candidate failure aborts the slot;
future planned ordinals are counted as `not_attempted_after_abort`, not fabricated as completions.
The conservation identity is:

```text
planned = terminal_records + not_attempted_after_abort
```

Every ordinal attempted before abort has exactly one committed terminal sample in a structurally
valid latency artifact, and no unknown or duplicate ordinal exists. Failure of the outer supervisor
to produce that artifact makes the attempt INVALID in the campaign manifest; it can never relax the
identity into a partial PASS. A committed candidate-return stage means the complete adapter
observation is already in supervisor-visible memory. Unless the outcome is a runner or external
interruption, the surviving supervisor performs exactly one oracle comparison, including after an
adapter-child crash. A PASS requires all planned ordinals to return `ok`; a proved candidate
crash/timeout remains FAIL even when it prevents later candidate-side evidence.

`CLOCK_MONOTONIC_RAW` is the timestamp authority, not the absolute-sleep clock. At phase start the
runner records paired RAW/MONOTONIC readings. It uses absolute `CLOCK_MONOTONIC` sleeps to a frozen
pre-spin margin, rechecks RAW after wake, and performs a bounded final spin on a controller core
that is not a candidate service core. CPU affinity, spin margin, mapping refresh, clock resolution,
and maximum scheduler-calibration error are plan fields and preflight gates.

## Latency timestamps, samples, and gates

Qualification Linux uses `CLOCK_MONOTONIC_RAW`. Each attempted request records these stages from
measured phase start:

1. `scheduled`: its deterministic due time, always present;
2. `enqueued`: the bounded queue accepted the ordinal;
3. `dispatch_start`: the state worker dequeued it;
4. `service_start`: request preparation finished and the adapter begins candidate work;
5. `candidate_return`: the adapter returned and its complete result/status was copied to and
   atomically published in supervisor-visible, runner-owned memory; and
6. `terminal`: the supervisor classified the request or stopped the child.

Present stages must follow that order. A stage mask distinguishes absent timestamps; absent fields
are canonically zero and never synthesized. Derived populations are:

| Population | Formula |
|---|---|
| scheduler lateness | `enqueued - scheduled` |
| state queue wait | `dispatch_start - enqueued` |
| runner preparation | `service_start - dispatch_start` |
| candidate service | `candidate_return - service_start` |
| offered end-to-end | `candidate_return - scheduled` |

The existing profile's `*_request_us` gates map to offered end-to-end latency, and
`state_queue_wait_us` maps to state queue wait. Candidate service and runner preparation are always
retained separately.
The profile must add explicit service gates before approval because Phase 0 requires both service
and end-to-end limits; a runner must not infer missing thresholds from results.

V1 finite performance trials retain exact raw samples rather than an implementation-specific
histogram. The stream has a 35-byte header—the 27-byte domain above plus
`record_count_u64_be`—followed by 58-byte records and no trailing bytes:

```text
ordinal_u64_be
scheduled_offset_ns_u64_be
enqueued_offset_ns_u64_be
dispatch_start_offset_ns_u64_be
service_start_offset_ns_u64_be
candidate_return_offset_ns_u64_be
terminal_offset_ns_u64_be
stage_mask_u8
outcome_u8
```

Stage-mask bits `0x01`, `0x02`, `0x04`, and `0x08` mean enqueued, dispatched, service-started, and
candidate-returned. Only prefix masks `0x00`, `0x01`, `0x03`, `0x07`, and `0x0f` are valid; all
other values are rejected. Outcome tags are exactly:

| Tag | Outcome | Required stages |
|---:|---|---|
| `0x00` | `ok` | all |
| `0x01` | `candidate_error` | all |
| `0x02` | `oracle_mismatch` | all |
| `0x03` | `queue_overflow` | none |
| `0x04` | `queue_timeout` | exactly enqueued |
| `0x05` | `service_timeout` | exactly enqueued, dispatched, service-started |
| `0x06` | `adapter_child_crash` | any valid prefix except returned |
| `0x07` | `preparation_timeout` | exactly enqueued and dispatched |
| `0x08` | `runner_error` | any valid prefix |
| `0x09` | `external_interruption` | any valid prefix |
| `0x0a` | `observation_overflow` | exactly enqueued, dispatched, service-started |

The exact encoded length is `35 + 58 * record_count`, checked before allocation. Ordinals are
contiguous from zero. Returned calls populate service and end-to-end order statistics; accepted and
dispatched calls populate their corresponding queue statistics. Candidate timeouts, crashes, and an
oversized returned observation are retained as right-censored lower bounds unless return was
already published, and independently fail the attempt; they are not invented candidate returns.
Preparation timeout and runner error are runner INVALID outcomes; external interruption is INVALID.
No latency gate is pass-eligible unless every planned request returned and matched the oracle.

Terminal precedence is deterministic. Once a complete return is published, the oracle worker
commits `ok`, `candidate_error`, or `oracle_mismatch`; a later child exit is a separate
attempt-level crash marker and still makes the attempt FAIL without replacing that request record.
Only a runner error or external interruption that prevents the required comparison may terminate a
returned (`0x0f`) record. Before return, the supervisor's first-terminal-condition compare-and-swap
selects exactly one queue, preparation, service, observation-overflow, runner, external, or
unrequested-child-exit outcome; a supervisor-requested kill is the selected condition's actuator,
not a second child-crash outcome. An unrequested child exit is `adapter_child_crash`. Terminal
records are immutable after commit.

Each attempt runs the adapter in a killable child process under an external supervisor. Fjall and
RocksDB calls are not assumed cancellable: service timeout stops scheduling, the supervisor kills
the child, records the terminal marker, and starts no later operation against that store. The
supervisor owns a preallocated shared-memory sample region and a bounded result ring. The plan
freezes result-slot count, maximum encoded observation bytes per slot, total result-region bytes,
one sequential oracle worker's control-core affinity, and maximum oracle lag in requests and ns.
Preflight checks the exact aligned header/slot reservation with checked arithmetic against the byte
ceiling and requires enough slots for the approved in-flight/lag bounds; there is no per-request
result allocation.

A result slot is reserved and bound to its ordinal before `service_start`. Its normal path is
`free -> reserved -> child_writing -> published -> oracle_checked -> terminal_committed -> free`.
The child publishes the complete result/status and candidate-return timestamp with release ordering;
the oracle worker acquires it, compares the C1 observation in ordinal order, and acknowledges it
only after the supervisor commits the terminal outcome. A slot cannot be reused before that
acknowledgement. Missing a free slot or the oracle-lag bound is `runner_error` and makes the attempt
INVALID rather than waiting in the candidate service interval or masquerading as candidate queue
pressure.

A legitimate observation is proven to fit during ordinal preflight. If an adapter response crosses
the slot bound, the child never writes past it and follows
`child_writing -> overflow_reported -> terminal_committed -> quarantined`. The fixed-size overflow
report contains ordinal, configured capacity, and the minimum required encoded bytes at the first
violating item. It has mask `0x07`, no candidate-return timestamp or oracle comparison, wins the
supervisor terminal CAS as `observation_overflow`, aborts the attempt as candidate FAIL, and remains
quarantined until teardown. Timeout/crash slots that may still have a child writer are likewise
quarantined after the child is killed. All regions are fully initialized and page-touched on the
assigned NUMA node before warmup, and sample records use a separate commit bitmap. The surviving
supervisor serializes them to the evidence device after child termination. An adapter child may
therefore crash after publishing a return but before terminal classification without losing that
sample. This preserves process-crash evidence without measured-path file I/O. Host/cache-loss fault
runs use a separate crash-safe marker journal and fault schema; they are not latency-gating trials.

Only the sample-recording path is allocation-, lock-, logging-, and merge-free. Request/output
scratch is pre-sized and pre-touched, and page-fault counters are retained. Null-adapter calibration
measures clock/sample/IPC overhead. Each candidate also has frozen gate-bearing telemetry settings
and paired observer-control trials with telemetry enabled/disabled; the measured delta is reported,
never subtracted. Detailed RocksDB `PerfContext` remains outside gate-bearing runs.

Open-loop due-to-return samples expose coordinated omission directly. V1 applies no synthetic
HdrHistogram correction; applying both would double-count omitted wait—an inference from the
open-loop timestamp model. Values are integer nanoseconds.
Nearest-rank quantile `p/1000` selects sorted rank `ceil(p * N / 1000)`. The runner derives
p50/p90/p95/p99/p99.9 and exact maximum, compares raw nanoseconds to `gate_us * 1000` with checked
arithmetic, and retains every terminal sample. P90 is diagnostic; all profile-named quantiles and
maximum are gates. There is no clipping, outlier removal, timeout deletion, or zero-sample pass.

The plan caps the raw region before allocation. V1 raw files are only the finite performance-trial
format; 24/72-hour request-latency/count evidence needs a separately reviewed bounded endurance
encoding and may not silently reuse or truncate this stream. The bracketed resource stream and
slope formulas may be reused only if DKS-Q2-008 explicitly freezes their endurance count/byte caps.

## Candidate service interval

The service timer begins immediately before the adapter primitive and ends only after returned keys
and values are copied into runner-owned buffers. Request generation, queueing, oracle execution,
digesting, evidence serialization, and resource polling are outside it. Their cost remains in
offered end-to-end latency where applicable and in runner-overhead evidence.

V1 requires exactly one foreground service worker, modelling one LaminarDB worker's serialized
atomic state path and giving the sequential C1 oracle one unambiguous ordinal order. A plan with any
other foreground count is structurally rejected. Candidate background flush/compaction workers
remain enabled and capped by the profile. Cross-vnode parallel scaling belongs to later runtime
integration evidence, not this v1 backend-service comparison. V1 evidence alone is expressly
insufficient to select a backend: the C3 concurrent shared-database prerequisite under DKS-Q2-009
must also pass. Instrumentation that materially changes candidate behavior is prohibited in
gate-bearing runs; expensive RocksDB `PerfContext` or equivalent tracing belongs only in separately
approved, labelled diagnostic reruns after qualification execution is authorized, and cannot
replace the original attempt.

## Physical layout and adapter semantics

Both candidates use one database and four physical keyspaces. Logical tables map as follows:

| Physical keyspace | Logical tables |
|---|---|
| `state` | aggregate `0x01`, window `0x02`, output bookkeeping `0x06` |
| `timer` | timer index `0x03` |
| `join_left` | join left `0x04` |
| `join_right` | join right `0x05` |

Atomic replacement restore uses generation indirection rather than one unbounded delete/insert
batch. The `state` keyspace stores a durable lifecycle record at `vnode_u32_be || 0x00`. It contains
serviceability, an optional active generation, the never-decreasing next-generation counter, and an
optional reserved staging generation. Data in every keyspace is:

```text
vnode_u32_be || 0x01 || generation_u64_be || table_tag_u8 || opaque_key
```

This makes a vnode/generation contiguous while retaining logical table identity. Every normal
operation enters the same per-vnode lifecycle guard, verifies `serving`, captures the active
generation, and completes before leaving the guard. One batch must atomically mutate every
participating keyspace. Restore and cleanup transition that guard exclusively; cached generation
pointers cannot bypass it.

Restore first establishes the external ownership fence. Under the exclusive lifecycle guard it
marks the vnode unservable, reserves `next_generation`, increments the counter durably, and records
the reservation before staging any data. It then writes and verifies only that reserved generation.
Activation reacquires the exclusive guard and atomically changes the active pointer, clears the
reservation, and marks the vnode serving. Consequently normal writes cannot land in an old
generation during staging or activation, and a crash exposes the complete old serving cut or an
explicitly unservable restore-in-progress state, never a partly activated generation.

Recovery never reuses a reserved generation: it reclaims the recorded orphan and reserves a higher
one, or fails before service. Old, abandoned, and staging generations count against the approved
frozen-generation cap; cleanup is bounded, resumable, and must bring the count below the cap before
another reservation. Reclamation scans every physical keyspace and verifies the target generation
is empty. Generation exhaustion or counter wrap is a terminal error.

Cleanup marks the vnode unservable and clears its active pointer under the lifecycle/ownership
fence, then removes all generations from all four keyspaces. Its durable metadata tombstone and
next-generation counter remain, so a cleanup crash is resumed and later acquisition cannot reuse a
prefix containing stale keys. A missing lifecycle record is unservable and is accepted only for a
provably empty, newly created database; finding vnode data without one is corruption. First
initialization creates the tombstone and reserves generation zero through the same protocol.

Snapshot/export reads the lifecycle pointer and data at one cross-keyspace sequence cut. A serving
snapshot may retain its generation while reclamation treats that generation as pinned; lifetime is
bounded and included in the frozen-generation cap.

Adapter reads return model logical order, even if that requires merging physical iterators. Cached
generation pointers are invalidated at every lifecycle or ownership change; a stale pointer may not
serve after the fence.

The private spike boundary covers batched point reads, bounded ranges, atomic writes/deletes,
consistent snapshot/export, sorted restore, vnode cleanup, explicit persistence, and stable
resource/pressure observations. It uses C1 logical request/observation types but is not a public
plug-in ABI or the production trait.

Returned Fjall slices must be copied/released before the service interval ends, and snapshot or
iterator lifetime is bounded. RocksDB native memory is included in candidate-leaf memory and child
process PSS/RSS observations.
An unavailable required counter is `unsupported`, never zero; a candidate that cannot expose a
stable required pressure signal fails conformance or must upstream one before selection.

## Persistence and checkpoint boundary

WAL/journal is enabled for both candidates. The logical truth table is:

| Acknowledged boundary | Visible/atomic | Survives process death | Survives proved host-cache loss |
|---|---|---|---|
| `buffered_batch` | complete batch | required | not required |
| `persist_data` | all prior batches | required | recovery-required journal/WAL contents and proved directory entries |
| `persist_all` | all prior batches | required | all recovery-required engine data and metadata covered by an audited primitive |

The [pinned-source audit](../reports/state-backend-static-audit-2026-07-23.md) makes the proposed
Fjall calls explicit: `batch.durability(Some(PersistMode::Buffer)).commit()`,
`Database::persist(PersistMode::SyncData)`, and `Database::persist(PersistMode::SyncAll)`. Their
complete 3.1.8 file/directory ordering and target cache-loss behavior remain unproved. The proposed
RocksDB 10.4.2 mapping keeps WAL enabled, `disable_wal=false`, and `manual_wal_flush=false`:
buffered batches use `WriteOptions.sync=false`, while `persist_data` calls the wrapper's
`DB::flush_wal(true)`. The wrapper exposes no separate `sync_wal`; the engine call reached by
`flush_wal(true)` performs `FlushWAL(true)`/`SyncWAL`. `DBOptions.use_fsync` is an open-time
configuration, not a per-fence switch; each setting is a separate candidate configuration and plan
identity. WAL sync does not establish `persist_all`; that boundary is unsupported unless
DKS-Q2-007 supplies and verifies an audited engine/file/directory sequence. No proposed mapping is
normative or execution-eligible yet.

Host-cache-loss recovery is evaluated over the adapter's serialized sequence of completely
submitted atomic batches. It must yield an atomic prefix satisfying:

```text
last acknowledged durable fence <= recovered prefix <= last completely submitted batch
```

Unfenced batches may therefore be wholly present or absent and may never be torn. Process-only
death retains the stronger `buffered_batch` guarantee in the table. Loss of acknowledgement after
a completed fence is ambiguous to the caller, but recovery must still satisfy the completed fence;
an error before or during an unacknowledged fence does not imply the old cut. Buffered service
latency includes its batch call; persistence-case service latency includes the named fence call and
uses an exact group/cadence from the plan.

These fences cover the engine journal/WAL, not portable snapshot publication, new-directory
durability, or a distributed checkpoint decision. A process kill tests process recovery. A plain
VM reset is insufficient for a power-loss claim because host/hypervisor caches may survive or
flush; that claim requires real power cycling or a documented cache-loss/block-device harness that
records device write cache, power-loss protection, filesystem barriers, and hypervisor cache mode.
`SIGKILL` alone is not power-loss evidence.

Portable logical snapshot/export is the common selection gate. A RocksDB native checkpoint or any
candidate-native directory copy is an optional diagnostic and cannot substitute for portable
artifact conformance. Setup, snapshot, export, restore, and cleanup intervals are reported
separately from foreground service latency. Snapshot/export overlap must also report foreground
tail inflation, retained versions, memory, disk, and time-to-reclaim.

## Resource observations and formulas

The adapter child and all backend threads execute in one dedicated candidate leaf cgroup v2. A new,
empty leaf is created for every attempt, its identity is recorded, and zero tasks are verified
before assigning the child; on the pinned Linux 6.1 environment this creation boundary, not an
unsupported write to read-only `memory.peak`, defines the peak reset. The external supervisor,
generator, observer, and evidence spool use a separate control cgroup and device. Candidate CPU is
exactly `cpu.stat usage_usec`; memory current/peak comes from that leaf; target-device physical
writes are its `io.stat` `wbytes` for the frozen major:minor. NVMe/SMART NAND writes are diagnostics,
not the common formula. PSS/RSS and FDs cover the complete adapter process tree.

On the frozen XFS `prjquota` profile, gate-bearing allocated-store observations use the current
accounted blocks of a unique per-attempt project ID assigned to the resolved candidate database
root with inheritance verified before open. They include WAL/journal, manifests, SSTs, temporary,
obsolete, and directory inodes. Candidate options must place every WAL, log, temporary, spill, and
data path beneath that root; an external path is rejected before open. The exact XFS quota syscall,
block unit, delayed-allocation
interpretation, and wrap/error behavior are frozen under DKS-Q2-006. A recursive `st_blocks * 512`
inventory before candidate open and after the resource tail independently audits project assignment;
hard-linked `(device,inode)` pairs count once. Symlinks, reparse points, mount crossings, files
escaping the root, an unexpected project ID, or disagreement outside the approved accounting
tolerance invalidates the attempt. Logical mutation bytes reuse C1's exact charge: opaque key plus
value for put and opaque key for delete.

The runner records nominal one-second monotonic resource observations plus the kernel's
whole-attempt peak. These are bracketed samples, not fictitious atomic cuts. After recording
`observation_begin`, it reads the published C1 logical-live-byte counter and project quota, reads the
other counters, reads quota and logical-live bytes again, then records `observation_end`. The
resource stream starts with the 28-byte `LDB-SBQ-RESOURCE-SAMPLES-V1\0` domain, then
`record_count_u64_be`, then 176-byte records containing these 22 `u64_be` fields in order:

```text
sample_index, observation_begin_offset_ns, observation_end_offset_ns,
cpu_usage_us, memory_current_bytes, memory_peak_bytes, io_read_bytes,
io_write_bytes, pss_bytes, rss_bytes, open_fds,
allocated_store_before_bytes, allocated_store_after_bytes,
compaction_debt_bytes, write_stall_total_ns, live_snapshots,
live_iterators, frozen_generations, file_dirty_bytes, file_writeback_bytes,
logical_live_before_bytes, logical_live_after_bytes
```

The exact length is `36 + 176 * record_count`, with sample indices contiguous from zero and no
trailing bytes. Begin must not exceed end; read skew is exactly `end - begin` and must meet the
plan's resource-observation gate. Unsupported required observations fail adapter conformance before
a gate-bearing run; there is no numeric sentinel. The manifest freezes each source, read order,
reset action, wrap behavior, sampling error, and availability.

One-second observations cannot stand in for formula boundaries. A second stream starts with the
25-byte `LDB-SBQ-RESOURCE-CUTS-V1\0` domain, then `record_count_u64_be`, then 128-byte records. Each
record is `cut_tag_u8`, seven zero reserved bytes, followed by these fifteen `u64_be` fields:

```text
observation_begin_offset_ns, observation_end_offset_ns,
cpu_usage_us, io_read_bytes, io_write_bytes,
memory_current_bytes, memory_peak_bytes,
allocated_store_before_bytes, allocated_store_after_bytes,
logical_live_before_bytes, logical_live_after_bytes,
write_stall_total_ns, compaction_debt_bytes,
file_dirty_bytes, file_writeback_bytes
```

The exact length is `33 + 128 * record_count`, with no trailing bytes. Tags are
`0x00 pre_measurement`, `0x01 write_stop`, `0x02 last_terminal`,
`0x03 measured_elapsed_end`, `0x04 resource_tail_stable_end`, and
`0x05 resource_tail_deadline`. A valid
performance attempt has exactly one of each of tags `0x00` through `0x03`, plus exactly one of
`0x04`/`0x05`, in increasing tag order; its record count is five. Begin must not exceed end and cut
skew is again `end - begin`. The logical event is derived exactly as:

```text
write_stop_offset_ns =
    max(schedule_horizon, last terminal_offset of any attempted mutating ordinal)
```

The `write_stop` counter observation begins at or after that event, once the mutation path is
drained; its later bracket is not substituted for the logical offset. The debt and writeback cut
occurs after `measured_elapsed_end` and either completion of the uninterrupted tail hold or the
deadline. Both the deadline and hold schedule are anchored to `write_stop_offset_ns`. The plan's
clock/resource-error gate covers every encoded skew.

Normative v1 formulas use checked integer arithmetic. Lower-bound gates round down; upper-bound
gates round up:

- throughput is the floor of oracle-valid completed logical rows times `1e9` divided by measured
  elapsed ns, where elapsed ends at the later of schedule horizon or last terminal record;
- achieved-rate permille is the floor of oracle-valid completed logical rows times 1000 divided by
  scheduled logical rows;
- CPU permille of the assigned capacity is the ceiling of the candidate leaf CPU delta between
  `pre_measurement` and `measured_elapsed_end`, converted to ns and multiplied by `1_000_000`,
  divided by measured elapsed ns times cgroup capacity in millicores;
- write amplification milli is the ceiling of the target-device `io_write_bytes` delta between
  `pre_measurement` and the recorded resource-tail end/deadline, times 1000 divided by logical
  mutation bytes, thereby including journal/WAL, flush, compaction, and delayed writeback writes;
- sampled space amplification milli uses every measurement/tail bracket and cut. Its conservative
  value is the ceiling of `max(allocated_before, allocated_after) * 1000` divided by
  `min(logical_live_before, logical_live_after)`; the gate uses the maximum sampled ratio and reports
  the resource-tail bracket. It is explicitly a bracketed sampled gate, not an atomic filesystem
  peak;
- write-stall permille is the ceiling of the union of normalized candidate stall intervals
  intersected with `[pre_measurement, measured_elapsed_end]`, times 1000 divided by measured elapsed
  ns; and
- compaction clear time starts at `write_stop_offset_ns`, after the final queued/in-flight mutation is
  terminal, and ends only after engine compaction debt, cgroup `memory.stat` `file_dirty` and
  `file_writeback`, and target-device `io.stat` write-byte growth all remain within their approved
  baselines/tolerances for the uninterrupted hold interval. Its upper-bound duration is exactly
  `resource_tail.observation_end_offset_ns - write_stop_offset_ns`.

The database process and candidate leaf stay alive through that tail. A named `syncfs` or
candidate-side persistence drain is permitted only when the plan applies the identical policy to
both candidates; its latency and writes remain in tail evidence. Reaching the deadline before the
combined engine/kernel/device condition holds is FAIL, not a shortened numerator.

A zero denominator in a gate-bearing case is invalid plan construction, not a zero result. The new
leaf's `memory.peak` is the one exact hard memory peak and covers setup, warmup, measurement, and the
resource tail. Maximum measurement-period `memory.current` from the one-second stream is sampled
evidence, not an independently reset exact peak; cut readings are boundary evidence, not interval
peaks. The leaf's creation and empty-task check are retained as reset evidence. Native/candidate
estimates are diagnostics and cannot replace common OS observations.

RSS and stable-live-disk slopes use actual monotonic timestamps. For `n` samples at or after the
profile's slope start, let `x` be whole elapsed milliseconds between each sample's
`observation_end` and the first included end. For RSS, `y` is `rss_bytes`; for stable disk it is
`max(allocated_store_before_bytes, allocated_store_after_bytes)`. The following equations are over
mathematical signed integers; the runner and independent validator use a pinned arbitrary-precision
implementation rather than bounded `i128`. Schema caps still bound sample count, timestamps,
artifact size, and final reported values. Compute:

```text
D = n * sum(x*x) - sum(x) * sum(x)
N = n * sum(x*y) - sum(x) * sum(y)
slope_bytes_per_hour = ceil(N * 3_600_000 / D)
```

`D` must be positive. Signed ceiling always rounds toward the worse/higher growth result. Stable
disk permille/hour is `ceil(slope_bytes_per_hour * 1000 / baseline_bytes)`, where baseline is the
nonzero maximum of the first included allocation endpoints. The plan freezes compaction-debt,
dirty/writeback and device-write baselines/tolerances, the resource-tail deadline, and uninterrupted
hold interval. Missing/late samples, an out-of-schema final value, counter reset, or an invalid
denominator makes the attempt INVALID unless a previously proved candidate crash has already
classified it FAIL.

## Fault schedule and attempt classification

Logical hook faults retain C1 occurrence addressing, including post-success ambiguity and counters
continuing across retry. Physical faults have a separate immutable schedule. Each entry binds fault
ID, paired candidate slot, case/repetition, logical phase/occurrence, process/vnode/table target,
trigger, parameters, expected markers, recovery deadline, and oracle outcome. The same logical
trigger is instantiated for both candidates; candidate-specific actuator mappings live in separate
adapter records. Planned, armed, reached, actuated, released, reopened, and recovered markers are
separate evidence.

Physical schedules cover process death, scoped project-quota/loop-device `ENOSPC`, injected I/O
error, corruption/truncation, FD pressure, concurrent open, complete local loss plus portable
restore, and exact N/N-1 versions. A fault tool must never fill or corrupt the host root or an
unresolved path.

Physical-fault attempts use their own strict manifest rather than a conditional branch in the
steady-performance manifest. Before execution, the contract must freeze the full fault/endurance
matrix, actuation counts and triggers, exact N/N-1 pins, cache-loss harness, and recovery criteria.

Attempt status is derived, never submitted:

- **PASS:** the slot is complete, every required artifact and sample verifies, no candidate failure
  exists, and every applicable gate passes;
- **FAIL:** oracle divergence, candidate error/crash/corruption, timeout/overflow, durability or
  recovery failure, numerical/resource gate miss, required telemetry unsupported, or failure under
  an actuated fault; and
- **INVALID:** identity/procedure/environment mismatch, preflight failure before candidate work,
  clock regression, sample/evidence loss, runner internal error, undeclared external interruption,
  or a scheduled fault that was not reached/actuated.

After valid measurement start, memory, disk, FD, thermal, NVMe, compaction, timeout, or process
failure is FAIL unless a declared external interruption independently invalidates the complete
affected pair or campaign. Causation guesses cannot reclassify a failure. A proved candidate crash
has precedence over candidate-side sample/log loss it caused; the external supervisor's committed
markers preserve the FAIL. Invalid attempts never pass. Neither invalid nor failed attempts may be
deleted or replaced inside a campaign. A correction creates a new campaign and reruns the entire
paired matrix from clean state; the earlier campaign remains retained.

Campaign completeness is derived over the frozen slot schedule: any INVALID slot makes the campaign
INVALID; otherwise any FAIL makes it FAILED; only exactly one PASS for every slot makes a COMPLETE
evidence campaign. COMPLETE is not a backend-selection or production verdict.

## Evidence manifest and retention

The bounded JSON manifest references large artifacts rather than embedding them. Bundle objects
live only at `objects/<lowercase-sha256>` beneath one resolved root. Each descriptor contains a
closed role, exact byte length, lowercase SHA-256, media type, and record count where applicable.
The validator streams local bytes and never fetches a URI. Symlinks/reparse points, path traversal,
duplicates, unknown roles, missing objects, size mismatch, digest mismatch, or trailing bytes are
invalid. An immutable remote URI is campaign metadata for the already verified bundle, not an
artifact-resolution mechanism.

The campaign binds candidate wrapper/engine versions and source digests; binary, lockfile, source
archive and SBOM; compiler/target/build flags; complete option dump; profile and plan; OS image and
package snapshot; kernel/libc; CPU, microcode, governor and NUMA; cgroup; NVMe model/firmware/SMART
and scheduler; filesystem/mount; preflight commands/results; order and workload seeds; raw samples;
resource/backend stats; logs; model digests; and every scheduled PASS/FAIL/INVALID attempt. Separate
physical-fault manifests and markers are content-addressed campaign objects.

The manifest is not its own trust root. After the schedule closes, a detached campaign-completion
record binds the approval, profile, plan, source archive, runner and candidate binary digests; exact
campaign-manifest byte length and SHA-256; frozen slot schedule and validator-derived campaign
status; immutable object-store bucket/key version and retention identity; completion UTC time; and
runner plus independent-review signer/protected-review provenance. It is excluded from the manifest
and source archive it binds, signed after validation, and itself retained by immutable version and
content digest. The independent validator recomputes classification and every referenced digest;
the selection report consumes this authenticated completion digest, never a mutable manifest path.

Campaign metadata sets an initial object-lock or equivalent immutable deadline at least 365 days
after campaign completion. Before selection, it must be extended through the named product support
sunset plus 365 days. Availability is verified at least every 30 days and those checks are retained,
including for failed and invalid lineages. Any binary, config, plan, formula, environment, schedule,
or artifact change creates a new lineage.

Distributed checkpoint coordination, source offsets, sink commit, delivery/exactly-once protocol,
admission, and independent production soak are outside the schema and cannot be inferred from
backend-local observations. Backend-local snapshot/export remains in scope.

## Blocking execution and selection issues

No real v1 runner plan or candidate execution is allowed until DKS-Q2-001 through DKS-Q2-008 are
resolved and independently reviewed. Backend selection additionally requires DKS-Q2-009:

| ID | Blocker |
|---|---|
| **DKS-Q2-001** | Close the provisional [Zipf generator](state-backend-zipf-generator-v1.md) sub-blockers; approve its exact-target determinism/error/interference evidence, workload-v2 identity/goldens, and hot-mix-versus-Zipf case assignment. |
| **DKS-Q2-002** | Freeze a nonempty exact matrix, rational offered rates, fixed/variable-width policies, gate mappings, and compatible dimensions. For an all-distinct write, `128 * (16 + 65,536) = 8,390,656` bytes before framing already exceeds 8 MiB; 1,000 compact join probes at fanout 64 declare 15,360,000 range bytes. Every actual ordinal still requires exact post-dedup preflight. |
| **DKS-Q2-003** | Define long-stream ordinals, streaming resident/spill prefill, live-byte/cardinality/timer bands and control law, exact opposite-side join fanout, stabilization, deterministic TTL/churn, and setup/post-warmup/final digests. C1's 4,096-request oracle and state-size salt cannot be repeated into a 200,000-request performance claim. |
| **DKS-Q2-004** | Freeze exact warmup/measured counts and rates, per-case total semantics, drain/cooldown/reset, scheduler calibration/affinity, and an explicit balanced seeded order vector. The current minimum-count/minimum-duration booleans and five-pair alternation are not a complete schedule. |
| **DKS-Q2-005** | Add service-latency, runner-overhead, scheduler, oracle-lag and observation-skew gates; freeze result-ring/raw-sample ceilings and gate-bearing telemetry control trials; and complete image/package/CPU/microcode/NUMA/NVMe identities. |
| **DKS-Q2-006** | Prove stable mappings for compaction debt, stalls, XFS project-quota bytes, cgroup dirty/writeback, device writes, snapshots/iterators, native memory, and pressure counters. The static audit fails unmodified Fjall 3.1.8 because debt/stall signals are absent and blocks the current RocksDB binding because its stall ticker omits a verified write-buffer-manager/database-scope path. Supply a proven complete source, patch/upstream, or reject; never encode unsupported as zero. |
| **DKS-Q2-007** | Implement and review detached approval/completion records, pinned Fjall/RocksDB persistence mappings, complete configuration dumps, and cache-loss truth-table conformance. |
| **DKS-Q2-008** | Freeze the paired physical-fault and 24/72-hour endurance matrices, actuator and N/N-1 pins, recovery criteria, and a bounded time-resolved endurance encoding distinct from finite raw samples. |
| **DKS-Q2-009** | Before selection, approve and pass a separate C3 shared-database concurrency contract: deterministic disjoint-vnode lanes and per-lane oracle order; hot-writer/victim and mixed point/range/snapshot traffic; victim plus aggregate tails, global stalls/resources; and barrier-controlled races with restore activation, cleanup, and pinned snapshots. |

Schema/validator work may proceed while these blockers are open. Synthetic plan and evidence
fixtures must be conspicuously ineligible. Exact candidate builds and semantic conformance may
follow, but the tool still exposes no measurement command until named owners approve the exact
profile and complete runner-plan hashes.

## Research basis

- [Open Versus Closed: A Cautionary Tale](https://www.usenix.org/conference/nsdi-06/open-versus-closed-cautionary-tale)
  and the [coordinated-omission study](https://vsis-www.informatik.uni-hamburg.de/getDoc.php/publications/569/Coordinated_Omission_in_NoSQL_Database_Benchmarking-Friedrich.pdf)
  support independent offered arrivals and due-to-return/terminal measurement.
- [HdrHistogram](https://github.com/HdrHistogram/HdrHistogram) documents coordinated-omission
  correction and bounded recording. V1 retains raw timestamps instead, avoiding double correction
  and library-specific histogram identity.
- [Fjall 3.1.8](https://docs.rs/crate/fjall/3.1.8) provides cross-keyspace batches, snapshots, and
  explicit persistence modes, while its tagged
  [backpressure paths](https://github.com/fjall-rs/fjall/blob/3.1.8/src/keyspace/mod.rs) and
  experimental counters require tail/telemetry qualification.
- RocksDB's [write-stall](https://github.com/facebook/rocksdb/wiki/Write-Stalls),
  [statistics](https://github.com/facebook/rocksdb/wiki/Statistics), and
  [tuning](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide) guidance motivate
  pressure, compaction, OS, and instrumentation-overhead evidence rather than latency alone. Those
  mutable guides inform method only. RocksDB's official
  [WAL-sync description](https://github.com/facebook/rocksdb/wiki/Track-WAL-in-MANIFEST) and pinned
  [10.4.2 options](https://github.com/facebook/rocksdb/blob/v10.4.2/include/rocksdb/options.h)
  constrain the proposed durability mapping; actual calls are frozen from that source plus
  `rocksdb` 0.24.0 bindings.
- The authoritative [Linux 6.1 cgroup v2](https://www.kernel.org/doc/html/v6.1/admin-guide/cgroup-v2.html)
  interfaces supply common CPU, memory, I/O, and peak observations across candidates. In that
  version `memory.peak` is read-only and covers the cgroup lifetime, hence one new leaf per attempt.
- [SLSA build provenance v1.2](https://slsa.dev/spec/v1.2/build-provenance) and the
  [USENIX artifact guidelines](https://www.usenix.org/conference/usenixsecurity22/artifact-appendix-guidelines)
  inform immutable build and evidence provenance.

ADR-008 separately records the 2025–2026 Flink, RisingWave, Materialize, Spark, Kafka Streams, and
systems-research decisions for the production distributed-state architecture. This runner ADR does
not duplicate those operator/checkpoint/delivery authorities.

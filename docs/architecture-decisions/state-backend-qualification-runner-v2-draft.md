# State backend qualification runner and evidence contract v2 — validation contract

- **Status:** approved for validation-only implementation; candidate execution remains prohibited
- **Direction approval recorded:** 2026-07-24
- **Validation scope accepted:** 2026-07-25 project-owner direction recorded by Cycle 38
- **Approval workflow for validation-only implementation:** none; ordinary repository review and the
  freezing commit provide the audit trail
- **Scope:** standalone tools/state-backend-qual validation and later qualification tooling
- **Frozen v4 validation scope:** Fjall 3.1.8 and RocksDB 10.4.2 through rocksdb 0.24.0 reference lineage
- **Preferred future product candidate:** TidesDB, subject to a new profile and all source-closure gates
- **Production backend selected:** none
- **Execution authorized:** no
- **Reserved schemas instantiated:** none
- **Cluster admission:** unchanged; [LDB-4007] and [LDB-0013] remain fail-closed
- **Direction basis:** [maintenance-health v2 proposal](state-backend-maintenance-health-v2-proposal.md)
- **Candidate design evidence:** [Cycle 19 paper mappings](../reports/state-backend-maintenance-health-mapping-designs-2026-07-24.md)
- **Retained logical inputs:** [C1 model](state-backend-qualification-model-v1.md) and
  [workload v2](state-backend-workload-v2.md)

## Decision state and authority

The workload/operations direction `APPROVE_MAINTENANCE_HEALTH_V2_DIRECTION`, the earlier project-owner
approval of the DKS runner direction, and the 2026-07-25 Cycle 38 instruction to remove the proposed
GitHub approval ceremony authorize this consolidated contract for validation-only implementation.
The freezing commit records the exact reviewed bytes. This authority permits only standalone
schemas, parsers, formulas, bounded readers, deterministic synthetic fixtures, and negative-
capability tests. It does not authorize native source or adapter construction, candidate execution,
backend selection, runtime integration, cluster admission, or a production claim.

No DKS approval workflow existed under `.github/workflows`; ordinary CI only checks the standalone,
explicitly ineligible validator. This contract requires no protected GitHub environment, provider
receipt, detached signature, named reviewer receipt, or multi-principal approval for validation-only
work. Independent review remains a technical quality check. Candidate construction, execution,
selection, runtime, and production retain the separate gates below.

This document is the complete v2 validation contract. Its normative rules do not require a reader to
compose the v1 runner with the maintenance-health proposal. The v1 contract and its fixtures remain
unchanged regression lineage and have no interpretive authority over a future v2 artifact.

The words MUST, MUST NOT, REQUIRED, SHALL, SHALL NOT, SHOULD, SHOULD NOT, and MAY are normative in
this document. The Cycle 38 review records the revised contract/profile lengths and SHA-256 values,
the closed review findings, and the validation-only scope. The commit containing that record freezes
this contract lineage. A repository field or validator process still cannot approve candidate
construction, execution, a result, or backend selection.

Validation-only implementation MUST mechanically preserve all of these negative capabilities: no
run/dispatch/approve/select/qualify command; no backend, candidate, cloud, network, or process-
spawning dependency; no runtime crate or admission consumer; and no output that claims qualification
or production evidence. Candidate source construction requires a later candidate-specific explicit
project-owner task. Candidate execution requires separate authorization over the exact candidate,
run plan, target, isolation, limits, and cost. Selection, runtime integration, and production each
retain their own reviewed decision and evidence gates.

Any predecessor reference to freezing "exact schemas" means freezing the complete normative wire
and schema semantics in this contract. JSON Schema files, parsers, validators, formulas, and
synthetic fixtures are the newly authorized validation-only implementation. Plan, evidence,
authorization, and completion containers remain later pre-execution work.

## Comparison boundary and redb separation

The frozen distributed-state-qual/v4 and linux-nvme-v4 profile is an exact-delta successor to the
v3 Fjall/RocksDB comparison profile. Its common 8 GiB block-cache, 2 GiB write-buffer, and six-
background-worker controls remain regression/reference data for those exact subjects. The 2026-07-25
project-owner direction makes TidesDB the preferred product candidate instead of RocksDB, but MUST
NOT relabel or reinterpret v4. RocksDB remains a mature reference only, not an intended product
backend. A TidesDB campaign requires a new profile identity and mapping after its exact native/Rust,
atomicity, recovery, checkpoint, resource-governance, and maintenance-health source closures. Native
remote storage remains disabled. No TidesDB source construction or execution is authorized here.

V4 MUST NOT be represented as a TidesDB or redb profile. Candidate-neutral bounds, primitive
parsers/evaluators, formulas, and negative-capability patterns may be reused by a later TidesDB
lineage. The exact profile-binding, mapping, and bundle wire identities in this contract bind v4 and
must receive successor identities or an explicit successor contract for TidesDB.

redb 4.1.0 is PARKED after its bounded Cycle 34 design timebox and is outside v4. Its archived paper
mapping and prescreen are vocabulary provenance only: they cannot instantiate this contract,
satisfy any qualification gate, be translated into v4, be pooled with v4 evidence, or create a
three-candidate ranking. Only a new, separately owner-authorized bounded micro-prescreen charter may
reopen it; a favorable observation could at most justify a later additive profile/control and
mapping proposal.

## Frozen validation identities and migration

The following identities are frozen for validation-only implementation. Their schemas and readers
are not instantiated until their implementation commits land, and none creates execution authority:

| Item | Frozen identity |
|---|---|
| runner contract | state-backend-runner-contract/v2 |
| comparison profile/schema | distributed-state-qual/v4 |
| comparison profile instance | linux-nvme-v4 |
| profile-use approval envelope | state-backend-profile-use-approval/v1 |
| candidate mechanism mapping | state-backend-mechanism-mapping/v2 |
| candidate health samples | state-backend-candidate-health-samples/v1 |
| resource formulas | state-backend-resource-formulas/v3 |
| synthetic mechanism bundle input | state-backend-mechanism-bundle-validation-input/v2 |

These existing byte contracts are retained without reinterpretation:

| Item | Retained identity |
|---|---|
| C1 reference model | state-backend-reference/v1 |
| workload | state-backend-workload/v2 |
| latency samples | state-backend-latency-samples/v1 |
| common resource samples | state-backend-resource-samples/v2 |
| common resource cuts | state-backend-resource-cuts/v2 |
| engine-stall intervals | state-backend-stall-intervals/v1 |
| target-device I/O | state-backend-target-device-io/v1 |
| physical layout | state-backend-physical-layout/v1 |
| physical faults | state-backend-physical-faults/v1 |
| attempt classification | state-backend-attempt-classification/v1 |

The previously reserved runner-plan/v1, evidence-manifest/v1, qualification-approval/v1, and
campaign-completion/v1 identities produced no real artifacts. They may be instantiated without a
version bump only if their first exact schemas bind, as separate length-and-SHA-256 descriptors,
the runner contract, profile, mechanism mapping, resource formulas, workload, candidate builds, and
every subordinate wire used by the object. A schema that fixes runner-contract/v1, profile v3,
mapping v1, debt samples v1, or formulas v2 MUST receive a new identity. No validator may guess a
subordinate version from the container version.

Profile v4 differs from the exact v3 JSON value only in:

1. schema_version becomes distributed-state-qual/v4;
2. profile_id becomes linux-nvme-v4; and
3. resource_gates.background_maintenance_debt_max_bytes is absent.

The prepared [v4 freeze-candidate bytes](../../tools/state-backend-qual/profiles/linux-nvme-v4.freeze-candidate.json)
are 7,838 bytes with lowercase SHA-256
`94652d30153d998628d4e1d2b5da87bce59f5064192eeba9e9331f3f40507392`. A preparatory independent
reconstruction reproduced the bytes from v3 using only the three transformations above, found zero
residual decoded differences, and confirmed UTF-8 without BOM, LF-only line endings, and one
trailing LF. Cycle 38 accepts these exact bytes as an explicitly ineligible validation input. This
does not approve the profile for a candidate run or make it qualification evidence.

Its schema identifier/title change only as required to describe those bytes. Every other value,
field, order-insensitive JSON meaning, threshold, and gate remains equal. An exact-delta test MUST
compare the complete decoded values after applying only those three transformations.

Consequently, v4 deliberately retains v3's `notice=NOT QUALIFICATION EVIDENCE`,
`status=candidate_unapproved`, `qualification_eligible=false`, null owner approvals, and null
environment image/package values. Those fields are immutable historical provenance; no parser,
plan, flag, or later record may rewrite, reinterpret, or silently override them. Validation-only
readers may consume v4 solely as an ineligible fixture. A future execution command may consume it
only as one member of an explicitly authorized pair:

1. the unchanged profile bytes; and
2. a detached `state-backend-profile-use-approval/v1` envelope.

The strict envelope is an unsigned, result-free declaration. It binds the exact profile role, byte
length, and SHA-256; this runner-contract descriptor; the independently generated exact-delta proof;
the named environment image and package-snapshot descriptors that replace neither null profile
field; the plan's explicit profile-use decision; and an explicit
acknowledgement that the embedded false/null fields remain false/null. It contains no signature or
self-approval field and has no authority by itself. A later, separate execution-design ADR must
define how an explicit project-owner run authorization binds the profile, envelope, plan, exact
candidate, target, isolation, limits, and cost before any execution command exists. This contract
does not prescribe GitHub approvals or signatures for that future boundary. Profile bytes alone, or
profile plus an unsigned envelope, remain execution-ineligible; an envelope cannot approve another
profile, fill a profile field, select a backend, or make the profile itself qualification evidence.

Profile v3, mapping v1, maintenance-debt samples v1, resource formulas v2, and bundle v1 remain
immutable, execution-ineligible regression fixtures. V2 readers MUST reject them, mixed versions,
converted fixtures, unknown versions, and attempted compatibility translation. There is no
compatibility adapter.

## Approval and threshold ownership

The runner contract is candidate-neutral: it freezes closed signal kinds, units, predicates,
formulas, validity rules, and authority. It does not contain candidate-native signal names or
candidate-specific numerical health limits.

Each immutable mechanism-mapping/v2 object freezes the exact candidate signal inventory, proof
descriptors, predicate instances, and every candidate-specific numerical threshold before any
candidate result exists. A mapping never approves itself. It carries no qualification-eligible
boolean and no approved status. The exact runner plan binds its length and SHA-256. Only the future
explicit run authorization described above may make it consumable by an execution command.

This separates neutral evaluator semantics from candidate-native numbers without permitting
result-driven threshold choice. Source construction may reveal that a paper signal is unsound; in
that case the mapping and any affected contract assumption are revised and independently reviewed
before approval and before results. No threshold may be edited in place after a result, and a new
mapping digest creates a new campaign lineage.

Approximate or estimated signals MUST bind a threshold-basis proof and an explicit safety-margin
proof before approval. A passing estimate proves only that its native predicate held. It is never
converted into exact remaining work, spare capacity, or a cross-engine score.

## Complete campaign and workload contract

### Plan, schedule, and reset

A runner plan is a complete, executable, nonempty matrix. It has no draft, placeholder, empty, or
executable-false form. Synthetic validator fixtures are a separate record class and are permanently
qualification-ineligible.

The plan names exactly two candidates, one compiled into each candidate binary. It binds the exact
raw profile, mapping, physical layout, workload-v2 case body/wrapper and expectation descriptors,
model/generator identities, candidate source/configuration/binary, adapter, runner, build,
environment, and policy bytes by length and SHA-256. Runtime result roots are result fields, never
future values embedded in a plan.

A campaign contains one immutable ordered slot schedule. Every slot has one contiguous index,
case, repetition, and candidate. Its seed is profile.fixed_seeds[repetition]. Every
case/repetition is one adjacent two-slot pair containing each candidate once. The schedule is
balanced AB/BA with per-case order-count imbalance at most one. A failed or invalid slot stays in
place. A rerun is a non-gating diagnostic lineage; a correction creates a new whole campaign.

Each candidate starts from a fresh directory populated from the same candidate-neutral setup stream.
Setup is verified after persist/close/reopen by a streaming independent-expected/candidate merge.
Warmup remains in state and in the oracle but is excluded from performance samples. Candidate-local
manual compaction, quiescing, cache dropping, or reuse of another candidate's state is forbidden
unless one named policy applies identically to both.

Each case fixes scenario, residency, contention, active vnode count, width policy, batch rows, join
fanout, setup/prefill, warmup, measured stream, churn/retention, persistence, state-control bands,
exact counts and rational rates, one foreground worker, in-flight and queue bounds, drain and
terminal deadlines, gate mappings, counter roles, and fault schedule. The long stream MUST keep
actual live bytes, cardinality, and timer density inside the approved bands at setup, throughout
warmup/measurement, and at final digest. Leaving a band is candidate FAIL when the approved
generator/reference proves the request valid; generator/reference failure is INVALID.

At the warmup horizon every measurement-bearing native health predicate MUST be within its approved
measurement band. Tail-only quiescence predicates are not required while offered mutations
continue. The runner neither waits longer nor invokes candidate-specific maintenance to improve the
starting state.

### Open-loop offered load and conservation

An offered rate is a positive reduced rate_numerator/rate_denominator requests per second.
Zero-based request i is due at:

    floor(i * 1_000_000_000 * rate_denominator / rate_numerator)

The exclusive horizon is:

    ceil(request_count * 1_000_000_000 * rate_denominator / rate_numerator)

Intermediate arithmetic is checked u128 and final offsets fit u64. Warmup and measurement have
separate counts/rates meeting all profile minima. Measured elapsed begins at phase start and ends at
max(schedule_horizon, last_terminal_offset). Drain deadline is an exact duration after the horizon.

Scheduling never waits for the prior completion. An ordinal may enter only the pre-reserved bounded
queue. Payload generation occurs after dispatch in pre-sized, pre-touched scratch. Queue exhaustion
caused by candidate backpressure, timeout, unrequested candidate exit, or candidate work remaining
at drain deadline is FAIL. Generator/reference/ring ownership failure is INVALID. Terminal abort
records later ordinals as not_attempted_after_abort. The exact conservation identity is:

    planned = terminal_records + not_attempted_after_abort

Every attempted ordinal has one immutable terminal record. Unknown/duplicate ordinals or a missing
required latency artifact are INVALID, never a partial pass. Once the candidate return is published
in supervisor-owned memory, the surviving supervisor performs exactly one oracle comparison unless
a proved runner/external interruption prevents it. PASS requires every planned ordinal to return
OK and match.

CLOCK_MONOTONIC_RAW is timestamp authority. Absolute sleep uses paired CLOCK_MONOTONIC readings,
then RAW recheck and a bounded final spin on a non-candidate controller core. Affinity, spin margin,
mapping refresh, clock resolution, and maximum calibration error are exact plan fields and preflight
gates.

## Latency and bounded runner hot path

Each attempted request records scheduled, enqueued, dispatch_start, service_start,
candidate_return, and terminal offsets from measured-phase start. Present stages are ordered.
Absent offsets are zero and declared by a prefix stage mask. Derived populations are:

| Population | Exact subtraction |
|---|---|
| scheduler lateness | enqueued - scheduled |
| state queue wait | dispatch_start - enqueued |
| runner preparation | service_start - dispatch_start |
| candidate service | candidate_return - service_start |
| offered end-to-end | candidate_return - scheduled |

Profile request gates apply to offered end-to-end; state_queue_wait applies to queue wait. Candidate
service and preparation remain separate. Final profile/plan approval MUST add explicit numerical
service gates; no implementation may infer them from request gates or results.

Latency-samples/v1 starts with the 27-byte ASCII domain
LDB-SBQ-LATENCY-SAMPLES-V1 plus its terminating zero byte (the zero is included in those 27 bytes),
then record_count_u64_be. Each 58-byte record is:

    ordinal_u64_be
    scheduled_offset_ns_u64_be
    enqueued_offset_ns_u64_be
    dispatch_start_offset_ns_u64_be
    service_start_offset_ns_u64_be
    candidate_return_offset_ns_u64_be
    terminal_offset_ns_u64_be
    stage_mask_u8
    outcome_u8

The exact length is 35 + 58 * record_count with no trailing bytes. Valid stage masks are 0x00,
0x01, 0x03, 0x07, and 0x0f. Outcome/stage requirements are:

| Tag | Outcome | Required stages |
|---:|---|---|
| 0x00 | ok | all |
| 0x01 | candidate_error | all |
| 0x02 | oracle_mismatch | all |
| 0x03 | queue_overflow | none |
| 0x04 | queue_timeout | enqueued only |
| 0x05 | service_timeout | through service_start |
| 0x06 | adapter_child_crash | any valid prefix except returned |
| 0x07 | preparation_timeout | through dispatch_start |
| 0x08 | runner_error | any valid prefix |
| 0x09 | external_interruption | any valid prefix |
| 0x0a | observation_overflow | through service_start |

Ordinals are contiguous from zero. Returned calls populate service/end-to-end; accepted/dispatched
calls populate their queue populations. Timeout/crash samples are retained as right-censored lower
bounds and fail independently. Preparation timeout, runner error, and external interruption are
INVALID. No latency gate is pass-eligible unless every planned request returned and matched.

The first terminal condition is committed by one supervisor compare-and-swap. A return published
before a later child exit is still compared; the later unrequested exit is an attempt-level FAIL.
A supervisor-requested kill is the actuator for the already chosen timeout/overflow condition, not
a second crash outcome. Terminal records never change.

The candidate runs in a killable child. The supervisor owns preallocated, page-touched request,
reference, result, and raw-sample regions. One canonical immutable request frame is validated before
the independent reference and adapter consume it. Candidate output cannot affect future requests.
Slot counts, aligned byte reservations, maximum request/observation bytes, reference lead,
comparison lag, timeouts, occupancy, NUMA placement, and control-core affinity are exact plan gates.
There is no per-request allocation, second generator expansion, unbounded run-ahead, measured-path
file I/O, logging, lock, or histogram merge in the sample-recording path.

A result slot is bound before service_start and follows:

    free -> reserved -> child_writing -> published -> oracle_checked
         -> terminal_committed -> free

An oversized candidate observation cannot write past its slot. It publishes a fixed overflow
report, becomes observation_overflow, aborts as FAIL, and is quarantined until the child dies. If
the correct reference observation does not fit, the attempt is INVALID. Timeout/crash slots with a
possible writer are also quarantined. The supervisor serializes committed evidence after child
termination.

Candidate service begins immediately before the adapter primitive and ends only after returned
keys/values are copied into runner-owned buffers. Generation, validation, reference execution,
comparison, digest, serialization, and resource polling are outside service but remain charged to
their own CPU, scheduler, lead/lag, page-fault, null-control, and interference evidence. No measured
overhead is subtracted.

Exactly one foreground service worker models one LaminarDB worker's serialized atomic path.
Candidate background workers remain enabled at the profile cap. Cross-vnode scaling is later product
evidence. Backend selection additionally requires C3 shared-database concurrency.

Raw integer nanoseconds are retained. Nearest-rank p/1000 selects rank ceil(p*N/1000). The runner
derives p50, p90, p95, p99, p99.9, and exact maximum. P90 is diagnostic; every profile-named
quantile and maximum is gating. There is no clipping, outlier removal, timeout deletion, synthetic
coordinated-omission correction, or zero-sample pass. Every raw gate value remains in nanoseconds
and is compared to `gate_us * 1000` using checked integer multiplication; overflow is INVALID, never
a saturated or rounded comparison. Finite raw latency wire MUST NOT be truncated or silently reused
for 24/72-hour endurance; endurance requires its own approved bounded encoding.

Null-adapter calibration and paired telemetry-on/off controls report throughput, every gate-bearing
latency percentile and maximum, CPU, memory, and observer resources. Deltas are reported, never
subtracted. Only the observer/sample-recording updates on the measured request path are required to
be allocation-, lock-, logging-, merge-, and I/O-free and bounded; the backend service itself may
perform its required locking and I/O. No per-row FFI call, metric query, allocation, or task spawn is
permitted in those observer updates.

## Logical layout, lifecycle, and adapter semantics

The exact layout is one database, 256 logical vnodes, and four physical keyspaces:

| Keyspace | Logical tables |
|---|---|
| state | aggregate 0x01, window 0x02, output bookkeeping 0x06 |
| timer | timer index 0x03 |
| join_left | join left 0x04 |
| join_right | join right 0x05 |

Each vnode lifecycle record is at vnode_u32_be || 0x00 in state. Data keys are:

    vnode_u32_be || 0x01 || generation_u64_be || table_tag_u8 || opaque_key

The durable lifecycle record contains serviceability, optional active generation, never-decreasing
next generation, and optional staging reservation. Normal operations enter one per-vnode lifecycle
guard, verify serving, capture active generation, and complete before release. One mutation batch
atomically updates all participating keyspaces.

Restore first obtains the external ownership fence, marks the vnode unservable, durably reserves and
increments a never-reused generation, stages/verifies only that generation, then atomically
activates it under the exclusive guard. A crash exposes the old complete serving cut or explicit
restore-in-progress, never partial activation. Recovery reclaims an orphan and uses a higher
generation or fails before serving.

Old/staging/pinned generations count against the frozen-generation cap. Reclamation is bounded and
resumable across all four keyspaces and verifies emptiness. Cleanup clears serviceability/active
generation under the fence, removes all generations, and retains the durable tombstone/counter.
Data without a lifecycle record is corruption except a proved empty new database. Generation wrap
or exhaustion is terminal. First initialization of a proved empty new database creates that durable
tombstone and reserves generation zero through the same fenced reserve/stage/verify/activate
protocol; it is not a special unfenced data-load path.

Snapshot/export reads lifecycle and data at one cross-keyspace sequence cut. Pinned generations,
snapshot/iterator lifetimes, and reclamation are bounded and observed. Reads return model order even
when physical iterators must merge. Cached generation pointers are invalidated by lifecycle or
ownership change.

The private adapter covers bounded batch reads/ranges, atomic mutations, snapshot/export, sorted
restore, cleanup, explicit persistence, and stable observation. It is not a public plugin or future
runtime trait. Returned engine-owned slices are copied/released inside service. Native memory is
inside candidate cgroup and process-tree PSS/RSS. Unsupported is never zero; N/A requires its exact
approved union arm.

## Persistence and portable snapshot boundary

WAL/journal is enabled. The candidate-neutral truth table is:

| Boundary | Visible/atomic | Process death | Proved host-cache loss |
|---|---|---|---|
| buffered_batch | complete batch | required | not required |
| persist_data | all prior batches | required | recovery-required log and proved directory entries |
| persist_all | all prior batches | required | all recovery-required data/metadata covered by audited primitive |

Candidate mappings freeze exact calls/configuration. Proposed calls in source audits are not
normative evidence. A boundary unsupported by an audited engine/file/directory sequence is
unsupported, not approximated.

Host-cache-loss recovery yields an atomic prefix:

    last acknowledged durable fence <= recovered prefix
                                      <= last completely submitted batch

Unfenced batches may be wholly present/absent, never torn. Process death retains buffered_batch.
Lost acknowledgement after a completed fence does not weaken that fence. Persistence-case service
includes its exact fence group/cadence.

Process kill is not power loss. Cache-loss proof requires power cycle or a documented block-device
harness recording device cache, power-loss protection, filesystem barriers, and hypervisor cache
mode. Portable logical snapshot/export is the common selection gate; a native checkpoint/directory
copy is diagnostic only. Setup, export, restore, cleanup, overlap tail inflation, retained versions,
resources, and reclaim time remain separate evidence. None proves distributed checkpoint, source
offset, sink commit, or exactly-once semantics.

## Common resource and device observations

All attempt offsets use one pre-child CLOCK_MONOTONIC_RAW origin, distinct from measured-latency
origin. The manifest binds it and measured_phase_start_offset_ns. Stall/device measurement start
equals measured phase start; measurement end is its checked sum with measured elapsed. Conversion
between origins uses checked addition only.

Each attempt uses a new empty candidate cgroup-v2 leaf. Candidate CPU is cpu.stat usage_usec;
memory current/peak is the leaf's value; target physical reads/writes are io.stat for the frozen
major:minor. Supervisor/generator/observer/spool use a separate control cgroup and device. Complete
process-tree PSS/RSS/FDs are observed.

An XFS prjquota project rooted before open accounts WAL/journal, manifests, tables, temporaries,
obsolete data, and directory inodes. All candidate paths remain under it. Before-open and post-tail
st_blocks inventory independently audits assignment, deduplicating hard links by device/inode.
Symlink/reparse, mount escape, path escape, wrong project ID, wrap/error, or disagreement outside
approved tolerance is INVALID.

Common resource samples v2 use the 28-byte ASCII domain LDB-SBQ-RESOURCE-SAMPLES-V2 plus its
terminating zero byte (included in those 28 bytes), record_count_u64_be, and 160-byte records with
these twenty u64 fields:

    sample_index, observation_begin_offset_ns, observation_end_offset_ns,
    cpu_usage_us, memory_current_bytes, memory_peak_bytes,
    io_read_bytes, io_write_bytes, pss_bytes, rss_bytes, open_fds,
    allocated_store_before_bytes, allocated_store_after_bytes,
    live_snapshots, live_iterators, frozen_generations,
    file_dirty_bytes, file_writeback_bytes,
    logical_live_before_bytes, logical_live_after_bytes

Length is 36 + 160*count. Indices are contiguous. Begin <= end and skew meets the approved plan.
Logical-live and allocation endpoints bracket the other reads. Unsupported common fields block
conformance; no sentinel exists.

Nominal observations follow a one-second monotonic schedule from the approved first-observation
boundary through the final resource-tail cut. The plan freezes the exact expected count, release
offsets, lateness bound, and treatment of a bracket intersecting each phase; missing, late,
duplicate, or extra observations are INVALID. For each record the observer reads, in order:
`observation_begin`; published C1 logical-live bytes and XFS project quota; all other listed
counters; project quota and logical-live bytes again; then `observation_end`. The manifest freezes
the exact source and read operation for every field, this order, reset action, wrap/saturation
behavior, sampling error, and availability. A source/read-order/reset/wrap mismatch is INVALID.

Common resource cuts v2 use the 25-byte ASCII domain LDB-SBQ-RESOURCE-CUTS-V2 plus its terminating
zero byte (included in those 25 bytes), record_count_u64_be, and 112-byte records: cut_tag plus
seven zero bytes, then:

    observation_begin_offset_ns, observation_end_offset_ns,
    cpu_usage_us, io_read_bytes, io_write_bytes,
    memory_current_bytes, memory_peak_bytes,
    allocated_store_before_bytes, allocated_store_after_bytes,
    logical_live_before_bytes, logical_live_after_bytes,
    file_dirty_bytes, file_writeback_bytes

Length is 33 + 112*count. A performance attempt has tags 0x00 pre_measurement, 0x01 write_stop,
0x02 last_terminal, 0x03 measured_elapsed_end, and exactly one of 0x04 tail_stable_end or 0x05
tail_deadline, in order. Its count is five. Write stop is:

    max(schedule_horizon,
        last terminal_offset of any attempted mutating ordinal)

The write-stop cut begins after that event and mutation drain. Tail schedules anchor to the logical
write-stop offset, not the later observation bracket. Every cut satisfies begin <= end and
`end - begin` meets the approved observation-skew gate. The final cut begins after the
measured-elapsed-end cut and after either completion of the uninterrupted hold or the tail deadline;
its end is no earlier than every preceding cut end. Cut sources, read order, reset/wrap behavior,
sampling error, and availability are frozen identically to nominal observations. Checked chronology
or provenance failure is INVALID.

Target-device I/O v1 uses the 28-byte ASCII domain LDB-SBQ-TARGET-DEVICE-IO-V1 plus its terminating
zero byte (included in those 28 bytes), exact profile SHA-256, major/minor, measurement start/end,
capture end, shard_count_u32, and anomaly flags_u32. Shards are contiguous, at most 256. Length is
100 + 112*shards. A shard record is:

    shard_u32 || four_zero_bytes ||
    issued_u64 || success_u64 || error_u64 || incomplete_u64 ||
    untracked_issue_u64 || orphan_completion_u64 || duplicate_issue_u64 ||
    maximum_duration_ns_u64 || maximum_issue_ns_u64 ||
    maximum_terminal_ns_u64 || maximum_logical_bytes_u64 ||
    maximum_local_sequence_u64 ||
    maximum_operation_u8 || maximum_status_u8 ||
    witness_present_u8 || five_zero_bytes

Operations are read 0x00, write 0x01, flush 0x02. Status is success 0x00, error 0x01, incomplete
0x02. Checked global conservation is issued = success + error + incomplete + untracked_issue.
Issues are charged to their issue-CPU shard; completion outcomes/maxima are charged to their
completion-CPU shard, so per-shard conservation is not required. Capture charges an incomplete to
its recorded issue shard after admission stops. Within a shard an equal-duration maximum retains
the least local outcome sequence; the offline global witness order is duration descending, shard
ascending, local sequence ascending. No shared global issue counter enters the block hot path.
Read/write maximum witnesses have positive `maximum_logical_bytes`; flush witnesses have zero.
A shard with any success/error/incomplete outcome MUST set `witness_present=1` and contain a
self-consistent maximum witness whose operation, status, times, duration, bytes, and local sequence
identify one counted outcome. A shard without such an outcome MUST set `witness_present=0` and all
maximum/witness fields to zero. Any inconsistency is INVALID.
Anomaly bits are 0x01 issue-state-capacity-exhausted, 0x02 event-loss, 0x04 counter-overflow,
0x08 unsupported-operation, 0x10 target-identity-mismatch, 0x20 tracer-lifecycle-error, and 0x40
capture-iteration-error; every other bit is invalid. Counters saturate and latch overflow, never
wrap. Any flag, untracked/orphan/duplicate issue, attribution mismatch, or population mismatch is
INVALID unless proved candidate-crash precedence applies. Error/incomplete is FAIL. The gate is the
maximum recorded issue-to-terminal or issue-to-capture duration, including censored incomplete
lower bounds, rounded up to milliseconds. Empty/untracked population cannot pass.
The population is every request issued on the exclusive target device in
[measurement_start,measurement_end), matched until capture_end. The filesystem uses no online
discard and the evidence/control path uses another device.

The tracer emits no per-request evidence stream. Exact tracer program/source/build bytes, attach
points, map layout and capacity, per-CPU shard count, retained issue-state bound, atomic operations,
saturation behavior, admission-stop/capture sequence, and paired observer-on/off CPU/latency/event-
loss controls are post-contract pre-execution gates under DKS-Q2-005/006. The approved runner plan
and qualification approval MUST bind them. Until those bounds pass independent source review and
target-host controls, no execution command may exist and target-device evidence cannot be produced.

## Candidate mapping v2

Mapping v2 is strict JSON, duplicate-key rejecting, at most 262,144 bytes, and rejects placeholders,
unknown fields, unknown enums, non-u64 nonnegative integers, all-zero hashes, and digest/identity
drift. IDs match [a-z0-9][a-z0-9._/-]{0,127}. Descriptors contain closed role, positive byte length,
nonzero lowercase SHA-256, and media type. Descriptor byte length is 1..17,179,869,184 and media
type is application/json, application/octet-stream, or application/zstd.

The top-level fields are exactly:

- schema_version = state-backend-mechanism-mapping/v2;
- notice = MAPPING REQUIRES DETACHED APPROVAL;
- mapping_id;
- profile { id, sha256 }, fixed to exact v4 bytes;
- candidate { id, engine, version, source, configuration }, where source has role
  candidate-source and configuration has role candidate-configuration;
- background_maintenance_health;
- engine_pressure_stalls; and
- target_device_io_latency.

The stall and target-device arms use the retained v1 identity and the complete byte/evaluator
semantics restated in this document. Stall evidence binds the mapping-v2 SHA-256; target-device
evidence binds only the exact profile-v4 SHA-256, while the plan/evidence manifest binds that common
artifact to the candidate mapping and attempt. The observed
stall object contains exactly kind=observed,
artifact_schema_version=state-backend-stall-intervals/v1,
population=all-source-intervals-intersecting-measurement/v1,
aggregation=interval-union-intersect-measurement-ceil-permille/v1, and 1..16 sorted mechanisms.
Each mechanism contains mechanism_id, source_contract,
coverage=foreground-admission-or-progress-stall-intervals, source_proof, and configuration_proof.
The N/A stall object contains exactly kind=not_applicable,
reason_code=no-engine-pressure-stall-mechanism-in-exact-build,
claim_scope=complete-candidate-process, source_proof, configuration_proof, and
bounded_probe_proof.

Target device is always observed and common. Its object contains exactly kind=observed,
artifact_schema_version=state-backend-target-device-io/v1,
population=requests-issued-during-measurement/v1, operations=[read,write,flush],
attribution=exclusive-target-device,
aggregation=maximum-issue-to-terminal-ceil-milliseconds/v1, and
incomplete_request_policy=candidate-fail-with-censored-lower-bound.

### Maintenance-health union

The health arm is exactly:

    observed {
        kind = observed,
        artifact_schema_version,
        population_contract,
        aggregation,
        inventory_proof,
        mechanisms[],
        signals[]
    }

or:

    not_applicable {
        kind = not_applicable,
        reason_code,
        claim_scope,
        source_proof,
        configuration_proof,
        bounded_probe_proof
    }

Observed constants are:

- artifact_schema_version = state-backend-candidate-health-samples/v1;
- population_contract = common-resource-v2-nominal-and-required-cuts/v1; and
- aggregation = conjunctive-independent-predicates/v1.

There are 1..16 mechanisms and 1..64 signals, each sorted by unique ID. A mechanism contains
mechanism_id, sorted nonempty signal_ids, source_proof with role mechanism-source-proof, and
configuration_proof with role mechanism-configuration-proof. Every reference resolves, every
signal is referenced, and for each
mechanism the union of referenced signal objectives includes backlog_or_in_flight_pressure,
background_failure, and tail_quiescence. One signal may cover multiple mechanisms. Values are never
summed or normalized. The observed object also binds one
inventory_proof descriptor with role maintenance-mechanism-inventory-proof covering the complete
enabled mechanism inventory.

For every mechanism, its referenced `production_minimal` signal subset MUST be nonempty and MUST
cover both backlog_or_in_flight_pressure and background_failure. Tail-quiescence evidence MAY be
qualification-only. Qualification activation samples the production-minimal subset plus any
qualification-only signals; it never substitutes a qualification-only pressure or failure signal
for a production-operability surface. This requirement has no unsupported or per-signal N/A waiver.

A signal contains exactly:

- signal_id and source_contract;
- raw_kind: unsigned_gauge, monotonic_counter, or boolean_state;
- unit: bytes, items, jobs, files, operations, nanoseconds, or boolean;
- scope { kind, instance_id }, where kind is process, database, column_family, keyspace, or store;
- quality: exact, approximate, or estimated;
- activation: production_minimal or qualification;
- sorted unique objectives from the three closed objectives;
- value_semantics identifier;
- sorted nonempty predicates, at most five; and
- content descriptors with roles signal-source-proof, signal-configuration-proof,
  signal-semantics-proof, signal-limitations-proof, signal-overhead-proof, and
  signal-threshold-basis-proof.

Boolean kind requires boolean unit; other kinds prohibit it. Every fixed CF/keyspace/store instance
is flattened into a stable signal ID. Dynamic labels, candidate-defined aggregation, debug
activation, custom evaluator code, and free-form expression languages are invalid.

Threshold-basis proof is mandatory for every signal and must include approved numerical predicate
basis. For approximate/estimated signals it additionally proves direction/error limitations and
the approved safety margin. The validator verifies descriptor identity, not proof truth.

Monotonic counters MUST be monotonic for the candidate process instance. Any decrease, reset,
wrap, or observed u64 saturation is INVALID. Candidate process restart is independently INVALID
unless a proved candidate crash already makes the attempt FAIL. Gauges may legitimately decrease;
booleans encode only 0 or 1.

The five predicate kinds are closed:

1. maximum_upper_bound { population, upper_bound }, for gauge/counter, where population is
   complete, measurement, or measurement_and_tail;
2. counter_delta_upper_bound { population, upper_bound }, for counters, where population is
   complete or measurement_and_tail;
3. required_boolean_state { population, required }, for boolean, where population is complete,
   measurement, measurement_and_tail, or tail_hold;
4. tail_upper_bound { upper_bound }, for gauge/counter; and
5. tail_no_increase, for gauge/counter.

Duplicate predicate kind/population pairs are invalid. Complete means every health record.
Measurement includes every nominal record whose bracket intersects the measured interval plus the
pre-measurement and measured-end cut records. Measurement_and_tail begins at pre-measurement and
ends at stable/deadline cut. Counter delta uses the first and last chronological values of that
population after global monotonicity validation. Tail predicates apply to every nominal/cut record
whose bracket intersects the claimed uninterrupted tail-hold interval. Tail_no_increase requires
each chronological value in that interval to be <= its predecessor; it has no tolerance or hidden
slope. A candidate that needs noise tolerance uses an approved absolute tail bound instead.

Mechanisms, signals, and their reference IDs sort by unsigned UTF-8 byte order. Objectives sort
backlog_or_in_flight_pressure, background_failure, tail_quiescence. Predicates sort in the numbered
kind order above, then complete, measurement, measurement_and_tail, tail_hold where a population
exists. Chronological value order is observation_end, observation_begin, population_tag,
population_index. These orders are wire rules, not implementation-map iteration order.

An objective label is valid only with a compatible predicate on that signal:

- backlog_or_in_flight_pressure requires a measurement or measurement_and_tail maximum bound, or a
  required boolean over one of those populations;
- background_failure requires both maximum_upper_bound {complete,0} and
  counter_delta_upper_bound {complete,0} on a monotonic counter, or a required boolean healthy state
  over complete; and
- tail_quiescence requires tail_upper_bound or required_boolean_state over tail_hold.
  Tail_no_increase is supplementary and never establishes a healthy band by itself.

A label without its compatible predicate is invalid and cannot satisfy mechanism coverage.

The not_applicable constants are:

- reason_code = no-asynchronous-state-storage-maintenance-in-exact-candidate-process; and
- claim_scope = complete-candidate-process.

It requires complete source/configuration proof and a bounded forced probe. There is no per-signal
N/A. Generic writer serialization, synchronous commit/reclaim/resize/repair/close, OS writeback,
and telemetry housekeeping do not make this arm applicable, but all remain charged to common
latency, resource, lifecycle, persistence, fault, and endurance gates. Unsupported is never N/A or
zero.

### Candidate health samples v1

The stream begins with the 28-byte ASCII domain LDB-SBQ-CANDIDATE-HEALTH-V1 plus its terminating
zero byte (included in those 28 bytes), then:

    mechanism_mapping_sha256[32]
    signal_count_u32_be
    record_count_u64_be

The fixed header is 72 bytes. For s signals, each record is 32 + 8*s bytes:

    population_tag_u8 || seven_zero_bytes ||
    population_index_u64_be ||
    observation_begin_offset_ns_u64_be ||
    observation_end_offset_ns_u64_be ||
    raw_value_u64_be[s]

The exact length is 72 + (32 + 8*s)*record_count, checked without overflow before allocation. The
artifact is at most 256 MiB. Signal order is the mapping's sorted signal order.

Records are every common-resource-v2 nominal sample as tag 0x00 with identical contiguous sample
index, followed by cuts 0x10 pre_measurement, 0x11 write_stop, 0x12 last_terminal, 0x13
measured_elapsed_end, and exactly one of 0x14 resource_tail_stable_end or 0x15
resource_tail_deadline. Cut indices are zero. This order is canonical and record_count is exactly
the common nominal sample count plus five.

The health population MUST exactly match common sample/cut tags and indices. Health and common
brackets need not be equal because they are separate off-event-loop reads. For each pair, absolute
begin-offset and end-offset differences MUST each be <= the exact
candidate_health_to_common_max_skew_ns in the approved plan. Each health bracket duration MUST be
<= candidate_health_observation_max_skew_ns. The health read pass uses CLOCK_MONOTONIC_RAW. The
common nominal cadence/lateness gate is authoritative, so a missing, late, duplicated, or extra
health record invalidates the attempt rather than silently reducing cadence.

All values in a record are read inside its bracket. Signals are evaluated independently, so the
record is not represented as an atomic cross-signal snapshot and no formula may combine values.
Candidate-specific coherent-snapshot requirements live in the approved signal semantics proof.
This bounded dense representation deliberately avoids per-signal timestamp rows, a dynamic metric
format, and a general evaluator.

### Health formula, tail, and disposition

Resource-formulas/v3 evaluates each signal predicate independently with checked integers and
conjoins every result. It emits no composite health number and no cross-engine score.

The common resource tail begins at write_stop_offset_ns. A stable-end claim is valid only when
cgroup dirty/writeback and target-device write growth plus every applicable health tail predicate
remain in their approved bands for one uninterrupted resource_tail_hold_ns. The stable-end offset
minus hold MUST not precede write stop. Every health record intersecting that interval participates,
and the complete common cadence/lateness and health/common-skew gates must pass. Reaching
resource_tail_clear_max_seconds before the common plus health conjunction holds is candidate FAIL
and produces the deadline cut. The database/cgroup remain alive through the cut. Close/reopen
happens later and cannot improve the tail.

Before execution, an absent/unsupported required source blocks mapping conformance. During a
well-formed valid attempt, source read failure, frozen threshold breach, required boolean mismatch,
counter delta breach, background failure, pressure-stall breach, failure to hold every tail band,
or tail deadline is candidate FAIL. A false approved N/A claim discovered by attributable async
state maintenance is candidate FAIL and revokes that mapping lineage.

Malformed artifacts, collector/evidence loss, missing populations, clock/sample corruption,
excessive skew/lateness, mapping/configuration drift, unknown/unreferenced signals, boolean
misencoding, counter reset/wrap/saturation, overflow, and hash/population mismatch are INVALID.
Candidate-crash precedence remains: a proved crash cannot be retried away as collector loss.

For a not_applicable arm, a candidate-health artifact MUST be absent. For an observed arm it MUST be
present. Both/neither, a zero-byte substitute, or a descriptor inconsistent with the union is
INVALID.

### Synthetic mechanism bundle v2

Mechanism-bundle-validation-input/v2 is permanently validation-only. Its top-level authority fields
are exactly:

- schema_version=state-backend-mechanism-bundle-validation-input/v2;
- notice=NOT QUALIFICATION EVIDENCE;
- record_class=synthetic_fixture;
- fixture_ineligible=true;
- status=candidate_unapproved;
- qualification_eligible=false; and
- validation_authorizes_execution=false.

It also contains bundle_id, candidate_id, the claimed attempt clock/timeline, validation limits,
target-device identity, and content descriptors for exact profile v4, mapping v2, common samples v2,
common cuts v2, conditionally present candidate-health samples v1, conditionally present stall
intervals v1, and mandatory target-device I/O v1. Descriptor roles, filenames, positive lengths,
nonzero lowercase hashes, and media types are closed. Filenames are single normal path components
and no two descriptors name the same entry.

The clock object contains source=CLOCK_MONOTONIC_RAW, origin_reading_ns,
measured_phase_start_offset_ns, measured_elapsed_ns, write_stop_offset_ns,
last_terminal_offset_ns, and device_capture_end_offset_ns. Limits contain
expected_nominal_resource_samples, resource_observation_skew_max_ns,
candidate_health_to_common_max_skew_ns, and candidate_health_observation_max_skew_ns. Fixture
values exercise formulas but establish no approved numerical limit.

The validator binds all raw bytes by length and SHA-256, applies every strict subordinate parser,
enforces profile/mapping/candidate/timeline/population joins, and reports only
VALID_INELIGIBLE_MECHANISM_BUNDLE with non-authoritative no_adverse_signal or
candidate_failure_signal. Malformed evidence is invalid. It cannot attest the clock, environment,
source-proof truth, owner approval, or candidate execution and never emits PASS, FAIL, approval,
selection, or qualification. It exposes no candidate-execution command.

## Retained stall wire and common formulas

Stall intervals v1 begins with the 27-byte ASCII domain LDB-SBQ-STALL-INTERVALS-V1 plus its
terminating zero byte (included in those 27 bytes), mapping SHA-256, mechanism_count_u32,
measurement start/end, and record_count_u64. Length is 87 + 32*count, checked before allocation,
and the complete artifact MUST NOT exceed 268,435,456 bytes. Each record is:

    mechanism_index_u32 || four_zero_bytes || source_sequence_u64 ||
    start_offset_ns_u64 || end_offset_ns_u64

Intervals are nonempty half-open, intersect measurement, keep actual pre-measurement start, use
contiguous per-mechanism sequence, and sort by start/end/mechanism/sequence. Active-at-end intervals
are censored exactly at measurement end. The formula clips then unions overlaps and ceiling-divides
union_ns*1000/measured_ns. Empty observed is zero only with approved exhaustive coverage. N/A is
typed and omits only this conjunct.

Resource-formulas/v3 retains checked integer rules:

- leaf memory.peak, maximum open FDs, live snapshots, live iterators, and frozen generations are
  direct upper-bound gates against their profile values; sampled memory.current is not substituted
  for memory.peak;
- throughput is floor(oracle-valid completed rows * 1e9 / measured_elapsed_ns);
- achieved-rate permille is floor(oracle-valid completed logical rows * 1000 /
  scheduled logical rows);
- CPU permille uses `cpu_usage_us` at the `pre_measurement` and `measured_elapsed_end` cuts. The
  nondecreasing delta is converted to nanoseconds by checked multiplication by 1,000, then evaluated
  as ceil(candidate CPU delta ns * 1,000,000 /
  (measured_elapsed_ns * cgroup capacity millicores));
- write amplification milli is ceil(target io_write byte delta from pre-measurement through tail *
  1000 / logical mutation bytes);
- sampled space amplification milli is the maximum over every bracket/cut of
  ceil(max(allocated endpoints)*1000/min(logical-live-before, logical-live-after)); either logical
  endpoint being zero makes the attempt INVALID and MUST NOT be filtered from the denominator;
- engine-stall permille is the interval-union formula above;
- target-device maximum milliseconds is ceil(max recorded duration_ns/1,000,000); and
- resource-tail duration is tail cut observation_end - write_stop_offset.

Every plan-owned zero denominator is invalid plan construction; a zero or reset observation where a
runtime denominator is required is INVALID attempt evidence. Device error/incomplete, timeout, and
tail deadline fail independently of numerical maxima. N/A removes only its candidate-native
conjunct. Queue, writer acquisition, service, end-to-end, throughput, memory, disk, I/O, and
lifecycle costs always remain.

The exact memory peak is the new leaf's whole-lifetime memory.peak. Sampled memory.current and cut
values are not interval peaks. The slope population is every nominal sample whose
`observation_end_offset_ns` is at or after
`measured_phase_start_offset_ns + slope_measurement_start_seconds * 1_000_000_000`, with all
products and sums checked. It contains at least two samples with distinct end offsets. The first
included sample in canonical sample-index order is the baseline. For each included sample, `x` is
the floor of `(observation_end - first_included_observation_end) / 1_000_000` in whole elapsed
milliseconds. For RSS, `y=rss_bytes`; for stable disk,
`y=max(allocated_store_before_bytes, allocated_store_after_bytes)`. Compute:

    D = n*sum(x*x) - sum(x)*sum(x)
    N = n*sum(x*y) - sum(x)*sum(y)
    slope_bytes_per_hour = ceil(N*3_600_000/D)

D must be positive. Mathematical signed integers and a pinned arbitrary-precision implementation
are required. Signed ceiling rounds toward worse growth. Stable-disk permille/hour is
`ceil(slope_bytes_per_hour*1000/baseline_bytes)`, where `baseline_bytes` is the nonzero maximum of
the first included sample's two allocation endpoints. RSS total growth is
`max(0, max(included rss_bytes) - first_included_rss_bytes)` over mathematical signed integers and
is gated directly against `rss_growth_total_max_bytes`; it cannot wrap on a falling series. Missing
or late samples, invalid denominators, counter reset, or out-of-schema results are INVALID unless
candidate-crash precedence applies.

## Faults, classification, evidence, and retention

Logical faults retain C1 occurrence addressing and post-success ambiguity. Each immutable physical
fault entry binds ID, paired slot, case/repetition, logical phase/occurrence, process/vnode/table
target, trigger, parameters, expected markers, recovery deadline, and oracle result. Candidate
actuator mappings are separate. Planned, armed, reached, actuated, released, reopened, and recovered
markers are distinct. One candidate-neutral logical physical-fault entry and trigger is instantiated
for both candidates in the pair; only the separately bound actuator mapping may translate that
logical trigger to candidate-specific mechanics. Candidate-specific trigger timing, occurrence, or
severity is forbidden.

The matrix covers process death, scoped quota/loop-device ENOSPC, I/O error, corruption/truncation,
FD pressure, concurrent open, complete local loss plus portable restore, and exact N/N-1 versions.
No tool may fill/corrupt host root or unresolved path. Physical faults use a separate strict
manifest. Exact actuation counts, triggers, cache-loss harness, N/N-1 pins, and recovery criteria
are approved before execution.

Attempt status is derived:

- PASS: complete slot, every artifact/sample verifies, no candidate failure, all gates pass;
- FAIL: candidate/oracle/corruption/error/crash/timeout/overflow, durability/recovery failure,
  numerical/resource/health gate miss, required telemetry unsupported, or actuated-fault failure;
- INVALID: identity/procedure/environment/preflight mismatch, clock regression, evidence loss,
  runner error, undeclared interruption, or scheduled fault not reached/actuated.

After valid measurement start, candidate memory/disk/FD/thermal/NVMe/health/stall/timeout/process
failure is FAIL. Causation guesses cannot relabel it. Invalid attempts never pass. No failed/invalid
slot is deleted or replaced. Campaign status is INVALID if any slot is invalid, otherwise FAILED if
any fails, otherwise COMPLETE only when every slot passes. COMPLETE is not selection or production
approval.

Manifests are bounded JSON referencing objects only at objects/lowercase-sha256 under one resolved
root. Descriptors bind closed role, length, digest, media type, and count. Validators stream local
bytes and reject URI fetch, traversal, symlink/reparse, duplicate/unknown role, missing object,
length/digest mismatch, trailing bytes, or identity drift. Approved readers additionally require
race-free no-follow, handle-relative opens, and opened-file identity verification.

The campaign binds source/binary/lock/SBOM/compiler/target/flags/options/profile/plan, image/packages,
kernel/libc/CPU/microcode/governor/NUMA, cgroup, device/firmware/SMART/scheduler, filesystem/mount,
preflight, schedule/seeds, raw samples, resources, logs, roots/counters, and every result.

After closure, a detached completion record binds the explicit run authorization, all exact inputs/
binaries, manifest length/digest, schedule, validator-derived status, immutable object version/
retention, UTC time, runner identity, and independent-review provenance. The independent validator
recomputes all classification and digests. Selection consumes the completion digest, never a mutable
path.

Object lock is at least 365 days after completion and before selection extends through product
support sunset plus 365 days. Availability is checked at least every 30 days and retained for pass,
fail, and invalid lineages. Any semantic/input/environment change creates a new lineage.

## Validation-only implementation acceptance tests

These tests are requirements for the Cycle 38-authorized validation-only implementation. They do
not authorize candidate construction or execution.

1. Exact-delta tests transform decoded v3/profile bytes only by the three permitted v4 changes,
   compare every remaining value, and reject v4 with any extra numerical or candidate change.
2. Strict JSON tests reject duplicate/unknown keys, unknown enum values, placeholders, noncanonical
   IDs, non-u64 numeric values, all-zero/uppercase/wrong-length hashes, missing descriptors,
   trailing JSON, over-limit input, and mapping/profile hash drift.
3. Mapping graph tests reject unsorted/duplicate mechanism, signal, objective, reference, and
   predicate arrays; dangling or unreferenced signals; uncovered per-mechanism objectives; dynamic
   scopes; any mechanism whose production-minimal subset does not cover both pressure and background
   failure; debug signals; wrong kind/unit or kind/predicate combinations; more than 16 mechanisms,
   64 signals, or five predicates; and custom expression/aggregation fields.
4. N/A tests require exact complete-process reason/scope/proof roles, prohibit partial N/A, require
   health artifact absence, and prove that common resources, stall applicability, device I/O,
   lifecycle, and latency remain mandatory.
5. Health-wire goldens cover zero and maximum legal raw values, booleans 0/1, all five predicates,
   observed exact/approximate/estimated qualities, canonical record order, both stable/deadline
   tails, and independently calculated expected disposition. Boolean 2, counter decrease/reset/
   wrap/saturation, arithmetic overflow, wrong signal count/hash, truncation, trailing bytes,
   nonzero reserved bytes, and over-256-MiB declared length are rejected. Separate stall-wire length
   tests accept the greatest record count whose exact `87 + 32*count` length is at most 256 MiB and
   reject the next count and any declared or actual cap-plus-one byte before allocation.
6. Cross-artifact tests require exact nominal/cut tag/index populations; exercise both sides and the
   exact boundary of bracket/skew limits; reject missing, duplicate, late, future, or extra records;
   and verify checked clock-origin/measurement/tail chronology.
7. Formula tests independently verify every retained throughput/CPU/amplification/stall/device/
   slope formula, conjunctive health predicates, counter populations/baselines, tail-hold
   intersection, candidate-failure signals, and invalid-evidence precedence. No predicate may
   suppress a common gate failure.
8. Mixed-lineage tests reject v3, mapping v1, debt v1, formulas v2, bundle v1, translated fixtures,
   unknown versions, and every cross-version combination.
9. Bounded streaming/property/fuzz tests cover exact length arithmetic, adversarial counts,
   truncation at every fixed field, deterministic errors, fixed parser memory, and no panic.
   Benchmarks verify the parser's declared buffer/record ceilings; they are not hot-path evidence.
10. Authority/CLI tests prove synthetic-only success text, absence of PASS/qualification claims,
    absence of a candidate run command, and rejection of any self-approval field.
11. Approved-reader tests, before real artifacts are permitted, exercise no-follow handle-relative
    opens, opened-file identity, replacement races, symlink/reparse/path traversal, mount crossing,
    duplicate object names, length/digest drift, and non-local object references.
12. Every golden is independently regenerated or hand-decoded; copying bytes from the implementation
    under test is not an independent oracle.

## Production boundary

This qualification can compare embedded worker-local working-state primitives. It cannot establish
vnode ownership, checkpoint publication, source position, sink transaction/fence, rebalance,
upgrade, grouped/window/join semantics, end-to-end exactly once, admission, or production readiness.
C3 concurrency, connector capability combinations, distributed lifecycle fault testing, and an
independently operated immutable release-candidate soak remain independent vetoes.

## Required independent contract review

Ordinary repository review must cover both schema/wire/arithmetic/evidence/exact-delta correctness
and operations/resource/hot-path/production-boundary correctness. It needs no named-person receipt,
protected provider event, or signature. Review stops on any unresolved placeholder, cross-document
conflict, unbounded parser or hot-path operation, fail-open or ambiguous N/A rule, unchecked
arithmetic/wire ambiguity, digest drift, self-authority, or weakened exactly-once, production, or
independent-soak gate. An unresolved stop remains a veto.

## Resolved validation-contract decisions

Cycle 38 freezes these decisions for validation-only implementation:

1. accept the exact 7,838-byte v4 profile and independently reconstructed three-change delta above;
2. after setup persist/close/reopen and independent setup verification both succeed, require the
   first gate-bearing common bracket and paired candidate-health bracket before the first warmup
   mutation, then keep both uninterrupted through the resource-tail cut;
3. use the four-way threshold-authority split: this contract owns closed types, units, predicates,
   formulas, validity/failure rules, and wire bounds; v4 retains common v3 numerical gates; candidate
   mappings own candidate-native limits, bases, and safety margins; and the future explicitly
   authorized plan owns service, cadence, skew, tail, calibration, occupancy, and observer-overhead
   values, all frozen before results;
4. use ordinary independent technical review with recorded findings instead of PF4 identity and
   immutable-receipt machinery; and
5. accept the project-owner direction and freezing commit instead of PF5 protected workflow,
   detached-signature, or multi-principal approval.

The following are deliberately post-contract, pre-execution blockers. They do not prevent approval
of a neutral validation contract, but validation-only implementation cannot satisfy them. Items 1-3
and 9-12 are universal. Items 4-6 apply only if a later owner reopens exact v4 reference execution;
items 7-8 apply to the preferred TidesDB lineage. No candidate command may exist until its universal
and candidate-branch blockers close; an inactive branch is never a prerequisite:

1. the complete workload/case/rate/order matrix and DKS-Q2-001 through Q2-004 proofs;
2. service latency, scheduler/calibration, ring/lead/lag/occupancy, null-control and telemetry-
   overhead values under DKS-Q2-005;
3. candidate_health_to_common_max_skew_ns,
   candidate_health_observation_max_skew_ns, resource_tail_hold_ns, and every other exact plan
   value named above;
4. for the frozen v4/reference lineage, source-closed mapping signal IDs/scopes/qualities,
   numerical predicates, threshold/safety proofs, and exact configuration for each exact subject;
5. for the frozen v4/reference RocksDB subject, scheduled-low/bottom, purge, background-error/
   recovery, safe-binding, complete pressure-stall sources, and exact purge/recovery policy;
6. for the frozen v4/reference Fjall subject, an explicit scheduler/lifecycle fork/upstream ownership
   decision; stock Fjall 3.1.8 remains unsupported and no active product source work is scheduled;
7. for a future TidesDB lineage, an exact-current lifetime-safe Rust/native integration,
   all-or-nothing apply, strict
   recovery and acknowledgement, immutable read cuts, cgroup-aware resource controls, and complete
   pressure-stall/background-error sources;
8. a new TidesDB profile plus successor mapping/profile-binding/bundle identities or contract after
   those closures; v4 remains immutable reference data;
9. exact plan/evidence/approval/completion schemas and validators, including hostile artifact
   handling;
10. exact target-device tracer program/source/build, attach points, map capacity/layout, issue-state
   bound, atomic operations, capture lifecycle, saturation/loss handling, and observer-overhead
   controls under DKS-Q2-005/006;
11. physical persistence/cache-loss, C3 concurrency, and bounded 24/72-hour endurance contracts; and
12. every source, mapping, execution, selection, integration, exactly-once, and independent-soak
   approval.

No unresolved numerical value may be inferred from v1 fixtures, candidate defaults, paper mappings,
prior runs, or another backend. Each later value is frozen in its named plan/mapping/approval bytes
before results and changes create a new lineage.

## Explicitly rejected expansion

This lineage does not add a metrics DSL, weighted score, dynamic plugin, candidate-defined
aggregation, per-signal timestamp row format, remote metrics service, compatibility converter,
runtime state backend, native dependency, observer, adapter, or execution command. Candidate health
is a veto; common correctness, offered latency, throughput, resources, persistence, fault, recovery,
and endurance remain the comparison surface.

Every later implementation cycle ends with independent AI-slop, overengineering/hot-path,
unused-code, production-readiness, documentation, and test review.

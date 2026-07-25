# State backend maintenance-health contract v2 — proposal

- **Status:** Direction approved on 2026-07-24; consolidated validation contract accepted by Cycle 38
- **Date:** 2026-07-24
- **Scope:** the background-maintenance part of DKS-Q2-006 only
- **Recorded direction:** additive v2 design approved; protected-workflow approval superseded
- **Worker-local product target:** TidesDB through official `tidesdb/tidesdb-rs`, published as Cargo
  package `tidesdb`; production admission remains **NO-GO**
- **Execution authorized:** no
- **Cluster admission:** unchanged; `[LDB-4007]` and `[LDB-0013]` remain fail-closed
- **Predecessor:** [state backend qualification runner v1](state-backend-qualification-runner-v1.md)
- **Trigger:** [Cycle 17 RocksDB source closure](../reports/rocksdb-mechanism-source-closure-2026-07-24.md)
- **Paper mappings:** [Cycle 19 candidate designs](../reports/state-backend-maintenance-health-mapping-designs-2026-07-24.md)
- **Direction record:** [Cycle 21 owner decisions](../reports/distributed-state-cycle-21-owner-decisions-2026-07-24.md)
- **Stage 3 output:** [consolidated runner v2 validation contract](state-backend-qualification-runner-v2-draft.md)
- **TidesDB successor design:** [official-binding local-state design](tidesdb-local-state-successor-design.md)
- **TidesDB T0 evidence:** [exact-package source closure](../reports/tidesdb-rs-t0-source-closure-2026-07-25.md)

Cycle 19 confirms that sampled typed gauges/counters/booleans can express the continuous health
obligations, while exposing additional candidate gaps. It keeps RocksDB's blocking tail wait
non-gating rather than disguising an active command as a sampled boolean. The mappings do not
instantiate v2, rank implementation cost, or authorize source work. On 2026-07-24 the project owner
recorded `APPROVE_MAINTENANCE_HEALTH_V2_DIRECTION`. That decision authorizes Stages 2 and 3 below;
Cycle 38 later accepts the consolidated validation contract without a protected approval workflow.

## Cycle 41 current-state reconciliation

The [consolidated runner](state-backend-qualification-runner-v2-draft.md) is the sole normative
successor design only for immutable v4 Fjall/RocksDB validation/reference semantics. This proposal
records that v1 -> v2 rationale and cannot override it. The
[TidesDB package design](tidesdb-local-state-successor-design.md) is authoritative for the later
one-CF/fresh-restore successor roles. Exact official Cargo package `tidesdb v0.11.1`/native 9.3.6
failed Cycle 41 T0: the safe public surface lacks mandatory exact
stall/background-error/cleanup/reaper facts, in addition to missing later native fixes, ambiguous
batch acknowledgement, and an unclosed general cgroup envelope. T1 and successor identities are
closed pending a new official package and a repeated T0. Redb is PARKED outside v4 and its older
paper mapping is vocabulary provenance, not scheduled work.
References below to exact schemas mean complete normative v4 wire and schema semantics in the
contract, not pre-approval JSON Schema or validator implementation.
Candidate-native numerical thresholds and margins belong to immutable candidate mappings; service,
cadence, skew, tail, calibration, occupancy, and observer-overhead values belong to the approved
plan. Detached approval binds those values before results.

For each observed maintenance mechanism, production-minimal signals must expose both backlog or
in-flight pressure and background failure. Tail-quiescence instrumentation may remain
qualification-only. This closes the proposal's production-operability intent without moving source,
adapter, observer, execution, or admission authority.

## Proposed decision

Replace only v1's universal, exact `background_maintenance_debt` byte scalar with a reviewed,
candidate-specific `background_maintenance_health` arm. Keep the exact foreground pressure-stall
arm and every common correctness, latency, throughput, resource, device-I/O, pressure, persistence,
recovery, fault, and endurance gate.

The health arm is an absolute operational veto, not a candidate score. Each enabled state-storage
maintenance mechanism must have the smallest sufficient set of typed signals for backlog or
in-flight pressure, background failure, and tail quiescence. Each signal is evaluated under its own
precommitted semantics and threshold. Unlike units are never normalized, summed, or ranked across
engines.

For this successor, background state-storage maintenance means asynchronous engine work that
creates, transforms, flushes, compacts, vacuums, recovers, garbage-collects, or reclaims
working-state data or its state-bearing metadata, WAL, manifest, and data-file lifecycle, including
asynchronous cleanup. It also covers engine pressure states caused by that asynchronous work which
throttle, block, or reject foreground state access. Generic writer serialization and synchronous
maintenance remain common queue/service/C3 evidence rather than making this arm applicable. Engine
logging and telemetry housekeeping are outside the candidate-native arm; their CPU, memory, I/O,
disk, and latency effects remain inside the mandatory common observations. This deliberate scope
change is one reason v1 cannot be reinterpreted in place.

The approved direction authorized the historical three-mapping design input and two-candidate
consolidated-contract drafting stage. Cycle 38 now authorizes only its validation implementation;
native source construction, adapters, and candidate runs retain separate gates. Cycle 19 supplies
the Stage 2 paper mappings, and the Stage 3
[consolidated validation contract](state-backend-qualification-runner-v2-draft.md) is authoritative
for immutable v4 validation/reference semantics only.

## What does not change

Within the v1 -> v2 v4 reference lineage, the successor retains these requirements without
weakening them. An accepted official TidesDB package may reuse only byte-and-semantics-identical
wires after a field-by-field ledger; its one-CF layout, physical-fault, persistence, and fresh-
restore semantics require new lineage:

- validation and synthetic fixtures authorize no candidate execution;
- an exact immutable profile, source, build, configuration, adapter, runner, and approval preimage;
- open-loop offered load and separate queue/service/end-to-end latency, with no outlier deletion or
  metric subtraction;
- the C1 storage model, workload v2, common resource samples/cuts v2, latency samples v1, physical
  layout/fault contracts, and portable snapshot/export/restore semantics;
- common XFS project-quota, cgroup CPU/memory/dirty/writeback/device-I/O, process-tree PSS/RSS/FD,
  target-device latency, disk growth, write/space amplification, snapshot/iterator/generation, and
  resource-tail observations;
- exhaustive, loss-detecting foreground engine-pressure stall intervals and their interval-union
  gate when that mechanism exists;
- fail-closed unsupported-versus-N/A semantics, attempt precedence, artifact retention, and
  independent review; and
- separate checkpoint, vnode ownership/rebalance, source/sink delivery, exactly-once, and product
  soak authorities.

The common gates remain the only cross-engine comparison surface. Candidate-native health can veto
a candidate but cannot compensate for or improve a common result.

## Identity and migration boundary

The semantic change is additive. The following identities are reserved by this proposal but are not
instantiated by a direction approval:

| Item | Proposed identity | Reason |
|---|---|---|
| Runner contract | `state-backend-runner-contract/v2` | Changes the maintenance-health obligation and formulas |
| Comparison profile | `distributed-state-qual/v4` / `linux-nvme-v4` | Removes the universal debt threshold; no other numerical change is implied |
| Candidate mapping | `state-backend-mechanism-mapping/v2` | Replaces the debt arm with typed health signals |
| Candidate health samples | `state-backend-candidate-health-samples/v1` | New independently evaluated signal population |
| Resource formulas | `state-backend-resource-formulas/v3` | Removes debt sum/maximum/tail conjuncts and adds health predicates |
| Synthetic bundle | `state-backend-mechanism-bundle-validation-input/v2` | Binds v4, mapping v2, health samples, stalls, common resources, and device I/O |

This document is a delta proposal. At proposal time it expected a later signed workload/operations
approval to freeze one consolidated contract. Cycle 38 supersedes that ceremony: project-owner
direction, ordinary technical review, and the freezing commit now authorize validation-only
implementation of the complete wire/schema/formula and threshold-ownership contract. Exact
candidate- and plan-owned numbers remain frozen separately before execution. An executable plan
must not depend on readers mentally composing two mutable Markdown files. V1's
normative semantics remain unchanged, but its document contains chronological amendments and is not
claimed to have an immutable file hash.

Unchanged byte contracts retain their versions: latency samples v1; common resource samples/cuts
v2; stall intervals v1; target-device I/O v1; physical layout/fault/attempt-classification v1; C1
model v1; and workload v2. The reserved plan, evidence, approval, and campaign-completion v1 schemas
have produced no real artifacts and may remain only if their eventual schemas bind subordinate
versions explicitly; if they hard-code runner-contract v1, they must be bumped before use rather
than guessed here.

Profile v3, mapping v1, maintenance-debt samples v1, resource formulas v2, and bundle v1 remain
immutable, execution-ineligible regression fixtures. A v2 validator must accept only v4 and mapping
v2 and reject debt-v1 artifacts, v3 profiles, converted fixtures, mixed versions, and translation
attempts. There is no compatibility adapter and no reinterpretation of old bytes.

Profile v4 is proposed to differ from v3 only by new identity/provenance values and removal of
`background_maintenance_debt_max_bytes`. All other field values and gate semantics remain equal
until their separately named blockers are approved. Exact-delta tests must enforce that claim.

## Candidate mapping v2

The replacement union is:

```text
background_maintenance_health =
    observed {
        complete enabled state-maintenance mechanism inventory,
        sorted nonempty signal inventory,
        candidate-health-samples/v1,
        source/configuration/semantics/limitations/overhead proofs,
        frozen per-signal measurement and tail predicates
    }
  | not_applicable {
        no asynchronous state-storage maintenance in the exact build/configuration,
        complete-candidate-process scope,
        source/configuration/bounded-probe proofs
    }
```

Mechanisms and signals are separate. A mechanism references one or more signals, and one signal may
cover multiple mechanisms when its source semantics say so. Overlap is permitted because signals
are never summed. Every enabled mechanism must be covered, every signal must be referenced, and the
complete mapping must cover backlog or in-flight pressure, background failure, and tail quiescence.

Each signal declares:

- stable signal and mechanism IDs plus exact engine/build/configuration identity;
- raw kind from a small closed set: unsigned gauge, monotonic counter, or boolean state;
- unit from a small closed set such as bytes, items/jobs/files, operations, nanoseconds, or boolean;
- process/database/column-family/store scope, with every fixed instance flattened into a stable
  signal ID rather than using a candidate-defined cross-instance aggregation;
- exact, approximate, or estimated quality, including valid configuration domain and known
  direction/error limitations;
- value semantics, cadence, observation brackets/clock, atomicity, reset/wrap/saturation behavior,
  activation level, and collection overhead;
- source/configuration/semantics/overhead proof descriptors; and
- one or more closed predicates: maximum upper bound, counter-delta upper bound, required boolean
  state, tail upper bound, or no-increase-during-tail.

Do not add a general expression language. If the three mapping designs cannot be represented by
this closed vocabulary, owners review a concrete additional primitive before schema work; later
exact mapping artifacts cannot inject a custom evaluator.

Approximate and estimated signals stay labelled as such. Their threshold basis and safety margin
must be approved before results exist. Passing such a threshold is only evidence that the native
health predicate held; it does not prove exact remaining work, spare capacity, or equivalence to
another candidate. Common offered-load, latency, resource, growth, amplification, and tail gates
provide the candidate-neutral capacity evidence.

## Formula and fail-closed rules

Every health signal is evaluated independently and all required predicates are conjoined with the
unchanged common and stall gates. The successor never produces a composite health number.

An absent or unsupported source blocks candidate conformance before execution. If a candidate that
declared an approved source cannot produce the required telemetry during a valid attempt, that is a
candidate failure. Malformed external artifacts, collector/evidence loss, clock or sample
corruption, source/configuration drift, unknown or unreferenced signals, late/excessive-skew
sampling, population/hash mismatch, and unexpected reset/wrap/saturation/overflow invalidate the
attempt. The existing candidate-crash precedence remains authoritative so a proved crash cannot be
retried away as collector loss.

A well-formed attempt is a candidate failure for any frozen health-threshold breach, background
error, pressure-stall breach, failure to reach every tail-bearing healthy band for the uninterrupted
hold, or tail deadline. A below-threshold estimate cannot suppress a common latency, resource,
device, pressure, growth, or error failure.

The common resource tail always applies. Every gate-bearing candidate-health signal must provide
its complete frozen nominal and required-cut population; collection is never opportunistically
omitted. An expensive source that scans state, takes a database-wide writer, allocates without a
fixed bound, or materially perturbs scheduling cannot appear in the qualification health artifact.
It may be a debug diagnostic, but the candidate still needs a cheap qualifying source for every
mandatory health objective.

## N/A and backend fairness

N/A applies only to the complete maintenance-health arm. It requires complete-process
source/configuration proof and a bounded forced probe showing that the exact build has no
asynchronous state-storage maintenance mechanism. There is no per-signal N/A and unsupported is
never encoded as zero.

N/A omits only candidate-native maintenance-health samples. It does not waive adapter queue/service
latency, writer acquisition, foreground pressure stalls, memory, disk, write amplification,
device I/O, pressure behavior, snapshot/restore, error, fault, endurance, or resource-tail gates.
Synchronous maintenance and lock contention are charged to the operations that experience them.
OS page-cache/writeback behavior is charged to common cgroup/device observations.

Consequently, redb cannot win because an LSM-specific mechanism is absent. Its single global writer,
commit/fsync and reclamation work, pinned-reader behavior, file growth, recovery/repair, and
hot-writer/victim tails remain vetoes. Likewise, an LSM does not receive credit for having more
metrics; the signals exist to show it remains operable under the common workload.

## Hot-path and production telemetry profiles

The contract distinguishes two gate-bearing activation profiles without requiring every production
deployment to run qualification-cost statistics continuously:

- `production-minimal`: bounded counters/gauges and operational alerts, with an allocation-free
  normal-request update path and no per-key or unbounded vnode labels;
- `qualification`: production-minimal plus the loss-detecting monotonic-clock stall and health
  observations required by the approved mapping; and
- `debug` is outside mapping and gate artifacts: expensive statistics, full scans, or
  high-cardinality diagnostics can never satisfy a qualification gate.

Gate-bearing reads run off the event loop. No per-row FFI call, metric query, allocation, lock, I/O,
or task spawn is allowed. Synchronous slow-path callbacks use preallocated bounded storage, never
unwind across FFI, and make overflow/loss invalidate evidence. Paired telemetry-on/off controls with
identical workload identities must measure throughput, every gate-bearing latency percentile and
maximum, CPU, memory, and observer-resource impact; numerical limits remain a DKS-Q2-005 owner
decision and are not invented by this proposal.

## Candidate implications

- **[Official `tidesdb/tidesdb-rs` binding: Cargo package `tidesdb v0.11.1` with native
  9.3.6](tidesdb-local-state-successor-design.md):** the project owner selected the official binding
  as the worker-local implementation line;
  production admission remains closed. The restricted facade permits one fixed prefixed CF, one
  dedicated owner lane, copied values, transaction-scoped iterators, and deterministic child-before-
  parent shutdown. No callbacks, private FFI, raw handles, patch/fork, native/system-library
  substitution, or unsafe workaround is permitted. The initial profile uses an exclusive new
  directory restored only from portable Laminar artifacts; native existing-directory state,
  checkpoint, remote storage, `FULL` durability, and strict native replay stay outside the product
  surface. Cycle 41 T0 passes restricted owner/lifetime containment but stops the exact release on
  relevant missing native fixes, acknowledged partial transactions, host-based memory resolution,
  and missing required stock health signals. A future verified-commit/fail-stop protocol may address
  only transaction acknowledgement and must pass its latency/fault gates. T1 is cancelled. A new
  official package must repeat T0 before any successor mapping. Immutable cuts, concurrency, latency, faults,
  delivery, qualification, and independent soak remain mandatory, and v4 cannot be relabelled.
- **[RocksDB 10.4.2](../reports/rocksdb-mechanism-source-closure-2026-07-24.md):** frozen v4/reference
  provenance only. The historical design would first choose the
  smallest paper-mapped set covering the fixed objectives from
  its typed estimate, pending/running state, L0/file pressure, progress, and background-error
  surfaces; keep other cache/memtable/SST/version/cleanup properties diagnostic unless a concrete
  objective requires them. Do not relabel any estimate as exact debt. The known complete-pressure
  stall gap still appears to need a bounded WBM/controller slow-path observer and safe bindings,
  but its mapping design may identify additional source or binding work.
- **[Fjall 3.1.8](../reports/state-backend-static-audit-2026-07-23.md):** frozen v4/reference
  provenance only. The exact-debt requirement
  goes away, but its stable public pressure/progress, error, resource-control, and stall surface is
  still insufficient. The historical mapping identified a potential telemetry/control patch, but
  no Fjall fork, patch, source closure, or adapter is scheduled.
- **[redb 4.1.0](../reports/redb-4.1.0-prescreen-mechanism-note-2026-07-23.md):** PARKED outside v4
  after the Cycle 34 design timebox. Its archived N/A design informed the closed vocabulary, but no
  prescreen, profile, adapter, or execution is scheduled. Reopening requires a new bounded charter.
- **[SurrealKV 0.21.2](../reports/state-backend-static-audit-2026-07-23.md):** the
  snapshot-retention correctness defect remains disqualifying regardless of telemetry-contract
  choice.

No candidate proceeds by elimination or receives a weighted score.

## Implementation stages and approval boundaries

1. **Freeze the direction — complete.** On 2026-07-24 the project owner chose the v2 design direction.
   This originally authorized only Stages 2 and 3 design work. Cycle 38 later authorized
   validation-only implementation through ordinary technical review and the freezing commit.
2. **Draft mappings on paper — complete historical input.** The RocksDB, Fjall, and redb designs
   confirmed the closed vocabulary. Only RocksDB and Fjall remain v4 comparison inputs; the redb
   design is archived provenance and authorizes no further work.
3. **Freeze the complete contract — complete in Cycle 38.** Incorporate the reviewed designs into one
   consolidated runner contract with exact wire/schema and formula semantics plus explicit mapping-
   owned and plan-owned threshold boundaries. Ordinary technical review and the freezing commit
   record the validation lineage. A later explicit run authorization must bind exact mapping, plan,
   candidate, target, isolation, limits, and cost before results. Any later semantic change creates
   a new lineage.
4. **Design the official-binding TidesDB successor — complete in Cycle 40.** The design binds exact
   Cargo package `tidesdb v0.11.1`/native 9.3.6 as the prescreen subject, a restricted one-CF facade,
   portable fresh restore, immutable logical cuts, cgroup governance, stock maintenance health,
   successor-lineage roles, hard upstream-wait stops, and independent soak without adding, building,
   or executing the dependency. RocksDB and Fjall retain immutable v4/reference value but have no
   scheduled source or adapter work. redb remains parked.
5. **Run T0 source/safe-subset closure — complete with STOP in Cycle 41.** Crate/tag/nested-archive
   attribution and restricted ownership containment pass; relevant later native fixes, exact or
   verified success, the general cgroup contract, and stock health gates fail. No machine work or
   dependency followed.
6. **Wait for a new official package; T1 is not authorized for v0.11.1.** Its native payload must
   contain every relevant later fix. The repeated T0 must prove exact transaction success or accept
   a Laminar verified-commit/fail-stop protocol with explicit hot-path/fault gates, and must close
   the resource/health contract. Only a complete pass may spend at most two working days/four machine-hours building and
   exercising that exact package in a disposable workspace. A T1 pass would authorize only a
   successor mapping/profile proposal, not runtime use.
7. **Implement only successor-required validation and qualification work.** Add genuinely reusable
   parsers/evaluators, bounded readers, negative-capability tests, and the new mapping/profile after
   T0/T1. Retain exact v3→v4 regression coverage but do not instantiate unused v4-only containers.
   Any artifact reader still needs race-free no-follow, handle-relative opens, opened-file identity
   verification, strict cross-reference checks, and bounded streaming. Then, only after all DKS-Q2
   approvals, add the restricted adapter and run the logical/C2/C3,
   physical-fault, portable-recovery, and 24/72-hour evidence on a frozen Linux/XFS/NVMe successor
   profile. Failure activates no fallback code.
8. **Integrate and certify separately.** The managed vnode lifecycle, grouped aggregates, windows,
   joins, connector delivery, exactly-once combinations, and independent release-candidate product
   soak remain later, separate release gates.

Every implementation cycle ends with the six-pass AI-slop, overengineering/hot-path, unused-code,
production-readiness, documentation, and test review.

## Evidence behind the proposal

Official production surfaces expose typed signals with native meanings rather than one universal
maintenance quantity:

| Source | Positively documented evidence | LaminarDB inference |
|---|---|---|
| [Flink 2.3 configuration](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/deployment/config/) and [checkpoint monitoring](https://nightlies.apache.org/flink/flink-docs-release-2.3/docs/ops/monitoring/checkpoint_monitoring/) | RocksDB property gauges can be column-family scoped, while DB statistics have their own database scope; the pending-compaction-bytes property is explicitly a level-only estimate. Approximate memory, job/state/count statistics, and optional native metrics retain their scope and documented performance cost; checkpoint latency is monitored separately | Preserve signal scope/quality and gate end-to-end outcomes separately |
| [RisingWave operational guide](https://docs.risingwave.com/performance/metrics) and [source reference pinned at `369b431`](https://github.com/risingwavelabs/risingwave/blob/369b431202ba538640a750ba87edbd5f9b78e343/docs/metrics/reference.md) | Pending bytes, L0/write-stop, upload memory, version/vacuum/event backlog, object-store failures, cache, and barrier stages are distinct operational signals | Use sustained typed predicates, not a sum across different failure modes |
| [Materialize metric data pinned at `e813cf0`](https://github.com/MaterializeInc/materialize/blob/e813cf09b35e1609e366d4ab357b2290c327a70c/doc/user/data/metrics.yml) | Queue/dropped/failure/concurrency/time measures for compaction, GC steps, storage latency, freshness, and backpressure retain their native units and caveats | Detailed maintenance health supplements latency/freshness; it does not replace them |
| [Spark 4.2 state](https://spark.apache.org/docs/4.2.0/streaming/apis-on-dataframes-and-datasets.html) and [tagged provider source](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/RocksDBStateStoreProvider.scala) | `StateOperatorProgress` reports trigger-level state/timing data. Provider custom metrics mix commit-phase values with native RocksDB statistics/properties under their own semantics; enabling total-row-count tracking adds a lookup on the write path | Activation, scope, and measured overhead are part of a signal contract |
| [Kafka Streams 4.3 monitoring](https://kafka.apache.org/43/operations/monitoring/) | Cumulative stall/compaction/flush statistics and current approximate/estimated RocksDB properties have different cadence and recording overhead | Do not convert cumulative sampled statistics into fictitious exact intervals |

The RisingWave source and Materialize metric inventory above are pinned to the reviewed 2026-07-24
repository commits. The rolling RisingWave operations page is navigational evidence only. A real
candidate mapping must pin every source/configuration/semantics/overhead proof by commit, archive,
or content digest in its approval preimage rather than inheriting these design-reference pins.
These pages prove what their public monitoring guidance exposes; lack of a documented universal
scalar is not proof that no internal implementation has one. The design inference is that common
outcomes plus honest native health signals are a more defensible production gate.

[SILK](https://www.usenix.org/conference/atc19/presentation/balmau) shows that foreground,
flush, and compaction interference drives LSM tail latency and that throughput-oriented policies can
worsen tails. The [RocksDB production-experience paper](https://www.usenix.org/system/files/fast21-dong.pdf)
adds resource allocation, format compatibility, recovery, and early error detection as independent
operational concerns. These results support retaining pressure p99/p99.9, resource, durability, and
error gates rather than treating any debt or health signal as sufficient.

## Alternatives

### Retain v1 and fund exact bookkeeping

Valid if the organization treats a direct, pairwise-disjoint outstanding-work byte population as a
hard requirement and accepts a maintained engine fork plus hot-path/scheduler risk. RocksDB and
Fjall remain blocked until their exact sources close. redb remains parked outside the comparison;
even a future prescreen could not select it without a later profile and the same common evidence.

### Relabel the RocksDB estimate as exact

Rejected. Its source semantics do not meet v1; relabelling would corrupt the evidence contract.

### Drop maintenance health and rely only on latency

Rejected. A finite latency run can miss accumulating maintenance, reclamation failure, imminent
write stops, and operationally invisible background errors.

### Build a generic metrics DSL or weighted health score

Rejected. It expands implementation and review surface, encourages incomparable scoring, and makes
candidate plugins part of the verdict engine. The closed predicates above plus common gates are
sufficient for the first concrete mappings.

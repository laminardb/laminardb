# State backend maintenance-health contract v2 — proposal

- **Status:** Direction approved on 2026-07-24; consolidated freeze candidate and later final
  two-owner contract approval required
- **Date:** 2026-07-24
- **Scope:** the background-maintenance part of DKS-Q2-006 only
- **Recommendation:** approve the additive v2 design direction described here
- **Production backend selected:** none
- **Execution authorized:** no
- **Cluster admission:** unchanged; `[LDB-4007]` and `[LDB-0013]` remain fail-closed
- **Predecessor:** [state backend qualification runner v1](state-backend-qualification-runner-v1.md)
- **Trigger:** [Cycle 17 RocksDB source closure](../reports/rocksdb-mechanism-source-closure-2026-07-24.md)
- **Paper mappings:** [Cycle 19 candidate designs](../reports/state-backend-maintenance-health-mapping-designs-2026-07-24.md)
- **Direction record:** [Cycle 21 owner decisions](../reports/distributed-state-cycle-21-owner-decisions-2026-07-24.md)

Cycle 19 confirms that sampled typed gauges/counters/booleans can express the continuous health
obligations, while exposing additional candidate gaps. It keeps RocksDB's blocking tail wait
non-gating rather than disguising an active command as a sampled boolean. The mappings do not
instantiate v2, rank implementation cost, or authorize source work. On 2026-07-24 the project owner
recorded `APPROVE_MAINTENANCE_HEALTH_V2_DIRECTION`. That decision authorizes Stages 2 and 3 below;
it is not the final contract approval.

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

The approved direction authorizes only the formal three-candidate mapping-design and consolidated-
contract drafting stages. It does not instantiate a v2 identity or authorize schema implementation,
native source construction, an adapter, or a candidate run. Those later steps retain the separate
gates below. Cycle 19 supplies the Stage 2 paper mappings; Cycle 21 may now prepare the Stage 3
consolidated freeze candidate for independent and owner review.

## What does not change

The successor retains these v1 requirements without weakening them:

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

This document is a delta proposal. After mapping-design review, a later signed workload-owner and
operations-owner approval must freeze `state-backend-runner-contract/v2` as one consolidated,
complete contract incorporating the retained requirements and this delta, including exact schemas,
formulas, and numerical thresholds. Only that final approval instantiates the v2 identity and may
authorize validation-only implementation. An executable plan must not depend on readers mentally
composing two mutable Markdown files. V1's normative semantics remain unchanged, but its document
contains chronological amendments and is not claimed to have an immutable file hash.

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

Candidate mappings distinguish three activation profiles without requiring every production
deployment to run qualification-cost statistics continuously:

- `production-minimal`: bounded counters/gauges and operational alerts, with an allocation-free
  normal-request update path and no per-key or unbounded vnode labels;
- `qualification`: production-minimal plus the loss-detecting monotonic-clock stall and health
  observations required by the approved mapping; and
- `debug`: expensive statistics, full scans, or high-cardinality diagnostics which cannot satisfy a
  qualification gate.

Gate-bearing reads run off the event loop. No per-row FFI call, metric query, allocation, lock, I/O,
or task spawn is allowed. Synchronous slow-path callbacks use preallocated bounded storage, never
unwind across FFI, and make overflow/loss invalidate evidence. Paired telemetry-on/off controls with
identical workload identities must measure throughput, every gate-bearing latency percentile and
maximum, CPU, memory, and observer-resource impact; numerical limits remain a DKS-Q2-005 owner
decision and are not invented by this proposal.

## Candidate implications

- **[RocksDB 10.4.2](../reports/rocksdb-mechanism-source-closure-2026-07-24.md):** first choose the
  smallest paper-mapped set covering the fixed objectives from
  its typed estimate, pending/running state, L0/file pressure, progress, and background-error
  surfaces; keep other cache/memtable/SST/version/cleanup properties diagnostic unless a concrete
  objective requires them. Do not relabel any estimate as exact debt. The known complete-pressure
  stall gap still appears to need a bounded WBM/controller slow-path observer and safe bindings,
  but its mapping design may identify additional source or binding work.
- **[Fjall 3.1.8](../reports/state-backend-static-audit-2026-07-23.md):** the exact-debt requirement
  goes away, but its stable public pressure/progress, error, resource-control, and stall surface is
  still insufficient. The mapping design must determine whether any telemetry/control patch is
  bounded and worth funding; its size and hot-path cost are unknown.
- **[redb 4.1.0](../reports/redb-4.1.0-prescreen-mechanism-note-2026-07-23.md):** a
  maintenance-health N/A claim is possible only after complete-process proof. Its global writer and
  synchronous work remain common latency/C3/persistence risks, so the separately governed
  prescreen still comes first.
- **[SurrealKV 0.21.2](../reports/state-backend-static-audit-2026-07-23.md):** the
  snapshot-retention correctness defect remains disqualifying regardless of telemetry-contract
  choice.

No candidate proceeds by elimination or receives a weighted score.

## Implementation stages and approval boundaries

1. **Freeze the direction — complete.** On 2026-07-24 the project owner chose the v2 design direction.
   This authorizes only Stages 2 and 3 design work; independent reviewers and the final two-owner
   contract approval remain separate.
2. **Draft three mappings on paper.** Produce RocksDB, Fjall, and redb mechanism/signal/N/A mapping
   designs against pinned intended builds/configurations, with every unproved source or binding gap
   explicit. These are review inputs, not `mapping/v2` artifacts. Use them to confirm the closed type
   and predicate vocabulary; reject a generic DSL or exhaustive property dump.
3. **Freeze the complete contract.** Incorporate the reviewed designs into one consolidated runner
   contract, exact schemas/formulas, and pre-result numerical telemetry-overhead and health
   thresholds. Independent reviewers recheck it; workload and operations owners sign the separate
   final contract approval. Any later semantic change creates a new lineage.
4. **Implement validation only.** Add v4, mapping v2, health-samples v1, formulas v3, and bundle v2
   to the standalone tool. Keep all fixtures synthetic and execution-ineligible. Test strict schema,
   ordering/cross-reference, mixed-version rejection, N/A, estimate labelling, missing/reset/wrap/
   loss/overflow, conjunctive predicates, tail behavior, bounded streaming, and exact v3→v4 delta.
   An approved artifact reader also needs race-free no-follow, handle-relative opens and opened-file
   identity verification; the current trusted-fixture boundary is insufficient.
5. **Close candidate sources under separate authority.** Only after the final contract and a
   candidate-specific source-closure approval, build and adversarially force the RocksDB sources
   identified by its design, including the known complete-stall observer, in an isolated
   exact-source workspace. For Fjall, use its design to decide whether a patch is worth funding; its
   size is not yet known. For redb, complete its separately approved prescreen first.
6. **Freeze the candidate mapping artifact.** After source construction and adversarial activation
   proof, finalize and approve the immutable `mapping/v2` artifact with exact build/configuration
   identity and proof digests. If source proof changes a contract assumption, revise and reapprove
   the contract before producing candidate results.
7. **Resume the existing gates.** Only after all DKS-Q2 approvals add exact candidate adapters, then
   run C1/C2/C3, physical fault, recovery, and 24/72-hour endurance evidence on the frozen Linux/XFS/
   NVMe profile. Select one backend and remove losing adapter/dependency code.
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
Fjall remain blocked until their exact sources close. redb still cannot be selected without its
writer/durability prescreen and common evidence.

### Relabel the RocksDB estimate as exact

Rejected. Its source semantics do not meet v1; relabelling would corrupt the evidence contract.

### Drop maintenance health and rely only on latency

Rejected. A finite latency run can miss accumulating maintenance, reclamation failure, imminent
write stops, and operationally invisible background errors.

### Build a generic metrics DSL or weighted health score

Rejected. It expands implementation and review surface, encourages incomparable scoring, and makes
candidate plugins part of the verdict engine. The closed predicates above plus common gates are
sufficient for the first concrete mappings.

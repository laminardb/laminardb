# Distributed state production-soak charter

- **Status:** Draft; `certification_eligible = false`
- **Applies to:** every cluster source/operator/output/sink scenario proposed for production
- **Parent:** [Phase 0 execution plan](../plans/distributed-keyed-state-phase-0-execution.md)

> This document is not production evidence. No distributed keyed operator is currently admissible,
> no numerical charter has been approved, and no independent release-candidate soak has run.

## Purpose

The soak is a separate production certification gate, not a longer unit test. It must expose
resource leaks, compaction/tail collapse, checkpoint/rebalance errors, stale ownership, and output
divergence using an oracle and execution environment independent of the implementation under test.

A backend qualification soak, Criterion benchmark, canary, weekly checkpoint soak, or
implementer-run integration test cannot satisfy this charter.

## Independence and immutable identity

A certification attempt must satisfy all of the following:

- the system under test is an unchanged release archive SHA-256 or OCI digest; the soak never
  builds LaminarDB;
- deployment chart, rendered configuration, dependency images, charter, oracle, and fault
  controller each have immutable identities and hashes;
- the oracle has no `laminar_*` crate/library dependency and uses public source, sink, checkpoint,
  health, and metrics surfaces only;
- the charter's workload, duration, event count, thresholds, fault schedule, and invalid-run rules
  were approved before the attempt and cannot be overridden at dispatch;
- the operator/reviewer did not implement the state backend or operator being certified, or an
  equivalently independent protected approval is recorded; and
- every attempt, including failures and invalid runs, has a permanent attempt ID and retained
  evidence. There is no silent retry or retry-until-green.

Changing a relevant binary, chart, runtime configuration, dependency, charter, oracle, or fault
controller invalidates previous evidence and requires a complete run from a clean deployment.

## Production-like environment

The frozen charter must identify:

- fixed Linux/kernel/container runtime/Kubernetes versions;
- dedicated CPU, memory/cgroup, local NVMe/filesystem/mount, FD, and network limits;
- at least three LaminarDB processes placed across declared failure domains;
- persistent local working-state volumes which can also be deliberately lost;
- cluster-shared object storage with a run-specific checkpoint prefix;
- replayable/splittable source and durable multiwriter sink instances with production-like
  replication and durability settings;
- Prometheus or equivalent external time-series collection plus node/cgroup/disk telemetry; and
- redacted rendered configuration and dependency topology.

The Helm StatefulSet can seed this deployment, but template rendering is not execution evidence.
The release Dockerfile toolchain must first match the workspace Rust requirement.

## Scenario matrix

Each scenario is certified independently. A green scenario cannot widen another operator, update
mode, connector, or delivery guarantee.

| Scenario | Initial intended path | Current status |
|---|---|---|
| Harness control | Kafka source -> stateless projection -> Kafka append sink | Engineering-only; proves oracle/deployment, never keyed-state production support |
| Grouped aggregate ALO | Kafka source -> grouped `COUNT(*)`/`SUM(Int64)` changed-group append snapshots -> certified durable multiwriter append sink | Blocked by `[LDB-4007]` and Phase 0/1 evidence |
| Fixed event-time window ALO | Certified source -> tumbling window/final append output -> compatible sink | Blocked by `[LDB-4007]` and timer/frontier lifecycle |
| Bounded interval join ALO | Two certified inputs -> inner equi-interval join -> compatible append sink | Blocked by `[LDB-4007]` and co-partitioned join lifecycle |
| Retraction/FullChangelog | Stateful operator -> FullChangelog-capable cluster sink | Blocked; no built-in qualifying sink |
| Exactly-once | Exact-certified source/state/sink combination | Blocked independently by `[LDB-0013]` |

Only scenarios explicitly proposed for a release are run, but every scenario proposed for that
release must pass.

The initial grouped-aggregate scenario is narrower than the general row: each logical group is
routed to one fixed Kafka input partition, `COUNT(*)` is mandatory as its logical state version,
and `SUM` is nullable `Int64` using checked Laminar arithmetic. Each successfully committed input
batch appends one current row per touched group; rows within the batch may be coalesced, so the
oracle accepts legal group-local count prefixes with gaps. Versions must increase within one
writer-authority interval. After a crash, a new fenced interval may replay from the older sealed cut
and therefore append a lower version after an unsealed higher version from its predecessor. The same
version must always carry the same operation identity and bit-identical payload, and provenance must
explain every interval boundary. After the frozen source cut, the exact final version for every
group is mandatory. A full scan/republication of all resident groups per processing cycle is neither
required nor eligible evidence.

## Frozen numerical contract

An eligible machine-readable charter contains no `TBD`, zero, or results-derived threshold. It
specifies at minimum:

- elapsed duration and acknowledged event count, with both minima required;
- event rate, Arrow-batch shape, key/value widths, key cardinality, state size relative to RAM,
  Zipf/hot-key skew, vnode count, and checkpoint cadence;
- p50/p95/p99/p99.9 end-to-end latency and maximum event-loop stall;
- checkpoint align/freeze/export/upload/sink-flush/seal limits and recovery RTO;
- hard encoded envelope/artifact/descriptor/payload bytes; checkpoint chain links/delta depth;
  operators and vnodes per transition; rows, canonical key/state bytes, output-buffer bytes; and
  aggregate plus per-restore staging reservations;
- maximum RSS/PSS/cgroup memory, LSM cache/memtable/journal/native memory, queue bytes/age, local
  bytes, disk utilization, FD count, snapshot/iterator count, frozen generations, timer count,
  checkpoint artifacts, compaction debt, and write amplification;
- accepted steady-state slopes for every retained resource; and
- exact repetition/fault counts and random seed.

Workload and operations owners must approve these values. Baseline measurements can inform their
decision but cannot automatically become the gate.

## External input and output oracle

The producer records one immutable ledger row for every broker-acknowledged input:

```text
run_id, scenario_id, event_id, topic, partition, offset,
event_time, logical_key, logical_values, payload_sha256, acknowledged_at
```

After production stops, the controller freezes exact per-partition source high-watermarks. It waits
for LaminarDB's durable source cut and required post-cut checkpoint progression, then freezes exact
per-partition sink high-watermarks. An incomplete source or sink cut cannot pass.

The independent model derives expected results solely from the ledger and published SQL semantics:

- projection: exact event IDs and values;
- grouped aggregate: required final count/sum per key, every observed version is a legal input
  prefix, and version order is valid within each fenced writer-authority interval;
- fixed window: exact key/window/aggregate rows after the declared watermark and lateness policy;
- bounded join: exact stable left/right event-ID pairs; and
- future changelog: exact signed operations and deterministic operation identity.

The sink reader consumes from the beginning through the frozen boundary and emits expected/actual
digests plus missing, extra, malformed, duplicate, conflicting, and stale-generation records.

For at-least-once, an external duplicate is allowed only when it is bit-equivalent and carries the
same replay-stable logical operation identity. That identity is tied to deterministic emission
causality and cannot be a checkpoint attempt alone. Each record also carries deployment, pipeline,
operator, vnode, assignment-generation, and writer-process provenance. Missing output, state double
application, two different records sharing an operation identity, work admitted after an owner's
authority was fenced, or output beyond the frozen boundary is a failure. A record admitted while
the generation was authoritative is not reclassified as stale merely because a broker acknowledged
it after the fence; it remains subject to the ordinary duplicate/conflict rules. If output metadata
cannot prove these distinctions, that scenario is not certifiable; logs are not a substitute.

## Externally actuated fault schedule

Release bits cannot depend on the test-only checkpoint kill gate. The frozen controller schedule
includes the applicable subset of:

- active-owner, follower, and leader hard process death;
- repeated restart/rejoin and `1 -> 3 -> 2` ownership changes;
- control/shuffle network partition while source and sink remain reachable;
- source broker restart and backpressure;
- object-store latency, timeout, and temporary unavailability;
- local disk pressure/`ENOSPC`, compaction pressure, and state larger than RAM;
- local-volume loss followed by portable restore;
- rolling N/N-1 upgrade and rollback; and
- faults bracketing state mutation/freeze, timer fire, output enqueue, sink flush, durable seal,
  source-cut publication, and assignment activation.

Every scheduled fault must have independent actuation evidence. A missed fault makes the attempt
invalid rather than silently reducing coverage.

## Result classification

`PASS` requires every scenario/oracle/resource/latency/progress/RTO assertion to pass and every
required artifact to exist.

The following are product `FAIL`, never invalid-run excuses:

- missing, extra, conflicting, malformed, or stale-owner output;
- state divergence, crash, corruption, or unrecoverable progress loss;
- numerical latency, resource, checkpoint, or RTO violation; or
- any unexplained LaminarDB anomaly.

`INVALID` is restricted to an unknowable result:

- artifact/charter/oracle/configuration identity mismatch;
- oracle or evidence-collection failure;
- incomplete source/sink boundary;
- scheduled fault not actuated; or
- infrastructure failure outside the declared fault domain which prevents judgment.

Invalid attempts are retained and require a complete rerun; they do not count as passes or disappear
from the attempt history.

## Required immutable evidence

- attempt/run manifest and complete attempt history;
- archive/image/chart/SBOM, charter, oracle, controller, config, and dependency digests;
- rendered deployment resources and redacted configuration hash;
- producer acknowledgement ledger and frozen source cuts;
- complete sink capture and frozen sink cuts;
- normalized expected/actual digests and machine-readable oracle result;
- full node, source, sink, object-store, controller, and Kubernetes event logs;
- Prometheus data or TSDB snapshot plus cgroup/node/disk/FD series;
- fault-controller schedule and observed actuation log;
- checkpoint/object-store inventory and manifest checksums; and
- independent reviewer decision and evidence URI/hash.

The existing GitHub workflow's 14-day node-log artifact is insufficient. A protected manual workflow
may publish a summary to CI, but raw evidence must live in immutable retained storage.

## Draft blockers and sign-off

This charter remains ineligible until all of these are resolved:

- [ ] named workload, operations, soak operator, and independent reviewer;
- [ ] approved target hardware/deployment and all numerical fields;
- [ ] immutable release and standalone-oracle artifact pipelines;
- [ ] machine-readable charter schema and preflight validator;
- [ ] stable output operation identity and assignment-generation evidence;
- [ ] grouped aggregate state path admitted after Phase 1 correctness gates;
- [ ] production-compatible source/object-store/sink environment;
- [ ] external fault controller and immutable evidence store; and
- [ ] dry-run evidence proving the harness without claiming operator certification.

Removing `certification_eligible = false` requires owner and independent reviewer signatures in the
same commit that freezes the machine-readable charter. It does not itself certify a release; only a
subsequent valid passing attempt can do that.

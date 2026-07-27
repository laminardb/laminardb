# Distributed state production-soak charter

- **Status:** Draft; `certification_eligible = false`
- **Applies to:** every cluster source/operator/output/sink scenario proposed for production
- **Parent:** [Phase 0 execution plan](../plans/distributed-keyed-state-phase-0-execution.md)

> This document is not production evidence. No distributed keyed operator is currently admissible,
> no numerical charter has been approved, and no independent release-candidate soak has run.

## Purpose

The soak is a separate production certification gate, not a longer unit test. It must expose
resource leaks, storage/maintenance tail collapse when applicable, checkpoint/rebalance errors,
stale ownership, and output divergence using an oracle and execution environment independent of the
implementation under test.

A backend qualification soak, Criterion benchmark, canary, weekly checkpoint soak, or
implementer-run integration test cannot satisfy this charter.

## Independence and immutable identity

A certification attempt must satisfy all of the following:

- the system under test is an unchanged release archive SHA-256 or OCI digest; the soak never
  builds LaminarDB;
- deployment chart, rendered configuration, dependency images, charter, oracle, and fault
  controller each have immutable identities and hashes;
- every attempt binds the exact `local-spill` working-state-profile identity, its approved
  applicability-contract hash, all resource/restore thresholds, and exact engine/build/mapping
  identity. Bounded memory remains reference/conformance-only and is not certifiable under this
  charter;
- the oracle has no `laminar_*` crate/library dependency and uses public source, sink, checkpoint,
  health, and metrics surfaces only;
- the charter's workload, duration, event count, thresholds, fault schedule, and invalid-run rules
  were approved before the attempt and cannot be overridden at dispatch;
- the operator/reviewer did not implement the state backend or operator being certified, or an
  equivalently independent protected approval is recorded; and
- every attempt, including failures and invalid runs, has a permanent attempt ID and retained
  evidence. There is no silent retry or retry-until-green.

Changing a relevant binary, chart, runtime configuration, dependency, working-state profile,
charter, oracle, or fault controller invalidates previous evidence and requires a complete run from
a clean deployment.

## Production-like environment

The frozen charter must identify:

- fixed Linux/kernel/container runtime/Kubernetes versions;
- dedicated CPU, memory/cgroup, FD and network limits plus the exact local NVMe/filesystem/mount
  limits for a local-spill profile;
- at least three LaminarDB processes placed across declared failure domains;
- isolated local-spill working-state volumes which can also be deliberately lost;
- cluster-shared object storage with a run-specific checkpoint prefix;
- replayable/splittable source and durable multiwriter sink instances with production-like
  replication and durability settings;
- Prometheus or equivalent external time-series collection plus node/cgroup/disk telemetry; and
- redacted rendered configuration and dependency topology.

The Helm StatefulSet can seed this deployment, but template rendering is not execution evidence.
The release Dockerfile toolchain must first match the workspace Rust requirement.

## Scenario matrix

The sole current production target is the local-spill working-state profile. Each proposed
`(scenario, local-spill-profile)` pair is certified independently; a green pair cannot widen another
operator, update mode, connector, or delivery guarantee. Bounded memory has no product-soak matrix
under this charter. Adding any future profile requires an owner-approved charter/ADR amendment
before a run, and prior local-spill evidence cannot be reused to certify it.

| Scenario | Initial intended path | Current status |
|---|---|---|
| Harness control | Kafka source -> stateless projection -> Kafka append sink | Engineering-only; proves oracle/deployment, never keyed-state production support |
| Grouped aggregate ALO | Kafka source -> grouped `COUNT(*)`/`SUM(Int64)` changed-group append snapshots -> certified, externally fenced Kafka append sink | Blocked by `[LDB-4007]` and Phase 0/1 evidence |
| Fixed event-time window ALO | Certified source -> tumbling window/final append output -> compatible sink | Blocked by `[LDB-4007]` and timer/frontier lifecycle |
| Bounded interval join ALO | Two certified inputs -> inner equi-interval join -> compatible append sink | Blocked by `[LDB-4007]` and co-partitioned join lifecycle |
| Retraction/FullChangelog | Stateful operator -> FullChangelog-capable cluster sink | Blocked; no built-in qualifying sink |
| Exactly-once | Exact-certified source/state/sink combination | Blocked independently by `[LDB-0013]` |

Only scenarios explicitly proposed for a release are run, but every scenario proposed for that
release must pass.

The initial grouped-aggregate scenario is narrower than the general row: each logical group is
routed to one fixed Kafka input partition, `COUNT(*)` is mandatory as its logical state version,
and `SUM` is nullable `Int64` using checked Laminar arithmetic. Each atomically applied input
batch appends one current row per touched group; rows within the batch may be coalesced, so the
oracle accepts legal group-local count prefixes with gaps. Versions must increase within one
writer-authority interval. After a crash, a new fenced interval may replay from the older sealed cut
and therefore append a lower version after an unsealed higher version from its predecessor. The same
version must always carry the same operation identity and bit-identical payload, and provenance must
explain every interval boundary. After the frozen source cut, the exact final version for every
group is mandatory. A full scan/republication of all resident groups per processing cycle is neither
required nor eligible evidence.

The admitted SQL shape has exactly one `COUNT(*)`, one SUM of a direct `Int64` column, and direct
partition-ABI-v1 grouping columns. Aggregate filters/HAVING/derived or multiple aggregates are not
part of this scenario. The planner must reject every volatile, time-relative, AI, or unclassified
upstream expression so the same Kafka prefix reproduces the same key and SUM payload.

## Cycle 49 checkpoint-versus-rotation pre-certification

This race-specific family is a prerequisite for, but cannot itself certify, keyed state. Its five
immutable scenario IDs are:

| ID | Workload and externally actuated membership change |
|---|---|
| `CVR-SL-F-ALO-v1` | Source-less logical input plus an idle admitted global aggregate; kill and rejoin an observed follower |
| `CVR-SL-L-ALO-v1` | The same source-less path; kill and rejoin the observed leader |
| `CVR-SF-F-ALO-v1` | Replayable Kafka source -> stateless projection -> Kafka append sink; kill and rejoin an observed follower |
| `CVR-SF-L-ALO-v1` | The same sourceful path; kill and rejoin the observed leader |
| `CVR-EO-REJECT-v1` | Cluster startup with `delivery = "exactly_once"`; negative admission control |

Each ALO attempt first proves one stable three-node assignment and committed checkpoint. The
controller then records a manual checkpoint reservation, actuates the selected hard process death
before that attempt reaches a terminal state, proves survivor recovery and a newer assignment,
restarts the victim, and records another checkpoint overlapping its rejoin rotation before proving
full membership and a further durable checkpoint. The checkpoint interval is reservation through
its unique terminal outcome; the rotation interval is external death/rejoin actuation through
convergence of every live node on the resulting durable assignment. They overlap only when those
half-open intervals intersect. A missing reservation, missed actuation, non-overlap, incomplete
membership/assignment boundary, or lost controller evidence makes that permanent attempt
`INVALID`; it is retained and never silently retried. A completed overlap with a product anomaly is
`FAIL`.

Every valid ALO attempt requires exactly one terminal outcome per reserved canonical checkpoint,
strictly increasing checkpoint and assignment identities, one exact assignment certificate and
participant roster per seal, no seal for a failed/aborted/superseded attempt, and convergence on the
same assignment. After recovery, the interrupted attempt must never complete, the first newly
reserved attempt must complete, each surviving node must apply exactly one recovery round without a
recovery failure, its process identity must remain unchanged, and source/sink progress must resume
within the frozen deadline. These public effects are the black-box callback-generation recovery
proof; object-lifetime regressions remain implementation tests.

For sourceful ALO, missing, extra, malformed, conflicting, stale-owner, or post-boundary output is
a failure. A duplicate is legal only when its replay-stable operation identity and payload bytes
are identical and the preceding sealed source cut proves that its offset belongs to that recovery
interval. Sink-admission sequence must strictly increase within each `(shard, writer interval)`,
and no predecessor record may follow its successor fence marker. `CVR-EO-REJECT-v1` passes only if
the unchanged release exits nonzero with `[LDB-0013]` before readiness or source, sink, checkpoint,
or state I/O.

For every node and process generation the evidence reports count, p50, p95, p99, p99.9, maximum,
and deadline-exhaustion count for `checkpoint_duration_seconds`,
`checkpoint_pipeline_stall_duration_seconds`, `checkpoint_barrier_local_duration_seconds`,
`checkpoint_aligned_resume_wait_seconds`, and `checkpoint_restorable_gate_wait_seconds`. The
external controller reports the same distribution fields for actuation-to-assignment publication,
publication-to-cluster convergence, recovery start-to-release, release-to-first durable checkpoint,
release-to-source/sink progress, and sink-cut-to-frozen-output completion. All repetition counts and
numerical limits must be approved in the frozen contract before dispatch; local measurements cannot
set them retrospectively.

`crates/laminar-server/tests/cluster_soak.rs` is reusable engineering scaffolding for the sourceful
path, role selection, cuts, recovery checks, and checkpoint histograms. It builds the test binary,
uses test-only gates, and lacks the source-less case, exact rotation-overlap controller, complete
duplicate/writer-fence oracle, and exactly-once negative case, so it is not independent evidence.
`tools/independent-soak-contract` validates an explicitly ineligible draft and semantic fixture; it
is not a deployment, fault, or evidence runner. The available WSL 2/Docker environment can run a
labelled functional smoke only. No Cycle 49 independent soak or production soak ran, and
`certification_eligible` remains `false`.

### Cycle 50 evidence disposition

The two sourceful ALO scenarios are **BLOCKED**, not merely unexecuted: current Kafka records have no
Laminar operation identity, writer interval, checked sink sequence, stable shard, or successor fence
marker. The live engineering oracle accepts any duplicate user `seq` once the expected set is
present and does not retain repeated payload bytes or bind them to an exact sealed source cut. The
then-current standalone fixture v1 supplies synthetic operation IDs, byte comparison, and
frozen/durable source-cut checks, but has no writer, assignment, shard, marker, or binding from
those cuts to a recovery-authority interval.

The two source-less scenarios are also **BLOCKED** on supported evidence. `/api/v1/cluster/vnodes`
reads the shared durable assignment snapshot on every node; identical replies do not prove that
each process adopted it. `/api/v1/cluster/checkpoints` exposes only latest summary metadata, and
Prometheus checkpoint histograms cannot recover an exact attempt maximum or deadline-exhaustion
count. Exact outcomes, recovery capsules, process leases, and adopted-assignment reports exist in
the object store, but their private paths, pruning, and envelopes are not a production evidence API.
Human-formatted tracing events remain diagnostic corroboration only. `CVR-EO-REJECT-v1` is publicly
observable, but its independent runner and frozen attempt contract do not yet exist, so it remains
ineligible and unexecuted.

Cycle 57 supplies a bounded stable-serving local-adoption view but not exact current recovery phase
or committed-`Release` consumption. Cycle 59 supplies local exact evidence for three of five
checkpoint latency families, but not full checkpoint or restorable-gate timing. Before any CVR
dispatch, complete those local facts, add a bounded versioned checkpoint-attempt/outcome view over
the existing durable authority, and make the independent runner consume them. The sourceful path
additionally requires the already-specified broker-enforced writer fence and record/marker
provenance. Missing evidence is `INVALID`; complete evidence showing malformed, conflicting, or
stale output is product `FAIL`. No independent CVR soak has run and `certification_eligible`
remains `false`.

### Cycle 51 evidence disposition

The root-excluded standalone tool now has a schema-v2 semantic oracle, but it remains synthetic and
explicitly `certification_eligible=false`. Its canonical case fixes the exact source and sink
partition inventories, including an empty source partition with a nonzero pre-delivery baseline;
separates the zero-input bootstrap checkpoint, later recovery checkpoint, recovery-base assignment,
and successor's current assignment; and checks predecessor/successor markers across every sink
partition. It also checks per-writer-interval admission sequences, raw source-offset replay
causality, byte equality for a repeated logical operation, increasing group-result versions within
an interval, independently derived vnode-to-shard ownership, and the final grouped result.

Missing, incomplete, or wrong-run checkpoint/assignment authority makes the attempt `INVALID`.
Given complete sink capture and authority, a wrong-run marker or complete evidence of stale,
conflicting, misordered, misowned, or otherwise malformed product output is product `FAIL`.
Assignment ownership and local process term are pre-reconciled in fixture evidence even though the
production design obtains them from separate supported views. The v2 fixture validates a frozen
key-to-vnode and vnode-to-shard semantic mapping, not the final Kafka partition/header bytes.

Cycle 51 adds no runtime header or marker encoder, Kafka transaction or producer fencing, supported
evidence endpoint, state backend, cluster admission, exactly-once claim, or independent runner. All
CVR scenarios therefore remain **BLOCKED**, no Cycle 51 soak ran, and `certification_eligible`
remains `false`.

### Cycle 52 evidence disposition

The root-excluded standalone tool now owns a strict envelope-v1 codec and the v2 fixture consumes
literal data-header and marker bytes. The exact layout remains defined only in
[ADR-008](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#distributed-output-envelope-v1).
Tests cover literal goldens, all prefixes and small trailing suffixes, version/kind/flag and length
rejection, bounded strings and vnode bitmaps, maximum sizes, deterministic hostile mutation, and a
65,536-header structural batch characterization. Data decoding borrows the supplied 66-byte slice;
marker decoding borrows variable fields; this is not a production latency or allocation benchmark.

No LaminarDB runtime or Kafka connector emits or reads the envelope, derives stable operation or
writer-interval identities, fences a producer, or exposes the required authority views. No backend,
cluster admission, delivery guarantee, or certification flag changed. All CVR scenarios remain
**BLOCKED**, no Cycle 52 soak ran, and `certification_eligible` remains `false`.

### Cycle 53 evidence disposition

The standalone v2 oracle now derives its grouped `COUNT(*)`/`SUM(Int64)` operation IDs from the
exact ADR-defined preimage and an explicit logical-key-to-ABI-v1-byte map. Matching changes to a
fixture label and wire header cannot hide a wrong ID. `SUM` and payload bytes deliberately do not
change identity; the existing byte-equality rule turns different bytes under one group/count ID
into a conflict. Identity derivation prehashes invariant scope and appends borrowed group bytes plus
count, but this structural property is not a production allocation or latency measurement.

A separate pure projection accepts a current assignment version/full-certificate-digest reference
plus complete vnode owner and canonical participant views, a current process lease, an immutable
committed recovery base, an opaque current/predecessor interval, and the exact planned shard vnodes.
It independently reconstructs the owner-map and full certificate digests, including participant
boots and the 129-participant cap. It rejects an inner digest, stale or same-version-conflicting
certificate, wrong node/boot or term, mixed ownership, and contradictory recovery evidence before
projecting marker and data headers. It does not allocate, rotate, durably persist, or broker-fence
an interval.

No runtime command carries the canonical group bytes/count or a durable pipeline incarnation, and
no transactional producer, supported authority endpoint, backend, admission, guarantee, or
certification flag changed. All CVR scenarios remain **BLOCKED**, no Cycle 53 soak ran, and
`certification_eligible` remains `false`.

### Cycle 54 evidence disposition

The root-workspace-excluded tool now unit-tests a synchronous fake writer around the frozen
authority and envelope. Confirmed marker fanout over every supplied affected partition opens data
atomically in the model; completeness of that partition inventory remains unproved. Each explicit
bounded data slice receives one checked shard/interval sequence range, and ambiguity poisons the
writer. The fake has no broker, timestamps, offsets, real fencing, public evidence, or measured
latency. In particular, an ambiguous marker has no visibility verdict until a future controlled
read-committed reconciliation test, so it cannot choose a production successor chain. Durable
interval non-reuse across fake chains, restarts, and `A -> B -> A` rotation is also unproved.

No backend, runtime connector, source/sink capability, cluster admission, delivery guarantee, or
certification flag changed. All CVR scenarios remain **BLOCKED**, no Cycle 54 soak ran, and
`certification_eligible` remains `false`.

### Cycle 55 evidence disposition

The root-workspace-excluded Kafka transaction probe uses the ADR-defined stable
[`transactional_id_v1`](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md#kafka-transactional-identity-v1-and-real-broker-evidence-boundary)
against a disposable one-node Redpanda topic. Repeated clean runs prove only deterministic broker
protocol behavior: exact synthetic three-partition marker fanout, provider-confirmed delivery,
confirmed abort followed by byte-identical retry, fatal predecessor fencing, and separate
read-committed/read-uncommitted visibility. Captures also check exact reserved-header cardinality,
an unrelated preserved header, marker null key and empty non-null payload, and unchanged data
key/payload. The broker subject is replication factor one and the probe does not run LaminarDB.

No matched `EndTxn` response was deliberately lost after request delivery, so an ambiguous marker
or data commit still has no tested reconciliation branch. The probe also provides no production
partition topology, source/state/sink atomic commit, interval-ID persistence, broker failover,
restart/disk durability, authentication, limit/pressure qualification, latency distribution, or
release-candidate soak evidence. All CVR scenarios remain **BLOCKED**, no Cycle 55 soak ran, and
`certification_eligible` remains `false`.

### Cycle 56 evidence disposition

The root-workspace-excluded probe adds a validation-only, one-broker matched-`EndTxn` v1 actuator.
Its four isolated marker/data cases deliberately distinguish a complete request followed by an
error-zero matching response withheld downstream from a complete request withheld upstream. Every
target-client connection closes before the same-ID successor reconciles separate read-committed and
read-uncommitted captures to frozen per-partition cuts. Read-committed evidence selects the
candidate marker and exposes predecessor data only in applied cases; it selects the last confirmed
marker or omits predecessor data in unapplied cases. Successor replay is visible in both data cases.

This closes a controlled ambiguity-proof gap in the standalone protocol evidence. It is not an
independent controller, release artifact, production topology, Laminar source/state/sink atomic
commit, durable interval allocator, replicated failover, authenticated connector, limit/pressure
test, or latency distribution. No backend, runtime connector, cluster admission, delivery
guarantee, or certification flag changed. `[LDB-4007]` and `[LDB-0013]` remain closed. All CVR
scenarios remain **BLOCKED**, no Cycle 56 soak ran, and `certification_eligible` remains `false`.

### Cycle 57 evidence disposition

The existing real-binary three-node engineering harness now consumes authenticated, bounded
stable-serving evidence from every expected live assignment participant. Each response binds the
exact node, boot, nonzero process term, and current-boot durable assignment adoption that still
matches the locally audited assignment fence; HTTP 200 itself means the process lease was sampled
live around the bounded checked-KV operation. A zero-vnode nonparticipant has no current matching
adoption evidence and is outside this positive-evidence set; an older durable slot record may remain.

The harness takes a durable assignment read before and after local samples, retries draining or
changing cuts, and rejects same-version digest/participant contradictions. Across hard kill/rejoin
it requires the durable assignment to advance, remove the killed boot, then bind the same stable
node to a new boot and higher process term with owned vnodes.

This is an engineering oracle extension, not the independent soak defined by this charter. The
route is unavailable during startup/recovery serving fences and records neither exact committed-
`Release` consumption nor current recovery phase. It provides no checkpoint-attempt history,
per-attempt timing maxima, output writer intervals/fencing, transactional source/state/sink
composition, backend qualification, immutable release subject, or independent operator. All CVR
scenarios remain **BLOCKED**, no Cycle 57 independent soak ran, and `certification_eligible` remains
`false`.

Two Windows/WSL2 Docker engineering commands exercised the new oracle with one in-checkpoint leader
kill. Both passed exact survivor and rejoin convergence plus complete ALO output-prefix accounting;
both complete tests still failed the existing terminal profile gate. The meaningful 90-second rerun
measured 98.81% rather than the required 99.00% of node1 checkpoint stalls within 1024 ms. It is
retained as a red engineering result, not retried until a favourable aggregate ratio appears, and
does not satisfy any CVR scenario.

### Cycle 59 evidence disposition

Cycle 59 adds a second engineering-only projection:
`GET /api/v1/cluster/local-checkpoint-barrier-timings`. It is bearer-protected, cache-disabled,
bounded by record count and response bytes, process-bound after the first page, and exposes only a
preallocated local ledger for pipeline stall, local barrier, and aligned resume. The collector
rejects unread-window overwrite, gaps, recording loss, metadata exhaustion, authority drift, and
exact count/diagnostic-bucket disagreement at a coherent observed Prometheus cut. Eviction after
successful export is allowed. Records through each observed cut stream to per-generation JSONL;
RAM remains bounded by fixed counters, maxima, assignment samples, and diagnostic witnesses, while
disk usage intentionally grows with observations.

The corrected optimized Windows/WSL2 run at `7782a032` passed its engineering gate after one
in-checkpoint leader kill. It reconciled 392 records across four process generations, found no
deadline exhaustion or three-family observation above 1024 ms, and accounted for all 79,996 input
IDs. Its engineering oracle tolerated and counted 2,758 duplicate output IDs without checking byte
identity or sealed-cut replay legality; this is neither the charter's at-least-once duplicate proof
nor exactly-once evidence. Authority was sampled at converged harness cuts, not for every unsampled
historical assignment version. The later `1a6dff80` substitution defense has focused deterministic
coverage but was not empirically rerun.

This endpoint and run do not satisfy this charter. The binary was built by the implementer from a
test target; the run had no frozen charter/independent operator, immutable retained evidence store,
qualified keyed backend/operator, production source/state/sink composition, or external fault
controller. Full-checkpoint/restorable-gate exact evidence and an instrumentation A/B remain open.
All CVR scenarios remain **BLOCKED**, no Cycle 59 independent soak ran, and
`certification_eligible` remains `false`.

### Cycle 60 instrumentation A/B protocol

Protocol v1 is frozen before measurement and is engineering-only. It estimates two effects that
must not be combined:

1. **A — recorder control / B — recorder treatment:** compare `40e3637b` with a derived source tree
   containing only the patch content of `6084462b` and `3909f4d4`. Do not add a runtime,
   configuration, Cargo-feature, or checkpoint-path branch to manufacture the control. Both arms
   run with diagnostic polling disabled. Archive and hash the derived tree and patch series before
   building; `034b14e9` is deliberately absent. Checkout `3909f4d4` itself includes `034b14e9`, so
   it is not the recorder-only treatment.
2. **C — polling control / D — polling treatment:** run one immutable binary built from
   `3909f4d4` in both arms. A separate observer process runs in both arms, pinned outside
   LaminarDB's CPU and memory allocation. The control executes the schedule without issuing
   requests; the treatment performs the Cycle 59 local-evidence and exact-timing requests. Freeze
   request deadlines, retry ceilings, response parsing, and retention before dispatch. Observer
   results may change request count only as the frozen retry policy declares; they must not control
   fault timing, progress waits, or run termination.

The recorder estimand is `B - A`; it excludes route installation and, because neither arm exposes
the ledger, is an intent-to-install comparison of the frozen recorder patches. C's endpoint can
confirm that the code-equivalent recorder mechanism activates, but that observation is not a B
runtime measurement. The polling estimand is `D - C`, conditional on recorder plus route already
being installed. Exact records exported only by D are arm-specific diagnostics, not an A/B
estimand: C can lose an unexported killed generation before any final read. `C - B` may be reported
as a descriptive route-install difference, but none of these contrasts may be summed or used to
infer an unmeasured interaction.

The fixed workload retains Cycle 59's three-node static topology, 64 key groups, 96 Kafka
partitions, and 400-record/s target, but replaces its response-timed unbounded producer with one
content-addressed trace. The trace contains exactly 80,000 records: ID `n`, decimal key `n`, payload
`{"seq":n}`, partition `n mod 96`, and monotonic target time
`t0 + n * 1_000_000_000 / 400` nanoseconds for `0 <= n < 80,000`. Generate and hash the complete
manifest before building any arm. `t0` follows three-node readiness, stable assignment, one durable
bootstrap checkpoint, and the first resource/Prometheus scrape. The measured window is exactly
`[t0, t0 + 290 seconds]`: the last target send is before 200 seconds and the remaining 90 seconds
is the fixed tail. Broker acknowledgements may lag but cannot change dispatch or the end anchor.

Set the periodic checkpoint interval to one hour and have the common driver issue manual
checkpoints in fixed absolute one-based slots: ordinals 1 through 80 at
`t0 + ordinal * 1.5 seconds`, ordinal 80 is the fault attempt, no requests are scheduled during the
predeclared fault/recovery gap, and ordinals 81 through 101 run at
`t0 + 255 seconds + (ordinal - 80) * 1.5 seconds`. The external scheduler must initiate within 50 ms
of a slot or make it `INVALID`; a still-running prior SUT request or unavailable serving SUT at a
slot is `FAIL`. Arm the observed leader before ordinal 80 and hard-kill it only after the existing
debug gate proves that exact attempt entered leader `Snapshotting`. Its 45-second gate deadline
starts when ordinal 80 is initiated; recovery and rejoin have 90 seconds from hard-kill actuation.
Missing either deadline, or failure to complete every scheduled request before `t0 + 290 seconds`,
is `FAIL`. Diagnostic observations cannot select the leader, ordinal, phase, timeout, input cut, or
window boundary.

A common external driver, configuration, dependency images, CPU/memory limits, topic layout,
object-store layout, fault controller, and correctness oracle apply to every arm. Materialize B as
a synthetic Git commit whose parent/tree/patch identities are retained. Build A, B, and the shared
C/D executable once each in clean identical builders with the workspace `soak` profile, identical
features, flags,
linker, `Cargo.lock`, and Rust toolchain; archive them by SHA-256 and reuse the same arm SHA in every
slot. Each run gets fresh topics and storage prefixes. The observer schedule is fixed at lifecycle
boundaries and once per five-second steady round; local-assignment retries use the existing 500-ms
cadence. Every arm uses the same non-diagnostic health, progress, durable-assignment, source, sink,
and external resource observations.

The current `cluster_soak` target couples diagnostic evidence to its control flow and therefore is
not this common driver. Before execution, the driver and observer must be separate hashed artifacts,
and review must show that arm selection cannot change workload, faults, or correctness decisions.
The polling arms must spawn the same binary; the recorder arms may differ only by the two frozen
source/build identities declared above.

Use eight complete temporal blocks. Each contains recorder control, recorder treatment, polling
control, and polling treatment. Precompute a two-replicate four-sequence Williams order, including
the arm-to-letter mapping and replicate order, from seed `0x4c44422d41423031`; record the resulting
schedule before warm-up. Perform and retain one unscored warm-up per arm. A run is the unit of
replication: checkpoint observations within a run must not be pooled as independent samples.
Retain every valid, invalid, failed, and warm-up attempt; never silently rerun a bad result.

Every attempt records source-tree/patch identities, executable SHA-256, `Cargo.lock` and Rust
toolchain identities, rendered configuration and observer hashes, run order/seed, host and
dependency telemetry, input/output cuts, exact fault evidence, and every timing surface available
to that arm. Only independently proved external host, dependency, driver, or evidence-collector
failure can make a slot `INVALID`; SUT-caused overload, telemetry loss, incomplete consumption, or
failure to reach the fault gate is `FAIL`. If the SUT reaches the declared gate but the external
controller fails to actuate, the slot is `INVALID`. One invalid slot invalidates its entire paired
block. Before warm-up, precompute two reserve instances of each of the four Williams sequences; a
rejected block may be replaced only by the next reserve with the identical within-block order.
Exhausting a sequence's reserves makes the experiment inconclusive. A valid product `FAIL` blocks
the gate and remains in the report; it is never excluded as an outlier.

Define one value per run before pairing: SUT completion throughput
`80,000 / (first complete distinct sink-output cut - t0)`; end-window distinct-output backlog;
total LaminarDB CPU seconds per wall second; maximum one-second sum of node RSS and median steady-
state sum after warm-up; worst-node bucket-derived p99 and p99.9 estimates computed by Prometheus
`histogram_quantile` interpolation after summing that node's process-generation pipeline-stall
bucket deltas; minimum per-node membership in the 1,024-ms bucket; and failover and rejoin time.
Also report checkpoint counts, every raw per-generation bucket, and producer-to-Kafka acknowledged
throughput as workload-conformance diagnostics. All 80,000 acknowledgements and distinct sink IDs
must arrive by the fixed window end; missing or conflicting output is `FAIL`.

Neither recorder arm provides comparable exact evidence. D may export exact records before the hard
kill while C loses that generation's in-memory ledger; therefore no full-run exact record or maximum
is an A/B estimand in v1. Retain any arm-specific exact record only as a labelled diagnostic. CPU,
RSS, checkpoint histograms, checkpoint count, and output throughput/backlog use only the fixed
measured window; failover and rejoin start at the recorded hard-kill actuation.

The external driver assigns each spawn a `{node ordinal, process-generation ordinal, OS process
identity}` and scrapes metrics/resources at `t0` and every integer second through `t0 + 290
seconds`, plus one required scrape immediately before hard-kill. Initial generations start at the
`t0` boundary; a restarted generation starts at its first serving scrape. They end at the required
pre-kill or `t0 + 290 seconds` boundary. Compute bucket/count/sum deltas only within one generation;
never subtract across a restart or counter decrease. A missing required boundary caused by the
external scraper is `INVALID`; an unavailable or regressed SUT metric while the process is serving
is `FAIL`. Sum nonnegative generation deltas per node before deriving its quantiles and 1,024-ms
ratio. The ratio numerator is the node's summed bucket delta and the denominator is its summed
`_count` delta. Every serving generation must contribute at least one observation and every node at
least 100; otherwise the run is `FAIL`.

For each block report the raw treatment-minus-control contrasts `B - A` and `D - C`. Separately
report adverse-oriented absolute and relative effects with positive meaning worse: use the raw
contrast for lower-is-better metrics, negate it for higher-is-better metrics, and divide the adverse
absolute effect by the control. A zero control makes the relative effect undefined and prevents a
relative-equivalence conclusion for that metric. Report all raw run values, every paired effect,
the median paired effect, and its percentile 95% interval from 100,000 paired-block bootstrap
resamples using seed `0x4c44422d41423031`. With eight blocks this interval is descriptive, not a
powered equivalence test. Derive the recorder effect only from `B - A` and the polling effect only
from `D - C`.
Cycle 57's two red runs remain historical observations: their binaries, evidence, and attempt
attribution differ, so they are not samples in either A/B population.

Protocol v1 cannot close the perturbation gate. Before any equivalence run, workload and operations
owners must approve absolute and relative margins for throughput, CPU, RSS, checkpoint tails, and
recovery, and a v2 protocol must freeze a prospective sample-size/precision justification plus a
simultaneous/multiplicity decision rule. Any future green conclusion is limited to that exact
engineering workload, build, and environment. It cannot qualify a backend, admit keyed state,
establish exactly once, generalize to production keyed workloads, or replace the independent
immutable release-binary soak.

### Cycle 61 executable-binding seam

The ignored `cluster_soak` target can now select a prebuilt server only through the all-or-nothing
`LAMINAR_SOAK_LAMINARDB_EXE` and `LAMINAR_SOAK_LAMINARDB_SHA256` environment pair. The path must be
absolute; the harness executes its canonical regular-file target; and the digest must be exactly 64
lowercase hexadecimal characters. With both variables absent, Cargo's test-built `laminardb`
remains the default. Any partial, empty, malformed, missing, non-file, mismatched, or subsequently
changed selection fails closed. On PowerShell, canonical lowercase input can be prepared as:

```powershell
$laminarSoakExe = (Resolve-Path -LiteralPath .\target\soak\laminardb.exe).Path
$laminarSoakSha = (Get-FileHash -Algorithm SHA256 -LiteralPath $laminarSoakExe).Hash.ToLowerInvariant()
$env:LAMINAR_SOAK_LAMINARDB_EXE = $laminarSoakExe
$env:LAMINAR_SOAK_LAMINARDB_SHA256 = $laminarSoakSha
```

Identity validation does not predict OS executability. A regular file with the declared digest but
missing execute permission, wrong executable format, or wrong architecture is rejected only by
`Command::spawn`; the cluster path may already have created its isolated topics/dependency clients.
The runner must classify it as a failed attempt, retain any created evidence, and never fall back
to Cargo's binary.

Resolution occurs before dependency or process side effects. Every initial and restart spawn then
requires a fresh hash check and consumes a private permit tied to that same resolved identity.
Restart checks occur immediately before the existing recovery/rejoin timer starts, so hash I/O is
not charged to the reported RTO. Spawn receipts bind node, PID, and digest; Kafka-feature cluster
receipts additionally bind process generation.

This is a controlled-run substitution guard, not process-image attestation. A portable path-based
spawn retains a verify/exec TOCTOU window; the runner must stage the executable read-only and retain
pre/post hashes. Hashing also warms the executable cache and uses host I/O during the workload, so
the resulting recovery values are common-harness relative measurements, not cold-start production
latency. The current harness still lets diagnostic collection affect control flow and is not the
Cycle 60 A/B common driver. Its fault gates are test-only, so selecting an ordinary release binary
also does not satisfy this charter's independent release-candidate fault soak.

No standalone observer was added in Cycle 61. It must land with a consuming driver dry run that
proves observer exit, hang, or malformed output cannot alter the immutable checkpoint, fault, or
end schedule; a schedule generator tested only by itself would not prove non-interference.

### Cycle 62 schedule scaffold

Cycle 62 adds [`tools/distributed-state-ab`](../../tools/distributed-state-ab/) as an unpublished,
standalone Cargo workspace with a committed lockfile. Its direct normal dependencies are
`serde`, `serde_json`, and `sha2`; it has no LaminarDB, async-runtime, network, Kafka, backend, or
candidate dependency. The driver and observer are separate executable byte identities. This is a
contract dry run labelled `NOT A/B OR CERTIFICATION EVIDENCE`, not the Cycle 60 experiment.

One strict manifest binds the exact driver, observer, server, trace manifest, declared-redacted
configuration, dependency manifest, virtual-control script, and protocol bytes by canonical
regular-file path, length, and SHA-256. Its raw hash is part of the common base plan, so limits,
authentication, C/D mapping, and every other manifest field are common too. Arm and injected child
behavior are command inputs and cannot enter that plan. Referenced trace, protocol, and control
artifacts are opaque provenance in v1; the tool does not parse or execute them.

The frozen driver schedule contains start, 101 checkpoint declarations, the input-target-end
declaration, and end at scheduled 290 seconds: 104 actions in all. The observer schedule contains
58 five-second slots from zero through 285 seconds, three nodes, and two route labels, for 348
planned probes. Slots at zero, 120, 200, and 255 seconds carry fixed schedule-anchor labels; those
labels are not observations that a kill, recovery, or rejoin happened. C suppresses all planned
probes and D serializes them in slot/node/route order with response-byte caps. Neither arm opens a
socket, issues HTTP, implements retries/pages/cursors, parses a response, executes the declared
workload/faults, contacts a SUT, or waits 290 wall-clock seconds.

The driver spawns the observer with an empty environment, piped stdin carrying only one fixed
one-way start signal, and piped stdout/stderr. Dedicated threads necessarily drain pipe bytes
concurrently but retain at most the manifest caps in memory, so stdout/stderr cannot grow the
driver's retained output artifacts beyond those caps. Only after the end seal does the driver
receive the captures, inspect process status, classify output, and create stdout/stderr artifacts. A valid
treatment result is preflighted against the stdout cap. The completion budget is separately bound
to 100--60,000 ms because observer artifact hashing is not part of the virtual schedule.

The driver materializes, rather than executes, the common action trace. It validates the complete
trace against the sealed plan, writes it with `create_new`, calls `sync_all`, and only then obtains
a private non-cloneable plan-bound end seal. Collection consumes that seal by value. Status, kill,
capture reception, and JSON interpretation occur afterward. Kill/reap and a second cleanup attempt
are bounded; a true cleanup failure is retained as `TerminationFailed` with the observer PID. The
base-plan and trace bytes are reread after collection before the final record is written.

The consuming Windows test matrix runs `{C,D} x {success,exit,hang,malformed}` from the same raw
manifest and requires byte-identical base plans and driver traces plus identical plan/trace hashes.
It also covers spawn failure without a changed driver trace, minimum valid stdout cap, exclusive
artifact directories, changed input identity, strict credential/unknown-field rejection, paths
containing spaces, capped pipe retention, and a parked-child kill/reap fixture. The resulting proof
is reviewed-code logical separation. There is no OS sandbox: a hostile same-user process can still
inspect or mutate accessible resources, path verification retains verify/open and verify/spawn
TOCTOU windows, and pipe-drain CPU/I/O is not an instrumentation-cost measurement.

The live observer remains blocked. LaminarDB currently places diagnostic GET routes and mutation
routes such as checkpoint, SQL/reload, and pipeline start/stop behind the same console bearer and
protected router. Giving that bearer to a polling process would give it control authority. A live
v2 must instead use an independently reviewed route-scoped read-only token or a content-bound
GET-only broker, receive a sanitized observer plan rather than the full manifest, and first pass a
loopback fake-server protocol test covering exact origin/method/path, deadlines, retry ceilings,
pagination/cursor transitions, response bounds, process identity, and zero C connections. No live
HTTP or cluster attempt is authorized by Cycle 62.

No A/B ran, no perturbation or latency was measured, and no production source/state/sink delivery
claim changed. Powered instrumentation equivalence, exactly-once composition, backend
qualification, and the independently operated immutable release-binary soak remain separate open
gates. `certification_eligible` remains `false`.

### Cycle 63 diagnostic-read authority decision

Cycle 63 is a code-free security and lifecycle decision. The live observer, HTTP client, broker,
cluster execution, and A/B remain blocked. The current tree has one protected router: its console
bearer covers the two local diagnostic GETs as well as checkpoint, SQL, reload, pipeline start/stop,
other console reads, and WebSocket access. A process holding that bearer is therefore a control
principal, regardless of which requests its source code intends to send.

LaminarDB will add a server-enforced, startup-bound `server.diagnostic_read_token`. A separate
diagnostic router will accept that credential only as an `Authorization: Bearer` header on these
two exact routes:

- `GET /api/v1/cluster/local-evidence`; and
- `GET /api/v1/cluster/local-checkpoint-barrier-timings` with its existing strict cursor query.

The two configured values are deliberately distinct. The existing console bearer remains accepted
on the two routes as the administrator credential; denying a principal that already has mutation
authority would not make an observer holding that credential safe. The meaningful boundary is the
opposite direction: the diagnostic bearer never authenticates a console, mutation, query-token,
cookie, WebSocket, or public-probe path. A live observer must receive the diagnostic secret and
must never fall back to the console secret. With no diagnostic token, the console bearer preserves
current engineering access, but that configuration is ineligible for live instrumentation; with
neither token the sensitive handlers retain their current `503` behavior.
Because the administrator bearer is a deliberate superset, a successful response alone cannot
prove which credential the caller held. The later supervisor must obtain a typed diagnostic secret
from its dedicated provisioning channel, refuse to start when that source is absent, and expose no
console-secret source or fallback to the observer. That provenance—not the HTTP status—is the
evidence-integrity check.

A configured diagnostic credential is admitted only when all of the following hold:

- server mode is `cluster`, `server.console_token` is also configured, and the two secrets differ;
- one shared auth validator runs both during file loading and at programmatic `run_server` entry,
  before cluster leases, listeners, or other startup side effects;
- the diagnostic value is exactly the canonical unpadded base64url encoding
  `[A-Za-z0-9_-]{43}` of 32 bytes. When diagnostic mode is enabled, the console value must satisfy
  the same rule; legacy console-only configurations retain their existing minimum. Decoding,
  canonical re-encoding, exact byte count, and distinctness are enforceable, while random
  generation remains an operator/provisioner obligation;
- the HTTP `server.bind` address is loopback. The current HTTP listener is plaintext and has no
  peer-address extractor or native TLS, so a non-loopback token would make a false transport claim;
  this also restricts v1 to co-located, single-host engineering clusters because the same HTTP port
  is advertised for inter-node checkpoint RPC. Multi-host A/B and production-soak use remain
  blocked pending a separately reviewed local diagnostic listener or native TLS/mTLS design; and
- the value appears only in server-side secret configuration or its supported environment
  substitution. It is never passed in the observer's URL, command line, environment, manifest,
  plan, log, evidence artifact, token digest, or non-secret key identifier.

The eventual router/auth matrix is normative. Statuses below assume the outer startup/recovery
serving gate is open; that gate may return `503` first and remains authoritative.

| Request | Diagnostic credential result | Other authority/result |
|---|---|---|
| Exact `GET` local-evidence, with no query | Authenticates; handler retains its state/fence checks and bounded `no-store` response | Console bearer remains an administrator credential; missing, wrong, duplicated, comma-joined, query, and cookie credentials do not authenticate |
| Exact `GET` local-checkpoint-barrier-timings with the existing strict query schema | Authenticates; existing cursor/process, loss, page, response, and fence checks remain | Unknown, missing, duplicate, or malformed query fields still fail closed |
| Either exact `GET` path when the diagnostic token is not configured | No diagnostic authority exists; a live observer is ineligible | A valid console bearer preserves current access; with no console token the handler returns `503` |
| `HEAD`, `POST`, `PUT`, `PATCH`, `DELETE`, `OPTIONS`, `CONNECT`, or `TRACE` on either path | Never invokes an evidence handler; a valid diagnostic bearer receives a method rejection | A valid console bearer receives the same method rejection; diagnostic CORS/preflight is not enabled; Axum's implicit GET-to-HEAD behavior must be overridden |
| Checkpoint, SQL, reload, pipeline start/stop/status, other console GETs, or `/ws/*` | `401` before the target handler; no mutation or upgrade | Existing console rules remain confined to the console router |
| Trailing/double slash, ASCII case change, percent-encoded separator/backslash/dot segment, or unknown path | No diagnostic route match and no target-handler call | No redirect or normalization fallback is introduced |
| Absolute-form URI carrying an otherwise exact path | Rejected before the handler when URI scheme or authority is present | Only origin-form requests are admissible |
| `/health`, `/ready`, `/metrics` | No new authority; these remain the existing public probes | They expose no diagnostic body and accept no diagnostic query-token meaning |
| Either diagnostic path in single-node or feature-disabled mode | No diagnostic auth decision or handler work | Preserve the current ready-state `404`; diagnostic configuration itself is cluster-only |

The middleware uses one immutable startup policy, rejects a presented diagnostic credential unless
its length matches one configured secret before constant-time comparison, and inserts a private
typed principal for the handler. Immutable credentials make a second post-capture token comparison
meaningless; the handlers instead retain their post-capture serving/process-fence checks. Successful
and rejected requests log path/method/status/latency only; no header, query string, credential, or
digest is logged. Diagnostic routes sit outside the console's permissive CORS layer. Existing
response caps and `Cache-Control: no-store` remain.

Availability is also server-enforced before live use. One non-queuing permit is shared by the two
diagnostic handlers per process; contention returns `429` without entering either handler. A fixed,
allocation-free rolling window admits at most eight handler starts per process per second, after
successful authentication, and excess returns `429`. Every admitted handler has a two-second
server deadline and timeout returns `504`; releasing the permit and accounting the start are tested
under success, rejection, timeout, and cancellation. These limits permit the two scheduled reads
and bounded cursor catch-up while containing a buggy or compromised credential holder. They are
route-local control-plane mechanisms, not a generic RBAC/rate-limit service, and add no row, state,
checkpoint-capture, source, or sink hot-path operation. The later A/B still measures their actual
cost.

Two existing reload behaviors must be corrected before the credential is enabled. The diff engine
labels `[server]` restart-only, but successful explicit reload currently republishes the entire new
configuration, and a file-watcher reload does the same when any reloadable DDL is also present.
That can rotate/remove HTTP authority while LaminarDB's checkpoint forwarder retains its startup
console token. In addition, a TOML parse error can retain substituted source text and the reload API
logs/returns that error, so a malformed secret line can be disclosed before `Secret` redaction is
constructed.

The implementation must therefore (1) strip TOML source input from parse errors before logging or
returning them, (2) have both reload entry points commit only the four reloadable named sections
(`source`, `lookup`, `pipeline`, and `sink`) while retaining every restart-only active value, and
(3) snapshot console and diagnostic auth policy at startup. Adding, removing, or rotating either
credential requires restart; there is no old/new grace overlap or live-reload fallback.
A configuration-file or executable change also invalidates a frozen A/B or soak attempt.

Tests cover pure restart-only changes and mixed DDL plus restart-only changes through both explicit
POST and file-watcher reload, including successful and failed DDL. Only `source`, `lookup`,
`pipeline`, and `sink` may change; the active console/diagnostic policy and the checkpoint
forwarder's startup credential remain unchanged. A sentinel substituted secret must be absent from
parse-error `Display`, `Debug`, source chains, startup output, watcher logs, and reload API bodies.
The shared validator is exercised through file loading and programmatic startup before side
effects. Route tests cover both credentials, every row above, absent CORS, duplicate and wrong-size
headers, immutable policy, limiter/deadline release, zero mutation side effects, and default-deny
behavior when a new console route is added.

A hashed external GET-only broker is rejected. Hashing binds which binary was selected but cannot
stop that binary, a dependency, or an exploit from spending its unrestricted console bearer on a
mutation. L3/L4 isolation cannot distinguish HTTP methods and paths on one origin. A genuinely
least-authority broker would still require server-side authorization, while adding IPC framing,
secret delivery, redirect/proxy/DNS defenses, lifecycle failure, copying, and measurement
perturbation.

The next implementation cycle is limited to parse-error redaction, reload commit semantics, the
startup auth policy, split routers, and their exhaustive configuration/route/race tests. It adds no
observer client or live request. Only after those gates pass may a loopback fake-server cycle define
sanitized-plan/secret delivery, exact origin and request construction, disabled redirects/proxies,
deadlines, retry/page/cursor state, response bounds, cancellation, and zero C connections. Live
effect-estimation, a powered equivalence experiment, and the independent immutable release-binary
soak remain later and separate.

## Frozen numerical contract

An eligible machine-readable charter contains no `TBD`, zero, or results-derived threshold. It
specifies at minimum:

- elapsed duration and reconciled broker-record count, with both minima required;
- event rate, Arrow-batch shape, key/value widths, key cardinality, state size relative to RAM,
  Zipf/hot-key skew, vnode count, and checkpoint cadence;
- p50/p95/p99/p99.9 end-to-end latency and maximum event-loop stall;
- checkpoint align/freeze/export/upload/sink-flush/seal limits and recovery RTO;
- hard encoded envelope/artifact/descriptor/payload bytes; checkpoint chain links/delta depth;
  operators and vnodes per transition; rows, canonical key/state bytes, output-buffer bytes;
  global encoded restore bytes; and separate per-task/global restore scratch;
- common maximum RSS/PSS/cgroup memory, queue bytes/age, FD count, snapshot/iterator count, frozen
  generations, timer count and checkpoint artifacts;
- local-spill engine cache/memtable/journal/native memory, local bytes, disk utilization,
  maintenance debt/health and write amplification;
- accepted steady-state slopes for every retained resource; and
- exact repetition/fault counts and random seed.

Workload and operations owners must approve these values. Baseline measurements can inform their
decision but cannot automatically become the gate.

## External input and output oracle

Before sending, the producer durably records one immutable intent per stable event ID and payload.
Acknowledgement is observation, not membership: Kafka may persist a record and lose its response.
After production/faults stop, the controller reads every actual source record from the run's start
offsets through frozen per-partition high-watermarks and reconciles it to intent. That broker-derived
ledger, including physical retries, is the oracle input:

```text
run_id, scenario_id, working_state_profile, intent_id, event_id, topic, partition, offset,
event_time, logical_key, logical_values, payload_sha256, acknowledgement_outcome
```

An actual record without matching intent, conflicting payloads for one event ID, unreadable range,
or mismatch between reconciliation and frozen cuts is a harness failure/invalid run, never silently
missing input. The controller then waits for LaminarDB's durable source cut and required post-cut
checkpoint progression and freezes exact per-partition sink high-watermarks. An incomplete source or
sink cut cannot pass.

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
causality and cannot be a checkpoint attempt alone. Each compact data header carries only envelope
version/kind, operation ID, writer-interval ID, and a sink-admission sequence that starts at zero and
strictly increases within each `(shard, interval)`. The serialized Kafka payload bytes are the
comparison authority and the independent reader computes their SHA-256; no separate digest is
transmitted per record. Intentional rewind or recreate changes pipeline incarnation; crash replay
does not.

Metadata alone cannot prove pre-fence admission. Each bounded Kafka sink-writer shard uses a stable
transactional ID derived from deployment, pipeline incarnation, sink, and shard. A successor
initializes it to broker-fence the predecessor, then commits deterministic predecessor/successor
markers to all affected output partitions in one confirmed transaction before admitting data. Each
marker carries deployment; pipeline incarnation and identity; operator/output and sink identity;
partition ABI; sink shard and owned vnode set; assignment certificate/digest; owner node, boot
incarnation, and durable process term; predecessor/successor interval IDs; and the exact recovery-
base `{epoch, checkpoint_id}` plus recovery-capsule digest. Every output record then uses
transactions from that fenced producer. The oracle resolves that immutable checkpoint reference,
derives the expected vnode from the canonical key and frozen ABI, verifies marker ownership, reads
committed data, rejects replay causally before its sealed source cut, and rejects an old-interval
record after the marker. A predecessor
transaction committed before the marker remains legal even if its acknowledgement arrived later;
an open transaction aborted by fencing is invisible. An ambiguous marker commit terminates that
writer and a new interval fences it before retry. Missing output, state double application, two
payloads sharing an operation identity, old-interval output after its partition marker, or output
beyond the frozen boundary is a failure. If provider-enforced fencing and markers cannot prove these
distinctions, the scenario is not certifiable; timestamps and Laminar logs are not substitutes.

The first interval follows the same authority rule. Before source delivery, graph execution, or sink
write admission, the controller proves that Laminar resolved exact source partitions and numeric
exclusive start baselines and committed a zero-input bootstrap checkpoint/capsule with empty state/
timers and the current pipeline/assignment identity. The unactivated sink may acknowledge only this
proved-empty flush. Its first marker has `predecessor = none` and references the bootstrap capsule;
readiness/data admission remain closed until that transaction is confirmed. Failure before the
bootstrap Commit retries startup. After Commit, the exact marker may retry in the same live interval
only after a definitely rejected attempt or confirmed abort; an unproved outcome or writer
retirement creates a new fenced interval against the same cut. A source that cannot expose the pre-delivery baseline is not
certifiable. The controller measures bootstrap and first-marker time as startup/RTO latency.

## Externally actuated fault schedule

Release bits cannot depend on the test-only checkpoint kill gate. Every profile runs the common
faults, and the frozen controller schedule includes all profile-applicable faults:

- active-owner, follower, and leader hard process death;
- repeated restart/rejoin and `1 -> 3 -> 2` ownership changes;
- control/shuffle network partition while source and sink remain reachable;
- source broker restart and backpressure;
- object-store latency, timeout, and temporary unavailability;
- local-spill cold-cache state larger than RAM, disk pressure/`ENOSPC`, corruption, applicable
  maintenance stalls, and complete local-volume loss followed by portable restore;
- rolling N/N-1 upgrade and rollback; and
- faults bracketing state mutation/freeze, timer fire, output enqueue, Kafka producer fencing and
  marker commit, sink flush, durable seal, source-cut publication, and assignment activation; and
- forced predecessor write attempts after each successor marker, which must be broker-rejected and
  absent from read-committed output.

Every scheduled fault must have independent actuation evidence. A missed fault makes the attempt
invalid rather than silently reducing coverage.

## Result classification

`PASS` requires every assertion applicable to that exact `(scenario, local-spill profile)` to pass
and every required artifact to exist. Reference-backend test evidence is not production evidence.

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

- attempt/run manifest with the exact working-state-profile identity and complete attempt history;
- archive/image/chart/SBOM, charter, oracle, controller, config, and dependency digests;
- rendered deployment resources and redacted configuration hash;
- durable producer-intent log, broker-readback reconciliation ledger, and frozen source cuts;
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
- [ ] approved per-scenario working-state-profile identities, applicability contracts and
      profile-specific thresholds/faults;
- [ ] immutable release and standalone-oracle artifact pipelines;
- [ ] machine-readable charter schema and preflight validator;
- [ ] stable output operation identity, assignment-generation evidence, and provider-enforced
      writer-fence markers;
- [ ] grouped aggregate state path admitted after Phase 1 correctness gates;
- [ ] production-compatible source/object-store/sink environment;
- [ ] external fault controller and immutable evidence store; and
- [ ] dry-run evidence proving the harness without claiming operator certification.

Removing `certification_eligible = false` requires owner and independent reviewer signatures in the
same commit that freezes the machine-readable charter. It does not itself certify a release; only a
subsequent valid passing attempt can do that.

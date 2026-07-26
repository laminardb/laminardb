# Checkpoint-attempt evidence audit — 2026-07-26

## Decision

Cycle 58 found that LaminarDB retained strong recovery authority for a terminal cluster checkpoint
but no exact per-process checkpoint-stall observations. Cycle 59 implements only the first bounded
observability slice: a process-local ledger and protected engineering endpoint for pipeline stall,
local barrier, and aligned resume. It does not add the durable exact-outcome/capsule audit described
below, and it does not widen cluster admission or delivery guarantees.

The program keeps two kinds of evidence separate:

1. a bounded, process-local timing ledger that records the exact attempts observed by the existing
   pipeline-stall timers, including sequence continuity and explicit loss; and
2. a future read-only, same-snapshot exact-outcome audit that joins selected attempts to retained
   immutable authority and, only for a live Commit, validates its content-addressed capsule.

Timing must not be added to the recovery-critical outcome or capsule formats. It is
participant-local, nondeterministic evidence, whereas those records are deterministic cluster
authority. Cycle 59 changes checkpoint-control instrumentation only; it adds no row-path work,
state backend, cluster admission, or source/sink guarantee.

## What is authoritative today

| Fact | Retained authority | Exact boundary |
|---|---|---|
| Checkpoint attempt | `CheckpointOutcome` carries nonzero `epoch` and `checkpoint_id`; runtime outcomes require `epoch == checkpoint_id` | Canonical identity is durable only while the exact live outcome or a matching committed/terminal continuity anchor remains retained |
| Terminal disposition | One cluster `Commit` or `Abort` is admitted through the append-only leader-authority sequence | Create-once and leader-fenced; not a timing record |
| Assignment and leader | A cluster outcome carries the canonical assignment fence and leader proof | Binds vnode roster, leader node/boot/term, and the selected attempt |
| Recovery image | A cluster Commit carries a content-addressed `RecoveryCapsuleRef` | Abort carries no capsule; a capsule created before outcome admission is an orphan, not Commit authority |
| Recovery contents | `ClusterRecoveryCapsule` binds pipeline identity, assignment, participant readiness/manifests/portable state, seal inventory, connector offset/metadata maps, assignment versions, and watermarks | Canonical body is capped at 8 MiB and verified against digest, length, deployment, attempt, and fence; connector maps are not provider-neutral exclusive cuts |
| Outcome history | Live authority links are bounded at 4096; compaction has separate artifact and terminal-history floors | When present, the committed and terminal anchors each preserve an exact cloned outcome body and may coincide; an anchor need not retain its capsule/artifacts and is not by itself a live recovery cut, while most other old exact outcomes disappear |
| Full checkpoint duration | `CheckpointResult` carries attempt, success, duration, error, and failure disposition | Ephemeral; a success log and aggregate histogram consume it, but no durable/bounded attempt inventory does |
| Pipeline pause | Pipeline-stall, local-barrier, aligned-resume, and restorable-gate histograms observe durations; Cycle 59 additionally retains a fixed-capacity process-local ledger for the first three | The ledger names process, assignment certificate, attempt, role, exact nanoseconds, handoff, and deadline status with sequence/overwrite/loss evidence; restorable-gate and full-checkpoint exact inventories remain absent |
| Early failures | One drop guard closes the local/aligned/stall histogram scope and attempts the corresponding exact ledger record | Early returns after guard construction remain exact when canonical context exists; missing/invalid context, duration overflow, contention, or counter exhaustion increments fail-closed loss evidence |
| Public checkpoint view | `/api/v1/cluster/checkpoints` reads the latest manifest and lists checkpoints | Coarse latest ID/epoch/time/source/sink/count metadata; it is not immutable outcome or capsule authority |
| Local assignment view | Cycle 57's `/api/v1/cluster/local-evidence` binds the live process to its current audited assignment adoption | It does not retain checkpoint phase, terminal settlement, or stage timing |

Primary code evidence:

- [`checkpoint_decision.rs`](../../crates/laminar-core/src/checkpoint_decision.rs) defines and
  validates `CheckpointOutcome` around lines 120–242, caps a standalone outcome at 64 KiB, and
  reconciles create-once writes around lines 1948–2058.
- [`recovery_capsule.rs`](../../crates/laminar-core/src/checkpoint/recovery_capsule.rs) defines the
  8-MiB canonical capsule and its content-addressed reference around lines 163–428.
- [`leader_lease.rs`](../../crates/laminar-core/src/cluster/control/leader_lease.rs) caps a cluster
  authority record at 256 KiB, bounds live authority at 4096 links, and supplies separate outcome,
  capsule, inventory, inferred-settlement, and retention reads around lines 4995–5257. Its ordinary
  head load can fall back to a LIST and pointer repair around lines 2834–2955.
- [`checkpoint_coordinator.rs`](../../crates/laminar-db/src/checkpoint_coordinator.rs) defines the
  ephemeral `CheckpointResult` around lines 927–970 and records only aggregate checkpoint/gate
  duration metrics around lines 3556–3574 and 7015–7027.
- [`checkpoint_timing.rs`](../../crates/laminar-db/src/checkpoint_timing.rs) defines the preallocated,
  nonblocking ledger, drop guard, loss metadata, and bounded snapshots.
- [`pipeline_callback.rs`](../../crates/laminar-db/src/pipeline_callback.rs) samples exact process,
  attempt, role, and assignment-certificate context for the three barrier routes.
- [`engine_metrics.rs`](../../crates/laminar-db/src/engine_metrics.rs) defines aggregate checkpoint
  histograms around lines 79–95 and 297–330.
- [`http.rs`](../../crates/laminar-server/src/http.rs) exposes the protected, cache-disabled,
  byte/record-capped process-local page without shared-storage reads.
- [`cluster_soak.rs`](../../crates/laminar-server/tests/cluster_soak.rs) streams exact records,
  validates ring algebra and authority, and reconciles each coherent observed ledger/Prometheus cut.

## Exact missing capability

The missing observability capability is not “more checkpoint metrics.” It is a loss-detecting
correlation lifecycle:

```text
local process authority + exact assignment + checkpoint attempt
       -> one exact local stall observation
       -> bounded sequence/retention evidence
       -> same-snapshot exact retained outcome (without later-outcome inference)
       -> validated live capsule reference for an exact Commit
```

After Cycle 59, the left-to-middle link exists for the process generations and converged assignment
cuts observed by the engineering harness. Sequence continuity, explicit loss, and exact histogram
reconciliation make those three timing families auditable. The durable right-hand join is still
missing, as are exact full-checkpoint and restorable-gate observations. The new passing run does not
retroactively identify Cycle 57's two slow attempts or assign causality to its 500-ms polling.

The capsule's connector maps are opaque provider values, not a normalized exclusive-cut contract.
For example, Kafka persists a last-consumed inclusive offset and connector code converts it to the
next offset on restore. A future projection cannot label raw values “exclusive source cuts” until a
versioned per-connector normalization contract exists. Exposing raw connector metadata is also not
acceptable merely to make an oracle convenient.

The durable outcome does not prove an external sink transaction completed. Coordinated sink commit
has its own cursor/reconciliation lifecycle, and the current Kafka path remains at-least-once.
Joining an attempt to a live Commit capsule therefore proves a recovery cut, not source/state/sink
atomicity or exactly-once delivery.

## Frozen requirements and implementation boundary

### Process-local barrier-pause timing ledger

This first slice covers exactly three of the five frozen latency families: pipeline stall, local
barrier, and aligned resume. Exact full-checkpoint and restorable-gate attempt maxima and deadline
evidence remain open; this ledger must not be described as closing the generic checkpoint-latency
contract.

One record must correspond to one observation of
`checkpoint_pipeline_stall_duration_seconds`. It must contain only fixed, bounded fields:

- monotonically increasing process-local sequence;
- node ID, boot incarnation, and process term sampled for that observation;
- canonical checkpoint attempt and leader/follower role;
- assignment version and digest used by that attempt;
- exact pipeline-stall and local-barrier nanoseconds;
- aligned-resume nanoseconds when that stage ran;
- whether local capture reached its durable-tail handoff; and
- whether the attempt's absolute deadline was exhausted before the timer closed.

The snapshot must return its oldest retained sequence, next sequence, overwrite count, and
contention/recording-loss count. Overflow, a sequence hole, authority mismatch, or counter mismatch
with the histogram invalidates soak evidence; none may be interpreted as “no slow attempts.”

The recorder is preallocated and process-local. Recording is O(1), performs no object-store,
network, filesystem, serialization, or state-backend operation, and never waits behind an evidence
reader. If a nonblocking record cannot be accepted, it increments explicit loss state. This work is
checkpoint-control-path instrumentation, not row-path work, but its measured overhead still needs
an A/B gate.

Do not use per-attempt Prometheus labels: they create unbounded cardinality. Do not use sampled
traces or text logs as certification authority: either may omit the violating attempt. Do not put
timing in `CheckpointOutcome` or `ClusterRecoveryCapsule`: that would add nondeterministic,
participant-specific data and storage pressure to recovery authority.

### Bounded consumer

The first consumer must be the existing three-node engineering harness, not an unused library
type. A protected, cache-disabled local page may expose only in-memory ledger records after a
caller-supplied sequence, with a fixed record count and response-byte cap. It must perform no shared
storage read. Process authority is sampled around the snapshot so records cannot be relabelled after
a lease/term change.

The harness captures every expected process generation before kill and at the end, incrementally before any
unread record can leave the ring. Physical eviction after a record was exported is safe; a stale
cursor, unread-window overwrite, gap, recording loss, or metadata exhaustion is not. It checks exact
record counts and diagnostic `le=1.024` classifications against the existing histogram deltas and
reports bounded console witnesses plus per-generation JSONL containing every record collected
through the coherent observed cut. Assignment anchoring covers versions independently observed at
converged cuts, not unsampled historical versions.

### On-demand exact-outcome audit

No current single API is sufficient for an external route. `cluster_outcome_with_recovery_capsule`
does not return the retention floors, a separate floor read can race, and
`cluster_attempt_settlement` infers closure from the highest later outcome rather than returning the
requested body. Cycle 59 does not expose either as a stable classifier.

First add one bounded core result from one audited authority head. It must carry the requested
canonical attempt, the exact admission link when retained, both retention floors, and exactly one
of:

- a live exact outcome and, for Commit, a capsule loaded and validated before the same outcome head,
  Commit head, and floors are rechecked;
- only when no live exact outcome remains reachable, a matching committed, terminal, or `both`
  continuity anchor, explicitly marked artifact/capsule-ineligible; or
- `no_exact_retained_outcome`, without calling a later outcome the requested attempt's settlement
  and without inferring that the attempt is still open.

Below the artifact floor, exact lookup may be absent and even an anchor's artifacts are not live.
Between the artifact and terminal-history floors, retained Commits remain reachable through the
Commit chain while most Aborts are compacted except an exact anchor. At or above the terminal floor,
the live terminal chain is authoritative. The result must preserve those distinctions rather than
collapse every `None` into one state.

The audit path must be read-only: it requires a non-creating deployment-identity read/validation
path and permits no `load_or_create_deployment_id`, fallback LIST, head-pointer repair, pruning, or
other mutation. A missing deployment identity or published head is unavailable. One exact read may
otherwise fetch up to 4096 authority records, each capped at 256 KiB—a nearly 1-GiB worst-case
authority read before the 8-MiB Commit capsule. Explicit operation/read/response bounds must
therefore be frozen before HTTP. The harness invokes it only for the maximum, SLO violations, and
deliberately selected controls after timing capture—not every 500 ms. A response may expose the
verified capsule reference and a bounded non-sensitive summary, but normalized source cuts remain a
separate connector-contract blocker.

## Validation gates

Before this evidence can support a production soak:

1. Unit tests prove monotonic sequences, fixed capacity, physical overwrite-count/ring algebra,
   unread-window loss reporting, nanosecond overflow handling, exact pagination, and fail-closed
   authority changes.
2. Each of the leader, immediate-follower, deferred-follower, early-error, cancel, abort, inline
   tail, and aligned-resume paths produces exactly one ledger record per stall histogram sample.
3. HTTP tests cover bearer reload, startup/recovery/terminal gates, caps, stale generation,
   malformed cursors, pagination, and slow/oversized framing.
4. Harness tests reject gaps, duplicates, regressions, unread-window overwrite, and histogram
   disagreement. Physical eviction after successful export is allowed and checked against exact
   ring algebra.
5. The future exact-outcome audit tests reject inferred later-outcome settlement, both kinds of
   compacted history, orphan/missing capsules, and capsule mismatch.
6. A controlled engineering A/B compares identical fixed workloads with the Cycle 57 shared-store
   polling disabled and enabled. It diagnoses perturbation; it cannot convert either earlier red run
   into a pass.
7. The independently operated immutable release-binary soak remains mandatory after the complete
   state/backend/rebalance/source/sink vertical is ready.

## Cycle 58 disposition

Cycle 58 is an audit/design cycle. It adds no runtime recorder or endpoint because the current
retained facts cannot support a truthful exact-timing response. Cluster keyed/stateful admission
remains fail-closed under `[LDB-4007]`/`[LDB-0013]`; TidesDB remains stopped before runtime
integration; bounded memory remains reference-only; and production remains **NO-GO**.

## Cycle 59 disposition

Cycle 59 implements the bounded three-family ledger, protected paginated endpoint, and real-harness
consumer. The recorder is preallocated and nonblocking on the checkpoint-control path; it performs
no row-path, network, filesystem, object-store, serialization, or backend work. Histogram samples
and the corresponding ledger record close under the same guard scope. The consumer binds records
to the exact live process and independently binds each sampled converged assignment version to its
full certificate digest. It streams every collected record to JSONL while retaining fixed-size
diagnostics in memory and finalizes each process generation at a coherent observed
ledger/Prometheus cut.

A Windows/WSL2 engineering run of the optimized test binary at commit `7782a032` used static
three-node discovery, MinIO, Redpanda, 96 Kafka partitions, 400 records/s, one in-checkpoint leader
`kill -9`, and a 90-second configured tail. It passed in 207.20 s: all 79,996 acknowledged IDs were
observed. The engineering oracle tolerated and counted 2,758 duplicate output IDs but did not prove
their byte identity or sealed-cut replay legality. Exact reconciliation covered 392 barrier-pause
timing records across four process generations; the diagnostic `le=1.024` bucket contained 100%
of samples for all three families and the existing pipeline-stall gate passed. No record reported
deadline exhaustion or missing handoff, and the collector observed no recording loss, gap, or
unread-window overwrite. Artifacts through each observed cut remain in the run directory named in
the Cycle 59 review.

This is engineering evidence only. The real run predates `1a6dff80`, whose substitution-defense
change has focused deterministic test/lint coverage but was not empirically rerun. The protected
route still lacks a direct nonempty paginated HTTP test, the instrumentation needs a controlled A/B,
and exact full-checkpoint/restorable-gate plus
same-snapshot durable outcome/capsule evidence remain open. The run used the current nontransactional
Kafka path and therefore establishes no exactly-once claim. No keyed runtime, state backend,
admission flag, source/sink capability, or independent immutable release-binary soak changed;
production remains **NO-GO**.

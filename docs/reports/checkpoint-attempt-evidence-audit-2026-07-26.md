# Checkpoint-attempt evidence audit — 2026-07-26

## Decision

LaminarDB already retains strong recovery authority for a terminal cluster checkpoint, but it does
not retain exact per-process checkpoint-stall observations. Cycle 58 therefore stops before adding
another HTTP route. An endpoint assembled from the current histograms, logs, or latest-manifest
view would look useful while being unable to identify the two Cycle 57 latency violations.

The next implementation slice must keep two kinds of evidence separate:

1. a bounded, process-local timing ledger that records the exact attempts observed by the existing
   pipeline-stall timers, including sequence continuity and explicit loss; and
2. a future read-only, same-snapshot exact-outcome audit that joins selected attempts to retained
   immutable authority and, only for a live Commit, validates its content-addressed capsule.

Timing must not be added to the recovery-critical outcome or capsule formats. It is
participant-local, nondeterministic evidence, whereas those records are deterministic cluster
authority. No state backend, cluster admission, source/sink guarantee, or runtime data path changes
in this cycle.

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
| Pipeline pause | Pipeline-stall, local-barrier, aligned-resume, and restorable-gate histograms observe durations | Aggregate counts/sums/buckets only; no attempt, assignment, process generation, exact maximum, or complete deadline-exhaustion identity |
| Early failures | Drop-observing barrier timers record early returns | They remain anonymous histogram observations |
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
- [`pipeline_callback.rs`](../../crates/laminar-db/src/pipeline_callback.rs) starts the three
  drop-observing barrier timer scopes around lines 2960, 3176, and 5539.
- [`engine_metrics.rs`](../../crates/laminar-db/src/engine_metrics.rs) defines aggregate checkpoint
  histograms around lines 79–95 and 297–330.
- [`cluster_soak.rs`](../../crates/laminar-server/tests/cluster_soak.rs) currently derives its SLO
  only from Prometheus aggregates around lines 954–1002 and 1267–1536.

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

Today the left side and the durable right side exist independently. The middle observation is
anonymous and aggregate, so a test cannot name the attempt that exceeded 1024 ms, determine which
local stage dominated it, or prove that every observation in its measurement window was retained.
The lack of correlation also prevents assigning or excluding Cycle 57's 500-ms diagnostic polling
as a contributor to the 98.81% result.

The capsule's connector maps are opaque provider values, not a normalized exclusive-cut contract.
For example, Kafka persists a last-consumed inclusive offset and connector code converts it to the
next offset on restore. A future projection cannot label raw values “exclusive source cuts” until a
versioned per-connector normalization contract exists. Exposing raw connector metadata is also not
acceptable merely to make an oracle convenient.

The durable outcome does not prove an external sink transaction completed. Coordinated sink commit
has its own cursor/reconciliation lifecycle, and the current Kafka path remains at-least-once.
Joining an attempt to a live Commit capsule therefore proves a recovery cut, not source/state/sink
atomicity or exactly-once delivery.

## Frozen requirements for the next slice

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

The harness captures every process generation before kill and at the end, incrementally if the ring
would otherwise wrap. It rejects gaps and checks exact record counts/SLO classification against the
existing histogram deltas. It must report the exact violating attempt(s), maximum, local-barrier
duration, aligned-resume duration, role, and process generation.

### On-demand exact-outcome audit

No current single API is sufficient for an external route. `cluster_outcome_with_recovery_capsule`
does not return the retention floors, a separate floor read can race, and
`cluster_attempt_settlement` infers closure from the highest later outcome rather than returning the
requested body. Cycle 59 must not expose either as a stable classifier.

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

1. Unit tests prove monotonic sequences, fixed capacity, overwrite/loss reporting, nanosecond
   overflow handling, exact pagination, and fail-closed authority changes.
2. Each of the leader, immediate-follower, deferred-follower, early-error, cancel, abort, inline
   tail, and aligned-resume paths produces exactly one ledger record per stall histogram sample.
3. HTTP tests cover bearer reload, startup/recovery/terminal gates, caps, stale generation,
   malformed cursors, pagination, and slow/oversized framing.
4. Harness tests reject gaps, duplicates, regressions, overwritten windows, histogram disagreement,
   inferred later-outcome settlement, both kinds of compacted history, orphan/missing capsules, and
   capsule mismatch.
5. A controlled engineering A/B compares identical fixed workloads with the Cycle 57 shared-store
   polling disabled and enabled. It diagnoses perturbation; it cannot convert either earlier red run
   into a pass.
6. The independently operated immutable release-binary soak remains mandatory after the complete
   state/backend/rebalance/source/sink vertical is ready.

## Cycle 58 disposition

Cycle 58 is an audit/design cycle. It adds no runtime recorder or endpoint because the current
retained facts cannot support a truthful exact-timing response. Cluster keyed/stateful admission
remains fail-closed under `[LDB-4007]`/`[LDB-0013]`; TidesDB remains stopped before runtime
integration; bounded memory remains reference-only; and production remains **NO-GO**.

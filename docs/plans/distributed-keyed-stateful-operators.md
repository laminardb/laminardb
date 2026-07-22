# Phased plan: distributed keyed and stateful operators

- **Status:** Planned; no new cluster operator is admitted by this document
- **Date:** 2026-07-22
- **Decision:** [ADR-008](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md)
- **Baseline evidence:** [validation report](../reports/cluster-keyed-state-validation-2026-07-22.md)

## Objective

Admit distributed keyed aggregates, event-time windowed aggregates, and stateful joins one vertical
at a time without regressing LaminarDB's low-latency path or weakening its fail-closed guarantees.
The deliverable is production evidence—bounded resources, portable recovery, fenced rebalance,
fault correctness, and measured tail latency—not merely removal of `[LDB-4007]`.

The critical path is:

```text
0. contracts/evidence
        |
1. managed working state
        |
2. grouped aggregates
        |
3. event-time windows
        |
4. stateful joins
        |
5. materialized output (separate admission)
        |
6. production certification and gradual rollout
```

Performance, fault injection, compatibility, operational telemetry, and end-of-cycle review run
through every phase; they are not a final cleanup sprint.

## Scope and non-goals

In scope:

- fixed-vnode managed working state on local memory/NVMe;
- portable full/delta checkpoint artifacts in shared storage;
- assignment-fenced acquire, restore, activate, revoke, and cleanup;
- positive physical-plan capability descriptors;
- grouped built-in aggregates, event-time windows, and progressively broader joins;
- truthful byte/disk/timer accounting and controlled pressure behavior;
- local-vs-cluster differential, fault, rescale, and latency certification; and
- a separate distributed materialized-output lifecycle.

Not in the initial program:

- a new consensus service, Raft implementation, or control-plane rewrite;
- object-store-primary live state, a Hummock/Persist clone, or per-record remote state calls;
- unaligned checkpoints, dual-write migration, or Megaphone-style routing before measurements;
- cluster exactly-once certification, transactional sinks, or guarantee widening;
- arbitrary UDAFs, unbounded joins, implicit semantic TTL, or best-effort state eviction;
- one database/file per vnode; or
- a new crate or generic framework before two concrete consumers demonstrate the boundary.

## Preconditions

The following are blocking inputs rather than tasks to hand-wave inside an operator phase:

1. The existing cluster at-least-once checkpoint, exact-attempt seal, assignment fence, and recovery
   capsule must pass their own deterministic fault gates. Stateful admission cannot mask a known
   durability-authority defect.
2. Cluster sources used by a certified scenario must have a supported placement/handoff contract.
   A stateful operator does not make an unsupported singleton or consumer-group source safe.
3. The deployment's vnode count and partitioning ABI are immutable and present in pipeline identity.
4. Shared object storage remains required recovery authority; local state storage is configured and
   quota-controlled but may be lost completely.
5. Numerical production workload and latency/recovery targets are checked in during Phase 0.

## Admission progression

| Milestone | Newly eligible `CREATE STREAM` shapes | Still fail-closed |
|---|---|---|
| Current | Stateless projection/filter; one direct global aggregate stage | Every group/window/join; all MVs |
| Phase 2 | Grouped `COUNT`/`SUM`/`AVG`, append-only `MIN`/`MAX` | `DISTINCT`, arbitrary UDAF, changelog min/max, windows/joins |
| Phase 3A/B | Certified tumbling then hopping event-time aggregates | Sessions, processing-time/custom triggers, analytic frames, joins |
| Phase 3C/D | Certified session windows and bounded analytic frames | Unbounded frames and uncertified trigger modes |
| Phase 4A | Append-only inner equi-interval join | Outer/changelog/ASOF/temporal/unbounded joins |
| Phase 4B/C | Certified outer/semi/anti, changelog, ASOF/temporal variants | Any shape without finite managed retention/distribution proof |
| Phase 5 | Certified distributed MVs/read paths for already-supported operators | Global subscriptions/orderings without a sequencing proof |

Admission changes are granular capability flags tied to descriptors and tests. A later phase never
widens an earlier operator's function, update, timer, or output mode by accident.

## Phase 0 — Contract and evidence freeze

**Purpose:** make correctness, compatibility, and performance measurable before backend code sets
accidental contracts.

Work:

1. Accept or revise ADR-008 through an owner review; record unresolved decisions explicitly.
2. Check in a reproducible benchmark profile:
   - CPU, RAM, local NVMe, OS/filesystem, object-store RTT/bandwidth;
   - state smaller than RAM and state larger than RAM;
   - fixed and variable-width keys/values, key cardinality, Zipf/hot-key skew;
   - Arrow batch sizes, input rate, checkpoint cadence, and failure/rebalance schedule;
   - aggregate, window, and two-input join workloads; and
   - absolute p99/p99.9 latency, throughput, checkpoint-pause, RTO, RSS, and disk limits.
3. Specify the partition/state ABI and add golden vectors for every supported key type, including
   null, decimal, timestamp/timezone, floating-point edge cases, and composite keys.
4. Specify stable operator/table ID derivation, state-schema compatibility rules, and N/N-1 rolling
   upgrade/rollback behavior.
5. Add the mandatory capability descriptor to design tests. Inventory every current operator as
   `Stateless`, `GlobalSingleton`, `VnodeKeyed`, `RebuildableReplicated`, or `LocalOnly` without
   changing admission.
6. Run a bounded backend spike against the workload profile. Qualify RocksDB build/platform support,
   write batches, snapshots, prefix/range scans, bulk restore, range delete, native-memory controls,
   compaction stalls, disk-full behavior, and corruption detection. Compare with the in-memory
   reference; do not turn the spike into an alternative framework bake-off.
7. Freeze a fault-point vocabulary and output-oracle format shared by later phases.
8. Audit existing aggregate vnode code for reusable invariants versus map-specific logic. Resolve
   or document vestigial values such as the discarded `has_ownership_partitioned_state` result;
   do not carry dead compatibility scaffolding into the new API.

Exit gate:

- ADR accepted with named reviewers and no unresolved correctness decision;
- benchmark and numerical SLO/RTO profile is reproducible on a clean runner;
- golden ABI/schema vectors and compatibility policy pass;
- backend spike meets the profile or the ADR is reopened;
- every operator has an explicit current capability classification; and
- Cycle 0/Phase 0 review contains no unowned blocker.

No DDL guard is relaxed in this phase.

## Phase 1 — Managed working-state substrate

**Purpose:** build and prove the shared lifecycle before attaching a production SQL operator.

Work packages:

### 1A. Service and namespace

- Implement the smallest batched service from ADR-008 inside the existing state/database modules.
  Extract a crate only if dependency direction or a second non-DB consumer requires it.
- Add canonical physical prefixes, persisted local metadata, process locking, ABI/schema validation,
  and safe cleanup scoped to one resolved pipeline directory.
- Provide in-memory reference and RocksDB production implementations behind the same conformance
  suite.
- Reject wrong deployment/pipeline/ABI/schema/decided-checkpoint identity before exposing any key.

### 1B. Resource governor

- Reserve before mutation across Rust/Arrow/operator buffers and RocksDB native cache/memtables.
- Enforce separate memory, local bytes, restore staging, frozen-generation, and compaction-debt
  limits with one-batch documented slack.
- Define pressure states, bounded backpressure, health transitions, and a typed controlled-fault
  error. Test disk full and native allocation failure; never rely on the OS OOM killer.

### 1C. Checkpoint bridge

- Atomically journal state mutations per vnode with the logical write batch.
- Rotate/freeze generations at an aligned barrier and materialize portable per-vnode deltas
  asynchronously through the existing artifact backend and exact-attempt seal.
- Build periodic full bases, chain limits, abort/failed-capture rearming, and retained-generation
  backpressure.
- Prove full local-disk-loss recovery, checksum/corruption rejection, and N/N-1 decoding.

### 1D. Ownership lifecycle

- Implement `Unowned -> Acquiring -> Restoring -> Validated -> Active` and
  `Active -> Frozen/Draining -> Revoked` in the graph/runtime.
- Reuse assignment/process fences and restoring-output suppression.
- Bound acquired-vnode input buffering, bulk install, post-acquire full rebase, and revoked-range
  cleanup. Prove stale owners cannot read/write/publish after rotation.

Tests:

- backend model/conformance tests over random atomic batches, scans, deletes, snapshots, and restore;
- crash before/after write, freeze, encode, upload, seal, install, activate, revoke, and range delete;
- checksum, truncated artifact, wrong ABI/schema/owner, disk full, object-store stall, compaction
  stall, and complete local directory loss;
- generation/iterator leak tests and resident/native byte accounting under sustained churn; and
- microbenchmarks with state both inside and outside cache, concurrent checkpoint, and restore.

Exit gate:

- zero unbounded retained collection in the substrate or test harness;
- barrier freeze cost is independent of total state in complexity and confirmed by size scaling;
- last sealed cut restores after local loss with exact model state;
- memory/disk/compaction limits fail predictably without corruption or OOM;
- no stateful SQL capability is yet enabled; and
- Phase 1 cycle review is approved.

## Phase 2 — Grouped aggregate vertical

**Purpose:** replace the latent aggregate map lifecycle with managed state and certify the first
distributed keyed operator end to end.

Work:

1. Add the aggregate `VnodeKeyed` descriptor and make planner admission consume it.
2. Reuse the existing canonical pre-aggregate shuffle and ownership/barrier fences. Remove duplicate
   map-era dirty/full/delta tracking only after the managed path is equivalent and all callers move.
3. Encode group accumulator, last-updated metadata, and last-emitted changelog value as stable state
   tables. Apply one Arrow batch with grouped multi-get and one atomic mutation batch.
4. Implement reviewed encodings for `COUNT`, `SUM`, `AVG`, and append-only `MIN`/`MAX`; preserve null,
   overflow, decimal, floating-point, update, and retraction semantics.
5. Keep `DISTINCT`, UDAFs, changelog `MIN`/`MAX`, multi-stage/derived fallback, and cluster MVs closed.
6. Add per-operator/vnode keys, bytes, dirty bytes, cache hit rate, batch read/write, skew, checkpoint,
   restore, and pressure metrics.
7. Remove the one-million-group safety fiction once the new hard byte policy is the only admitted
   cluster path. Embedded compatibility may retain the old implementation temporarily behind an
   explicit local-only path, with a removal issue and owner.

Correctness matrix:

- random append/update/retract batches versus embedded full recomputation;
- all supported Arrow key/value types, nulls, overflow/error paths, hash golden vectors, many
  aggregates in one logical stage, and hot keys;
- multi-node remote shuffle with every vnode boundary and zero-vnode workers;
- checkpoints during dirty state, failed capture, owner death before/after seal, `1 -> 3 -> 2`
  rotation, stale messages, and repeated acquire/revoke;
- output oracle under the advertised at-least-once boundary: no lost state/result, documented
  replay duplicates only, and no double-application inside restored state; and
- cache-resident and spill-heavy latency/throughput/compaction profiles.

Exit gate:

- the newly admitted aggregate matrix is exactly enumerated and all other shapes still produce
  `[LDB-4007]` before mutation;
- fault/differential suites report zero state divergence;
- numerical p99/p99.9, checkpoint, resource, and RTO targets pass on the Phase 0 profile;
- local embedded performance has a reviewed regression result;
- rolling upgrade/rollback and checkpoint compatibility pass; and
- Phase 2 review approves removal of obsolete map code and docs.

Rollout starts internal/experimental, then a canary cluster allowlist, then default admission only
after at least one release cycle of telemetry. Rollback disables new DDL while retaining the reader
needed to drain or restore already-created pipelines.

## Phase 3 — Event-time windows and timers

**Purpose:** add managed time/frontier semantics instead of treating a window as only another group
key.

### 3A. Tumbling windows

- Implement vnode-owned event-time timer tables, input watermark/frontier checkpointing, allowed
  lateness, trigger state, output/retraction markers, and atomic fire/cleanup.
- Unify running and window-close state on the managed representation; do not preserve two unrelated
  map/checkpoint paths.
- Certify append-only tumbling aggregates first.

### 3B. Hopping windows

- Add bounded fan-out accounting, timer coalescing, incremental panes only if benchmarks justify
  them, and cleanup proof for overlapping windows.

### 3C. Session windows

- Add ordered range lookup, deterministic merge, timer replacement, late merge/retraction, and
  atomic multi-window mutation. Session support has its own admission bit and fault matrix.

### 3D. Analytic frames

- Classify bounded row/range frames separately from event-time grouping windows. Require a stable
  ordering/partition proof and byte-bounded frame state. Unbounded frames remain rejected unless
  their resource contract is explicit.

Tests and exit gates:

- differential event-time oracle across out-of-order rows, equal timestamps, empty windows, nulls,
  watermark stalls/regression attempts, allowed lateness, late drops, and session merges;
- crash at timer selection, output, deletion, watermark checkpoint, and post-restore refire;
- skewed windows, millions of timers, disk pressure, checkpoint/rebalance with pending timers, and
  owner change exactly at close time;
- no premature fire, lost fire, unbounded retained closed window, or silent late-data policy;
- each subphase independently meets the Phase 0 tail/resource/RTO profile and completes its cycle
  review before its admission bit changes.

Processing-time/custom-trigger support is not implied by event-time certification.

## Phase 4 — Stateful joins

**Purpose:** build co-partitioned, time-bounded two-input state with explicit output semantics.

### 4A. Inner interval join

- Canonicalize one equi-join key ABI and install required exchanges on both inputs.
- Store two vnode-owned, time-ordered multisets keyed by join key/event time/row identity.
- Persist both watermarks, bounds, eviction timers, and row multiplicity in the same checkpoint cut.
- Probe bounded ranges in batches and atomically store input/output bookkeeping.

### 4B. Outer, semi, and anti variants

- Add unmatched-row timers, null-padding markers, delayed output rules, and deterministic retractions.
- Prove that opposite-side watermarks, not wall-clock polling, authorize unmatched output/cleanup.

### 4C. Changelog joins

- Add signed multiplicities, unique/deterministic row identity, join-result weights, and retraction
  state. Test negative/zero multiplicity and cross-cycle duplicate inputs.

### 4D. ASOF, temporal, and lookup variants

- Add versioned ordered history and direction/tie rules for ASOF/temporal joins.
- Keep temporal table/snapshot version in the checkpoint cut.
- Design lookup state as `RebuildableReplicated` or vnode-keyed mutable state according to its
  source contract; do not silently classify a remote cache as durable join state.

Tests and exit gates:

- differential SQL oracle over match cardinality, nulls, duplicates, equal timestamps, interval
  boundaries, out-of-order data, watermarks, and changelog weights;
- two-input barrier/replay permutations, one-side pause/failure, network reorder/loss, owner change,
  disk pressure, and crash around unmatched output/eviction;
- hot join key and asymmetric-rate profiles with bounded probe/result batches and backpressure;
- finite state follows from declared interval/watermark/retention semantics—an internal TTL is never
  the proof;
- each join family has a separate admission flag, compatibility vector, production metrics, and
  approved cycle review.

Unbounded joins remain `[LDB-4007]` until a separately reviewed semantic retention contract exists.

## Phase 5 — Distributed materialized output

**Purpose:** remove the independent blanket cluster MV rejection only after retained output and
reads have a distributed lifecycle.

Work:

- define output partitioning and stable row identity for append and changelog/upsert MVs;
- write output through assignment-fenced managed tables and checkpoint it with upstream operator
  state;
- route point/range reads to owners or implement a reviewed distributed merge;
- specify read snapshot/epoch consistency during rebalance and recovery;
- restore/activate MV output before serving it;
- define cluster subscription ordering, replay, and backpressure separately; and
- prevent a stateless query from bypassing MV output-state admission.

Exit gate:

- stateless and certified stateful MVs survive node loss and `1 -> 3 -> 2` rotation with a read
  oracle and no stale-owner response;
- checkpoint and read consistency are documented without claiming external exactly-once;
- query/subscribe latency and retained-output quotas pass; and
- Phase 5 review approves the exact MV matrix. Named stateful streams may ship earlier.

## Phase 6 — Production certification and rollout

This phase does not add operator semantics. It closes cross-cutting evidence:

1. Run the complete PGVal-style matrix over data rate, topology, partitions, skew, checkpoints,
   process death, network disruption, object-store stalls, disk full/corruption, compaction stalls,
   and rolling upgrade/rollback.
2. Run sustained soak with leak slopes for Rust heap, RocksDB native memory, file descriptors,
   iterators/snapshots, local bytes, frozen generations, timers, and checkpoint artifacts.
3. Publish reproducible p50/p95/p99/p99.9 and RTO results for cache-resident, spill-heavy, skewed,
   checkpointing, and rebalancing workloads. A skipped external test is reported as missing
   evidence, never a pass.
4. Exercise operational alerts, capacity exhaustion, local disk replacement, corrupt checkpoint,
   failed upgrade, and admission rollback runbooks.
5. Audit security of local state directories, credentials, artifact encryption/integrity, log/error
   redaction, and tenant/pipeline quota isolation.
6. Remove experimental flags only per operator matrix, with staged canary percentages and automatic
   rollback thresholds.

General availability requires zero correctness-oracle failures, all committed numerical gates,
approved production/operations review, and no unresolved severity-1/2 issue. This does not remove
`[LDB-0013]`; cluster exactly-once has its own plan and evidence.

## End-of-cycle review contract

Every numbered phase and lettered operator-admission subphase ends with a committed review under
`docs/reviews/distributed-keyed-state-cycle-<n>.md`. The review is written after tests and before the
admission/phase merge. It must name evidence and owners rather than checking boxes by assertion.

Required passes:

1. **AI-slop:** verify every symbol/path/config/source claim against the tree; remove speculative
   APIs, fake precision, duplicated prose, cargo-cult architecture, stale TODOs, and generated filler.
2. **Over-engineering:** challenge every abstraction, dependency, feature flag, migration mode, and
   public option; record what is deliberately deferred and why the smallest vertical is insufficient
   without any retained mechanism.
3. **Unused/dead code:** run compiler/clippy feature matrices plus reachability/search review; remove
   superseded maps, hooks, adapters, metrics, configs, test helpers, and ignored return values, or
   assign a dated removal issue.
4. **Production readiness:** review failure containment, resource bounds, security, upgrades,
   rollback, observability, on-call actions, data compatibility, and evidence against numerical
   SLO/RTO gates.
5. **Documentation:** keep ADR as decision authority and this file as sequencing authority; update
   public capability docs, remove superseded diaries/research, test every link, and cut repetition.
6. **Tests:** list exact commands/results, skipped suites and prerequisites, nondeterminism/retry
   counts, fault coverage, differential oracle, performance environment, and coverage gaps. A test
   that matches zero cases or needs unrecorded temporary instrumentation is a failure.

The reviewer must conclude `APPROVE`, `APPROVE WITH OWNED FOLLOW-UPS`, or `BLOCK`. A block leaves the
admission flag closed. The current documentation cycle's concrete review is
[Cycle 0](../reviews/distributed-keyed-state-cycle-0.md).

## Commit and change discipline

- Commit contract/test scaffolding, substrate slices, operator migration, and admission changes
  separately; do not combine a backend rewrite with a guard removal.
- Each commit must build its affected feature matrix and preserve fail-closed behavior.
- Admission is the last commit in an operator cycle, after recovery/performance evidence.
- Compatibility readers land before writers; rollback readers remain until every supported cut is
  beyond the old format.
- Destructive local cleanup is namespace-scoped, assignment-fenced, and independently tested.
- Avoid drive-by control-plane, connector, or SQL syntax changes; create a separate ADR/plan if
  measurements reveal that expansion is necessary.

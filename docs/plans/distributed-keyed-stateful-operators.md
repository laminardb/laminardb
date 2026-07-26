# Phased plan: distributed keyed and stateful operators

- **Status:** Planned and backend-gated; exact Cargo package `tidesdb v0.11.1` stopped at T0; no new cluster
  operator is admitted by this document
- **Date:** 2026-07-22
- **Last reconciled:** 2026-07-25 during Cycle 45
- **Decision:** [ADR-008](../architecture-decisions/ADR-008-managed-vnode-keyed-state.md)
- **Baseline evidence:** [validation report](../reports/cluster-keyed-state-validation-2026-07-22.md)
- **Phase 0 execution:** [file-level implementation plan](distributed-keyed-state-phase-0-execution.md)

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

The Cycle 20 [working-state placement analysis](../reports/state-working-state-options-2026-07-24.md)
separates the capability from a named engine but does not change sequencing authority. Phase 1
remains blocked by the existing Phase 0 review gate. Any later gate split requires an accepted ADR/
plan amendment with named scope and owners. The intended broad/variable-state production profile
  still waits for the selected official-binding TidesDB local-spill target to qualify. Cycle 41
  stops exact v0.11.1/native 9.3.6 at T0 and cancels T1; backend-neutral Laminar lifecycle,
  publication-boundary, checkpoint, resource-admission, and health-composition work may continue,
  while the TidesDB path waits for a new official package and repeated source closure. The package choice is not runtime
  authorization or production admission; alternatives require an explicit owner decision. The
  project decision keeps bounded memory as a reference/conformance implementation
  only; it has no cluster product schedule or production-soak matrix under this plan.

Cycle 42 completes one backend-neutral, runtime-consumed correction: a normal incremental
aggregate error after state application may have begun is recovery-required, and the coordinator
does not publish that cycle or start a due checkpoint. It does not add managed working state,
qualify a backend, or satisfy the future native-root poison/fresh-restore contract.

Cycle 43 makes analytic frame-history advancement transactional with its residual projection. The
candidate tail is installed only after projection succeeds, so an ordinary projection failure or
cancellation remains failed-before-apply and cannot double-append on replay. This changes neither
the cluster rejection nor any backend gate.

Cycle 44 makes returned ASOF failures replay-safe: ingest errors remain pre-apply; join/projection
errors become recovery-required only after current-cycle right state or schema changes; and a
returned eviction error is recovery-required after pruning begins. Panic/cancellation poisoning and
empty-buffer right-schema checkpoint/restore remain open. No cluster capability is admitted.

Cycle 45 versions the ASOF operator checkpoint without changing its v1 buffer body. A bounded schema
appendix preserves learned right-side shape when no rows remain; non-empty v1 checkpoints migrate by
deriving that shape, while ambiguous empty v1 LEFT checkpoints fail recovery-closed. Restore also
checks buffer/index/schema coherence before one atomic install, and live right-schema drift fails
before ingest. This does not solve cancellation/panic poisoning, distribution, rebalance, delivery,
backend qualification, or independent soak, and admits no cluster capability.

## Scope and non-goals

In scope:

- fixed-vnode managed working state on local memory/NVMe;
- portable full/delta checkpoint artifacts in shared storage;
- assignment-fenced acquire, restore, activate, revoke, and cleanup;
- positive physical-plan capability descriptors;
- grouped built-in aggregates, event-time windows, and progressively broader joins;
- truthful byte/disk/timer accounting and controlled pressure behavior;
- local-vs-cluster differential, fault, rescale, and latency certification;
- source/operator/sink delivery compatibility and checkpoint-tail certification; and
- a separate distributed materialized-output lifecycle.

Not in the initial program:

- a new consensus service, Raft implementation, or control-plane rewrite;
- object-store-primary live state, a Hummock/Persist clone, or per-record remote state calls;
- unaligned checkpoints, dual-write migration, or Megaphone-style routing before measurements;
- cluster exactly-once certification, checkpoint-coupled transactional sink commits, or guarantee
  widening; bounded Kafka transactions used only for externally auditable writer fencing remain
  required by the initial at-least-once scenario;
- arbitrary UDAFs, unbounded joins, implicit semantic TTL, or best-effort state eviction;
- one database/file per vnode; or
- a new crate or generic framework before two concrete consumers demonstrate the boundary.

## Preconditions

The following are blocking inputs rather than tasks to hand-wave inside an operator phase:

1. The existing cluster at-least-once checkpoint, exact-attempt seal, assignment fence, and recovery
   capsule must pass their own deterministic fault gates. Stateful admission cannot mask a known
   durability-authority defect.
2. Cluster sources used by a certified scenario must be non-ephemeral, `Splittable`, and have a
   supported assignment-scoped checkpoint/handoff contract. At this baseline, Kafka is the only
   built-in external source that qualifies. A stateful operator does not make a singleton or
   consumer-group-only source safe.
3. A certified external-output scenario needs a `DurableAtLeastOnce + MultiWriter` sink that accepts
   the operator's output mode. A connector mismatch remains fail-closed before I/O.
4. The deployment's vnode count and partitioning ABI are immutable and present in pipeline identity.
5. Shared object storage remains required recovery authority; local state storage is configured and
   quota-controlled but may be lost completely.
6. Numerical production workload and latency/recovery targets are checked in during Phase 0.

## Admission progression

| Milestone | Newly eligible `CREATE STREAM` shapes | Still fail-closed |
|---|---|---|
| Current | Stateless projection/filter; one direct global aggregate stage | Every group/window/join; all MVs |
| Phase 2 first gate | Append-only grouped `COUNT(*)` plus `SUM(Int64)` | All broader aggregates, retractions, windows/joins |
| Later Phase 2 gates | Reviewed broader `COUNT`/`SUM`, `AVG`, append-only `MIN`/`MAX` | `DISTINCT`, arbitrary UDAF, changelog min/max, windows/joins |
| Phase 3A/B | Certified tumbling then hopping event-time aggregates | Sessions, processing-time/custom triggers, analytic frames, joins |
| Phase 3C/D | Certified session windows and bounded analytic frames | Unbounded frames and uncertified trigger modes |
| Phase 4A | Append-only inner equi-interval join | Outer/changelog/ASOF/temporal/unbounded joins |
| Phase 4B/C | Certified outer/semi/anti, changelog, ASOF/temporal variants | Any shape without finite managed retention/distribution proof |
| Phase 5 | Certified distributed MVs/read paths for already-supported operators | Global subscriptions/orderings without a sequencing proof |

Admission changes are granular capability flags tied to descriptors and tests. A later phase never
widens an earlier operator's function, update, timer, or output mode by accident.

## Delivery compatibility gate

The release unit is a certified source/operator/output/sink scenario, not an operator name in
isolation:

| Dimension | Initial cluster requirement | Consequence |
|---|---|---|
| Runtime guarantee | `AtLeastOnce` | `BestEffort` and `ExactlyOnce` remain rejected; the latter stays behind `[LDB-0013]` |
| Source | Non-ephemeral, `Splittable`, assignment-scoped handoff | Kafka is the only current built-in external source path; source partitions and SQL-key vnodes remain distinct |
| Operator state | One coordinator-admitted state/timer/output-bookkeeping cut with a durable terminal Commit and source cursor | Replay cannot double-apply internal state; externally flushed results may repeat |
| Changed-group append snapshots | `DurableAtLeastOnce + MultiWriter + AppendOnly` plus externally auditable writer fencing | One current row per touched group/batch; versions increase per authority interval, while a fenced recovery interval may replay an older committed prefix |
| Retraction/changelog output | `DurableAtLeastOnce + MultiWriter + FullChangelog`, or a new assignment-fenced mutable-sink contract | No current built-in cluster sink qualifies, so these combinations remain closed |

Checkpoint certification preserves CP-5 ordering: drain/enqueue operator output, flush every
durable sink, then seal source positions. It measures real sink flush and state-capture latency in
the same deadline. A stable output identity is part of the state ABI so replay can be recognized,
but at-least-once permits an externally visible duplicate after a crash. Exactly-once is a later
per-combination program requiring an exact-certified source and leader-term-fenced external commit;
local backend durability is neither necessary nor sufficient for that claim.

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
   - source replay/handoff and sink-flush latency/backpressure profiles;
   - aggregate, window, and two-input join workloads; and
   - absolute p99/p99.9 latency, throughput, checkpoint-pause, RTO, RSS, disk, artifact/decode,
     chain-depth, operator/vnode-count, and restore-staging limits.
   The current candidate numerical gates live only in the machine-readable
   [`linux-nvme-v3` candidate](../../tools/state-backend-qual/profiles/linux-nvme-v3.candidate.json),
   which remains explicitly unapproved, execution-ineligible, and not evidence. Its ownership map assigns backend,
   artifact-conformance, and product-integration sections to different executors; an LSM run cannot
   satisfy sink, checkpoint, or failover gates. The v1 profile remains an immutable regression
   fixture and cannot be used by a new runner plan; v2 is likewise an immutable regression fixture.
3. Specify the partition/state ABI and add golden vectors for every admitted key type plus explicit
   rejection vectors for floating-point, nested, and other excluded types. Persist hydrated routing
   identity separately from the artifact's Laminar-owned state contract. Treat restored routing
   bytes as opaque unless a panic-free strict decoder has independently validated them.
4. Specify stable operator/table ID derivation, concrete builtin codec registry, Laminar-owned codec
   versions, checked arithmetic/null semantics, a dedicated bounded row DTO and outer artifact
   directory, populated-state goldens, and N/N-1 rolling upgrade/rollback behavior. Initial managed
   state is append-only `COUNT(*)` plus `SUM(Int64)`; no live DataFusion/rkyv type is durable ABI.
   The outer directory has manifest-selected magic/version, canonical BODY entries, explicit
   unchanged-parent REFERENCE entries, and no fallback decoder. Reserve encoded bytes before fetch;
   account decode/ingestion scratch separately.
5. Add the mandatory capability descriptor to design tests. Inventory every current operator as
   `Stateless`, `GlobalSingleton`, `VnodeKeyed`, `RebuildableReplicated`, or `LocalOnly` without
   changing admission.
6. Close the exact candidate's DKS-Q2-006 mechanism gate before adapter work. Cycle 17 stopped the
   Cycle 16 RocksDB stall-only recommendation at source proof because v1's maintenance-debt arm
   cannot be closed by that narrow binding. That work now remains frozen v4/reference provenance,
   not an active product track. The Cycle 18
   [decision matrix](../reports/state-backend-contract-decision-matrix-2026-07-24.md) recommends an
   additive maintenance-health successor. Cycle 21 records the direction approval, and Cycle 38
   accepts the consolidated contract for validation-only implementation without a GitHub approval
    workflow. No candidate construction or execution authority follows. Cycle 38 made TidesDB the
    preferred local-spill candidate instead of RocksDB; Cycle 40 selects the official
    `tidesdb/tidesdb-rs` binding, Cargo package `tidesdb v0.11.1`, with native 9.3.6 as the exact
    prescreen subject and only integration line, without
    qualifying or admitting it. Cycle 41's
    [T0 source closure](../reports/tidesdb-rs-t0-source-closure-2026-07-25.md) stops that release:
    later one-CF correctness/memory-safety fixes are absent; transactions can acknowledge a short
    partial batch; and the general cgroup envelope and mandatory public health facts remain
    unclosed. Restricted owner/lifetime containment passes. A future full-key verified-commit/
    fail-stop design can address only acknowledgement and must pass hot-path/fault gates. The accepted
    [package design](../architecture-decisions/tidesdb-local-state-successor-design.md) permits only
    a restricted facade with one database, one retained fixed CF, one dedicated bounded blocking
    lane, copied values, transaction-scoped iterators, and deterministic child-before-parent
    shutdown. It prohibits callbacks, package handles crossing the facade, private FFI, raw handles,
    patches/forks, native/system-library substitution, and unsafe workarounds. Opening or serving
    prior native state, native checkpoint, remote mode, strict replay, and per-batch `FULL` are
    outside the initial profile. T1, a runtime dependency, an adapter, and successor profile
    identities are cancelled for v0.11.1. A future official native/package release repeats the
    complete one-working-day/zero-machine-hour T0; only a pass may fund T1's at-most-two-working-day/
    four-machine-hour isolated package feasibility. Cgroup
    resources, maintenance health, immutable cuts, concurrency, tails, faults, portable restore,
    delivery, and independent soak remain hard gates. TidesDB native remote storage stays disabled.
    Cycle 19's reviewed
   [candidate mappings](../reports/state-backend-maintenance-health-mapping-designs-2026-07-24.md)
   define the historical RocksDB source/binding closure and Fjall scheduler/lifecycle closure used
   by the immutable v4 reference lineage. Redb 4.1.0 is parked after its Cycle 34 design timebox; it has no scheduled
   protocol or adapter work and may reopen only under the bounded micro-prescreen charter recorded
   in its canonical protocol. Unmodified Fjall 3.1.8 and SurrealKV 0.21.2 do not proceed to adapters.
   These engine gates apply to the general local-spill profile. They are not an architectural need
   of the in-memory reference, but the current Phase 0 gate still blocks Phase 1. Run any later
   package-admitted TidesDB subject through the bounded successor profile: Arrow-batch-sized atomic
   requests, realistic
   hot/cold multi-key reads, timer scans, snapshot/export overlap, sorted restore, vnode drop/GC,
   maintenance pressure/write stalls, hard memory/disk/FD limits, `kill -9`, torn/corrupt data,
   `ENOSPC`, and N/N-1 format rehearsal. Include 24–72-hour churn/TTL soak. A pass qualifies the
   chosen target for later integration; a hard failure disqualifies it and returns backend choice to
   an explicit owner decision.
7. Record the complete delivery matrix: source consistency/topology and handoff; operator update
   mode and output identity; sink durability/topology/input mode; CP-5 ordering; permitted ALO
   duplicates; and combinations that remain closed. Benchmark at least one real certified source
   and sink rather than only an in-memory harness. Kafka output needs broker-enforced writer fencing
   plus partition fence markers; the source oracle derives its ledger by reconciling durable intents
   with actual broker records rather than acknowledgement callbacks.
8. Freeze a fault-point vocabulary and output-oracle format shared by later phases. Cross source
   drain/replay, state mutation/freeze, timer fire, output enqueue, sink flush, durable decision,
   external publication, assignment rotation, and ambiguous commit.
9. Freeze an independent production-soak charter: release artifact and topology, real connector and
   object-store dependencies, external oracle, minimum duration/event volume, scheduled faults and
   rebalances, leak-slope/latency/progress thresholds, raw-artifact retention, invalid-run rules,
   and a reviewer who did not implement the operator/backend under test.
10. Audit existing aggregate vnode code for reusable invariants versus map-specific logic. Resolve
   or document vestigial values such as the discarded `has_ownership_partitioned_state` result;
   do not carry dead compatibility scaffolding into the new API.

Exit gate:

- ADR accepted with named reviewers and no unresolved correctness decision;
- benchmark and numerical SLO/RTO profile is reproducible on a clean runner;
- golden ABI/schema vectors and compatibility policy pass;
- the placement-neutral service/lifecycle and in-memory conformance subject are reviewable without
  implying admission; before broad-profile admission, the selected official-binding TidesDB target passes
  reproducible conformance, latency, resource, fault and operability gates, or it is disqualified
  and the profile stays closed; the in-memory subject remains reference/conformance-only and
  supplies no admission evidence;
- at least one source/operator/append-sink scenario has a complete ALO oracle and every unsupported
  output/delivery combination has a fail-closed assertion;
- the independent production-soak charter is approved before implementation results can influence
  its duration, workload, oracle, or thresholds;
- every operator has an explicit current capability classification; and
- the latest Phase 0 cycle review contains no unowned blocker.

No DDL guard is relaxed in this phase.

## Phase 1 — Managed working-state substrate

**Purpose:** build and prove the shared lifecycle before attaching a production SQL operator.

Work packages:

### 1A. Service and namespace

- Implement the smallest batched service from ADR-008 inside the existing state/database modules.
  Extract a crate only if dependency direction or a second non-DB consumer requires it.
- Add canonical logical prefixes and ABI/schema validation. The local-spill implementation also adds
  persisted metadata, process locking and safe cleanup scoped to one resolved pipeline directory.
- Provide the in-memory semantic/lifecycle implementation first and the Phase-0-selected local-
  spill backend behind the same contract and conformance suite. Neither implementation changes
  admission by existing; do not retain losing disk qualification adapters.
- For TidesDB local spill, use only the official `tidesdb/tidesdb-rs` binding (Cargo package
  `tidesdb`) behind the restricted package facade: one
  worker-local database, one retained fixed physical managed-state CF, one dedicated bounded
  blocking lane, and logical pipeline/operator/table/vnode/generation prefixes. Do not allocate a
  database or physical tree per vnode or expose package types outside the facade.
- Encode hot values with a compact schema-versioned binary format. Do not use per-group Arrow IPC,
  live DataFusion/rkyv checkpoint types, read-before-write accounting, or the removed cold-tier
  wrapper.
- Reject wrong deployment/pipeline/ABI/schema/decided-checkpoint identity before exposing any key.

### 1B. Hot-path scheduler

- Build and cache immutable codec/schema contracts at planning or initialization. Concrete-UDF
  checks, schema canonicalization, dependency-version selection, SHA-256, IPC/rkyv parsing, and
  compatibility lookup never run per row or per processing batch; post-freeze artifact work runs
  off the compute/event-loop thread.
- Deduplicate state keys per Arrow batch and submit one logical multi-read plus one atomic mutation
  batch; a backend without native multi-get must still satisfy the same batched latency contract.
- Complete cache-only work inline only when it cannot block. Route every disk-capable call to a
  long-lived bounded blocking-worker pool; do not create a future or `spawn_blocking` task per row.
- Preserve mutation order within each vnode/table lane while allowing independent lanes to run in
  parallel. Bound queue bytes, age, and concurrency, and propagate storage pressure to ingestion.
- Defer a cold Arrow batch as one unit with bounded input and watermark holds. Aligned barriers drain
  all pre-cut state requests before freezing; the compute/event-loop thread never performs LSM I/O.

### 1C. Resource governor

- Reserve before mutation across Rust/Arrow/operator buffers and, when applicable, engine cache/
  memtables/journal, snapshots/iterators/pinned values, OS page cache, and native memory.
- Enforce memory, restore-staging and frozen-generation limits for every profile. Local-spill
  profiles additionally enforce local-byte and maintenance-debt limits. Every limit has at most one
  documented batch of slack.
- Define pressure states, bounded backpressure, health transitions, and a typed controlled-fault
  error. Every profile tests reservation exhaustion without cursor/output advance or OOM; local-
  spill profiles additionally test disk full and applicable native allocation failure. Never rely
  on the OS OOM killer.

### 1D. Checkpoint bridge

- Record state mutations atomically per vnode and generation with the logical write batch; a local
  engine journal is an implementation detail, not cluster recovery authority.
- Rotate/freeze generations at an aligned barrier and materialize portable per-vnode deltas
  asynchronously through the existing artifact backend and exact-attempt seal.
- Build periodic full bases, chain limits, abort/failed-capture rearming, and retained-generation
  backpressure.
- Prove complete local working-state loss recovery, checksum/corruption rejection and N/N-1
  decoding; local-spill profiles additionally prove physical local-disk loss.

### 1E. Ownership lifecycle

- Implement `Unowned -> Acquiring -> Restoring -> Validated -> Active` and
  `Active -> Frozen/Draining -> Revoked` in the graph/runtime.
- Replace the split acquired/revoked staging maps with one exact transition identity containing
  the committed cut, checkpoint fence, target assignment/vnode count/owner digest, acquired
  chains, revoked set, and authoritative lifecycle roster. Never convert a missing chain to empty.
- Add a mandatory state-lifecycle interface separate from ordinary `GraphOperator` methods.
  Stateful capability without that interface is a preflight error; stateless operators remain
  legitimate nonparticipants, and no successful default can discard named state.
- Preflight the complete batch before callbacks, prepare all operator/vnode shadows, and abort all
  shadows on error while retaining the exact inbox item. With intake closed, enter exclusive graph
  publication, use the rotation fence to revalidate assignment/process scope, and perform only
  infallible shard/generation swaps. Retain old handles for destruction after the short section;
  activate the complete set and remove the inbox item only after every operator publishes.
- Require an authoritative, canonical operator roster with an explicit `BODY` or `REFERENCE` for
  every operator/vnode pair. Each BODY declares `FULL`/`DELTA`/`EMPTY`, and every resolved chain
  terminates in `FULL` or `EMPTY`. Omission, duplicate names, mixed attempts, and topology drift
  fail before prepare.
- Build and cache an uninitialized SQL operator's exact plan/codec contract in a pure, fallible graph
  construction phase before artifact fetch. Prepare consumes only preflighted rows under that
  contract; activation cannot precede semantic validation or fall back to node-local DataFusion
  state.
- Reuse assignment/process fences and restoring-output suppression. Intake remains closed while
  the current assignment has restoring vnodes or a staged transition.
- Bound acquired-vnode input buffering, bulk install, post-acquire full rebase, and revoked-range
  cleanup. Prove stale owners cannot read/write/publish after rotation.
- Introduce vnode-owned state shards before the aggregate migration so acquire/revoke publication
  is a bounded pointer swap rather than a full-map scan.

Tests:

- backend model/conformance tests over random atomic batches, scans, deletes, snapshots, and restore;
- crash before/after write, freeze, encode, upload, seal, install, activate, revoke, and range delete;
- late-operator and later-vnode prepare failure leaves all live state unchanged, retains the exact
  staged transition, and activates no vnode; an explicit `EMPTY` base removes stale state while
  missing/extra/duplicate roster entries fail before prepare;
- uninitialized operators cannot activate after byte staging alone; exact semantic/state-contract
  goldens, same-name custom UDAF rejection, global vnode-0, truncation at every envelope/row
  boundary, declared length/count max and max-plus-one, reserved fields, unknown versions, duplicate
  or out-of-order/cross-vnode keys, trailing bytes, and every restore reservation fail closed without
  passing managed artifact bytes to Arrow. Any future IPC codec owns its separate framing,
  compression, dictionary, decoded-expansion, and second-batch/EOS matrix;
- checksum, truncated artifact, wrong ABI/schema/owner, object-store stall and complete local state
  loss; local-spill profiles also cover disk full/corruption and maintenance stalls;
- generation/iterator leak tests and resident/native byte accounting under sustained churn;
- scheduler saturation tests proving bounded queues, lane order, watermark holds, and no event-loop
  blocking; and
- microbenchmarks with concurrent checkpoint and restore; local-spill profiles cover state both
  inside and outside cache.

Exit gate:

- zero unbounded retained collection in the substrate or test harness;
- barrier freeze cost is independent of total state in complexity and confirmed by size scaling;
- latest coordinator-admitted cut with a durable terminal Commit restores after local loss with
  exact model state;
- applicable memory/disk/maintenance limits fail predictably without corruption or OOM;
- no stateful SQL capability is yet enabled; and
- Phase 1 cycle review is approved.

## Phase 2 — Grouped aggregate vertical

**Purpose:** replace the latent aggregate map lifecycle with managed state and certify the first
distributed keyed operator end to end.

Work:

1. Add the aggregate `VnodeKeyed` descriptor and make planner admission consume it.
   Cache the exact codec/schema contract on the physical operator; processing only reuses the
   existing encoded key and static vnode mapping.
2. Reuse the existing canonical pre-aggregate shuffle and ownership/barrier fences. Remove duplicate
   map-era dirty/full/delta tracking only after the managed path is equivalent and all callers move.
3. Encode only named semantic state. For the first vertical that is the canonical group key, checked
   count, and checked `Int64` SUM non-null count/accumulator; map-era `last_updated_ms` and `last_emitted`
   are not copied without a consumer. Apply one Arrow input batch with one grouped state read and one
   atomic mutation/output-enqueue batch; no record performs its own blocking LSM operation.
4. Implement the reviewed Laminar codec/executor for exactly one append-only `COUNT(*)`, one nullable
   `SUM` of a direct `Int64` column, and direct ABI-v1 grouping columns. Check every group-local input
   prefix in source order, preflight the whole Arrow batch, and fault with no mutation/output on a
   late overflow. Use the same implementation for this embedded/reference shape before admission.
   Require fresh/populated, null-only, split/coalesced overflow, late-group rollback, and impossible
   restored-state goldens; a matching UDAF name is not codec identity.
5. Keep multiple aggregates, filters/HAVING/derived expressions, broader COUNT/SUM types, `AVG`,
   `MIN`/`MAX`, `DISTINCT`, retractions, UDAFs, multi-stage fallback, and cluster MVs closed. Require
   positive replay-determinism for all upstream expressions; reject volatile/time-relative/AI UDFs.
6. Add per-operator/vnode keys, bytes, dirty bytes, cache hit rate, batch read/write, skew, checkpoint,
   restore, and pressure metrics.
7. Remove the one-million-group safety fiction once the new hard byte policy is the only admitted
   cluster path. Embedded compatibility may retain the old implementation temporarily behind an
   explicit local-only path, with a removal issue and owner.

Correctness matrix:

- random append-only batches versus the shared checked embedded/reference implementation;
- all admitted Arrow key types, null-only SUM, prefix overflow/error paths, hash golden vectors,
  unsupported multiple aggregates, deterministic-expression proofs, volatile-UDF rejection, and
  hot keys;
- multi-node remote shuffle with every vnode boundary and zero-vnode workers;
- checkpoints during dirty state, failed capture, owner death before/after seal, `1 -> 3 -> 2`
  rotation, stale messages, and repeated acquire/revoke;
- output oracle under the advertised at-least-once boundary: no lost state/result, documented
  replay duplicates only, and no double-application inside restored state.
- Kafka assignment handoff plus at least one admitted durable multiwriter append sink, including
  broker-enforced old-writer fencing, partition fence markers, ambiguous source acknowledgements,
  and crash before/after sink flush and source-position seal.
- every selected profile's cache-resident and near-capacity skew, frozen-generation, allocator/RSS
  retention and controlled-exhaustion latency/throughput profile; and
- local-spill-only cold-cache, spill-heavy and maintenance-pressure profiles.

Exit gate:

- the newly admitted aggregate matrix is exactly enumerated and all other shapes still produce
  `[LDB-4007]` before mutation;
- fault/differential suites report zero state divergence;
- numerical p99/p99.9, checkpoint, resource, and RTO targets pass on the Phase 0 profile;
- the exact selected working-state profile has a reviewed performance regression result;
- changed-group append-snapshot versus full-changelog modes are explicit: the certified append
  scenario passes, while every unsupported retraction/changelog sink combination remains
  fail-closed;
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
- Extend the existing committed source-handoff cut, which already binds source cursors and
  watermarks; do not create a competing watermark authority. Source drain/reassignment cannot move
  the frontier past unprocessed input.
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
- timer selection, state mutation, timer removal/advance, emission identity, and output bookkeeping
  are one atomic transition; ALO recovery may re-fire an externally visible output but cannot lose
  or internally double-apply it;
- skewed windows, millions of timers, checkpoint/rebalance with pending timers, owner change exactly
  at close time, and local-spill disk pressure;
- no premature fire, lost fire, unbounded retained closed window, or silent late-data policy;
- each subphase independently meets the Phase 0 tail/resource/RTO profile and completes its cycle
  review before its admission bit changes.

Processing-time/custom-trigger support is not implied by event-time certification.
Any subphase that emits retractions remains closed to external cluster sinks until the delivery
gate has a certified `FullChangelog` path.

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
- Keep external publication fail-closed until a multiwriter FullChangelog log sink or an
  assignment/key-affine mutable-sink lifecycle is certified. A mutable path must expose vnode
  assignment, fence its previous writer, and use deterministic operation IDs; `MultiWriter` alone
  is not a correctness proof.

### 4D. ASOF, temporal, and lookup variants

- Add versioned ordered history and direction/tie rules for ASOF/temporal joins.
- Keep temporal table/snapshot version in the checkpoint cut.
- Design lookup state as `RebuildableReplicated` or vnode-keyed mutable state according to its
  source contract; do not silently classify a remote cache as durable join state.

Tests and exit gates:

- differential SQL oracle over match cardinality, nulls, duplicates, equal timestamps, interval
  boundaries, out-of-order data, watermarks, and changelog weights;
- two-input barrier/replay permutations, one-side pause/failure, network reorder/loss, owner change,
  crash around unmatched output/eviction, and local-spill disk pressure;
- hot join key and asymmetric-rate profiles with bounded probe/result batches and backpressure;
- finite state follows from declared interval/watermark/retention semantics—an internal TTL is never
  the proof;
- each join family has a separate admission flag, compatibility vector, production metrics, and
  approved cycle review; compute support and external-output support are reported separately.

Unbounded joins remain `[LDB-4007]` until a separately reviewed semantic retention contract exists.

## Phase 5 — Distributed materialized output

**Purpose:** remove the independent blanket cluster MV rejection only after retained output and
reads have a distributed lifecycle.

Work:

- define output partitioning and stable row identity for append and changelog/upsert MVs;
- write output through assignment-fenced managed tables and checkpoint it with upstream operator
  state;
- add an assignment-aware sink/read topology rather than reusing `MultiWriter` as a mutable-key
  ownership claim;
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

1. Run the common PGVal-style matrix over data rate, topology, partitions, skew, checkpoints,
   process death, network disruption, object-store stalls and rolling upgrade/rollback. The
   local-spill profile additionally forces cold cache, disk full/corruption, maintenance stalls and
   complete local-disk loss.
2. Run the Phase 0-chartered independent black-box soak for each exact scenario and
   working-state-profile identity against the unchanged release-candidate
   binary in a production-like multi-process environment. Use real certified source, object store,
   and sink; an external oracle must check progress, output/state correctness, allowed ALO
   duplicates, recovery, and ownership fencing for every scenario proposed for GA. Track leak
   slopes for Rust heap and, when applicable, engine cache/memtables/journal and native allocation,
   file descriptors, iterators/snapshots, frozen generations, timers and checkpoint artifacts plus
   local bytes when applicable. Archive
   raw evidence and obtain independent reviewer sign-off. The backend spike, ordinary integration
   suite, or canary cannot satisfy this gate.
3. Publish reproducible p50/p95/p99/p99.9 and RTO results for cache-resident, near-capacity, skewed,
   checkpointing and rebalancing workloads plus spill-heavy results when applicable. A skipped
   profile-applicable external test is reported as missing evidence, never a pass.
4. Exercise operational alerts, capacity exhaustion, corrupt checkpoint, failed upgrade and
   admission rollback runbooks plus local-disk replacement for local-spill profiles.
5. Audit credentials, artifact encryption/integrity, log/error redaction and tenant/pipeline quota
   isolation plus local-state-directory security when applicable.
6. Remove experimental flags only per operator matrix, with staged canary percentages and automatic
   rollback thresholds.

General availability requires zero correctness-oracle failures, all committed numerical gates, a
valid independently reviewed release-candidate soak, approved production/operations review, and no
unresolved severity-1/2 issue. Any relevant binary/configuration change or unexplained soak anomaly
requires a complete rerun. This does not remove `[LDB-0013]`; cluster exactly-once has its own plan
and evidence.

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
   SLO/RTO gates. For a production claim, verify the independent soak's release identity, external
   oracle, raw artifacts, reviewer independence, and complete valid pass without unexplained gaps.
5. **Documentation:** keep ADR as decision authority and this file as sequencing authority; update
   public capability docs, remove superseded diaries/research, test every link, and cut repetition.
6. **Tests:** list exact commands/results, skipped suites and prerequisites, nondeterminism/retry
   counts, fault coverage, differential oracle, performance environment, and coverage gaps. A test
   that matches zero cases or needs unrecorded temporary instrumentation is a failure.

The reviewer must conclude `APPROVE`, `APPROVE WITH OWNED FOLLOW-UPS`, or `BLOCK`. A block leaves the
admission flag closed. Reviews are cumulative; use the highest numbered completed
`docs/reviews/distributed-keyed-state-cycle-*.md` review as the current cycle boundary.

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

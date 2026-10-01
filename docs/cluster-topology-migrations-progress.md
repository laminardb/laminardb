# Cluster topology migration implementation checkpoint

Status: legacy adoption/status, core admission, the old-topology checkpoint cut and local additive candidate validation implemented; runtime topology migration
is incomplete and topology writes remain fenced. The requested definition of
done has not been met.

## Baseline

- Starting commit: `5d81ba9b18d80343373ecfaec4793df8c5caccf1` (fetched
  `origin/main` on 2026-09-30). Initial worktree was clean and detached.
- Working branch: `feature/cluster-topology-migrations`.
- No `AGENTS.md` was found in the worktree or its ancestors. Read
  `CONTRIBUTING.md`, the crate READMEs, SQL cluster admission documentation, and
  the existing qualification/real-process soak harness instructions.
- Toolchain: Windows MSVC, rustc 1.98.0, cargo 1.98.0. Workspace minimum: 1.95.
- Unit validation: `--no-default-features --features cluster`. Real-process soak
  and all-target Clippy: `cluster,aws,kafka`. Non-cluster server and `cluster,ffi`
  DB builds were also checked. Full default connector suites were not run.
- Locked dependencies: Arrow 58.4.0, DataFusion 53.1.0, sqlparser 0.61.0,
  object_store 0.13.2, tokio 1.53.1, serde 1.0.229, serde_json 1.0.151,
  sha2 0.10.9, uuid 1.26.1, chitchat 0.13.0, tonic 0.14.6.
- Existing formats: leader authority 12, catalog encoding 1, checkpoint
  manifest 10, canonical pipeline identity 7. Catalog reference `version` is
  an encoding version, not a topology counter.

## Integration and invariants

`LaminarDB::execute` -> `execute_single` -> `execute_parsed_single` applies the
cluster guard. HTTP and the embedded API/FFI delegate to it; pgwire refuses DDL
at its frontend and directs clients to HTTP. DDL handlers
also enforce runtime admission. `execute_cluster_bootstrap_batch` is cold-start
only and seals the complete inventory; manifest replay is a task-local exception.
Neither exception may authorize runtime migration.

`CatalogManifestStore` is a view of `LeaderLeaseStore`. Immutable SHA-256 catalog
blobs are installed before a create-only leader authority record is published by
the store's conditional head contract. Checkpoint admission, outcomes, assignment
decisions, faults and recovery releases use this same authority. The repository
does not use Raft. Preserve every existing authority field on every transition.

`BarrierCoordinator` and checkpoint artifact admission certify the exact vnode
owner/boot roster. Recovery freezes owners and evidence reporters, observes actor
termination, reconciles prepared effects, restores a cut, and commits release.
Use those rules; a reachable majority is insufficient. The intake gate must not
prevent a required checkpoint barrier from reaching its source.

Pipeline identity hashes the entire logical graph and state ABI. Operator state
frames are indexed by stream name, not traversal position, but that alone is not
compatibility evidence. Unchanged streams retain their catalog incarnation;
drop/recreate must allocate another one. Subscription distribution certificates
also bind pipeline identity, so their generation/replay semantics need an explicit
mapping when the complete graph changes. Never bypass the fingerprint check.

## Planned commit protocol

Adopt a sealed legacy catalog as topology 1 by one fenced authority append that
retains its exact manifest reference and bytes, deployment identity and checkpoint
history. Upgrade the authority encoding at that append: old readers must fail
closed, including writers that read the predecessor before a concurrent upgrade.
Require a coordinated binary upgrade; capability advertisements alone do not
revoke old process/sink ownership.

The intended migration states are Planned -> Preparing -> Quiescing ->
CutPrepared -> Committed -> Activating -> Active. Pre-commit failure may abort;
committed failure requires target recovery. The logical commit must atomically
publish the target manifest reference and its exact reconciled old-topology cut,
state mapping, source initialization vector, and participant proof in the shared
authority. The locally active version is separate from the committed version.

Check versions at graph install/release and existing transport batch ownership
boundaries. Do not add record-path locks, serialization, copies or allocation.
Retain migration roots and replay references before admitting cleanup. Keep the
checkpoint allocator monotonic across versions. A stopped target graph must use
a target checkpoint or an explicitly authorized migration root.

## Target support matrix (not yet enabled)

| Mutation | Classification | Required contract |
| --- | --- | --- |
| Independent source -> stateless stream -> supported sink | Additive, future-only | Persisted concrete source positions, old cut, new sink fencing |
| Stateless downstream stream/sink | Additive, future-only | Activation at the cut; no implied history/backfill |
| Unchanged stateful operators | State preserving | Exact DDL/incarnation and state ABI mapping, timers/watermarks/output frontiers |
| Leaf/dependency-ordered removal | Dependency safe | Settled sink effects, observed termination, retained references |
| Key/schema/window/aggregate/source/sink changes | Transformation required or unsupported | Remain rejected until separately certified |
| Materialized views, uncertified connectors, arbitrary local SQL | Unsupported in cluster | Existing cluster admission applies |

## Legacy increment validation (2026-09-30)

- Baseline `cargo test -p laminar-core --features cluster --lib catalog_manifest
  --no-default-features`: 9 passed.
- Baseline `cargo test -p laminar-db --no-default-features --features cluster
  --lib live_topology_ddl_is_fenced_in_a_configured_one_owner_cluster`: 1 passed.
- `cargo test -p laminar-core --no-default-features --features cluster --lib
  -- --quiet`: 978 passed after the final deadline/test-module changes.
- `cargo test -p laminar-db --no-default-features --features cluster --lib
  -- --quiet`, with `RUST_MIN_STACK=4194304`: 1,916 passed. The first attempt
  with the default Windows test-thread stack overflowed in an existing recovery
  test. The successful retry uses the stack setting already used by repository CI.
- `cargo test -p laminar-server --no-default-features --features cluster
  --bin laminardb http::tests -- --quiet`, with that stack setting: 83 passed,
  including the authenticated uninitialized/legacy/adopted/corrupt status route.
- Arrow queue baseline and final modified comparison completed (30 samples each).
  The first comparison failed to find the baseline because Criterion selected a
  different output directory; an explicit `CRITERION_HOME` fixed it. No significant
  change was detected: 6.1545 us baseline versus 6.2107 us modified per 32-batch
  burst, relative interval [-2.7019%, +1.8246%], p=0.82. This is not pipeline or
  migration latency. Raw samples and the final output are checked in with the
  [measurement scope](test-evidence/topology-adoption-2026-09-30/README.md).
- Rebuilt real-process `three_node_alo_legacy_topology_adoption_restart_soak`:
  1 passed in 520.26 s. Three actual server processes, a leader kill inside a
  checkpoint, identical-inventory adoption at authority sequence 10, full restart
  on the same namespace, all nodes locally active at topology 1, continued input
  and checkpoint 36. Independent Kafka oracles observed every expected bounded
  and temporal pair across 188,194 logical input IDs; replay duplicates remained
  allowed under ALO. Detailed counts and timings are in the evidence above.
- The real-process run used an unoptimized Windows binary with a test-only
  16 MiB main stack and CI's 4 MiB spawned-thread setting. The original debug
  executable overflowed its default main stack before adoption. An earlier
  harness build also failed because its full restart was after producer stop;
  the final harness moves restart before stop and preserves all timing evidence.
  Both failed logs are retained locally. This is functional evidence; SLO modes
  were `observe`, and the measured stall budget was exceeded.
- Local Docker project `ldb-topology-9929` used repository MinIO/Redpanda fixtures
  at ports 19000/19092 and its own bucket `topology-tests-9929`. All spawned servers
  exited and only this Docker project was removed after the passing test.
- `cargo clippy -p laminar-core -p laminar-db -p laminar-server
  --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings`:
  passed after the final harness change.
- `cargo check -p laminar-server --no-default-features`: passed.
- `cargo check -p laminar-db --no-default-features --features cluster,ffi`: passed.
- `cargo +nightly fmt --all -- --check` and `git diff --check`: passed.

Not run: an additive SQL/API migration, migration phase/fault matrix, state
mapping/install/release, drop/recreate and subscription cutover, mixed-binary
adoption, exactly-once migration or full default connector suites. These need the
remaining protocol implementation and appropriate multi-process connector
fixtures (Kafka/S3 for ALO; certified transactional sinks for EO). No environment
change alone makes the currently missing migration tests runnable. Migration
pause, consumer latency, RSS, allocations and artifact-growth comparisons were
not measured.

## Completed increment (2026-09-30)

- Distinct checked logical-version/request IDs and explicit Uninitialized,
  LegacySealed and Versioned(topology 1) observations.
- Format 12 remains unchanged before adoption. Explicit fenced adoption upgrades
  one shared authority append to 13, retains exact catalog bytes/hash/generations,
  deployment UUID and checkpoint/outcome/assignment/recovery metadata, and rejects
  downgrade/replacement. Every later append preserves the baseline.
- Deadline/CAS bounds, unknown response and cancellation recovery, leader/renewal
  contention tests, damaged-artifact failure and retained adoption anchor.
- Read-only deployment lookup; catalog/startup reads audit the original baseline
  and deployment instead of initializing missing identity or assuming a version.
- DB and console-authenticated HTTP topology status, typed registry codes
  LDB-6060..6065, exact replay tracking, and local process/recovery/terminal fences.
- Operator and engineering checkpoint guides. New deterministic coverage has
  17 authority/adoption tests plus DB status and two HTTP status tests. All
  existing runtime topology guards remain in place.
- Existing three-process stateful Kafka soak extended with identical-inventory
  adoption, all-node restart, status checks, continued checkpoint IDs and its
  independent output oracles. It is an upgrade compatibility scenario; it does
  not submit an additive graph migration.

## Completed increment (2026-10-01)

Continuation started clean at `d5867bf03e34207f7a97ac3800d4fb1eac17ad76`.
The implementation extends existing authority/checkpoint/assignment contracts;
it adds no parallel scheduler, consensus service or general workflow framework.

- Authority format 14 preserves baseline and a bounded immutable request journal.
  Core candidate admission freezes the exact parent, candidate, owner map and boot
  roster. One shared append serializes it with checkpoint and assignment admission.
  It only implements `Planned -> Aborted`, leaving the active graph and catalog
  unchanged. Semantic compatibility and the cutover worker remain unfinished.
- Production graceful assignment publication reserves its exact immutable proposal
  before raw snapshot publication. The existing watcher/driver materializes an
  interrupted intent. Exact drain/recovery settlement releases the reservation;
  drain settlement materializes it before release. Settled proposal cleanup follows
  the admitted floor in bounded batches.
- Payload-bound retries return the original result, including from a replacement
  leader process. Stale proofs and changed payloads fail. Renewal preserves a
  reservation; term change or a durable recovery fault atomically aborts pre-cut
  preparation. Committed-phase behavior is not represented by this abort helper.
- Console-authenticated `GET /api/v1/cluster/topology/operations/{operation_id}`
  reports the typed durable state and retained evidence. Unknown identities return
  an uncached 404, malformed UUIDs 400, and damaged evidence fails closed.
- Plan/journal/retry/read bounds and retained authority anchors are implemented.
  Journal eviction and orphan topology-artifact cleanup remain missing; the fixed
  64-request limit rejects further admission. No public submission uses this partial
  contract. A coordinated binary upgrade is required before normal format-14 drains.
- All 17 new deterministic admission/drain fault tests and final all-target Clippy
  pass. The final full suite passes 995 core, 1,967 DB and 354 server tests
  (one existing DB test ignored). Non-cluster server and cluster FFI builds, fmt
  and diff checks also pass. The current
  [validation evidence](test-evidence/topology-admission-2026-10-01/README.md)
  records commands, build identity and the scope of each result.
- Rebuilt three-process stateful Kafka/S3 adoption/restart soak passes in 879.42 s:
  326,129 logical input IDs, all expected bounded/temporal/matrix/window outputs,
  allowed ALO duplicates, leader failure/rejoin and all-node restart. The frozen
  input prefix reaches checkpoint 46. S3 readback confirms format 14, baseline 1
  and no pending drain. This is unchanged-graph evidence, not runtime migration.
  Debug SLOs are observational and miss the stall budget. Only task fixtures were
  removed after the run. Queue comparison is 6.1545 us original versus 6.1181 us
  modified per 32-batch burst; Criterion reports no significant change (p=0.07).

## Completed increment (2026-10-01, checkpoint cut)

Started clean at `ab18ca39b4ce07d6f00ad2d63e513a6ead54473f`. This continues
increment C through the old-graph checkpoint cut. It reuses the manual checkpoint
owner, capture, durable tails and coordinated recovery. There is no general
workflow framework, new dependency or per-record work.

- Format 15 atomically binds `Quiescing` and exact checkpoint artifact admission
  before Prepare/source barrier publication. Ordinary checkpoints and assignment
  transitions remain excluded. The configured controller supplies namespace and
  process/leader fences; retries cannot replace the attempt or frozen roster.
- Source barriers arrive before intake closes. Existing shuffle/operator/sink
  drains and full pipeline/state identity checks still apply. Retained intermediate
  shuffle replay is explicitly rejected for topology cuts.
- The old-topology Commit and its exact cut reference share one authority append.
  Commit alone remains `Quiescing`. The leader reports after globally aggregated
  external sink settlement; followers finish local checkpoint application. Every
  frozen exact process must report before `CutPrepared`. Intake and successor sink
  epochs stay held. These receipts do not authorize target installation or prove
  observed actor retirement.
- A term change or recovery fault aborts the uncommitted candidate, retaining any
  successful parent checkpoint and receipts. Explicit abort of an unresolved cut
  is rejected; even a prepared-cut abort requires coordinated recovery/restart to
  reopen intake. An application/publication error preserves the durable checkpoint
  and reports a continuation error instead of treating it as a failed sink Commit.
- Pruning pins request, binding and Commit authority anchors. Live preparation
  pins the old checkpoint index against newer artifact-floor cleanup. Aborted
  status retains authority evidence while normal checkpoint/replay retention can
  eventually retire old artifacts.
- Added deterministic cut identity, complete roster, stale boot, cancellation,
  ambiguous successful response, leader-race, deadline, recovery and damaged-root
  coverage, plus runtime Prepare/gate/sink/manual-owner boundary tests.
- The first real-process attempt found an ordinary checkpoint/topology admission
  race before Prepare. The reserved checkpoint had no retained leader proof, so
  abandonment could not persist Abort and faulted the pipeline. Proof ownership
  now begins at exact reservation, including deadline/process failure before
  Prepare. Deterministic coverage forces the flags race, verifies the exact Abort,
  no intake hold/Prepare, unchanged Planned status and same-operation retry.
  An unused reservation with no prepared state or transactional sinks can abort
  without recovery only when the same authority append proves artifact admission
  is absent. A semaphore test lets cut admission win that append and rejects the
  shortcut. Admitted Abort retains normal recovery/artifact ownership.
- The second real-process attempt reached the exact old checkpoint and all process
  receipts, then found that assignment refresh reopened intake. Capture now sets
  a specific cut hold under the existing authority-transition lock. Assignment
  refresh retains its certificate but cannot open intake or a successor sink epoch.
  The existing authorized recovery Release owns hold removal. Deterministic tests
  verify assignment reopening is refused, rejected release preserves the hold,
  and an authorized retry clears it. No new data-path lock or check was added.
- The new real-process `three_node_alo_topology_cut_abort_restart_soak` stages a
  candidate through the core API, drives the existing authenticated manual checkpoint
  route, verifies all processes held at the exact cut, and restarts the unchanged
  topology on the same namespace. It reuses the existing independent Kafka stateful
  oracles. Candidate planning/SQL migration submission are deliberately unfinished.
- Final validation passes 1,009 core, 1,970 DB and 354 server cluster tests (one
  existing DB test ignored), 414 non-cluster core tests, all-target Clippy with
  `cluster,aws,kafka`, non-cluster server and `cluster,ffi` builds, fmt and diff
  checks. Exact commands and raw results are in the
  [cut evidence](test-evidence/topology-cut-2026-10-01/README.md).
- The final optimized three-process cut/abort/restart soak passes in 313.58 s.
  Checkpoint 47 reaches CutPrepared with all exact receipts in 2.145 s; full restart
  activates the unchanged topology in 45.102 s and retains the candidate abort and
  old cut. All expected stateful output is observed across 109,733 logical input
  IDs; ALO replay duplicates remain allowed. The frozen prefix reaches checkpoint
  82. Gate closure through deliberate restart/recovery is 46.7..46.9 s; this is
  not target activation or consumer-visible latency. Sampled combined working set
  peaks at 729,796,608 bytes, without a matched baseline or allocation measurement.
  Earlier runs exposed the two fixed races; a later unoptimized run reached
  CutPrepared but missed the unchanged 90 s restart budget. The final run uses
  the repository's `soak` profile and keeps that budget. Queue time is 6.0732 us
  versus the original 6.1545 us; Criterion reports change within noise threshold
  (p=0.01), not a certified throughput improvement. SLO modes remain observational.

## Completed increment (2026-10-01, local candidate planning)

Started clean at `d58d797514f1126926c69127010e1a634d65de30`. This implements
DB-owned local candidate compilation and a public dry-run route, reusing the
existing catalog, physical planner, graph and connector checks. It adds no general
workflow framework, dependency, runtime actor or per-record work.

- `LaminarDB::validate_cluster_topology_change` and console-authenticated
  `POST /api/v1/cluster/topology/validate` audit the exact adopted parent and
  compile parent/target catalogs privately. They support local validation of
  independent replayable-source/stateless-stream/durable-sink additions and
  stateless downstream streams/sinks while checking preserved managed definitions.
- Descriptor format 1 binds stable catalog names/incarnations, canonical definitions,
  global ABI/config, schemas, physical/state-codec contracts, connector implementation
  versions/cancellation contracts and transitive dependencies. Full pipeline
  identity encoding 7 and authority format 15 are unchanged. Ordinary recovery
  remains strict; no state is restored across different fingerprints.
- The private Created database shares frozen factories and a snapshot of live
  routing resource availability. It has no authority, transport or runtime. Small
  schema-only source queues reject intake without spawning their normal drain
  tasks. Empty managed graphs use the configured state budget and are dropped
  sequentially; no historical Arrow/state data is copied.
- New objects are explicitly future-only. Source positions remain unresolved and
  must be concretely resolved once at the cut and persisted before target commit.
  The response lists missing participant agreement, cut, durable mapping/progress,
  observed retirement, atomic target commit and installed-target Release.
  Matching local reports do not constitute durable participant certificates.
- One local compiler, 64 statements/256 KiB SQL, 256 total objects, 1 MiB
  descriptor, 512 KiB HTTP body and a 30 second deadline bound validation.
  Cancelled/busy/timed-out validation admits no durable operation. Unsupported
  replacements, removals, new state, MV/reference tables, custom implementations
  and uncertified connector/execution contracts fail closed.
- New deterministic tests cover exact preserved incarnation/dependency binding,
  independent/downstream additions, zero connector lifecycle effects and durable
  writes, missing artifacts, conflicting/changed parents, unsafe changes,
  source/sink/changelog/filter admission, missing live routing scope, bounded
  queues without background tasks, cancellation, concurrency and deadlines.
  Router coverage exercises authentication, local scope, request limits and
  typed conflicts/unsupported operations using actual Kafka factories.
- The real three-process cut/abort/restart harness now dry-runs the same candidate
  on every running stateful node, compares complete descriptors, verifies intake
  remains active, and records request latency. It still does not install the target.
- Final validation passes 1,011 core, 1,984 DB and 356 server tests with
  `cluster,aws,kafka` (one existing DB test ignored), 414 non-cluster core tests,
  all-target Clippy, non-cluster server and `cluster,ffi` builds, fmt and diff
  checks. Exact commands and output are in the
  [planning evidence](test-evidence/topology-planning-2026-10-01/README.md).
- The optimized three-process scenario passes in 321.13 s. All nodes return the
  same 48-object plan, preserving 47 objects including 24 managed-state contracts;
  request times are 1,367.189 / 470.731 / 2,322.800 ms with intake active. Cut 71
  reaches CutPrepared in 2.731 s; full restart activates unchanged topology 1 in
  37.104 s and retains the candidate abort and successful parent cut. Independent
  stateful oracles observe every expected output across 111,741 logical input IDs,
  with the frozen prefix durable through checkpoint 113. Sampled combined working
  set peaks at 709,943,296 bytes. SLO modes remain observational; no matched
  resource/throughput baseline or consumer-visible migration latency is measured.
  All test server processes exited and only the isolated Compose fixtures were
  removed, preserving volumes and artifacts.

## Remaining work

1. Durable participant capability/compatibility certificates and binding of the
   implemented local candidate descriptor to core admission.
   Core pre-cut admission, payload-bound status and serialization with the production
   assignment writer are implemented. The manual checkpoint path establishes the
   old cut; no worker certifies compatibility, stages or commits a target.
2. Authorized state/ABI mappings, concrete new-source positions and durable target
   migration roots. Old checkpoint binding, quiescence and live root pinning exist.
3. Observed actor retirement, install/release and post-commit recovery.
4. Public SQL/atomic API, detached ownership and leader routing. Topology/operation
   status and local dry-run validation are implemented; activation/write routes are not.
5. Removal/replacement contracts, fault matrix and existing soak extensions.
6. Real multi-process stateful migration/restart oracle and comparative resource,
   steady-state and pause-inclusive performance measurements.

Do not remove LDB-6043 or route runtime DDL through bootstrap while these runtime
links remain unfinished. Do not interpret authority-only tests as migration
certification.

Use [operator guidance](cluster-topology-operations.md) and the
[engineering checkpoint](cluster-topology-engineering.md). Raw local logs and
Criterion output are under ignored `target/topology-evidence` and
`target/topology-baseline`; checked-in queue sample evidence is under
`docs/test-evidence/topology-adoption-2026-09-30` and
`docs/test-evidence/topology-admission-2026-10-01`. Cut results are in
`docs/test-evidence/topology-cut-2026-10-01`; the latest local candidate validation
and stateful restart results are in
`docs/test-evidence/topology-planning-2026-10-01`. This is a resumable checkpoint
on `feature/cluster-topology-migrations`; the final handoff identifies its exact
commit SHA. No changes were pushed and no pull request was created.

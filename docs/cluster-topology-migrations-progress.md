# Cluster topology migration implementation checkpoint

Status: legacy adoption/status, core admission, durable participant preparation, the old-topology checkpoint cut, local additive candidate validation, exact-cut root staging, sealed new-source initialization/startup, private restore/retirement, atomic target Commit, held installation, participant-complete Release and DB-owned phase progress implemented; public runtime topology migration
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

## Completed increment (2026-10-01, participant certification)

Started clean at `aa6336a5e1bd18d9adc604f1069accca92d87515`. This connects
the isolated candidate compiler to the existing exact-process, assignment and
shared authority contracts. It adds no migration worker, generic framework,
dependency or per-record work.

- The existing format-1 report and digest now have one shared typed definition
  for local planning and durable decoding. Canonical bounded descriptor blobs are
  content addressed and immutably bound to protocol-2 admission. Authority format
  16 preserves baseline 1 and older request/cut history; it never downgrades.
- `LaminarDB::prepare_cluster_topology_operation` and console-authenticated local
  `POST /api/v1/cluster/topology/operations/{operation_id}/prepare` load the exact
  admitted target and independently compile it. The whole report must match.
  Caller-supplied compatibility reports cannot bypass compilation. Source/sink
  lifecycle effects, actor installation and intake closure remain absent.
- The configured controller uses its actual process authority, including when its
  storage differs from checkpoint/catalog storage. Exact local assignment adoption,
  boot/term and leader authority fence publication. Each shared append records one
  immutable certificate; `Preparing` completes only for the frozen owner/evidence
  roster. New cuts require every certificate's current term and the exact parent
  pipeline identity. Legacy reservations remain readable but cannot start new cuts.
- Identical retries, lost responses and cancellation resolve to the retained
  append. Abort/restart retains certificates and the successful parent cut. Status
  and pruning audit/pin all certificate anchors. Ordinary checkpoint admission
  defers incomplete preparation and held cuts before reserving an attempt; existing
  manual cut ownership and Prepare-time race cleanup remain in force.
- Bounds remain one compiler, 30 seconds compilation, 45 seconds end to end,
  15 seconds/16 CAS attempts for publication, 1 MiB descriptors, 129 participants,
  64 retained requests and the existing 256 KiB authority record limit. A large
  roster can reach the record limit before the request count. There is no public
  submission or automatic certificate collection yet.
- Added exact-roster/idempotency, mixed protocol, divergence, stale term,
  cancellation, lost-response, paused-time timeout, leader-race, pruning and
  damaged evidence tests. DB tests use distinct configured process storage and
  connector lifecycle spies; router tests cover auth and typed invalid/nonrunning
  responses. A golden test decodes the previous checked-in report and preserves
  its descriptor digest. The full cluster suite passes 1,020 core, 1,986 DB and
  356 server tests (one existing DB test ignored). All-target Clippy, non-cluster
  server and cluster FFI builds, 414 non-cluster core tests, fmt and diff checks
  pass. Exact commands and output are in the
  [preparation evidence](test-evidence/topology-preparation-2026-10-01/README.md).
- The optimized three-process scenario passes in 294.19 s. All three processes
  independently compile and durably certify the identical 48-object candidate
  in 1.657 s, with per-node request times 626.095 / 482.627 / 486.585 ms and
  intake active. The exact roster completes at authority 539. Cut 81 reaches
  CutPrepared in 10.123 s; full restart activates unchanged topology 1 in 38.469 s
  and retains all certificates, the candidate abort and successful parent cut.
  Independent oracles observe every expected stateful output across 101,989
  logical input IDs, with the frozen prefix durable through checkpoint 119.
  Logged gate hold through deliberate restart/recovery is 48.695..48.735 s;
  sampled combined working set peaks at 727,097,344 bytes. These are old-graph
  functional observations, without a matched baseline or consumer-visible
  migration latency measurement. All test servers exit; only isolated fixture
  containers/network are removed, preserving volumes and evidence.

## Completed increment (2026-10-01, exact-cut root staging)

This continuation starts clean at `9db1b6862309ebc27917c7bc01da86fb88dbd4ef`,
after participant certification. The original baseline and dependency/feature
selection remain unchanged.

- The DB/core API stages immutable migration-root requirements for certified
  stateless downstream additions at a held `CutPrepared` checkpoint. It uses the
  configured checkpoint reader and actual process/assignment authorities, including
  separately stored process leases. No candidate actor, restore, topology Commit
  or Release is authorized; catalog T and its allocator remain unchanged.
- Root encoding 1 preserves exact catalog incarnations and certified compatibility
  digests, maps the existing `graph:<canonical name>` state slots, rejects unknown
  frames/missing managed state, and lists future-only additions. Source/snapshot/
  channel progress, state ranges, timers, watermarks, sink decisions and output
  segments remain referenced through the exact old cut. Unresolved new-source
  positions are explicitly rejected.
- Preserved subscriptions retain their stream generation, schema, distribution,
  query/changelog/retention contracts and full partition sequence vector. Required
  target certificates change only the strict pipeline identity; target installation
  must explicitly consume these mappings. No historical manifest is rewritten and
  ordinary strict restore identity checks remain enabled.
- One format-17 shared authority append pins the canonical root and its first
  sequence. Exact retries resolve to the same binding, including after cancellation
  or a lost response. Status and pruning audit/pin its canonical body and first
  append. A live prepared root retains the existing cut artifact-floor pin. After
  Abort, ordinary checkpoint/replay retention owns old artifacts; historical root
  audit still works after those artifacts retire.
- Metadata reads are sequential, capped at 16 MiB in aggregate before reads. Root
  bodies are capped at 1 MiB. The DB has a 30 second deadline and authority staging
  retains the existing 15 second/16 CAS budget. The existing source/channel progress
  validator moves into the shared checkpoint validator for both recovery and staging.
  No new scheduler, task owner, general workflow, dependency or per-record work is
  introduced.
- Ten core failure/retry/retention tests and a DB test with lifecycle spies and
  separately configured process storage pass. The full cluster suite passes
  1,030 core, 1,987 DB and 356 server tests, with one existing DB test ignored.
  Current build and real-process results are recorded in the
  [root staging evidence](test-evidence/topology-root-2026-10-01/README.md).
- All-target Clippy, non-cluster server and cluster FFI checks, all 414 non-cluster
  core tests, formatting and diff checks pass. The optimized three-process scenario
  passes in 345.46 s. Core staging takes 508.011 ms, inspecting 606,855 bytes of
  participant metadata and pinning a 50,338-byte root at authority 437. It retains
  47 object mappings and nine exact subscription sequence vectors; identical retry
  and all-node status agree. Full restart activates unchanged topology 1 in
  59.970 s and retains the root, certificates, successful cut 60 and candidate abort.
  Every expected bounded/temporal output is observed across 122,826 logical input
  IDs, durable through checkpoint 89, with allowed ALO duplicates. Logged gate hold
  through deliberate restart/recovery is 64.512..64.687 s. Sampled combined server
  working set peaks at 826,122,240 bytes. These are old-graph functional/control-path
  observations, without a matched baseline, staging allocation measurement or
  consumer-visible migration latency distribution. Source, server and harness
  hashes are verified before/after the run. All test servers exit; only isolated
  fixtures are removed, preserving volumes/evidence.

## Sealed new-source initialization, 2026-10-02

- Continuation starts clean at `cad04b580d2890078c5eb0fef4d8b66a61b06717`.
  Root format 2 pins concrete initial cursors for every certified new source.
  Format-1 roots keep their exact canonical bytes and authority-17 binding.
  New roots require authority 18; ordinary pipeline/checkpoint identities are
  unchanged. A root remains staging evidence in CutPrepared, with catalog T active.
- The DB privately replays the immutable target with configured factories,
  reconciles durable stream incarnations and checks the certified pipeline and
  environment. Its existing compiler slot serializes local discovery. Core calls
  this resolver only after the admitting-leader, full current process preparation
  and exact assignment fences. No source/sink actor or target graph starts.
- The existing source connector trait has one fail-closed read-only initialization
  hook. Kafka supports explicit topics with earliest/latest numeric positions and
  a complete global unowned inventory, including empty/never-read partitions.
  It does not subscribe, assign, poll records, join groups, acknowledge input or
  create topics. A 10-second total budget and one permit retained through native
  drop bound outstanding clients using the existing blocking-task tracker.
- A bounded create-only slot at
  `control/topology-source-root-staging/v1/<operation UUID>/<plan SHA-256>.json`
  seals the first vector before content-addressed root publication and the one
  shared authority append. Concurrent reads converge on that vector; cancellation
  after sealing, lost responses and CAS retries never reevaluate it. Pre-seal
  reads grant no boundary. Replacement leaders abort the pre-commit operation and
  preserve the parent cut. Both root and slot are bounded at 1 MiB; the 64-request
  journal bounds extra slot retention at 64 MiB. Cleanup belongs to future journal
  retention, and a published root no longer depends on its staging slot.
- Target consumption of the cursor and preserved state/subscription mappings,
  logical topology Commit and target Release are still unfinished. Guaranteed
  ordinary Kafka startup continues to reject unsealed latest, and LDB-6043 remains.
- Full selected-feature validation passes 1,039 core, 912 connector, 1,988 DB
  and 356 server tests (4,295 total), with the real-broker test and one existing
  model-download test excluded from that run. The broker test then passes in
  1.42 s: latest resolves `[2, 3, 0]` in 308.418 ms, earliest resolves zero,
  and no reader or group acknowledgement starts. All-target Clippy, non-cluster
  server/cluster FFI checks, all 414 non-cluster core tests, formatting and diff
  checks pass. The server soak has one test-only dependency on the existing
  connector crate; no external packages or versions change.
- The optimized three-process cut/abort/full-restart scenario passes in 370.38 s.
  All three processes certify the 51-object candidate; core staging takes
  505.860 ms and pins a 51,909-byte root at authority 402 with one Kafka hook call,
  the exact global `[2, 3, 0]` cursor, 47 preserved mappings and nine subscription
  vectors. Full restart activates unchanged topology 1 in 59.687 s; abort at 403
  retains the successful cut 54 and sealed root. Every expected bounded/temporal
  output is observed across 132,278 logical input IDs, durable through checkpoint
  85, with allowed ALO duplicates. Gate hold through deliberate restart/recovery
  is 64.947..65.055 s. Sampled combined server working set peaks at 896,397,312
  bytes; the harness's whole-run observed peak is 147,226,624 bytes. These are
  functional/control-path observations without matched or pause-inclusive latency
  certification. Source, Cargo metadata and binary hashes match before/after the
  scenario. All seven servers exit and only isolated fixtures are removed. Exact
  commands, artifacts and limits are in the
  [source initialization evidence](test-evidence/topology-sources-2026-10-02/README.md).

## Private target restore preparation, 2026-10-02

- Continuation starts clean at `9e525345e95b960315d6305a4ff93c1664de6a8d`.
  The controller reads an opaque exact operation/root/cut/process input from its
  configured authorities. Complete current participant certification, the admitting
  leader and the unchanged assignment fence are required. No new authority format,
  phase, log append or dependency is introduced.
- The DB reuses the isolated compiler, strict historical-parent recovery loader,
  existing graph construction and operator codecs. It checks the complete certified
  target and environment, rebuilds the root from verified manifests before state
  reads, decodes local state with the frozen roster and validates preserved
  subscription generations/exclusive sequences. Ordinary target-fingerprint
  recovery remains rejected, and historical checkpoint identities stay unchanged.
- One opaque unstarted image owns the existing local compiler permit until dropped.
  Its target graph uses existing fenced shuffle handles solely as channel decoding
  context, without live graph/vnode handles or actors. Error, cancellation and the
  45-second deadline drop partial state and retain the parent hold. Authority,
  runtime hold and the parent environment are rechecked before returning it.
- Existing source cursors retain their actual attempt and assignment origin. New
  cursors remain the sealed global unowned vector with no fabricated processing
  attempt. The fail-closed connector hook validates availability without resealing;
  Kafka checks exact inventory and retention bounds with its existing one-client
  native-work permit and 10-second budget. It does not assign, subscribe, poll,
  acknowledge or start a reader. Final installation must validate and filter again.
- Manifest metadata remains bounded at 16 MiB, roots at 1 MiB, state/payload at the
  configured graph budget and node reads at the checkpoint limit. Encoded buffers
  are released after decode, before cursor validation. Held parent state, target
  state, codec scratch and read buffers overlap transiently; these limits do not
  establish a total RSS bound or production migration performance.
- Atomic topology Commit, observed old-actor retirement, target installation,
  participant-complete Release, detached ownership and public migration submission
  remain unfinished. LDB-6043 remains. This is the next preparation increment,
  not the original runtime migration definition of done.
- All 14 focused tests and the full 4,309-test selected-feature suite pass:
  1,044 core, 914 connectors, 1,995 DB and 356 server. All-target Clippy passes
  with warnings denied. The real-broker test passes in 1.34 s, sealing `[2, 3, 0]`
  in 278.283 ms and validating it unchanged after another append, without starting
  a reader or acknowledging input. Operator decoding is tested with an actual
  aggregate that continues from 30 to 45, nine state frames, incarnation 7 and
  nonzero subscription sequences. Deadline/cancellation, stale authority,
  missing/corrupt payload, strict target-fingerprint rejection and payload limits
  all retain the parent hold and release the local preparation slot.
- Non-default server and cluster/FFI checks, all 414 non-cluster core tests,
  formatting and diff checks pass. No dependency or Cargo metadata changes.
- The optimized three-process cut/abort/full-restart scenario passes in 325.65 s.
  All three processes certify the 51-object target, then core authorization and
  strict RecoveryManager loading verify 1,608 local frames / 20,336,760 bytes from
  their actual held checkpoint 63. Per-participant authorization/loading takes
  299.247..310.187 ms; sealed Kafka availability validation takes 110.653 ms and
  retains `[2, 3, 0]`. These reads run in the harness; target operator decoding is
  tested separately in the actorless DB fixture. No target is committed/installed.
  The 51,859-byte root is bound at authority 451; abort at 453 retains it and the
  parent cut. Full restart activates unchanged topology 1 in 57.430 s. All expected
  bounded/temporal outputs are observed across 114,027 logical input IDs, durable
  through checkpoint 109 with allowed ALO duplicates. Gate hold through deliberate
  restart/recovery is 61.426..61.503 s. Sampled combined server working set peaks
  at 745,369,600 bytes; the harness's whole-run observed peak is 144,019,456 bytes.
  No matched or pause-inclusive migration performance is certified. Source/Cargo
  and binary hashes match before/after the scenario, all seven servers exit, and
  only isolated fixtures are removed. Exact commands and limits are in the
  [private restore evidence](test-evidence/topology-restore-2026-10-02/README.md).

## Observed parent retirement, 2026-10-02

- Continuation starts clean at `1430fe8cd251c2fcc227c1be303020eb99c15523`.
  A prepared exact-root target can now retire its originating DB's held parent.
  The existing compiler guard identifies that DB; no identity registry, workflow
  framework, authority format, phase, receipt, scheduler or dependency is added.
- Complete current root/old-cut/leader/process/assignment authorization and exact
  local parent definitions are checked before signalling stop and after observing
  terminal cleanup. The same production lifecycle joins compute, retires vnode
  claims, settles decision/sink-open work and observes source/sink actors and all
  tracked connector children. Cancellation and close requests alone do not count
  as terminal proof. Watcher failures remain visible and cannot certify an image.
- The total deadline is 45 seconds. Unresolved handles stay in their existing DB
  owners across deadline/cancellation. Retry can continue cleanup using the same
  current image. Retirement retains ShuttingDown, the intake/cut hold, checkpoint
  namespace and historical parent catalog/coordinator identity. The private target
  remains unstarted; dropping it frees the compiler but never reopens intake.
- Public start/stop cannot release a held cut. Terminal shutdown and authorized
  coordinated recovery retain their cleanup paths; recovery can take over after
  pre-commit abort. The image's retirement flag is a local observation, not a durable
  readiness receipt or output permit, and must be revalidated at the future commit
  and installation boundary. Process-owned shuffle generation fencing, atomic
  topology Commit, install/Release, detached ownership and public submission remain
  unfinished. LDB-6043 remains; the runtime migration definition of done is unmet.
- Validation results and commands are recorded in the
  [parent retirement evidence](test-evidence/topology-retirement-2026-10-02/README.md).
  All ten focused tests and the full 4,319-test suite pass: 1,044 core, 914 connector,
  2,005 DB and 356 server. All-target Clippy with warnings denied, non-default server
  and cluster/FFI checks, formatting and working/staged diff checks pass. Final source
  hashes remain unchanged through validation. The production terminal wrappers,
  process-fenced sink actor, connector child trackers and OS namespace lock are
  exercised with a controlled watcher in the DB fixture. No broker or optimized
  three-process scenario is rerun: those existing scenarios do not yet drive this
  internal retirement API. Target migration/performance certification remains unfinished.

## Durable target preparation observations, 2026-10-02

- Continuation starts clean at `7a9496889a68841d8fce6a79bd41cf2506273dfc`.
  The retained private image can drive observed retirement and publish an exact
  participant/boot/process-term preparation receipt through its configured
  controller and the existing fenced authority append. The DB accepts no caller
  termination flag, process identity, root or receipt. No new worker, registry,
  scheduler, dependency or per-record work is added.
- The first receipt writes authority format 19 and requires target preparation
  protocol 3. The immutable candidate plan remains protocol 2. Every frozen
  owner/evidence process is required, and each receipt is audited against its
  original retained append. Identical retries append nothing; concurrent reporters,
  leader replacement and lost responses use the existing conditional-write contract.
- The operation stays CutPrepared with its original plan/root/cut and committed
  topology 1. Another participant's receipt may advance status while all immutable
  restore requirements stay exact. Old encodings omit empty receipts. Counts remain
  bounded at 129 processes and authority bodies at 256 KiB.
- A receipt is a historical restore/retirement observation, not image residency,
  installed receiver/sink readiness or an output permit. Image drop retains the
  cut/namespace hold and historical proof. Commit must revalidate every current
  process/assignment and provide usable target recovery before its first checkpoint.
  Atomic Commit, target generation fencing, install/Release, detached work and public
  SQL submission remain unfinished. LDB-6043 remains; the runtime migration
  definition of done is unmet.
- Commands, validation results and fixture limits are recorded in the
  [target preparation evidence](test-evidence/topology-target-preparation-2026-10-02/README.md).
  All 15 focused tests and the full 4,334-test selected-feature suite pass:
  1,054 core, 914 connectors, 2,010 DB and 356 server. All-target Clippy with
  warnings denied, non-default server and cluster/FFI checks, formatting and
  working/staged diff checks pass. All 19 changed Rust source hashes and Cargo.lock
  remain unchanged through final verification and staging. No broker or optimized
  multi-process scenario is rerun; their existing harness does not yet drive this
  new internal DB method. Real target migration and performance certification
  remain unfinished.

## Atomic target Commit and private reconstruction, 2026-10-02

- Continuation starts clean at `ca2674957e162bf508f6347529e70eea1a4b006b`.
  The existing shared authority now atomically binds the committed target catalog
  and exact operation/root/cut/descriptor/participant evidence. No second head,
  scheduler, registry, framework, dependency or per-record work is added.
- Authority format 20 and protocol-4 preparation require every frozen exact
  owner/evidence process to support Commit and explicit root reconstruction. The
  immutable plan remains protocol 2; root encodings remain 1/2. Historical protocol-3
  receipts stay readable but cannot be rewritten/upgraded in place or authorize Commit.
  Commit revalidates the whole current original process/assignment roster, retained
  image, parent catalog and latest settled cut. All existing bounds/fault fences apply.
- One irreversible append advances the committed logical version and preserves
  the original baseline, manifest bytes/object incarnations, old checkpoint/outcome
  links and allocator. Leader replacement and recovery faults preserve Committed;
  explicit abort rejects. Ordinary checkpoint/assignment/topology admission and
  parent recovery Release remain held, including cached recovery-admission snapshots.
- The retained image's DB can Commit and reconstruct after image loss. A separate
  Created DB can replay the committed catalog and privately restore before its first
  checkpoint. Strict parent PipelineIdentity/manifests/checksums and actual operator
  codecs remain mandatory; no target checkpoint, cold-started preserved state or
  historical acknowledgement is invented. Current boot/term and durable local
  adoption are audited around restore. New boots may use a newer assignment only
  with identical vnode owners/domain/ABI and the complete stable participant roster;
  changed ownership/rescaling rejects. New-source positions are validated, not resolved again.
- The DB keeps the 45-second total cooperative budgets and existing compiler slot,
  payload/state limits and retirement task ownership. The parent stays ShuttingDown
  with cut/intake/namespace held; target images remain Created/unstarted. Cancellation
  or damaged artifacts retain Commit and drop partial images. Durable operation
  status resolves ambiguous writes; committed recovery never rolls back the catalog.
- Cold replay accepts the exact full current inventory or the exact full adopted
  original bootstrap and reconstructs the target in both cases. Arbitrary subsets
  or changed definitions reject. Committed and locally active versions remain
  separate. Ordinary startup rejects pending Commit until target installation/Release
  are implemented. This is internal Commit/private recovery, not activated runtime
  migration or automatic full-cluster target restart. LDB-6043 remains.

- Validation is recorded in the
  [Commit evidence](test-evidence/topology-commit-2026-10-02/README.md). The final
  focused command passes all 21 cases (13 core and eight DB). The final selected
  four-package suite passes 4,355 tests: 1,067 core, 914 connectors, 2,018 DB and
  356 server, with the same two existing ignored tests. All-target Clippy with
  warnings denied, non-default server, cluster/FFI, formatting and diff checks pass.
  All 33 changed Rust source hashes and Cargo.lock remain unchanged through final
  validation and staging. Existing bootstrap rejection and corrupt-catalog recovery
  diagnostics remain compatible.
- Core tests use the exact-root two-process authority fixture and a real full
  monotonic peer process takeover. DB tests use actual aggregate codecs, preserved
  subscriptions/cursors, a controlled watcher and OS namespace locking. A separate
  Created DB restores the committed inventory with the same configured control
  process; this does not certify a full target runtime restart. No broker or
  optimized multi-process scenario is rerun because the existing harness does not
  drive these internal methods. Activated migration and performance certification
  remain unfinished.

## Committed transport generation fencing, 2026-10-02

- Continuation starts clean at `646fab31e19db2d24c5d3bfbcefa0175d7992e72`.
  The next installation prerequisite now binds the process-owned shuffle endpoints
  and private graph to the exact committed logical topology/catalog digest. It
  extends existing assignment publication, scope cancellation, delivery tracking
  and graph execution boundaries. No framework, scheduler, dependency, authority
  format or preparation/root protocol change is introduced.
- The existing delivery lock serializes pending admissions/loss reporting with
  the topology audit/reset. Old connections, handshake tokens and blocked sends
  cancel; old queued/staged data/frontiers/barriers drop before loss accounting.
  Genuine unrepaired loss blocks installation without forgiveness. Conflicting
  digests, downgrades, inactive/mismatched assignments and expired processes reject.
  Assignment/recovery preserve the topology floor. Identical target retries retain
  delivery sequence continuity and do not reset the domain.
- Hello and the request/response handshake carry the exact version/digest pair.
  Zero plus empty denotes legacy fabric, never inferred topology 1. Migrated
  peers reject legacy, divergent and malformed identities; client echo checks
  reject old binaries ignoring the fields. Data/control payloads and Arrow schemas
  remain unchanged. Fixed graph bindings and retained async send plans reject
  predecessor work at batch/ownership boundaries without per-row costs.
- `prepare_cluster_topology_transport(&mut image)` reobserves actual actor retirement
  and fresh complete Commit/process/assignment/adoption authorization, validates
  sealed cursor availability, and holds existing assignment/execution ownership.
  The total cooperative budget is 45 seconds. Created recovery requires empty
  runtime/connector ownership. Cancellation after local publication retains the
  target fence and hold; exact retry or root reconstruction resumes preparation.
- Success leaves the operation Committed, private target unstarted, parent
  ShuttingDown, catalog/coordinator unchanged and intake/cut/namespace held. No
  acknowledgement, actor readiness or Release permit is produced. Protocol-4
  preparation does not certify current target installation capability/readiness;
  future participant-complete Release must obtain both. Runtime installation,
  automatic target recovery and public migration remain unfinished. LDB-6043 remains.
- Validation is recorded in the
  [transport evidence](test-evidence/topology-transport-2026-10-02/README.md).
  All 18 focused tests pass (11 core, seven DB). The selected-feature suite passes
  4,373 tests: 1,078 core, 914 connectors, 2,025 DB and 356 server, with the same two
  existing ignored tests. All-target Clippy with warnings denied, non-default server,
  cluster/FFI, formatting and diff checks pass. All 30 changed Rust/protobuf source
  hashes and Cargo.lock remain unchanged through final verification and staging.
  Real loopback gRPC tests exercise topology changes under
  unchanged process/assignment/recovery identities. DB tests use actual aggregate
  codecs, sealed cursors, controlled watcher ownership and OS namespace locking.
  Their private codec execution and Created reconstruction do not certify target
  actors, output or multi-process target restart. No broker or optimized migration
  performance scenario is rerun; the existing harness does not drive this internal method.

## Atomic sealed source startup, 2026-10-02

- Continuation starts clean at `f20cf05ecae70c6a53a7dce8a3ffbe1a52867830`.
  Runtime installation needs an atomic way to install new-source positions from
  the immutable root. `SourcePosition::Initialized` now carries that complete
  unowned cursor without a fabricated checkpoint attempt or processed history.
  Prepared source positions convert to exact preserved Resume or Initialized.
- The startup request rejects BestEffort initialized delivery and assigned cursors.
  The runtime requires explicit sealed-start capability before connector I/O;
  default custom implementations and unsupported built-ins fail closed. Kafka
  validates complete source/channel/inventory/retention evidence, reuses numeric
  offsets through manual assignment and filters the global vector by current owners.
  Retries never resolve latest again. Guaranteed ordinary Initial/latest remains
  rejected; existing BestEffort assignment policy is retained. Saved positions
  disable automatic topic creation.
- Existing metadata/native task bounds, shared startup deadline, process lease
  checks, cleanup and task ownership are reused. The source reader stays deferred
  until polling. The coordinator seeds committed offsets only from durable Resume;
  Initialized neither seeds processed progress nor acknowledges the skipped prefix.
  No general framework, registry, authority format, dependency or per-record work
  is introduced.
- This is an installation prerequisite. Actual runtime catalog/coordinator and
  source/sink integration, stale sink completion fencing, current participant
  capabilities/readiness, Release and automatic post-Commit recovery remain
  unfinished. LDB-6043 and the bootstrap boundary remain. Native broker/real Redpanda
  connector cases and an owned held actor fixture do not certify a complete
  stateful migration or multi-process target restart. Validation is recorded in the
  [source startup evidence](test-evidence/topology-source-start-2026-10-02/README.md).
- All 12 focused cases and the explicit real Redpanda retry case pass. The selected
  four-package suite passes 4,385 tests: 1,078 core, 921 connectors, 2,030 DB and
  356 server, with three ignored cases (the same two existing cases and the new
  broker case run separately). All-target Clippy with warnings denied, minimal
  server, cluster/FFI and the affected optional connector builds pass, as do
  formatting and working/staged diff checks. All 27 changed Rust source hashes
  and Cargo.lock remain unchanged through final verification and staging. Optional
  connector runtime suites and optimized multi-process migration/performance
  scenarios are not rerun. The isolated broker fixture is removed after its test.

## Held committed runtime installation, 2026-10-03

- Continuation starts clean at `53e63fad8b83e84076d045d6ede11242127a538c`.
  `install_committed_cluster_topology(image)` now consumes the exact committed
  private image through the existing detached startup owner. It reobserves parent
  retirement and current Commit/assignment/process authority, prepares transport,
  replays the exact target catalog and transfers the already decoded graph.
- The live catalog and checkpoint coordinator bind the certified target identity
  and environment. The coordinator retains the historical parent checkpoint and
  outcome unchanged; the first target checkpoint must capture full vnode state.
  Preserved source/stream handles, incarnations and subscription frontiers survive.
  Source actors use exact Resume/Initialized positions. Unsupported sealed starts
  reject before target sink I/O. Reference tables without an initialization mapping
  remain unsupported.
- The source/sink actors and compute control loop can become locally Running while
  intake/cut stay held. Target sink epoch admission is deferred. Success publishes
  no readiness receipt, Release or locally active version. Ordinary start, manual
  gate opening and runtime DDL remain fenced. No protocol, dependency, scheduler,
  general framework or per-record work is added.
- The existing startup owner continues after caller cancellation. One cooperative
  45-second budget spans transport, lifecycle/catalog replay and runtime preparation;
  bounded terminal cleanup can extend the call. Compute watcher ownership is stored
  before waiting for readiness, so dropped startup futures cannot orphan compute.
  Failed startup joins/retains existing owners and keeps Commit, cut and namespace
  ownership. A reconstructed root can retry after observed cleanup.
- Tests exercise actual restored aggregate execution and target callback/sink
  wiring, held actors, cancellation, failed-start retry, unsupported source startup,
  process loss, the owned deadline and Created-DB installation before a target
  checkpoint. The local test gate opening is a codec/runtime oracle, not Release.
  The Created DB retains the fixture's configured process identity, not a full
  multi-process restart. Validation is recorded in the
  [installation evidence](test-evidence/topology-installation-2026-10-03/README.md).
- All eight focused cases pass. The selected four-package suite passes 4,393
  tests: 1,078 core, 921 connectors, 2,038 DB and 356 server, with the same three
  ignored cases. The 19 changed Rust source hashes and Cargo.lock remain frozen
  through final verification and staging. All-target Clippy with warnings denied,
  minimal server, cluster/FFI, formatting and working/staged diff checks pass.
  No broker or optimized multi-process/performance
  scenario is rerun; the current harness does not drive held runtime installation
  and Release.

## Installed runtime certification and Release, 2026-10-03

- Continuation starts clean at `1b2fcf26b0308e4b5ed8d957d37edb575c501316`.
  Authority format 21 and installation protocol 5 implement
  `Committed -> Activating -> Active`. Exact current owner/evidence processes
  certify their actual held runtime UUID, state/transport binding, live source/sink
  actors, sink acknowledgements and receiver mesh. The complete roster, including
  zero-vnode evidence processes, is required for durable Release.
- The existing DB control executor owns certification, leader Release and follower
  application with one existing compiler slot and a 45-second cooperative deadline.
  Caller cancellation leaves the bounded owner running. Sink witnesses/epoch
  admission precede the final authority audit and local intake opening. Commit and
  published Release remain immutable; unreleased leader replacement recollects all
  receipts. A new process/runtime cannot borrow an old runtime's Release.
- Every sink actor now shares a unique revocation token with its operations and
  handles. Revocation precedes actor abort, rejects connector admission, same-poll
  late completion and buffered success, and retires the connector even if its usual
  cancellation policy allows reuse. Existing native-child tracking still governs
  termination and unknown external outcomes. Dead source/sink actors cannot certify
  readiness merely because their connector children remain owned.
- Held assignment refresh preserves the exact installed certificate. Released
  refresh uses the certified Release before the first target checkpoint and can
  coexist with exact target checkpoint work. Artifact admission rejects a parent
  pipeline checkpoint after Commit; generic recovery still cannot relabel the
  historical parent cut. Local status requires the exact applied, live runtime.
- No scheduler, general framework, dependency, state-copy or per-record topology
  work is introduced. This completes the explicit installation/Release library
  path, while owned orchestration, automatic target recovery and public SQL/API
  remain unfinished. LDB-6043 and startup guards remain. Validation and its local
  controlled-connector limits are recorded in the
  [activation evidence](test-evidence/topology-activation-2026-10-03/README.md).
- All 20 new focused cases pass. The final selected four-package suite passes
  4,413 tests: 1,087 core, 921 connectors, 2,049 DB and 356 server, with the same
  three ignored cases. All-target Clippy with warnings denied, minimal server,
  cluster/FFI, formatting and working/staged diff checks pass. All 34 changed Rust
  source hashes and Cargo.lock are frozen through final verification and staging.
  No broker, real migration/restart, soak or comparative performance run is repeated.

## Database-owned phase driver, 2026-10-03

- Continuation starts clean at `02202d7910d3d85da6e1946c2c0097e4f2df4c77`.
  The existing recovery monitor now drives admitted topology operations on every
  node. Its long-lived future owns one private target image and the existing
  compiler permit, independently of API/status observers. No additional scheduler,
  queue, dependency, authority encoding or generic workflow framework is added.
- Healthy polls independently compile, use the existing checkpoint owner, wait
  for exact cut application, stage the root, privately restore, observe retirement,
  certify protocol four, commit the complete target, install held actors, certify
  protocol five and apply participant-complete Release. Follower images observe
  the existing Commit. Original Release still cannot authorize a replacement runtime.
- Local faults/recovery/drain/process fences drop private preparation before later
  recovery actions. An aborted held cut requests coordinated parent recovery;
  dropping an image cannot open intake. A still-held retired parent can reconstruct
  its private image after monitor loss. Failed installation observes cleanup and
  retries the same immutable root and sealed source positions. A phase stalled
  for 180 seconds requests coordinated recovery; exact checkpoint owners retain
  their terminal cleanup obligations.
- The latest UUID/status sequence is a bounded polling hint, never authority.
  Every phase uses audited status and the existing current-authority methods.
  Held local boundaries take precedence over a later request. Idle polling does
  not clone the installed manifest/root. The first debug run exposed a stack
  overflow; heap ownership of the large image/active control futures fixes it with
  the repository's 4 MiB test and two-worker control stacks unchanged.
- All nine focused cases pass. The final selected suite passes 4,422 tests:
  1,088 core, 921 connectors, 2,057 DB and 356 server, with the same three ignored
  cases. The local runtime oracle preserves 30 plus three future rows of value 5
  as 45, and verifies monitor/installation progress after observer cancellation.
  All-target Clippy with warnings denied, minimal server, cluster/FFI, formatting
  and diff/source checks pass. Ten changed Rust source hashes and Cargo.lock remain
  frozen through final verification and staging.
  This is one-process controlled ALO evidence. No broker, real multi-process
  migration/restart, transactional migration or comparative performance is run.
  See the [driver evidence](test-evidence/topology-driver-2026-10-03/README.md).

## Remaining work

1. Wire automatic target recovery, including a replacement process/runtime after
   Release and whole-cluster startup. Select a newer exact target checkpoint when
   present; otherwise explicitly authorize the migration root through the existing
   stopped/recovered/release quorum. Never borrow the original runtime UUID's Release.
   Ordinary startup and parent recovery remain fenced after Commit.
2. Public SQL/atomic API, detached submission and leader routing. Topology/operation
   status and local dry-run validation are implemented; activation/write routes are not.
3. Removal/replacement contracts, reference-aware root retirement and bounded journal
   reclamation, fault matrix and existing soak extensions.
4. Real multi-process stateful migration/restart oracle and comparative resource,
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
`docs/test-evidence/topology-cut-2026-10-01`; local candidate validation results
are in `docs/test-evidence/topology-planning-2026-10-01`. The latest exact-process
certification and stateful restart results are in
`docs/test-evidence/topology-preparation-2026-10-01`; exact-cut root staging results
are in `docs/test-evidence/topology-root-2026-10-01`; sealed source initialization
and the latest stateful restart run are in
`docs/test-evidence/topology-sources-2026-10-02`. Private target restore preparation
and its checks are recorded in
`docs/test-evidence/topology-restore-2026-10-02`. Parent retirement and its checks
are recorded in `docs/test-evidence/topology-retirement-2026-10-02`.
Durable target preparation observations and their checks are recorded in
`docs/test-evidence/topology-target-preparation-2026-10-02`.
Atomic Commit/private reconstruction checks are recorded in
`docs/test-evidence/topology-commit-2026-10-02`.
Committed transport preparation checks are recorded in
`docs/test-evidence/topology-transport-2026-10-02`.
Atomic sealed source startup checks are recorded in
`docs/test-evidence/topology-source-start-2026-10-02`.
Held committed runtime installation checks are recorded in
`docs/test-evidence/topology-installation-2026-10-03`.
Current runtime certification, sink generation and Release checks are recorded in
`docs/test-evidence/topology-activation-2026-10-03`.
DB-owned phase progress and its checks are recorded in
`docs/test-evidence/topology-driver-2026-10-03`.
This is a resumable checkpoint
on `feature/cluster-topology-migrations`; the final handoff identifies its exact
commit SHA. No changes were pushed and no pull request was created.

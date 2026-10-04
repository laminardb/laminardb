# Cluster topology migration progress

Public additive SQL/API submission passes the selected regression suite, actual
actor/checkpoint tests and the three-process Kafka/S3 migration/full-restart
oracle, including three injected failures. Complete-map admission fixes the
observed rebalance defect. Post-Commit boot admission and root-retained ordinary
cleanup now pass deterministic and native regressions. Attempt 19 exposed a
shuffle recovery/install ordering defect on its third replacement; the exact
prepared loss-cutoff repair passes attempt 20 with all three kills and the whole
original-bootstrap restart. Matched steady pair three also passes all final oracles.
Safe removals, replacements and root/journal reclamation remain
unsupported until their incarnation and replay contracts exist.

## Baseline and current validation

- Original clean baseline: `5d81ba9b18d80343373ecfaec4793df8c5caccf1`, fetched
  from `origin/main` on 2026-09-30.
- Branch: `feature/cluster-topology-migrations`. The current five-source increment
  starts from `b9c3c9485e013281628aab9cc72943a7c07db908`, following the public
  migration, complete-map recovery and retained-root repairs. No user changes
  were reset or stashed. No push or PR has been made.
- Windows MSVC, rustc/cargo 1.98.0, unchanged Cargo.lock. Locked dependencies
  include Arrow 58.4.0, DataFusion 53.1.0, sqlparser 0.61.0, object_store 0.13.2,
  tokio 1.53.1 and tonic 0.14.6. No AGENTS.md applies.
- The latest full four-package `cluster,aws,kafka` library/binary suite passes
  4,478 tests: core 1,112, connectors 921, DB 2,086, server 359; three existing
  tests remain ignored. The focused topology suite passes 291 tests, including
  Committed/Activating boot replacement, retained-root cleanup and damaged-root
  deletion refusal. Eight test threads and 4 MiB stacks retain existing deadlines.
  Five Rust sources are frozen against `b9c3c948`; formatting and diff checks pass.
  All-target Clippy with warnings denied and the minimal-server check pass.
  Qualified source commit: `a7dcd6a755a59282fe2cf9a1c0bc1a4e77b7c2b8`.
  Earlier cluster/FFI checks passed; default connector feature suites are not run.
- Earlier [public evidence](test-evidence/topology-public-2026-10-03/README.md) binds 37
  changed Rust sources and Cargo.lock. No binary/performance identity is implied
  by those unit-test hashes.

## Implemented behavior and invariants

The existing conditional append authority is the serialization point; this
repository does not use Raft. Catalog version, manifest encoding, pipeline
identity, object incarnation, checkpoint epoch, assignment and process terms
remain separate domains. Catalog encoding remains one. Public protocol six
requires authority encoding 23 before the old cut; coordinated binary upgrade
still observes all old processes stopped.

The durable phases are Planned → Preparing → Quiescing → CutPrepared → Committed
→ Activating → Active. Pre-Commit abort reconciles an already committed old cut.
Commit binds the exact target, root, mapping and concrete source initializations.
Release requires actual held installation by the complete process roster. A
manifest or cancellation request is insufficient evidence. Post-Commit recovery
retains target authority and uses a new runtime UUID with coordinated Release.

The existing DB-owned recovery monitor owns phase progress and one private image.
No parallel scheduler, workflow framework, checkpoint service or record-path
remote lookup is introduced. Heavy control futures are heap-owned; control
runtime workers and stack settings remain unchanged. Checkpoint barriers can
finish while post-cut intake is held. Source, sink and child-task termination is
observed before superseded ownership is released.

| Operation | Current contract |
| --- | --- |
| Independent supported source → stateless stream → durable sink | Atomic candidate validation; persisted once-resolved source positions; future-only activation. |
| Stateless downstream stream or compatible sink | Future input after Release; no backfill. |
| Unchanged managed aggregate/window/join | Exact identities, definitions, codecs, state, timers, source/watermark and output progress preserved. |
| Full restart | Greatest exact target checkpoint, otherwise the authorized root; original adopted bootstrap is an assertion, never a rollback. Library actor cases and the three-process cold restart pass. |
| Removal, replacement, new stateful operators, key/schema/window/source/sink transformations | Rejected until their explicit contracts are implemented and tested. |

Public `LaminarDB::execute` and HTTP SQL intercept supported running-cluster DDL
before the live catalog write lock. They do not use bootstrap/replay exceptions.
SQL returns an asynchronous operation receipt; atomic arrays use caller UUID and
expected parent. Identical retries read the original receipt before compiler or
current-parent gates; changed bytes conflict. Follower submission makes one
authenticated hop under the original bounded deadline. Status separates durable
Commit from local runtime activation. All other mutation guards remain active.

Existing sealed catalogs use explicit exact-manifest/deployment adoption without
changing historical bytes, hashes, generations or checkpoints. Capability
advertisement does not retire cached old writers. Missing deployment identity
fails without creating a replacement namespace.

Subscription reconnect crosses pipeline identities only through audited released
roots. Generation/schema/query/distribution/changelog/retention contracts remain
strict. Cleanup uses audited predecessor edges and exact replay horizon references;
corrupt or missing evidence prevents deletion. Migration roots still conservatively
pin old state, and the journal rejects new identities at 64 retained operations.

## Current increment

- The exact owned-runtime loss regression fails against `b9c3c948`: held
  installation rejects unrepaired loss, while repair requires Release. The
  five-source repair permits installation only under the pending cutoff for
  the exact authorized coordinated-recovery generation. It does not advance the
  repair floor before full-roster Release. New loopback transport tests reject
  later loss, stale generations and the permanently poisoned counter. All 291
  focused and 4,478 full tests pass, as do Clippy, minimal check and formatting.
  The unchanged stock optimized build passes in 24m 13s. Server `9cd06a8c...`
  passes attempt 20 in 356.28 s with all final oracles. Replacement full
  Release/checkpoints take 42.88/32.99/29.51 s; whole cold restart reaches fresh
  output in 49.44 s. Sampled combined RSS peaks at 745,672,704 bytes. Matched
  steady pair three passes both original/current final oracles in 213.77/192.50 s.
  Exact final checkpoint 148 and retained roots 50/57 are verified after the
  artifact floor advances to 123. The 49-test boundary index and separate native
  loss observations are retained. Native attempt 19 remains a failed historical result.
- Local commit `582b7cf0` adds observed Created-state recovery teardown, permits
  only the exact assignment/checkpoint handoff pin through recovery Release, and
  strengthens protected-state cleanup preflight. State objects are fully hashed
  with 256 KiB reads, eight requests, an 8192-object/4 GiB bound and a 15-second
  deadline. Cleanup admission also rechecks topology/handoff authority after I/O.
  These fixes remove no migration/replay pin.
- Local commit `f4c7e3de` uses the existing portable checkpoint bootstrap for
  historical assignment state under the exact current owner map. Native attempt
  14 (`60169d45...` stock server) reached both public migrations, target checkpoints
  35/42 and all six new-pipeline pairs. All three original-bootstrap cold
  replacements restored private state, then coordinator installation rejected
  `recovered.reassigned`. The unchanged 90-second Release ceiling expired after
  197.93 seconds. No fresh Release or final stateful/sink/sequence pass is claimed.
- The current nine-source repair shares complete-owner/portable-cut validation
  between private restore and coordinator installation. Runtime readiness checks
  the exact selected root or target reference without relabelling historical
  manifests. A real-actor regression passes both cuts, coordinated Release,
  aggregate continuation (45/60), incompatible-owner rejection and a new
  current-assignment checkpoint. Two earlier fixture attempts failed before
  selection and are excluded from the production regression claim.
- Local commit `b8f38a02` records that repair and its passing three-process cold
  restart. The next six-source increment adds complete-owner-map validation to
  assignment drain reservation and failure-recovery admission, plus read-only
  preflight before process fencing. Two meaningful authority regressions failed
  before the repair (0.15 s drain, 30.19 s recovery); the focused suite now passes
  285 tests. Rejected proposals leave authority and assignment heads unchanged;
  same-slot takeover still reaches Release and the next exact target checkpoint.
  The full suite passes 4,472 tests (three existing ignored); all-target Clippy
  and the minimal server check pass. The stock server build passed in 24m 03s;
  retained server SHA-256 is
  `5f270ef4789f1b2be15e7571109a528ef2521ace82c93179dccb9282bde56459`,
  with the unchanged soak profile and 1 MiB Windows main stack. Native attempt
  18 passes both public migrations, three injected failures and the final whole
  original-bootstrap restart, with all independent final oracles, in 423.53 s.
  Replacements reached full-roster Release/checkpoint in 41.57/33.15/36.52 s;
  the cold restart reached fresh output in 70.51 s. The unchanged 90-second
  ceiling passed. Pause-inclusive consumer p95/p99 were 18,835.13 ms over nine
  observations; sampled combined server RSS peaked at 844,259,328 bytes.
  Exact final checkpoint 118 bytes/hash and durable authority were verified.
- Local commit `4a972885` records complete-map admission, the passing three-kill
  native qualification and the second matched process pair. The next increment
  exercises Committed/Activating process replacement before a target checkpoint
  and permits obsolete target-checkpoint cleanup while retaining roots. Both
  meaningful authority regressions fail on that commit (137 passed, two failed,
  31.68 s). Their exact test-source hashes are retained before the repair.
  Candidate cleanup stops before the latest retained root and includes all
  committed roots in protected state/output inventory. Root metadata reads use
  only the audited immutable cut Commit; ordinary expired cuts remain unavailable.
  The first repair passed cleanup but failed the pending-root handoff validation
  and an obsolete blanket-cleanup assertion (137 passed, two failed, 31.68 s).
  The final focused suite passes 288 and the full suite passes 4,475 tests; the
  exact root pin and root stop boundary remain mandatory. All-target Clippy
  and the minimal check pass. The stock optimized build passes in 24m 00s;
  retained server SHA-256 is
  `a2eabeadc2c3eb9802ea50c64d6235527e6bbac025647276f73051f225387c6c`,
  with the unchanged 1 MiB main stack and harness. Native qualification and a
  third matched pair do not qualify this candidate yet. Attempt 19 passes both
  migrations and its first two replacements (43.19/31.47 s), then fails the third
  full-roster Release at the unchanged 90-second ceiling after 284.86 s. Floor
  52 and exact roots 38/44 are retained and verified. The surviving leader's
  transport rejects unrecovered delivery loss before installation, while the
  loss repair floor is promoted only after Release. The existing prepared
  recovery-generation cutoff supplies the required scoped installation proof.
  No cold restart or final oracle pass is claimed, and the performance chain
  stopped before pair three. Source/binary/failure evidence remains retained.
- Focused verification passes 284 tests; the full selected suite passes 4,471
  (three ignored). All-target Clippy denies warnings and passes; the minimal
  server and formatting checks pass. Sources are frozen against `f4c7e3de` and
  the stock optimized server build passed in 22m 07s. Retained server SHA-256
  is `54e3a3d2ddb2927bfbc664c8ef522d84c0809b0d2d89664dcc3d193ac8c392e0`;
  the ordinary 1 MiB Windows main stack and unchanged soak profile are verified.
- Native attempt 15 passed after 280.45 seconds with zero extra kills: both
  migrations, all six new-pipeline pairs, whole original-bootstrap cold restart,
  resumed target checkpoints and independent final stateful/sink/sequence oracles.
  Cold restart to fresh output took 48,421 ms. Nine consumer observations include
  the cut hold; p95/p99 were 18,444.08 ms. Sampled combined server RSS peaked at
  749,150,208 bytes. These are correctness-oracle observations, not latency SLOs.
  Attempt 16 failed before migration or kills when the preserved Kafka fixture
  reached its 1,000-partition capacity. Only that fixture limit was raised to
  2,000; no topic or storage was deleted. Attempt 17 recovered its first leader
  kill and checkpointed in 43.86 seconds. Its second follower kill triggered
  automatic survivor rescaling to a two-node map. Recovery correctly rejected
  that map; the 90-second ceiling expired, and final oracles were not reached.
  Assignment admission must retain the committed topology's complete owner map
  while waiting for a fresh replacement process. No recovery fence was relaxed.
- Existing deterministic fault evidence covers lost responses, cancellation,
  leader/process replacement, every durable migration phase, owned actor startup,
  target-cut selection and cleanup races. The
  [46-test boundary index](test-evidence/topology-public-process-2026-10-03/root-boot-fault-coverage.json)
  binds the current 4,475-test run. It is neither a new fault framework nor a
  claim of native kills at every phase.
- Three alternating original/modified stock queue trials average 6.05832/6.02855
  microseconds per 32-batch burst (-0.49145% point estimate). The matched first
  three-process steady pair passed all final oracles at 400 paced IDs/s in
  228.05/229.53 seconds. Current node 2's p95 graph-cycle bucket bound rose from
  1 to 5 ms; all p99 bounds were 5 ms. Full-run RSS peaks were 932,233,216 and
  805,085,184 bytes. The modified server was the earlier `f9a05d5f...` build;
  the current recovery repair is outside those process measurements. Allocation
  events and queue depth were unavailable. The second matched pair uses the
  newly qualified `5f270ef4...` server and passes all final oracles in
  210.01/201.24 s. Both producers acknowledge 400 paced IDs/s. All p50 bounds
  are 0.5 ms and p99 bounds 5 ms; current node 2's p95 is 5 ms versus the
  original's 1 ms. RSS peaks are 837,165,056/719,101,952 bytes. Stored endpoints
  contain 84,501,429/97,532,712 bytes; they are neither capacity nor growth-rate
  claims. Windows heap profiling tools exist, but this shell lacks administrator
  rights and tracing is disabled. No machine configuration was changed. See
  [performance evidence](test-evidence/topology-performance-2026-10-04/README.md).

Full passing and failed-run source/binary identities, raw measurements, exact authority and
checkpoint references remain in
[process evidence](test-evidence/topology-public-process-2026-10-03/README.md).
The owned Kafka/S3 fixtures retain their exact namespaces, topics and volume;
no storage reset or external deletion was used.

## Qualification and guarded follow-up work

The initial additive A–D contract is implemented and qualified against source
commit `a7dcd6a7`. Full tests, the stock build, three-kill/full-restart attempt 20,
matched pair three, exact durable-byte collection and the operator/engineering
handoff are complete. All owned test executables have terminated. Fixtures,
topics, volumes and durable namespaces remain retained. No push or PR was made.

- Removals and replacements need stop/sink-settlement, dependency projection,
  retired-incarnation and replay evidence beyond the current additive descriptor.
  They remain unsupported; changed keys/schemas/state semantics stay guarded.
- Root consumption and bounded journal retirement need proof that state/output,
  replay and idempotency references have retired. Release alone is insufficient;
  roots stay protected and admission fails closed at 64 retained operations.
- Allocation events need an elevated Windows tracing session. Queue item counts,
  preparing-only consumer latency and pure restore duration need separate
  instrumentation. Current phase windows include polling/control I/O, consumer
  observations include the cut hold, and artifact endpoints are not growth rates.
- Default connector variants and native exactly-once Delta/S3 scenarios require
  their features and fixtures. Native kills at every durable phase are not
  claimed; the deterministic boundary index states its actual fault coverage.

Exact commands, source identities, results and unmeasured cases are recorded in
the [implementation report](cluster-topology-implementation-report.md).

## Earlier evidence

Detailed historical commands, source identities, results and limitations remain in
the evidence directories below; this file summarizes the current resume state.

| Increment | Evidence |
| --- | --- |
| Legacy baseline and adoption | [2026-09-30](test-evidence/topology-adoption-2026-09-30/README.md) |
| Atomic admission | [Admission](test-evidence/topology-admission-2026-10-01/README.md) |
| Old checkpoint cut | [Cut](test-evidence/topology-cut-2026-10-01/README.md) |
| Isolated planner | [Planning](test-evidence/topology-planning-2026-10-01/README.md) |
| Participant preparation | [Preparation](test-evidence/topology-preparation-2026-10-01/README.md) |
| Exact migration root | [Root](test-evidence/topology-root-2026-10-01/README.md) |
| Concrete source initialization | [Sources](test-evidence/topology-sources-2026-10-02/README.md) |
| Private state restore | [Restore](test-evidence/topology-restore-2026-10-02/README.md) |
| Observed retirement | [Retirement](test-evidence/topology-retirement-2026-10-02/README.md) |
| Actual target preparation | [Target preparation](test-evidence/topology-target-preparation-2026-10-02/README.md) |
| Atomic Commit | [Commit](test-evidence/topology-commit-2026-10-02/README.md) |
| Transport generation | [Transport](test-evidence/topology-transport-2026-10-02/README.md) |
| Sealed source startup | [Source start](test-evidence/topology-source-start-2026-10-02/README.md) |
| Held actors | [Installation](test-evidence/topology-installation-2026-10-03/README.md) |
| Full installed Release | [Activation](test-evidence/topology-activation-2026-10-03/README.md) |
| Existing monitor orchestration | [Driver](test-evidence/topology-driver-2026-10-03/README.md) |
| Target checkpoint selection | [Target recovery](test-evidence/topology-target-recovery-2026-10-03/README.md) |
| Replacement recovery and cold startup | [Coordinated recovery](test-evidence/topology-coordinated-recovery-2026-10-03/README.md) |
| Replay and horizons | [Replay](test-evidence/topology-replay-2026-10-03/README.md) |

Use the [operator guide](cluster-topology-operations.md) and
[engineering guide](cluster-topology-engineering.md). Raw current logs and builds
are under ignored `target/topology-evidence`; the original benchmark checkout and
samples remain under `target/topology-baseline`.

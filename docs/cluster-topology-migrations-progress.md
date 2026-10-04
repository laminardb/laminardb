# Cluster topology migration progress

Public additive SQL/API submission passes the selected regression suite, actual
actor/checkpoint tests and the three-process Kafka/S3 migration/full-restart
oracle. Additional injected-failure qualification exposed a rebalance defect. Safe removals,
root/journal reclamation and current matched performance evidence remain
unfinished. The complete requested scope has not yet been met.

## Baseline and current validation

- Original clean baseline: `5d81ba9b18d80343373ecfaec4793df8c5caccf1`, fetched
  from `origin/main` on 2026-09-30.
- Branch: `feature/cluster-topology-migrations`. This continuation starts from
  `f4c7e3de1ffed921c5fff94203aa216258760b75`, following the public-submission
  and observed cold-recovery repairs. No user changes were reset or stashed. No push or PR has been made.
- Windows MSVC, rustc/cargo 1.98.0, unchanged Cargo.lock. Locked dependencies
  include Arrow 58.4.0, DataFusion 53.1.0, sqlparser 0.61.0, object_store 0.13.2,
  tokio 1.53.1 and tonic 0.14.6. No AGENTS.md applies.
- The latest full four-package `cluster,aws,kafka` library/binary suite passes
  4,471 tests: core 1,107, connectors 921, DB 2,084, server 359; three existing
  tests remain ignored. The focused topology suite passes 284 tests, including
  older-assignment root/target actor recovery and a subsequent checkpoint.
  Eight test threads and 4 MiB stacks retain existing deadlines. Nine Rust
  sources are frozen against `f4c7e3de`; formatting and diff checks pass.
  All-target Clippy with warnings denied and the minimal-server check pass.
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

The durable phases are Planned â†’ Preparing â†’ Quiescing â†’ CutPrepared â†’ Committed
â†’ Activating â†’ Active. Pre-Commit abort reconciles an already committed old cut.
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
| Independent supported source â†’ stateless stream â†’ durable sink | Atomic candidate validation; persisted once-resolved source positions; future-only activation. |
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
  [42-test boundary index](test-evidence/topology-public-process-2026-10-03/portable-installation-fault-coverage.json)
  binds the current 4,471-test run. It is neither a new fault framework nor a
  claim of native kills at every phase.
- Three alternating original/modified stock queue trials average 6.05832/6.02855
  microseconds per 32-batch burst (-0.49145% point estimate). The matched first
  three-process steady pair passed all final oracles at 400 paced IDs/s in
  228.05/229.53 seconds. Current node 2's p95 graph-cycle bucket bound rose from
  1 to 5 ms; all p99 bounds were 5 ms. Full-run RSS peaks were 932,233,216 and
  805,085,184 bytes. The modified server was the earlier `f9a05d5f...` build;
  the current recovery repair is outside those process measurements. Allocation
  events and queue depth were unavailable. See
  [performance evidence](test-evidence/topology-performance-2026-10-04/README.md).

Full passing and failed-run source/binary identities, raw measurements, exact authority and
checkpoint references remain in
[process evidence](test-evidence/topology-public-process-2026-10-03/README.md).
The owned Kafka/S3 fixtures retain their exact namespaces, topics and volume;
no storage reset or external deletion was used.

## Next work, in order

1. Prevent assignment admission from rescaling an Active committed topology;
   preserve current process fencing and allow complete-map replacement only.
   Repeat the three-kill native run plus another complete original-bootstrap
   restart. Preserve the full roster and 90-second Release ceiling and retain the independent
   stateful, sink and sequence oracles and pause-inclusive consumer observations.
2. Implement only removals whose stop, sink-settlement and state contracts prove
   safety. Removal also needs retired-incarnation and replay evidence; the current
   additive descriptor cannot supply it. Preserve unsupported transformation
   guards and distinct drop/recreate identities, with no external DROP deletion.
3. Reclaim old state only after an exact validated target checkpoint and replay
   horizon make it unnecessary. Preserve root audit metadata and reuse serialized
   cleanup reservations. Release alone cannot consume a root. Add bounded journal
   retirement without forgetting idempotency or replay continuity.
4. Repeat matched original/current steady process measurements with idle compilers
   and the newly qualified server. Keep preparation, pause, checkpoint, restore,
   activation and recovery-to-freshness measurements distinct.
5. Update verified operator examples, the support matrix and final handoff. Additional
   failure qualification, removals and root/journal retirement remain unfinished;
   do not mark the complete request done based on unit tests alone.

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

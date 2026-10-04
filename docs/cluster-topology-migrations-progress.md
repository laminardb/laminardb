# Cluster topology migration progress

Public additive SQL/API submission now passes the selected regression suite and
an actual one-process actor/checkpoint oracle. The new three-process Kafka/S3
migration and full-restart test is being qualified. Safe removals, root/journal
reclamation and matched performance evidence remain unfinished. The complete
requested definition of done has not yet been met.

## Baseline and current validation

- Original clean baseline: `5d81ba9b18d80343373ecfaec4793df8c5caccf1`, fetched
  from `origin/main` on 2026-09-30.
- Branch: `feature/cluster-topology-migrations`. This continuation starts from
  `1b168570b9df4a359fafb56a9a16d09d5a73d67b` with public-submission changes already
  present. No user changes were reset or stashed. No push or PR has been made.
- Windows MSVC, rustc/cargo 1.98.0, unchanged Cargo.lock. Locked dependencies
  include Arrow 58.4.0, DataFusion 53.1.0, sqlparser 0.61.0, object_store 0.13.2,
  tokio 1.53.1 and tonic 0.14.6. No AGENTS.md applies.
- The latest full four-package `cluster,aws,kafka` library/binary suite passes
  4,467 tests: core 1,107, connectors 921, DB 2,080, server 359; three existing
  tests remain ignored. Two later retention cases pass in the separate final
  focused run and are not included in that full-suite total. Eight test threads
  and 4 MiB stacks retain existing deadlines.
  All-target Clippy denies warnings; minimal server, cluster/FFI, formatting,
  diff and frozen-source checks pass. Default connector feature suites are not run.
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
| Full restart | Greatest exact target checkpoint, otherwise the authorized root; original adopted bootstrap is an assertion, never a rollback. Library actor cases pass; new multi-process test remains pending. |
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

- Public protocol/format gating, detached admission, explicit adoption, leader
  forwarding, typed HTTP/SQL receipts and idempotency are implemented.
- Actual parent/target actors execute atomic API then ordinary SQL migration.
  Aggregate 30 → 45 → 60, source cursors 6/94 then 9, increasing checkpoint epochs,
  unchanged state and one source-position resolution are asserted.
- Concurrent expected-parent requests admit exactly one operation. Terminal retry
  succeeds with the compiler held and shutdown marked; changed payload fails.
  Raw HTTP forwarding checks bearer, UUID, parent, SQL and remaining budget, typed
  fences, response bounds and redirect rejection. HTTP authorization, body and
  identity bounds and uncertain SQL operation identity pass.
- Initial fixture failures were corrected by supplying actual Running parent,
  bound checkpoint coordinator, completed startup and leased barrier transport.
  Production guards were not weakened. A Windows link failure from a nearly full
  disk was resolved by removing obsolete generated debug symbols in this worktree.
- The existing stateful three-process soak now includes public explicit adoption,
  two migrations, a proven checkpoint gate with pause-inclusive consumer timing,
  an independent new-pipeline oracle and full restart with original configuration.
  Its optimized build passes; successful process qualification remains pending.
  Older cut/abort soaks cannot certify it.
- The optimized stock server built successfully and booted three Kafka/S3 processes.
  Public legacy adoption succeeded. The migration request raced reserved checkpoint
  49 and returned 409 without admission. Checkpoint/cleanup contention now retries
  the same compiled plan under the original 45-second deadline. The cleanup-wait
  regression and all 278 focused topology tests pass; all-target Clippy passes.
  The repaired stock server subsequently completed both public migrations,
  exact target checkpoints and the consumer boundary on three processes. That
  run failed when the original kill loop attempted a two-survivor rescale before
  replacing the killed leader; complete topology recovery requires the unchanged
  owner map. A test-only full-roster replacement branch is being built. Full
  restart and final stateful/sink/sequence qualification remain pending. See
  [contention evidence](test-evidence/topology-public-race-2026-10-03/README.md).
  Partial process outcomes, identities, phase and pause-inclusive observations
  are in [process evidence](test-evidence/topology-public-process-2026-10-03/README.md).
- The corrected optimized harness linked with stock settings. An initial rerun
  hit the owned broker's partition-memory limit; raising its allocation to 4 GiB
  retained the exact volume and 112 topic definitions. The zero-kill cold-restart
  run then passed both public migrations and target checkpoints 52/55, but failed
  because recovery's Created stop returned without closing its runtime token.
  Recovery now uses observed teardown in that state; public stop behavior is
  unchanged. Its owned-task/drain regression passes in the focused library suite:
  298 tests passed and two were ignored across Core, connectors and DB. This
  command does not exercise server binary tests. The subsequent full selected
  suites passed 4,463 tests with three ignored, including 359 server tests.
  The repaired stock optimized server build completed with unchanged ThinLTO and
  one codegen unit. Attempt 12 passed both migrations, target checkpoints 27/33
  and all three cold-process replacements. Observed Created teardown and source
  settlement now succeed. Recovery then deadlocked on its exact assignment-handoff
  pin: Start required no pin, but pin retirement requires a new target checkpoint.
  No cold-restart success is claimed. The authority retained the full fresh roster
  and exact checkpoint 33, and the existing 90-second Release deadline expired.
  Recovery now accepts only a pin whose complete assignment and checkpoint
  reference equal the selected cut, retains that pin through Release, and rechecks
  pin/checkpoint/cleanup authority after I/O. A regression reproduced the guard
  failure; its full-suite rerun passed 4,464 tests with three ignored, including
  the real assignment-recovery pin regression. Native qualification still requires
  the new optimized executable. The earlier 4,463 passing tests precede this fix.
- Two deterministic cleanup regressions reproduce unsafe preflight around a
  Planned migration. The fix blocks cleanup before a cut exists and rechecks
  topology/handoff references before cursor publication. All-target Clippy passes
  with both fixes. Root-state consumption and journal reclamation remain separate
  unfinished work; this fix removes no recovery/replay pin.

- Retention protected-cut preflight now verifies every owned and referenced state
  object by complete SHA-256, using 256 KiB reads, eight concurrent requests,
  an 8192-object/4 GiB bound and a 15-second read deadline. Missing/corrupt state
  reproduced the old acceptance defect; its matrix, duplicate/bounds and stalled
  read tests pass in the 4,467-test full suite (three ignored). All-target Clippy
  denies warnings and passes. The later empty-object/final-range cases passed
  in the final focused retention run; the minimal server build also passes.
  This does not consume any migration root or change the
  64-operation journal bound. Native qualification and matched measurements remain
  pending; no complete definition-of-done claim is made.

- Fresh original/modified stock queue benchmarks completed three alternating
  100-sample trials each with idle compilers: average trial slopes were 6.05832
  and 6.02855 µs per admitted/consumed 32-batch burst (−0.49145% point estimate).
  This is shared Arrow queue cost, not production throughput or consumer latency.
  The original stock three-process steady scenario passed all final oracles in
  228.05 seconds, at 400.0 paced logical IDs/s; graph-cycle bucket upper bounds
  were p50 0.5 ms, p95 1 ms, p99 5 ms. Full-run sampled combined RSS was
  932,233,216 bytes. The matched current server run remains pending while its
  frozen 14-source stock optimized build runs. That build subsequently completed
  in 23m 53s, with unchanged ThinLTO/one-CGU settings and the retained server hash
  `f9a05d5f…`. Attempt 13 passed both migrations and exact target checkpoints
  36/42, then failed cold restore because the interval join decoder saw archived
  assignment 1 under current assignment 2. Recovery Start now succeeds, but no
  replacement Release or final oracle is claimed. The matched current steady
  run passed all final oracles in 229.53 seconds at the same paced 400.0 IDs/s;
  combined sampled RSS peaked at 805,085,184 bytes. All current p99 graph-cycle
  bounds remained 5 ms, while one node's p95 bound rose from 1 ms to 5 ms.
  Exact identities, raw samples and
  scope are in [performance evidence](test-evidence/topology-performance-2026-10-04/README.md).

## Next work, in order

1. Run and fix the new real-process public migration/restart soak. Keep its existing
   independent stateful, sink, checkpoint and sequence oracles. Record exact binary
   hashes, fault outcomes, consumer latency including the pause and peak resources.
2. Implement only removals whose existing stop, sink-settlement and state contracts
   can prove safety. Preserve unsupported transformation guards and distinct
   drop/recreate incarnations; do not delete external topics/tables as a DROP effect.
3. Reclaim unreferenced old state only after an exact validated target checkpoint
   and replay horizon make it unnecessary. Preserve metadata needed to audit roots.
   Reuse existing serialized cleanup reservations. Do not remove pins merely because
   a target is Active. The 64-identity journal needs explicit bounded reclamation.
4. Extend deterministic phase-boundary fault coverage and repeated process faults.
   Run matched baseline/current steady throughput/latency/resource measurements
   separately from preparation, cut, restore, activation and recovery freshness.
5. Update verified operator examples and the final support/evidence handoff.

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

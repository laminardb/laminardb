# Cluster topology migration implementation checkpoint

Status: legacy adoption/status increment implemented; runtime topology migration
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

## Validation log

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

## Completed increment

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

## Remaining work

1. Durable migration operation evidence/journal, expected-parent and payload-bound
   idempotency, participant capability gates, and admission serialized with actual
   checkpoint/recovery/assignment transitions. Baseline adoption is implemented;
   a target topology and migration state machine are not.
2. Exact checkpoint-bound quiescence, root pinning and authorized state restore.
3. Observed actor retirement, install/release and post-commit recovery.
4. Public SQL/atomic API, detached ownership, dry run and leader routing. The
   read-only topology status contract is implemented; write routes are not.
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
`docs/test-evidence/topology-adoption-2026-09-30`. This is a resumable checkpoint
on `feature/cluster-topology-migrations`; the final handoff identifies its exact
commit SHA. No changes were pushed and no pull request was created.

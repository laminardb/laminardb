# Pre-cut admission validation, 2026-10-01

This continues `d5867bf03e34207f7a97ac3800d4fb1eac17ad76` on
`feature/cluster-topology-migrations`, from a clean worktree. The original task
baseline remains `5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Dependency versions
and `Cargo.lock` are unchanged.

The change reserves a canonical candidate/request and exact owner/boot roster
through the existing append-only authority. The production graceful assignment
writer reserves its exact immutable drain proposal through that same authority
before publishing the raw snapshot. Cancelled publication can be materialized by
the existing watcher/driver, and its exact decision settles the reservation.
Only `Planned -> Aborted` is implemented for topology requests. Admission does
not certify compatibility, establish a checkpoint cut, commit a target or start
candidate actors. The catalog remains topology 1; runtime SQL remains `LDB-6043`.

Format 14 requires a coordinated binary upgrade, even before explicit legacy
catalog adoption, because normal assignment drains now write this format. The
operator guide describes the gate. This evidence does not certify mixed binaries.

## Deterministic validation

The 17 new admission/drain tests cover concurrent parent requests, payload-bound
retry/abort, checkpoint and drain admission races, cancellation before raw
publication, lost authority responses, term-change/recovery abort, replacement
process retry, exact drain settlement, retained anchors, corrupt plans, journal
exhaustion, deadline expiry, changed predecessor generations and bounded proposal
cleanup. They reuse the existing fault-injection stores and authority contracts.
No test here proves state-preserving logical migration.

Commands use Windows MSVC, Rust/Cargo 1.98.0 and the CI test-thread stack setting:

```powershell
$env:RUST_MIN_STACK = '4194304'
cargo test -p laminar-core --no-default-features --features cluster --lib topology_admission -- --quiet
cargo test -p laminar-core -p laminar-db -p laminar-server --no-default-features --features cluster --lib --bins -- --quiet
cargo clippy -p laminar-core -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
cargo check -p laminar-server --no-default-features
cargo check -p laminar-db --no-default-features --features cluster,ffi
```

Focused tests pass (17/17). All-target Clippy passes with warnings denied after
the final replacement-process retry fix. The final full suite passes 995 core,
1,967 DB and 354 server tests, with one existing DB test ignored.
HTTP coverage uses real routing and console authorization to check planned and
aborted status, malformed/unknown identities, exact response evidence, unchanged
committed version and corrupt-artifact failure.
The non-cluster server and cluster FFI checks pass.

## Real-process build

The rebuilt server uses `cluster,aws,kafka` and an unoptimized Windows test-only
16 MiB main stack, as required by the earlier debug-run stack overflow. Spawned
threads use `RUST_MIN_STACK=4194304`. These are functional test conditions, not a
production latency configuration.

```powershell
cargo rustc -p laminar-server --no-default-features --features cluster,aws,kafka --bin laminardb -- -C link-arg=/STACK:16777216
Copy-Item -LiteralPath target/debug/laminardb.exe -Destination target/topology-evidence/laminardb-admission-test-stack.exe
cargo rustc -p laminar-server --no-default-features --features cluster,aws,kafka --test cluster_soak
```

The copied server SHA-256 is
`0e9fca8976c70e9865eefcb0897399cca83c45177ac9226be526629201379fd1`.
The existing override checks this hash before each process spawn. Copying the
binary before rebuilding the harness prevents its Cargo binary dependency from
replacing the enlarged-stack test executable.

## Real-process result

`three_node_alo_legacy_topology_adoption_restart_soak` passes in 879.42 seconds.
Three real processes use the same verified binary and a unique shared namespace.
The cluster adopts the identical inventory at authority sequence 10, kills the
observed leader inside a checkpoint, advances to checkpoint 14 in 70.236 seconds,
rejoins that process in 68.644 seconds, then restarts every process with input
still running. All participants replay and locally activate topology 1 in 70.289
seconds. The frozen main input prefix is durable through checkpoint 46/epoch 46.

Independent Kafka oracles consume their frozen broker boundaries:

| Oracle | Expected result observed | Allowed ALO duplicates |
| --- | --- | --- |
| Bounded join, 326,129 logical input IDs | All 1,168,956 pairs | 224,312 |
| Temporal load, the same input IDs | All 326,129 pairs | 163,106 |
| Bounded join matrix | All 30 rows | 24 |
| Nullable temporal ASOF canary | 2 exact rows | 0 |
| Inner temporal probe canary | 3 exact rows | 0 |
| CoreWindow canaries | 4 window rows across 8 records | Included in the 8 records |

This is unchanged-graph upgrade/restart and assignment-path evidence. It is not
an additive SQL migration or exactly-once certification. An immutable S3 readback
at authority sequence 349 has format 14, baseline topology 1, no pending drain and
no migration journal. The exact JSON and observation summary are saved here; the
JSON SHA-256 is `6e423d97020c03aa55414907f957a0b74f2f6795131873efbd3471272b71bddc`.
Normal drain publication exercises the new format; baseline and checkpoint
progress survive restart without storage reset.

The run uses repository MinIO `RELEASE.2024-11-07T00-52-20Z` and Redpanda
`v26.1.13` fixtures, in project `ldb-topology-9929`, with task-specific container
names. With the bucket created, the exact invocation is:

```powershell
$env:RUST_MIN_STACK = '4194304'
$env:LAMINAR_SOAK_SECONDS = '5'
$env:LAMINAR_SOAK_KILLS = '1'
$env:LAMINAR_SOAK_KAFKA_PARTITIONS = '12'
$env:LAMINAR_SOAK_CHECKPOINT_SLO_MODE = 'observe'
$env:LAMINAR_SOAK_HOT_SLO_MODE = 'observe'
$env:LAMINAR_SOAK_CHECKPOINT_URL = 's3://topology-tests-9929/checkpoints'
$env:LAMINAR_SOAK_S3_ENDPOINT = 'http://127.0.0.1:19000'
$env:LAMINAR_SOAK_S3_ACCESS_KEY = 'laminar'
$env:LAMINAR_SOAK_S3_SECRET_KEY = 'laminar-test-secret'
$env:LAMINAR_SOAK_S3_REGION = 'us-east-1'
$env:LAMINAR_SOAK_KAFKA_SOURCE_BROKERS = '127.0.0.1:19092'
$env:LAMINAR_SOAK_LAMINARDB_EXE = (Resolve-Path -LiteralPath target/topology-evidence/laminardb-admission-test-stack.exe).Path
$env:LAMINAR_SOAK_LAMINARDB_SHA256 = '0e9fca8976c70e9865eefcb0897399cca83c45177ac9226be526629201379fd1'
& 'target/debug/deps/cluster_soak-76640dbcab3dbe20.exe' three_node_alo_legacy_topology_adoption_restart_soak --ignored --nocapture
```

Five seconds is the steady observation interval after the fault/restart proofs,
not the full test duration. These credentials are public fixture defaults. The
fixture names/executable filename are specific to this local test build. All
spawned server processes exited. Only this Compose project's two containers and
network were removed after preserving evidence; unrelated containers were kept.

These requests also ran against the real fixture:

```powershell
curl.exe --silent --show-error -H 'Authorization: Bearer laminardb-cluster-soak' 'http://127.0.0.1:19310/api/v1/cluster/topology'
curl.exe --silent --show-error -H 'Authorization: Bearer laminardb-cluster-soak' 'http://127.0.0.1:19310/api/v1/cluster/topology/operations/00000000-0000-0000-0000-000000000099'
```

The catalog response reports committed/local version 1. The unknown operation
returns 404 with `Cache-Control: no-store`; omitting authorization returns 401
when serving is healthy. During coordinated recovery the serving fence returns
503. Saved response bodies/headers and unit router tests cover the distinction.
No command above submits or activates a topology migration.

The debug run did not meet performance certification. SLOs were `observe`;
only 44.05% of 84 measured stalls were within 1,024 ms and checkpoint duration
averaged 10,800 ms over 86 observations. A subscription-encoding deadline also
triggered an additional coordinated recovery before the injected kill. Hot-cycle
histograms exclude checkpoint/recovery pauses, and accounted state is not peak
RSS. This run cannot establish comparative migration or consumer latency.

The result excerpt is saved here. Full logs remain under ignored
`target/topology-evidence/admission-process-soak.txt`; process and timing logs are
under `target/tmp/soak-471780-1790835647243218300`. S3 fixture data was ephemeral;
the retained readback provides the stated authority observation.

## Steady queue comparison

The existing `accepted_push/arrow_16` benchmark accepts and drains 32 shared Arrow
batches of 256 rows per iteration on one thread. It measures a queue burst, not
individual event latency, an active cluster, checkpoint contention or a migration.
The original baseline and its raw samples remain in the
[2026-09-30 evidence](../topology-adoption-2026-09-30/README.md).
This comparison ran after both compilers finished and before the process soak.

```powershell
$env:CRITERION_HOME = (Resolve-Path -LiteralPath target/topology-baseline/crates/laminar-core/target/criterion).Path
cargo bench -p laminar-core --no-default-features --features cluster --bench streaming_bench --target-dir target/topology-benchmark-target -- accepted_push/arrow_16 --warm-up-time 1 --measurement-time 2 --sample-size 30 --baseline topology-original
```

| Build | Criterion burst estimate |
| --- | --- |
| Original task baseline | 6.1545 us |
| This increment | 6.1181 us, interval [6.0448, 6.2065] us |

Criterion reported no significant change (`p=0.07`, 30 samples), with its relative
change interval [-4.6848%, -0.1404%]. The exact console output, modified samples,
estimates and relative-change estimates are saved alongside this note. Do not
derive consumer p50/p95/p99 or an unqualified production throughput claim from
these iteration samples.

## Remaining certification

The public stateful additive-migration oracle, checkpoint cut/state mapping,
actor retirement/target release, SQL/dry-run submission, post-commit failures,
subscription cutover, removal/replacement, mixed-version rejection and EO
migration are not implemented or certified. Migration preparation/cutover and
pause-inclusive consumer latency, RSS, allocations and migration artifact growth
cannot be measured until the cutover exists. Ordinary queue benchmarks cannot
substitute for those measurements.

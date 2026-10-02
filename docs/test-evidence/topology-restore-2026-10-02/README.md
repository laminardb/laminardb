# Private target restore preparation, 2026-10-02

This continuation starts clean at `9e525345e95b960315d6305a4ff93c1664de6a8d`
on `feature/cluster-topology-migrations`. The original project baseline is
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Windows MSVC and Rust/Cargo 1.98
are unchanged. The workspace requires Rust 1.95. Locked versions include
DataFusion 53.1.0, Arrow 58.4.0, object_store 0.13.2, Tokio 1.53.1, rdkafka
0.39.0 and async-trait 0.1.92. No Cargo file, external dependency or version changes.

The DB now prepares an opaque unstarted target image from a held CutPrepared
operation with its published root. Its configured controller checks the complete
current process roster, admitting leader, exact old Commit and frozen assignment.
The isolated compiler checks the immutable target, full descriptor and environment.
Recovery verifies the historical parent manifests and reconstructs every root
requirement before loading state. Existing operator codecs decode local state;
preserved subscription incarnations and exclusive sequences must equal the root.
The parent checkpoint identity stays unchanged, and ordinary recovery using the
target fingerprint remains rejected.

Existing source positions retain their real committed attempt/assignment. New
positions retain the sealed global unowned vector without an invented processing
attempt. Kafka validates exact broker inventory and retention bounds, including
empty partitions, without resolving latest again or starting/acknowledging a reader.
It reuses the existing tracked native task, one-client permit and 10-second budget.
Final installation must validate availability and select owned partitions again.

One returned image owns the existing compiler permit until dropped. Cancellation,
failure and the 45-second total deadline discard partial state and keep the parent
hold. Aggregate manifest metadata is capped at 16 MiB, the root at 1 MiB, verified
payload and decoded state use the configured graph budget, and node reads retain
the configured checkpoint cap. Encoded state buffers are released before source
validation. Old graph state, target state, codec scratch and read buffers overlap
transiently; these are separate component limits, not a total-process RSS cap.

No new log append, authority encoding, phase, scheduler or generic framework is
introduced. Success changes no live catalog/coordinator and grants no install,
source/sink actor, output or intake-release authority. Atomic topology Commit,
observed old-actor retirement, installation, participant-complete Release, detached
ownership and public submission remain unfinished. LDB-6043 stays in place.
This increment does not meet the original runtime migration definition of done.

## Unit and compatibility checks

All Cargo commands use `CARGO_BUILD_JOBS=1`, `RUST_MIN_STACK=4194304` and `--locked`.

```powershell
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_restore -- --nocapture
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins -- --quiet
cargo clippy --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
cargo check --locked -p laminar-server --no-default-features
cargo check --locked -p laminar-db --no-default-features --features cluster,ffi
cargo test --locked -p laminar-core --no-default-features --lib -- --quiet
cargo fmt --all -- --check
git diff --check
```

The selected-feature suite passes **4,309 tests**: 1,044 core, 914 connectors,
1,995 DB and 356 server. The broker test and one existing model-download test
are ignored in that run. All 14 focused restore tests also pass. All-target
Clippy passes with warnings denied. Result exports omit the existing MSVC OpenSSL
missing-PDB warning prelude; those linker diagnostics are retained in raw local
logs. Full default-feature suites and the model-download test are not run.

Non-default server and cluster/FFI checks, all **414 non-cluster core tests**,
formatting and diff checks pass. See [unit results](unit-results.txt),
[focused tests](restore-tests.txt) and [build checks](build-checks.txt).

New tests cover root absence, exact process/boot/term, new-leader abort, unchanged
authority/catalog, root requirement reconstruction, sealed cursor reuse, strict
ordinary target recovery, payload bounds, missing/corrupt state, cancellation,
deadline and recovery fencing after decoding. The DB fixture captures actual
aggregate/channel state and eight vnode archives, then checks that a new target
continues its sum from 30 to 45 rather than cold-starting at 15. It checks preserved
subscription generation 7 and nonzero sequences, distinct old/new cursor origins,
retained-image busy behavior, retry without resealing, no connector lifecycle
effects, no actors and the unchanged held parent. It is an actorless component test.

## Broker validation

The ignored real-broker test uses the repository's isolated Redpanda fixture at
`127.0.0.1:19092`:

```powershell
$env:LAMINAR_KAFKA_TEST_BROKERS = '127.0.0.1:19092'
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_initialization_real_broker -- --ignored --nocapture
```

It seals `[2, 3, 0]` on its own three-partition topic, validates the same vector,
appends another record and validates again without moving the cursor. Earliest
still resolves `[0, 0, 0]`. An independent observer checks absent committed group
offsets. The source remains Created with no reader, and the test deletes only its
unique topic. These are metadata/control-path observations, not output guarantees
or comparative latency certification.

The broker test passes in **1.34 seconds**. Its latest lookup takes **278.283 ms**;
validation succeeds before and after the append with the same `[2, 3, 0]` vector,
no reader and no committed group offsets. See [broker results](kafka-broker-results.txt).

## Real-process cut, verified state reads, abort and restart

The existing stateful three-process scenario independently certifies a candidate,
holds its exact old cut and stages its root with Kafka `[2, 3, 0]` initialization.
The harness now invokes production root authorization and strict RecoveryManager
loading for each exact frozen process, verifies only that process's local state
frames and records payload bytes/read time in `topology-restore-observations.json`.
It also validates the sealed Kafka cursor without reevaluating it.

These library calls run in the harness against the three processes' actual held
checkpoint artifacts. They do not decode/retain target images inside the server
processes; actual operator decoding is tested separately above. The target is never
committed or installed. The existing independent old-graph output oracles, full
restart on the same namespace, retained root and pre-commit abort remain required.

The scenario passes in **325.65 seconds**. All three exact processes certify the
51-object target. The old cut is checkpoint **63**. Root staging takes **457.883 ms**
and binds a **51,859-byte** root at authority **451**, retaining 47 preserved object
mappings, nine subscription vectors and one sealed Kafka initialization. Total
participant manifest metadata is **607,278 bytes**.

| Frozen participant (sorted identity) | Verified local frames | Verified payload bytes | Root authorization and state verification |
| --- | ---: | ---: | ---: |
| First | 528 | 7,396,639 | 299.247 ms |
| Second | 552 | 7,151,074 | 310.187 ms |
| Third | 528 | 5,789,047 | 302.594 ms |

These are **1,608 frames / 20,336,760 bytes** in total. Read-only sealed Kafka
validation takes **110.653 ms** and keeps `[2, 3, 0]`. Full restart activates
unchanged topology 1 in **57.430 s**; pre-commit abort at authority **453** retains
the root and successful old cut. All expected bounded/temporal outputs are observed
across **114,027 logical input IDs**, durable through checkpoint **109**, with
allowed ALO duplicates. Logged intake hold through deliberate restart/recovery is
**61.426..61.503 s**. No target image is installed or output permitted.

The sampled combined server working-set maximum is **745,369,600 bytes**, and the
harness's whole-run observed peak is **144,019,456 bytes**. The wrapper samples at
a nominal one-second interval (306 observations) and sees seven server processes;
all exit. These are functional/control-path and whole-run observations. No matched
baseline, target-decoding allocation profile or consumer-visible pause-inclusive
migration latency distribution is established.

See [restore observations](topology-restore-observations.json),
[combined observations](restore-observations.json),
[resource samples](restore-soak-01-resources.json),
[scenario output](restore-soak-01.stdout.txt) and
[oracle/timing log](restore-soak-01.stderr.txt). The checkpoint JSONL files retain
261 exact timing records, with zero missing durable-tail handoffs or exhausted
checkpoint deadlines.

The optimized repository soak profile retains debug assertions and overflow checks:

```powershell
cargo build --locked --profile soak -p laminar-server --no-default-features --features cluster,aws,kafka --bin laminardb --test cluster_soak
Copy-Item -LiteralPath target/soak/laminardb.exe -Destination target/topology-evidence/laminardb-restore-test-stack.exe
& 'C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Tools\MSVC\14.43.34808\bin\Hostx64\x64\editbin.exe' /STACK:16777216 target/topology-evidence/laminardb-restore-test-stack.exe
```

The Windows test copy has a 16 MiB main-thread stack; the normal binary keeps its
1 MiB default. Payload hashes after PE headers must match. `run-soak.ps1` records
the exact test environment, binaries, sample interval and process resource
observations. Changed Rust sources and Cargo metadata are hashed before/after the
build/scenario. This is a dirty checkout built from the starting commit above;
the [hash inventory](restore-build-identity.json) identifies the added implementation
precisely. All 32 changed Rust source hashes, eight Cargo metadata hashes and both
server/harness hashes match before/after the scenario. PE payloads after their
1,024-byte headers match between normal and test-copy servers. The optimized build
passes in 29m 24s; see [build result](restore-soak-build-results.json).

The task's isolated Docker project is `ldb-topology-9929` and bucket is
`topology-tests-9929`; credentials are repository fixture credentials. Only its
MinIO/Redpanda containers/network are removed after collecting evidence. No
production namespace or unrelated container is modified.

```powershell
docker compose -p ldb-topology-9929 -f tests/docker/compose.yml -f target/topology-evidence/compose.yml up -d --wait minio redpanda
docker exec laminardb-topology-9929-minio mc alias set topology-local http://127.0.0.1:9000 laminar laminar-test-secret
docker exec laminardb-topology-9929-minio mc mb --ignore-existing topology-local/topology-tests-9929
& target/topology-evidence/run-restore-soak.ps1
docker compose -p ldb-topology-9929 -f tests/docker/compose.yml -f target/topology-evidence/compose.yml down
```

[The wrapper](run-soak.ps1) and [container-name overrides](compose-overrides.yml)
are saved here. The fixtures have no named data volume; teardown removes only this
ephemeral fixture namespace after its artifacts are exported. Full restart within
the test uses the same namespace without resetting the catalog or checkpoint state.

The [progress checkpoint](../../cluster-topology-migrations-progress.md),
[operator guide](../../cluster-topology-operations.md) and
[engineering guide](../../cluster-topology-engineering.md) describe the supported
subset and remaining work. Full default-feature/external connector suites,
transactional-sink migration, target install/post-commit faults, matched baseline
resource comparisons and pause-inclusive migration latency remain unverified.

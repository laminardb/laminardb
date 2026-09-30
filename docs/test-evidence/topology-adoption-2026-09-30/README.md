# Local topology adoption evidence

Scope: legacy identical-inventory adoption and status. Runtime graph migrations
remain disabled. These measurements do not certify migration latency or delivery.

Environment: Windows 11 Pro 10.0.26200, AMD Ryzen 9 7900X (12 cores / 24 logical
processors), 31.1 GiB visible RAM, MSVC rustc/cargo 1.98.0. This is a shared desktop
run with no CPU isolation. No dependencies were upgraded. The queue benchmark
uses `--no-default-features --features cluster`, the existing release/bench
profile (opt-level 3, thin LTO, one codegen unit), 1 second warmup, 2 second
measurement and 30 Criterion samples per version.

## Queue comparison

The existing `accepted_push/arrow_16` case admits and consumes 32 Arrow batches
of 256 string rows per iteration through a 64-entry queue. Batch clones preserve
Arrow buffer ownership. Its throughput counts rows referenced by queue messages;
it does not measure row processing or an external consumer.

Baseline: clean starting commit `5d81ba9b18d80343373ecfaec4793df8c5caccf1`, archived
under ignored `target/topology-baseline`. Modified: working tree with the final
adoption/status control-read deadline and local fencing checks. The final queue
measurement ran after the earlier soak's processes exited.

| Burst time (Criterion estimate, 95% interval) | Baseline | Modified |
| --- | --- | --- |
| Nanoseconds per 32-batch burst | 6,154.5 [6,083.3, 6,263.4] | 6,210.7 [6,169.4, 6,276.7] |

Criterion's comparison reports relative change [-2.7019%, -0.2923%, +1.8246%],
p = 0.82; no significant change detected in this short run. Baseline had 5/30
outliers; modified had 3/30. This is limited evidence about the existing queue
path, not a production throughput/latency guarantee.

Raw Criterion files and [final console output](modified-queue-benchmark.txt):
[baseline samples](baseline-samples.json),
[baseline estimates](baseline-estimates.json), [modified samples](modified-samples.json),
[modified estimates](modified-estimates.json), [change estimates](change-estimates.json).
`sample.json` contains aggregate iteration timings, not per-row or per-consumer
latency observations. p50/p95/p99 end-to-end latency cannot be inferred from it.

Commands actually used from the baseline checkout and working tree, respectively:

```powershell
cargo bench -p laminar-core --no-default-features --features cluster --bench streaming_bench --target-dir '..\topology-benchmark-target' -- accepted_push/arrow_16 --warm-up-time 1 --measurement-time 2 --sample-size 30 --save-baseline topology-original

# Use the same Criterion data directory for both copies of the checkout.
$env:CRITERION_HOME = (Resolve-Path 'target\topology-baseline\crates\laminar-core\target\criterion').Path
cargo bench -p laminar-core --no-default-features --features cluster --bench streaming_bench --target-dir 'target\topology-benchmark-target' -- accepted_push/arrow_16 --warm-up-time 1 --measurement-time 2 --sample-size 30 --baseline topology-original
```

The first attempted comparison failed because Criterion's output directory was
relative to the different checkout, despite the common Cargo build target. The
successful comparison uses an explicit `CRITERION_HOME`. Failed-run log is retained
under ignored `target/topology-evidence/modified-streaming.txt`.

## Functional validation

With repository CI's `RUST_MIN_STACK=4194304`, the cluster feature suites passed:
core 978 tests, DB 1,916 tests, server HTTP 83 tests. No artifact corruption or
uncertain write response is converted to an empty/legacy success fallback.

All-target Clippy passed with `cluster,aws,kafka` and `-D warnings`. The server
also passed `cargo check -p laminar-server --no-default-features`; the DB passed
`cargo check -p laminar-db --no-default-features --features cluster,ffi`.

## Real-process legacy adoption and restart

The final rebuilt soak passed: 1 test in 520.26 seconds. See the checked-in
[result excerpt](real-process-result.txt) and [actual HTTP response](status-example.json).
The existing independent ALO Kafka oracles found all 673,613 bounded-join pairs
and 188,194 temporal-load pairs across 188,194 logical input IDs, with 30,094 and
16,129 allowed replay duplicates respectively. The bounded matrix, temporal
canaries and windows passed their independent expected-row checks. This proves
an identical-inventory format upgrade with stateful recovery, not an additive
topology migration or exactly-once external effects.

All three processes used the same verified executable, SHA-256
`64908ddc4cef345c3db5b80bd3e5cc36709d70918845b2742d26fea79ff8a716`.
It is a Windows unoptimized debug build with a test-only 16 MiB main stack;
spawned threads use CI's `RUST_MIN_STACK=4194304`. The initial normal debug binary
overflowed its main stack before adoption. No stack setting was changed in the
repository's runtime configuration. The earlier harness executable also failed
its progress check after stopping input; the final build restarts while input is
live and finalizes timing evidence before retiring each process.

The shared S3-compatible namespace came from the repository MinIO fixture,
`s3://topology-tests-9929/checkpoints`, and Kafka from the repository Redpanda
fixture. Docker project `ldb-topology-9929` was isolated from other containers.
Its services and network were removed after the passing test.

Observed boundaries: checkpoint 1 before adoption, adoption authority sequence
10, leader failover to checkpoint 12 in 64.927 s, rejoin in 68.695 s, all-node
restart and local activation in 68.145 s, and frozen input durable at checkpoint
36. These are observations from a shared desktop debug run. Only 54.79% of 73
observed pipeline stalls were within 1024 ms; checkpoint duration averaged
9445 ms over 70 observations. The run did **not** meet performance certification.
Its hot-cycle histogram bounds exclude checkpoint/recovery pauses and are not
consumer-visible p50/p95/p99 measurements. Accounted operator-state bytes are
not peak process RSS.

Commands used to build the Windows harness and enlarged-stack server:

```powershell
cargo rustc -p laminar-server --no-default-features --features cluster,aws,kafka --test cluster_soak
cargo rustc -p laminar-server --no-default-features --features cluster,aws,kafka --bin laminardb -- -C link-arg=/STACK:16777216
```

The server was copied to `target/topology-evidence/laminardb-test-stack.exe` before
rebuilding the harness, and selected with the existing executable/hash override.
With the fixture bucket already created, the exact final test invocation was:

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
$env:LAMINAR_SOAK_LAMINARDB_EXE = (Resolve-Path 'target/topology-evidence/laminardb-test-stack.exe').Path
$env:LAMINAR_SOAK_LAMINARDB_SHA256 = '64908ddc4cef345c3db5b80bd3e5cc36709d70918845b2742d26fea79ff8a716'
& 'target/debug/deps/cluster_soak-76640dbcab3dbe20.exe' three_node_alo_legacy_topology_adoption_restart_soak --ignored --nocapture
```

The five seconds configure the steady observation period after fault/restart
proofs, not total test duration. The artifact filename is specific to this
feature/toolchain build. The test's fixture credentials above are public defaults,
not deployment credentials. The HTTP request below was also executed successfully
against node 0 after full restart; the unauthenticated request returned 401:

```powershell
curl.exe --fail --silent --show-error -H 'Authorization: Bearer laminardb-cluster-soak' 'http://127.0.0.1:19310/api/v1/cluster/topology'
```

Complete local logs remain under ignored `target/topology-evidence`; process logs
and full checkpoint timing evidence are under `target/tmp/soak-387236-1790805251475831400`.
The checked-in result is an excerpt, not a replacement for the full timing logs.

Not measured: migration preparation latency, cutover pause, target state restore
and activation, failure-to-freshness for a migrated graph, pause-inclusive
consumer latency, peak RSS, allocations, queue depth during migration, and
migration-root artifact growth. No runtime graph cutover exists to measure yet.

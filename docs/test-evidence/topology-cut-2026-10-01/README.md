# Old-topology checkpoint cut validation, 2026-10-01

This continuation starts clean at `ab18ca39b4ce07d6f00ad2d63e513a6ead54473f`
on `feature/cluster-topology-migrations`. The original baseline remains
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Toolchain, locked dependencies and
the initial support matrix are recorded in the
[progress file](../../cluster-topology-migrations-progress.md).

The implemented slice is `Planned -> Quiescing -> CutPrepared`, with pre-target
abort and coordinated recovery of the unchanged topology. It atomically binds
artifact admission to the exact old attempt, preserves a definitive old checkpoint
Commit, and waits for every frozen process's application receipt. The leader's
receipt follows the existing aggregated external sink settlement. Intake and
successor sink publication stay held. No candidate actor or topology 2 is installed.

Normal cluster topology SQL remains rejected with LDB-6043. Candidate planning,
compatibility certificates, target mapping/install/release and public submission
are unfinished. A real cut/restart result does not satisfy the requested additive
SQL migration definition of done or certify exactly-once migration.

## Deterministic and build validation

With `RUST_MIN_STACK=4194304`:

```powershell
cargo test -p laminar-core -p laminar-db -p laminar-server --no-default-features --features cluster --lib --bins -- --quiet
cargo clippy -p laminar-core -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
cargo check -p laminar-server --no-default-features
cargo check -p laminar-db --no-default-features --features cluster,ffi
cargo test -p laminar-core --no-default-features --lib -- --quiet
```

The complete cluster suite passes 1,009 core, 1,970 DB and 354 server tests; one
existing DB test is ignored. All-target Clippy and both compatibility builds pass.
The non-cluster core suite passes 414 tests. A preliminary non-cluster filtered
`topology_cut` command selected zero tests because cluster control is absent; the
complete non-cluster suite above is the meaningful verification.

Fourteen new core tests cover exact binding, all-process completion, stale boots,
unknown successful binding/Commit/receipt responses, caller cancellation, definitive
Abort ownership, leader change, recovery faults, bounded pruning, damaged index,
malformed/rewound evidence, a semaphore-controlled leader race and a paused-time
binding deadline. Runtime tests cover real Prepare/artifact binding before intake
capture, manual ownership, exclusive/replay-free flags, and fenced successor sink
writes on completion publication failure. These are authority/runtime boundary
tests, not target state compatibility or transactional connector certification.
The real in-memory runtime test also forces ordinary flag selection and exact
reservation before core topology admission. Prepare rejects the stale flags;
the original leader proof persists an exact Abort without fanout or intake hold.
The same operation remains Planned and binds a later monotonic attempt.
Reservation-only cleanup requires no prepared state or transactional sinks and
atomically excludes admitted artifacts. Exact retry audits the original Abort
append. A semaphore test lets cut admission win that append; normal recovery
remains mandatory after admission. The initial proof-only fix exposed this
second recovery boundary in the runtime test and was extended accordingly.
The existing recovery release test also verifies that a rejected release retains
the cut hold and an authorized retry clears it. The DB status fixture drives the
same capture helper and checks that assignment-style reopening leaves intake closed
and local activation null. The hold uses the existing authority-transition lock;
the data path keeps its existing gate read.

An early core run exposed compatibility regressions in remote barrier validation;
the existing authority checks/error behavior are preserved in the final suite.
One new sink test initially assumed explicit recovery epoch opening must fail;
the corrected test verifies ordinary writes remain fenced and the committed
checkpoint reports its continuation error. Failed preliminary logs remain under
ignored `target/topology-evidence`.

## Real-process build and invocation

The existing Kafka/S3 stateful harness is extended with
`three_node_alo_topology_cut_abort_restart_soak`. It stages a changed catalog
payload through the trusted core admission API, then uses the existing authenticated
`POST /api/v1/checkpoint` route and leader forwarding. It checks CutPrepared and
the complete exact process roster, reads held topology/operation status on every
process, and restarts all processes on the same namespace. Existing independent
bounded/temporal join, matrix aggregate and window oracles continue across restart.
The staged candidate is never compiled or activated.

```powershell
$env:RUST_MIN_STACK = '4194304'
cargo rustc --profile soak -p laminar-server --no-default-features --features cluster,aws,kafka --bin laminardb -- -C link-arg=/STACK:16777216
Copy-Item -LiteralPath target/soak/laminardb.exe -Destination target/topology-evidence/laminardb-cut-test-stack.exe
cargo rustc --profile soak -p laminar-server --no-default-features --features cluster,aws,kafka --test cluster_soak
```

The copied test server SHA-256 is
`998306cc9ced476176f51241816c988ab72f38ef26733edf902fe9f4da67af9e`,
169,088,512 bytes. The repository's optimized `soak` profile retains debug
assertions and overflow checks. The Windows test binary uses a 16 MiB main stack;
spawned threads use the existing CI stack setting. The harness build may replace
the normal binary, so the copy is made first. Existing OpenSSL missing-PDB linker
warnings appear in both builds. No library versions or production stack settings
were changed.

Isolated fixtures use the repository Compose file and a local override changing
only the two container names:

```powershell
docker compose -p ldb-topology-9929 -f tests/docker/compose.yml -f target/topology-evidence/compose.yml up -d --wait minio redpanda
docker exec -e MC_HOST_topology=http://laminar:laminar-test-secret@127.0.0.1:9000 laminardb-topology-9929-minio mc mb --ignore-existing topology/topology-tests-9929
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
$env:LAMINAR_SOAK_LAMINARDB_EXE = (Resolve-Path -LiteralPath target/topology-evidence/laminardb-cut-test-stack.exe).Path
$env:LAMINAR_SOAK_LAMINARDB_SHA256 = '998306cc9ced476176f51241816c988ab72f38ef26733edf902fe9f4da67af9e'
& 'target/soak/deps/cluster_soak-d825d342a31555ec.exe' three_node_alo_topology_cut_abort_restart_soak --exact --ignored --nocapture
```

These credentials are public fixture defaults. Five seconds is the final steady
observation interval, not the complete fault/restart/oracle run. The actual
invocation uses a small local PowerShell wrapper to sample only this test binary's
process working sets once per second and preserve harness stdout/stderr separately.
It does not change server code or sampled event/queue paths. Prepared/aborted
operation JSON is saved with the surviving node logs under `target/tmp/soak-*`.

The first real-process run failed in 293.28 s before cut binding: ordinary
checkpoint 18 raced with topology admission and its pre-Prepare abandonment had
no retained leader proof. It faulted the pipeline instead of persisting Abort.
The fix retains proof ownership at exact checkpoint reservation, including
failure before Prepare. The failed stdout/stderr/resource logs are retained
locally as `cut-soak-01.*`.

The second run failed in 298.04 s after the manual checkpoint returned success
and checkpoint 19 reached CutPrepared with every frozen process receipt. Its
ordinary checkpoint 18 lost the admission race and now aborted cleanly. The
assignment watcher then reopened the source gate while refreshing its certificate.
The fix preserves a specific topology-cut hold under the existing authority lock;
only the existing authorized coordinated recovery Release can clear it. Assignment
refresh retains the certificate needed by the cut tails, without reopening intake
or a successor sink epoch. Failed logs are retained locally as `cut-soak-02.*`.
The third unoptimized run failed in 497.60 s at the unchanged 90 s full-restart
activation wait. It reached checkpoint 21/CutPrepared in 20.366 s at binding
sequence 264, Commit 277 and complete-receipt sequence 281. All three HTTP status
checks observed held intake. Every restarted process restored checkpoint 21;
the next coordinated recovery Start selected the same cut and was still restoring
at timeout. Failed logs and resource observations remain locally as `cut-soak-03.*`.
This debug binary's SHA-256 was
`b2b922778f32c8d47404a63284700a4f4e4b828ba307b019d77f46d1d7f6812a`
(236,268,032 bytes).
Readback of authority sequence 313 confirms format 15, candidate abort
`leader_changed` at sequence 284, unchanged catalog and Commit head 277/checkpoint
21, and the exact retained cut/receipt roster. See
[the readback summary](failed-debug-authority-summary.json) and
[prepared cut](failed-debug-cut-prepared.json). This proves retained authority,
not successful restart activation.
The final run uses the repository's optimized `soak` profile, retaining debug fault
injection, overflow checks and the same recovery deadline. It passes in 313.58 s.

## Final real-process result

Three real processes establish checkpoint 2, adopt the unchanged catalog at
authority sequence 15, and persist temporal history through checkpoint 21 before
the observed leader is killed inside a checkpoint. Survivors advance to checkpoint
28 with exact assignment evidence in 41.118 s; rejoin takes 47.258 s.

Operation `cc6bdb0a-7488-4042-86b7-58a8efe159ac` is admitted at sequence 330.
Checkpoint 47 reaches CutPrepared in 2.144659 s after the manual request starts:
binding sequence 332, definitive Commit 338 and all three exact receipts at 342.
Each process reports committed topology 1 and locally active version null while
held. The [prepared status](topology-cut-prepared.json) includes the exact old
pipeline identity, assignment digest, boots and checkpoint root.

All processes restart on the same namespace and activate the unchanged topology
in 45.102 s. The candidate aborts with `leader_changed` at sequence 346, preserving
its exact cut and receipts. Every process returns the same
[aborted status](topology-cut-aborted.json). Continued input advances through
checkpoint 82 before its frozen prefix is checked by independent Kafka oracles:

| Oracle | Expected result observed | Allowed ALO duplicates |
| --- | --- | --- |
| Bounded join, 109,733 logical input IDs | All 392,430 pairs | 5,561 |
| Temporal load, the same input IDs | All 109,733 pairs | 1,132 |
| Bounded join matrix | All 30 rows | 0 |
| Nullable temporal ASOF canary | 2 exact rows | 0 |
| Inner temporal probe | 3 exact rows | 0 |
| CoreWindow canaries | 4 window rows | 4 total ALO records |

The [stdout](real-process-stdout.txt) and [stderr](real-process-stderr.txt) retain
the complete result, observations and oracle counts. The namespace is
`s3://topology-tests-9929/checkpoints/606064-1790863889993291700/checkpoints`.
No target is installed; this is an old-cut abort/restart test.

[Gate observations](gate-hold-observations.json) span old-process capture through
authorized new-process recovery release: 46.907 s (node 0), 46.698 s (node 1) and
46.738 s (node 2). These include cut settlement and the deliberate all-process
restart. They measure control-gate closure, not consumer-visible latency or target
activation. Hot-cycle histograms exclude these pauses: node 0 and node 1 report
p50 <= 0.5 ms, p95 <= 1 ms and p99 <= 5 ms; node 2 reports p50 <= 0.5 ms and
p95/p99 <= 5 ms. They are observational, with no matched live-pipeline baseline.
The checkpoint profile records 178 stalls all <= 1.024 s, and 173 checkpoint
durations averaging 2.043 s. State capture remains non-certifying because the
retained-state floor is disabled.

[Process observations](process-resources.json) contain 280 nominal one-second
samples across seven server processes. The maximum sampled combined working set
is 729,796,608 bytes; per-process peak working-set observations range from
181,440,512 to 298,909,696 bytes. Shared pages may be counted more than once.
These are not unique RSS, allocations or comparative resource certification.
Accounted operator state is also recorded in stderr, including 105,707,790 bytes
for temporal load; it is not process memory. All servers exited after the test.
Only this task's Compose project is removed without deleting its volumes.

## Queue comparison

```powershell
$env:CRITERION_HOME = (Resolve-Path -LiteralPath target/topology-baseline/crates/laminar-core/target/criterion).Path
cargo bench -p laminar-core --no-default-features --features cluster --bench streaming_bench --target-dir target/topology-benchmark-target -- accepted_push/arrow_16 --warm-up-time 1 --measurement-time 2 --sample-size 30 --baseline topology-original
```

Original time is 6.1545 us and the current estimate is 6.0732 us per accepted
32-Arrow-batch burst (256 rows per batch). The current interval is
[6.0447, 6.1036] us. Criterion reports relative time interval
[-5.0363%, -0.7764%], p=0.01, and **Change within noise threshold**. The push path
was not modified; this is no basis for claiming a throughput improvement.

Measurements run after builds and before the soak. This benchmark covers queue
operations, not row latency, source/sink effects, a live pipeline, control-path
authority reads or migration pauses. Checkpoint admission adds bounded shared
authority reads; its performance requires separate production measurement. The
soak's SLOs are observational and its hot-path histograms exclude control pauses.

## Remaining certification

No public additive migration, compatibility/state mapping, target install/release,
post-target-commit failure, removal/replacement, subscription cutover, new-source
latest position, mixed-binary rejection, exactly-once migration or full connector
suite is certified. The full target phase fault matrix and comparative preparation,
activation, allocation, retained-artifact growth and pause-inclusive consumer
latency measurements require those missing runtime links. Process working-set
observations have no matched resource baseline and are not an allocation or unique
RSS measurement. See the [engineering guide](../../cluster-topology-engineering.md).

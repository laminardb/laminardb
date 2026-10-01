# Exact-cut root staging validation, 2026-10-01

This continuation starts clean at `9db1b6862309ebc27917c7bc01da86fb88dbd4ef`
on `feature/cluster-topology-migrations`. The original baseline remains
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Rust/Cargo 1.98, locked dependencies
and feature selection are unchanged. See the
[implementation checkpoint](../../cluster-topology-migrations-progress.md) and
[operator guidance](../../cluster-topology-operations.md).

Authority format 17 pins a canonical, immutable root only after complete
participant certification and application of the exact old cut. Preserved
objects keep their incarnations and certified state identities. Source/snapshot/
channel progress, state ranges, sink decisions and output segments remain in the
referenced old checkpoint. Preserved subscription certificates change only their
required target pipeline identity and keep their stream generation and complete
partition sequence vector. The existing recovery progress validator is shared
with root staging; the strict pipeline identity remains version 7.

The DB API uses the configured checkpoint reader and actual process/assignment
authorities, requires the held cut and closed intake, and starts no actors.
Root staging reads metadata only, bounded at 16 MiB aggregate participant
manifests and a 1 MiB root. The DB deadline is 30 seconds; authority staging uses
15 seconds/16 CAS attempts. Retries, cancellation and lost responses resolve to
one immutable authority binding. Status/pruning audit and retain its first append.
Abort retains the root metadata; ordinary checkpoint/replay retention then owns
old payload artifacts.

This is an internal DB/core increment. No target catalog can be committed,
restored or activated. New-source additions are rejected until concrete connector
positions can be resolved once and persisted. Normal cluster SQL remains guarded
by LDB-6043. It does not satisfy the original runtime migration definition of done.

## Deterministic and build validation

With `CARGO_BUILD_JOBS=1` and `RUST_MIN_STACK=4194304`:

```powershell
cargo test -p laminar-core -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_root -- --quiet
cargo test -p laminar-core -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins -- --quiet
cargo clippy -p laminar-core -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
cargo check -p laminar-server --no-default-features
cargo check -p laminar-db --no-default-features --features cluster,ffi
cargo test -p laminar-core --no-default-features --lib -- --quiet
cargo fmt --all -- --check
git diff --check
```

The focused run passes **10 core and one DB test**. The full cluster suite passes
**1,030 core, 1,987 DB and 356 server tests**, with one existing DB test ignored.
[Unit results](unit-results.txt) retain full-suite output after the compiler/linker
warning prelude. All-target Clippy, non-cluster server and cluster FFI checks,
all **414 non-cluster core tests**, formatting and diff checks pass. Exact commands
and outputs are in [build checks](build-checks.txt). The final core Clippy changes
only document identifiers and add a semicolon to a unit-valued branch; behavior
is unchanged from the full suite.

New deterministic tests cover exact state/source/channel/subscription mappings,
effect-free metadata reads, idempotent retry, missing or changed manifests,
unknown state slots, unresolved new sources, incompatible subscription contracts,
payload/process/assignment fencing, a leader append race, paused-time deadline,
lost response, cancellation after create, root immutability, live cut retention,
abort/pruning, damaged root/anchor, retired old metadata, format rejection and
size/overflow budgets. The DB test uses actual configured process authority in
separate storage and connector lifecycle spies. Existing recovery tests continue
to exercise the same moved progress checks.

## Real-process validation

The existing optimized three-process cut/abort/restart scenario is extended to
stage and retry a root at the held cut, compare the binding on every process and
retain it through full restart/recovery. Its independent Kafka output oracles
continue to test the unchanged stateful parent graph. Target activation and
post-target-commit recovery remain outside this scenario.

Build and copy the optimized test server before building the harness, whose
binary dependency may replace the normal executable:

```powershell
New-Item -ItemType Directory -Path target/topology-evidence -Force | Out-Null
cargo rustc --profile soak -p laminar-server --no-default-features --features cluster,aws,kafka --bin laminardb -- -C link-arg=/STACK:16777216
Copy-Item -LiteralPath target/soak/laminardb.exe -Destination target/topology-evidence/laminardb-root-test-stack.exe
cargo rustc --profile soak -p laminar-server --no-default-features --features cluster,aws,kafka --test cluster_soak
```

The repository's soak profile retains debug assertions and overflow checks.
The copied Windows test server uses a 16 MiB main stack; worker threads use the
existing 4 MiB CI test setting. Production stack settings remain unchanged.
The run uses the isolated `ldb-topology-9929` MinIO/Redpanda Compose project at
ports 19000/19092, bucket `topology-tests-9929`, 12 Kafka partitions, one leader
kill and a final five-second steady interval. Checkpoint and hot-cycle SLO modes
are observational. The wrapper samples only this test's server working sets at
a nominal one-second interval. No compiler runs during the timed scenario.

The exact environment and invocation are in [run-soak.ps1](run-soak.ps1), with
isolated container names in the [fixture override](fixture-compose.yml):

```powershell
Copy-Item docs/test-evidence/topology-root-2026-10-01/fixture-compose.yml target/topology-evidence/compose.yml
Copy-Item docs/test-evidence/topology-root-2026-10-01/run-soak.ps1 target/topology-evidence/run-root-soak.ps1
docker compose -p ldb-topology-9929 -f tests/docker/compose.yml -f target/topology-evidence/compose.yml up -d --wait minio redpanda
docker exec -e MC_HOST_topology=http://laminar:laminar-test-secret@127.0.0.1:9000 laminardb-topology-9929-minio mc mb --ignore-existing topology/topology-tests-9929
pwsh -NoProfile -File target/topology-evidence/run-root-soak.ps1
```

The wrapper's harness filename must match the current build. Credentials above
are the repository's public local fixture defaults. Remove only this isolated
Compose project after the run, preserving volumes:

```powershell
docker compose -p ldb-topology-9929 -f tests/docker/compose.yml -f target/topology-evidence/compose.yml down
```

### Result

The scenario passes in **345.46 seconds**, with one passed test and no failures.
See the [test result](root-soak-01.stdout.txt) and
[scenario observations](root-soak-01.stderr.txt). Both optimized builds pass;
[build results](root-soak-build-results.json) record their exact commands and exits.
All seven test server processes exit. Only the isolated Compose containers/network
are removed, preserving volumes and workspace evidence.

The [build identity](root-server-build-identity.json) records the modified starting
checkout, all 20 changed Rust source hashes, the harness and copied server:
SHA-256 `7cb9891a2b306e2d068b0a89af6acf3c0556891acbe49324db4b096fb7bbfaed`,
170,100,736 bytes. Server, harness and source hashes are verified before and after
the run. Subsequent changes only finalize documentation/evidence.

| Observation | Result |
| --- | --- |
| Node 0 / 1 / 2 local validation | 999.696 / 610.432 / 503.249 ms |
| Node 0 / 1 / 2 independent compilation and durable certification | 696.391 / 487.833 / 476.852 ms |
| Complete frozen roster certification | 1.721 s, including status checks; all three exact process certificates |
| Candidate | 48 objects: 47 preserved, 24 managed-state contracts and one future-only stateless stream |
| Exact old cut | Checkpoint/epoch 60 reaches CutPrepared in 4.461 s with every exact process application receipt |
| Core root staging, including live proof lookup | 508.011 ms from the optimized harness |
| Metadata bound observed | 606,855 bytes across all three participant manifests; 50,338 canonical root bytes |
| Preserved subscription sequence vectors | Nine; stream generations and all certificate fields except target pipeline identity preserved |
| Root binding | One immutable authority append, sequence 437; identical retry returns the same status |
| Full restart | Unchanged topology 1 activates in 59.970 s; abort retains root/certificates and successful parent cut |
| Logged intake hold through deliberate restart/recovery | 64.512..64.687 s across the three nodes |
| Frozen input prefix | 122,826 logical IDs, durable through checkpoint/epoch 89 |
| Bounded / temporal output oracles | All 437,447 / 122,826 expected pairs observed; 6,355 / 1,178 permitted ALO duplicates |
| Other stateful oracles | All 30 matrix, two nullable temporal, three inner temporal and four window rows observed |
| Sampled combined server working set | Peak 826,122,240 bytes across this test's server processes |

The [staged root](topology-migration-root.json) includes exact cut and staging
timings, aggregate participant metadata bytes, preserved object identities,
required subscription certificates/frontiers and returned status. Its canonical
reference is independently re-encoded and checked: SHA-256
`1b7e162e94f225017df72848d4e7fe1e7bd50be500abc77e86e888af37cf0f81`.
The [prepared status](topology-cut-prepared.json) retains cut binding sequence 425,
old checkpoint Commit 428 and every frozen receipt. The
[recovered abort](topology-cut-aborted.json), sequence 438, records `leader_changed`
and retains the same root binding. The
[three matching reports](topology-local-validations.json) and
[preparation responses](topology-participant-preparations.json) preserve the
independently compiled descriptor and certificates; the roster completes at 424.

The [gate observations](gate-pause-observations.json) retain exact logged close and
authorized recovery Release timestamps. This interval includes deliberate shutdown,
full restart and old-graph recovery. It does not measure target activation or a
consumer-visible migration latency distribution.

The active-window producer accepts 400.0 logical pairs/s; bounded and temporal
durable output account for 383.4 and 381.8 pair equivalents/s. Hot-cycle p50 is
<= 0.5 ms, p95 <= 1 ms and p99 <= 5 ms on all nodes. Exact checkpoint timing
covers all seven process generations: 219 records, no missing durable handoff,
deadline exhaustion or recorded SLO violation; maximum pipeline stall is
647.634 ms. Checkpoint duration averages 1,772 ms over 216 observations. SLO
modes are observational and the retained-state floor is disabled. The seven
`checkpoint-timing-node*-generation*.jsonl` files retain exact timing records;
only their ignored final `.log` suffix is removed.

The [resource record](root-soak-01-resources.json) contains 334 nominal one-second
samples across the run. Root staging executes in the optimized harness against
the live authority; server memory sampling excludes that harness. These samples
do not measure staging-specific allocations, queue depth, retained artifact growth
or pause-inclusive consumer latency. There is no matched baseline for a resource/
throughput comparison. The [summarized observations](root-observations.json) are
derived from the retained exact artifacts. This increment adds no per-row or
per-batch migration work; shared checkpoint validation runs on the control path.

## Remaining certification

Missing work includes concrete new-source initialization, root-authorized target
restore/subscription replay, observed actor retirement, atomic target Commit and
participant-complete target Release. Public SQL/submission and detached migration
ownership, removal/replacement, exactly-once migration and matched performance/
allocation/consumer-latency evidence remain unfinished.

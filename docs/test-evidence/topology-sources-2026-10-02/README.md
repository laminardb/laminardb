# Sealed source initialization validation, 2026-10-02

This continuation starts clean at `cad04b580d2890078c5eb0fef4d8b66a61b06717`
on `feature/cluster-topology-migrations`. The original baseline is
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Windows MSVC and Rust/Cargo 1.98
are unchanged. The only Cargo change is a test-only link from the server to the
existing workspace connector crate, with default features disabled. No package
version or external dependency changes. A locked dependency missing from the
local cache, `jiff 0.2.37`, was fetched before verification.

Root format 2 carries a complete unowned initial cursor for every certified new
source. Kafka uses its existing version-2 cursor with numeric next-to-read
baselines, including zero for empty partitions. Format-1 roots keep their exact
canonical bytes. One shared authority append, format 18, binds a new-source root
while leaving the operation CutPrepared and the parent catalog unchanged.

The first successfully created bounded staging slot seals the vector before root
publication. Cancellation after sealing, a lost response or concurrent different
metadata reads cannot move that boundary. Reads cancelled before sealing grant
no boundary. Replacement leaders abort the pre-commit operation and preserve its
successful parent cut. Published root bodies do not depend on the staging slot.

The DB uses configured factories, an isolated replay of the immutable target,
durable stream-generation reconciliation and its existing compiler slot. The
Kafka hook only reads metadata/watermarks. It does not start a reader, subscribe,
assign, join a group, acknowledge input or create a topic. Existing tracked native
work retains one process-wide metadata permit through consumer destruction;
cancelled retries cannot accumulate clients. The lookup budget is 10 seconds,
with at most 64 explicit topics and 4,096 partitions. Roots/slots are at most
1 MiB, participant metadata at most 16 MiB, and authority staging remains bounded
by 15 seconds/16 CAS attempts. The DB deadline is 30 seconds.

These are internal initialization and restore requirements. Target consumption,
state restore, actor retirement, logical topology Commit and target Release
remain unfinished. Ordinary guaranteed Kafka startup still rejects unsealed
latest. Public cluster SQL remains fenced by LDB-6043. This increment does not
satisfy the original runtime migration definition of done. See the
[progress checkpoint](../../cluster-topology-migrations-progress.md),
[operator guide](../../cluster-topology-operations.md) and
[engineering guide](../../cluster-topology-engineering.md).

## Unit and compatibility checks

All commands use `CARGO_BUILD_JOBS=1` and `RUST_MIN_STACK=4194304`.

```powershell
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins -- --quiet
cargo clippy --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
cargo check --locked -p laminar-server --no-default-features
cargo check --locked -p laminar-db --no-default-features --features cluster,ffi
cargo test --locked -p laminar-core --no-default-features --lib -- --quiet
cargo fmt --all -- --check
git diff --check
```

The full selected-feature suite passes **4,295 tests**: 1,039 core, 912 connectors,
1,988 DB and 356 server. The broker test and one existing model-download DB test
are ignored in that run. All-target Clippy, compatibility checks, all **414
non-cluster core tests**, formatting and diff checks pass. See
[unit results](unit-results.txt) and [build checks](build-checks.txt), which omit
the existing MSVC OpenSSL PDB warning prelude. Full default-feature connector
suites, model-download and other external connector environments are not run.

New tests cover complete cursor/descriptor binding, one vector and authority
append across retries/concurrency, cancellation after source seal, lost seal
response, replacement-leader fencing, malformed/oversized slots and cursors,
unsupported modes before native I/O, empty partitions, bounded permit waiting,
the authority-18 gate and exact preservation of the previous real format-1 root
hash/bytes. The DB test uses configured factory/lifecycle spies, separately
configured process storage and a durable preserved stream incarnation of 7.

## Broker check

Using the isolated repository Redpanda fixture at `127.0.0.1:19092`:

```powershell
$env:LAMINAR_KAFKA_TEST_BROKERS = '127.0.0.1:19092'
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_initialization_real_broker -- --ignored --nocapture
```

The ignored broker test passes in **1.42 seconds**. It creates its own topic with
three partitions and fixture input counts `[2, 3, 0]`. Latest discovery returns
exact numeric cursors `[2, 3, 0]` in **308.418 ms**, while earliest returns
`[0, 0, 0]`. Both retain a complete global inventory, no assignment version and
no fabricated consumed offsets. The source stays Created without a consumer or
reader actor. An independent consumer observes no committed group offsets. The
test deletes only its unique topic. See [broker results](kafka-broker-results.txt).
This is one local metadata observation, without a comparative baseline or latency
certification.

## Real-process cut, abort and restart

The existing stateful three-process scenario is extended with an independent
Kafka source, stateless stream and sink, plus the existing downstream projection.
All three running processes validate and independently certify the same candidate
through the public read-only and preparation endpoints. Admission and root staging
use the core library; no public migration submission/staging route exists.
The DB factory path is separately covered by the unit test above. The optimized
harness supplies the built-in Kafka hook against the live prepared authority.

The scenario checks the exact old cut, root retry/all-node status, concrete
`[2, 3, 0]` cursors and retention through full restart in the same namespace.
Its independent output oracles verify the unchanged parent graph. The candidate
is never committed or activated. Existing subscriptions and old state/progress
remain in the exact checkpoint; staging does not read/copy state or Arrow payloads.

The optimized build uses the repository soak profile, including debug assertions
and overflow checks:

```powershell
cargo build --locked --profile soak -p laminar-server --no-default-features --features cluster,aws,kafka --bin laminardb --test cluster_soak
Copy-Item -LiteralPath target/soak/laminardb.exe -Destination target/topology-evidence/laminardb-source-test-stack.exe
& 'C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Tools\MSVC\14.43.34808\bin\Hostx64\x64\editbin.exe' /STACK:16777216 target/topology-evidence/laminardb-source-test-stack.exe
```

On this Windows runner, the MSVC `editbin /STACK:16777216` option sets the
16 MiB main stack on the copied test server only. The PE32+ reserve is read back
and verified; production build/source settings are unchanged. Worker threads use
the existing 4 MiB test setting. The
[MSVC option](https://learn.microsoft.com/en-us/cpp/build/reference/stack?view=msvc-170)
changes the executable's stack reserve without rebuilding its code. Binary and
source hashes are recorded and checked before/after the scenario.

The run uses the isolated `ldb-topology-9929` Compose project and public test
credentials, MinIO at 19000, Redpanda at 19092, bucket `topology-tests-9929`, 12 old
source partitions, one leader kill and a final five-second steady interval.
Checkpoint/hot-cycle SLO modes are observational. The wrapper samples the owned
server processes and harness at nominal one-second intervals. No compiler runs
during the timed scenario. Exact fixture/environment commands are:

```powershell
Copy-Item docs/test-evidence/topology-sources-2026-10-02/fixture-compose.yml target/topology-evidence/compose.yml
Copy-Item docs/test-evidence/topology-sources-2026-10-02/run-soak.ps1 target/topology-evidence/run-source-soak.ps1
docker compose -p ldb-topology-9929 -f tests/docker/compose.yml -f target/topology-evidence/compose.yml up -d --wait minio redpanda
docker exec -e MC_HOST_topology=http://laminar:laminar-test-secret@127.0.0.1:9000 laminardb-topology-9929-minio mc mb --ignore-existing topology/topology-tests-9929
pwsh -NoProfile -File target/topology-evidence/run-source-soak.ps1
docker compose -p ldb-topology-9929 -f tests/docker/compose.yml -f target/topology-evidence/compose.yml down
```

The MSVC tool path is this runner's installation. The wrapper's harness filename
must match the current build. Fixtures are removed only after exporting evidence
and observing all owned servers exit. No volumes are deleted.

The scenario passes in **370.38 seconds**, with one test passed and no failures.
See [test result](source-soak-01.stdout.txt),
[scenario observations](source-soak-01.stderr.txt),
[build result](source-soak-build-results.json) and
[resource samples](source-soak-01-resources.json). All seven owned test servers
exit. Only the isolated Compose containers/network are removed.

The [build identity](source-build-identity.json) records all 22 changed Rust source
hashes, Cargo metadata and both binaries. The copied server is 170,210,816 bytes,
SHA-256 `0d6cd58738c2e226490ea8e0b14a75be16ac35f4e0a437bac70a878beaeb509e`.
Its payload after the PE headers matches the normal optimized server exactly;
the test-only main stack reserve is verified as 16 MiB with a 4 KiB commit.
All source, manifest, server and harness hashes match before/after the run.
Subsequent changes only finalize documentation/evidence.

| Observation | Result |
| --- | --- |
| Node 0 / 1 / 2 local validation | 956.931 / 366.948 / 368.671 ms |
| Complete frozen roster certification | 1.672 s, all three exact process certificates; sequence 389 |
| Candidate | 51 objects: 47 preserved, one downstream projection and one independent source/stream/sink |
| Exact old cut | Checkpoint/epoch 54 reaches CutPrepared in 5.002 s; cut binding 390, old Commit 393, all three application receipts |
| Core root staging, including live proof lookup | 505.860 ms in the optimized harness |
| Metadata read / canonical root | 607,444 participant metadata bytes / 51,909 root bytes |
| New-source initialization | One hook call, three complete unowned channels, exact next offsets `[2, 3, 0]` |
| Preserved requirements | All 47 object mappings and nine complete subscription sequence vectors |
| Root binding | One shared authority append, sequence 402; identical retry and all-node status agree |
| Full restart | Unchanged topology 1 activates in 59.687 s; leader-change abort at 403 retains the root, certificates and parent cut |
| Logged intake hold through deliberate restart/recovery | 64.947..65.055 s across the three processes |
| Frozen old input | 132,278 logical IDs, durable through checkpoint/epoch 85 |
| Bounded / temporal output oracles | All 472,242 / 132,278 expected pairs observed; 3,987 / 1,081 allowed ALO duplicates |
| Other stateful oracles | All 30 matrix, two nullable temporal, three inner temporal and four window rows observed |
| Sampled combined server working set | Peak 896,397,312 bytes |
| Harness observed peak working set | 147,226,624 bytes across the whole scenario |

The [staged root](topology-migration-root.json) includes its real cursor, exact
cut, timings and returned status. Its canonical length/hash are independently
re-encoded and checked: SHA-256
`d971796ed884e784359ead9cf2636837a34445d7f21989a436ec99d78985db5d`.
The [prepared status](topology-cut-prepared.json),
[recovered abort](topology-cut-aborted.json),
[matching validations](topology-local-validations.json) and
[preparation responses](topology-participant-preparations.json) preserve the
exact authority evidence. [Derived observations](source-observations.json) and
[gate timestamps](gate-pause-observations.json) come from those records and the
retained node logs.

All seven `checkpoint-timing-node*-generation*.jsonl` files retain 206 exact
records, no missing durable handoff or deadline exhaustion, and a maximum recorded
pipeline stall of 608.477 ms. Active-window production is 399.9 logical pairs/s;
bounded/temporal durable output accounts for 397.3/397.4 pair equivalents/s.
Hot-cycle p50 is <=0.5 ms and p99 <=5 ms; p95 is <=1 ms on nodes 0/1 and <=5 ms
on node 2. SLO modes are observational and the retained-state floor is disabled.

The 352 nominal one-second resource samples cover the whole run. Harness memory
includes its producers, output oracles and staging; it is not a staging-specific
allocation measurement. The gate interval includes deliberate shutdown/restart
and old-graph recovery. It does not measure target activation or pause-inclusive
consumer latency. No matched baseline, queue-depth or retained-artifact-growth
comparison was run, and these observations do not certify production throughput
or latency. This increment adds no per-record/per-batch migration work.

## Remaining certification

Root-authorized target restore, observed actor retirement, atomic target Commit,
participant-complete target Release and recovery before the first target
checkpoint are still missing. Public SQL/submission and detached ownership,
removal/replacement, exactly-once migration and matched steady-state, allocation,
artifact-growth and consumer-visible pause/latency evidence remain unfinished.

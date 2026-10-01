# Local candidate planning validation, 2026-10-01

This continuation starts clean at `d58d797514f1126926c69127010e1a634d65de30`
on `feature/cluster-topology-migrations`. The original baseline remains
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`; toolchain and locked dependencies
are unchanged. See the [implementation checkpoint](../../cluster-topology-migrations-progress.md)
and [operator API guidance](../../cluster-topology-operations.md).

The new public API compiles an additive candidate against an explicitly adopted
parent, using a private catalog and empty managed graph. Descriptor format 1 binds
catalog incarnations, definitions, schemas, ABI/state contracts, connector versions
and dependencies. It performs no durable admission, source-position discovery,
target commit, retained-state restore, actor retirement or target release. Normal
cluster SQL still returns LDB-6043. Matching local plans are not participant certificates.

## Deterministic and build validation

With `RUST_MIN_STACK=4194304`:

```powershell
cargo test -p laminar-core -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins -- --quiet
cargo clippy -p laminar-core -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
cargo check -p laminar-server --no-default-features
cargo check -p laminar-db --no-default-features --features cluster,ffi
cargo test -p laminar-core --no-default-features --lib -- --quiet
cargo +nightly fmt --all -- --check
git diff --check
```

The full feature-enabled suite passes **1,011 core, 1,984 DB and 356 server tests**,
with one existing DB test ignored. Clippy and both compatibility builds pass;
414 non-cluster core tests pass. [Unit results](unit-results.txt) and
[build checks](build-checks.txt) retain the final output. Existing Windows OpenSSL
missing-PDB linker warnings remain; no production stack setting or dependency changed.

New tests cover independent/downstream candidates, stable preserved
incarnations and dependency closure despite changed graph traversal, connector
lifecycle spies, all-authority-write failure, exact inventory/authority invariance,
strict pipeline identity separation, parent conflicts and damaged artifacts,
unsupported state/name mutations, source/sink placement/delivery/changelog and
filter admission, missing live routing resources, cancellation/concurrency,
paused-time deadlines, request bounds and schema-only queues that reject intake
without a runtime or drain task. The HTTP test uses actual Kafka factories with
an unreachable broker and verifies local scope, authentication, repeatability,
typed errors, body limits and absence of durable admission.

Initial fixture runs lacked the live shuffle resources required for stateful DDL
and used a non-changelog aggregate for a retraction assertion. The corrected tests
exercise the real scope check and explicit EMIT CHANGES. The planner now supplies
only a snapshot of actual live routing availability to its private DDL checks.
Review of the normal catalog constructor also found detached queue drain tasks;
schema-only planning endpoints now avoid those tasks and reject all intake.
Preliminary logs remain under ignored `target/topology-evidence`.

## Real-process scenario

The existing `three_node_alo_topology_cut_abort_restart_soak` requests the same
authenticated dry-run on all three running stateful nodes, compares the entire
descriptor and checks topology 1 remains locally active. It records request
elapsed time and at least four preserved managed-state mappings. Only afterward
does trusted core admission reserve the candidate and the existing manual
checkpoint route establish the old cut. Full restart resumes the unchanged graph
and retains the pre-target abort and cut evidence. Independent Kafka bounded/
temporal join, matrix aggregate and window oracles check continued output.

Build and copy the optimized test server before building the harness, whose
binary dependency may replace the normal executable:

```powershell
New-Item -ItemType Directory -Path target/topology-evidence -Force | Out-Null
cargo rustc --profile soak -p laminar-server --no-default-features --features cluster,aws,kafka --bin laminardb -- -C link-arg=/STACK:16777216
Copy-Item -LiteralPath target/soak/laminardb.exe -Destination target/topology-evidence/laminardb-planning-test-stack.exe
cargo rustc --profile soak -p laminar-server --no-default-features --features cluster,aws,kafka --test cluster_soak
```

The repository's soak profile retains debug assertions and overflow checks. The
copied Windows test server has a 16 MiB main stack; worker threads use the CI
setting above. The run uses the isolated `ldb-topology-9929` MinIO/Redpanda
Compose project at ports 19000/19092, bucket `topology-tests-9929`, 12 Kafka
partitions, one leader-kill round and a final five-second steady interval.
The full checkpoint/adoption/restart/oracle sequence takes minutes. Checkpoint
and hot-cycle SLO modes are `observe`; this is functional evidence, not production
latency certification. No compiler runs during timing. A local PowerShell wrapper
samples only this test server's process working sets once per second.

The exact environment and harness invocation are retained in [run-soak.ps1](run-soak.ps1).
The [fixture override](fixture-compose.yml) retains the isolated container names.
Copy both files to the ignored paths used by the run before reproducing:

```powershell
Copy-Item docs/test-evidence/topology-planning-2026-10-01/fixture-compose.yml target/topology-evidence/compose.yml
Copy-Item docs/test-evidence/topology-planning-2026-10-01/run-soak.ps1 target/topology-evidence/run-planning-soak.ps1
```

After the builds above, the fixture setup and invocation used were:

```powershell
docker compose -p ldb-topology-9929 -f tests/docker/compose.yml -f target/topology-evidence/compose.yml up -d --wait minio redpanda
docker exec -e MC_HOST_topology=http://laminar:laminar-test-secret@127.0.0.1:9000 laminardb-topology-9929-minio mc mb --ignore-existing topology/topology-tests-9929
pwsh -NoProfile -File target/topology-evidence/run-planning-soak.ps1
```

The checked-in wrapper is a copy of that file; its harness filename must match
the executable produced by the current build. The [build identity](planning-server-build-identity.json)
records the modified starting checkout used to build the tested server:
SHA-256 `b38e28d20e5a768548d1f9ba21e838f6f75d68874051be86c469ffac1bcee4cc`,
169,346,048 bytes. Subsequent changes only finalize documentation and evidence.

### Final result

The test passes in **321.13 seconds**, with no failed tests. See the exact
[test result](planning-soak-01.stdout.txt) and [scenario output](planning-soak-01.stderr.txt).

| Observation | Result |
| --- | --- |
| Node 0 local validation | 1,367.189 ms |
| Node 1 local validation | 470.731 ms |
| Node 2 local validation | 2,322.800 ms |
| Identical candidate inventory on all nodes | 48 objects: 47 preserved, 24 with managed-state contracts; one future-only stateless stream |
| Intake during validation | Remains active; topology 1 remains locally active |
| Old cut | Checkpoint/epoch 71 reaches CutPrepared in 2.731 s with all three exact process receipts |
| Full restart | Unchanged topology 1 activates in 37.104 s; the candidate remains uncommitted |
| Frozen input prefix | 111,741 logical IDs, durable through checkpoint/epoch 113 |
| Bounded/temporal join oracles | All 399,708 / 111,741 expected pairs observed; 2,571 / 471 permitted ALO duplicates |
| Other stateful oracles | All 30 matrix rows, 2 nullable temporal rows, 3 inner temporal rows and 4 window rows observed |
| Sampled combined working set | Peak 709,943,296 bytes across this test's server processes |

The [three local reports](topology-local-validations.json) have compatibility
digest `7b507d4f20dfdb0ba03dd6559c047493abcc83a5c9141d1157ea743fc8b08f4e`.
Their parent pipeline identity matches the exact old cut, while the target has a
different strict pipeline identity. The [prepared cut](topology-cut-prepared.json)
binds authority sequence 488, checkpoint Commit 494 and complete receipts at 498.
The [recovered abort](topology-cut-aborted.json), sequence 502, records
`leader_changed` and retains that successful parent checkpoint.

The active-window producer accepts 400.1 logical pairs/s; bounded and temporal
durable output account for 404.0 and 403.8 pair equivalents/s. All nodes have
observed hot-cycle p50 <= 0.5 ms, p95 <= 1 ms and p99 <= 5 ms. Exact checkpoint
timing logs cover all seven process generations: 271 observations, no missing
handoff, deadline exhaustion or recorded SLO violations. Checkpoint duration
averages 1,571 ms over 267 observations. These are observations from this workload;
SLO modes are `observe` and the retained-state floor is disabled.
The seven `checkpoint-timing-node*-generation*.jsonl` files retain those exact
timing records; only their filename's final `.log` suffix is removed for check-in.

The [resource record](planning-soak-01-resources.json) covers the whole run, with
284 samples at a nominal one-second interval, including faults and restart.
It does not isolate planning allocations or measure consumer-visible latency.
There is no matched baseline for a throughput/resource comparison, and restart
duration is not a target cutover measurement. All test server processes exited;
only the isolated Compose project was removed, preserving its volumes and logs.

## Remaining certification

This does not exercise target installation or post-target-commit recovery. Missing
work includes durable owner-complete plan certificates, source initialization and
state/output/subscription progress mappings, observed actor retirement, atomic
target commit and installed-target Release. SQL/API migration, removal/replacement,
full phase/fault matrix, exactly-once migration and matched resource/throughput/
consumer-latency comparisons remain untested because their protocol paths are
unfinished. The local planning boundary and unchanged-graph restart cannot satisfy
the original runtime migration definition of done.

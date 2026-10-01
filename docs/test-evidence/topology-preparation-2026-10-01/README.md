# Participant certification validation, 2026-10-01

This continuation starts clean at `aa6336a5e1bd18d9adc604f1069accca92d87515`
on `feature/cluster-topology-migrations`. The original baseline remains
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Rust/Cargo 1.98, the locked
dependencies and feature selection are unchanged. See the
[implementation checkpoint](../../cluster-topology-migrations-progress.md) and
[operator guidance](../../cluster-topology-operations.md).

Protocol-2 admission immutably binds the existing format-1 candidate report.
Each participant independently compiles the admitted target through the local
preparation API and appends a certificate under exact boot, process-term,
assignment and leader fences. Only the complete frozen owner/evidence roster
permits a new old-topology checkpoint cut. Authority format 16 retains all prior
baseline, request and cut evidence. The descriptor's public JSON and digest,
strict pipeline identity 7 and ordinary checkpoint restore checks are preserved.

Preparation leaves the parent catalog and intake active, opens no connectors and
starts no candidate actors. It does not install or commit topology 2, resolve
new-source positions, restore retained state under a target mapping, retire old
actors or release target output. Normal cluster SQL remains guarded by LDB-6043.

## Deterministic and build validation

With `CARGO_BUILD_JOBS=1` and `RUST_MIN_STACK=4194304`:

```powershell
cargo test -p laminar-core -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_ -- --quiet
cargo test -p laminar-core -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins -- --quiet
cargo clippy -p laminar-core -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
cargo check -p laminar-server --no-default-features
cargo check -p laminar-db --no-default-features --features cluster,ffi
cargo test -p laminar-core --no-default-features --lib -- --quiet
cargo fmt --all -- --check
git diff --check
```

The targeted run passes **50 core, 42 DB and 3 server tests**. The full cluster
suite passes **1,020 core, 1,986 DB and 356 server tests**, with one existing DB
test ignored. [Unit results](unit-results.txt) retain the full-suite test output;
the compiler/linker warning prelude remains in the raw ignored log. Final blank
lines in text evidence copies are normalized for Git whitespace checks.
All-target Clippy, the non-cluster server and cluster FFI checks, formatting and
diff checks pass. All **414 non-cluster core tests** pass. Exact commands and
final output are in [build checks](build-checks.txt). The final Clippy corrections
change only imports and test expressions; no behavior changed after the full suite.

New deterministic coverage exercises exact complete rosters, idempotency,
divergent compilation, protocol mismatch, stale boot/term and assignment evidence,
leader races, successful writes with lost responses or cancellation, paused-time
deadlines, certificate anchor retention and damaged/missing/oversized bindings.
A golden test decodes the preceding checked-in report and reproduces its existing
digest. DB tests use separate configured process and checkpoint stores, independently
compile the authoritative target, and verify connector lifecycle counters, durable
receipts, unchanged intake/runtime and busy/shutdown/divergence behavior. Router
tests cover authentication, malformed identities and non-running conflicts.
Checkpoint boundary tests verify incomplete preparation defers before reservation
and preserves existing Prepare-time cleanup races.

Preliminary compilation exposed fixture/style issues (owned managed-codec string
comparison, unnecessary vector and exhaustive test match), corrected before final
checks. An early concurrent local build was cancelled and its identified child
compilers cleaned up; subsequent builds use one job. No unrelated process was
stopped. Existing Windows OpenSSL missing-PDB linker warnings remain. Preliminary
logs are retained under ignored `target/topology-evidence`.

## Real-process scenario

The existing `three_node_alo_topology_cut_abort_restart_soak` dry-runs the same
candidate on three running stateful nodes, compares full reports and internally
admits the descriptor-bound plan. It then calls the real console-authenticated
local preparation API on every frozen process, checks each durable certificate
and complete sequence, and verifies topology 1 remains locally active. The
existing manual checkpoint API establishes the old cut only afterward. Full
restart resumes the unchanged graph and retains the candidate abort, certificates
and successful parent cut. Independent bounded/temporal join, matrix aggregate
and window Kafka oracles check continued stateful output.

Build and copy the optimized test server before building the harness, whose
binary dependency may replace the normal executable:

```powershell
New-Item -ItemType Directory -Path target/topology-evidence -Force | Out-Null
cargo rustc --profile soak -p laminar-server --no-default-features --features cluster,aws,kafka --bin laminardb -- -C link-arg=/STACK:16777216
Copy-Item -LiteralPath target/soak/laminardb.exe -Destination target/topology-evidence/laminardb-preparation-test-stack.exe
cargo rustc --profile soak -p laminar-server --no-default-features --features cluster,aws,kafka --test cluster_soak
```

The repository's soak profile retains debug assertions and overflow checks.
Only the copied Windows test server uses a 16 MiB main stack; worker threads use
the existing 4 MiB CI test setting. No production stack setting changed. The run
uses the isolated `ldb-topology-9929` MinIO/Redpanda Compose project at ports
19000/19092, bucket `topology-tests-9929`, 12 Kafka partitions, one leader-kill
round and a final five-second steady interval. The complete sequence takes
minutes. Checkpoint and hot-cycle SLO modes are observational. The local wrapper
samples only this test's server working sets at a nominal one-second interval.

The exact environment and invocation are in [run-soak.ps1](run-soak.ps1); the
[fixture override](fixture-compose.yml) preserves isolated container names. Copy
both files to the ignored paths used by the run before reproducing:

```powershell
Copy-Item docs/test-evidence/topology-preparation-2026-10-01/fixture-compose.yml target/topology-evidence/compose.yml
Copy-Item docs/test-evidence/topology-preparation-2026-10-01/run-soak.ps1 target/topology-evidence/run-preparation-soak.ps1
docker compose -p ldb-topology-9929 -f tests/docker/compose.yml -f target/topology-evidence/compose.yml up -d --wait minio redpanda
docker exec -e MC_HOST_topology=http://laminar:laminar-test-secret@127.0.0.1:9000 laminardb-topology-9929-minio mc mb --ignore-existing topology/topology-tests-9929
pwsh -NoProfile -File target/topology-evidence/run-preparation-soak.ps1
```

The wrapper's harness filename must match the executable produced by the current
build. Fixture credentials above are the repository's public local test defaults.
Remove only the isolated Compose project after the run, preserving volumes:

```powershell
docker compose -p ldb-topology-9929 -f tests/docker/compose.yml -f target/topology-evidence/compose.yml down
```

### Final result

The test passes in **294.19 seconds**, with one passed test and no failures.
See the exact [test result](preparation-soak-01.stdout.txt) and
[scenario output](preparation-soak-01.stderr.txt). Both optimized builds pass;
their commands and exits are retained in [build results](preparation-soak-build-results.json).
No compiler runs during timing. All seven test server processes exit, and only
the isolated Compose containers/network are removed, preserving volumes and logs.

The [build identity](preparation-server-build-identity.json) records the modified
starting checkout, changed Rust file hashes, test harness hash and copied server:
SHA-256 `62fdc6b6c5cfab392456940c2df90bfd0e0caccae9717a8698398c144fd8ae36`,
169,765,888 bytes. Subsequent changes only finalize documentation and evidence.

| Observation | Result |
| --- | --- |
| Node 0 / 1 / 2 local dry-run | 590.181 / 422.977 / 471.389 ms |
| Node 0 / 1 / 2 independent compilation and durable preparation | 626.095 / 482.627 / 486.585 ms |
| Complete frozen roster preparation | 1.657 s, including status checks; all three exact process certificates |
| Identical candidate report | 48 objects: 47 preserved, 24 managed-state contracts and one new future-only stateless stream; 22,717 canonical bytes |
| Intake during preparation | Remains active; committed and locally active topology versions remain 1 |
| Old checkpoint cut | Checkpoint/epoch 81 reaches CutPrepared in 10.123 s with every exact process application receipt |
| Full restart | Unchanged topology 1 activates in 38.469 s; the candidate remains uncommitted |
| Cut gate hold through deliberate full restart/recovery | 48.695..48.735 s across the three node logs |
| Frozen input prefix | 101,989 logical IDs, durable through checkpoint/epoch 119 |
| Bounded/temporal join oracles | All 364,215 / 101,989 expected pairs observed; 1,788 / 167 permitted ALO duplicates |
| Other stateful oracles | All 30 matrix rows, 2 nullable temporal rows, 3 inner temporal rows and 4 window rows observed |
| Sampled combined working set | Peak 727,097,344 bytes across this test's server processes |

The [three local reports](topology-local-validations.json) have compatibility
digest `6151a81daf94cee855993de3157c6bc61440ecd05f789c70e05d52f217acafb5`.
The [local preparation responses](topology-participant-preparations.json) retain
each independently compiled certificate and latency. Receipts first appear at
authority sequences 536, 537 and 539; only sequence 539 marks the roster complete.
The [prepared cut](topology-cut-prepared.json) binds the exact parent identity at
sequence 540, old checkpoint Commit 544 and complete application receipts 549.
The [recovered abort](topology-cut-aborted.json), sequence 552, records
`leader_changed` and retains the complete preparation and successful parent cut.

The [gate observations](gate-pause-observations.json) retain the exact logged cut
fence and recovery Release timestamps. Followers log gate opening after consuming
Release; the leader logs completed recovery after its gate release. This interval
includes deliberate shutdown, full restart and old-graph recovery. It is not a
target activation duration or consumer-visible migration latency.

The active-window producer accepts 400.0 logical pairs/s; bounded and temporal
durable output account for 393.1 and 392.5 pair equivalents/s. Hot-cycle p50 is
<= 0.5 ms on all nodes; p95 is <= 1 ms on nodes 0/1 and <= 5 ms on node 2;
p99 is <= 5 ms on all nodes. These histogram observations do not isolate
preparation latency and do not include the processing pause in a consumer latency
distribution. Exact checkpoint timing covers all seven process generations:
285 records, no missing durable handoff, deadline exhaustion or recorded SLO
violations; maximum logged pipeline stall is 754.934 ms. Checkpoint duration
averages 1,145 ms over 281 observations. SLO modes are observational and the
retained-state floor is disabled. The seven `checkpoint-timing-node*-generation*.jsonl`
files preserve the exact records; only the final ignored `.log` suffix is removed.

The [resource record](preparation-soak-01-resources.json) covers the complete run
with 254 samples at a nominal one-second interval. It does not measure
preparation-specific allocations, queue depth, retained artifact growth or
consumer-visible latency. There is no matched baseline for a throughput/resource
comparison. Existing queue baseline/comparison evidence remains in the
[cut validation](../topology-cut-2026-10-01/README.md); this increment adds no
per-record or per-batch migration work.

## Remaining certification

This scenario does not exercise target installation or post-target-commit
recovery. Missing work includes concrete source initialization, authorized state/
output/subscription progress mappings, observed actor retirement, atomic target
commit and installed-target Release. Public SQL/submission, automatic certificate
collection/detached ownership, removal/replacement, the full phase/fault matrix,
exactly-once migration and matched resource/throughput/consumer-latency comparisons
remain unfinished. Old-graph certification/cut/restart does not satisfy the
original runtime migration definition of done.

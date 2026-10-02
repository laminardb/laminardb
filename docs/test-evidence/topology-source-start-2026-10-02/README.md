# Atomic startup from sealed topology source cursors, 2026-10-02

This continuation starts clean at `f20cf05ecae70c6a53a7dce8a3ffbe1a52867830`
on `feature/cluster-topology-migrations`. The original baseline is
`5d81ba9b18d80343373ecfaec4793df8c5caccf1`. Windows MSVC, Rust/Cargo 1.98 and
the workspace Rust 1.95 minimum remain unchanged. Cargo.lock and dependency
metadata are unchanged. Locked versions include DataFusion 53.1.0, Arrow 58.4.0,
Tokio 1.53.1, rdkafka 0.39.0 and async-trait 0.1.92. The supplemental build uses
the existing deltalake 0.32.4, iceberg 0.10.1, mongodb 3.9.1 and async-nats 0.47.0.

`SourcePosition::Initialized` distinguishes the root's sealed new-source cursor
from a durable engine Resume. It carries no checkpoint attempt, processed history
or acknowledgement. The prepared image converts preserved sources to their exact
Resume attempt/cursor and new sources to Initialized without a fabricated attempt.
This conversion is a control-path operation and grants no authority or Release.

The source-start request rejects BestEffort initialized delivery and already-owned
cursors. The runtime rejects custom connectors without an explicit sealed-start
capability before startup. Kafka implements the capability using existing bounded
read-only metadata validation and atomic manual assignment; other built-ins reject.
It validates complete canonical channels, numeric next-to-read offsets, current
inventory and retention before creating an active consumer. Current vnode owners
filter the global vector. Reader startup remains deferred until polling; retries
reuse the first vector after high watermarks advance. Ordinary guaranteed Initial
with latest still rejects. BestEffort group/manual assignment policy is retained.
Saved positions also disable automatic topic creation.

The metadata path retains its 10-second total budget, 64-topic/4096-partition bounds,
existing tracked native work and semaphore through client destruction/cancellation.
Runtime startup reuses the shared stage deadline, process lease checks, cleanup and
terminal task ownership. The coordinator seeds committed progress only from
durable Resume, never Initialized. No skipped prefix is treated as acknowledged.
There is no new framework, registry, authority format, dependency or per-record
work.

Focused cases cover:

- Exact committed-root conversion retaining preserved attempt/cursor/ownership and
  the distinct unowned new-source boundary, with no append, actors or effects.
- An actual owned source actor servicing controls with intake held, no polls or
  acknowledgements, subsequent local test polling and observed terminal cleanup.
- Shared startup deadline cleanup, assigned-cursor rejection and rejection of a
  custom source that could otherwise ignore the initialized position.
- Native librdkafka MockCluster startup from sealed latest `[2,3,0]`, including an
  empty partition, two fresh source retries, exact post-boundary IDs `[100,101,102]`,
  no subscription or broker acknowledgements, and a later saved-cursor Resume.
- Two vnode owners selecting disjoint subsets of one global vector; changed
  inventory/future offsets, processed/wrong-source cursor rejection; shared
  validation deadline and cancellation retaining a reusable exact request.

The separate ignored real-broker case creates one unique three-partition topic on
Redpanda v26.1.13 at `127.0.0.1:19092`, runs the same numeric startup/retry oracle,
observes absent group commits with an independent consumer and deletes that topic.
The fixture supplies the later Resume attempt; it is a cursor codec check, not a
durable engine checkpoint, full DB restart or multi-process migration certification.
The held actor test's gate opening is test control, not topology Release authority.

Commands use the established sequential four-package feature union:

```powershell
$env:CARGO_BUILD_JOBS = '1'
$env:RUST_MIN_STACK = '4194304'
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_start_ -- --nocapture
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins -- --quiet
cargo clippy --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --all-targets -- -D warnings
cargo check --locked -p laminar-server --no-default-features
cargo check --locked -p laminar-db --no-default-features --features cluster,ffi
cargo check --locked -p laminar-connectors --no-default-features --features files,postgres-cdc,mongodb-cdc,nats,otel,websocket,delta-lake,iceberg
cargo fmt --all -- --check
git -c core.excludesFile= diff --check
git -c core.excludesFile= diff --cached --check
```

The isolated broker uses the repository fixture and the retained container-name
override. Only Redpanda is started; teardown does not request volume deletion.

```powershell
docker compose -p ldb-topology-9929 -f tests/docker/compose.yml -f docs/test-evidence/topology-source-start-2026-10-02/fixture-compose.yml up -d --wait redpanda
$env:LAMINAR_KAFKA_TEST_BROKERS = '127.0.0.1:19092'
cargo test --locked -p laminar-core -p laminar-connectors -p laminar-db -p laminar-server --no-default-features --features cluster,aws,kafka --lib --bins topology_start_kafka_real_broker_sealed_latest_retries_without_history_or_ack -- --ignored --nocapture
docker compose -p ldb-topology-9929 -f tests/docker/compose.yml -f docs/test-evidence/topology-source-start-2026-10-02/fixture-compose.yml down
```

All 12 focused cases pass: seven connector cases (3.33 s) and five DB cases
(0.94 s). The full selected-feature suite passes 4,385 tests: 1,078 core
(30.56 s), 921 connectors (45.13 s), 2,030 DB (11.92 s) and 356 server (6.05 s).
Three cases are ignored in that suite: the same two existing ignored cases plus
the new external-broker case. The explicitly run real Redpanda case passes
(2.85 s). These are validation runtimes, not migration-pause measurements.

All-target Clippy with warnings denied passes (1 min 06 s), as do the minimal
server build (12.64 s), cluster/FFI build (18.98 s), supplemental optional-connector
build (4 min 11 s), formatting and working/staged diff checks. Optional connector
runtime suites are not run. All 27 changed Rust source hashes and Cargo.lock remain
unchanged through final validation and staging. The isolated broker is healthy
before the test and its container/network are removed afterward without requesting
volume deletion.

[Focused results](focused-tests.txt), [full suite results](unit-results.txt),
[broker results and fixture identity](broker-results.txt),
[build checks](build-checks.txt) and [source identity](source-identity.json)
retain the final evidence.

Raw logs remain under ignored `target/topology-evidence`. Checked-in result files
omit the large native OpenSSL missing-PDB prelude. Source identity refers to tested
working-tree bytes; Git normalizes line endings. No production binary identity or
performance result is inferred. Earlier lint and focused fixture failures remain
in the raw logs: a test-only futures import was replaced with the standard library,
and three actor fixtures now explicitly use at-least-once/manual checkpoints instead
of default BestEffort. No delivery guard was weakened. The optional build's initial
cache-unpack permission failure was resolved using the same locked dependencies.

Runtime catalog/coordinator installation, source/sink actor integration, stale sink
completion fencing, current participant readiness and participant-complete Release
remain unfinished. Automatic post-Commit runtime recovery, public submission and
the real stateful migration/restart/failure/performance oracle remain unfinished.
The operation stays Committed and the migration's intake/cut holds remain required.
LDB-6043 remains; runtime DDL is not routed through bootstrap. No push or pull
request is made.

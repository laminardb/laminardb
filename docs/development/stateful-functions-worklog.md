# Stateful Process Functions worklog

**Status/date:** Active implementation, 2026-10-06. This is a resumable engineering log, not a support claim.

## Baseline

- Checkout: `codex/stateful-process-functions`, branched from freshly fetched `origin/main` at `2642c1dbfd8131430433d5c55be2f6609e6d79ce` (also the starting `HEAD`). Untracked `.zcode/` predates this work and is untouched.
- Toolchain: `rustc 1.98.0 (88d9e12ae 2026-08-18)`, `cargo 1.98.0`; manifest MSRV 1.95. Lockfile pins Arrow 58.4.0, DataFusion 53.1.0, object_store 0.13.2, and partition-key `xxhash-rust` 0.8.18. `rust-toolchain.toml` selects stable.
- `cargo test --workspace --lib` before changes: failed on four time-sensitive `laminar-core` tests (`cluster::control::process_lease::tests::delayed_first_poll_cannot_renew_after_initial_deadline` and three `durable_local_store::tests::{cancelled_delete_holds_ownership_until_the_filesystem_job_finishes,cancelled_overwrite_drains_before_reopen_rmw,immutable_create_does_not_wait_for_mutable_operation_order}`). `laminar-connectors`: 1977 passed; `laminar-core`: 969 passed, 4 failed. Isolated `cargo test -p laminar-core --lib -- --test-threads=1 --quiet` passed 414 tests; `cargo test -p laminar-core --features cluster --lib -- --test-threads=1 --quiet` passed 961. The initial parallel failure is a baseline concurrency-sensitive issue, not a feature regression.

## Verified integration points

- `OperatorGraph::add_query` wires a `GraphOperator`; `GraphOperator` owns `process_with_frontiers`, managed-state accounting, vnode checkpoint capture/restore, and frontier control. The graph's post-process budget check is mandatory for declared managed-state operators.
- `LaminarDB::build_connector_operator_graph` constructs the graph from connector registrations before recovery. `pipeline_lifecycle::runtime_launch` resolves stream schemas, installs output providers, sets the managed-state budget, initializes participants, then recovers them. Dynamic SQL stream creation uses the coordinator control message and an acknowledged catalog reservation.
- `PartitionKeyCodecV1` in `laminar-core` is the canonical key encoding/routing ABI. Reuse it for process-function keys; null and unsupported keys must fail admission or processing explicitly.
- Cluster admission checks actual SQL/runtime shapes separately from `OperatorCapability`. A new process operator must report cluster rejection until vnode transfer, shuffle ordering, and recovery have failure tests.
- `AiInferenceOperator` and `LookupEnrichOperator` illustrate bounded async worker patterns but their local caches and call logs are not authoritative state.

## Sources checked on 2026-09-23

- [Arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html): structural type details, nullability, decimal and timestamp parameters.
- [Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html) and [PyArrow FlightStreamWriter](https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightStreamWriter.html): transport evaluation inputs; no transport has been selected or implemented.
- [DataFusion SQL extension guide](https://datafusion.apache.org/library-user-guide/extending-sql.html): SQL extension background. The existing LaminarDB graph path is the actual execution boundary.

## Current implementation and evidence

- Trusted native registration is offline through `LaminarDB::register_native_process_function`. It accepts one direct or connector append-only source, one non-null UTF-8 key, UTC microsecond event time, one `ValueState<Int64?>`, named event-time timers, and one append-only output. Local `BestEffort` admits native Rust and loopback Rust/Python; local `AtLeastOnce` admits native and loopback Rust with a replayable connector source and checkpointing. The process operator uses canonical `PartitionKeyCodecV1` keys, concrete per-vnode state, managed accounting, and existing graph checkpoint frames. Complete handler responses are validated before their state, timer, and output changes are accepted. The descriptor and structural schema digest are bound into pipeline identity and restore validation.
- Engine input frontiers use milliseconds; Arrow activation and timer timestamps use microseconds. The operator converts at its boundary, including output-watermark hold. The running-database recovery test exposed and now covers this mismatch.
- `cargo test -p laminar-db --lib process_function::tests -- --nocapture`: 7 passed with default features after the final native edits, including SQL stream composition, real source/subscription execution, database checkpoint/reopen with state and timer continuity, atomic invalid-response rejection, bounded timer callback rescheduling, and restore-budget rejection. This proves local state restoration, not source replay or external sink exactly-once.
- `cargo bench -p laminar-db --bench process_function_bench --no-default-features -- --sample-size 10 --measurement-time 3`: Criterion one-row end-to-end mean 24.575 µs before and 26.007 µs after the graph deferred-work change on this Windows host. Criterion estimated +4.095% (95% interval +0.237% to +7.835%, p=0.06), reporting no detected change. This includes source handoff, coordinator, native handler, and subscription; it is not a state-lookup or compiled-query measurement. A target-hardware and IPC profile remains pending.
- `cargo bench -p laminar-core --bench latency_bench -- --sample-size 10 --measurement-time 3`: the existing tumbling-window assignment reference measured 1.4341 ns mean. It does not measure process-function work.
- `cargo clippy --workspace --all-features --all-targets -- -D warnings`, `cargo clippy --workspace --no-default-features -- -D warnings`, `cargo +nightly fmt --all -- --check`, `cargo run --quiet --manifest-path tools/readability-check/Cargo.toml -- .`, and `git diff --check` passed after the final native edits.
- `cargo test --workspace --lib --quiet` compiled the cluster path, then `laminar-connectors` passed 1977 and `laminar-core` passed 973. Its `laminar-db` binary failed with Windows `STATUS_STACK_OVERFLOW` in a coordinated-recovery test. An isolated test also overflowed at the default test-thread stack and passed with `RUST_MIN_STACK=8388608`. The full workspace library suite then passed with `RUST_MIN_STACK=8388608 cargo test --workspace --lib -- --test-threads=1` (PowerShell environment assignment preceding the command). The default-stack parallel gate remains red; do not hide it behind the adjusted run.

### Continuation: native conformance and local latency (2026-09-23)

Three new conformance tests cover batch splitting and independent-key permutation, absent/null/unchanged/clear across checkpoint restore, and state isolation for two operators with the same function identity. `cargo test -p laminar-db --lib process_function::tests -- --nocapture` and `cargo test -p laminar-db --no-default-features --lib process_function::tests -- --quiet`: 10 passed in each configuration. No production record-path code changed.

After the continuation, both workspace Clippy gates, nightly formatting, and the readability checker passed. The full workspace library suite passed with `RUST_MIN_STACK=8388608` and `--test-threads=1`; the unadjusted Windows stack failure recorded above remains unresolved.

Criterion baseline: Windows x86_64 MSVC, AMD Ryzen 9 7900X (12 cores, 24 logical), Rust 1.98.0, optimized bench profile with thin LTO and `--no-default-features`. The input has a UTF-8 key, signed 64-bit amount, and UTC microsecond timestamp; the handler uses one signed 64-bit state slot and emits one Arrow row per input, with no timers.

| Workload | Mean | 95% interval |
|---|---:|---:|
| One row, source to subscription | 26.398 µs | 26.042–26.781 µs |
| 64 distinct keys, source to subscription | 107.73 µs/batch | 106.29–109.46 µs |
| 64 rows for one key, source to subscription | 108.18 µs/batch | 107.23–109.22 µs |
| Prepared native handler, one row | 423.63 ns | 421.66–425.94 ns |
| Prepared native handler, 64 rows | 28.589 µs/batch | 28.352–28.894 µs |

Command: `cargo bench -p laminar-db --bench process_function_bench --no-default-features -- <filter> --noplot --sample-size 40 --warm-up-time 5 --measurement-time 10` for the end-to-end rows; handler-only rows used 30 samples, 3-second warm-up, and 7-second measurement. The first one-row run immediately after release compilation measured 40.141 µs; later 30-sample end-to-end means ranged 24.717–25.846 µs (one row), 107.73–108.73 µs (distinct keys), and 105.22–112.13 µs (one key) without production code changes. Handler-only input uses prepared activation snapshots and excludes routing, validation, coordinator handoff, and subscription. These are development-machine latency means, not sustained throughput, target-hardware, p99, or sampled CPU/IPC results.

### Continuation: portable native descriptor (2026-09-23)

The validated native descriptor now has a bounded, canonical JSON manifest with structural Arrow fields, explicit v1 runtime/protocol/key ABI/state codec and append-only/late-event policies. `from_manifest_json` rejects unknown fields, unsupported types or policy changes, invalid decimal parameters and input over 64 KiB. The implementation digest is still caller-supplied trusted-build identity; the manifest does not package or attest native code, and its protocol version does not mean a remote worker transport exists.

Pipeline identity and the operator checkpoint now bind the SHA-256 of the complete canonical manifest. This replaces the checkpoint's narrower schema-only digest and the pipeline identity's duplicate field serialization. State codec version 2 deliberately rejects checkpoints produced by the earlier development slice; no migration path is implemented. The record path and native handler ABI did not change, so the earlier Criterion measurements remain the local baseline for this increment.

The focused native suite has 13 tests in both default and no-default-feature builds, including manifest type round trips, malformed/incompatible manifest rejection, and rejection of a changed timer contract on restore. The final no-default-feature focused run passed after the last code edit. The final workspace library suite passed with `RUST_MIN_STACK=8388608 cargo test --workspace --lib -- --test-threads=1`: 1,977 connector, 973 core, 1,984 database, and 870 SQL tests passed; derive had no library tests. The unadjusted default-stack Windows gate remains red for the pre-existing coordinated-recovery overflow recorded above. Both workspace Clippy gates, nightly formatting, the readability checker, and `git diff --check` passed after the final code edit.

### Continuation: bounded Rust remote reference transport (2026-09-24)

The `process-remote` feature now contains one versioned bidirectional gRPC service with Protobuf control frames and self-contained Arrow IPC streams for input and output batches. The manifest binds `trusted_native_rust`, `remote_rust`, or `remote_python` as distinct runtimes; native registration still requires the native runtime. Host activations carry canonical key, independent logical ID, per-activation event time and state existence/value. Invocation scope carries operator/vnode, owner and recovery generations, separate batch/attempt IDs, input watermark, and deadline. Results carry activation IDs, explicit state mutations and timer operations, zero-to-many output sections, and a completion count. The Rust reference worker accepts only a loopback listener and executes handlers in a bounded blocking pool. Client and worker use admission credits; a cancelled blocking handler keeps its worker credit until it finishes.

Selection: [Arrow Flight `DoExchange`](https://arrow.apache.org/docs/format/Flight.html) supports bidirectional data and metadata, but [PyArrow's high-level Flight writer](https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightStreamWriter.html) begins with one schema. The function contract needs independent input/output schemas and typed state/timer sections. Explicit gRPC frames keep those sections and completion validation visible without building a Flight metadata multiplexing layer. This is an architectural inference from the documented APIs, not a Python interoperability result. Python/PyArrow interoperability is the next test.

The prototype caps each IPC frame at 8 MiB and request/response wire payloads at 32 MiB, rejects compressed, dictionary and noncanonical IPC, checks decoded schema/rows/bytes, and retains no authoritative worker state. The client returns a complete staged result for host validation; no result is applied by the transport. `cargo test -p laminar-db --no-default-features --features process-remote --lib process_function:: -- --quiet` passed 19 tests on the final code: live TCP zero/multiple outputs, null/absent state, timer callback, descriptor mismatch, truncated IPC, cancelled-call credit retention, scalar type/decimal/timestamp round trip, and malformed-byte property cases. The remote client and reference worker are not yet wired into `LaminarDB` registration or the operator graph; this is a transport boundary, not remote pipeline support. The current feature is loopback only and does not claim authenticated nonlocal deployment, Python support, restart recovery, or cluster support.

The native record path is unchanged apart from an offline runtime admission check, so the coordinator/core-operator before/after benchmark gate is not triggered by this increment. The remote path still needs its own sampled CPU/IPC and p99 measurements after graph integration. Native development checkpoints retain codec v2 and the same canonical manifest for `trusted_native_rust`.

The full workspace library suite passed with `RUST_MIN_STACK=8388608 cargo test --workspace --lib -- --test-threads=1`: 1,977 connector, 973 core, 1,984 database (1 ignored), and 870 SQL tests. The unadjusted Windows stack failure remains the known baseline caveat. `cargo clippy --workspace --all-features --all-targets -- -D warnings`, `cargo clippy --workspace --no-default-features -- -D warnings`, and feature-specific remote Clippy all passed after the final code edit. Nightly formatting and the readability checker passed after the last code changes.

### Continuation: Python/PyArrow worker interoperability (2026-09-24)

The installable `laminardb-process` package now provides a loopback-only Python 3.13 gRPC worker, PyArrow batch handler contract, explicit absent/null/value state view, proposed mutations and timer operations, a runnable command, pinned runtime dependencies, and a local example manifest/handler. The worker loads the canonical manifest (allowing one file line ending), checks the invocation digest and protocol, requires a bounded gRPC deadline, enforces request and response frame/invocation budgets, limits concurrent handler calls, and retains a credit while a cancelled handler finishes. It constructs the complete response before sending it; the host remains authoritative for validation and application. The Python package does not attest transitive imported code against `implementation_digest`; at this increment no database pipeline admitted the remote runtime.

The wire implementation follows the [gRPC Python generated-code/service API](https://grpc.io/docs/languages/python/generated-code/) and [PyArrow IPC stream API](https://arrow.apache.org/docs/python/api/ipc.html), checked 2026-09-24. The local test environment uses Python 3.13.1, PyArrow 20.0.0, NumPy 2.4.0, grpcio/grpcio-tools 1.84.0, and Protobuf 7.36.2. The checked-in Python messages are generated from the same `.proto` as the Rust stubs. The `requirements.lock` and package metadata pin runtime dependencies; the container recipe was added but could not be built here because the Docker daemon is unavailable.

With `LAMINAR_PROCESS_PYTHON=python` and the local dependency path, `cargo test -p laminar-db --no-default-features --features process-remote --lib process_function::remote::tests::python -- --test-threads=1` passed seven tests: live different-schema exchange, zero/multiple output and timers, scalar/nullable/decimal/UTC timestamp round trips, independent-key batching invariance, absent/null/present state, descriptor mismatch and truncated IPC, cancelled-call credit retention, and canonical example binding. The full focused `process_function::` suite passed 26 tests. `python -m unittest discover -s python/laminardb_process/tests -p 'test_*.py'` passed three local boundary tests. The package built and installed as a wheel in an ignored local target directory. Linux CI now installs the pinned package and runs both Python and Rust cross-language tests under the all-feature suite. The workspace library suite passed with the documented Windows stack adjustment: 1,977 connector, 973 core, 1,984 database (1 ignored), and 870 SQL tests. Both workspace Clippy gates, feature-specific remote Clippy, nightly formatting, and the readability checker passed. No coordinator or core record-path code changed; a target-hardware CPU/IPC and p99 measurement is still required before a latency claim.

### Continuation: local asynchronous remote execution (2026-09-24)

Remote Rust and Python clients now register through the embedded local `BestEffort` API against one direct in-memory source. The graph schedules at most 32 loopback calls per client on the main runtime. It retains one bounded input step, sends key-distinct batches from one vnode, permits independent keys to run concurrently, and keeps each key busy until its complete result is validated and applied. The compute task never waits for gRPC or Arrow IPC. A bounded completion channel wakes the coordinator; task cancellation and worker failure fence the graph for recovery. The graph withholds source progress and checkpoint capture while calls are pending, holds output watermarks behind unresolved activations, then advances the input watermark and dispatches due timers after accepted input. The worker receives the previously accepted watermark, which matches native timer validation. Remote output still uses the native authoritative state/timer staging and vnode checkpoint frames.

`LocalPythonWorker` verifies the canonical manifest and direct handler-file SHA-256, starts a child without a shell, bounds readiness and per-call time, connects on loopback, monitors exit, and exposes explicit asynchronous shutdown. A dropped local owner requests process cancellation; the supervisor reaps the child. It does not automatically restart a crashed process or attest imported modules and data. The copyable `process_python` Cargo example ran on Windows and printed `key=a total=60` and `key=a total=110`, then shut down both database and worker. The local client, Rust reference worker, and Python worker now cap concurrent calls at 32. Server configuration and remote authenticated transport remain unimplemented.

The supervisor now starts in the handler directory and the Python worker checks that the imported module resolves to the same file whose digest the host verified. On Windows, the check uses file identity so Rust's extended-length canonical path and Python's ordinary path name agree. A CLI test rejects a mismatched origin. Imported dependencies remain outside the direct handler digest.

The new graph and running-database tests cover same-key serialization, independent-key batching, source-progress deferral, checkpoint drain, timers, worker loss, an inactive worker runtime, Python output, and direct handler digest rejection. The Linux-only test kills a real Python worker and checks supervisor exit reporting. The real Python database test and local example passed on Windows; the Linux kill test awaits CI. `python -m unittest discover -s python/laminardb_process/tests -p 'test_*.py'` passed three tests after the canonical newline fix. No local replay certification, packaged server route, exactly-once claim, or cluster admission follows from these tests.

Final focused validation: with an absolute `LAMINAR_PROCESS_PYTHON_DEPS` path and `LAMINAR_PROCESS_PYTHON=python`, `cargo test -p laminar-db --no-default-features --features process-remote --lib process_function:: -- --test-threads=1` passed 33 tests, including terminal classification of malformed remote input; the Python unit suite passed four. The earlier focused run with a relative dependency path failed six Python imports because Cargo runs crate tests from the crate directory; rerunning with the absolute path passed. The workspace library gate passed with the documented Windows stack setting and serial tests: 1,977 connector, 973 core, 1,984 database (one ignored), and 870 SQL tests. Both workspace Clippy modes, nightly formatting, and the readability checker passed after the final code edit.

Native Criterion before/after comparison used an isolated worktree at `09807999` and the current checkout with `cargo bench -p laminar-db --bench process_function_bench -- --noplot`. Criterion slopes, in ns per iteration, were 35,156 → 28,984 (one-row end to end), 110,408 → 113,027 (64 distinct keys), 110,098 → 111,586 (64 same-key rows), 468 → 426 (one-row handler), and 30,053 → 29,627 (64-row handler). The largest observed increase was 2.37%; none crossed the repository's 5% hot-path block threshold. These are noisy development-host means, not target-hardware p99, sustained throughput, or sampled CPU/IPC evidence.
`cargo bench -p laminar-core --bench latency_bench -- --noplot` also passed after the change; tumbling assignment measured 1.4101 ns mean versus the 1.4646 ns pre-change reference. No `laminar-core` production code changed.

### Continuation: local remote checkpoint recovery (2026-09-24)

A remote Rust graph now captures a quiescent checkpoint and restores its keyed state and pending event-time timer into a fresh graph. The test observes the timer callback output and then processes another input against the restored state. Two running-database Python tests use fresh database and worker instances against the same local checkpoint directory. One covers orderly restart, state continuity, and timer expiry; the other kills the actual Python child after a committed checkpoint, verifies supervisor exit reporting, then starts a replacement worker and restores the accepted total. The crash test uses `kill -KILL` on Unix or `taskkill /F` on Windows. The Windows sandbox denied `taskkill` in the normal test runner; the same focused test passed with elevated local test permission. No production record-path code changed.

The `process_python` Cargo example now has `checkpoint STORAGE_DIR` and `resume STORAGE_DIR` commands. They run as separate processes, commit and restore local state, and check the expected totals rather than only printing them. Both commands passed on Windows with outputs `60` and `110`. The SDK README has Bash and PowerShell commands. This qualifies restoration of committed state with a new worker; the direct in-memory source cannot replay an input lost before a checkpoint. There is still no packaged server route, automatic worker replacement within a running database, authenticated nonlocal transport, or cluster admission.

Final validation: the 36-test `process_function::` suite passed with the Python worker enabled, including the real child kill/restart case; four Python unit tests passed. `cargo test --workspace --lib -- --test-threads=1` passed with `RUST_MIN_STACK=8388608`, the previously documented Windows adjustment. Both workspace Clippy gates, nightly formatting, the readability checker, and `git diff --check` passed. The unadjusted Windows default-stack failure remains the earlier known baseline. Only test-only worker PID visibility and the example changed outside tests/docs, so no coordinator or core record-path benchmark was triggered; target-hardware p99/CPU/IPC evidence remains outstanding.

### Continuation: single-node server binding (2026-09-25)

The single-node server now accepts startup-only `[[process_function]]` entries. Each entry creates one direct source from a single `CREATE SOURCE` statement, resolves artifact and Python import paths relative to the config file, verifies and starts a loopback Python worker, and registers its immutable descriptor before other configured pipeline DDL. The server owns each child until database shutdown and reports cleanup errors alongside startup failures. Config reload reports process-binding changes as restart-only. The console-policy `GET /api/v1/process-functions` route lists output/source, function and pipeline identities, descriptor version, runtime, and direct implementation digest. The existing SQL API can feed this direct source using signed Unix microsecond literals for `TIMESTAMP` columns; existing WebSocket subscriptions receive output. A sample server TOML and shell/PowerShell instructions are checked in.

Admission remains single-node `best_effort` with at most 32 configured functions and 32 calls per worker. Cluster and stronger delivery settings fail validation before worker launch. The source binding rejects multiple SQL statements and verifies that the DDL created the named source. The existing console bearer policy protects the new inspection route when a token is configured; non-loopback binds require one. The full no-default-feature server binary suite with `process-remote` passed 262 tests, including a fresh database/worker restart through the server config path that restored a committed total from 60 to 110, route authentication, and restart-only reload behavior. The first cold Windows run timed out at a 5-second Python readiness bound; the sample config now uses a bounded 15-second deadline and the rerun passed. The generic worker bound remains 30 seconds.

This server route still uses a direct in-memory source. It cannot replay uncommitted input or claim at-least-once/exactly-once delivery. An idle worker death is noticed on the next call; there is no live replacement or independently attested imported dependency bundle. Target-hardware p99, sustained throughput, and CPU/IPC qualification remain outstanding. No coordinator-cycle or core-operator production code changed in this increment.

Final validation after the startup-module extraction: 262 server binary tests passed with `--no-default-features --features process-remote` and Python enabled. The feature-disabled process-config admission test and the database SQL microsecond-timestamp test passed. `cargo test --workspace --lib -- --test-threads=1 --quiet` passed with `RUST_MIN_STACK=8388608`: 1,977 connector, 973 core, 2,008 database (one ignored), and 870 SQL tests. Both workspace Clippy modes, nightly formatting, the readability checker, and `git diff --check` passed. The existing Windows default-stack caveat still applies; the full test command used the documented larger test stack. Windows linking emitted missing OpenSSL `.pdb` debug-symbol warnings but completed successfully.

### Continuation: fail-closed server worker loss (2026-09-25)

The single-node CLI now waits for every owned Python supervisor to report process exit. An unexpected exit while idle or during a call fences HTTP serving, stops the database and server tasks, reaps the worker, and returns a nonzero shutdown error so an external process manager can restart the whole server. The server does not retry the call or replace a worker in place. For a database configured with process functions, generic graph auto-restart is disabled: it cannot safely relaunch against the same dead worker client and direct source. This also means other pipelines in that database need a whole-server restart after a fault.

The new server test launches a digest-bound real Python child for each of two cases: idle exit and exit after an invocation has entered the handler. It verifies the server exits within a bound, fences serving, and closes the database. The direct source still cannot replay uncommitted input; this test qualifies failure detection and full-server shutdown, not recovery of an in-flight event or exact delivery. No coordinator-cycle or core-operator code changed.

Final validation: the no-default-feature server binary suite with `process-remote` and Python enabled passed 263 tests. The existing real Python child-kill/checkpoint-recovery test passed under elevated local test permission; its first filtered invocation matched zero tests and was rerun with the correct filter. `cargo test --workspace --lib -- --test-threads=1 --quiet` passed with `RUST_MIN_STACK=8388608`: 1,977 connector, 973 core, 2,008 database (one ignored), and 870 SQL tests. Both workspace Clippy modes, nightly formatting, and the readability checker passed after moving the single-node shutdown lifecycle into its own module. The documented Windows default-stack caveat remains; workspace tests did not enable the optional Python test environment. The workspace build emitted the existing missing OpenSSL `.pdb` linker warnings but exited successfully.

### Continuation: embedded replayable-source recovery (2026-09-25)

Embedded registration now accepts a configured append-only connector as the input to a native or loopback Rust/Python process function. It still requires local `BestEffort` delivery; the existing connector startup admission checks the actual append-only contract. The single-node server's startup binding still creates only a direct source.

A focused running-database test uses a deterministic two-record replayable connector with a cursor attached to each batch and a real digest-bound Python child. It commits the first result and cursor, releases a second record, then the handler exits the child during that invocation. A fresh database and worker restore the committed total and source cursor, replay only the pending record, and emit 110. The test uses a single local test connector and manual checkpoint; it does not certify Kafka, sink publication, host-process death, automatic worker replacement, or an at-least-once guarantee. No coordinator-cycle or core-operator production code changed.

Validation: the focused `process_function::` suite passed 37 tests with the Python worker enabled. Its first sandboxed run passed 36 and hit the known Windows `taskkill` access denial in the existing worker-kill test; the full rerun with local test permission passed. The focused single-node server worker-exit regression passed. `cargo test --workspace --lib -- --test-threads=1 --quiet` passed with `RUST_MIN_STACK=8388608`: 1,977 connector, 973 core, 2,009 database (one ignored), and 870 SQL tests. Both workspace Clippy modes with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. The existing unadjusted Windows test-stack caveat remains. Target-hardware tail latency and sampled CPU/IPC were not measured in this increment.

### Continuation: local file connector recovery (2026-09-25)

The embedded Python path now has an end-to-end regression using the production `FILES` source and durable-at-least-once `FILES` sink. It publishes the first JSON input file atomically, observes output 60, and commits a checkpoint with the source inventory and process state. A second file reaches the Python handler, which exits before returning a result. A fresh database and worker restore the checkpoint, skip the committed file, replay the pending file, and publish output 110. The test checks both immutable sink files contain only 60 and 110.

A separate subprocess regression uses the same file connectors with the loopback Rust reference worker. Its child host commits output 60, then pauses inside the second invocation. The parent terminates that host and starts a fresh host and worker against the committed checkpoint. Recovery skips the first file, replays the pending file, and publishes 110; the sink has exactly the two expected outputs. This covers whole-host loss for the Rust reference path and worker-process loss for the Python path. It does not test whole-host loss with a separately supervised Python child. Both remain admitted only as `BestEffort`. Source files must remain immutable and available for replay. The SQL `FORMAT` contract does not admit Arrow IPC files, even though the file connector has an Arrow IPC decoder, so this route uses JSON.

Validation: the focused `process_function::` suite passed 38 tests with Python enabled in the normal runner, excluding the existing Windows `taskkill` case; that case passed separately with local test permission. Both file tests also passed individually. `cargo test --workspace --lib -- --test-threads=1 --quiet` passed with `RUST_MIN_STACK=8388608`: 1,977 connector, 973 core, 2,011 database (one ignored), and 870 SQL tests. Both workspace Clippy modes with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. The unadjusted Windows test-stack caveat remains. No coordinator-cycle or core-operator production code changed, so no before/after hot-path Criterion gate applies. Target-hardware tail latency and sampled CPU/IPC remain unmeasured. Imported Python dependencies are still outside the handler digest.

### Continuation: local at-least-once Rust admission (2026-09-29)

Embedded registration now admits trusted native and loopback Rust handlers under `AtLeastOnce` when the source is a configured connector. Existing startup admission verifies that its contract is replayable, checkpointing is enabled with node-durable storage, and any sink has durable acknowledgement. Direct in-memory sources, Python handlers without immutable dependency binding, and `ExactlyOnce` remain rejected. The single-node server's Python startup binding remains `BestEffort`; both cluster forms remain closed.

The production `FILES` source/sink host-termination regression now runs with `AtLeastOnce`. It commits the first source file and process state, kills the host during the second remote Rust invocation, and restores through a new host and worker. Recovery skips committed input, replays the pending file, publishes 110 after 60, and assigns the same logical activation ID observed by the interrupted worker. This proves one local crash cut; an output published after the last checkpoint can be emitted again on replay. It does not certify Python package replay or distributed ownership. No coordinator-cycle or core-operator production code changed, so no before/after hot-path Criterion gate applies.

Validation: the focused `process_function::` suite passed 40 tests with the Python worker enabled, excluding the existing Windows `taskkill` test that passed separately in the previous increment. The new host-loss test, direct-source/exactly-once rejection, and Python dependency gate passed individually. `cargo test --workspace --lib -- --test-threads=1 --quiet` passed with `RUST_MIN_STACK=8388608`: 1,977 connector, 973 core, 2,013 database (one ignored), and 870 SQL tests. Both workspace Clippy modes with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. Clippy first found that registration used a test-only connector-manager accessor; the production `sources()` lookup replaced it and both Clippy modes passed on the corrected code. The existing Windows default-stack caveat and missing OpenSSL `.pdb` linker warnings remain.

### Continuation: local duplicate-output replay cut (2026-09-29)

The `AtLeastOnce` file connector host-loss regression now covers a second cut. The first input and its state are checkpointed; the second Rust worker invocation returns and the FILES sink durably publishes 110. The parent then terminates the host without a second checkpoint. A fresh host and worker restore the first cut, replay the same logical activation ID, recompute state from 60 to 110, and publish 110 again. The immutable sink files therefore contain 60, 110, and 110. This demonstrates the advertised duplicate-output boundary without advancing process state twice. The earlier mid-invocation cut remains covered by the same fixture. No production code or coordinator-cycle path changed.

Validation: both host-loss cuts passed in the focused file tests. The wider `process_function::` suite passed 41 tests with Python enabled, excluding the existing Windows `taskkill` case previously verified under elevated permission. `cargo test --workspace --lib -- --test-threads=1 --quiet` passed with `RUST_MIN_STACK=8388608`: 1,977 connector, 973 core, 2,014 database (one ignored), and 870 SQL tests. Both workspace Clippy modes with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. The existing Windows default-stack caveat and missing OpenSSL `.pdb` linker warnings remain. No before/after hot-path Criterion gate applies to this test-only change; target-hardware tail latency and sampled CPU/IPC remain unmeasured.

### Continuation: separately supervised Python host-loss cut (2026-09-29)

A new subprocess regression keeps the real Python worker under the parent test's supervisor while a separate database host processes production FILES input and publishes to the durable FILES sink. The host checkpoints output 60, then publishes output 110 and is terminated before another checkpoint. The Python worker remains alive after host termination. The parent stops it, starts a fresh worker and database host, restores the first cut, and verifies replay produces 110 rather than 160; the sink contains 60, 110, and 110. This covers whole-host loss after Python output publication, including the duplicate-output boundary. The test remains `BestEffort`: the example handler's imported dependencies are not bound to its implementation identity.

The focused host-loss test passed. The wider `process_function::` suite passed 42 tests with Python enabled, excluding the existing Windows `taskkill` case previously verified under elevated permission. `cargo test --workspace --lib -- --test-threads=1 --quiet` passed with `RUST_MIN_STACK=8388608`: 1,977 connector, 973 core, 2,015 database (one ignored), and 870 SQL tests. Both workspace Clippy modes with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. The existing Windows default-stack caveat and missing OpenSSL `.pdb` linker warnings remain. No production execution path changed; only test-only endpoint visibility was added to the supervisor. Target-hardware p99 and sampled CPU/IPC remain unmeasured.

### Continuation: pending Python invocation after host loss (2026-09-29)

The separately supervised Python worker now has a second whole-host-loss cut. A test handler records entry into the second activation and remains inside the call while the parent terminates the database host. Only the checkpointed first output (60) is present in the FILES sink at that cut. The worker survives host termination; a fresh worker and database restore the checkpoint and replay the same activation ID. Recovery publishes 110 once, with sink totals 60 and 110 and no double state advance. The test uses the production FILES connector composition under `BestEffort`. Imported Python code and dependencies remain outside the descriptor digest, so this result does not admit Python `AtLeastOnce`.

The focused pending-cut test and the 43-test `process_function::` suite passed with Python enabled, excluding the existing Windows `taskkill` case previously verified under elevated permission. Workspace library tests passed with `RUST_MIN_STACK=8388608`: 1,977 connector, 973 core, 2,016 database (one ignored), and 870 SQL tests. Both workspace Clippy modes with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. Existing OpenSSL missing-PDB linker warnings and the Windows default-stack caveat remain. No production or coordinator-cycle code changed, so the hot-path before/after Criterion gate does not apply. Target-hardware p99 and sampled CPU/IPC remain unmeasured.

### Continuation: bounded process-state frame restore (2026-09-29)

Vnode checkpoint capture now sorts borrowed key/state entries and serializes them without cloning the full authoritative map. Restore rejects a serialized frame above a conservative JSON expansion bound derived from the remaining state budget before deserializing it. It moves decoded entries into a temporary map, checks the cumulative key, timer, and state limits after each entry, and publishes the state and timer index only after every entry passes. The wire format is unchanged. Tests cover the prior owned encoding, an escaped key near the state limit, and rejection of an oversized frame before JSON parsing. This reduces checkpoint and restore temporary copies; it does not measure process RSS or qualify sustained resource use.

No coordinator cycle or core-operator record path changed, so the hot-path before/after Criterion gate does not apply. Target-hardware tail latency and sampled CPU/IPC remain unmeasured.

Validation: the two pre-change restore tests passed. The final `process_function::` suite passed 46 tests with Python enabled, excluding the existing Windows `taskkill` case previously verified with local test permission. The final workspace library gate passed with `RUST_MIN_STACK=8388608` and serial execution: 2,019 database tests passed (one ignored) and 870 SQL tests passed; connector and core suites also passed. Both workspace Clippy modes with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. The Windows default-stack caveat and existing missing OpenSSL `.pdb` linker warnings remain.

### Continuation: bounded vnode capture and local state churn (2026-09-29)

Vnode capture now writes JSON through the existing `BoundedBytesWriter` using the remaining staged-state allowance. It stops encoding when the frame exceeds that allowance, before allocating a complete oversized frame; retained vector capacity is still charged after encoding. A regression captures a populated vnode with an 8 KiB key under a 128-byte allowance, verifies the failure leaves live state unchanged, then retries and compares the exact frame bytes. The sorted entry roster remains a separate temporary allocation bounded by the declared key count.

A native test replaces one timer on each of 64 keys for 64 rounds under a 16 KiB retained-state limit, checkpointing and restoring every 16 rounds. It checks managed-state accounting after each round, equivalent accounting after each restore, and one final callback per key. This covers bounded logical state and timer-index behavior under repeated replacement; it is not a sampled process-RSS or long-duration qualification. No coordinator-cycle or core-operator record path changed, so the before/after Criterion gate does not apply. Target-hardware tail latency and CPU/IPC remain unmeasured.

Validation: both new focused tests and the Python-enabled `process_function::` suite passed (48 selected tests; the existing Windows `taskkill` case was excluded and was previously verified with local test permission). `cargo test --workspace --lib -- --test-threads=1 --quiet` passed with `RUST_MIN_STACK=8388608`: 2,021 database tests passed (one ignored), 870 SQL tests passed, and connector/core libraries passed. The all-features Clippy gate first found a redundant conversion in the new test; it passed after removal. The no-default-features Clippy gate also passed. Nightly formatting, the readability checker, and `git diff --check` passed.

### Continuation: Python child verifies direct handler source (2026-09-29)

The supervised Python child now reads its declared handler file, compares those bytes with the manifest's implementation digest, and compiles and executes those same bytes. This closes the launch gap between the host's pre-spawn hash check and the code the child runs, including stale `.pyc` files with matching source timestamps and sizes. It rejects a handler module name that differs from the file or was loaded before verification. The Python boundary suite covers changed source and stale bytecode. Imported modules, package installations, data files, and later dynamic file access remain outside this binding; Python `AtLeastOnce` admission stays closed. No coordinator-cycle or core-operator record path changed.

Validation: the Python unit suite passed six tests. The no-default-feature process-function suite passed 43 selected tests. The all-feature suite first failed the unrelated Rust worker-loss test; that test passed alone, and the full selected suite passed 48 tests on rerun. Both suite runs excluded the existing Windows `taskkill` test, previously passed with elevated local permission. `cargo test --workspace --lib -- --test-threads=1 --quiet` passed with `RUST_MIN_STACK=8388608`: 2,021 database tests passed (one ignored), 870 SQL tests passed, and connector/core libraries passed. Both workspace Clippy modes with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed.

### Continuation: explicit Python import roots (2026-09-29)

The supervised Python worker now starts with only the handler directory and configured `python_paths` on `PYTHONPATH`. It passes `-s -P` to exclude the Python user site and implicit current-directory imports, and removes `PYTHONHOME` from the child environment. A real child-process regression supplies an ambient-only module, confirms the same worker starts with explicit roots, then verifies the digest-bound handler cannot import the ambient module. The quickstart now creates a virtual environment and selects its interpreter. This narrows accidental import drift; dependencies in the interpreter environment, explicit roots, and later dynamic file access remain mutable and unbound to the descriptor. Python `AtLeastOnce` stays rejected. No coordinator-cycle or core-operator record path changed.

Validation: the new child-process regression passed after the test dependency root explicitly included PyArrow and NumPy, which on this machine were previously found only in the Python user site. The Python-enabled no-default-feature process-function suite passed 44 selected tests, excluding the existing Windows `taskkill` case previously verified with elevated permission. The single-node server binary suite passed 263 tests. Workspace library tests passed with `RUST_MIN_STACK=8388608` and serial execution: 1,977 connector, 973 core, 2,022 database (one ignored), and 870 SQL tests. Both workspace Clippy modes with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. The user-site package copy is ignored local test setup, not a repository artifact or a dependency-binding claim.

### Continuation: native local at-least-once host-loss cuts (2026-09-29)

The production `FILES` source/sink host-loss fixture now exercises the admitted native Rust handler as well as the loopback Rust worker. In the pending cut, the parent kills the database host while the native handler is in its second invocation; a fresh host restores the committed state and replays that activation ID to publish 110 after 60. In the published cut, output 110 reaches the durable FILES sink before the parent kills the host without another checkpoint. Recovery replays the same ID and publishes 110 again, leaving sink totals 60, 110, and 110. The fixture shares source/sink setup and recovery assertions between the two Rust runtimes. This validates native recovery at those two cuts; it does not certify sustained process RSS, other connector compositions, or either cluster mode. No production or coordinator-cycle code changed.

The existing remote published-cut test passed before editing. The four Rust FILES host-loss cases passed after the final fixture change. The Python-enabled `process_function::` suite passed 51 tests with `--no-default-features --features process-remote,files`, excluding the existing Windows `taskkill` case previously verified with local test permission. The workspace library gate passed with `RUST_MIN_STACK=8388608` and serial tests: 1,977 connectors, 973 core, 2,024 database (one ignored), and 870 SQL tests passed. Both workspace Clippy gates with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. The existing Windows default-stack caveat remains. This test-only change does not trigger the coordinator/core before-and-after Criterion gate; target-hardware p99, CPU/IPC, and sustained process RSS remain unmeasured.

### Continuation: bounded process metadata restore (2026-09-29)

The process operator's whole-checkpoint metadata frame now has a 512-byte v1 limit on capture and before JSON decode on restore. Its fixed fields, 64-character binding digest, and widest integer values fit within that limit. A valid frame padded to 4 KiB was previously accepted and applied altered counters; the new regression first reproduced that behavior, then verified rejection leaves the operator unchanged. Vnode state frames retain their separate state-budget bound. This closes one restore-time allocation path, not a sustained process-RSS qualification. The change touches checkpoint capture/restore only, so the coordinator-cycle and core-operator before/after Criterion gate does not apply.

Validation: the focused no-default-feature process suite passed 21 tests. The Python-enabled `process_function::` suite passed 53 selected tests with `--no-default-features --features process-remote,files`, excluding the existing Windows `taskkill` case previously verified with local test permission. `cargo test --workspace --lib -- --test-threads=1 --quiet` passed with `RUST_MIN_STACK=8388608`: core 973, database 2,026 (one ignored), SQL 870, and the connector library passed. Both workspace Clippy gates with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. Target-hardware p99, CPU/IPC, and sustained RSS remain unmeasured.

### Continuation: sampled native Rust memory under fixed-cardinality load (2026-09-29)

On the Windows x86_64 AMD Ryzen 9 7900X development host, the existing optimized `process_function_bench` ran the embedded database, direct source, native running-total handler, and subscription for two fixed 64-row input shapes: 64 distinct keys and 64 rows for one key. Each invocation emits 64 Arrow rows and updates signed 64-bit state; neither workload registers timers or checkpoints. Built with `cargo bench -p laminar-db --no-default-features --bench process_function_bench --no-run`, then ran the produced executable with `--bench native_process_64_distinct_keys` or `--bench native_process_64_same_key`, followed by `--noplot --discard-baseline --sample-size 20 --warm-up-time 5 --measurement-time 180`. Criterion reported about 1.7 million batch iterations per workload. An initial direct invocation without `--bench` entered Criterion's one-shot test mode and was discarded.

The benchmark process was sampled once per second with Windows `Get-Process` (`WorkingSet64`, `PrivateMemorySize64`, and process CPU seconds). Raw CSV and benchmark output remain in ignored local `target/process-rss-qualification-20260929{,-skew}/`. The 64-distinct-key run completed in 189.2 seconds with 185 samples: Criterion mean 107.21 µs/batch (95% interval 106.55–107.97), peak working set 18.418 MiB. Across samples from 95–185 seconds, working set was 18.414 MiB and private bytes 6.496 MiB, with zero fitted growth at page granularity. The single-key run completed in 188.2 seconds with 184 samples: mean 105.79 µs/batch (105.47–106.21), peak working set 17.977 MiB. In the same steady window, working set ranged 17.973–17.977 MiB (fitted slope 0.017 KiB/s) and private bytes stayed 6.180 MiB. Both runs exited successfully. These measurements cover one fixed-cardinality process for about three minutes, not sustained churn, checkpoint/restore, worker overload, target-hardware p99, or sampled IPC; wider resource qualification remains open.

### Continuation: single-node server FILES source binding (2026-09-29)

The single-node Python startup binding now has an integration regression using `source_sql` with the production FILES JSON source. It loads the server TOML, validates the single-node/`BestEffort` config, starts a real digest-bound Python worker, processes the first file as total 60, and commits the source cursor and process state. A new database and worker use the same config and checkpoint; the subscription is open before recovery, so replay of the committed first file would be visible. Publishing the second file yields 110, demonstrating that the binding supports a replayable connector source and restores its committed cursor. No production code or delivery admission changed. The test does not cover whole-server host loss during an invocation, durable sink configuration, or Python `AtLeastOnce`; imported dependencies remain mutable.

Validation: the pre-edit existing direct-source server test passed. The final FILES regression and the Python-enabled `--no-default-features --features process-remote,files` server binary suite passed (264 tests). The workspace library gate passed with `RUST_MIN_STACK=8388608` and serial execution: database 2,026 tests passed (one ignored), SQL 870, and connector/core libraries passed. Both workspace Clippy gates with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. This is test-only work outside the coordinator-cycle/core-operator record path, so the before/after Criterion gate does not apply.

### Continuation: configured sink from a process output (2026-09-29)

Server config validation now accepts a `[[sink]]` input naming a configured process-function output, in addition to a SQL pipeline. The server already installs process outputs before `execute_config_ddl` creates sinks; validation was the missing startup gate. The config regression first reproduced the rejection, then passed while retaining rejection of an unknown input name. The real Python FILES test now uses the same server DDL order to configure a FILES JSON sink. It observes immutable sink totals 60 and then 60, 110 across the database/worker restart, together with the committed source cursor and state continuity. This verifies configured publication for that normal restart path, not sink behavior at a whole-server failure cut or stronger delivery. Python remains `BestEffort` because its imported dependencies are not immutably bound. No coordinator-cycle or core-operator record path changed.

Validation: the focused config and real-worker integration tests passed; the final Python-enabled `--no-default-features --features process-remote,files` server binary suite passed 264 tests. The full workspace library gate passed with `RUST_MIN_STACK=8388608` and serial execution: database 2,026 tests passed (one ignored), SQL 870, and connector/core libraries passed. Both workspace Clippy gates with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed.

### Continuation: full-server replay after Python worker exit (2026-09-29)

A full single-node `run_server` regression uses the configured FILES JSON source, Python process function, and FILES JSON sink. It publishes the first file, observes durable output 60, and commits a checkpoint. A digest-bound test handler then exits the Python child during the second file's invocation. The server detects the child loss, fences serving, closes the database, and leaves only 60 in the sink. A fresh server and worker restore the committed cursor and state, replay the pending file, and publish 110; the sink ends with exactly 60 and 110. The replacement is shut down through the same worker-exit lifecycle after committing. This qualifies that in-flight worker-loss cut through actual server startup and shutdown. It is not a host-process kill or the already-published-but-uncheckpointed cut. Python remains `BestEffort` because imported dependencies are mutable. No production or coordinator-cycle code changed.

Validation: the focused full-server regression passed before and after test-fixture cleanup. The final Python-enabled `--no-default-features --features process-remote,files` server binary suite passed 265 tests. The workspace library gate passed with `RUST_MIN_STACK=8388608` and serial execution: database 2,026 tests passed (one ignored), SQL 870, and connector/core libraries passed. Both workspace Clippy gates with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed.

### Continuation: configured server host-process loss (2026-09-30)

Two subprocess regressions now run the single-node `run_server` startup path with the TOML-loaded Python function, production FILES JSON source, and configured FILES JSON sink. The child server host commits the first file and process state with output 60. For the pending cut, the second Python invocation records logical activation ID 1 and remains inside the handler while the parent kills the server host; only 60 has reached the sink. For the published cut, the sink durably contains 60 and 110 before the parent kills the host without another checkpoint. A test marker releases and exits the orphaned Python child, then a fresh server and worker restore the committed cut. The pending case publishes 110 once; the published case republishes 110, leaving sink totals 60, 110, 110. Both replayed invocations retain logical activation ID 1. These tests execute the server runtime in a test subprocess, not the standalone CLI entry point. Python remains `BestEffort`; imported dependency code is still mutable, and no cluster admission changed.

Validation: the three focused configured FILES server tests and the full Python-enabled server binary suite passed (267 tests). The workspace library gate passed with `RUST_MIN_STACK=8388608` and serial tests: 1,977 connectors, 973 core, 2,026 database (one ignored), and 870 SQL tests. Both workspace Clippy modes with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. The known unadjusted Windows test-stack failure remains. No production or coordinator-cycle code changed, so the hot-path before/after Criterion gate does not apply.

### Continuation: standalone CLI host-process loss (2026-09-30)

The standalone `laminardb` binary now has two integration regressions for the same configured Python FILES source/sink route. Each writes a real TOML file, starts the CLI with `--config`, waits for `/ready`, publishes file 60, and commits the first cut through `POST /api/v1/checkpoint`. The parent then kills the CLI process during the second Python invocation or after output 110 reaches the durable sink without another checkpoint. The digest-bound test handler acknowledges a cleanup signal before exiting the orphaned worker. A new CLI process loads the same config and checkpoint; it skips committed file 60, replays activation ID 1, and yields sink totals 60, 110 for the pending cut or 60, 110, 110 for the published cut. The replacement also completes a checkpoint through the HTTP API. This covers binary bootstrap, config parsing, the control endpoint, and both abrupt host cuts on the local Windows test host. It remains a `BestEffort` Python path; no imported dependency closure or cluster ownership is certified.

Validation: both focused `process_cli_host_loss` integration tests passed with real Python dependencies and `--no-default-features --features process-remote,files`. The workspace library gate passed with `RUST_MIN_STACK=8388608` and serial tests: 1,977 connectors, 973 core, 2,026 database (one ignored), and 870 SQL tests. Both workspace Clippy modes with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. The known unadjusted Windows stack failure remains. No production code or coordinator-cycle path changed.

### Continuation: sampled native timer/checkpoint churn and overload (2026-09-30)

The existing 64-key native operator test now caps one input step at 64 rows and verifies that a 65-row batch is rejected without changing managed-state accounting. An ignored, manual resource test repeats that same bounded cycle 2,700 times in one process. Each cycle processes 64 rounds of 64 input rows, replaces keyed event-time timers, captures and restores whole-operator and vnode state every 16 rounds, fires the final 64 callbacks, and rejects one overload batch. The normal focused test passed. The manual test passed after 141.16 seconds: 172,800 input rounds, 11,059,200 input activations, 10,800 checkpoint/restore rounds, and 2,700 rejected overload batches.

On the same Windows AMD Ryzen 9 7900X development host, the debug-profile test executable was sampled once per second with `Get-Process` into ignored `target/process-resource-churn-20260930/samples.csv` (138 samples; 140.24 seconds at the last sample). Peak working set was 16.875 MiB. Across samples after 30 seconds, working set ranged 16.359–16.875 MiB, private bytes 4.910–5.434 MiB, and the fitted working-set slope was 0.046 KiB/s. The last sampled process CPU was 139.91 seconds, close to one busy core over this test run. This demonstrates bounded memory for one fixed 64-key state shape with repeated timer, checkpoint, restore, and input-row-overload work. It is not an optimized throughput or p99 benchmark, full server/worker RSS qualification, a growing-cardinality test, or target-hardware CPU/IPC evidence. No production or coordinator-cycle code changed.

Validation: the focused normal test and the ignored manual stress test passed. The workspace library gate passed with `RUST_MIN_STACK=8388608` and serial execution: 1,977 connectors, 973 core, 2,026 database (two ignored, including the manual stress), and 870 SQL tests. Both workspace Clippy modes with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. The known unadjusted Windows test-stack failure remains.

### Continuation: configured Python worker saturation and input-byte admission (2026-09-30)

A single-node `run_server` regression sets the Python binding to one in-flight call and the manifest to one activation per transport batch. The handler holds the first FILES JSON record while 31 more distinct-key files arrive; after a 500 ms saturated interval, only one worker invocation has entered and no sink output is visible. Releasing it yields 32 output files. The server config binds a 512 KiB source FIFO and graph input buffer, while the function manifest caps each input step at 32 rows and 128 KiB. A later valid JSON file with a 256 KiB key causes an input-budget fault before another handler call; the 32 published outputs remain unchanged. The test then exits the worker and observes whole-server shutdown. Each FILES JSON input is one complete JSON object per file; a newline-delimited multi-object file is not this connector's format contract. This verifies configured worker-slot and function input-byte admission under one bounded backlog, not sustained worker RSS, actual source FIFO occupancy, target tail latency, or Python `AtLeastOnce`. No production or coordinator-cycle code changed.

Validation: the focused test and the full Python-enabled `--no-default-features --features process-remote,files` server binary suite passed (268 tests). The workspace library gate passed with `RUST_MIN_STACK=8388608` and serial execution: 1,977 connectors, 973 core, 2,026 database (two ignored), and 870 SQL tests. Both workspace Clippy modes with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. The known unadjusted Windows test-stack failure remains.

### Continuation: sampled server and Python saturation resources (2026-09-30)

The engine now exposes `source_queue_reserved_bytes` through the existing Prometheus `PullingGauge`. A scrape reads the current connector FIFO's semaphore reservations, including parked messages and partially reserved sends. It excludes unreserved producer batches, decoder scratch, graph input, and the process operator's pending roster. Startup replaces a weak reference to the observed queue; neither the observer nor a scrape keeps a stopped generation alive. The regression verifies parking, partial reservation cancellation, replacement while the old queue remains alive, and expiry. Coordinator preparation now owns this startup binding as a separate construction phase. No coordinator-cycle or core-operator record processing changed, and no metric updates or locks were added to the record path.

The standalone CLI fixture has a short regression and an ignored manual saturation test. Both use the configured FILES JSON source/sink, 64 keys with 4 KiB string suffixes, one Python invocation slot, one activation per transport batch, and a test handler that delays each invocation by 50 ms. The FIFO cap is 128 KiB; graph input ports allow 256 batches and 2 MiB; the function input step allows 256 rows and 2 MiB; keyed state and timers are capped at 64 each with an 8 MiB retained-state limit. An initial fixture with an 8-batch/128 KiB graph cap failed admission when a cycle staged 29 batches/131,283 bytes. The corrected fixture gives the drain cycle its own allowance while keeping the FIFO small enough to saturate. The normal 128-record run passed and verified durable running totals.

On the Windows x86_64 AMD Ryzen 9 7900X development host, the debug-profile `standalone_server_python_saturation_resource_stress` passed with 4,096 input/output records in 242.79 seconds, including setup and cleanup. Its measured load interval was 232.56 seconds. All 917 HTTP metric samples respected their configured limits; 901 samples showed the FIFO fully reserved at 131,072 bytes, and its final reservation was zero. Sampled graph input was zero throughout: this execution transfers the current input step to the process operator and holds subsequent input in the source FIFO. This does not measure the operator-owned roster or exercise a persistent nonzero graph backlog. The FILES sink contained the expected per-key running totals, 50 through 3,200, each repeated 64 times. Both server and worker were sampled once per second with `Get-Process` (229 samples per process).

| Process | Peak working set | Peak private bytes | Working set after 40 s | Private bytes after 40 s | Fitted working-set slope after 40 s | Last sampled CPU |
|---|---|---|---|---|---|---|
| Server | 58.121 MiB | 16.387 MiB | 56.309–58.121 MiB | 14.477–16.387 MiB | 7.948 KiB/s | 17.312 s |
| Python worker | 67.398 MiB | 786.113 MiB | 66.758–67.398 MiB | 785.117–786.113 MiB | 3.167 KiB/s | 12.453 s |

Raw memory samples, queue samples, and test output are retained in ignored `target/process-server-resource-20260930/`. The worker's first sampled private bytes were already 782.230 MiB before the sustained sample window. These are Windows working-set/private-byte measurements with artificial handler delay and a growing completed-file cursor, not target-hardware p99, IPC, an OS-enforced memory cap, or proof of a long-duration RSS plateau. The residual growth and large worker private-byte footprint require further characterization before broad resource claims. Python remains `BestEffort`; neither cluster form is admitted.

Validation: the queue-observation regression, three normal CLI tests (two host-loss cuts and short saturation), ignored manual resource test, and all 268 Python-enabled server binary tests passed. The workspace library gate passed with `RUST_MIN_STACK=8388608` and serial execution: 1,977 connectors, 973 core, 2,027 database (two ignored), and 870 SQL tests. Both workspace Clippy modes with `-D warnings`, nightly formatting, the readability checker, and `git diff --check` passed. Clippy first rejected numeric casts in the observer and test; checked conversions resolved them, and the full library gate and short CLI test passed again. The existing unadjusted Windows stack caveat remains. Production changes are limited to startup and scrape-time observation, so the coordinator-cycle/core-operator before/after Criterion gate does not apply.

### Continuation: memory after drain and Python native thread limits (2026-09-30)

The standalone CLI saturation fixture now commits one checkpoint after all FILES sink outputs are durable, then samples an idle interval without new input or checkpoints. The normal test uses one idle second; the ignored resource test uses 120 seconds. Every idle scrape requires zero source FIFO reservations, zero graph input charge, and an unchanged emitted-row count; the sink totals must also remain unchanged. The fixture's existing worker-exit observer samples PyArrow's live/peak pool allocation once per second, outside handler calls, when resource observation is enabled. HTTP connection establishment now has a three-second deadline alongside the existing read/write deadlines, keeping the resource observation loops bounded.

The first load/idle run used the launcher behavior at `f4b5c51c` with this increment's observation fixture. On the same Windows x86_64 AMD Ryzen 9 7900X development host, debug profile, Python 3.13.1, PyArrow 20.0.0, NumPy 2.4.0, gRPC 1.84.0 and Protobuf 7.36.2, it processed the same 4,096 files, 64 fixed keys with 4 KiB suffixes, one worker slot and 50 ms artificial handler delay. Load drained in 232.616 seconds with 901 full-FIFO samples, then idle observation completed in 120.239 seconds; the entire test passed in 363.26 seconds. Source and graph charges stayed within 128 KiB and 2 MiB, respectively; sampled graph charge remained zero. The committed FILES source inventory contained exactly 4,096 paths and 376,833 UTF-8 bytes of serialized path JSON. The source retains both its exact membership index and persistent serialized inventory as recovery state; fixed function key cardinality does not bound an indefinitely growing completed-file inventory.

After discarding the first 30 idle seconds, 86 one-second OS samples per process covered about 88 seconds. Server working set ranged 61.922–62.004 MiB (fitted slope 1.319 KiB/s), with private bytes fixed at 18.230 MiB. Worker working set ranged 67.570–67.738 MiB (2.702 KiB/s), and private bytes ranged 786.145–786.262 MiB (1.885 KiB/s). Its 354 PyArrow pool samples reported a 12,800-byte peak and ended at zero live bytes. Pool accounting excludes Python objects, gRPC and other native allocations, so zero live Arrow bytes does not prove a whole-process plateau.

A separate staged import probe sampled `stdlib`, gRPC, NumPy, PyArrow, compute, SDK worker, a live 16-byte Arrow allocation, and release in fresh processes. Default NumPy import reached 768.590 MiB private bytes; selecting Arrow's `system` pool still reached 766.758 MiB at that same pre-PyArrow stage. Setting `OPENBLAS_NUM_THREADS=1` reduced it to 31.488 MiB, and SDK-worker import to 40.516 MiB versus the default 779.078 MiB. NumPy's build configuration identifies OpenBLAS 0.3.30 with `MAX_THREADS=24`. This isolates the large import-time footprint to the NumPy/OpenBLAS thread setting on this build; it does not attribute every later allocation. The small allocation/release probe also retained private bytes after its pool's live allocation returned to zero; changing Arrow's allocator alone is not a justified fix for the NumPy import footprint. [Arrow 20 memory-pool documentation](https://arrow.apache.org/docs/20.0/cpp/memory.html) and [OpenBLAS startup variables](https://www.openmathlib.org/OpenBLAS/docs/runtime_variables/) describe the controls used by the probe.

The local supervisor now sets `OMP_NUM_THREADS=1` and `OPENBLAS_NUM_THREADS=1` on the child command before interpreter startup, reusing the shipped container's existing settings. RPC concurrency and Arrow CPU/I/O pool limits retain their existing bounds. A real-process regression supplies 24-thread host settings and records child environment values in `sitecustomize` before NumPy imports. It first failed with `['24', '24', false]`, then passed with `['1', '1', false]`; the worker completes normal readiness and explicit shutdown. This is a cold launch change for embedded and single-node supervised Python, not an OS CPU quota or control over arbitrary trusted handler threads. Python remains `BestEffort` and cluster admission stays closed. No coordinator-cycle or core-operator record path changed.

The same resource test passed again after the launcher change, deliberately inheriting 24-thread host values. Load drained in 239.201 seconds with 927 full-FIFO samples; idle observation completed in 120.271 seconds, and the entire test in 368.03 seconds. All 455 idle queue samples were empty with the output count fixed at 4,096. The sink contained the same expected per-key totals as before. Its committed FILES inventory contained 4,096 paths and 405,505 UTF-8 bytes of path JSON; the larger byte count reflects the longer artifact-directory prefix. The 357 PyArrow samples again peaked at 12,800 bytes and ended at zero. Both server and worker exited after explicit cleanup.

| Process after launch fix | Peak working set | Peak private bytes | Working set after first 30 idle seconds | Private bytes after first 30 idle seconds | Fitted idle working-set slope | Fitted idle private-byte slope |
|---|---|---|---|---|---|---|
| Server | 62.449 MiB | 18.672 MiB | 62.348–62.449 MiB | 18.535–18.574 MiB | 1.513 KiB/s | 0.582 KiB/s |
| Python worker | 66.109 MiB | 47.590 MiB | 65.945–66.109 MiB | 47.520–47.590 MiB | 2.445 KiB/s | 1.048 KiB/s |

The idle fits use 83 OS samples per process over about 87.7 seconds. Sampled peak worker private bytes fell from 786.262 to 47.590 MiB; its resident working set changed much less. This identifies and removes the large import-time thread-pool footprint for the measured build. Small idle increments remain unclassified, and neither run proves a long-duration process plateau or bounded total memory for an ever-growing FILES source. Other validation processes shared the development host during parts of these samples. These are debug resource observations with artificial worker delay, not throughput, p99, or CPU/IPC qualification.

Raw probes, sampling drivers, analysis script, checkpoint artifacts, queue/Arrow/OS CSV files, and output are retained in ignored `target/process-server-idle-20260930/` (before/import probes) and `target/process-server-idle-20260930-capped/` (after). The driver runs `standalone_server_python_saturation_resource_stress --exact --ignored --nocapture --test-threads=1` from the freshly built `process_cli_host_loss` executable, with `LAMINAR_PROCESS_PYTHON=python`, `LAMINAR_PROCESS_PYTHON_DEPS` pointing to `target/process-python-deps`, and `LAMINAR_PROCESS_RESOURCE_DIR` pointing to a fresh run directory. To rebuild: `cargo test -p laminar-server --no-default-features --features process-remote,files --test process_cli_host_loss --no-run`. Drivers sample Windows `Get-Process` once per second under a 600-second overall deadline; an existing run directory is rejected to preserve earlier evidence.

Validation: the pre-import regression passed after reproducing the uncapped startup; the real-Python process-function suite passed 54 selected tests (one manual stress test ignored, the existing Windows `taskkill` case excluded as previously qualified). All 268 Python-enabled server binary tests, six Python boundary tests, three normal CLI tests, and both manual load/idle resource runs passed. The first normal CLI run failed at the pending host-loss cut with Windows connection timeout 10060; after adding the connection deadline/context, all three CLI cases passed together. The original connection failure's cause was not reproduced. The workspace library gate passed with `RUST_MIN_STACK=8388608` and serial tests: 1,977 connectors, 973 core, 2,028 database (two ignored), and 870 SQL. Both workspace Clippy modes with `-D warnings`, nightly formatting, readability, and `git diff --check` passed. Clippy initially required code formatting in the new rustdoc; its correction changed no execution behavior. The existing Windows default-stack caveat, OpenSSL debug-symbol linker messages, and proc-macro future-compatibility notice remain. This cold startup/test change does not trigger the coordinator/core before-and-after Criterion gate.

### Continuation: Python environment file identity (2026-09-30)

Starting from `ba30f590`, the optional `python_environment` descriptor field now binds a v1 file-tree fingerprint, selected interpreter path, exact `module:function` entry point, runtime-tree SHA-256 and ordered import-tree SHA-256 values. The handler directory is first, followed by configured `python_paths`. Absent bindings retain the existing canonical manifest bytes. Explicit null, unknown fields, unsupported versions, invalid paths/digests and use with another runtime fail validation. The existing complete-descriptor digest carries the new identity into invocation negotiation, pipeline identity and v2 checkpoint compatibility without a state-codec change.

`PythonEnvironmentBinding::capture` and the `package_process_python` example produce the same identity used by local launch verification. The inventory includes source, existing bytecode, native files, data and empty directories; it sorts relative UTF-8 paths and hashes framed entry types, paths, sizes and file digests. A golden vector pins the v1 format. All declared trees share caps of 32,768 entries and 4 GiB, with 16 import roots, 1,024-byte relative paths and a 512 MiB file limit; overlap consumes the budget again. Symlinks, Windows junctions/reparse points and special files are rejected. The function manifest must remain outside hashed trees to avoid a self-reference. Startup verification runs on a blocking task. The launcher now bounds manifest reads and streams the direct handler digest rather than retaining its whole file. There is no new record-path work.

Embedded and single-node supervision accept `runtime_root` only together with the descriptor binding. Bound launch requires an explicit interpreter file and uses `-I -S -B`, validates standard-library path containment, then adds the declared import roots. Directory identity comparison handles Windows paths with and without a verbatim prefix. The child also checks the declared entry point and requires the verified handler file. Legacy launches retain their import-path resolution. The Python 3.13 flags were checked against the [official command-line documentation](https://docs.python.org/3.13/using/cmdline.html) and [path initialization documentation](https://docs.python.org/3.13/library/sys_path_init.html), accessed 2026-09-30.

The real Python regression restores totals 60 and 110 with matching environment files and verifies that bound startup disables site initialization and bytecode writes. Changing an imported helper fails before spawn. Repackaging that helper leaves the direct-handler digest unchanged, produces a different environment binding, and rejects the previous checkpoint's pipeline identity. Even this verified binding still fails Python `AtLeastOnce` registration. A TOML-loaded server test verifies configured SQL input and matching-package recovery. Unit coverage checks runtime/SDK drift, added/removed files, root order, relocation, entry-point changes, malformed identity, inventory budgets and Windows junction rejection. Python boundary coverage includes an uncontained standard-library path and entry-point/file requirements.

The packaging command ran successfully against Python 3.13.1 at `C:/Python313`, the SDK source tree and the existing pinned dependency tree. Its generated manifest and command log are retained under ignored `target/process-environment-binding-20260930/`. A sandboxed inventory initially failed to read `distutils-precedence.pth`; Windows owner-only permissions also blocked `typing_extensions.py` during an attempted separate dependency copy. An approved read confirmed those permissions and file hashes, and the real inventory/recovery tests passed with approved access to the original complete tree. The incomplete copy under `target/process-python-bound-deps-20260930/` is not used for qualification. The first bound worker also exposed the Windows verbatim-path comparison bug; the directory-identity correction passed the real recovery test.

Validation: 62 selected real-Python process-function tests passed (one manual stress test ignored; the previously qualified Windows `taskkill` case excluded), all 269 Python-enabled server binary tests and nine Python boundary tests passed, and the packaging CLI completed. The workspace library gate passed with `RUST_MIN_STACK=8388608` and serial execution: 1,977 connectors, 973 core, 2,036 database tests (two ignored) and 870 SQL. Both workspace Clippy modes with `-D warnings`, nightly formatting, readability (19 module/193 function exceptions) and `git diff --check` passed. Clippy initially requested `if let` for command preparation and borrowing the inventory I/O error; both were corrected. The recorded Windows default-stack, OpenSSL debug-symbol and proc-macro future-compatibility caveats remain. Changes are limited to descriptor validation, packaging and startup; the coordinator-cycle/core-operator record-path Criterion gate does not apply.

This is a content identity and quiescent deployment drift check. It does not freeze files between verification and import or during execution, resolve the complete import/native-load closure, prevent trusted handlers from changing import paths, or bind external data. Python remains `BestEffort`; neither cluster form nor stronger Python delivery is enabled from this metadata.

### Continuation: Windows Python file guards and startup cancellation (2026-09-30)

Starting from `fd9871a5`, bound local Python supervision on Windows opens existing files with `FILE_SHARE_READ`, hashes those same handles, and retains them through worker execution. The manifest is held from its bounded read; the runtime/import inventories retain regular files and directories. Existing incompatible writers reject verification, and subsequent write/delete/rename opens on held entries fail. Opened-handle metadata rejects reparse points and entry-type changes. The existing inventory budget limits retention to 32,768 entries, 17 roots and one manifest (32,786 handles); this excludes unrelated process/socket/engine handles. Packaging uses temporary guards and releases them when capture returns. The v1 tree digest and checkpoint identity format are unchanged. The existing optional workspace `windows-sys` 0.61.2 dependency supplies named filesystem constants without a version upgrade. Other operating systems retain startup drift checks without these Windows sharing restrictions.

The supervisor now takes ownership immediately after spawn, before the caller's next await, and owns readiness, connection, execution and cleanup. A cancellation guard signals that same owner when startup is abandoned. Normal shutdown, readiness timeout, handler initialization failure and startup cancellation retain file handles through successful child reaping; cleanup errors retain their original reporting contract. There are no new record-path operations, resource polling tasks or per-call hashes. The Windows/Rust APIs were checked against [CreateFileW](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew) and [OpenOptionsExt](https://doc.rust-lang.org/std/os/windows/fs/trait.OpenOptionsExt.html), accessed 2026-09-30.

Nine environment unit tests passed, including the unchanged v1 golden digest, existing writer rejection, denied future edits/deletion/rename, guard release after failed verification, and explicit proof that new directory entries remain possible. Three real Python regressions passed in 155.16 seconds with Python 3.13.1 and the original complete dependency tree: lazy imports and engine output 60 work while handler/manifest/dependency edits fail; normal shutdown releases guards; cancellation, readiness timeout and initialization failure complete child cleanup and release the guards, with child absence independently checked using Windows `Get-Process`; matching-package recovery and dependency/checkpoint drift rejection remain intact. These tests require the same approved read access to owner-only dependency files recorded in the previous increment.

Final validation: `RUST_MIN_STACK=8388608 cargo test --workspace --lib -- --test-threads=1 --quiet` passed 1,977 connector, 973 core, 2,041 database (two ignored), and 870 SQL tests. Both workspace Clippy gates with `-D warnings`, nightly formatting, readability (19 module/193 function exceptions) and `git diff --check` passed. The freshly built workspace-feature database test binary passed 67 selected real-Python `process_function::` tests in 226.46 seconds, with one manual stress test ignored and the previously qualified Windows `crashed_python_worker_restores_database_checkpoint`/`taskkill` case excluded. The Python-enabled server binary suite passed 269 tests in 77.62 seconds; the command was `cargo test -p laminar-server --no-default-features --features process-remote,files --bin laminardb -- --test-threads=1 --quiet`. Python SDK boundary tests passed nine cases. `cargo run -p laminar-db --no-default-features --features process-remote,files --example package_process_python -- examples/process_python/manifest.json target/process-file-guards-20260930/manifest.json C:/Python313 C:/Python313/python.exe examples/process_python/handler.py handle python/laminardb_process target/process-python-deps` completed against the real deployment. Gate logs and the generated manifest are retained in ignored `target/process-file-guards-20260930/`. Unix execution was not run for this Windows-specific enforcement step. No coordinator-cycle/core-operator code changed, so its Criterion/IPC gate does not apply; no new latency claim is made.

The first compile exposed a private visibility error, which was corrected. A subsequent compile exhausted the drive. Only the checked workspace `target/debug/incremental` cache was removed, freeing about 89 GiB while preserving source, checkpoint and resource-test evidence. Cargo's fingerprint directory was absent on the following retry, so dependencies were rebuilt. The workspace build took 16m29s and all-feature Clippy 11m24s, including separate Windows native dependency builds. The known default Windows test-stack failure, OpenSSL debug-symbol linker messages and proc-macro future-compatibility notice remain baseline caveats.

This is partial enforcement for embedded and single-node Windows supervision. It does not seal directory additions, ancestor paths or metadata, resolve the full import/native-load closure, survive abrupt host termination, or qualify a failed OS kill/reap path. Windows sharing permits attribute changes: the file-guard test changes a held file's modification time while its contents remain write-protected. Existing timestamp-validated bytecode can therefore change import selection without changing the v1 file-tree digest; this was checked against the installed Python 3.13.1 `importlib` implementation and [Python's cache validation rules](https://docs.python.org/3.13/reference/import.html#cached-bytecode-invalidation). Sharing restrictions are not a sandbox or a cross-platform immutable deployment. Python remains `BestEffort`, and both cluster forms and stronger delivery remain rejected.

### Continuation: content-based Python source loading (2026-09-30)

Starting from `d48d0695`, environment-bound local Python launch supplies `-X pycache_prefix=<interpreter-file>` alongside `-I -S -B`. The prefix is an existing regular file, so filesystem source-cache paths beneath it cannot resolve. On Windows that interpreter file is already held by the inventory guards; this adds no handles, temporary directories, dependency or record-path work. CPython applies the option before early interpreter imports, including `encodings`. Bootstrap checks that the prefix identifies the interpreter and bytecode writes are disabled. Filesystem source modules therefore compile from their bound source bytes, regardless of timestamp or hash caches in `__pycache__`. Frozen modules, sourceless bytecode and ZIP imports retain their interpreter behavior; their containing files are still inventoried. The v1 inventory digest format and delivery admission are unchanged. These APIs were checked against [CPython 3.13 cache-prefix behavior](https://docs.python.org/3.13/library/sys.html#sys.pycache_prefix) and the installed Python 3.13.1 import machinery, accessed 2026-09-30.

The new Windows regression copies the runtime into a private fixture, excludes site packages and existing caches, then installs a poisoned timestamp cache for `encodings`. An ordinary isolated `-B` launch demonstrates that it executes before `-c`. The regression failed on the original launcher with `early timestamp cache executed` and readiness EOF in 47.98 seconds. After the fix it passed in 27.75 seconds: bound startup ignores that early cache, an eager timestamp cache and an unchecked-hash cache; changing a guarded lazy module's timestamp makes its poisoned cache otherwise valid but leaves the captured binding unchanged; the actual worker and database still emit total 60 from source. The installed runtime is not modified. An initial post-fix comparison used the wrong test handler name; using the descriptor's actual selected handler corrected that fixture assertion.

This closes mutable filesystem timestamp selection for the standard source importer under the bound launch policy. Source compilation can increase startup and first-import work; no worker latency improvement is claimed. Trusted handlers can still change import controls. Directory additions, ancestor paths, full import/native-load scope, abrupt host termination and failed OS cleanup remain open. Python continues to admit only embedded/single-node `BestEffort`; stronger Python delivery and both cluster forms remain rejected.

Final validation: `RUST_MIN_STACK=8388608 cargo test --workspace --lib -- --test-threads=1 --quiet` passed 1,977 connector, 973 core, 2,042 database (two ignored), and 870 SQL tests. Both required workspace Clippy gates with `-D warnings`, nightly formatting, readability (19 module/193 function exceptions) and `git diff --check` passed. With the real Python environment variables recorded above, `cargo test -p laminar-db --no-default-features --features process-remote,files --lib process_function:: -- --test-threads=1 --skip crashed_python_worker_restores_database_checkpoint --nocapture` passed 68 selected tests in 206.25 seconds; one manual resource test remains ignored and the previously qualified Windows `taskkill` fixture is excluded. The Python-enabled server binary suite passed all 269 tests in 90.04 seconds. Nine SDK boundary tests passed in 1.764 seconds with bytecode writes disabled; the existing bootstrap containment fixture now supplies the required cache prefix. Logs are retained under ignored `target/process-bytecode-selection-20260930/`. Unix execution was not run. No coordinator-cycle/core-operator code changed, so its before/after Criterion and IPC gate does not apply; target-hardware and longer resource qualification remain open. The previously recorded Windows stack, OpenSSL debug-symbol and proc-macro future-compatibility caveats remain.

### Continuation: bound Python path ancestry (2026-09-30)

Starting from `01a0df3332f8a853059cf51c2c4bea30dc90d179`, packaging and bound startup inspect configured path ancestors before canonicalization can erase links. Windows uses the existing read-share directory handles, acquired from the filesystem root downward, and the existing supervisor retains them with the inventoried files. This covers the manifest, handler, interpreter, runtime root and explicit import roots. Paths allow at most 128 ancestors; the existing inventory/root limits bound retained path and inventory handles to fewer than 38,000. There is no new dependency, configuration, descriptor/checkpoint format or record-path work. Unbound launch behavior is unchanged. Other operating systems validate ancestors without claiming lifetime protection. The stable [`std::path::absolute` API](https://doc.rust-lang.org/std/path/fn.absolute.html) preserves links for this inspection; [Windows read sharing](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew) supplies the existing lifetime guard. Sources checked 2026-09-30.

The new junction-ancestor regression failed before the fix: an ordinary import directory beneath a junction was accepted and canonicalized into its target (0.05 seconds). Final focused environment coverage passed all 12 tests in 0.25 seconds, including unchanged v1 digest/relocation, rejected directory/file junction ancestors, bounded path depth, denied ancestor rename and release after dropping the owner. A metadata-only directory-open experiment failed the existing rename assertion; the final implementation reuses the original read-access policy. The local sandbox denies opening the user-profile ancestor, so these tests require approved read access outside the workspace; no ACL or installation permissions were changed.

Real-process validation: with `LAMINAR_PROCESS_PYTHON=C:/Python313/python.exe`, `LAMINAR_PROCESS_PYTHON_RUNTIME_ROOT=C:/Python313`, `LAMINAR_PROCESS_PYTHON_DEPS=C:/Users/sujit/source/laminardb/target/process-python-deps`, and `RUST_MIN_STACK=8388608`, `cargo test -p laminar-db --no-default-features --features process-remote,files --lib process_function:: -- --test-threads=1 --skip crashed_python_worker_restores_database_checkpoint --nocapture` passed 71 selected tests in 207.20 seconds. One manual resource test remains ignored; the previously qualified Windows `taskkill` fixture remains excluded. The extended actual-worker regression rejects junction-backed manifest/handler paths, successfully imports its guarded lazy module, denies deployment-ancestor rename during execution and permits it after shutdown. Startup cancellation, readiness timeout and handler error each permit deployment-directory rename only after existing reaping/guard-release checks. Matching-package recovery, dependency-drift rejection and existing Rust/Python host-loss tests passed. These host-loss tests still exercise the existing `BestEffort` fixtures and do not qualify an immutable Python host-loss profile.

Final workspace validation: `RUST_MIN_STACK=8388608 cargo test --workspace --lib -- --test-threads=1 --quiet` passed 1,977 connector, 973 core, 2,045 database (two ignored), and 870 SQL tests. Serial library durations were 162.31, 14.64, 96.24 and 1.91 seconds, respectively; compilation took 6m36s. Both required workspace Clippy gates passed with `-D warnings` (37.46 seconds for all features/targets, 13.97 seconds without default features). Nightly formatting, readability (19 module/193 function exceptions) and `git diff --check` passed. With the real Python environment above, `cargo test -p laminar-server --no-default-features --features process-remote,files --bin laminardb -- --test-threads=1 --quiet` passed all 269 server tests in 93.08 seconds (2m09s compilation), including configured bound-worker startup/recovery. Logs are retained under ignored `target/process-python-ancestors-20260930/`. SDK Python code is unchanged; its standalone tests and the packaging CLI were not rerun in this increment. No coordinator-cycle/core-operator code changed; its before/after Criterion and IPC gate does not apply. Unix execution and target-hardware/longer resource qualification remain unrun. The previously recorded Windows stack, OpenSSL debug-symbol and proc-macro future-compatibility caveats remain.

Scope follows the original Phase D package/security requirement and the user's instruction to avoid overengineering. Directory additions, metadata/native-load scope, abrupt host loss and failed OS cleanup still prevent a complete immutable Python deployment claim. No ACL mutation, oplock watcher, custom import machinery or new deployment framework was introduced. Python remains embedded/single-node `BestEffort`; stronger Python delivery and both cluster forms remain rejected. Continue with the existing container and executable example qualification rather than expanding local file guards into a sandbox. A read-only Docker engine availability check succeeded with Docker Engine 29.7.2 on the local Linux/amd64 backend; no container was built or launched in this increment.

### Continuation: container checkpoint quickstart (2026-10-01)

Starting from `8db174d5e5e886c3bf399aa3cc3ddb4e515451c9` on `codex/stateful-process-functions`, this increment returns to the original Phase D container/example requirement. The local Linux/amd64 backend is Docker Engine 29.7.2 with Compose 5.3.1. The baseline Python image built, then exited with `ModuleNotFoundError: handler`. Its command now supplies the existing `--handler-file` option, which checks the manifest digest and executes the verified source. The worker image runs as UID/GID 10001 and has a bounded gRPC readiness probe. The same Dockerfile has an optional `example` target that builds the existing Rust engine example with `rust:1.95-bookworm`, `Cargo.lock`, two build jobs and no debug symbols. The default target remains the independent Python worker. No dependency versions, protocol, descriptor/checkpoint codec, or coordinator/core record path changed.

The Rust example accepts `LAMINAR_PROCESS_ENDPOINT` and reuses `RemoteProcessClient::connect_loopback`; the external caller owns worker shutdown. Its existing local supervisor and checkpoint/resume execution share the same database code. `examples/process_python/compose.yaml` uses ordinary Compose controls: one CPU, 256 MiB memory without swap, 64 tasks, read-only roots, 16 MiB nonexecutable temporary filesystems, dropped capabilities, no privilege escalation, an init process and rotated worker logs. The worker has only loopback networking; the engine joins that namespace. The checkpoint volume is ordinary Docker storage without a quota. No published ports, Docker socket in the engine, Helm abstraction or nonlocal transport were introduced. [Compose service options](https://docs.docker.com/reference/compose-file/services/), [image inspection](https://docs.docker.com/reference/cli/docker/image/inspect/), and [Dockerfile healthchecks](https://docs.docker.com/reference/dockerfile/#healthcheck) were checked on 2026-10-01.

Before activation, the copied commands resolve build tags to local image IDs and save the resolved Compose configuration. The qualified worker identity is `sha256:d17a4c5ca9262b9d93da71430d5678ddda69595a3e4e223c6345b56fcc4531ca`; the Rust example identity is `sha256:395348e38b70206de7de95a2d8ef7ace880455fd2d01f691d5dc0d722f78e4ff`. These are the actual identities reported by this Docker image store; local IDs are not portable registry references. The image metadata/build logs also retain resolved base identities. Operators can distribute the worker with a resolved registry digest and retain that artifact. The descriptor still binds the handler/protocol separately; saved deployment identity is not worker attestation or a stronger Python delivery certificate.

The documented Compose checkpoint command printed `key=a total=60` and succeeded in 27.52 seconds including first deployment/readiness. After `restart worker`, the resume command created a fresh engine container, restored the committed state and printed `key=a total=110` in 11.69 seconds. Worker init PIDs changed from 9576 to 10191. An in-container probe verified UID/GID 10001, `cpu.max=100000 100000`, memory limit 268435456, zero swap, PID limit 64, zero effective capabilities, enabled no-new-privileges, a read-only root and a 16777216-byte nonexecutable `/tmp`; its only network interface was loopback. The worker used Python 3.13.1, grpcio 1.84.0, protobuf 7.36.2, PyArrow 20.0.0, NumPy 2.4.0, typing-extensions 4.16.0 and setuptools 84.0.0, matching the lock. The final stop exited with code zero, with no OOM. All qualification containers were removed; images, saved configuration and `laminar-process-20261001_checkpoints` were retained. These debug observations qualify completed-cut embedded `BestEffort` recovery, not crash replay or latency.

The CLI worker now handles SIGTERM with [gRPC's five-second graceful stop](https://grpc.github.io/grpc/python/grpc.html#grpc.Server.stop), rejects new calls, restores the previous signal handler and explicitly stops the server on exit. Compose supplies a ten-second final process deadline for blocked handlers. A real subprocess/RPC regression holds an activation in the handler, signals shutdown, observes rejected new calls, releases the activation, receives the complete response and observes exit zero. Against the pre-change SDK from Git it failed with `BrokenPipeError` in 0.974 seconds. The final installed-wheel Linux suite passed all ten SDK tests in 2.867 seconds; Windows passed nine with the POSIX signal case skipped in 1.925 seconds. Early test-driver attempts needed inherited dependency paths and an empty container working directory to avoid source shadowing; the final comparisons use those corrected environments. The replaced intermediate image no longer resolved, so the clean baseline comparison uses an isolated Git copy. No production fallback or weakened assertion was added.

Final gates: `RUST_MIN_STACK=8388608 cargo test --workspace --lib -- --test-threads=1 --quiet` passed 1,977 connector, 973 core, 2,045 database (two ignored), and 870 SQL tests; compilation took 6m22s and the full command 694.31 seconds. Both required workspace Clippy gates with `-D warnings` passed (27.78 seconds all features/targets; 18.44 seconds without defaults). Nightly formatting, readability (19 module/193 function exceptions) and diff checks passed. With the same real Python variables recorded in the previous increment, the selected `process_function::` suite passed 71 tests in 239.90 seconds (one manual test ignored; the previously qualified Windows `taskkill` case excluded). The Python-enabled server binary suite passed all 269 tests in 100.24 seconds. `cargo test -p laminar-server --no-default-features --features process-remote,files --test process_cli_host_loss -- --test-threads=1 --nocapture` passed three CLI host-loss/saturation tests in 50.87 seconds; its longer manual resource test remains ignored. Evidence is retained in ignored `target/process-container-20261001/`, including expected baseline failures, installed image metadata, resolved configuration, controls, checkpoint/restore output, cleanup state and gate logs. The existing Windows default-stack, OpenSSL debug-symbol and proc-macro future-compatibility caveats remain. No Criterion/IPC gate applies to these cold launch/example changes; longer resource, target-hardware and tail-latency qualification remains open.

This completes the scoped container quickstart increment, not all of Phase D. Python remains embedded/single-node `BestEffort`; the new container demonstration uses embedded registration. Complete dependency/effect binding, real immutable-profile host loss and cleanup-failure qualification remain open before stronger Python delivery. Both cluster forms stay rejected.

### Continuation: Account activity example (2026-10-01)

Starting from `78f564a8`, original brief section 19 now has a public account
monitor in `process_account`: matching native Rust and vectorized Python,
engine-owned Int64 totals, upward threshold crossings at 100, and named
`inactive` event-time output without clearing totals. Python uses PyArrow 20
checked addition, comparisons and selection over the bounded input batch, then
returns one-row slices. Both handlers reject null state, total overflow and timer
overflow. No engine, coordinator, state codec, scheduling or transport code changed.
The original running-total fixture and its recovery/host-loss tests remain intact.
The new handler's SHA-256 is
`3966a08b7536a91dca874ab2860de4ba9159f1bd12ae4c4f351b0e411ffac682`;
local Git attributes preserve LF bytes for its bound source and manifest.

The driver checks all four output columns against hand-calculated expectations:
19 rows across four accounts, out-of-order timestamps across accounts, repeated
keys, a distinct duplicate Bob event that counts twice, repeated threshold
crossings, timer replacement, retained totals after inactivity, and controlled
110/111/123/130 ms frontiers. Checkpoint mode accepts the first five rows; resume
accepts the remaining fourteen in a fresh engine and Python worker. Bob's saved
timer remains untouched by continuation and fires at 110040 µs. Alice's replaced
timer fires once at 111000 µs. A separate handler test repeats the same logical
activation with its original state snapshot and checks purity across worker reuse.
The example defines inactivity relative to the most recently accepted input;
per-account fixture timestamps do not regress. No sorting or business deduplication
is claimed.

Initial driver attempts exposed the existing source progress boundary: publishing
an atomic watermark does not by itself wake and execute an idle graph. An empty
batch faults timestamp extraction, and a checkpoint wake alone does not advance
a quiescent process operator's timer frontier. Those attempts failed the reference
and were removed. The final fixture uses ordinary Dana transactions to drive
input cycles after each explicit watermark; their outputs are included in the
independent reference. No fabricated timer input or alternate scheduler was added.

The SDK Dockerfile now supplies `account-worker` and `account-example` targets,
sharing its existing runtime layers and Compose controls. The original `worker`
remains the default target. Native client images contain only their selected
example binary. Copied commands resolve image IDs and save configuration before
activation. The qualified account worker is
`sha256:b713672da5ed724574b5793bb6a344766dbbcf65523d52fa407d718725e758ff`;
the client is
`sha256:043430a99d85111894f3b9e5b95643489fde4f65ef355c954e153ae10edf2ef7`.
The Linux/amd64 client build took 312.41 seconds (Rust compilation 4m13s).
Compose checkpoint passed in 12.79 seconds including deployment/readiness; worker
restart followed by fresh-engine resume passed in 6.37 seconds. Init PIDs changed
37491 to 38465. The existing in-container probe verified UID/GID 10001, one CPU,
256 MiB with zero swap, 64 PIDs, zero effective capabilities, no-new-privileges,
read-only root, 16 MiB nonexecutable temporary storage and loopback-only networking.
Python/dependency versions match the previous qualified profile. Final stop exited
zero without OOM. The Compose containers were removed; the images, resolved
configuration, evidence and `laminar-account-20261001_checkpoints` volume remain.

Validation: the native reference/restart tests passed two tests without default
features; the real Python-enabled example target passed all three tests in 3.40
seconds after a 239.61-second build/run command. That Python test starts and
explicitly stops three real workers. The rebuilt native CLI separately passed
full, checkpoint and resume commands. Four Python handler tests passed in 0.825
seconds. The required workspace library gate, with the existing
`RUST_MIN_STACK=8388608` Windows setting and serial execution, passed 1,977 connector,
973 core, 2,045 database (two ignored), and 870 SQL tests in 288.10 seconds including
26.41 seconds compilation. Both required workspace Clippy gates with `-D warnings`
passed (15.33 seconds all features/targets; 15.63 seconds without defaults).
Nightly formatting, readability (19 module/193 function exceptions) and diff
checks passed. Logs, expected failed driver attempts, native checkpoints, image
metadata, controls and container recovery evidence are retained in ignored
`target/process-account-20261001/`. Existing Windows default-stack, OpenSSL linker
debug-symbol and proc-macro future-compatibility caveats remain. This example adds
no coordinator/core change and makes no latency or throughput claim; their
Criterion/IPC gate was not triggered.

This completes the scoped account demonstration. It qualifies embedded completed-cut
`BestEffort` recovery; no crash replay is promised for its direct in-memory source.
Single-node admission is unchanged. Python stronger delivery and both cluster
forms remain closed pending their existing qualification gates.

### Continuation: bound process restoration and shared-cut regressions (2026-10-01)

Starting from `32bace2f67a2093446468408538409dcbc690f95`, this increment begins
original Phase E with the existing process checkpoint participant. A vnode frame
contains keyed values and timers, but its descriptor/partition binding lives in
the whole-operator frame. Restoration now requires that metadata to have passed
validation first. Metadata may be installed once into a fresh operator; it cannot
reset a live operator or pending remote work. Vnode restoration also rejects
pending worker proposals. Failed validation leaves the operator unchanged and a
fresh operator may retry valid metadata. Existing valid checkpoint bytes and the
codec are unchanged. The production change adds one local restoration flag and
checks only constructor/restore paths; no record dispatch, coordinator cycle,
core operator, dependency, protocol or registration admission changed.

The new regression fixture captures real process graph frames, writes them with
`ObjectStoreCheckpointStore` onto a temporary local object store, reopens the
store, and reads the exact ranges through `RecoveryManager`. Its seeded
one-participant cluster-format cut binds owner 7, assignment 7, all 256 vnodes,
source position 3 and watermark 105. It constructs the index and Commit outcome
in the test, including the format's required portability flag; it does not run
CAS publication, acquire a process lease or certify portable distributed state.
Tests verify restored totals, timer replacement/consumption, source-cut metadata,
changed implementation binding, stale assignment, changed boot identity, wrong
vnode inventory and the graph-payload limit. Missing metadata is tested with a
state-only image so timer validation cannot mask a binding bypass.

The real loopback Rust-worker regression delays an uncheckpointed same-key
activation, restores the cut into a separate graph using the same worker/client,
and updates the restored total. It then receives the old graph's actual RPC
proposal into that graph's completion channel before discarding it. The recovered
graph keeps total 111, the untouched second key's timer and its own replacement
timer; subsequent input produces 112. This verifies separation of graph
generations sharing a transport, including reused numeric activation IDs. It
does not fence an old graph against a live cluster assignment. Another real RPC
test rejects restoration during pending work and verifies that the rejected
restore does not discard that invocation's valid result.

Validation: with `RUST_MIN_STACK=8388608` and no real-Python environment selected,
`cargo test -p laminar-db --lib --no-default-features --features
cluster,process-remote,files process_function::tests:: -- --test-threads=1 --quiet`
passed 51 tests (one resource stress test ignored) in 71.71 seconds. The whole
command took 1,205.94 seconds including compilation and build-lock waiting. It
includes all six new regressions and the existing native/Rust file-source
host-loss cases. Python-dependent tests that return without the environment
remain unqualified by this run; the SDK, server CLI and container examples were
not changed or rerun in this increment.

`cargo test --workspace --lib -- --test-threads=1 --quiet`, with the same documented
Windows stack setting, passed 1,977 connector, 973 core, 2,051 database (two
ignored), and 870 SQL tests: 5,871 passed in total. The whole command took
1,306.80 seconds. Both required workspace Clippy gates passed with `-D warnings`
(483.02 seconds all features/targets, 712.42 seconds without defaults, including
build-lock waiting). Nightly formatting, readability (unchanged 19 module/193
function exceptions), and diff checks passed. No coordinator/core record path
changed, so its Criterion/IPC gate was not triggered; no latency claim is made.
The Windows default-stack, OpenSSL debug-symbol and proc-macro compatibility
caveats remain.

Preliminary runs exposed an invalid test-handler command and the cluster index's
mandatory portability flag; the fixtures were corrected. Two existing host-loss
tests timed out in the broad default-connector run, then passed unchanged in the
feature-specific run above and the workspace gate. Both preliminary logs are
retained with final qualification artifacts under ignored
`target/process-cluster-recovery-20261001/`. An independent topology-validation
build ran in the same checkout; its compiler processes were left running. Only
the four files belonging to this restoration increment are included in its commit.

Both cluster forms remain rejected. This is recovery preparation for embedded
and single-node process operators, with cluster-format fixtures. Real one-owner
cluster lease loss/restart, fenced publication, and distributed transfer remain
unqualified. There is no new scheduler, state backend or speculative transfer
framework. Python's supported delivery profile remains `BestEffort`.

### Continuation: process vnode preparation and publication (2026-10-01)

Starting from `fc7363ebc3a514aaa37f09aa8ad80b35df83a3b8` on
`codex/stateful-process-functions`, this Phase E increment implements the process
operator's existing `GraphOperator` prepare/abort/publish/finish hooks. It does not
enable cluster registration or remove the graph's transfer-admission rejection.
The toolchain remains Rust 1.98.0 and Cargo 1.98.0; no dependency, protocol,
checkpoint codec or public API changes are needed.

Preparation binds to canonical predecessor/target fences, requires a fresh graph
for older-cut bootstrap or the exact installed predecessor for an adjacent live
transition, and rejects pending worker calls. Every acquired vnode needs its
donor's validated descriptor metadata. Donor counters merge by maximum, but each
timer is validated against its own donor's generation. Donor watermarks must
describe the same cut; a live transfer cannot lower the installed watermark.
The graph remains responsible for exact live owner rosters and verified donor
provenance. Bootstrap additionally checks the supplied predecessor owner map.

Only changed vnode maps are staged. A replacement due-timer index is reserved
before publication; unchanged keyed maps remain resident. Accounting includes
prepared and retired state, map capacities, timer-index copies, slot/assignment
metadata, borrowed payloads and decode headroom. Key, timer and state limits apply to the
resulting live state as well. Publication swaps prepared allocations and retains
displaced state for explicit cleanup outside graph authority locks. Abort keeps
live state untouched and retains its allocations until the same cleanup hook.
An enum owns the preparation/cleanup lifecycle and blocks reuse before cleanup.
Startup and transfer restoration share the existing metadata and vnode decoding
rules; direct startup restoration rejects overlapping transition state.

Tests use captured native process frames and canonical assignment fixtures to
exercise bootstrap, abort/retry, live acquire/revoke, retained-key continuity,
revoked timers, multiple donors, counter merging, malformed/duplicate/missing
donor state, stale assignment/boot identity, and temporary/final state budgets.
The existing actual-RPC pending-restore regression also attempts a transition and
verifies rejection leaves the delayed invocation's valid result intact. These
are participant-hook tests, not actual process-lease, shuffle, CAS-publication or
cluster-admission qualification.

Baseline: the unchanged feature-specific process suite passed 51 tests (one
resource stress test ignored) in 27.45 seconds, 30.99 seconds including Cargo.
The unchanged recovery-manager tests passed four tests in 0.22 seconds using the
same baseline binary. The feature-specific command
`cargo test -p laminar-db --lib --no-default-features --features
cluster,process-remote,files process_function::tests:: -- --test-threads=1 --quiet`
then passed 58 tests (one resource stress test ignored) in 43.26 seconds, 744.45
seconds including compilation. The rebuilt binary's four recovery-manager tests
also passed in 0.01 seconds. No real-Python environment was selected; its tests
that return without the environment do not add Python qualification here.

After the two lint fixes below, final `cargo test --workspace --lib --
--test-threads=1 --quiet` passed 5,878 tests: connectors 1,977 in 155.60 seconds,
core 973 in 15.39 seconds, db 2,058 in 92.74 seconds (two ignored), derive zero,
and SQL 870 in 2.18 seconds. This run includes all seven new tests and the
modified real-RPC pending-work regression on final Rust source. The whole command
took 1,303.96 seconds, including 17m13s compilation. Both test runs used the
existing Windows `RUST_MIN_STACK=8388608` setting; the unadjusted stack boundary
has not been fixed. Cargo build concurrency was limited to two jobs.

Required `cargo clippy --workspace --all-features --all-targets -- -D warnings`
and `cargo clippy --workspace --no-default-features -- -D warnings` passed in
53.10 and 13.49 seconds. Nightly formatting, `git diff --check` and readability
passed; the checker retains 19 module and 193 function exceptions without growth.
Existing OpenSSL debug-symbol and proc-macro future-compatibility warnings remain.
No coordinator/core operator or record dispatch changed; the mandatory
Criterion/IPC gate for those paths was not triggered and no performance claim is
added. SDK, CLI and container code are unchanged and were not separately rerun.

Preliminary attempts are retained: one fixture needed a `Bytes`-to-`Vec` conversion;
one sandboxed compile ended with exit code -1 before testing; a duplicated test
definition was removed; and Clippy requested a borrowed internal transition view
and a checked-width vnode result. The latter uses the existing codec's `u32`
return directly. An independent Cargo job was left running throughout. Evidence
is retained under `target/process-vnode-transition-20261001/`.

Embedded and single-node server admission are unchanged. Both cluster forms and
stronger Python delivery remain rejected. The new assignment fence is installed
by participant bootstrap publication only; initial and same-assignment cluster
startup still need authoritative graph/control binding before intake. Actual
lease loss, old-owner responses, shuffle ordering and distributed publication
remain unqualified; these operator-hook tests do not open cluster admission.

## Deployment scope and qualification gates

| Mode | Current admission | Required before enabling |
|---|---|---|
| Embedded local | Trusted native or connected loopback Rust/Python worker, direct or append-only connector source, `BestEffort`; native and loopback Rust with replayable connector source, durable checkpoint and sink under `AtLeastOnce`; bounded vnode capture and guarded restore with per-entry validation; production file source/sink replay pending and durably published uncheckpointed activations after native or remote Rust host termination; a separately supervised Python worker survives both cuts under `BestEffort`; optional Python file identity is checked at launch and bound into recovery, with existing file contents and configured path ancestors guarded during Windows supervision; Linux/amd64 Compose example restores a completed checkpoint across fresh engine and worker processes under bounded container controls | Complete Python environment immutability, sustained process-RSS qualification, and target-hardware profiling before wider admission or latency claims |
| Single-node server | Startup-bound Python manifest/worker in TOML, direct in-memory or FILES JSON source via `source_sql`, SQL input and WebSocket output, configured FILES JSON sink from process output, console-policy binding inspection, `BestEffort`; committed FILES cursor and process state restore with sink publication through the startup binding; in-flight worker exit triggers fenced whole-server shutdown; server-runtime and standalone CLI host-loss regressions replay pending input and republish uncheckpointed sink output; sampled 4,096-record worker saturation and two-minute idle interval respect queue charges; supervised Python sets native thread limits before imports | Immutable dependency bundle, longer resource qualification with the growing FILES inventory accounted for, and target-hardware latency evidence before stronger delivery |
| Cluster with one node | Rejected at registration and operator capability | Shared-checkpoint binding, assignment-fenced vnode state/timers, shuffle provenance, one-owner loss/restart and stale-attempt tests |
| Distributed cluster | Rejected | All one-node gates plus cross-node shuffle ordering, vnode acquisition/revocation, timer/pending-work transfer, rescale and node-loss tests |

One-node cluster execution uses the cluster lifecycle and cannot be treated as an embedded shortcut. Keep cluster admission closed until actual ownership and recovery tests pass. Exact delivery additionally requires the repository's certified source/sink composition; deterministic handler results alone do not certify it.

### Continuation: main integration (2026-10-05)

Resumed the clean `codex/stateful-process-functions` branch at
`d75233d08ad0f334dbcb1443aea355b43999dd09` after finding the checkout on `main`.
Fetched and merged `origin/main` at
`f62931d7cc6502caea2e2af200b9eadf7366eb39` (topology migrations and dependency
updates). Five conflicts combined the new startup lifecycle with process package
identity binding, process-only startup, and the explicit unqualified transfer
rejection. Existing setup/authority phases moved into concept owners to respect
main's reduced readability baselines; no baseline exception increased.

Merge commit: `1d5f3f29ae1b7f6991e12601c39e2b97e455a5dd`. Current toolchain:
rustc 1.99.0 (`b940084d7`, 2026-09-28), cargo 1.99.0 (`5f94df478`, 2026-08-27).
The merged lockfile resolves Arrow 58.4.0, DataFusion 53.1.0, object_store 0.13.2,
tonic 0.14.6 and prost 0.14.4; this increment makes no further dependency edits.
The first all-feature Clippy attempt failed in `aws-lc-sys` 0.45.0's build script
with Windows PermissionDenied under the sandbox. Normal native-build access
then exposed three integration errors: the moved checkpoint module's
`StorageProvider` import, topology's exhaustive process-contract name, and the
test replay source's unsupported `SourcePosition::Initialized` case. After those
repairs, all-feature Clippy passed in 1,195.16 seconds including Cargo lock wait.

The unchanged feature-specific process baseline passed 57 tests, failed one,
and ignored one in 92.33 seconds (1,573.53 seconds including compilation).
`at_least_once_native_republishes_file_after_host_termination` timed out waiting
five seconds for its killed child to exit. The unchanged test passed its
isolated rerun in 8.58 seconds; the timeout and assertions were not weakened.
No real-Python environment was selected. Evidence is retained under
`target/process-cluster-authority-20261005/`.

### Continuation: process startup assignment binding (2026-10-05)

Starting from merge commit `1d5f3f29`, fresh and same-assignment restored process
state now receives the graph-ready assignment before compute launch. The
existing durable history audit supplies the binding. For process participants,
the graph verifies its pipeline and vnode domain, captures the live local process
identity and exact registry/transport binding, derives the local vnode roster,
invokes participant hooks, then revalidates the authority. Startup accepts both
shuffle endpoints fenced or both certified for the target; live transitions keep
their active-certificate requirement. The private state binding does not open
record intake. A failed hook or changed authority drops the private graph image.

The process hook rejects pending calls, unfinished transitions, changed bindings,
invalid rosters, state outside local ownership, and prior execution without
restored metadata. It preserves the saved state/timer cut and accounting. Once
bound, raw metadata and vnode restore are sealed. Existing staged transitions
retain their prepare/publish/abort/finish ownership. Registration and capability
admission still reject both cluster forms; this increment changes cold startup
and restore checks, without changing coordinator cycles, record dispatch, worker
generation fields, protocol, SDK or dependency versions.

Focused qualification: the feature-specific process suite passed 61 tests (one
manual stress test ignored) in 24.97 seconds, 222.97 seconds including Cargo.
Five graph tests passed in 0.05 seconds, 94.81 seconds including compilation.
They use real process-lease CAS acquisition and monotonic takeover, loopback
shuffle certificates, lease loss before/during hooks, and a replacement boot at
term two. Initial fixture failures required installing the shared shuffle lease,
using observed takeover rather than timestamp-only acquisition, and constructing
an actual domain mismatch rather than using a rejected late topology setter.
The first full workspace run took 2,018.01 seconds, including 25m01s compilation.
Connectors passed 1,989 tests (two ignored) in 161.72 seconds and core passed
1,126 in 204.48 seconds. The db suite passed 2,192, failed ten, and ignored two
in 149.13 seconds; Cargo did not reach derive/SQL execution. Nine failures exposed
an overly broad startup hook: SQL and source-only graphs activate transport later
or have no shuffle. Binding now selects only `ProcessFunctionV1`; its cold
authority constructor explicitly permits fenced startup transport. Two added
regressions cover the existing no-process lifecycle and prove that private
binding leaves transport fenced while live transfer remains rejected.

The tenth failure was the Python quickstart's strict canonical-byte test after
the Windows checkout converted its JSON and handler to CRLF. The example now
uses the account example's existing two-file `.gitattributes` policy to retain LF.
The files' logical Git contents and the test are unchanged; the original binary's
unchanged artifact test passes after byte normalization. No assertion or runtime
admission was weakened.

These cold authority tests do not qualify process record intake, lease-loss
recovery, assignment publication or distributed transfer. Both final Clippy gates
pass: all features/targets in 38.32 seconds and without defaults in 7.77 seconds.
Final `cargo test --workspace --lib -- --test-threads=1 --quiet` passed 6,189
tests: connectors 1,989 in 157.66 seconds (two ignored), core 1,126 in 204.30
seconds, db 2,204 in 278.99 seconds (two ignored), derive zero, and SQL 870 in
0.64 seconds. The whole command took 835.84 seconds, including 3m11s recompilation.
It includes all seven final graph tests, the native state/timer and pending-RPC
regressions, and all ten repaired workspace regressions. Both workspace test runs
used the existing Windows `RUST_MIN_STACK=8388608` setting and two build jobs;
the unadjusted stack boundary is not newly qualified.

Nightly formatting, diff checks, and readability pass with 18 module and 214
function exceptions without growth. Existing OpenSSL debug-symbol and proc-macro
future-compatibility warnings remain. No coordinator-cycle, core-operator or
record dispatch code changed; the mandatory record-path Criterion/IPC gate was
not triggered and no performance claim is added. Real-Python, SDK and container
suites are not rerun in this increment; Rust/native transport tests do not add
Python qualification. Evidence is retained in
`target/process-cluster-authority-20261005/`.

### Continuation: single-owner process execution fencing (2026-10-05)

Starting from `262ecab0dde146c3fbcd6efa2694ee465119ce53`, this increment connects
the private startup binding to actual process input and result application.
Cluster graph construction selects an awaiting-authority execution state. Startup
then pins the existing registry snapshot, transport incarnation, shared process
lease deadline and recovery generation. Multi-owner assignments fail before
intake until ordered cross-node input and frontier handling are qualified.
Both cluster forms remain rejected by registration and operator capability.

The single-owner path validates accepted input, derives canonical vnodes with the
cached key codec, and reuses `route_checkpointed_batch`. Routed Arrow input and
key/vnode scratch are checked against the declared input and temporary graph
budgets before state application. This is not a full process-RSS measurement of
the routing helper's internal metadata. Native callbacks and completed worker
proposals require current authority immediately before applying state. Worker
opens carry the pinned assignment/recovery generations for data and timers;
completion guards retain that scope. Local execution retains zero generations.
The record path uses the existing monotonic deadline and atomic versions, without
registry/certificate locks, network or storage work. No coordinator, core,
protocol, SDK, Python, dependency or runtime-admission change is included.

Nine focused tests pass in 3.36 seconds, 92.74 seconds including compilation:
canonical vnode routing and per-key order through the real graph, frame reopen
and timers, unbound/fenced transport, native loss before input and during the
handler, natural expiry, input/temporary-budget rejection, multi-owner rejection,
actual Rust worker generation fields, delayed assignment/recovery replies, and
a replacement boot after monotonic process-lease CAS takeover at term two. The
replacement restores the selected state/timer frames and rejects the old reply
and boot binding. These tests use real lease managers and loopback transports;
private hooks leave public cluster admission closed. They do not certify a
committed distributed checkpoint, live vnode publication or cross-node transfer.
The first focused compile required keeping metadata as bytes rather than cloning
`OperatorCheckpoint`; two new timer assertions then needed the fixture's actual
`inactive` output label. Existing behavior and assertions were unchanged.

Unchanged process baseline: 61 passed, one ignored, in 29.74 seconds using the
existing all-feature binary. Both Clippy gates pass, all features/targets
in 391.92 seconds including build-lock wait and without defaults in 37.54 seconds.
The first Clippy attempt found one unnecessary `String::to_string` in the new
benchmark-only fixture; the constructor now moves that error into `DbError`.
Nightly formatting, diff checks and readability pass with 18 module and 214
function exceptions without growth. The first workspace run passed connectors
(1,989, two ignored) and core (1,126), then db passed 2,212, failed one and ignored
two. The unchanged native pending-input host-loss test reached the existing
five-second child-exit timeout; its isolated rerun passed in 4.64 seconds. A second
workspace run encountered the same timeout while optimized compilation was
active. The private native/worker tests now use `tests/mod.rs` and `tests/remote.rs`;
their final test build and all-feature Clippy rerun pass (272.71 seconds including
lock wait). The final full workspace run, without simultaneous compilation,
measurement or profiling, passed all 6,198 tests: 1,989 connectors (two ignored),
1,126 core, 2,213 database (two ignored) and 870 SQL. The command completed in
649.37 seconds; the database suite took 284.03 seconds. The five-second host-exit
assertion and production behavior were unchanged. All runs use two build jobs
and `RUST_MIN_STACK=8388608`;
the unadjusted Windows stack boundary is not newly qualified. No real-Python
environment is selected.

Criterion before/after runs use 30 samples, three-second warm-up and seven-second
measurement, without concurrent tests or compilation. The no-default-feature
reference excludes cluster guards, so a matching cluster-enabled reference was
also built from a Git archive of the exact starting commit. Cargo reused the same
executable name across source trees; the final binary was preserved, the unchanged
reference rebuilt, and both case lists verified before measurement. Their separate
executables and hashes are retained in the evidence directory. The cluster-enabled
comparison's largest point regression is 2.91%; no existing-path point regression
exceeds 5%. The unchanged handler's measured variation is not an implementation
improvement claim. Core window assignment is unchanged and does not measure the
process operator.

| Existing path | Before mean | After mean | Criterion relative mean change |
|---|---:|---:|---:|
| Cluster feature enabled, one-row source/subscription | 31.038 us | 31.202 us | +0.53% |
| Cluster feature enabled, 64 distinct keys | 105.338 us | 108.403 us | +2.91% |
| Cluster feature enabled, 64 rows sharing a key | 108.604 us | 107.417 us | -1.09% |
| No defaults, one-row source/subscription | 31.336 us | 30.469 us | -2.77% |
| No defaults, 64 distinct keys | 108.465 us | 105.807 us | -2.45% |
| No defaults, 64 rows sharing a key | 114.774 us | 108.999 us | -5.03% |
| No defaults, prepared one-row handler | 424.804 ns | 438.341 ns | +3.19% |
| Core 60-second tumbling-window assignment | 1.412 ns | 1.402 ns | -0.69% |

The benchmark-only direct fixture compares identical prepared inputs through the
production operator; it excludes connector, coordinator and subscription work.

| Prepared input | Local mean | Private single-owner mean |
|---|---:|---:|
| One row | 1.691 us | 2.876 us |
| 64 distinct keys | 70.575 us | 176.626 us |
| 64 rows sharing one key | 73.005 us | 85.135 us |

This new path has measurable routing cost, especially per-vnode Arrow grouping
and repeated operator key encoding for distinct keys. There was no admitted
cluster process path to compare before this increment. These numbers are not a
cluster throughput qualification; the dev-host distinct-key mean falls short of
the 500 K events/s reference target. Keep that cost visible when qualifying
cross-node input, without adding an alternative router or speculative optimizer.
Measurements are on Windows x86_64 MSVC, Ryzen 9 7900X, 12 cores/24 logical
processors, Rust 1.99.0 with thin LTO, without a product or tail-latency claim.

Hardware counter capture completed after Windows administrator consent. WPR
recorded retired instructions and total cycles on context switches during two
30-second profiles of the preserved final binary: prepared local and private
single-owner execution with 64 distinct keys. Both benchmarks exited successfully;
the task-owned recorder saved a 122,683,392-byte trace and stopped without cleanup
failure. Capture and cleanup took 76.59 seconds. The pre-existing NT Kernel Logger
was left running. The first non-elevated attempt had returned `0x80070005`
(`Access is denied`).

IPC counter analysis remains unverified. The installed Microsoft TraceProcessor
libraries throw `NullReferenceException` in `SymbolFlyweightDataSource` when
registering `UseProcessorCounters`, before trace processing. The trace and actual
benchmark PIDs/digests are retained for analysis; no IPC value or successful IPC
qualification is claimed. The user requested committing this increment with that
unresolved analysis recorded. Complete the counter analysis before further
record-path qualification or cluster admission. Primary references checked on 2026-10-05:
[PMU recording](https://learn.microsoft.com/en-us/windows-hardware/test/wpt/recording-pmu-events)
and [TraceProcessor](https://learn.microsoft.com/en-us/windows/apps/trace-processing/tutorial).
Evidence is under `target/process-owner-execution-20261005/`.

### Continuation: saved hardware counter analysis (2026-10-05)

The preceding IPC analysis is now complete. Microsoft's standalone
`Microsoft.Windows.EventTracing.Processing.All` 1.12.10 reads the saved trace
successfully; the WPA-bundled 1.8.1 reader failed during data-source registration.
The analysis-only .NET project, package lock and output remain in the ignored
evidence directory. No runtime source or workspace dependency changed. Primary
references checked on 2026-10-05: the
[standalone reader tutorial](https://learn.microsoft.com/en-us/windows/apps/trace-processing/tutorial)
and [Microsoft package](https://www.nuget.org/packages/Microsoft.Windows.EventTracing.Processing.All/1.12.10).

The reader requires both `InstructionRetired` and `TotalCycles`, filters by the
actual recorded benchmark PIDs, and divides summed retired instructions by summed
cycles across each process's context-switch intervals. It rejects missing or
nonpositive counters. Processing succeeds with `AllowLostEvents` and
`AllowTimeInversion` both false. Each process includes its benchmark harness and
initialization; these are process-wide 30-second profiles, not isolated handler
instructions or representative tail-latency measurements.

| Prepared 64 distinct keys | PID | Instructions | Cycles | Scheduling intervals | IPC |
|---|---:|---:|---:|---:|---:|
| Local | 277124 | 542,582,599,826 | 152,287,203,118 | 961 | 3.563 |
| Private single-owner | 298440 | 503,628,265,522 | 156,098,871,426 | 514 | 3.226 |

Both exceed the repository's 2.0 profiling guideline for this workload on the
stated dev host. This closes the counter-extraction prerequisite for `edc9f6dd`;
the distinct-key throughput limitation and closed cluster admission remain.
The trace SHA-256 is
`D15DC8982C5E11A50C2278FC5544B7511A303718617A97F7C8F5E019CC490A19`;
both captured workloads use the preserved final executable SHA-256
`B28E66A24E348D82F63F0CC3A9D10F8397C58EC54484E2CC9A7691303A59A12F`.
`ipc-standalone-results.json`, `ipc-standalone-analysis.log`, and the reader source
record the method and exact totals. The existing Rust gates for `edc9f6dd` remain
applicable because this continuation changes only the worklog and ignored analysis
artifacts; no Rust tests or benchmarks were rebuilt or rerun.

### Continuation: ordered cross-node process input (2026-10-05)

This increment starts from `283755a1441374b4b466c14007e9a5d5b064d001` on the
existing branch, with Rust/Cargo 1.99.0 and unchanged workspace dependencies.
Private multi-owner execution now uses the existing graph shuffle hooks,
canonical vnode router, transport admission credits and remote worker scheduler.
One bounded queue retains graph-delivered peer batches/frontiers; one background
send task owns a local input cut until every peer's admission outcome is known.
Safe pre-admission failures retain the same send plan for a later graph cycle;
partial/uncertain sends require recovery. Queued retries remain runnable.

Frontiers follow earlier data on each peer channel. The effective frontier waits
for input application and worker completion; cached minimum batch times hold
output progress without rescanning Arrow rows. Idle channels require ordered
revival. Stage, canonical routes, ownership, assignment, process lease, topology
and recovery generation are checked before retention and application. Invalid
input schema/key/time is terminal; resource failures retain the existing recovery
classification. A send future pins recovery before it is scheduled, so an old
plan cannot acquire a newer generation's stream or sequence.

Drained metadata records the exact assignment/digest, node and applied peer/local
frontiers. Same-assignment restore under a newer recovery generation rebroadcasts
the local frontier before intake and preserves state, timers and activation
progress. Distributed metadata cannot be installed into local execution. Local
serialized metadata is unchanged and remains bounded to 512 bytes;
cluster-enabled decoding has a 256 KiB ceiling for the peer roster and rejects
oversized local frames. Builds without cluster support keep the original
512-byte pre-decode bound. Distributed donor/frontier transfer remains explicitly
rejected until coordinated reassignment qualification.

Embedded and single-node server execution retain their existing public paths.
Public single-node cluster and multi-node cluster admission remain closed. These
tests certify neither deterministic replay across independent source channels nor
a committed distributed source/state/sink cut. No second scheduler, state backend,
protocol, SDK or dependency is added.

The unchanged execution baseline passes nine tests. Its first restricted-sandbox
run passed six but failed three worker connections; the same baseline passes with
loopback access. The final focused configuration is
`cargo test -p laminar-db --lib --no-default-features --features cluster,process-remote,files process_function:: -- --test-threads=1 --quiet`:
110 passed, one ignored, in 40.11 seconds (159.93 including compilation). Thirteen
new execution tests cover actual two-peer lease/transport fixtures, canonical
routing and per-channel key order, frontier/timer ordering, idle revival, retained
budgets, safe send retry, stale authority, drained restore, mode/assignment/metadata
rejection, graph barrier draining, and real Rust worker scopes and delayed replies.
The twelve core topology transport tests pass in 1.22 seconds, including a future
created before a recovery-generation change. The initial core filter matched zero
tests; the corrected `topology_transport::` filter is the reported result. A
no-default-feature test separately verifies the local pre-decode metadata bound.

All required Rust gates pass with `CARGO_BUILD_JOBS=2` and
`RUST_MIN_STACK=8388608`. The final `cargo test --workspace --lib` passes 6,212:
1,989 connectors (two ignored), 1,127 core, 2,226 database (two ignored), and 870
SQL; 427.62 seconds including compilation. Both Clippy configurations pass;
the final all-feature/all-target source check takes 29.66 seconds, and the
no-default check 9.77 seconds. Nightly formatting, diff checks and readability
pass with unchanged 18 module/214 function exceptions. The initial Clippy run
found an inverted branch and truncating fixture casts; these are corrected.
Existing native FILES host-loss/replay tests pass. No real-Python environment,
Kafka/S3 distributed soak or committed distributed process cut is qualified here.

Criterion uses 30 samples, three-second warm-up and seven-second measurement,
without concurrent builds or tests. The exact preserved starting binary is the
reference. The optimized build initially reused a stale core artifact missing
the new method; preserving and moving its release fingerprint forces the affected
packages to rebuild. Cargo's package-clean command refused the untagged target
directory. The rebuilt final binary contains all nine direct cases and has SHA-256
`B3F606AD68E06B55BD5AD1D8BD0B8838A4BAD992D016958E12AEFFC2DA408445`.
The reference hash remains
`B28E66A24E348D82F63F0CC3A9D10F8397C58EC54484E2CC9A7691303A59A12F`.

| Existing path | Before mean | After mean | Relative mean change |
|---|---:|---:|---:|
| Prepared local, one row | 1.818 us | 1.467 us | -19.32% |
| Prepared local, 64 distinct keys | 77.813 us | 65.951 us | -15.24% |
| Prepared local, 64 rows sharing a key | 83.324 us | 70.492 us | -15.40% |
| Private single-owner, one row | 2.993 us | 2.704 us | -9.66% |
| Private single-owner, 64 distinct keys | 196.873 us | 176.211 us | -10.50% |
| Private single-owner, 64 rows sharing a key | 89.728 us | 77.499 us | -13.63% |
| Source/subscription, one row | 26.538 us | 26.997 us | +1.73% |
| Source/subscription, 64 distinct keys | 112.963 us | 107.748 us | -4.62% |
| Source/subscription, 64 rows sharing a key | 111.256 us | 109.764 us | -1.34% |
| Core tumbling-window assignment | 1.399 ns | 1.407 ns | +0.56% |

No existing-path point regression exceeds 5%. The source/subscription comparisons
have no statistically detected change; their confidence intervals and outliers
remain in the evidence. The broad direct-path reductions are observations on this
host, not an optimization or product-throughput claim.

| New private two-owner topology | Mean |
|---|---:|
| One row | 61.901 us |
| 64 distinct keys | 199.228 us |
| 64 rows sharing a key | 145.532 us |

The two-owner fixture uses two real loopback endpoints in one current-thread
runtime, identical prepared keys/handler, unknown frontiers and no timers. It
measures routing, background transport of remote rows, application and draining;
it excludes sources/subscriptions, worker RPC, shared checkpoint persistence and
real multi-process/network latency. There was no admitted path before this task.
The distinct-key result remains below the 500 K events/s reference rate; these
means do not establish target-hardware throughput or tail latency.

Fresh hardware profiling completes all three 30-second distinct-key cases from
the final binary, each with exit code zero, in 111.80 seconds including recording
and trace cleanup. Only the task-owned recorder is started/stopped; WPR confirms
it is stopped. The standalone `Microsoft.Windows.EventTracing.Processing.All`
1.12.10 reader accepts the trace with lost events and time inversion disallowed.
Weighted IPC is the sum of retired instructions divided by the sum of cycles
for each actual benchmark PID's scheduling intervals.

| Profile | PID | Retired instructions | Cycles | Intervals | Weighted IPC |
|---|---:|---:|---:|---:|---:|
| Local, 64 distinct keys | 318028 | 548,205,921,634 | 151,751,959,699 | 420 | 3.613 |
| Private single-owner, 64 distinct keys | 349540 | 522,471,043,840 | 163,646,932,146 | 1,297 | 3.193 |
| Private two-owner, 64 distinct keys | 337396 | 377,561,405,983 | 161,380,132,475 | 1,634 | 2.340 |

All three meet the IPC > 2.0 guideline. The counters include initialization and
the benchmark harness; they do not isolate handler instructions, certify real
multi-process cluster load, or establish target-hardware tail latency. The final
ETL has SHA-256
`06E60DEFFF5B1EC09DDF664BF83E4AF5EDBB7EF8183E5A86904DEFB60065731E`.
Its manifest binds the same final executable hash reported above.

The first recorder saves a partial local trace before Windows PowerShell fails
to retrieve the redirected benchmark's exit code. The
[upstream handle-caching correction](https://github.com/PowerShell/PowerShell/issues/5421)
is verified with the same executable's `--list` invocation. A corrected
administrator launch is canceled by Windows; the approved retry completes the
full capture. These capture-only repairs change no Rust source or benchmark
binary. Evidence, exact commands, durations, hashes, Criterion means/intervals
and capture scripts are under `target/process-shuffle-20261005/`; the partial
trace, capture failure and its counter analysis remain under `ipc-first-partial/`.

## 2026-10-05 — committed process ownership transfer and host-loss restore

Status: private graph implementation, correctness gates, Criterion validation
and hardware IPC profiling complete. Starting commit:
`d921dfc893aa742d23bb1bc046b783351ba9772a`.

This increment applies to private single-owner and multi-owner cluster graphs.
Both public cluster admission forms remain closed. It reuses the existing
assignment drain, graph participant lifecycle, checkpoint stores, range handoff,
and `RecoveryManager`.

Process transitions now validate each donor's exact predecessor assignment,
participant and peer roster, and require one drained effective frontier, including
idleness. State and timers are prepared together with replacement execution and
shuffle authority. Publication swaps prepared values under the graph rotation
fence; the existing finish phase releases displaced allocations. New channels
begin active at the committed cut and re-establish idleness using the target
roster. A single-owner watermark that cannot reconstruct an exact millisecond
frontier is rejected before mutation.

The shared-cut fixture aligns real peer barriers, persists participant manifests
and state frames, creates an immutable committed index, and records the exact
shared CAS outcome. It records and finalizes a real assignment-drain decision
before live publication and loads acquired frames through checkpoint range
handoff. Focused qualification covers:

- Two-owner live transfer with retained timers and continued routed input.
- Two-owner committed restore onto three owners with exact donor vnode selection,
  checkpoint source positions and one timer application per key.
- Missing, wrong-assignment, wrong-participant, wrong-roster and inconsistent
  donor metadata; damaged selected donor objects fail recovery.
- A delayed remote Rust reply from a lost owner, rejected after a replacement
  restores and executes from the committed cut.
- An actual killed child host after it applies an uncommitted input; the fresh
  owner restores committed totals, source positions and timers.

The host-kill test preserves a real in-memory CAS-committed object namespace as a
read-only filesystem restart image before applying the uncommitted input. The
native `LocalFileSystem` backend has no conditional update; the fixture does not
emulate that authority protocol. Both donor graphs run in the killed child. This
does not qualify a cloud store, independent failed/surviving peer processes,
coordinated recovery-round publication, certified source/sink delivery or replay
ordering between independent source channels. Existing activation counters are
merged from the committed donors; stable callback identifiers across a changed
source merge remain unqualified.

Evidence is under `target/process-handoff-20261005/`. The unchanged baseline has
110 passing process tests and one ignored opt-in test. The final focused command,
`cargo test -p laminar-db --lib --no-default-features --features
cluster,process-remote,files process_function:: -- --test-threads=1 --quiet`,
passes 117 tests with that same ignored test in 183.01 seconds including the
build. All-feature/all-target workspace Clippy passes in 28.37 seconds. Workspace
library tests pass 6,219 tests with four ignored tests in 166.79 seconds. The first
workspace run has five unchanged OAuth/Kafka mock-service failures; all pass from
the exact same connector binary in isolation and the original workspace command
then passes unchanged. No connector production code or test expectations change.
No-default-feature workspace Clippy passes in 11.15 seconds; nightly formatting
passes in 7.03 seconds. Readability passes in 10.14 seconds with the same 18 module
and 214 function exceptions.

### Hot-path validation

Host: AMD Ryzen 9 7900X, 12 cores/24 logical processors, Windows build 26200 x64.
Toolchain: Rust 1.99.0 (`b940084d7`), Cargo 1.99.0 (`5f94df478`); `object_store`
0.13.2. No dependency changes. The optimized benchmark build uses
`cargo bench -p laminar-db --bench process_function_bench --no-default-features
--features benchmark-internals --no-run --message-format=json` and finishes in
803.88 seconds. The preserved baseline executable has SHA-256
`B3F606AD68E06B55BD5AD1D8BD0B8838A4BAD992D016958E12AEFFC2DA408445`;
the current executable has SHA-256
`64B74E76E2D504DDC7DBA207175936AE5CE63369C8E1909E374121D6E3E27DAF`.

Both full process runs use 30 samples, 3-second warm-up and 7-second measurement.
The baseline saves `handoff-before`; the current run compares that baseline. Runs
take 165.16 and 161.76 seconds respectively. Criterion mean estimates follow;
confidence intervals, outliers and samples remain in the evidence.

| Case | Before (us) | Current (us) | Change |
|---|---:|---:|---:|
| Source/subscription, one row | 33.627 | 32.637 | -2.94% |
| Source/subscription, 64 distinct keys | 115.496 | 109.579 | -5.12% |
| Source/subscription, 64 rows sharing a key | 117.557 | 111.792 | -4.90% |
| Local operator, one row | 1.580 | 1.691 | +7.03% |
| Local operator, 64 distinct keys | 68.454 | 69.926 | +2.15% |
| Local operator, 64 rows sharing a key | 73.542 | 72.964 | -0.79% |
| Single-owner operator, one row | 2.773 | 2.864 | +3.31% |
| Single-owner operator, 64 distinct keys | 177.022 | 191.742 | +8.31% |
| Single-owner operator, 64 rows sharing a key | 77.762 | 80.244 | +3.19% |
| Two-owner operator, one row | 61.845 | 62.600 | +1.22% |
| Two-owner operator, 64 distinct keys | 204.280 | 213.185 | +4.36% |
| Two-owner operator, 64 rows sharing a key | 146.029 | 148.790 | +1.89% |
| Handler only, one row | 0.459 | 0.453 | -1.27% |
| Handler only, 64 rows | 30.510 | 29.486 | -3.36% |

The two first-pass means above 5% are investigated with the same immutable
binaries, same parameters and separate preserved samples. The order is baseline,
current, current, baseline; no Rust or benchmark code changes between runs.

| Case | Pair 1 before/current (us) | Change | Pair 2 before/current (us) | Change |
|---|---:|---:|---:|---:|
| Local operator, one row | 1.509 / 1.564 | +3.64% | 1.637 / 1.549 | -5.34% |
| Single-owner operator, 64 distinct keys | 184.820 / 185.581 | +0.41% | 183.278 / 184.213 | +0.51% |

All paired increases stay below 5%; the changed baseline measurements demonstrate
run variance behind the first-pass comparisons. No speculative optimization is
added. The four paired runs take 21.06, 24.16, 20.80 and 20.92 seconds.
`cargo bench -p laminar-core --bench latency_bench` with the same sample/timing
parameters measures 1.436 to 1.450 ns (+0.95%) in 14.73/12.90 seconds. These dev-host
means do not qualify cloud recovery, real multi-process network latency,
representative tail latency or target-hardware throughput.

Fresh hardware profiling on 2026-10-06 completes all three 30-second distinct-key
cases from the current executable, each with exit code zero, in 105.42 seconds
including recording and cleanup. WPR confirms the task-owned recorder is stopped.
The `Microsoft.Windows.EventTracing.Processing.All` 1.12.10 reader accepts the trace
with lost events and time inversion disallowed; analysis takes 2.44 seconds.
Weighted IPC is the sum of retired instructions divided by the sum of cycles for
each actual benchmark PID's scheduling intervals.

| Profile | PID | Retired instructions | Cycles | Intervals | Weighted IPC |
|---|---:|---:|---:|---:|---:|
| Local, 64 distinct keys | 41368 | 554,494,155,234 | 158,375,555,796 | 595 | 3.501 |
| Private single-owner, 64 distinct keys | 23268 | 522,259,723,447 | 157,050,612,042 | 437 | 3.325 |
| Private two-owner, 64 distinct keys | 29424 | 377,036,566,957 | 165,214,085,222 | 478 | 2.282 |

All three meet the IPC > 2.0 guideline. The counters include initialization and
the benchmark harness; the two-owner case uses real loopback endpoints in one
runtime. These measurements do not certify independent-process cluster load or
target-hardware tail latency. The final ETL has SHA-256
`52C02396556EB2528870A45203DD89D3BD4A2AE4906DC2F25C51E509534DCFE5`.
Its manifest binds every benchmark PID to the current executable hash above.
The capture result, strict counter analysis, recorder status, scripts and artifact
hashes remain in `target/process-handoff-20261005/`.

## 2026-10-06 — Vnode-owned callback replay qualification

Status: private replay profile implemented; public cluster admission remains
closed. This continues Phase E from committed ownership transfer.

An uninterrupted execution and restore from the same committed cut originally
produced equal output values but different callback IDs after a two-to-three-owner
rescale. The failing regression records both input and timer identities; an
owner-wide sequence cannot transfer a vnode's identity independently of its
other vnodes. The red run remains in
`target/process-replay-20261006/replay-regression-red.log`.

Private single-owner and distributed execution now retain one `u64` sequence per
vnode. An ID encodes `sequence * vnode_count + vnode` in the existing pipeline /
operator namespace. Checked arithmetic prevents wraparound. The fixed vector is
allocated before cluster binding and charged to managed-state accounting; it
adds no per-record allocation. It uses the existing canonical vnode lookup.
The sequence is captured even after every key in a vnode is cleared, so clearing
state does not reuse a callback identity or require retained key tombstones.
Acquired and revoked sequences move with the existing prepared state slots and
publish behind the existing rotation fence. Native call rejection rolls back
reserved IDs in reverse order. Remote input reserves IDs in accepted input order
before the existing key-distinct scheduler changes RPC batches; pending calls
continue to prevent checkpoint capture.

Cluster operator metadata carries activation sequencing ABI 1, and each cluster
vnode frame carries its sequence. Missing or unknown ABI, missing counters and
impossible counter values fail restore before state installation. Old private
cluster cuts lack this ABI and are rejected. Cluster selection must precede
input, restore and assignment binding; restoring local metadata first cannot
reinterpret it as cluster state. Embedded and single-node-server execution retain
their existing operator-wide IDs and byte-compatible local codec-2 frames.

Five regressions extend the existing suite. Native and real loopback Rust worker
runs compare uninterrupted execution against exact committed-cut restoration
with changed batch sizes and a two-to-three-owner rescale. Each compares eight
post-cut input callbacks, four timer callbacks, callback IDs and output rows.
Other cases cover empty-key state restoration without identity reuse, an
exhausted vnode rolling back earlier reservations before invoking the handler,
and incompatible checkpoint sequencing fields. The existing native lease-loss
test also checks that rejected calls leave all vnode sequences unchanged.

The qualified profile reproduces callback order within each vnode and its
watermark cuts. It does not establish deterministic merging of independent
source channels, ordering of arbitrary asynchronous timer rescheduling, or full
coordinated recovery with independently failed and surviving engine processes.
The committed store and engine peers in these replay fixtures are in process;
the Rust worker uses actual loopback RPC. Public registration and graph admission
continue to reject both cluster forms. No source/sink delivery certification or
stronger Python delivery is added.

### Validation

With `CARGO_BUILD_JOBS=2` and `RUST_MIN_STACK=8388608`, the final focused command
`cargo test -p laminar-db --lib --no-default-features --features
cluster,process-remote,files process_function:: -- --test-threads=1 --quiet`
passes 122 tests with one existing ignored case in 210.38 seconds including
compilation. The final `cargo test --workspace --lib` passes 1,989 connector,
1,127 core, 2,238 database and 870 SQL tests in 161.34 seconds. Both required
Clippy commands pass: all features/all targets in 82.16 seconds and no default
features in 20.16 seconds. Nightly formatting passes in 6.33 seconds;
readability passes in 9.62 seconds with the unchanged 18 module and 214 function
exception counts.

The first workspace run fails seven unchanged connector cases during concurrent
release compilation: two FILES lifecycle deadlines and five OAuth mock cases.
All seven pass individually from the exact workspace connector binary and
feature set, then the unmodified workspace command passes with compilation idle.
The first no-default Clippy attempt is denied access to Cargo's workspace
manifest by the sandbox; rerunning with workspace build access passes. Earlier
Clippy findings are fixed with a checked vnode conversion and a named transfer
slot. Earlier stale-reply comparisons incorrectly include reserved, unaccepted
IDs in an applied-state image; those comparisons are corrected, while native
rejection explicitly checks sequence rollback. No connector production behavior
is changed. Logs and exit codes, including failed attempts, are retained under
`target/process-replay-20261006/`.

### Hot-path validation

Host: AMD Ryzen 9 7900X, 12 cores/24 logical processors, Windows build 26200 x64.
Rust 1.99.0 (`b940084d7`), Cargo 1.99.0 (`5f94df478`), `object_store` 0.13.2;
no dependency changes. The optimized process benchmark build uses
`cargo bench -p laminar-db --bench process_function_bench --no-default-features
--features benchmark-internals --no-run --message-format=json` and finishes in
807.59 seconds. The preserved baseline executable has SHA-256
`64B74E76E2D504DDC7DBA207175936AE5CE63369C8E1909E374121D6E3E27DAF`;
the final executable has SHA-256
`A2AA6ABF7CF40B676A0986EB16221A0361C4E1DCBB80787799738805713236E9`.

Both process runs use 30 samples, 3-second warm-up and 7-second measurement.
The baseline saves `replay-before`; the final run compares that baseline. Runs
take 161.86 and 157.14 seconds. Criterion mean estimates follow; these are dev-host
measurements, not target-hardware latency or independent-process cluster load.

| Case | Before (us) | Final (us) | Change |
|---|---:|---:|---:|
| Source/subscription, one row | 30.455 | 29.723 | -2.40% |
| Source/subscription, 64 distinct keys | 105.455 | 107.666 | +2.10% |
| Source/subscription, 64 rows sharing a key | 168.472 | 107.968 | -35.91% |
| Local operator, one row | 1.645 | 1.617 | -1.72% |
| Local operator, 64 distinct keys | 80.499 | 66.966 | -16.81% |
| Local operator, 64 rows sharing a key | 78.259 | 69.703 | -10.93% |
| Single-owner operator, one row | 3.092 | 2.868 | -7.23% |
| Single-owner operator, 64 distinct keys | 198.801 | 179.868 | -9.52% |
| Single-owner operator, 64 rows sharing a key | 84.142 | 80.509 | -4.32% |
| Two-owner operator, one row | 62.765 | 61.870 | -1.43% |
| Two-owner operator, 64 distinct keys | 203.495 | 195.585 | -3.89% |
| Two-owner operator, 64 rows sharing a key | 147.135 | 147.237 | +0.07% |
| Handler only, one row | 0.426 | 0.424 | -0.41% |
| Handler only, 64 rows | 29.587 | 31.884 | +7.76% |

The sole first-pass increase above 5% is the unchanged handler-only 64-row case.
Paired runs preserve the same binaries and parameters, ordered baseline, final,
final, baseline. Pair 1 measures 28.097 / 29.331 us (+4.39%); pair 2 measures
32.388 / 31.491 us (-2.77%). Both paired increases stay below 5%. The changed
baseline measurements show run variance; no Rust or benchmark code changes
between trials and no speculative optimization is added. Trials take 11.08,
11.17, 11.36 and 11.52 seconds. The first baseline run overlaps startup of the
task's MinIO test container; that container is removed before final measurements.
The broad apparent improvements are not claimed as optimizations from this fix.

The required core `latency_bench` uses the same sample/timing parameters. Runs
take 13.39 and 12.28 seconds; the final mean is 2.06% below the baseline. Failed
attempts, final source hashes, binary hashes, timings and comparison estimates
remain in `target/process-replay-20261006/`.

Fresh hardware profiling completes all three 30-second distinct-key cases from
the final executable, each with exit code zero, in 105.59 seconds including
recording and cleanup. WPR confirms the task-owned recorder is stopped. The
`Microsoft.Windows.EventTracing.Processing.All` 1.12.10 reader accepts the trace
with lost events and time inversion disallowed; analysis takes 1.86 seconds.
Weighted IPC is the sum of retired instructions divided by the sum of cycles
for each actual benchmark PID's scheduling intervals.

| Profile | PID | Retired instructions | Cycles | Intervals | Weighted IPC |
|---|---:|---:|---:|---:|---:|
| Local, 64 distinct keys | 56084 | 552,267,839,501 | 157,341,723,761 | 648 | 3.510 |
| Private single-owner, 64 distinct keys | 52996 | 520,410,325,321 | 156,465,509,386 | 418 | 3.326 |
| Private two-owner, 64 distinct keys | 53872 | 361,976,616,319 | 155,543,144,588 | 473 | 2.327 |

All three meet the IPC > 2.0 guideline. Counters include initialization and the
benchmark harness; the two-owner case uses real loopback endpoints within one
runtime. This does not qualify independent-process cluster load or target-hardware
tail latency. The ETL has SHA-256
`3C499CB86C1AC6E8C7C64C1870AC31073A762572863FC0CA43063DDCA1CD4AD5`.
The manifest binds each benchmark PID to the final executable hash above.
Capture results, strict counter analysis, recorder status and artifact hashes
remain in `target/process-replay-20261006/`.

## 2026-10-06 — Independent graph-owner recovery over shared S3 checkpoints

Status: completed and validated. Public cluster
registration and graph admission remain closed. This is the next Phase E fault
qualification increment from `06802e96`.

An ignored integration case extends the existing private process-graph fixtures
with independent owner processes and a live loopback MinIO store. It exercises
native Rust and the actual loopback Rust worker separately. Each fault starts in
a fresh UUID namespace with a committed cut bound to its own predecessor
assignment. Owners align actual shuffle barriers and save their own participant
manifests and state objects. The parent publishes the index through the existing
checkpoint decision and leader authority stores. Restoration uses the existing
`RecoveryManager`, donor range/digest validation, startup binding and shuffle
execution fences. No durability backend, scheduler, dependency or production
runtime code is added.

The three scenarios cover replacement of a killed owner while another owner
survives and renews its lease, entry of a third owner with rescaled vnode
ownership, graceful exit to one surviving owner, and loss of the sole owner
before its replacement is ready. Replacement uses a full-TTL observation and the
real shared process-lease takeover, with a new boot incarnation and process term.
The sole-owner case publishes its predecessor-bound handoff before killing the
old owner. All scenarios first apply additional uncommitted work, restore the
selected committed cut, change input batches from four rows to one, and compare
eight input callbacks, four timer callbacks, callback IDs and output rows against
uninterrupted execution. The surviving graph rejects an attempted activation
after its execution authority changes.

Every scenario then damages a selected committed donor object. Restoration must
report the range/length error, retain the same committed reference and leave no
restored graph; it cannot select another checkpoint. Fixture control messages
are capped at 1 MiB, callback/output inventories at 64, command inventory at
4,096, and each response/lifecycle wait at 20 seconds. Each fault has a 90-second
parent deadline and each child a 120-second lifetime. Cleanup owns the exact
spawned child handles, joins graceful exits, and kills/joins survivors after
failure while preserving the primary failure and cleanup diagnostics.

These are graph lifecycle tests driven by a test parent. Their source positions
are controlled fixture cursors, and their per-owner recovery KV is the existing
in-memory fixture. They do not exercise the database-owned `RecoveryMonitor`
Prepare/Start/Release intake gates, server durable recovery KV, connector/sink
certification, autonomous final-owner drain, arbitrary timer rescheduling or a
deterministic merge of independent source channels. The remote Rust worker runs
inside its owner process. Separate worker-host placement and target-hardware
latency are not qualified here. Both public cluster forms continue to reject
process-function registration and startup.

The local service uses the repository's pinned MinIO fixture image
`laminardb-minio-test:2024-10-13` (image SHA-256
`03c59f175c68c3543d35c9df483ae49aa9d187eff096a54db07268b12e9c4180`), bound
only to `127.0.0.1:19010`. The dedicated bucket is `process-peers`. With that
fixture running, the executable command is:

```powershell
$env:CARGO_BUILD_JOBS='2'
$env:RUST_MIN_STACK='8388608'
$env:LAMINAR_PROCESS_TEST_S3_ENDPOINT='http://127.0.0.1:19010'
$env:LAMINAR_PROCESS_TEST_S3_BUCKET='process-peers'
cargo test -p laminar-db --lib --no-default-features --features cluster,process-remote,files process_function::operator::execution::tests::shuffle::committed::peers::independent_owners_restore_the_committed_cut_after_host_loss_and_rescale -- --exact --ignored --nocapture
```

The explicit ignored test passes all six runtime/fault combinations in 45.47
seconds; the command takes 265.51 seconds including compilation. The qualified
`laminar_db-0dc6c3690381aaf6.exe` has SHA-256
`2AE28E21C6A30CA0F7786FAB151AD68681DA0E1DA8767B43371D5ECE3A2D552B`.
The run starts 16 owner processes in six independent namespaces. All spawned
owners are joined, including the four deliberately killed owners. The owned
MinIO container is removed after its exact ID and task label are checked;
existing Docker services are left running. Binary/source hashes, commands,
timings, failed attempts and cleanup evidence remain in
`target/process-peers-20261006/`.

The only later Rust edit adds `#[cfg(feature = "process-remote")]` to the existing
remote-only takeover helper, removing its unused-code warning in the native
feature set. Its body and all qualified runtime paths are unchanged. The
qualification source snapshot is preserved separately from the final source
hashes. Only test code changes; before/after hot-path benchmarks and new IPC
capture are not required for this increment.

### Validation

The baseline remote-enabled focused suite passes 122 tests, with one ignored,
in 34.09 seconds. After this increment it passes 122 tests, with two ignored, in
34.66 seconds. The native-only final build passes 63 tests, with two ignored, in
108.47 seconds including compilation (7.75 seconds of tests), without the
takeover helper's unused-code warning.

The first workspace run fails in one mocked Kafka startup test and four Iceberg
OAuth tests. All five pass in isolation from that exact
`laminar_connectors-143397847506616c.exe` binary (SHA-256
`D2E5733E0E94E30E0819FA5A67DC5465363BE91459458714CE724E64ADA3A4C6`).
The unchanged workspace command then passes with compilation idle. No connector
source, feature selection or test behavior is changed. The failed run, isolated
commands and successful retry remain in the evidence directory.

All required gates pass with `CARGO_BUILD_JOBS=2` and `RUST_MIN_STACK=8388608`:

| Gate | Result | Command seconds |
|---|---|---:|
| `cargo test --workspace --lib` | 1,989 connector, 1,127 core, 2,238 database and 870 SQL tests pass; five ignored | 164.07 |
| `cargo clippy --workspace --all-features --all-targets -- -D warnings` | Pass | 37.37 |
| `cargo clippy --workspace --no-default-features -- -D warnings` | Pass | 9.66 |
| `cargo +nightly fmt --all -- --check` | Pass | 8.44 |
| `cargo run --quiet --manifest-path tools/readability-check/Cargo.toml -- .` | Pass; 18 module and 214 function exceptions unchanged | 11.21 |

Earlier fixture attempts expose an already-installed assignment-version update
and an invalid chained handoff proof. The final fixture accepts an identical
installed assignment and gives each fault a fresh namespace and a committed cut
bound to that fault's actual predecessor. The existing canonical authority checks
continue to reject the invalid chained proof. Compile and Clippy findings are
resolved before the six-case run. The readability baselines and lockfile are
unchanged.

## 2026-10-06 — Database recovery rounds and controlled source order

Status: completed and validated. Public cluster process-function
registration and graph admission remain closed. This continues original Phase E
from `57766088` without changing production runtime behavior.

An ignored server integration test runs two actual `LaminarDB` instances and their
database-owned `RecoveryMonitor` tasks against live loopback MinIO. It uses the
server's `ObjectStoreClusterKv`, renewable process and leader leases, verified
shared namespaces, sealed catalog bootstrap, certified assignment and actual
shuffle mesh. Its admitted projection pipeline has an empty source that exposes
bounded Start holds and poll counters. Recovery selects GENESIS (epoch 0); the
test does not insert a process function into a publicly rejected cluster graph.

The first held Start keeps both intake gates closed and poll counters unchanged
after the stopped quorum. Releasing that hold allows the matching durable
`ReleaseCommitted` and then fresh polling. A second fault rejects the old round's
Release. With that Start held, the leader manager withdraws its grant, waits its
full six-second TTL and acquires a new fencing proof. The old Start cannot Release;
the database monitors retain its control and finish a later generation under the
new proof. The durable generation agrees with the committed Release and both
intake gates reopen. Process leases use a separate 60-second TTL. Cleanup releases
all source holds before shutting either database down, then cancels and joins the
fixture-owned renewal tasks while preserving primary and cleanup failures.
A focused failure case verifies that cleanup reports an already-completed failed
task without polling its consumed join result again; only timed-out tasks are
aborted and subsequently joined.

Companion private graph tests restore a committed cut from the existing in-memory
shared object-store fixture, rescale from two owners to three and replay one fixed
source order using six-row and one-row batches. Native Rust and the actual loopback
Rust worker must reproduce 14 independently specified callback identities and
output rows. The cases replace and cancel timers, then register one subsequent
timer from each live timer callback. A repeated key's timestamp moves backward
above the accepted watermark, proving arrival order rather than event-time sorting.
Explicit watermark cuts remain unchanged. A negative permutation case produces
equal independent-key output but different vnode callback IDs: ordered positions
within partitions do not certify a deterministic merge of independent channels.
Rustdoc and the example describe this boundary.

These are separate control and state-replay qualifications. The database case
uses two instances in one OS process, an empty source and GENESIS; the stateful
case uses private graph hooks and fixture source positions. Together they do not
qualify process-function restoration through database recovery from a committed
cut, independent database process failures, autonomous final-owner drain,
connector/sink delivery or a certified source-channel merge. Embedded and
single-node server runtime behavior is unchanged; public cluster admission stays
closed. Only tests, rustdoc and example documentation change, so new hot-path
benchmarks and IPC capture are not required.

The initial live control run passes in 20.43 seconds (43.99 seconds including compilation)
using the pinned `laminardb-minio-test:2024-10-13` image on `127.0.0.1:19010`, bucket
`process-rounds`, and a fresh UUID namespace. With that local fixture running:

```powershell
$env:CARGO_BUILD_JOBS='2'
$env:RUST_MIN_STACK='8388608'
$env:LAMINAR_PROCESS_TEST_S3_ENDPOINT='http://127.0.0.1:19010'
$env:LAMINAR_PROCESS_TEST_S3_BUCKET='process-rounds'
cargo test -p laminar-server --bin laminardb --no-default-features --features cluster,aws cluster::recovery_round_tests::database_rounds_hold_intake_until_exact_durable_release -- --exact --ignored --nocapture
```

### Validation

The final remote-enabled stateful suite passes 125 tests with two ignored in
39.59 seconds (178.29 seconds including compilation); the native-only suite passes
65 with two ignored in 5.46 seconds (143.46 seconds including compilation). The
final cluster suite explicitly includes the live ignored case and passes all 54
tests, including cleanup failure, in 44.07 seconds (126.04 seconds including
compilation). Its final `laminardb-5e7a97b809aa37fa.exe` SHA-256 is
`4D212B773A1D80E83A74FA92698EA30627084FF6968066EFA9F4519346F9AC6D`.

All required gates pass with `CARGO_BUILD_JOBS=2` and `RUST_MIN_STACK=8388608`:

| Gate | Result | Command seconds |
|---|---|---:|
| `cargo test --workspace --lib` | 1,989 connector, 1,127 core, 2,241 database and 870 SQL tests pass; five ignored | 163.60 |
| `cargo clippy --workspace --all-features --all-targets -- -D warnings` | Pass after the cleanup correction | 10.55 |
| `cargo clippy --workspace --no-default-features -- -D warnings` | Pass | 20.28 |
| `cargo +nightly fmt --all -- --check` | Pass | 5.07 |
| `cargo run --quiet --manifest-path tools/readability-check/Cargo.toml -- .` | Pass; 18 module and 214 function exceptions unchanged | 9.60 |

The first workspace run fails in the same five Kafka/OAuth mock cases recorded
at the previous commit. All five pass separately from that exact connector binary
(SHA-256 `D2E5733E0E94E30E0819FA5A67DC5465363BE91459458714CE724E64ADA3A4C6`),
then the unchanged workspace command passes. No connector code, feature selection
or workspace test behavior changes. The lockfile and readability baselines remain
unchanged.

Commands, timings, source/binary hashes and failed fixture attempts are retained in
`target/process-rounds-20261006/`. The early failures preserve the assignment,
catalog, source-placement and lifecycle checks. The fixture now initializes
checkpointing through normal startup, accounts for Prepare's permitted shutdown
tail poll, and observes the real leader TTL. An unchanged watermark initially
allowed the native source-order fixture to miss a delayed peer batch; it now waits
for every expected input output before advancing timers. No runtime admission or
authority check is relaxed to make these cases pass.
Two shortened exact-name attempts execute no tests and are excluded from the
qualification results. The final suites execute the actual named cases. The owned
MinIO container is removed after checking its exact ID and task label; it has no
host mounts, and existing Docker services are left running.

## 2026-10-06 — Independent database recovery from a committed shared cut

Status: completed and validated. This continues
original Phase E from `0f3b841d`. Public cluster process-function registration and
graph admission remain closed. Production runtime code is unchanged.

An ignored server test starts two independent database processes from the actual
cluster test binary. Each owns its database, recovery monitor, renewable leases,
leased control RPC endpoint and loopback shuffle transport. It reuses the existing
server test fixture for namespace proof, catalog bootstrap and assignment setup,
and the server's real `ObjectStoreClusterKv` over loopback MinIO. The existing
GENESIS suite moves into a module family so these resources are shared without
duplicating a control backend.

The admitted SQL pipeline is a direct-source keyed aggregate. Two bounded input
partitions carry deterministic row positions, explicit watermarks and independent
cursors. Initial input crosses the shuffle mesh and updates one key on each vnode.
The database commits checkpoint 1 with totals 32 and 30, two participant manifests
and both source cursors at 2. Further input reaches totals 63 and 60 without another
checkpoint. The parent forcibly kills the second process, then supplies the
survivor's controlled discovery watch with the remaining member. The production
snapshot watcher and rebalance controller authorize assignment 2 with both vnodes
owned by the survivor; the database monitor selects the committed cut, restores
both source cursors and holds Start behind closed intake.

The held Start checks the exact handoff reference, including its index digest and
length, the new owner fence, both restored cursors and the retained prior Release.
Making the remaining input available while Start is held leaves poll counts and
output unchanged. Only the matching durable `ReleaseCommitted` opens intake.
Replaying the uncommitted suffix and one further row per partition then produces
totals 104 and 100, demonstrating donor-state continuity and rewind of the
survivor's uncommitted state. The observation sink appends and syncs a bounded
local log before acknowledging each batch, satisfying its at-least-once fixture
contract. It does not certify an external connector or exactly-once delivery.

Commands and messages have explicit byte/count/deadline bounds. Child cleanup
releases the source hold, cancels and joins the existing rebalance tasks, shuts
down the database and joins lease renewal. The parent joins normal exits and the
intentional forced exit, preserving primary and cleanup errors. A failure in the
older same-process GENESIS fixture exposed a teardown transport race; both
databases now receive close intent before either shared-client runtime retires.
Initial fixture attempts also preserve the sink-durability and reserved-channel
checks rather than relaxing runtime admission. The older phase wait is now 90
seconds: its previous 40-second bound excluded the monitor's existing 60-second
orphan-Start retry after leader-proof replacement. The final suite completes that
slower production path without injecting another test fault or clearing retained control.

The first successful independent case completes in 19.06 seconds (66.60 seconds
including compilation) with PIDs 102460 and 102444, committed epoch 1 and recovery
generation 2. The final suite below also checks the exact handoff digest added
during review. With the pinned local MinIO image running on `127.0.0.1:19010` and
bucket `process-db-recovery`:

```powershell
$env:CARGO_BUILD_JOBS='2'
$env:RUST_MIN_STACK='8388608'
$env:LAMINAR_PROCESS_TEST_S3_ENDPOINT='http://127.0.0.1:19010'
$env:LAMINAR_PROCESS_TEST_S3_BUCKET='process-db-recovery'
cargo test -p laminar-server --bin laminardb --no-default-features --features cluster,aws cluster::recovery_round_tests::committed::surviving_database_recovers_lost_vnodes_and_replays_the_committed_source_cut -- --exact --ignored --nocapture
```

This is cluster aggregate/control qualification with an injected discovery
snapshot after a real OS-process kill. It does not certify gossip failure
detection, process-function database restore, independent-channel callback order,
external sink delivery, replacement of the last owner or autonomous final-owner
drain. Embedded and single-node server behavior is unchanged. No hot-path
production bodies change, so new Criterion and IPC capture are not required.
Evidence is retained in `target/process-db-recovery-20261006/`.

### Validation

The final cluster server suite includes both live ignored cases and passes all
55 tests in 105.41 seconds (128.81 seconds including compilation). The final
`laminardb-5e7a97b809aa37fa.exe` SHA-256 is
`6C199BE420D7CB2A1844D15C4C552A658BB13BB164F5E1AB21D4D62B450DB327`.

All required gates pass with `CARGO_BUILD_JOBS=2` and `RUST_MIN_STACK=8388608`:

| Gate | Result | Command seconds |
|---|---|---:|
| `cargo test --workspace --lib` | 1,989 connector, 1,127 core, 2,241 database and 870 SQL tests pass; five ignored | 162.65 |
| `cargo clippy --workspace --all-features --all-targets -- -D warnings` | Pass | 26.75 |
| `cargo clippy --workspace --no-default-features -- -D warnings` | Pass | 1.14 |
| `cargo +nightly fmt --all -- --check` | Pass | 5.20 |
| `cargo run --quiet --manifest-path tools/readability-check/Cargo.toml -- .` | Pass; 18 module and 214 function exceptions unchanged | 9.82 |

The workspace suite passes on its first run; no connector retry is needed. Earlier
server runs retain the failed fixture attempts, the teardown race and the phase
timeout before the lifecycle-bound correction. No dependency, lockfile,
readability baseline, production runtime body or admission check changes. The
source/binary hashes, commands, durations and owned-fixture identity are retained
with the logs. The owned MinIO container is removed after verifying its exact ID,
task label and absence of mounts; existing Docker services remain running.

## 2026-10-06 — Source-order contract integration

Status: completed and validated. This continues original Phase E from `3812b721` on
`codex/stateful-process-functions`.

`SourceContract` now separates `SourceReplayOrder` from per-partition row
positions. The default is `Unspecified`; `SingleChannel` requires the same
ordered suffix, including different keys, independent of poll timing and size,
with a retained physical channel identity. It does not certify watermark cuts
or timer replay. Local native and remote Rust `AtLeastOnce` process registration
requires this declaration on a replayable, append-only singleton source.
Registration checks before reserving the output; startup checks its immutable
registration snapshot before source I/O, then checks the instantiated source.

Checkpoint identity includes a declared source order. Undeclared sources retain
their previous canonical bytes. Contract identity inspection now supplies the
same Arrow schema as startup, including when the declaration depends on that
schema. The schema remains structurally fingerprinted; its injected encoding
does not become a raw identity option. A regression test covers both declarations
and unchanged raw options.

Built-in connectors remain undeclared. FILES retains processed paths and partial
file cursors, but not the discovery order of an uncommitted suffix. Its four
native/remote host-loss cases now use `BestEffort` with checkpoints and retain
every original cursor, callback-ID, state and duplicate-output assertion. A new
FILES admission test proves that its replayability alone does not qualify it.

The focused tests cover rejection without output-name reservation or source
startup, startup revalidation, instantiated-source validation, best-effort
admission and the same native/remote rule. A timer-free native case commits total
60 and source cursor 1, faults on the next invocation, restores the committed
state and cursor, then replays callback ID 1 to produce total 110.

Final validation used `CARGO_BUILD_JOBS=2`, `RUST_MIN_STACK=8388608`, rustc
1.99.0 (`b940084d7`) and cargo 1.99.0 (`5f94df478`). Command times include
compilation. All rows below ran after the schema inspection fix.

| Command | Result | Seconds |
| --- | --- | ---: |
| `cargo test -p laminar-db --lib --no-default-features --features cluster,process-remote,files process_function:: -- --quiet` | 132 passed, 2 ignored | 10.21 |
| `cargo test -p laminar-db --lib --no-default-features process_function:: -- --quiet` | 31 passed, 1 ignored | 84.52 |
| `cargo test -p laminar-db --lib --no-default-features --features cluster,process-remote,files pipeline_identity::tests:: -- --quiet` | 10 passed | 148.62 |
| `cargo test --workspace --lib` | 6236 passed, 5 ignored | 393.79 |
| `cargo clippy --workspace --all-features --all-targets -- -D warnings` | passed | 49.60 |
| `cargo clippy --workspace --no-default-features -- -D warnings` | passed | 9.83 |
| `cargo +nightly fmt --all -- --check` | passed | 6.89 |
| `cargo run --quiet --manifest-path tools/readability-check/Cargo.toml -- .` | passed; 18 module and 214 function exceptions unchanged | 9.96 |

The unchanged baseline passed 125 remote-enabled process tests with two ignored.
The first new-test compilation failed on fixture field names and a trait import;
both were corrected. Final review then aligned schema-dependent identity and
reran every affected suite and required gate. Minimal-feature tests retain the
existing unused `ExternalOutputPressure` methods warning; the workspace build
retains the OpenSSL PDB and `proc-macro-error2` future-compatibility warnings.
Neither Clippy gate reports a warning. Cargo.lock and readability baselines are
unchanged.

Final test binary SHA-256:

- Remote-enabled `laminar_db-0dc6c3690381aaf6.exe`:
  `9C0799FEA4F3FB40CD555711472A0E0DCCB5061CDB4C7B62C4AC89278CF06810`.
- Native-only `laminar_db-1c55abe7ac99f2ac.exe`:
  `59FEEE7D0E02EDA2BD84BDD2D97AEC35624A85680B52E1C731B6ABF84F819CFB`.

Both public cluster process-function paths remain closed. The admitted
single-channel/singleton profile applies to local execution, including embedded
use. Cluster's splittable placement and reproducible input/watermark cuts still
need qualification before database-owned process recovery can be admitted.
Only cold admission/identity code and tests change; no coordinator cycle,
operator record path, scheduler, state backend or dependency changes. Criterion
and IPC capture are not required for this increment. Exact command logs, times,
source and binary hashes are retained in `target/process-source-order-20261006/`.

## 2026-10-06 — Splittable source ordering and matching watermark cuts

Status: completed and validated.
This continues original Phase E from `54b20be1` on
`codex/stateful-process-functions`.

`SourceReplayOrder::SingleChannelFixedBatches` requires identical ordered rows
in identical nonempty replay batches after a committed cursor, independent of
poll limits and timing. The bounded profile requires replayability, append-only
input and deterministic row positions. It permits singleton or splittable
placement with one logical source and one global physical input channel. Raw
`SingleChannel` order no longer qualifies at-least-once process registration.
The declaration is bound into checkpoint identity; undeclared source identity
bytes remain unchanged. No built-in connector advertises the new profile.

Cold startup configuration reuses the existing FIFO and cycle budgets to execute
each fixed batch before its successor, overriding configured coalescing and
source drain. Event-time cuts use the existing bounded-out-of-orderness
generator. External watermark calls, clock-driven idleness and the future-skew
guard cannot alter those cuts. Timers require subsequent input to advance event
time. The source watermark owner moves from `db/mod.rs` into its own leaf module,
with its five existing private tests; the record observation loop is unchanged.
Its physical identity remains bound through an empty owned inventory, and
changed identities or multiple-channel cuts fail closed. This local invariant
does not certify a global distributed source inventory or cluster recovery.

Native and real loopback Rust worker tests compare an uninterrupted reference
with a committed-cut restore after an uncommitted callback failure. They verify
the complete callback IDs, their assigned logical order, state, timer replacement
and timer output, plus source cursor rewind. Different-key remote calls remain
concurrent; callback transcripts are compared by engine ID and output by key,
preserving emitted order within each key. Global worker arrival or cross-key
output order is not promised. Replay uses poll target 1 instead of 1024,
with configured coalescing and idleness and an external watermark call unable to
change the cut. Another case accepts an atomic two-row replay batch above the
poll target. Independent logical sources fail before source startup.

The new Criterion case exercises the actual at-least-once database path with a
splittable-declared connector, 64 distinct keys, fixed replay batches and poll
target 1. Existing local/one-owner/two-owner operator and end-to-end cases have a
before-change baseline. No scheduler, state backend, dependency, checkpoint
format or public cluster process-function admission is added.

### Validation

Final validation uses `CARGO_BUILD_JOBS=2`, `RUST_MIN_STACK=8388608`, rustc
1.99.0 (`b940084d7`) and cargo 1.99.0 (`5f94df478`). Command times include
compilation.

| Command | Result | Seconds |
| --- | --- | ---: |
| `cargo test --workspace --lib` | 6,243 passed, five ignored | 163.77 |
| `cargo clippy --workspace --all-features --all-targets -- -D warnings` | Passed after final fixture edits | 5.67 |
| `cargo clippy --workspace --no-default-features -- -D warnings` | Passed | 58.05 |
| `cargo +nightly fmt --all -- --check` | Passed | 6.79 |
| `cargo run --quiet --manifest-path tools/readability-check/Cargo.toml -- .` | Passed; 18 module and 214 function exceptions unchanged | 9.91 |

The final workspace binary runs all 11 source-order cases again: 11 passed,
none ignored, 4.55 command seconds. The exact remote matching-cut case passes
three further runs, each executing one test, in 3.62, 3.59 and 3.60 seconds.
Its SHA-256 (`laminar_db-a63677b7d4fa08f4.exe`) is
`4EE6DE6CE5B894DE6392A6CAE4FC43DC22A0E36E7E3B0D5AF37619785FE0FB1A`.
Before the final fixture comparison normalization, the production-final
remote-enabled suite passes 136 tests with two ignored, and the minimal-feature
native suite passes 34 with one ignored. Identity and watermark suites pass
10 and eight tests respectively; the final workspace reruns those cases.

Failed attempts remain in the evidence. Initial compilation corrected a row
position constructor and kept private watermark tests with their owner. The
first complete process suite exposed error precedence for a direct source and
an incorrect assumption that concurrent worker arrival order matched engine
callback IDs; both are corrected. Two existing FILES host-loss cases pass in
isolation after missing their deadline under the parallel suite. The focused
suite then passes with one test thread. The first workspace attempt fails only
the native Kafka mock metadata timeout; that exact case passes on the same
binary in isolation (4.91 seconds), followed by a passing unadjusted complete
workspace retry. No connector behavior, timeout or admission is relaxed.

The existing minimal-test unused `ExternalOutputPressure` methods warning,
OpenSSL PDB linker warning and `proc-macro-error2` future-compatibility warning
remain. Both Clippy gates pass without warnings. Cargo.lock and readability
baselines are unchanged. Exact logs, commands, durations, source/binary hashes
and performance evidence are retained in `target/process-replay-cuts-20261006/`.

### Latency and hardware profile

Before and after measurements use the same optimized feature set
`benchmark-internals,cluster`, with default features disabled, 30 Criterion
samples, three seconds warmup and seven seconds measurement. The baseline is
the frozen `54b20be1` executable. Its build and run take 819.12 seconds; the final
optimized rebuild takes 788.86 seconds, the 14-case comparison 154.31 seconds,
and the new fixed-cut case 12.73 seconds. Core `latency_bench` runs before and
after in 11.87 and 12.07 seconds. Measurements run without other task builds or
tests. These are development-hardware means, not representative tail latency.

The first comparison reports three means above the 5% gate. All raw results
remain available. The frozen old one-row executable also slows from 25.44 to
31.30 microseconds when remeasured; its sequential pair with the final executable
is +3.69%. The local same-key pair is -5.05%. The remaining single-owner
64-distinct-key fixture executes no source-watermark code, and its operator body
is unchanged. A 100-sample pair on logical CPU 4 reports +5.73%; reversing the
same pinned run order reports -0.76%. Equal-weight means across both orders are
185.18 versus 189.67 microseconds (+2.43%). This resolves the remaining unstable
comparison without changing production code or machine-wide settings.

| Measurement | Mean / assessed change |
| --- | --- |
| Maximum remaining existing-case increase | +4.75%; all assessed means within the 5% gate |
| Core tumbling assignment | 1.4373 to 1.4422 ns; +0.34% |
| New fixed 64-row cut, poll target 1 | 119.44 microseconds per batch; 95% mean CI 115.98–124.12 |
| Actual coordinator thread IPC | 2.1508 |
| Whole-process IPC, including source I/O and consumer | 1.8178 |

Hardware counters use the final executable SHA-256
`14A5078E9903B8DA4DDEAB4BCDE3681278E66D3212EDDA480B3CAD3B45AC7CA2`.
The task-owned WPR capture completes in 62.88 seconds with successful cleanup.
The strict `Microsoft.Windows.EventTracing.Processing.All` 1.12.10 reader rejects
lost events and time inversion; it reads 419,821,551,326 instructions and
230,947,771,813 cycles from owned PID 143968. Thread attribution on that same
trace identifies the active `laminar-compute` thread 144088: 364,996,212,372
instructions and 169,702,484,467 cycles. Its IPC exceeds the hot-path rule of
thumb. The lower whole-process ratio includes source polling and subscription
consumption. The other three compute threads belong to filtered fixture startup
and contribute fewer than 1.24 million cycles each.

The thread reader extends the existing utility using its exact locked local
packages; it builds without warnings and changes no repository dependency.
The trace SHA-256 is
`799ABFD31B37B85CAAAF31A595A144D4545B4AB86D8F9FEBCDAD7F697D13A4F7`.
Qualified Rust source and lockfile hashes still match after profiling. Public
cluster process-function admission remains closed, and target-hardware workload
and tail-latency qualification remain pending.

### Continuation: database-controlled cluster process recovery (2026-10-06)

This Phase E increment connects private process graphs to the existing database
startup, shared checkpoint and coordinated Prepare/Start/Release lifecycle.
Four unignored cases cover native Rust and the real loopback Rust worker with
one owner of two vnodes and two owners of one vnode each. No public cluster
registration or subscription admission changes. Private qualification fields
and capability overrides are compiled only for tests and default to rejection.
Embedded and single-node behavior and the production record path are unchanged.

Each case compares an uninterrupted reference with recovery from checkpoint 1
after two uncommitted state mutations and an injected indeterminate apply
outcome. The database restores cursor 2, the deployment/pipeline binding,
assignment, keyed state and timer state. Source starts are held while the
test checks that intake, polling, callbacks and output remain closed, and that
the exact selected checkpoint remains recovery authority. Opening the held
starts produces the matching committed Release before intake resumes.

The fixture declares the existing splittable fixed-batch replay contract. One
global physical channel follows vnode zero; the other owner reports an empty
physical inventory. Its participant-local idle checkpoint marker is checked
separately from the physical channel. Both runs commit the same source-decision
cuts: cursor 2/watermark 104, then cursor 7/watermark 164 (milliseconds).
The second database checkpoint supplies the durable cut needed for pending
timers. No watermark or recovery control is injected directly.

Qualification compares all nine suffix callback IDs, keys, event times,
callback kinds and state views, and independently specified output totals,
threshold changes and timer timestamps. It checks that the replaced timer at
110,000 microseconds does not fire. Callback transcripts are ordered by engine
ID; output is grouped by key with its within-key order preserved. Independent
worker arrival and cross-key output order remain concurrent.

The controllers, database compute runtimes, leased barrier RPC and shuffle
transport execute normally. Shared object storage and control KV are in memory,
leases have bounded fixture deadlines, and all database owners remain in one
OS process under the same assignment. This qualifies a recoverable compute
fault; it does not qualify OS process loss, durable control restart, lease
renewal or ownership change through the database. Existing graph-level process
transfer tests and the database aggregate node-loss prerequisite remain separate.
Private owner-local output observation makes no sink-delivery or distributed
subscription guarantee. Cleanup is bounded and preserves the primary failure.

Initial attempts exposed fixture errors in public subscription admission,
shuffle assignment installation, SQL watermark interval units and empty-owner
channel counting. Waiting for timers before committing the source decision also
stalled as expected. A generic native handler error correctly became terminal;
the final injected error uses the existing typed indeterminate-apply variant to
exercise recoverable rounds. Production failure classification is unchanged.
Clippy then required descriptive account variable names and inclusion of the
test field in the existing Debug formatter. Failed commands remain in the
evidence alongside the final results.

### Validation

Validation uses `CARGO_BUILD_JOBS=2`, `RUST_MIN_STACK=8388608`, rustc 1.99.0
(`b940084d7`) and cargo 1.99.0 (`5f94df478`). The baseline is `ac2798a3`.
Before editing, the exact `cluster,process-remote,files` feature set passes
61 coordinated-recovery tests (141.00 command seconds) and 136 process tests
with two ignored (35.55 seconds), each with one test thread.

Before the final naming/Debug edits, the four new cases pass in 49.22 command
seconds, including 25.28 seconds of compilation and 22.94 seconds of test
execution. The full process suite passes 140 tests with two ignored in 59.54
seconds; the minimal native suite passes 34 with one ignored in 81.59 seconds.
Their exact commands are:

```text
cargo test -p laminar-db --lib --no-default-features --features cluster,process-remote,files process_function::cluster_recovery_tests:: -- --quiet --test-threads=1
cargo test -p laminar-db --lib --no-default-features --features cluster,process-remote,files process_function:: -- --quiet --test-threads=1
cargo test -p laminar-db --lib --no-default-features process_function:: -- --quiet --test-threads=1
```

The final workspace suite reruns all four new cases successfully with the
corrected fixture names and Debug output. Final repository gates use the same
environment above; times include compilation.

| Command | Result | Seconds |
| --- | --- | ---: |
| `cargo test --workspace --lib` | 6,247 passed, five ignored | 292.57 |
| `cargo clippy --workspace --all-features --all-targets -- -D warnings` | Passed | 79.19 |
| `cargo clippy --workspace --no-default-features -- -D warnings` | Passed | 19.72 |
| `cargo +nightly fmt --all -- --check` | Passed | 6.27 |
| `cargo run --quiet --manifest-path tools/readability-check/Cargo.toml -- .` | Passed; 18 module and 214 function exceptions unchanged | 13.17 |

The earlier workspace pass takes 452.96 seconds with the same counts. The
minimal-test unused `ExternalOutputPressure` methods warning, OpenSSL PDB linker
warning and `proc-macro-error2` future-compatibility warning remain as previously
recorded. Both Clippy gates pass with warnings denied. No dependencies,
checkpoint formats, readability baselines, coordinator-cycle or core-operator
code change. Criterion and IPC requalification is unnecessary for qualification
code and cold Debug formatting. Exact commands, logs, durations and qualified
source/binary hashes are retained in `target/process-database-recovery-20261006/`.

## Next executable task (2026-10-06; completed below)

Continue original Phase E with database-controlled process recovery after real
node/process loss, using durable shared checkpoint/control authority and the
fixed-batch global physical channel profile. Reuse the existing process-loss and
assignment infrastructure; join the independently tested process vnode transfer
and database recovery lifecycle without adding a second scheduler or state
backend. Verify restored ownership, source inventory and matching source-decision
cuts before publication and replay.

Fixture positions do not certify connector/sink delivery or independent-channel replay
equivalence. Keep both public cluster admission paths closed through these gates.
Do not add a second scheduler or state backend. Coordinator/core changes require
the repository's before/after Criterion and IPC gates.

Complete Python dependency/effect binding, host-loss and cleanup-failure
qualification before stronger Python delivery. Longer resource qualification
must account for retained FILES history and the remaining small idle memory
increments; target-hardware CPU/IPC and representative tail latency remain
required before product latency claims. The atomic-only idle watermark boundary
observed by this example is documented above; it has not been changed or qualified
as autonomous idle timer progress.

### Continuation: qualified cluster admission (2026-10-07)

This completes the bounded Rust cluster profile in original Phase E. Native Rust
and real loopback remote Rust use public registration in single-owner and
multi-owner clusters with at-least-once delivery. Admission requires one logical
append-only, replayable source declaring deterministic row positions,
`SingleChannelFixedBatches` and splittable placement of one global physical
channel. Both declared and instantiated source contracts are checked. Built-in
connectors still leave this replay profile unspecified; this qualification does
not certify Kafka partition merging or FILES discovery order.

Cluster registration binds deployment-supplied code before source DDL. The new
`process_function_bootstrap_sql()` API returns one canonical `CREATE STREAM ...
AS SELECT * FROM laminar_process(source, manifest)` statement for the existing
ordered bootstrap batch. The existing parser, typed catalog namespace, immutable
manifest, rollback and replay machinery own it. SQL never loads code. Changed
predicates, limits, source or package descriptors cannot reinterpret that binding.
Fresh-owner replay installs the same source and process output; process catalog
generation remains one. Live topology mutations reject process pipelines instead
of attempting an uncertified package/state upgrade.

The test-only database admission switch and operator capability override have
been removed. Public one-owner and two-owner database recovery-round cases pass
for both Rust runtimes. The second owner reconstructs its catalog from the sealed
manifest. Additional admission tests reject exactly-once delivery, package/SQL
drift, duplicate bindings, unsupported source placement and live topology changes.
An unbound graph retains its input, emits nothing and cannot take a quiescent
checkpoint. Local native/Rust at-least-once and local Python best-effort behavior
retain their existing contracts.

The existing server process-loss harness now also runs the account-activity
function. Each runtime compares an uninterrupted run with actual OS process
termination of either node 7 (leader/global-source owner) or node 8 (other vnode
owner). Durable MinIO checkpoint/control storage, renewable process and leader
leases, ordinary database checkpoints, leased barrier RPC, rebalance watchers and
Prepare/Start/Release rounds execute normally. Discovery loss is injected through
the existing membership watch; this is not a gossip/network-partition certificate.
The remote Rust worker uses real loopback gRPC in each database process.

Before publication, the survivor verifies the committed checkpoint reference,
assignment two, restored vnode ownership, replay cursor two and the unchanged
global physical channel. Held source starts keep intake, polling, callbacks and
output closed until the exact new Release is committed. Uncommitted changes are
replayed from the old cut. Callback IDs, keys, timestamps, state views, totals,
threshold transitions and four timer firings match the uninterrupted run at
cursor two/watermark 104 and cursor seven/watermark 164. The replaced timer at
110,000 microseconds never fires. Output observation follows fsynced fixture sink
writes; clearing observation does not erase durable output or claim exactly-once
publication. Existing independent-owner host-loss/rescale and damaged-donor
qualification also passes with public capability metadata.

The weekly/manual checkpoint fault workflow now includes these process node-loss
and committed ownership-transfer gates using its existing MinIO service. Explicit
loopback fixture credentials support that job without inheriting cloud credentials.
The workflow itself has not been dispatched from this branch.

Cluster Python, exactly-once process delivery, independent-channel merging,
distributed subscriptions over process output and live package/catalog upgrades
remain explicit rejections. The stock server's Python binding remains local
best-effort; Rust cluster applications register through the Rust API. Stronger
Python delivery still needs enforced lifetime dependency/effect binding. Arbitrary
native code remains trusted; digest negotiation establishes compatibility, not
cryptographic attestation. Target-hardware throughput and tail latency are not
newly certified here.

### Validation

Evidence is retained in `target/process-cluster-admission-20261007/`. The baseline
is `c6980f4a`; rustc is 1.99.0 (`b940084d7`), cargo is 1.99.0 (`5f94df478`), with
`CARGO_BUILD_JOBS=2` and `RUST_MIN_STACK=8388608`. The pre-change process suite
passes 140 tests with two ignored in 162.55 command seconds.

Initial failures exposed missing process catalog-generation reconciliation,
fixture timer/timeout API mistakes, initial shuffle certificate/deadline setup,
and two assertions that still assumed blanket cluster rejection or an error
instead of the existing deferred-input behavior. These were corrected without
changing record execution or recovery failure classification; failed logs remain.

Public database recovery rounds pass four cases in 121.28 seconds. The corrected
full process suite passes 142 tests with two ignored in 182.42 seconds. Live native
and remote Rust database node-loss cases pass in 314.92 seconds, including 123.80
test seconds. The existing independent-owner shared-checkpoint host-loss/rescale
gate passes in 168.06 seconds, including 54.53 test seconds. Exact commands are:

```text
cargo test -p laminar-db --lib --no-default-features --features cluster,process-remote,files process_function:: -- --quiet --test-threads=1
cargo test -p laminar-server --bin laminardb --no-default-features --features cluster,aws,process-remote cluster::recovery_round_tests::committed::process:: -- --ignored --nocapture --test-threads=1
cargo test -p laminar-db --lib --no-default-features --features cluster,process-remote,files process_function::operator::execution::tests::shuffle::committed::peers::independent_owners_restore_the_committed_cut_after_host_loss_and_rescale -- --exact --ignored --nocapture
```

The production changes are cold registration, catalog and admission checks.
Coordinator-cycle, core operators, process record execution, dependencies,
checkpoint codecs and readability baselines are unchanged. Criterion and IPC
requalification is unnecessary for these cold changes. Readability passes with
the same 18 module and 214 function exceptions.

All required gates pass on the final source:

| Gate | Result | Command seconds |
| --- | --- | ---: |
| `cargo test --workspace --lib` | 6,249 passed; five ignored | 308.10 |
| `cargo test -p laminar-server --bin laminardb --no-default-features --features cluster,aws,process-remote cluster:: -- --include-ignored --nocapture --test-threads=1` | 57 passed; none ignored, including the MinIO fixtures | 466.68 |
| `cargo clippy --workspace --all-features --all-targets -- -D warnings` | Passed | 57.39 |
| `cargo clippy --workspace --no-default-features -- -D warnings` | Passed | 22.53 |
| `cargo +nightly fmt --all -- --check` | Passed | 5.29 |
| `cargo run --quiet --manifest-path tools/readability-check/Cargo.toml -- .` | Passed; baselines unchanged | 12.64 |

The all-feature Clippy run required an explicit `String::clone()` in cold
registration; the corresponding formatter adjustment also passed on retry.
The workspace tests were rerun after that correction. This equivalent copy and
formatting are the only source changes after the external recovery qualification.
Failed lint/format logs are retained with the passing retries. Final source hashes
match all 1,207 recorded Rust/Cargo/workflow files; `Cargo.lock` is unchanged.
The task-owned MinIO container was stopped and removed after qualification.

Cluster admission is resolved for the qualified Rust profile above. The explicit
unsupported profiles remain closed and need their own acceptance evidence before
admission can widen.

### Continuation: PR worker cleanup and loss qualification (2026-10-07)

PR #558 at `e27efc91` includes the upstream subscription merge. CI run
`37591021693` exposed Unix `drop_non_drop` in Python supervision and a race in
`lost_worker_fences_unaccepted_remote_result`. The Unix lint is reproduced with
the exact workspace/all-features/all-targets command on Rust 1.99.0 in a local
Linux container.

The supervisor now drops the complete verified environment after reaping the
child and before publishing exit. Its retained file guards remain owned for that
lifetime, including on Windows; no lint suppression or shutdown-order change is
needed. The loss fixture verifies a successful RPC, signals the existing worker
shutdown API and waits for accepted connections to close before the next call.
It waits for an actual deferred outcome rather than a notification alone. The
pipeline recovery-error and non-quiescent checkpoint assertions remain intact.
No production record execution, public API or admission profile changes.

Final validation (`CARGO_BUILD_JOBS=2`, `RUST_MIN_STACK=8388608`):

- Windows all-feature loss fixture: one passed (232.68 command seconds, including
  compilation; 2.07 test seconds).
- Linux `cargo test -p laminar-db --all-features --lib process_function:: -- --test-threads=1`:
  133 passed, two ignored (471.98 command seconds; 92.53 test seconds). Optional
  Python process fixtures retain their existing dependency checks; Python was not
  installed in this Rust-only build container.
- Linux workspace Clippy passes with `-D warnings` for all features/targets
  (64.47 seconds) and no default features (83.05 seconds).
- Windows `cargo test --workspace --lib` with `RUST_TEST_THREADS=2`: 6,258 passed,
  five ignored (361.45 seconds). The initial unrestricted run hit seven connector
  timeout/mock failures; their code is unchanged and the bounded rerun passes
  every affected case. Failed logs are retained, including an intermediate patch
  syntax error corrected before final qualification.
- Nightly formatting, readability (18 module/213 function exceptions) and diff
  checks pass. This fix does not change readability baselines; the upstream merge
  removed one existing function exception.

Evidence is retained in `target/process-pr-ci-20261007/`. The change is cold
supervisor cleanup and test code; no new Criterion/IPC qualification is required.

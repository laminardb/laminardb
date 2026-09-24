# Stateful Process Functions worklog

**Status/date:** Active implementation, 2026-09-24. This is a resumable engineering log, not a support claim.

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

- Trusted native registration is offline through `LaminarDB::register_native_process_function`. It currently accepts one direct in-memory append-only source, one non-null UTF-8 key, UTC microsecond event time, one `ValueState<Int64?>`, named event-time timers, and one append-only output. Only local `BestEffort` admission is enabled. The process operator uses canonical `PartitionKeyCodecV1` keys, concrete per-vnode state, managed accounting, and existing graph checkpoint frames. Complete handler responses are validated before their state, timer, and output changes are accepted. The descriptor and structural schema digest are bound into pipeline identity and restore validation.
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

## Deployment scope and qualification gates

| Mode | Current admission | Required before enabling |
|---|---|---|
| Embedded local | Trusted native, direct in-memory source, `BestEffort`; optional local checkpoint restores managed state | Replayable source/sink certification for stronger delivery claims; native fault and budget tests |
| Single-node server | No packaged registration/invocation route yet | Versioned server catalog/admin binding, supervised worker configuration, restart example, admission and security tests |
| Cluster with one node | Rejected at registration and operator capability | Shared-checkpoint binding, assignment-fenced vnode state/timers, shuffle provenance, one-owner loss/restart and stale-attempt tests |
| Distributed cluster | Rejected | All one-node gates plus cross-node shuffle ordering, vnode acquisition/revocation, timer/pending-work transfer, rescale and node-loss tests |

One-node cluster execution uses the cluster lifecycle and cannot be treated as an embedded shortcut. Keep cluster admission closed until actual ownership and recovery tests pass. Exact delivery additionally requires the repository's certified source/sink composition; deterministic handler results alone do not certify it.

## Next executable task

Run a sampled CPU/IPC profile and representative tail-latency workload on target hardware before using the local Criterion means as a product latency claim. Next add a Python/PyArrow worker for the selected Protobuf/gRPC contract and cross-language conformance tests, then connect the remote client through bounded asynchronous operator scheduling and supervised local worker lifecycle. Qualify local recovery and server invocation before cluster one-owner admission, then distributed handoff. Do not enable either cluster form from capability metadata alone.

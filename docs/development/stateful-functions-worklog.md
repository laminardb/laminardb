# Stateful Process Functions worklog

**Status/date:** Active implementation, 2026-09-23. This is a resumable engineering log, not a support claim.

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

## Deployment scope and qualification gates

| Mode | Current admission | Required before enabling |
|---|---|---|
| Embedded local | Trusted native, direct in-memory source, `BestEffort`; optional local checkpoint restores managed state | Replayable source/sink certification for stronger delivery claims; native fault and budget tests |
| Single-node server | No packaged registration/invocation route yet | Versioned server catalog/admin binding, supervised worker configuration, restart example, admission and security tests |
| Cluster with one node | Rejected at registration and operator capability | Shared-checkpoint binding, assignment-fenced vnode state/timers, shuffle provenance, one-owner loss/restart and stale-attempt tests |
| Distributed cluster | Rejected | All one-node gates plus cross-node shuffle ordering, vnode acquisition/revocation, timer/pending-work transfer, rescale and node-loss tests |

One-node cluster execution uses the cluster lifecycle and cannot be treated as an embedded shortcut. Keep cluster admission closed until actual ownership and recovery tests pass. Exact delivery additionally requires the repository's certified source/sink composition; deterministic handler results alone do not certify it.

## Next executable task

Add native key-isolation, batch-splitting, and absent/null/clear conformance tests; profile the new record path and set a target-hardware latency baseline. Then implement a versioned language-neutral descriptor and one bounded Arrow IPC/Protobuf remote transport with a Rust reference worker before the Python/PyArrow SDK. After local worker recovery and server invocation are qualified, implement and test cluster one-owner admission, then distributed handoff. Do not enable either cluster form from capability metadata alone.

# LaminarDB Python process worker

This package supplies a loopback Python/PyArrow worker for the versioned process
function transport. A handler accepts immutable `Activation` values and returns
`ActivationResult` values. LaminarDB supplies state snapshots and owns accepted
state, timers, checkpoints, and output publication. Handler-local mutable data is
not managed state.

## Local worker quickstart

Use Python 3.13. From the repository root:

```bash
python -m pip install -r python/laminardb_process/requirements.lock
python -m pip install --no-deps ./python/laminardb_process
cargo run -p laminar-db --no-default-features --features process-remote --example process_python
```

The example accepts a `key`, `amount`, and UTC microsecond `ts` row, returns the
updated total, and registers a named event-time timer. See `handler.py` for the
complete user function. The Rust example checks the manifest and handler digest,
starts the Python child, registers its connected client, prints totals `60` and
`110`, then stops the database and worker. Set `LAMINAR_PROCESS_PYTHON` when the
Python executable is not named `python`. The worker accepts only an explicit
loopback IP because plaintext nonlocal
transport is not admitted. Each client RPC must carry a deadline of at most
30 seconds so an incomplete request cannot occupy a worker slot indefinitely.
Local worker concurrency is capped at 32 calls per process.

To exercise the real Rust host client against this Python worker, set
`LAMINAR_PROCESS_PYTHON` to the Python executable that has the dependencies and
run:

```bash
cargo test -p laminar-db --no-default-features --features process-remote --lib process_function::tests::remote_pipeline
```

The local Rust API can register a connected loopback Rust or Python worker into
an embedded best-effort pipeline. The example uses the Python supervisor and a
running database. Single-node server and cluster registration are still closed.

The v1 manifest fixes schema, key, timer names, resource limits, runtime and an
implementation digest. The worker checks the canonical manifest digest on every
invocation; one final file newline is ignored. `LocalPythonWorker` verifies the
direct handler file before launch. Imported modules and data still need an
immutable package binding before replay claims. One invocation contains distinct
keys from one vnode. Results may emit
zero or more Arrow batches and propose state/timer changes, but the host applies
only a complete validated response.

## Container image

From the repository root, build the same pinned package and example:

```bash
docker build -f python/laminardb_process/Dockerfile -t laminardb-process-local .
```

The image starts the loopback example worker. A client must share its network
namespace; exposing the plaintext worker on a nonlocal interface is unsupported.

The checked-in Protobuf messages are generated from
`crates/laminar-db/proto/process_worker.proto` with `grpcio-tools==1.84.0`:

```bash
python -m grpc_tools.protoc -I crates/laminar-db/proto \
  --python_out=python/laminardb_process/laminardb_process \
  crates/laminar-db/proto/process_worker.proto
```

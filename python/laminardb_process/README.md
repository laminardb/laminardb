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
cd examples/process_python
laminardb-process-worker --manifest manifest.json --handler handler:handle --bind 127.0.0.1:50051
```

The example accepts a `key`, `amount`, and UTC microsecond `ts` row, returns the
updated total, and registers a named event-time timer. See `handler.py` for the
complete user function. The worker prints `READY <port>` once the listener is
bound. It accepts only an explicit loopback IP because plaintext nonlocal
transport is not admitted. Each client RPC must carry a deadline of at most
30 seconds so an incomplete request cannot occupy a worker slot indefinitely.

To exercise the real Rust host client against this Python worker, set
`LAMINAR_PROCESS_PYTHON` to the Python executable that has the dependencies and
run:

```bash
cargo test -p laminar-db --no-default-features --features process-remote --lib process_function::remote::tests::python
```

These tests launch their own worker and canonical manifest. The remote client is
not yet registered into database pipelines, so the command above tests the
language boundary rather than a running database pipeline.

The v1 manifest fixes schema, key, timer names, resource limits, runtime and an
implementation digest. The worker checks the exact manifest digest on every
invocation. The caller that packages a handler must verify its implementation
digest against immutable code; this reference worker does not attest imported
modules. One invocation contains distinct keys from one vnode. Results may emit
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

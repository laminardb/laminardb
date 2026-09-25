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
loopback IP because plaintext nonlocal transport is not admitted. Each client
RPC must carry a deadline of at most
30 seconds so an incomplete request cannot occupy a worker slot indefinitely.
Local worker concurrency is capped at 32 calls per process.

To see state survive a database and Python worker restart, use a new checkpoint
directory and run these as two separate commands from the repository root:

```bash
checkpoint_dir="$(mktemp -d)"
cargo run -p laminar-db --no-default-features --features process-remote --example process_python -- checkpoint "$checkpoint_dir"
cargo run -p laminar-db --no-default-features --features process-remote --example process_python -- resume "$checkpoint_dir"
```

On PowerShell:

```powershell
$checkpointDir = Join-Path $env:TEMP ("laminardb-process-" + [guid]::NewGuid())
cargo run -p laminar-db --no-default-features --features process-remote --example process_python -- checkpoint $checkpointDir
cargo run -p laminar-db --no-default-features --features process-remote --example process_python -- resume $checkpointDir
```

The first command prints `key=a total=60` and commits a local checkpoint. The
second starts a new database and worker, restores the checkpoint, and prints
`key=a total=110`. Both commands use a direct in-memory source, so input that
was not checkpointed cannot be replayed after a crash. The local recovery test
also checks that a saved event-time timer fires after restart.

To exercise the real Rust host client against this Python worker, set
`LAMINAR_PROCESS_PYTHON` to the Python executable that has the dependencies and
run:

```bash
cargo test -p laminar-db --no-default-features --features process-remote --lib process_function::tests::remote_pipeline
```

The local Rust API can register a connected loopback Rust or Python worker into
an embedded best-effort pipeline. The example uses the Python supervisor and a
running database.

## Single-node server

The server can register the same Python package at startup from
[`examples/process_python/server.toml`](../../examples/process_python/server.toml).
Install the locked Python requirements above, then run from the repository root:

```bash
cargo run -p laminar-server --no-default-features --features process-remote --bin laminardb -- --config examples/process_python/server.toml
```

The config creates a direct `events` source, verifies the immutable manifest and
handler file, starts the loopback worker, and registers the `activity` output
before other pipeline DDL. Paths in `[[process_function]]` are relative to the
config file. In a second terminal, inspect the binding and insert one event:

```bash
curl http://127.0.0.1:8080/api/v1/process-functions
curl -X POST http://127.0.0.1:8080/api/v1/sql \
  -H 'Content-Type: application/json' \
  --data-binary @- <<'JSON'
{"sql":"INSERT INTO events VALUES ('a', 60, 100000)"}
JSON
```

On PowerShell, use `Invoke-RestMethod` with the same endpoints:

```powershell
Invoke-RestMethod http://127.0.0.1:8080/api/v1/process-functions
Invoke-RestMethod http://127.0.0.1:8080/api/v1/sql -Method Post `
  -ContentType 'application/json' `
  -Body '{"sql":"INSERT INTO events VALUES (''a'', 60, 100000)"}'
```

The integer timestamp is signed microseconds since the Unix epoch. Subscribe
to `ws://127.0.0.1:8080/ws/activity` before inserting to observe the result;
for example, a browser console can use:

```javascript
const ws = new WebSocket("ws://127.0.0.1:8080/ws/activity");
ws.onmessage = event => console.log(event.data);
```

Use a dedicated local `[checkpoint].url` for a restartable deployment. On
restart, the same config and immutable artifacts must be present so the
checkpoint binding can be verified. A committed state/timer checkpoint can be
restored by a new server and worker; the direct source cannot replay input
that was not committed. This route admits only single-node `best_effort`
execution. Worker loss during a call faults the pipeline; an idle worker loss
is observed by the server even without another call. The server revokes serving,
stops the database and exits with an error after either loss. A process manager
can restart the entire server with the same checkpoint and artifacts. There is
no in-place worker replacement, and uncommitted direct-source input is lost.
Cluster mode remains rejected. The HTTP control API uses the
existing console bearer token policy; configure `server.console_token` before
binding it beyond loopback.

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

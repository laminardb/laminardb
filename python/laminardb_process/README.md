# LaminarDB Python process worker

This package supplies a loopback Python/PyArrow worker for the versioned process
function transport. A handler accepts immutable `Activation` values and returns
`ActivationResult` values. LaminarDB supplies state snapshots and owns accepted
state, timers, checkpoints, and output publication. Handler-local mutable data is
not managed state.

The [account activity example](../../examples/process_account/README.md) runs the
same monitor in native Rust and vectorized Python. It checks running totals,
threshold crossings, named inactivity timers and completed-checkpoint recovery
against a fixed independent reference. Its container targets reuse the Compose
controls below. The smaller running-total quickstart remains available here.

## Local worker quickstart

Use Python 3.13 in a virtual environment. From the repository root on Bash:

```bash
python3.13 -m venv .venv
export LAMINAR_PROCESS_PYTHON="$PWD/.venv/bin/python"
"$LAMINAR_PROCESS_PYTHON" -m pip install -r python/laminardb_process/requirements.lock
"$LAMINAR_PROCESS_PYTHON" -m pip install --no-deps ./python/laminardb_process
cargo run -p laminar-db --no-default-features --features process-remote --example process_python
```

On PowerShell:

```powershell
py -3.13 -m venv .venv
$env:LAMINAR_PROCESS_PYTHON = (Resolve-Path .venv/Scripts/python.exe).Path
& $env:LAMINAR_PROCESS_PYTHON -m pip install -r python/laminardb_process/requirements.lock
& $env:LAMINAR_PROCESS_PYTHON -m pip install --no-deps ./python/laminardb_process
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
The supervised local launcher and container set `OMP_NUM_THREADS=1` and
`OPENBLAS_NUM_THREADS=1` before Python imports native libraries. Arrow's CPU and
I/O pools use the configured call limit. These settings limit nested native
parallelism; they do not impose an OS CPU quota or control handler-created threads.

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
cargo test -p laminar-db --no-default-features --features process-remote,files --lib process_function::tests::remote_pipeline
```

The local Rust API can register a connected loopback Rust or Python worker into
an embedded best-effort pipeline. The example uses the Python supervisor and a
running database. Embedded registration also accepts an append-only connector
source. When that connector can resume from a committed cursor, a fresh database
and worker can replay input that was pending at worker exit. The focused
`replayable_source_replays_pending_input_after_python_worker_exit` test exercises
this with a deterministic local connector. A separate test uses the production
`FILES` source and durable file sink, exits a real Python worker during a
pending call, and verifies replay and both published outputs. A Rust reference
worker test terminates the whole host process during a pending file input and
checks fresh-host recovery. These paths remain admitted as `best_effort`;
stronger delivery requires its own admission and recovery qualification.

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
direct handler file before launch. The child checks the same digest and executes
the verified source bytes, including when a stale Python bytecode cache exists.
Imported modules and data still need an immutable package binding before replay
claims. The supervised child uses the handler directory and configured
`python_paths`; it ignores the host's `PYTHONPATH`, Python user site, implicit
current-directory imports, and `PYTHONHOME`. Install dependencies into the
selected virtual environment or provide explicit import roots. One invocation
contains distinct keys from one vnode. Results may emit
zero or more Arrow batches and propose state/timer changes, but the host applies
only a complete validated response.

## Environment binding

An optional `python_environment` in the function manifest binds the selected
interpreter, `module:function` entry point, complete runtime tree and ordered
import trees. `LocalPythonWorkerConfig.runtime_root` and the server's
`[[process_function]].runtime_root` enable verification before spawning the worker.
The handler directory is the first import tree; `python_paths` supplies the rest
in order. A changed, added or missing file changes the binding. Checkpoints and
pipeline identity include it, so rebuilding dependencies requires a new binding
and cannot restore state from the previous package.

Use a self-contained CPython 3.13 installation. Bound startup uses `-I -S -B`,
checks that the interpreter's standard-library paths remain inside `runtime_root`,
then adds only the declared import roots. Virtual environments whose standard
library lives outside that root are rejected. Site initialization and bytecode
writes are disabled. `-X pycache_prefix` points to the interpreter file, so paths
beneath it cannot contain source caches. This applies before bootstrap, including
the initial `encodings` import. Filesystem source modules compile from source;
neither timestamp nor hash caches in `__pycache__` can override it. The launcher
uses the already inventoried executable, adding no file handles. Frozen modules,
sourceless `.pyc` modules and ZIP imports keep their interpreter behavior and
their containing files remain in the bound inventory. See
[CPython's cache-prefix behavior](https://docs.python.org/3.13/library/sys.html#sys.pycache_prefix) and the
[Python 3.13 command-line controls](https://docs.python.org/3.13/using/cmdline.html).

The packaging command writes a new canonical manifest from an existing function
contract. Keep the output outside every hashed tree to avoid a self-reference.
For example, on PowerShell with Python installed at `C:/Python313`:

```powershell
$runtimeRoot = 'C:/Python313'
$python = Join-Path $runtimeRoot 'python.exe'
$package = Join-Path (Resolve-Path target).Path 'process-package'
$handlers = Join-Path $package 'handlers'
$dependencies = Join-Path $package 'deps'
New-Item -ItemType Directory -Path $handlers | Out-Null
Copy-Item examples/process_python/handler.py $handlers
& $python -m pip install --target $dependencies -r python/laminardb_process/requirements.lock
& $python -m pip install --target $dependencies --no-deps ./python/laminardb_process
cargo run -p laminar-db --no-default-features --features process-remote --example package_process_python -- `
  examples/process_python/manifest.json (Join-Path $package 'manifest.json') `
  $runtimeRoot $python (Join-Path $handlers 'handler.py') handle $dependencies
```

For embedded use, pass the generated manifest, handler file, interpreter,
`runtime_root`, and dependency directory to `LocalPythonWorker::start`. For a
server config saved in the package directory, set these fields in its existing
`[[process_function]]` entry:

```toml
manifest = "manifest.json"
handler_file = "handlers/handler.py"
function = "handle"
python = "C:/Python313/python.exe"
runtime_root = "C:/Python313"
python_paths = ["deps"]
```

The inventory accepts at most 16 import roots, 32,768 total file/directory entries,
4 GiB total file bytes and 512 MiB per file; overlapping trees consume the budget
again. Paths must be UTF-8 and at most 1,024 bytes relative to their root. Links,
Windows reparse points (including configured path ancestors) and special files
are rejected. Configured paths allow at most 128 ancestors. All files, data and
empty directories are included. Hashing runs on a blocking startup task, with no new
per-record work. Identical trees may move while retaining their identity.

On Windows, bound supervision opens inventoried files with read sharing only,
hashes those handles, and retains them during startup and execution. It also
retains the manifest, inventoried directories and configured path ancestors.
Ancestors are opened from the filesystem root downward before canonicalization
can erase a link. Read access to those directories is required. Existing writers
reject startup; later edits, deletion and replacement of held entries fail with a
sharing violation. Read access remains available for imports and native loaders.
Normal shutdown, failed readiness and cancelled startup reap the child before
releasing the handles. Packaging releases its temporary handles when capture
returns. See [Windows file sharing](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew).

The tree and path-depth limits bound this retention to fewer than 38,000 handles;
process, socket and unrelated engine handles are additional. Other operating
systems validate ancestors and retain startup drift checking without enforced
sharing.

The deployment must still be quiescent and trusted. Directory additions and
metadata are not sealed. Mutable filesystem timestamps no longer select source
bytecode caches under the standard filesystem importer.
Abrupt host termination releases the guards.
Handler changes to import controls, external data and libraries loaded
from outside the declared trees are not protected. This does not establish the
complete immutable dependency closure. Python remains `BestEffort` in embedded
and single-node modes; `AtLeastOnce`, `ExactlyOnce` and both cluster forms remain
rejected.

To run the environment-bound Rust regressions, set `LAMINAR_PROCESS_PYTHON` to
an explicit interpreter file and `LAMINAR_PROCESS_PYTHON_RUNTIME_ROOT` to its
installation root. Set `LAMINAR_PROCESS_PYTHON_DEPS` to the explicit dependency
directory when packages are not installed in the SDK import root. Bound workers
do not use global site packages. The database test verifies matching-package
recovery, rejection of dependency drift, and checkpoint rejection after a
dependency rebuild; the configured server test exercises SQL input and recovery.
Windows regressions also verify denied edits, successful lazy imports, and child
cleanup with guard release after shutdown, startup cancellation, readiness
timeout and handler initialization failure. A configured path beneath a junction
ancestor is rejected, and deployment-directory renaming remains blocked through
worker execution and succeeds after cleanup.

## Container checkpoint quickstart

Use Docker with Linux containers and the Compose plugin. From the repository
root, build the pinned worker and the Rust engine example:

```sh
docker build --target worker -f python/laminardb_process/Dockerfile -t laminardb-process-local .
docker build --target example -f python/laminardb_process/Dockerfile -t laminardb-process-example .
```

The default Dockerfile target is the worker. The `example` target builds the same
Rust example with the workspace MSRV and locked dependencies, without debug
symbols. It is a development build, not a performance reference.

Resolve the tags to local content-addressed image IDs before activation. On Bash:

```bash
export LAMINAR_PROCESS_WORKER_IMAGE="$(docker image inspect --format '{{.Id}}' laminardb-process-local)"
export LAMINAR_PROCESS_EXAMPLE_IMAGE="$(docker image inspect --format '{{.Id}}' laminardb-process-example)"
mkdir -p target/process-container
```

On PowerShell:

```powershell
$env:LAMINAR_PROCESS_WORKER_IMAGE = docker image inspect --format '{{.Id}}' laminardb-process-local
$env:LAMINAR_PROCESS_EXAMPLE_IMAGE = docker image inspect --format '{{.Id}}' laminardb-process-example
New-Item -ItemType Directory -Path target/process-container -Force | Out-Null
```

The following commands work in either shell. Use a fresh Compose project name
for the first checkpoint; reusing an existing checkpoint causes the example's
expected-total check to fail.

```sh
docker compose -p laminar-process-quickstart -f examples/process_python/compose.yaml config --output target/process-container/compose.resolved.yaml
docker compose -f target/process-container/compose.resolved.yaml run --rm example checkpoint /var/lib/laminardb-process/checkpoints
docker compose -f target/process-container/compose.resolved.yaml restart worker
docker compose -f target/process-container/compose.resolved.yaml run --rm example resume /var/lib/laminardb-process/checkpoints
docker compose -f target/process-container/compose.resolved.yaml down
```

The saved configuration retains the selected image IDs, so subsequent commands
do not resolve the build tags again. Keep it and the named checkpoint volume
for recovery. Local image IDs are specific to the Docker daemon. For registry
distribution, operators should use a resolved `repository@sha256:...` reference
and retain that image; the engine does not pull images or access a Docker socket.

The first engine container prints `key=a total=60` and commits a checkpoint.
After the worker restart, a new engine container restores state and prints
`key=a total=110`. `down` removes the containers and retains the checkpoint
volume. This demonstrates embedded `BestEffort` recovery from a completed cut;
the direct source does not replay uncommitted input. The descriptor binds the
handler and protocol; the saved deployment configuration pins the complete
images. This does not certify Python replay equivalence or enable cluster mode.

The worker verifies `handler.py` before loading it. Both images run as UID/GID
10001. The [Compose configuration](../../examples/process_python/compose.yaml)
sets each container to one CPU, 256 MiB memory without swap, 64 tasks, a read-only
root filesystem, and a 16 MiB temporary filesystem with execution disabled.
It drops Linux capabilities and forbids privilege escalation. The engine writes
only to its checkpoint volume. That volume uses ordinary Docker storage and has
no quota; provision its capacity separately for longer runs.
See the [Compose service options](https://docs.docker.com/reference/compose-file/services/)
for these deployment controls.

The worker's network namespace has only loopback, and the engine shares it.
The example publishes no ports and has no external egress. Exposing the
plaintext worker on a nonlocal interface remains unsupported. The gRPC readiness
check runs after package loading, and Compose waits for it before launching the
engine. The fixed image probe targets port 50051; override the healthcheck if
you change the worker's bind port. Worker logs rotate at 1 MiB with two files.

SIGTERM stops new calls and gives active calls five seconds to complete before
gRPC cancellation. Compose allows ten seconds before forcibly stopping the
container. A blocked handler cannot be interrupted safely inside Python; the
container deadline is the final bound. Stop intake and checkpoint/drain the
engine before stopping a worker in a running deployment. The example commands
finish the engine process before each worker restart.
The drain uses [gRPC's graceful stop](https://grpc.github.io/grpc/python/grpc.html#grpc.Server.stop).

To use an independently deployed worker with the local Rust example, set
`LAMINAR_PROCESS_ENDPOINT=http://127.0.0.1:50051`. It connects through the existing
bounded client and uses the example's compiled descriptor. The caller owns the
worker lifecycle, and must run in the same network namespace with the matching
example package. Without this variable, the local Python quickstart starts its
own supervised worker.

The checked-in Protobuf messages are generated from
`crates/laminar-db/proto/process_worker.proto` with `grpcio-tools==1.84.0`:

```bash
python -m grpc_tools.protoc -I crates/laminar-db/proto \
  --python_out=python/laminardb_process/laminardb_process \
  crates/laminar-db/proto/process_worker.proto
```

# Account activity process function

This embedded example implements the same monitor in native Rust and vectorized
Python/PyArrow. LaminarDB owns one `Int64` running total and one named `inactive`
event-time timer per account. Every input emits its total. An upward crossing
from below 100 emits `kind=threshold`; other input emits `kind=running`. Each
input replaces that account's timer at `ts + 10,000 µs`. A timer emits
`kind=inactive` with the current total and keeps the total for later activity.
Both implementations reject null state and signed integer overflow.

## Run locally

From the repository root, native Rust needs no process-worker feature:

```bash
cargo run -p laminar-db --no-default-features --example process_account -- native
```

For Python, install the locked worker package using the
[SDK quickstart](../../python/laminardb_process/README.md#local-worker-quickstart),
then run:

```bash
cargo run -p laminar-db --no-default-features --features process-remote --example process_account -- python
```

`LAMINAR_PROCESS_PYTHON` selects the interpreter. Optional
`LAMINAR_PROCESS_PYTHON_DEPS` adds an explicit dependency directory. The host
checks the manifest and handler digest before launching its supervised worker.
Each command registers `activity` over `events`, ingests the fixed fixture,
checks all four output columns, prints each accepted row and ends with
`reference matched`. Neither handler stores authoritative data in private memory.
Python gathers the bounded batch and uses Arrow checked addition, comparisons
and selection; one-row output slices retain that batch's buffers.

## Fixed reference and event time

The Rust driver contains hand-calculated expectations independent of both
handlers. Outputs across accounts may interleave; each phase checks the exact
multiset of expected rows. Arrival order is retained within an account.

| Phase | Account | Amount | Input ts (µs) | Expected kind | Total |
|---|---|---:|---:|---|---:|
| Initial | alice | 60 | 100000 | running | 60 |
| Initial | bob | 20 | 100040 | running | 20 |
| Initial | alice | 50 | 100080 | threshold | 110 |
| Initial | bob | 20 | 100040 | running | 40 |
| Initial | alice | -30 | 100090 | running | 80 |
| Continue | alice | 25 | 101000 | threshold | 105 |
| Continue | charlie | 7 | 100500 | running | 7 |
| After timers | bob | 10 | 112000 | running | 50 |
| After timers | alice | -10 | 112001 | running | 95 |
| After timers | alice | 5 | 112002 | threshold | 100 |

The second Bob event is a distinct business event with identical fields. It
counts again. Replaying the same logical activation with the same supplied
state gives the same proposed result; application-level business deduplication
is not supplied by this example.

Timestamps arrive out of order across accounts, with repeated keys and one
equal timestamp within Bob's input. A one-second SQL watermark delay holds
automatic progress behind this fixture. The source API publishes a watermark
atomically; an input cycle forwards it to
operators. Dana's ordinary transactions drive these cycles, and their output is
included in the reference: amounts 1, 2, 3 and 4 at 110001, 111001, 123001 and
130001 µs produce totals 1, 3, 6 and 10. This account remains active while the
others go inactive; its final timer at 140001 µs stays pending at shutdown.

After continuation, the driver explicitly advances the source watermark to
110 **milliseconds** and checks no timer output, then to 111 ms. The latter
fires exactly:

| Account | Kind | Total | Output ts (µs) |
|---|---|---:|---:|
| bob | inactive | 40 | 110040 |
| charlie | inactive | 7 | 110500 |
| alice | inactive | 105 | 111000 |

Alice's replacement timer fires at 111000, so the earlier timer at 110090 emits
nothing. Bob's timer is left untouched by continuation, proving its recovery
from the initial checkpoint. A watermark of 123 ms fires Bob at 122000 with total
50 and Alice at 122002 with total 100. Advancing to 130 ms emits only Dana's
running total. No wall-clock sleep triggers a timer. Worker calls and output
waits have five-second deadlines.

This handler defines inactivity relative to the most recently accepted input's
timestamp. It does not sort input or retain the maximum timestamp. The fixture's
per-account times do not regress. Input behind an accepted watermark is late;
the demo does not qualify a business policy for such records.

V1 admits one direct source and preserves accepted arrival order within each
key. Event timestamps do not sort records. Replayable offsets and ordered row
positions describe progress within a source partition; they do not specify how
independent channels merge. Reproducing callback IDs requires the same callback
order within each vnode and the same watermark cuts relative to input. The
cluster replay tests use one fixed source order, split it into different
batch sizes, and retain those cuts. They cover timer replacement, cancellation
and callbacks that register another timer. This qualifies that controlled
profile; independent-channel merging remains uncertified. Event-time sorting
and multi-input process functions remain unsupported.

Local native and remote Rust `AtLeastOnce` registration now requires a replayable,
append-only connector declaring `SourceReplayOrder::SingleChannelFixedBatches` with
deterministic row positions. The bounded local profile permits singleton or splittable
placement with one logical source and one global physical input channel.
Registration and startup check that declaration before source I/O, and checkpoint
identity binds it. Built-in connectors leave it unspecified; FILES does not retain
the discovery order of an uncommitted suffix. Its process host-loss tests therefore
use `BestEffort` with checkpoints. Fixed replay batches define matching input/watermark
cuts: the engine executes one batch at a time and derives progress from event timestamps.
Coalescing, wall-clock idleness/future-skew decisions and external watermark calls do not
advance that profile. Timers need subsequent input to advance event time. Raw
`SingleChannel` order and independent-channel merging remain insufficient. This example
uses `BestEffort` in both languages.

## Cluster admission

Native Rust and loopback remote Rust admit `AtLeastOnce` in both single-owner and
multi-owner clusters. The source must declare the fixed-batch contract above with
`SourceTopology::Splittable`: one global physical channel follows its assigned
owner, including after node loss. Independent partition merges remain rejected.
No built-in connector currently certifies this profile; applications must supply
a connector that implements it. Durable output uses the existing sink contracts.
This FILES example does not become a cluster replay source.

Each database owner registers the same immutable descriptor and trusted code
before replaying or sealing the startup catalog. After registration, obtain the
canonical invocation and place it between source and consumer DDL:

```rust,ignore
db.register_native_process_function("activity", "events", descriptor, handler).await?;
let process_sql = db.process_function_bootstrap_sql("activity")?;
db.execute_cluster_bootstrap_batch(&[source_sql, process_sql, sink_sql]).await?;
```

The generated statement uses `CREATE STREAM activity AS SELECT * FROM
laminar_process('events', '<manifest>')`. SQL references deployment-supplied code;
it never downloads or loads a package. Modified predicates, limits, projection,
manifest, or source bindings are rejected. Fresh owners replay the exact sealed
statement. Descriptor/checkpoint compatibility is checked before state restore.
Live catalog changes involving process pipelines require a new checkpoint namespace.

The database process-loss tests use durable shared control/checkpoint storage,
renewable leases, public bindings and a durable at-least-once fixture sink. They
kill either the leader/source owner or the other vnode owner, restore the committed
cursor and timer state, and hold intake through matching Prepare/Start/Release
rounds. Native and real loopback Rust worker callbacks and output match an
uninterrupted run at the same committed watermark cuts. Existing transfer tests
cover reassignment, rescale, network interruption and delayed old-owner replies.

Cluster Python, exactly-once process delivery, distributed subscriptions over
process output, and live package/catalog upgrades remain explicitly rejected.
Python still needs immutable lifetime dependency/effect binding for replay-capable
delivery. The stock server's Python configuration remains local best-effort;
native and remote Rust cluster bindings use the Rust API. Trusted native handlers
must be bounded, deterministic and nonblocking. Remote worker digest negotiation
checks compatibility; deployment code identity remains the operator's responsibility.

## Completed-checkpoint recovery

Use a fresh directory for each runtime. Native and Python descriptors differ,
so their checkpoints cannot be interchanged. On Bash:

```bash
native_dir="$(mktemp -d)"
cargo run -p laminar-db --no-default-features --example process_account -- native checkpoint "$native_dir"
cargo run -p laminar-db --no-default-features --example process_account -- native resume "$native_dir"
python_dir="$(mktemp -d)"
cargo run -p laminar-db --no-default-features --features process-remote --example process_account -- python checkpoint "$python_dir"
cargo run -p laminar-db --no-default-features --features process-remote --example process_account -- python resume "$python_dir"
```

On PowerShell:

```powershell
$nativeDir = Join-Path $env:TEMP ("laminar-account-native-" + [guid]::NewGuid())
cargo run -p laminar-db --no-default-features --example process_account -- native checkpoint $nativeDir
cargo run -p laminar-db --no-default-features --example process_account -- native resume $nativeDir
$pythonDir = Join-Path $env:TEMP ("laminar-account-python-" + [guid]::NewGuid())
cargo run -p laminar-db --no-default-features --features process-remote --example process_account -- python checkpoint $pythonDir
cargo run -p laminar-db --no-default-features --features process-remote --example process_account -- python resume $pythonDir
```

`checkpoint` checks the five initial rows and persists totals and pending timers.
`resume` starts a fresh database and, for local Python, a fresh worker, then
checks the remaining fourteen rows including the saved Bob timer. A missing or
stale checkpoint fails the fixed reference. These commands use a direct in-memory
source and `BestEffort`: uncheckpointed input cannot be recovered after a crash.

## Container recovery

The account targets reuse the SDK Dockerfile and the
[bounded Compose deployment](../../python/laminardb_process/README.md#container-checkpoint-quickstart).
Build from the repository root:

```bash
docker build -f python/laminardb_process/Dockerfile --target account-worker -t laminar-account-worker:local .
docker build -f python/laminardb_process/Dockerfile --target account-example -t laminar-account-example:local .
```

On Bash, resolve images and save the deployment before activation:

```bash
export LAMINAR_PROCESS_WORKER_IMAGE="$(docker image inspect --format '{{.Id}}' laminar-account-worker:local)"
export LAMINAR_PROCESS_EXAMPLE_IMAGE="$(docker image inspect --format '{{.Id}}' laminar-account-example:local)"
mkdir -p target/process-account-container
```

On PowerShell:

```powershell
$env:LAMINAR_PROCESS_WORKER_IMAGE = docker image inspect --format '{{.Id}}' laminar-account-worker:local
$env:LAMINAR_PROCESS_EXAMPLE_IMAGE = docker image inspect --format '{{.Id}}' laminar-account-example:local
New-Item -ItemType Directory -Force target/process-account-container | Out-Null
```

The remaining commands work in both shells. Use a fresh Compose project for a
new initial checkpoint:

```bash
docker compose -p laminar-account-quickstart -f examples/process_python/compose.yaml config --output target/process-account-container/compose.resolved.yaml
docker compose -f target/process-account-container/compose.resolved.yaml run --rm example checkpoint /var/lib/laminardb-process/checkpoints
docker compose -f target/process-account-container/compose.resolved.yaml restart worker
docker compose -f target/process-account-container/compose.resolved.yaml run --rm example resume /var/lib/laminardb-process/checkpoints
docker compose -f target/process-account-container/compose.resolved.yaml down
```

The worker and engine restart separately. `down` retains the checkpoint volume.
Local image IDs are daemon-specific; distribute a resolved registry digest when
moving to another host. The saved images identify this development deployment;
they do not certify stronger Python replay or delivery guarantees. Native Rust
runs trusted code in the compute process; Python uses the loopback worker with
bounded calls. Cluster registration remains closed in both one-node and
distributed modes. No benchmark or production latency claim is made by this
small functional example.

## Verify

With the Python interpreter and package paths set as above:

```bash
cargo test -p laminar-db --no-default-features --features process-remote --example process_account -- --test-threads=1
```

For the handler tests, use the selected interpreter. On Bash:

```bash
"$LAMINAR_PROCESS_PYTHON" -B -m unittest discover -s examples/process_account -p test_handler.py -v
```

On PowerShell:

```powershell
& $env:LAMINAR_PROCESS_PYTHON -B -m unittest discover -s examples/process_account -p test_handler.py -v
```

The engine tests execute the full reference and the split recovery path for
each runtime. The Python engine test runs when `LAMINAR_PROCESS_PYTHON` is set.
Handler tests cover a mixed input/timer batch, repeated logical activation,
worker reuse and checked integer boundaries. Python must be able to import the
SDK and locked dependencies; `LAMINAR_PROCESS_PYTHON_DEPS` is a Rust launcher
setting, so standalone Python tests need an installed package or `PYTHONPATH`.

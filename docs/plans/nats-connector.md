# NATS connector

Source + sink connector for NATS, covering Core NATS and JetStream, plus a
cluster-discovery backend on JetStream KV.

## Goals

- One `nats` connector type, two modes: `core` (non-durable pub/sub) and
  `jetstream` (durable streams, default).
- At-least-once by default; exactly-once on JetStream with user-supplied
  message id.
- Per-subject lanes for watermarks and offset tracking — matches how the
  Kafka source treats partitions.
- Cluster coordination backend using JetStream KV as a peer to
  `kafka-discovery`, not a replacement.

## Non-goals (v1)

- Push consumers. Pull only.
- Ephemeral consumers. Durable name required.
- Subject templates with placeholders (`orders.{region}.created`).
  Literal subject or single `subject.column` only.
- Protobuf format. Deferred to a cross-cutting "protobuf format"
  effort covering Kafka/NATS/files.
- Avro without the `kafka` feature. Reuses the existing
  `SchemaRegistryClient`; fails at open() otherwise.

## Crate layout

```
crates/laminar-connectors/src/nats/
├── mod.rs          // registration
├── config.rs       // source + sink config + enums
├── source.rs       // NatsSource — mode dispatch inside
├── sink.rs         // NatsSink  — mode dispatch inside
├── subject.rs      // literal | column → subject
├── dedup.rs        // Nats-Msg-Id accessor from configured column
├── discovery.rs    // JetStream KV cluster-discovery backend
└── metrics.rs
```

Features:

```toml
nats           = ["dep:async-nats"]
nats-discovery = ["nats", "laminar-core/cluster-unstable"]
```

Pinned: `async-nats = "0.47"`.

Registration sits beside Kafka in
`crates/laminar-db/src/db.rs::register_builtin_connectors`, gated by
`#[cfg(feature = "nats")]`.

## SDK prerequisite — PR #1

Add one method to `SourceConnector` with a default no-op. The coordinator
calls it after the manifest is persisted and after the sink's
`commit_epoch` returns. NATS uses it to ack the full set of messages for
the committed epoch.

```rust
async fn notify_epoch_committed(&mut self, _epoch: u64)
    -> Result<(), ConnectorError> { Ok(()) }
```

Kafka is **not** migrated in this PR. Today's timer-based commit stays.
The migration to epoch-gated commits is a separate hardening PR
sequenced after #1 stabilizes — it is not behavior-neutral and deserves
its own release note.

## Source

### Config (WITH-clause keys)

```
-- connection
servers                nats://a:4222,nats://b:4222            (required)
mode                   core | jetstream                       (default jetstream)
auth.mode              none | user_pass | token | nkey | jwt | creds_file
<auth fields>          routed through storage credential resolver
tls.*                  ca / cert / key / key.password

-- jetstream
stream                 ORDERS                                  (required jetstream)
subject                orders.>                                (core required; JS optional filter)
subject.filters        orders.us.*,orders.eu.*                 (JS multi-filter, 2.10+)
consumer               laminar-orders                          (required durable name)
deliver.policy         all | new | by_start_sequence | by_start_time
start.sequence | start.time                                    (only when matching policy)
ack.policy             explicit | none                         (default explicit)
ack.wait.ms            60000                                   (see validation)
max.deliver            5
max.ack.pending        10000
fetch.batch            500
fetch.max.wait.ms      500
fetch.max.bytes        1048576

-- core
queue.group            laminar-workers                         (core only, load balance)

-- format & metadata
format                 json | csv | raw | avro                 (avro needs kafka feature)
include.metadata       false                                   (_subject, _stream_seq, _consumer_seq,
                                                                _timestamp, _num_delivered)
include.headers        false                                   (_headers as JSON)
event.time.column      ""
schema.registry.*      (same keys as Kafka; only live with kafka feature)

-- error handling
poison.dlq.subject     orders.dead                             (Term on max_deliver + republish)
```

### Per-subject lanes

`PartitionInfo.id = <subject>`. The source opens a lane on first
observation of a subject (or eagerly for each declared `subject.filters`
entry). `OffsetTracker` keys by subject → last `stream_seq`. The
watermark tracker treats each subject as an independent lane for
idle-partition detection, matching `KafkaWatermarkTracker`.

### Lifecycle (jetstream)

1. `open()`:
   - Build `async_nats::ConnectOptions` from auth/tls, `connect()`.
   - `jetstream::new(client)`, `get_stream(stream).await` for validation.
   - `create_or_update_consumer` with `consumer::pull::Config`.
   - Run validation rules below; fail fast with `LDB-5xxx` on any
     violation.
   - Spawn reader task: loops on `consumer.fetch().batch(N).expires(T).messages()`,
     pushes `(payload, AckHandle, subject, stream_seq, timestamp, headers)`
     into a bounded `crossfire::mpsc::Array`. **Reader never acks.**
2. `poll_batch(max)`:
   - Drain the queue into per-subject deserializer inputs + metadata
     columns.
   - Deserialize via the shared `RecordDeserializer`.
   - Record every `AckHandle` in the current epoch's pending set (keyed
     by epoch number).
   - Update `OffsetTracker` per subject.
3. `checkpoint()`:
   - Emit `SourceCheckpoint { consumer, per_subject_seq }`.
4. `restore(cp)`:
   - Trust the server-side ack floor. The durable consumer resumes
     delivery from its floor on reconnect. If the manifest's seq is
     strictly greater than the server floor (possible only after
     out-of-band consumer surgery), log loudly at warn and proceed from
     the server floor — don't try to "rewrite" the consumer.
5. `notify_epoch_committed(e)`:
   - Ack **every** `AckHandle` in epoch `e`'s pending set. Not the
     high watermark — the full set. JetStream's ack floor only advances
     over contiguous prefixes; gaps cause permanent redelivery of
     already-processed messages.
6. `close()`: drain reader, drop client.

### Core mode

`client.subscribe(subject)` or `queue_subscribe(subject, queue_group)`.
Override `supports_replay() → false`. Checkpoint is a no-op. Reject at
plan time if `DELIVERY = EXACTLY_ONCE` is combined with `mode=core`.

### Startup validation (hard-fail with `LDB-5xxx`)

- `mode=core` with `delivery.guarantee=exactly_once`.
- `mode=jetstream`, exactly-once requested, no `dedup.id.column` on the
  paired sink.
- `checkpoint_interval + 5s ≥ ack.wait.ms`.
- `ack.wait.ms * max.deliver ≥ stream.duplicate_window` on the paired
  sink side — rollback redelivery must stay inside the dedup window.
- Consumer exists with conflicting immutable fields
  (`filter_subjects`, `ack_policy`, `deliver_policy`). Error text tells
  the operator to rotate the durable name or delete the consumer
  out-of-band.
- `format=avro` without the `kafka` feature.

### Back-pressure surfaces

`max_ack_pending` saturation pauses delivery server-side silently.
Surface as:

- `laminar_nats_source_pending_acks` gauge.
- `HealthStatus::Degraded { reason: "max_ack_pending saturated" }`
  when the pending count is within 5% of the cap for >30s.

### Consumer config drift

`create_or_update_consumer` returns `ErrConsumerCreate` on
immutable-field conflict. Surface as
`ConnectorError::Configuration("consumer '{name}' exists with
incompatible config; rotate the durable name or delete out-of-band")`.

## Sink

### Config

```
servers, mode, auth.*, tls.*
stream                 ORDERS_OUT                              (for validation)
subject                orders.processed                        (literal) OR
subject.column         out_subject                             (per-row)
expected.stream        ORDERS_OUT                              (Nats-Expected-Stream header)

delivery.guarantee     at_least_once | exactly_once
dedup.id.column        event_id                                (required for exactly_once)

max.pending            4096                                    (PubAck outstanding)
ack.timeout.ms         30000
flush.batch.size       1000

format                 json | csv | raw | avro
header.columns         trace_id,tenant
poison.dlq.subject     orders.failed
```

### Lifecycle

- `open()`: connect → `jetstream::new()`. Query stream config; validate
  `duplicate_window` against source-side `ack.wait * max.deliver`.
- `write_batch`: for each row compute subject, headers (header.columns +
  `Nats-Msg-Id = <row[dedup.id.column]>` when exactly-once),
  serialized payload; call `jetstream.publish_with_headers`; push the
  returned `PublishAckFuture` into a `VecDeque` capped at `max.pending`.
- `pre_commit`: `try_join_all` on the deque with `ack.timeout.ms` each;
  any non-duplicate error fails the epoch. Duplicate acks are counted
  (metric) and accepted.
- `commit_epoch`: no-op on the NATS side; bump `last_committed_epoch`.
- `rollback_epoch`: drop in-flight buffer. Already-landed publishes are
  covered by server dedup on retry (validated at open()).
- `close()`: flush the deque with a bounded deadline.

### Capabilities

```rust
SinkConnectorCapabilities::new(Duration::from_secs(5))
    .with_idempotent()        // always in JS mode
    .with_exactly_once()      // when dedup.id.column set
    .with_two_phase_commit()  // pre_commit awaits all PubAckFutures
    .with_partitioned()       // subject-per-row
```

No `changelog` / `upsert` — NATS is append-only.

### Core sink

`client.publish`. Capabilities declare at-least-once best-effort only.
`pre_commit` is a no-op. Reject plan-time if paired with
`exactly_once`.

## Cluster discovery (`nats-discovery` feature)

Alternative to `kafka-discovery`. A cluster picks **one** coordinator
backend at boot; mixing Kafka and NATS coordination inside a single
cluster is not supported and is rejected at startup.

### Backend selection

```
cluster.coordinator    kafka | nats        (required when cluster mode is enabled)
```

Resolution order:

- Both features compiled in → config key selects the backend.
- Only one feature compiled in → config key must match the compiled
  backend or `open()` fails.
- Both config keys set, or `cluster.coordinator` points at a backend
  whose feature is not compiled in → hard-fail at startup with
  `LDB-5xxx`.
- Leader detects a peer announcing a different coordinator backend
  (visible in the heartbeat payload) → fence the cluster: refuse to
  publish an assignment and raise `HealthStatus::Unhealthy`. Prevents
  a mid-flight misconfiguration from silently splitting the cluster
  across two coordination planes.

### KV buckets

- `_laminar_nodes` — TTL 30s, key = `node_id`, value = heartbeat payload
  (addr, epoch, assignment_version). Each node refreshes every 10s;
  dead nodes auto-expire.
- `_laminar_leader` — single key `leader`, value = `node_id`. Election
  via `create` on the key (CAS fail = someone else won). Leader
  refreshes TTL; on expiry, watchers race to claim.
- `_laminar_assignments` — key = version number, value =
  `AssignmentSnapshot`. Written with `PutMode::Create` so concurrent
  leader writes cannot clobber. Same struct and versioned layout as the
  existing cluster primitives.

### Node watch loop

Subscribe to `_laminar_assignments` updates. On new version, call
`LaminarDB::adopt_assignment_snapshot` (already present). Leader
election and assignment publication reuse the existing debounced
rebalance controller from #342.

### Bucket lifecycle

Auto-create on first use with `replicas=3, history=5, ttl=30s`.
Overridable via:

```
nats.discovery.bucket.replicas       3
nats.discovery.bucket.history        5
nats.discovery.nodes.ttl.ms          30000
nats.discovery.leader.ttl.ms         15000
```

Operators who prefer preprovisioning use
`laminar-cli cluster init --coordinator=nats --servers=...`; the
runtime then refuses to create buckets and errors on missing ones.

### Why JetStream KV over Kafka for this

- Native CAS on `create` — leader election without compacted-topic
  hacks.
- Native TTL on heartbeats — no consumer-group liveness dance.
- Sub-ms watch notifications vs Kafka poll.

### Scope

Opt-in, `cluster-unstable` feature flag for honesty. Peer to
`kafka-discovery` at compile time, mutually exclusive at run time.
Shops with Kafka stay on `kafka-discovery`; NATS-only shops pick
`nats-discovery`. No hybrid.

## Test matrix

Five scenarios. All gated behind `--features nats` and `--ignored`
(testcontainers, `nats:2.10-alpine`). Same stance as the Mongo tests.

1. **Round-trip**: publish via sink → consume via source → identity.
2. **Broker kill mid-epoch**: kill `nats-server` during a write batch,
   restart, verify no loss and no duplicates downstream.
3. **LaminarDB kill after pre_commit, before manifest**: restart,
   rollback path fires, verify no duplicate rows.
4. **Forced redelivery**: sleep the sink past `ack.wait`, verify source
   redelivers, sink dedup drops, `duplicate` metric non-zero.
5. **Startup validation**: each of the six hard-fail rules raises the
   correct `LDB-5xxx` code with the documented message.

Discovery (nats-discovery feature):

6. **Three-node convergence**: kill leader, verify a new leader claims
   the TTL'd key and publishes a new assignment version that the
   remaining nodes adopt.

## Staging

| PR | Scope | Merge criterion |
|---|---|---|
| 1 | `SourceConnector::notify_epoch_committed` hook + coordinator wiring. Kafka untouched. | All existing connector tests green. |
| 2 | `nats` feature. Core + JetStream pull source, at-least-once sink. Per-subject lanes. Startup validation. Back-pressure + drift surfaces. | Scenarios 1, 2, 4, 5. |
| 3 | JetStream sink exactly-once via `Nats-Msg-Id`. `duplicate_window` validation. | Scenario 3 + full exactly-once matrix. |
| 4 | `nats-discovery` feature. KV-based nodes / leader / assignments. | Scenario 6. |

Kafka commit-timer → epoch-based migration: separate PR, sequenced
after PR 1 has been live at least one release cycle.

## Open items

- Default KV bucket replicas (`3`) assumes a 3-node JetStream cluster.
  Single-node dev deployments need `1`. Propose: auto-detect via
  `jetstream.account_info()` topology and pick accordingly; fall back
  to `replicas=1` with a startup warning if the account reports fewer
  than 3 servers.

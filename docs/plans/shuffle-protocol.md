# Shuffle Protocol

**Status:** design lock for Phase C.3 of `distributed-stateful-pipelines.md`.
**Scope:** wire format, flow control, ordering, connection management, and
checkpoint-barrier integration for cross-instance data shuffling.
**Decisions are opinionated;** each has a rationale and an explicit
"why not the alternative."

## Why this doc exists

The shuffle moves events from the instance that *consumed* them to the
instance that *owns the vnode* for their key. Most of the distributed
runtime's hard engineering lives here: wire protocol, back-pressure,
ordering, exactly-once under crash. The parent design doc deferred the
details to "Phase C.3 — shuffle operator implementation." This is that.

## What the shuffle is (and isn't)

**Is:** an in-process component on every instance that

- accepts `RecordBatch`es from the source/operator layer with an attached
  `vnode_id`,
- routes each batch to the TCP connection for the owning instance,
- applies back-pressure when the receiver is slow,
- delivers batches to the receiving operator in FIFO order per sender,
- propagates checkpoint barriers in-band.

**Isn't:**

- An RPC layer. There's no request/response. One-way streams.
- A generic message bus. Arrow `RecordBatch` payloads only.
- A service mesh. No discovery; instance addresses come from chitchat.
- A replication protocol. One sender, one receiver, point-to-point.

## The five decisions

### 1. Wire format: Arrow IPC streams over TCP

Each (sender, receiver) pair has one persistent TCP connection. The
sender writes an Arrow IPC stream (`arrow_ipc::writer::StreamWriter`);
the receiver reads the same stream (`arrow_ipc::reader::StreamReader`).

**Why:** Arrow IPC is already in the workspace, already used for
checkpoint serialization (`laminar-core::serialization`), and handles
schema evolution, nullability, and all the Arrow types. TCP gives us
ordered reliable delivery at a cost that's a rounding error on modern
networks.

**Why not gRPC:** adds `tonic` + `prost` + protobuf schema generation
for a path where both ends are our own code. The HTTP/2 framing
overhead (typically ~1% but adds latency for small batches) isn't
worth the service-definition ergonomics we don't need.

**Why not QUIC:** UDP-based, head-of-line-blocking-free, potentially
faster — but the Rust QUIC story is still young (quinn works but isn't
as battle-tested as tokio TCP), and we'd be solving a problem we don't
have (HOL blocking matters on lossy networks; we assume same-AZ LAN).

**Why not raw framed bytes (length-prefix + schema-less):** loses the
schema negotiation. First byte of pain: a stateful operator restarts
with a rolled SchemaRef and every in-flight batch has the wrong
layout.

### 2. Flow control: credit-based, bytes-denominated

Before sending, the sender must hold ≥ `batch_size_bytes` of credit
granted by the receiver. Initial credit: 16 MiB per connection. The
receiver replenishes credit by sending explicit `Credit(delta_bytes)`
control messages as it drains the batch queue.

If the sender is out of credit, it blocks on a per-connection `Semaphore`
until credit arrives. No buffering-past-capacity; the per-operator
input channel upstream absorbs transient bursts.

**Why bytes over messages:** batch sizes vary by orders of magnitude
(small control events, large window emissions). A message-count credit
under-protects against memory pressure from big batches.

**Why not just let TCP's window do it:** TCP's window controls the wire
only. It doesn't stop the *sender application* from enqueueing. Our
sender writes into a tokio buffer, which then lazily drains to the
kernel; the sender keeps enqueuing until process memory is exhausted.
We need app-level back-pressure, and credit is the simplest correct
model.

**Why not watermark-based:** credit is explicit, tunable per
deployment, and composes cleanly with the aligned checkpoint protocol.
Watermark-based flow control conflates event time with buffer
pressure and is harder to reason about.

### 3. Ordering: per-(sender, receiver) FIFO, which gives per-key FIFO

TCP guarantees FIFO on a single connection. All events for a given
`vnode_id` go to the same receiver (ownership is stable within an
assignment version). Since every key hashes to exactly one vnode, **all
events for a given key traverse the same connection in order.**

**Per-key order: guaranteed.**
**Per-vnode order: guaranteed (subset of per-key).**
**Global order across all keys on a receiver: guaranteed (single
connection per sender→receiver pair).**
**Global order across all senders: NOT guaranteed.** Event A from sender
X and event B from sender Y can interleave arbitrarily at a receiver.
This is correct for keyed operators (they partition state by key) and
the right choice for non-keyed operators (see §9.1 of the parent doc —
multi-source pipelines must be event-time driven).

**Consequence:** if a shuffle is re-routed mid-pipeline due to a
rebalance, ordering during the transition is NOT preserved for keys
that change owners. Rebalance uses the assignment-version fence to
force a barrier-aligned handover; in-flight state for a moved vnode is
drained before the new owner takes over. Covered in §7 of this doc.

### 4. Connection management: lazy, pooled, persistent

**Establishment:** when the sender needs to write its first batch to
receiver R, it opens a TCP connection to R's shuffle-listen address
(published via chitchat KV: `members/<id>/shuffle_addr`). The
connection stays open.

**Topology:** at steady state, up to N×(N−1) directed connections in a
full mesh. At N = 8 that's 56 sockets per process — fine. At N = 32
(992 sockets) we're past the design scope; deferred.

**Teardown:**

- On chitchat membership loss for R: sender drops the connection
  immediately, marks R's credit as 0, upstream operator sees the
  failure on its next send.
- On idle timeout (60 s): connection closed to release kernel state.
  Re-established lazily on next send.
- On process shutdown: graceful close; pending batches drained up to
  a bounded timeout (10 s).

**Why persistent:** connection setup (TCP three-way + initial Arrow IPC
schema exchange) costs ~1–5 ms. At 500 K events/s, per-event setup is
unviable. Pooling amortizes to zero for steady pipelines.

**Why not a full mesh established at startup:** many pairs never talk.
Kafka source 1 → Kafka source 2 has no cross-flow. Lazy avoids burning
socket count and file descriptors for empty channels.

### 5. Barrier integration: in-band, aligned

Checkpoint barriers flow through the shuffle in-band, in the same Arrow
IPC stream as data. The protocol embeds two message types in a single
discriminated union at the Arrow level:

- **Data message:** one `RecordBatch`, schema matches the negotiated
  schema for this connection.
- **Control message:** small fixed-schema batch carrying a
  `Barrier { epoch, checkpoint_id, flags }` or a
  `Credit { delta_bytes }` record.

Receivers read sequentially. On a `Barrier`, the receiver's input
channel raises an alignment event — the Chandy-Lamport receiver waits
until barriers have arrived from all upstream connections before
snapshotting and forwarding the barrier downstream. Data messages
arriving between barriers from different connections buffer behind the
alignment point.

This is the standard aligned-checkpoint protocol. Unaligned is out of
scope (§A.2 of the parent doc).

**Why in-band:** a separate control connection would have independent
ordering relative to data. Aligned checkpoints rely on the data / barrier
ordering being identical on every path; the only way to guarantee that
is in-band.

## Message format

All messages are Arrow `RecordBatch`es on the IPC stream. The first byte
of a logical message is a 1-byte tag that the receiver reads before
inspecting the batch:

```
0x01 = Data(batch)       // payload: user RecordBatch for this pipeline
0x02 = Barrier(record)   // payload: 1-row batch of [checkpoint_id: u64, epoch: u64, flags: u64]
0x03 = Credit(record)    // payload: 1-row batch of [delta_bytes: u64]
0xFF = Close(record)     // payload: 1-row batch of [reason: Utf8]  — graceful drain
```

Tag byte precedes each batch. Arrow IPC stream framing handles the rest.
Schema handshake happens at connection open: sender writes a "hello"
message (1-row batch carrying pipeline ID + data-schema fingerprint);
receiver verifies against its expected schema or closes with a
schema-mismatch reason.

**Why a tag byte ahead of the batch:** Arrow IPC streams are
schema-typed, so each batch on the stream must match the stream's
schema. Mixing Data and Control would need a union type, which is
awkward and defeats schema stability. A tag byte lets us run multiple
typed sub-streams over one logical stream without Arrow metadata
contortions.

## API surface

Concrete types, no traits:

```rust
pub struct ShuffleSender {
    connections: DashMap<InstanceId, Arc<Connection>>,
    schema: SchemaRef,
    local_id: InstanceId,
    metrics: Arc<EngineMetrics>,
}

pub struct ShuffleReceiver {
    listener: TcpListener,
    inbound: mpsc::Sender<ShuffleMessage>,  // to operator input
    metrics: Arc<EngineMetrics>,
}

pub enum ShuffleMessage {
    Data { from: InstanceId, batch: RecordBatch },
    Barrier { from: InstanceId, barrier: CheckpointBarrier },
}
```

A single `ShuffleSender` per instance multiplexes across N receivers;
a single `ShuffleReceiver` per instance demultiplexes incoming
connections. Operators hold references to both via normal dependency
injection at pipeline start.

**No `ShuffleTransport` trait.** The review lesson from Phase A applies:
one concrete implementation, extract a trait when a second transport
emerges.

## Back-pressure propagation

```
Source consumer ◀──── (credit exhausted) ──── ShuffleSender
                                                  │
                                                  ▼
                                            TCP connection
                                                  │
                                                  ▼
                                       ShuffleReceiver (slow drain)
                                            │
                                            ▼
                                   Operator input channel (full)
```

When the operator input channel is full, the receiver stops issuing
credit. The sender's per-connection semaphore blocks. The source
consumer then back-pressures its Kafka/CDC read.

This is the same pattern as Flink; it's the standard way to build
distributed stream back-pressure. The only subtle bit is that the
credit message itself is a small batch — it must not be held up by the
slow drain, which means **control messages have reserved receiver
buffer space** (32 KiB per connection) outside the data credit. No
deadlock.

## Failure cases and recovery

| Failure | Detection | Recovery |
|---|---|---|
| TCP peer drop (receiver restarted) | Connection `EOF` on read; `BrokenPipe` on write | Sender drops connection; upstream sees send error; chitchat reports peer down within ~5 s; rebalance reroutes |
| Slow receiver | Credit not replenished past timeout (30 s) | Sender logs warning; upstream operator sees back-pressure; no connection drop unless TCP keepalive fails |
| Sender crash mid-batch | Receiver sees truncated IPC stream → decode error | Receiver drops connection, flags upstream; chitchat reports sender down |
| Network partition | TCP keepalive failure after ~10 min (kernel default, we set to 30 s) | Connection drops; rebalance |
| Checkpoint barrier lost (sender died before injecting) | Receiver's alignment timeout fires | Checkpoint coordinator aborts the epoch; next checkpoint retries |

None of these are shuffle-specific inventions. Every Flink / Arroyo
shuffle has to handle the same list; nothing here is novel.

## Assignment version fence

Every shuffle message carries the sender's current `assignment_version`
(from `VnodeRegistry`). Receiver validates on arrival:

- If `msg.version == receiver.version`: process.
- If `msg.version < receiver.version`: drop silently (sender is stale;
  rebalance in progress; the new owner is about to connect).
- If `msg.version > receiver.version`: receiver is stale; poll chitchat
  for the new version, then retry validation.

This is the same fence used by `ObjectStoreBackend` for writes. Covered
at length in §2 of the parent doc; here we just note that shuffle
honors it.

## Performance targets

These are the numbers we intend to hit; they're the definition of
"done" for C.3 benchmarks:

- **Per-connection throughput:** ≥ 1 GiB/s on loopback, ≥ 500 MiB/s LAN
  (10GbE).
- **Per-event end-to-end added latency (sender send → receiver consume):**
  p50 < 200 µs, p99 < 2 ms on LAN at low load.
- **Barrier propagation:** p99 < 10 ms under low load; under full
  back-pressure, bounded by the slowest input's drain rate (this is
  fundamental to aligned checkpoints).
- **Memory ceiling:** per-connection, 16 MiB credit + 32 KiB control +
  ~1 MiB TCP kernel buffer ≈ 17 MiB. At N = 16 peers, per-instance
  ceiling ≈ 270 MiB. Tunable via config.

## What we're NOT doing in v1

- **Compression.** Bandwidth isn't the bottleneck at our target rates.
  Add per-connection zstd later if benchmarks prove otherwise.
- **Encryption.** v1 assumes a trusted LAN. TLS later, probably via
  `rustls` + mutual auth from a shared CA.
- **Custom congestion control.** Stock TCP CUBIC is fine at our scale.
- **Batching at the shuffle layer.** Batches arrive pre-sized from the
  upstream operator; the shuffle does not re-batch.
- **Multiplexing multiple logical streams over one connection.** One
  pair = one connection. Multiplex later if connection count becomes a
  problem (past 16–32 instances).
- **Sidecar control connection.** Barriers and credits flow in-band.
- **Cross-DC / WAN shuffle.** Assumes same-AZ latency.

## Open questions to resolve before C.3 code lands

1. **Listener address publication.** Chitchat KV is `members/<id>/…`.
   Do we use a single `shuffle_addr` entry (works fine; one address per
   instance) or separate by protocol version (`shuffle_addr_v1`)? Go
   with single entry; version the handshake instead.
2. **Max batch size.** Arrow IPC has no hard upper bound but very large
   batches hurt memory. Proposal: reject batches > 64 MiB at the
   sender with an operator-level error. Splits are the upstream
   operator's problem.
3. **Schema evolution across restarts.** If an operator's output
   schema changes between versions, the receiver rejects. Is this the
   right behavior, or should we attempt field-by-field compatibility?
   For v1: reject, emit clear error, force coordinated restart. Field-
   by-field compat is future work and would need its own design.
4. **Metrics.** Beyond the three already in `engine_metrics.rs`, we'll
   want shuffle-specific counters (bytes/messages per connection, credit
   depletion events, reconnect counts). Land with the operator in C.3;
   use the `metric-by-label` pattern so we don't duplicate per-peer.
5. **Graceful shutdown ordering.** Closing a connection mid-barrier
   breaks alignment. Sender's shutdown must inject a final `Close`
   message after any in-progress barrier. Implementation detail; call
   out in the code.

## What C.3 builds

With this doc as the contract:

- `crates/laminar-core/src/shuffle/` module
- `Connection` — one TCP pair, Arrow IPC framed
- `ShuffleSender` + `ShuffleReceiver` concrete types
- Credit-flow logic
- Chitchat KV integration for `members/<id>/shuffle_addr`
- Barrier in-band propagation wired to `CheckpointBarrierInjector`
- Assignment-version fence on send/receive
- Failure injection test harness (per the failure-scenario matrix in
  the parent doc's Phase C.5)

Estimated effort matches the parent doc's C.3 line: 1–2 weeks of
focused implementation + test after this design lands.

## Revision history

- **v1 (this doc):** initial lock. Arrow IPC over TCP; credit-based
  bytes-denominated flow control; persistent lazy-pooled connections;
  in-band aligned barriers; assignment-version fence.

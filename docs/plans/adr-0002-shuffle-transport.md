# ADR 0002: Shuffle transport — keep hand-rolled or adopt arrow-flight

**Status:** open (needs benchmarks)
**Context date:** 2026-04-19

## Problem

`crates/laminar-core/src/shuffle/` is ~900 LOC of hand-rolled TCP
transport:

- `message.rs` (300 LOC): `[u32 length][u8 tag][payload]` framing,
  Arrow IPC embedded in the Data payload.
- `transport.rs` (580 LOC): connection pool, Hello handshake, liveness
  tracking, KV-based peer discovery.

Apache Arrow Flight exists. It is a gRPC-based streaming protocol
purpose-built for `RecordBatch` transport. DataFusion's Ballista (the
upstream's own distributed executor) uses it. Influx and several other
Arrow-centric systems use it in production.

## The question

Is the custom transport's complexity budget justified? Every LOC we own
is LOC we have to secure (TLS), instrument (metrics), debug (Phase 2.2
was a hand-rolled liveness fix), and evolve (credit flow, backpressure).

Alternatives:

- **Keep custom transport.** Gives us tight control over framing and
  low per-frame overhead. Potentially better tail latency.
- **Adopt arrow-flight.** Delete ~700 LOC. Get TLS, connection
  keepalive, auth hooks, and stream-level backpressure for free. Lose
  tail-latency control; pay gRPC/HTTP2 framing overhead.

## Decision framework

Not made yet. The right answer depends on benchmarks we haven't run:

1. **Per-frame latency** on loopback and LAN, custom vs arrow-flight.
   If arrow-flight is within ~20% of custom, adopt it.
2. **Memory overhead** per connection. gRPC keeps more per-stream
   state; if we're fanning out to 100+ peers, that matters.
3. **Throughput under batch-heavy workloads.** A custom codec can
   amortize framing overhead differently than gRPC message framing.

## Work breakdown if we adopt

1. Define a Flight service for the shuffle: `DoPut` for outbound
   VnodeData, a `DoAction` or `DoGet` for barriers.
2. Replace `ShuffleSender` / `ShuffleReceiver` with Flight clients /
   servers.
3. Map `ShuffleMessage::{Hello, VnodeData, Barrier, Close}` onto Flight
   message types. Hello goes into Flight's handshake; Barrier may need
   `DoExchange` bidirectional.
4. Delete `shuffle/transport.rs`, most of `shuffle/message.rs` (keep
   the barrier frame codec only if needed).
5. Fold Phase 2.2 (stale-connection purge) away — gRPC/tonic handles
   reconnection.

Estimate: 1.5 weeks once benchmarks justify.

## Current stance

Defer. The custom transport works and Phase 2.2 closed the most recent
real bug. Revisit when:

- We need TLS (Phase 4): arrow-flight is free, custom is several
  hundred LOC of rustls plumbing.
- We hit a bug that would have been prevented by a mature library.
- Benchmarks show <20% latency delta.

## Consequences of deferring

- We own every liveness / framing / security bug in `shuffle/*`
  ourselves.
- Feature gap vs Ballista grows over time.
- Complexity budget stays spent on transport.

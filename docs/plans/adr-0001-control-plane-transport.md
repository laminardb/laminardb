# ADR 0001: Control-plane transport for the barrier protocol

**Status:** proposed
**Context date:** 2026-04-19

## Problem

The current `BarrierCoordinator` writes JSON-encoded barrier announcements
and acks into a chitchat gossip KV (`crates/laminar-core/src/cluster/control/barrier.rs`).
Followers discover announcements via `observe_barrier`, the leader polls
for acks via `wait_for_quorum` with `tokio::time::sleep(50ms)` between
scans. Two problems:

1. **Chitchat is SWIM**. State converges in O(log N) gossip rounds — in
   our current config, a gossip interval of 1s — so a per-checkpoint 2PC
   protocol pays at least one gossip round-trip per phase. With Prepare
   and Commit both flowing through gossip, that's ~2s of floor latency
   added to every checkpoint.

2. **50ms poll loops compound this.** `wait_for_quorum` and the
   follower's decision wait both tokio-sleep between scans. Even with
   instant gossip, each checkpoint pays ~100ms of polling overhead.

Flink (Netty/Akka), RisingWave (tonic), and Arroyo (tonic) all use
direct RPC for barriers. Gossip is appropriate for membership and
phi-accrual failure detection; it is the wrong tool for time-sensitive
control messages.

## Proposal

Port the barrier coordination to a dedicated tonic bidirectional
streaming RPC. Keep chitchat for membership, health, and the
`SHUFFLE_ADDR_KEY` + other slowly-changing metadata. Design sketch:

```protobuf
service ClusterControl {
  // Leader opens a stream per follower. Leader pushes Announce; follower
  // replies with Ack on the same stream. Keepalive + reconnection
  // handled by tonic.
  rpc Barrier(stream FollowerAck) returns (stream BarrierAnnouncement);
}
```

- **Transport:** tonic gRPC over TCP. TLS when `cluster-tls` feature is
  on (Phase 4 lands there anyway).
- **Encoding:** protobuf. Drops the JSON-on-checkpoint-path inconsistency
  with the rest of the engine's rkyv-everywhere state encoding.
- **Latency:** sub-millisecond for Prepare → Ack → Commit on the same
  subnet; bounded by TCP RTT, not gossip convergence.
- **Failure model:** tonic surfaces stream breaks as errors the
  coordinator can react to directly. No poll loops.

## Work breakdown

1. Define the proto, wire tonic server + client.
2. Replace `ClusterKv` seam with a `ControlRpc` trait; keep the
   chitchat-based impl as a fallback for the membership path.
3. Rewrite `BarrierCoordinator::wait_for_quorum` as an event-driven
   await over acks arriving on the stream (no polling).
4. Migrate `observe_barrier` to be a stream `recv()` instead of a KV
   read.
5. Delete the barrier-over-chitchat code path.

Estimate: 2 weeks. Touches roughly `laminar-core/cluster/control/*` and
`laminar-db/src/checkpoint_coordinator.rs` follower/leader branches.

## Alternatives considered

- **Keep chitchat, drop poll interval to 10ms.** Reduces polling floor
  but still pays the gossip convergence cost. Does nothing for the
  JSON overhead. Rejected.
- **Use Raft (openraft) for leader election + 2PC.** Overkill — we
  don't need log replication, we need request/response. Would add
  significant operational complexity.
- **tarpc.** Reasonable, but tonic is the Rust streaming-RPC default;
  Arroyo and many others use it; the ecosystem around it is deeper.

## Consequences

- Adds tonic as a workspace dep.
- Cluster binary size increases (tonic + prost).
- Checkpoint latency drops by ~seconds.
- Follower decision latency becomes sub-RTT instead of poll-bounded.
- Natural path for TLS (Phase 4) via tonic's built-in rustls support.

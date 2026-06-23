# Shuffle-barrier alignment doesn't recover after a node kill -9 + respawn

Pre-existing cluster-recovery bug, surfaced by the Lever-2 delta soak (which added a
distributed aggregate → a cross-node shuffle stage). **Not a delta bug** — reproduces with
delta off (`LAMINAR_SOAK_AGG=1`, no `LAMINAR_SOAK_DELTA_CHAIN_MAX`). Exactly-once-adjacent;
fix carefully and validate with the kill-9 soak before touching alignment again.

## Symptom (reproduced 2026-06-22)
`cluster_soak.rs::three_node_kill9_soak` with a distributed agg: after the kill rounds, every
checkpoint aborts on the leader with
`[LDB-8002] shuffle barrier alignment timed out for checkpoint N (waiting on {<peer instance id>})`,
so `cluster_commits` never advances and the test fails at `progress after rejoin: cluster commits
to reach 5`. The waited-on peer is **fully respawned and healthy** (its `GET /metrics` returns 200,
gossip state `active`, generation stable) — yet its shuffle barrier never reaches the leader.
Pass-through kill-9 soaks (no shuffle stage) were always green, which is why this was never seen.

Repro:
```
# reset Redpanda first (kill-9 debris degrades it — recreate the laminar-infra redpanda-0/1/2)
LAMINAR_SOAK_KAFKA_BROKERS=127.0.0.1:29092 LAMINAR_SOAK_AGG=1 LAMINAR_SOAK_SECONDS=90 \
  cargo test -p laminar-server --test cluster_soak three_node_kill9_soak -- --ignored --nocapture
```
(OpenSSL + ORT env from the `windows-test-openssl-env` memory.)

## Root cause (established) and what is NOT the cause
- Node identity (`instance_id` = `node-{id}`, e.g. `16059159448129433833`) is **stable across
  respawn** — it's derived from the config `node_id`, not the process. So the cluster sees a killed
  node return as the *same* logical node with a new gossip generation + **new random shuffle/barrier
  ports** (the barrier-sync and shuffle gRPC servers bind ephemeral ports each process start).
- `align_shuffle_barriers` (`crates/laminar-db/src/operator_graph.rs`, ~`fn align_shuffle_barriers`)
  builds the wait set from `controller.live_instances()` and prunes peers not in `live_now` on a
  500ms tick (`RECHECK`), with `ALIGN_TIMEOUT = 8s`. The respawned peer's "active" windows are
  ~20–38s, so an 8s alignment falls entirely inside an active window → the peer is never pruned →
  the leader waits the full 8s and times out. But pruning it is the *wrong* answer post-rejoin: the
  peer is a healthy participant that *should* send a barrier.
- **NOT the cause:** generation-blind pruning. A speculative fix (add `ClusterKv::generation_of` +
  `ClusterController::peer_generation` + prune peers whose generation advanced mid-alignment) was
  tried and **reverted** — the soak failed identically because by the post-rejoin alignments the
  peer's generation is already stable. Do not re-attempt this; the barrier *should* arrive.

So the real question: **why does a respawned, running peer's shuffle barrier never reach the leader?**

## Transport mechanics (the suspects)
`crates/laminar-core/src/shuffle/transport.rs`:
- `ShuffleSender`: per-peer pool keyed by `ShufflePeerId`. `connection_for` returns the cached
  `PeerConn` if `is_alive()`, else purges and re-resolves via `discover_peer` (reads `SHUFFLE_ADDR_KEY`
  newest-generation → the peer's *current* address) and `open_call` (client-streaming gRPC; first
  frame is `Hello(local_id)`). `PeerConn`'s driver flips `alive=false` on the first transport error.
- `ShuffleReceiver` (~`struct ShuffleReceiver`): server side; each inbound call's first frame is the
  sender's `Hello(node_id)`; decoded messages surface on a bounded queue. Advertises its bound addr
  to gossip under `SHUFFLE_ADDR_KEY`.

Hypotheses to instrument (the alignment is symmetric — every node fans its barrier to peers and waits
to *receive* each peer's barrier):
- **H1 — leader→peer fan-out into a dead stream.** `connection_for(peer)` returns a cached `PeerConn`
  whose `is_alive()` is still true (a gRPC half-open to a kill-9'd process may not error until a send
  actually fails; the driver buffers `ShuffleMessage`s, so the failure may be swallowed without
  flipping `alive`). The leader fans its barrier into the void → the peer never sees it. (Doesn't fully
  explain it alone: the peer should align off the gossip `Prepare` and send its *own* barrier anyway.)
- **H2 — peer→leader on a stale leader address.** The respawned peer (fresh empty sender pool) opens a
  new call to the leader via `discover_peer(leader)`; if it resolves a stale leader addr it sends into
  the void. `read_from` picks the newest generation, so this *should* be correct — verify.
- **H3 — receiver doesn't replace a reconnecting peer's stream (most likely).** When the respawned peer
  opens a NEW inbound call with `Hello(peer_id)`, does the receiver cleanly *replace* the peer's prior
  (dead) stream/registration, or ignore the duplicate / keep routing to the dead one? A receiver that
  keys per-peer state by `ShufflePeerId` and doesn't reset it on a fresh `Hello` would drop the new
  stream's barriers.
- **H4 — the respawned peer never emits a barrier.** Its pipeline/shuffle operator restarts but doesn't
  observe the leader's `Prepare` (gossip `observe_barrier`) or isn't aligned to emit — check the
  follower align path in `pipeline_callback.rs` (`align_shuffle_for_leader` is leader-only; find the
  follower side) and whether a just-recovered node participates.

## Plan
### Phase 0 — Reproduce + instrument (no behavior change)
Add temporary `tracing` (debug) on both sides, keyed by `(checkpoint_id, peer_id)`:
- Sender: every `fan_out_barrier` target + whether `connection_for` reused a cached conn or reconnected
  + the resolved addr; log when a `PeerConn` flips `alive=false`.
- Receiver: every inbound `Hello(peer_id)` (with remote addr) and every `Barrier(checkpoint_id)` surfaced,
  tagged with which peer it came from.
- Alignment: log the `remaining` set each tick + each peer's resolved shuffle addr.
Run the repro; pinpoint exactly where the respawned peer's barrier is lost (no Hello? Hello but no
barrier? barrier sent but never received? sent to a stale addr?).

### Phase 1 — Fix the transport reconnection
Most-likely fixes (confirm against Phase 0):
- **Receiver:** on a fresh `Hello(peer_id)` from a peer that already has live per-peer state, drop/replace
  the stale stream so the new one is authoritative (clean reconnect).
- **Sender:** make `is_alive()` / the driver detect a remotely-closed or stale connection promptly, OR
  proactively invalidate the pooled `PeerConn` for a peer whose `SHUFFLE_ADDR_KEY` changed (respawn ⇒ new
  port) — re-resolve and reconnect. Tie invalidation to a membership/address-change signal rather than
  waiting for a send to fail.
- Keep the change minimal and on the transport, not the alignment — the alignment is correct once the
  barrier actually flows.

### Phase 2 — Validate
- The kill-9 delta soak must go green:
  `LAMINAR_SOAK_KAFKA_BROKERS=127.0.0.1:29092 LAMINAR_SOAK_DELTA_CHAIN_MAX=6 cargo test -p laminar-server
   --test cluster_soak three_node_kill9_soak -- --ignored --nocapture` (reset Redpanda first).
- Also green with delta off (`LAMINAR_SOAK_AGG=1`) — the bug is delta-independent.
- Add an in-process regression in `cluster_integration.rs`: a 2-node shuffle, kill+respawn one node
  (or simulate a peer reconnecting with a new shuffle address), assert alignment completes and commits
  resume — so this can't regress without a real broker.

## Invariants / cautions
- Don't prune a healthy participant from the wait set to "fix" liveness — that risks dropping a barrier
  a real in-flight checkpoint needed (EO correctness). Fix the transport so the barrier arrives.
- A node that restarts keeps its identity (config `node_id`); the transport must tolerate a stable-id
  peer reappearing on a new address. (An alternative — per-process identity/generation so a restart is a
  clean evict+join — is a larger model change; out of scope unless Phase 0 shows it's the cleanest path.)

## Anchors
- `crates/laminar-core/src/shuffle/transport.rs` — `ShuffleSender::connection_for` / `discover_peer` /
  `PeerConn::is_alive` / `open_call`; `ShuffleReceiver` (Hello handling, `recv`, queue).
- `crates/laminar-db/src/operator_graph.rs` — `align_shuffle_barriers` (wait set, `RECHECK`, `ALIGN_TIMEOUT`).
- `crates/laminar-db/src/pipeline_callback.rs` — `align_shuffle_for_leader` (leader) + the follower align path.
- `crates/laminar-core/src/cluster/control/controller.rs` — `live_instances()`.
- Related memory: `cross-node-barrier-alignment`, `cluster-failover-hardening`.

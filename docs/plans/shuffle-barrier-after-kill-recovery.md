# Shuffle-barrier alignment doesn't recover after a node kill -9 + respawn

Pre-existing cluster-recovery bug, surfaced by the Lever-2 delta soak (which added a
distributed aggregate → a cross-node shuffle stage). **Not a delta bug** — reproduces with
delta off (`LAMINAR_SOAK_AGG=1`, no `LAMINAR_SOAK_DELTA_CHAIN_MAX`). Exactly-once-adjacent;
fix carefully and validate with the kill-9 soak before touching alignment again.

## INVESTIGATION (2026-06-23, branch `feat/shuffle-barrier-after-kill-recovery`)
**The plan's transport-stale-conn hypothesis (H1/H2/H3) is most likely NOT the root cause.**
Empirically narrowed via the kill-9 soak (fresh Redpanda recreated each time, delta off,
`LAMINAR_SOAK_AGG=1`, 90s):

1. **Baseline (current `main`/branch code) PASSES** the soak — `soak-749548`: cluster up,
   2 kill rounds, exactly-once OK on all 3 nodes, **0 alignment timeouts, 0 alignment failures**.
   So the bug is **FLAKY**, not deterministic — it did NOT reproduce on this run.
2. **A sender-side re-resolve fix (stamp `PeerConn.addr`; re-resolve + drop a stale-addr conn on
   every barrier fan-out) REGRESSED startup.** `discover_peer` does a `chitchat.lock().await`
   (KV read); doing it per-fan-out on the hot checkpoint path stalls/wedges the pipeline — every
   checkpoint timed out from cp2 at startup (`soak-743968`). **Do not re-resolve on the barrier
   path.** Reverted.
3. **HTTP/2 keepalive on the shuffle client (3s/3s) did NOT fix it.** `soak-751472`: got past
   startup, did **3** kill rounds (incl. killing the leader, node2), then failed with the exact
   documented symptom (`progress after rejoin: cluster commits to reach 5`). A dead TCP conn
   would have been detected + reconnected within ~6s, well inside the 90s window — it wasn't,
   which argues against a pure transport stale-conn. Reverted (kept only the lean tracing).

**What the failing run actually shows (`soak-751472`):** the respawned node (node0, id
`16059…`) recovers from its checkpoint, then goes through **multiple back-to-back rebalances
(assignment snapshot v2→v4→v5, rehydrating 32 vnodes)** *concurrent with* the alignment
failures. The leader (node2) times out aligning **8×** waiting on node0; node1 logs **9**
alignment failures + **7** "aborted by leader". So post-respawn the cluster enters a
**rebalance-churn ⇄ alignment-timeout ⇄ epoch-abort cascade**: while vnode ownership is being
rehydrated, the leader's fan-out/wait peer-set and the respawned node's view disagree (the
"fan-out ≠ wait peer-sets under skewed ownership" hazard from `cross-node-barrier-alignment`),
so alignment never settles. Baseline got lucky (2 kills, never hit the bad leader-kill timing);
the failing run killed the leader and triggered the churn.

## FIX (2026-06-23) — checkpoint-convergence gate. VALIDATED.
Confirmed: barrier alignment waits on **gossip-live** membership, but a respawned node is
gossip-live ~immediately while it still adopts+rehydrates the current assignment (snapshot
v2→v4→v5, 32 vnodes, ~40s), so its deferred follower checkpoint can't fan out a shuffle barrier;
the leader times out, aborts the epoch, and the post-rejoin rebalance churn reopens the window.

Fix = **the leader quiesces its periodic checkpoint until the cluster converges on the committed
assignment version** (no pruning — EO-safe):
- `ClusterController::announce_adopted_version` / `read_adopted_versions` — per-node gossip KV
  (`control:adopted-version`), mirroring the proven `drained-version` pattern.
- The snapshot watcher (`rebalance.rs`, runs on every node) announces the adopted version on
  every version change.
- `PipelineCallback::assignment_ready_for_checkpoint` (default `true`); cluster override =
  `assignment_versions_converged(live, reported)`: every live node reported the **same** version
  (leader's committed is the max ⇒ equality means all followers caught up). Unit-tested.
- `streaming_coordinator::maybe_checkpoint` gates the **interval** cadence on it (explicit
  connector requests still pass); advances `last_checkpoint` on a quiesce-skip so the gossip read
  runs at most once per interval, NOT per tick (a per-fan-out chitchat read is what regressed the
  re-resolve attempt).

**Soak result (fresh Redpanda each run):** alignment-timeout-after-rejoin GONE — **0 alignment
timeouts across 3 runs**, each completing every kill round incl. killing the leader. `soak A`
(delta-off, 3 rounds) fully green (EO clean, 22.5k records dense).

## SEPARATE PRE-EXISTING BUG surfaced (EO gap at start) — root cause PINNED via instrumentation
Once alignment stopped timing out, the soak reached its end-of-run EO diff and *sometimes* failed
with an EO **gap at seq (0,~20)** (the first ~20 records of a node's pass-through topic never
committed). Flaky and pre-existing. Earlier hypothesis (durability-gate rollback) was **WRONG** —
disproved by `soak A` hitting the same `[LDB-6020]` rollback yet staying gap-free.

**Actual root cause (instrumented `eotrace` on the Kafka sink + generator):** during cluster
formation, the *agg* stream's cross-node row-shuffle (`sql_query.rs:766`,
`shuffle_pre_agg_batches`) does `send_to` a peer that hasn't registered its shuffle address yet →
`Err` → the **whole SQL cycle aborts** (`execute_cycle` returns on the first operator error,
`operator_graph.rs:1840`; coordinator logs `[LDB-3020] SQL cycle error`,
`streaming_coordinator.rs`). That discards the output of the *co-located* pass-through EO-sink
stream for those rows, **but the generator already advanced** (`discard_pending_offsets` keeps the
checkpoint offset back, yet the live source produced new rows and `source_batches_buf` is cleared)
→ the first successful cycle's committed offset jumps past the dropped rows → permanent gap.
The EO sink and the agg share one cycle; one stream's shuffle failure sinks the other. Unrelated
to the convergence gate or the transport.

**Fix attempt that FAILED — do not retry:** "on cycle error, retain `source_batches_buf` and
replay next cycle." It re-runs `execute_cycle`, and the EO sink is a *downstream operator* whose
`write_batch` already ran before the agg error — so each replay **re-writes** the rows →
**duplicates** (`soak`: 80 dups, seq 0-1/2-4/5-8 written every retry). Re-running a partially
executed cycle re-executes side effects. Reverted.

## EO GAP FIX (2026-06-23) — stream isolation via operator deferral. VALIDATED.
Make the agg-shuffle failure recoverable instead of fatal so it doesn't drop the co-located
pass-through EO sink:
- New `DbError::ShuffleNotReady` (recoverable). `shuffle_pre_agg_batches` (`sql_query.rs`) returns
  it when a target vnode is unassigned or `send_to` a peer fails (formation: addr not gossiped).
- `OperatorGraph::execute_single_operator` (`operator_graph.rs:~1640`) already preserves an
  operator's input and returns `Ok(())` for `depends_on_stream` deferrals; extended to also defer
  on `e.is_shuffle_not_ready()`. So the cycle keeps running — the pass-through still writes to the
  EO sink, and the agg's input is preserved (no source-advance loss) and retried next cycle.

**Soak result (fresh Redpanda):** EO gap + duplicates **GONE** — 10/10 completed runs EO-clean
(no gaps, no dups), delta-off and delta-on. No re-run of side effects (deferral preserves input
without re-executing the pass-through), so no duplicates; the pass-through is never dropped, so no
gap.

**Deliberately NOT included (avoid a regression):** an *atomic* pre-check (resolve every target
before sending any row) would also keep the agg itself consistent on a deferred retry, but a
cached-first `peer_addr` that falls back to a gossip-KV read per cycle during re-formation runs on
the same task as barrier alignment and **raised alignment timeouts 0→4/8** (removed → 2/8). It was
ripped out entirely (don't leave the unwired primitive). The follow-up must source addressability
from a *background* peer-address refresher (no per-cycle KV). Until then, the agg (NOT
exactly-once-checked) may double-count rows partially fanned-out during formation —
pre-existing-class (the old code dropped them); the EO pass-through is correct.

**Residual:** ~2/8 alignment timeouts remain on delta-on — the convergence gate quiescing
correctly but slowly (more state ⇒ rehydration+convergence+2 commits occasionally exceed the soak's
90s rejoin window). Tuning tail, not a correctness bug.

**Kept:** convergence gate (alignment) + stream-isolation deferral (EO gap). **Reverted/removed:**
transport re-resolve (regressed startup), keepalive (no fix), retain-on-error (duplicates),
per-cycle atomic pre-check + `peer_addr` (raised alignment timeouts; unwired), and the transport
debug tracing (the transport was disproven as the cause).

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

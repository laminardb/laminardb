# Two-Phase Checkpoint Ordering

**Status:** design lock for Phase B of `distributed-stateful-pipelines.md`.
**Scope:** the exact ordering between sink precommit, state durability, and
sink commit during a distributed checkpoint. Answers the question the SB-08
fix entry deferred: "when exactly does `state.epoch_complete()` get called,
and what happens when it returns `false`?"

## The invariant we're protecting

**Sinks must not commit observable output for epoch N until every operator's
state for epoch N is durable.**

If a sink commits first and state isn't durable, a crash before state
commits leaves the world in an "output produced, state lost" shape.
On recovery, the operator replays from the previous checkpoint and
re-emits what was already committed downstream. That's a duplicate, and
exactly-once is violated.

Getting this right is the whole reason a distributed checkpoint needs
two phases.

## The ordering

```
 ┌──────────────────────────────────────────────────────────┐
 │                       per epoch N                        │
 ├──────────────────────────────────────────────────────────┤
 │ 1. Barrier injected at sources                           │
 │ 2. Operators snapshot their state → writes to backend    │
 │ 3. Sinks precommit  (e.g., Kafka open transaction)       │
 │ 4. Coordinator persists manifest                         │
 │ 5. state.epoch_complete(N, vnodes) = durability gate     │◀── SB-08
 │ 6. Sinks commit     (e.g., Kafka commit transaction)     │
 │ 7. Epoch advances to N+1                                 │
 └──────────────────────────────────────────────────────────┘
```

Step 5 is the load-bearing addition from SB-08. It asks the state backend:
"have all vnodes in `vnodes` persisted their partial for epoch N?"

- **Single-instance, non-durable backend (`InProcessBackend`):** trivially
  true. No peers, no disk, no coordination — the check is a no-op but
  validates that the call shape works.
- **Single-instance, durable backend (`LocalBackend`):** the check verifies
  the local filesystem actually holds the partials. A filesystem write
  that hasn't flushed fails this check.
- **Distributed (`ObjectStoreBackend`):** the check verifies every peer
  instance has committed its partials to shared storage. This is the
  version that matters.

## Failure handling per step

| Step | What fails | Coordinator action |
|------|-----------|-------------------|
| 2 — state snapshot | Backend I/O, serialization | Abort checkpoint. Rollback sinks that already precommitted. Epoch does not advance. |
| 3 — sink precommit | Sink-specific (Kafka `beginTransaction` failed, DB connection dropped) | Abort. Rollback the sinks that *did* precommit. Epoch does not advance. |
| 4 — manifest persist | Object store write failed | Abort. Rollback sinks. Epoch does not advance. |
| 5 — `epoch_complete` returns `false` | A vnode didn't persist; peer is behind | Abort. Rollback sinks. Log which vnodes are missing. Epoch does not advance. The next checkpoint retries. |
| 5 — `epoch_complete` returns `Err` | Backend unreachable / permission denied | Treat as `false`. Abort + rollback + retry. |
| 6 — sink commit | Transaction commit failure on one or more sinks | Record per-sink failure in the manifest. Epoch does not advance. Next recovery uses `sink_commit_statuses` to replay commit on Pending/Failed sinks. |

The key correctness property: **rollback happens before the window where
a partial commit could leak output.** Between steps 3 and 6, nothing is
observable downstream; rollback is clean.

## Why "all vnodes" rather than "my vnodes"

A coordinator runs per LaminarDB instance. It knows which vnodes its own
operators wrote to — but the `epoch_complete` check for exactly-once needs
to cover **all** vnodes the pipeline is sharded across, because a peer
instance's failure to persist is equally fatal.

In the cluster design (§7 of `distributed-stateful-pipelines.md`), the
elected leader drives the commit decision. Only the leader calls
`epoch_complete`; non-leader instances observe the announcement and
ack via chitchat KV. The leader's `vnodes` argument is the full registry
range.

For Phase B (single-instance), the `vnodes` argument is either empty
(skip the check) or `[0..vnode_count)` (check all locally-owned slots).
Either produces the same result — the check is satisfiable by the
only writer.

## Interval floor: why 2 seconds in cluster mode

Step 4 (manifest persist) and step 5 (durability gate) each involve at
least one object-store round-trip when running on S3. At ~100 ms per
call plus the operator snapshot time, a full two-phase cycle costs
~1–3 seconds in practice (see §A.1 of the parent design doc for the
per-step budget).

Attempting to run this cycle more frequently than every ~2 seconds
means the coordinator spends more than half its time on coordination
overhead rather than forward progress. `CheckpointConfig::interval <
2s` is rejected at startup in cluster mode with a pointer to this
section.

Single-instance (embedded or standalone) doesn't have this constraint
because steps 4 and 5 are local; the default 10 s cadence is kept but
shorter intervals are allowed.

## What Phase B actually lands

1. `CheckpointCoordinator` gains an optional
   `state_backend: Option<Arc<dyn StateBackend>>` field and a
   `vnode_set: Vec<u32>` that defaults to empty.
2. Between step 4 (manifest persist) and step 6 (sink commit), the
   coordinator calls `state_backend.epoch_complete(epoch, &vnode_set)`
   if the backend is present. A `false` or `Err` returns trigger sink
   rollback and a failed-checkpoint result.
3. `ServerConfig::validate` rejects `checkpoint.interval < 2s` when
   `mode = "cluster"`.
4. Four metrics in `engine_metrics.rs`:
   - `laminardb_checkpoint_duration_seconds{phase=...}` (already exists,
     just add per-phase labels)
   - `laminardb_barrier_alignment_wait_ms{operator=...}` (new)
   - `laminardb_watermark_propagation_ms` (new)
   - `laminardb_pipeline_e2e_latency_seconds{pipeline=...}` (new)

Phase C wires the `vnode_set` from `VnodeRegistry` and starts populating
partials via per-operator `write_partial` calls. At that point, the
gate becomes load-bearing instead of trivially satisfied.

## What Phase B deliberately does NOT land

- Unaligned checkpoints. Aligned only; see §A.2 of the parent doc.
- Adaptive cadence based on observed overhead.
- Per-operator durability checks. `epoch_complete` operates at the
  vnode level, not per-operator.
- Cross-source transactions. Each sink commits its own transaction;
  the coordinator orchestrates the ordering.

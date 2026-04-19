# ADR 0003: DistributedAggregateRule starves the row-shuffle drain

**Status:** accepted (rule uninstalled in production pending fix)
**Context date:** 2026-04-19

## Problem

`DistributedAggregateRule` rewrites DataFusion's
`RepartitionExec::Hash` between `AggregateExec::Partial` and
`AggregateExec::FinalPartitioned` into a `ClusterRepartitionExec`
wrapped by `CheckpointedRepartitionExec`. When installed, the rule
fires during `CREATE MATERIALIZED VIEW`'s schema-extraction
`df.collect()`. The resulting `ClusterRepartitionExec::dispatch_inbound`
task takes the `ShuffleReceiver` mutex via `.recv().await` and holds
it for the engine's lifetime.

The row-shuffle bridge (`shuffle_pre_agg_batches` in
`crates/laminar-db/src/operator/sql_query.rs`) also wants to drain
the same receiver, using `drain_available` / `try_lock`. `try_lock`
fails because the rule's `dispatch_inbound` never releases.

Net result: if both paths are installed, the row-shuffle drain is
starved — local aggregation proceeds but remote partials never make it
into the accumulator.

## Current stance

The production `laminar-server` (`crates/laminar-server/src/cluster.rs`)
no longer installs the rule. Streaming aggregates go through the
row-shuffle bridge (`IncrementalAggState` + `shuffle_pre_agg_batches`),
whose state persists via the existing operator-checkpoint mechanism —
not via the rule's `CheckpointedRepartitionExec`.

The rule and the exec stay in `laminar-sql` for future non-streaming
distributed aggregation (the `CachedPlan` path), once this conflict
is fixed.

## Proposed fix (deferred)

**Option A: Topic-multiplex the receiver.** One `ShuffleReceiver` per
socket, multiple logical subscribers. Introduce a `Subscriber` handle
with a filter (by message tag or by vnode range). `dispatch_inbound`
claims "CheckpointedRepartitionExec messages"; row-shuffle claims
"row-shuffle messages". Each subscriber gets its own channel; the
accept-side reader fans out by filter.

**Option B: Unify the paths.** Don't have two drain paths. Row-shuffle
is the streaming default; `CheckpointedRepartitionExec` is for
non-streaming. Only install one at a time based on query shape.

**Option C (minimal):** Expose a `drain_without_lock` variant on
`ShuffleReceiver` backed by a lock-free queue. This is invasive — the
receiver currently owns the mpsc rx behind a mutex for the single-
consumer case.

**Preferred direction:** A, because it makes the receiver a cleaner
primitive — multi-subscriber channels are a well-trodden pattern — and
doesn't force one path to own the socket.

## Consequences of deferring

- Row-shuffle is the only working streaming-aggregate path.
- State recovery on restart goes through
  `aggregate_state/checkpoints.rs` (per-operator JSON/rkyv checkpoint),
  not through `CheckpointedRepartitionExec::with_recovery_epoch`.
- The `set_recovery_epoch` plumbing on `DistributedAggregateRule` is
  test-only until this ADR is resolved.

# Incremental delta checkpoints (Lever 2) — aggregate operator

Per-vnode incremental checkpoint state for `IncrementalAggState`: write only the
groups that changed since the chain base instead of a full O(keyspace) columnar
dump every epoch. Cluster-only, exactly-once-critical.

## Levers (context)
- **Lever 1 (landed, prod):** whole-vnode byte-identity *reference* partials —
  `VnodePartial{base_epoch=Some, operators=[]}`; 1-hop recovery. Re-bases before
  the base ages out of the prune window (`max_ref_age = max_retained`).
- **Lever 2 (this plan):** per-key *delta* partials — changed groups + tombstones
  since the parent epoch; N-hop chain recovery.

## Landed before Phase 3 (commit #444)
- **Phase 1** — per-vnode dirty tracking: `dirty_keys_by_vnode` / `removed_by_vnode`,
  populated at every mutation site, gated on `delta_vnode_count.is_some()` (set only
  when `delta_enabled`, cluster). Reset on capture. Bookkeeping only.
- **Phase 2** — codec: `encode_delta_for_vnode(v) -> AggVnodeDelta{changed, tombstones_ipc}`
  + `apply_delta` (REPLACE changed per key, remove tombstones). `#[cfg(all(test, cluster))]`,
  not on the prod path. `delta_enabled` has **no production caller** yet.

## Phase 3 — emission policy + chain recovery (this work)

Model: **incremental per-epoch deltas, chained**. Each delta = keys changed in that
epoch (dirty sets reset each capture, as today). Recovery replays base FULL + ordered
deltas. Bounded by `delta_chain_max` (clamped `< max_retained`) so the base never ages
out of the prune window.

### 3a. Format (`vnode_partial.rs`)
Add a delta variant. Three kinds, derived:
- FULL: `operators` non-empty, `base_epoch=None`, `deltas=[]`.
- REFERENCE (Lever 1, delta-off only): `operators=[]`, `base_epoch=Some`, `deltas=[]`.
- DELTA: `deltas` non-empty (possibly-empty payloads), `base_epoch=Some(parent)`, `operators=[]`.
```
struct OpDelta { changed: Vec<u8>, tombstones_ipc: Vec<u8> }
VnodePartial { checkpoint_id, operators, base_epoch, deltas: Vec<(String, OpDelta)> }
```
`base_epoch` doubles as the parent link for deltas. Reader walks parents collecting
`deltas` until a FULL.

### 3b. Codec promotion (`aggregate_state.rs`)
- `AggVnodeDelta`, `encode_delta_for_vnode`, `apply_delta`: `#[cfg(test)]` → `#[cfg(feature="cluster")]`.
- New capture entry that, per vnode, returns FULL slice OR delta payload + clears that
  vnode's dirty sets. Operator tracks `last_full_epoch[v]`; forces FULL when None
  (first capture / just-acquired) or `epoch - last_full_epoch[v] >= chain_cap`, or when
  the delta would exceed the full size (heuristic).
- `apply_delta` already REPLACE — chain replay = `merge_groups(full_base)` then
  `apply_delta(d)` forward. Confirm empty-delta (carry-forward) round-trips.

### 3c. Emission (`sql_query.rs` capture, `checkpoint_coordinator.rs` write)
- `StagedSlice` gains a `Delta{changed, tombstones}` variant (or a parallel staged-delta map).
- When `delta_enabled`, the operator drives full-vs-delta; the coordinator's Lever-1
  byte-compare reference path is bypassed (unchanged vnode → empty delta, cheap; cap re-bases).
- Coordinator records DELTA partials with `base_epoch = prev committed epoch for that vnode`.
  First capture after acquiring a vnode forces FULL (no local chain history).

### 3d. Chain recovery (`recovery_manager.rs`, `operator_graph.rs`, `pipeline_lifecycle.rs`)
- `VnodeRehydrator`: replace the single-hop base follow with a parent walk — collect the
  ordered chain (FULL base bytes + ordered delta payloads per operator) back to the FULL.
  A missing/undecodable link in the window → vnode starts fresh (as today, conservative).
- `RehydratedVnode` / report carries the ordered chain, not one resolved blob.
- `apply_vnode_state`: apply FULL base (`merge_groups`) then each delta (`apply_delta`) in order.

### 3e. Config / wiring
- `delta_enabled` from `[checkpoint]` (cluster, **default off**); `delta_chain_max`
  (default e.g. 16, clamp `< max_retained`). Plumb server → `StreamCheckpointConfig`
  → operator. Set on the agg state at build (`set_delta_enabled`).

### 3f. Prune
- Verify prune retains the whole chain: keeping the most recent `max_retained` epochs with
  `chain_cap < max_retained` keeps base + all links. Add an assertion/test.

### 3g. Tests + sign-off
- Unit: multi-delta chain encode→apply reproduces producer; empty-delta carry-forward;
  re-base at cap; tombstone-then-readd within a chain.
- Recovery: rehydrate from a >2-deep chain == full-capture baseline.
- Gate/prune: chain base survives the prune window; gate seals on delta partials.
- Soak (follow-up): kill-9 exactly-once with delta capture on; graceful-rotation EO.

## Invariants
- `apply_delta` is REPLACE, never additive merge — delta carries post-update group state.
- A vnode just acquired on rotation re-bases FULL on its first capture (no chain history).
- `delta_chain_max < max_retained` — base must outlive the chain head within the prune window.
- **Carry-forward is load-bearing:** every non-empty (operator, vnode) emits each epoch (an empty
  delta if unchanged), so `chain_len` (delta count) == epochs-since-base — the count cap then also
  bounds the base age, so the base can't silently age out of retention via sparse deltas.
- Delta off → behavior byte-identical to today (Lever 1 path untouched).

## Research validation (2026 — Flink, RisingWave, Arroyo, Spark, Kafka Streams, Materialize, Feldera)
Three independent web-research passes (sources in the session). Verdict: **the model is correct and
well-precedented.** It IS Flink's Changelog backend (FLIP-158) / **Spark's RocksDB changelog
checkpointing** — Spark is a near-isomorphism: `.zip` base + chained `.changelog` deltas, re-based at
`minDeltasForSnapshot`(10) ≪ `minBatchesToRetain`(100) = our cap<retention exactly. Kafka Streams
(changelog topic + log-compaction re-base) and RocksDB universal compaction (`max_size_amplification`)
are the same family. Confirmed decisions + refinements folded in:
- **Per-operator chain resolution (the open question): YES** — unanimous. A `VnodePartial` may bundle
  multiple operators, but each `operator_name` resolves its own latest-FULL + later deltas
  independently. Our format supports this (a partial can carry both `operators` (re-based ops) and
  `deltas` (delta ops); the per-vnode `base_epoch` is the chain link, recovery resolves per operator).
  Each agg's `IncrementalAggState` already tracks its own per-vnode `delta_chain_len`, so per-operator
  independence falls out for free.
- **Force-FULL-on-acquire: validated** = Flink NO_CLAIM (forces a FULL on restore to break a base
  owned by a prior generation). Keep for v1. (RisingWave moves zero bytes on rebalance via
  vnode-owned chains in shared storage — that's the v2 optimization; tracked, not v1.)
- **GC hardening (Flink `SharedStateRegistry`):** ideal GC is ref-count/reachability over base+delta
  objects, not cap<retention arithmetic. v1 keeps cap<retention (Spark-proven) + the existing
  reachability of the prune window; ref-counting is a hardening follow-up. Also free at
  `(operator, vnode)`-slice granularity to avoid FLIP-306 bundle-pinning amplification.
- **Keep the cap SMALL** (single digits, bound by recovery-latency SLA — Flink saw +66–225% recovery
  with long chains). **Stagger/jitter re-bases across vnodes** (vary effective cap by `vnode % spread`)
  so they don't all re-base on the same epoch → upload storm (the spike FLIP-158 exists to kill).
- **Ordering (FLINK-25261/23170):** persist new FULL base → confirm → only then reclaim the old chain.
  `base_epoch` must only ever point at a committed epoch.
- **v2 / hardening:** make the only sequential dependency a small central manifest (RisingWave
  `HummockVersion` / our `CatalogManifestStore`), not an object-by-object linked list the reader walks.

## Dual-path question — can incremental subsume the current (full + Lever-1 reference) path?
**Yes, as the end state — but stage it; don't drop the proven path until the soak proves incremental.**
Incremental already contains the others: **FULL capture = the chain base (re-base)**; **Lever-1
reference = an empty-delta carry-forward**. So one unified mechanism (incremental + periodic re-base)
replaces three. This is exactly what Spark did (changelog checkpointing replaced full-snapshot-every-
batch) and where Flink heads (changelog backend). Changelog aggregates degrade gracefully — they
re-base FULL every epoch (until v2 delta-encodes `last_emitted`), i.e. the unified path == today's full
path for them, no correctness loss. **Rollout:** (1) land incremental opt-in, default OFF; (2)
soak-validate EO with it ON; (3) flip default ON, bake; (4) delete the full-every-epoch default + the
separate Lever-1 byte-compare reference path, leaving one mechanism. Do NOT delete before step 2/3.

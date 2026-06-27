# A1 — incremental running-state aggregates (O(dirty), not O(total groups))

Status: **scoped / not started.** Targets the **non-windowed `GROUP BY` MV** path (default running
state, no `EMIT CHANGES`). `EMIT CHANGES` (changelog) and windowed aggregates are already
incremental.

## Why (profiled, release, this machine)

Non-windowed running-state aggregate, per cycle:

| groups | #1 emit (`emit_running_state`) | #2 capture (`checkpoint_groups`) | baseline (fold 1 row) | emit/baseline |
| --- | --- | --- | --- | --- |
| 10k | 1.3 ms | 6.6 ms | 6 µs | 222× |
| 100k | 21 ms | 91 ms | 25 µs | 829× |
| 1M | **358 ms** | **1.05 s** | 34 µs | **10,536×** |

Both costs are **O(total groups)**; the real work is O(changed groups) and is ~tens of µs. Emit
fires **every cycle** (the throughput ceiling — can't cycle faster than ~2.8×/s at 1M) and
cascades (MV store replace-all → sinks → serialize → shuffle). Capture fires **per checkpoint**
(stalls ingest >1 s at 1M, inline on the pipeline task). A3 already took capture's constant factor
(~9×, ~1 µs/grp); the remaining lever is making both **O(dirty)**.

Profiler lives at `aggregate_state.rs::tests::profile_agg_emit_vs_capture` (`#[ignore]`d) — use it
as the before/after gate.

## A1 = two separable halves

They share the goal (O(dirty)) but touch different machinery and carry different risk. They can
ship independently.

### A1-capture — checkpoint O(dirty)  (addresses #2; lower risk, self-contained)

Make per-vnode partial capture the **primary** agg checkpoint instead of whole-node
`checkpoint_groups`.

- **Today:** `sql_query.rs:894` `checkpoint()` → `checkpoint_groups()` (whole-node, O(groups));
  recovery → `restore_groups` (`aggregate_state.rs:1484`). Per-vnode (`checkpoint_groups_by_vnode`
  :1564, `write_vnode_partials` in `checkpoint_coordinator.rs:993`, `VnodeRehydrator` +
  `apply_vnode_state` :1131) is supplementary (cluster rebalance).
- **Change:** capture only **dirty vnodes'** groups (reuse `dirty_vnodes` = dirty-since-capture,
  reset at capture ~`:1646`), persist via `write_vnode_partials`, and switch single-instance
  recovery from `restore_groups` to `VnodeRehydrator` + `apply_vnode_state`. Give single-instance a
  `vnode_count` (cluster fences already no-op single-instance per the prior separability spike).
- **Granularity:** vnode-coarse — one changed group dirties one vnode (~`N/vnode_count` groups
  captured). At 256 vnodes that's ~256× less work (1M → ~4k groups ≈ ~4 ms vs 1.05 s). Per-group
  capture granularity would need a new dirty-since-capture *key* set (more memory) — out of scope.
- **Risk:** it's a **recovery-model switch** (per-vnode becomes the source of truth). Must preserve
  the destructive-accumulator rebuild (DataFusion `distinct`/`array_agg` have destructive
  `state()`; A3's `snapshot_state_scalars` handles this — keep it on the per-vnode path). Whole-node
  `checkpoint_groups`/`restore_groups` can be deleted once per-vnode is primary (no back-compat).
- **Gate:** kill-9 EO file-checkpoint soak (the reliable green here) + the profiler before/after +
  full lib/state-tier suites. Flag-gated default-OFF until soaked, then make default.
- **Size:** M–L. **No consumer-contract change.**

#### BLOCKER found during scoping (2026-06-27): per-vnode partials have no durable home in embedded single-instance

The prior "machinery exists, just un-gate it" premise holds **only for object-store-backed
deployments**. Verified:
- Per-vnode partials are written via `StateBackend::write_partial` to `(vnode, epoch)`. The durable
  impl is `ObjectStoreBackend` (`laminar-core/state/object_store.rs`). The embedded default is
  `InProcessBackend` — explicitly **non-durable** ("in-memory hashmap… embedded single-process
  runs"); `write_partial` inserts into a `RwLock<HashMap>` → **lost on kill-9**. And a `StateBackend`
  is only installed if explicitly provided (`builder.rs:488`).
- Durable single-instance recovery today uses the **file manifest** (`db.rs:2201`,
  `ObjectStoreCheckpointStore` on a local dir) carrying **whole-node `operator_states`**, *not*
  per-vnode partials.

So switching the primary agg checkpoint to per-vnode partials in embedded single-instance would
write the partials to a non-durable backend and **break kill-9 recovery**. A1-capture therefore
forks by deployment:
- **A1-capture (object-store-backed)** — cluster, or single-instance with `object_store_url` set.
  Durable partials already exist → genuinely "un-gate + make primary + switch recovery". Medium.
  Validate with an object-store kill-9 soak.
- **A1-capture (embedded/file single-instance)** — needs **new durable per-vnode partial storage**
  (a file-backed `StateBackend`, or fold partials into the manifest store). Larger; new storage code.

**Recommendation:** target **object-store-backed first** — that's where large-state (high-cardinality)
aggregates are realistically checkpointed anyway, the durable machinery is present, and it's the
lower-risk increment. Add embedded-durable partials only if an embedded high-cardinality deployment
needs it. Decide the target deployment before implementing.

#### REFRAME (2026-06-27): A1-capture = the delta-checkpoint "staged-unification flip", not a new build

Owner decision: **object-store-backed**, validated via MinIO in Docker. Crucial overlap found — the
**Delta-checkpoint Lever 2** work, committed on *this branch* (`5e6cc265` + `49e98a49`,
`docs/plans/incremental-delta-checkpoint-lever2.md`), already built and **kill-9/changelog
soak-validated** the hard parts, default-OFF and cluster-gated:
- per-vnode **O(dirty) delta capture** (`checkpoint_delta_by_vnode`), durable object-store partials
  (`write_vnode_partials`), multi-hop **chain recovery** (`collect_chain`/`resolve_op_chain`,
  `apply_vnode_chain`), per-operator chain resolution.

But it runs **alongside** the whole-node path, not replacing it. Verified: every
`checkpoint_with_barrier` does **both** `capture_and_serialize_operator_state` (whole-node
`snapshot_state` → `checkpoint_groups`, O(all groups), into the manifest) **and** `capture_vnode_states`
(delta partials). So today delta-ON *adds* cost; the O(dirty) win needs the whole-node agg capture
**removed**. That removal + routing agg recovery through the chain is exactly the delta plan's
remaining step: *"flip default → delete the full-every-epoch path"* (soak-gated).

**So A1-capture (object-store) = the unification flip, scoped to cluster/sharded + object-store:**
1. Behind a flag, make `snapshot_state` **skip whole-node agg state** (aggs no longer serialized into
   the manifest `operator_graph` blob) when the delta chain is the authority — non-agg operators +
   MV stores + source offsets stay in the manifest.
2. Route **agg recovery through the chain** (`VnodeRehydrator` + `apply_vnode_chain`) as primary,
   not `restore_groups` from the manifest.
3. Keep `delta_chain_max` set (delta path active). Flag-gated default-OFF.
4. Validate with the **MinIO 3-node kill-9 soak** (`cluster_soak.rs`, `LAMINAR_SOAK_DELTA_CHAIN_MAX`),
   which already exercises delta chain recovery; assert the manifest no longer carries agg state and
   recovery is chain-only, with no gap/dup.

This is far smaller and lower-risk than a from-scratch build — the codec, durability, and chain
recovery are soaked; A1-capture is making them authoritative. Requires cluster + sharding (the
vnode partitioning) + an object-store backend — the deployment the MinIO soak covers.

#### IMPLEMENTED (2026-06-27, default-OFF, MinIO soak pending)
Two steps, both committed behind a default-OFF `delta_primary` flag:
- **Step 1** — cluster startup requires a durable `StateBackend` (`is_durable()`; LDB-0011). Committed
  `d41907a6`.
- **Step 2** — the flip:
  - Flag `delta_primary` plumbed `StreamCheckpointConfig`/server `CheckpointSection` → `OperatorGraph`
    (`set_delta_primary`) → `SqlQueryOperator`, effective only with delta enabled + sharded.
  - Capture-skip: agg `checkpoint()` returns no whole-node group state when `delta_primary`
    (`skip_whole_node_agg`).
  - Recovery: `stage_owned_vnodes_for_delta_primary` (pipeline_lifecycle) rehydrates every owned
    vnode's delta chain into the existing `rehydrated_vnode_state` staging map after the graph restore;
    the first cycle's `apply_rehydrated_vnodes` rebuilds the aggregates — reusing the soaked
    rebalance-acquire apply path, no new apply code.
  - Unit test `delta_primary_skips_whole_node_agg_capture` (operator: checkpoint None on / Some off).
  - Soak knob `LAMINAR_SOAK_DELTA_PRIMARY` added to `cluster_soak.rs`.
- **Gate before flipping default:** MinIO 3-node kill-9 soak
  (`LAMINAR_SOAK_DELTA_CHAIN_MAX=6 …`) — assert chains rehydrate, the manifest carries no agg state,
  recovery is chain-only, no gap/dup. Then delete the whole-node agg capture path.

#### VALIDATED + FLIPPED + UNIFIED (2026-06-27)
- **Validation:** kill-9 soak (chains rehydrate, zero delta/chain errors, EO intact) + two
  deterministic `cluster_integration` tests asserting **correct values** (doubled totals) after
  recovery on both paths: `delta_primary_crash_rehydrates_aggregate_from_chain` (crash + rebalance)
  and `delta_primary_aggregate_survives_graceful_restart` (full restart — the path the soak can't
  reach, since a respawned node owns nothing until rebalance). The graceful test is the one that
  proves the `stage_owned_vnodes_for_delta_primary` restart path with `owned>0`.
- **Flip + cleanup (unification):** the separate `delta_primary` flag is **removed** — enabling
  incremental delta checkpoints (`delta_chain_max`) now *implies* chain-primary recovery. One
  mechanism, per the delta plan's endpoint. `skip_whole_node_agg() == delta_chain_max.is_some()`;
  recovery staging keyed on `delta_chain_max`. The escape hatch is simply not enabling delta
  (`delta_chain_max = None` → whole-node manifest, unchanged). Removed: the `delta_primary` fields on
  `StreamCheckpointConfig` / server `CheckpointSection`, `OperatorGraph`/`SqlQueryOperator`
  `set_delta_primary`, the soak `LAMINAR_SOAK_DELTA_PRIMARY` knob.
- **Remaining:** the whole-node `checkpoint_groups` function stays (non-sharded / non-delta aggs
  still use it); for delta-enabled sharded aggs it is no longer called (skip). A future single-node
  embedded-durable-partials effort could extend A1-capture beyond cluster/object-store.

### A1-emit — per-cycle emit O(dirty)  (addresses #1; the bigger win, higher risk)

Stop re-evaluating + re-materializing all groups every cycle.

- **Today:** `emit_running_state` (`aggregate_state.rs:1232`) walks **all** groups: `convert_rows`
  on all keys + per agg `evaluate()` per group + `iter_to_array` over N rows. It must emit the full
  snapshot because the MV store is **replace-all** (`mv_store.rs:58`) and append-only consumers
  expect a full snapshot, not a changelog.
- **The shortcut that doesn't work:** an eval-cache (old P0.1b — cache clean groups' `ScalarValue`,
  skip `evaluate()`) only removes the `evaluate()` axis. For cheap accumulators (SUM/COUNT)
  `evaluate()` is already trivial; the O(N) cost is **materializing the N-row arrays**
  (`convert_rows` + `iter_to_array`), which eval-cache does **not** remove. So eval-cache is *not*
  the win here (this is why P0.1b was shelved). The only way to make emit O(dirty) is to **emit
  fewer rows** → a changelog/upsert, which is a consumer-contract change.
- **Change (own ADR):** give the default non-windowed running-state MV **incremental emit**: emit
  only dirty groups (reuse the `dirty_keys` changelog machinery already at `:1300`) into an
  **upsert MV store** mode (merge dirty groups, keep the full snapshot for `SELECT * FROM mv`
  reads) instead of `MvStorageMode::Aggregate` replace-all. Per-cycle work becomes O(dirty); the
  full snapshot is read on demand from the upsert store.
- **Consumer-contract decision (the crux — decide before building):** the operator output also
  feeds chained MVs, stream subscribers, and sinks. Incremental emit hands them an upsert/retraction
  changelog. Options: (a) upsert-capable consumers only (MV reads + upsert sinks + retraction-aware
  chained MVs) and keep full-emit for append-only sinks; (b) maintain the snapshot in the store and
  re-derive full output only for append-only consumers (partial win). This is the "upsert MV +
  delta-aware consumers" ADR — large blast radius across every downstream consumer.
- **Risk:** high (output-semantics change across consumers). **Size:** L–XL.

## Recommended sequence

1. **A1-capture first** — bounded, self-contained, machinery exists, no consumer change; removes the
   1 s checkpoint stall. Soak, make default.
2. **Decide the A1-emit consumer contract** (the ADR above) — this is the gate, not code. Without an
   upsert/delta consumer model there is no O(dirty) emit.
3. **A1-emit** — only after (2). It's the larger throughput win but the larger blast radius.

Note on totals: emit fires every cycle, capture per checkpoint — so over a window with infrequent
checkpoints, **emit dominates total cost**, but it's the harder change. A1-capture is the safer
first increment; A1-emit is where the throughput ceiling actually lifts.

## Don't repeat / constraints
- No in-memory byte cache of group state (doubles memory, violates the memory budget).
- Preserve destructive-accumulator rebuild (`snapshot_state_scalars`) on any incremental path.
- Reuse `dirty_vnodes` (dirty-since-capture) for A1-capture and `dirty_keys` (dirty-since-emit,
  already wired for `EMIT CHANGES`) for A1-emit — don't conflate them.
- Format/recovery-model change is free (zero external users); delete the superseded whole-node path.

## A1-emit — cold-start implementation plan (START HERE next session)

Decision: **ADR-007 Option A** (`docs/adr/ADR-007-incremental-emit-running-state-aggregates.md`,
local/gitignored) — default non-windowed running-state MV emits a **dirty-only changelog** into a
**keyed upsert MV store**; `SELECT * FROM mv` keeps returning the full snapshot. A1-capture (capture
side) is DONE; this is the emit side. Profiler proves the win: emit is 358 ms @ 1M groups *every
cycle* (`profile_agg_emit_vs_capture`, `#[ignore]`d in `aggregate_state.rs`).

**Read first:** ADR-007 (decision + rejected options + open questions), then this doc's A1-emit
section above.

**Grounded entry points (verified 2026-06-27):**
- MV storage-mode choice: `ddl.rs:1153` `register_mv_provider` (`MvStorageMode::Aggregate` for
  non-windowed aggs) — add the `Upsert` mode here, keyed by the GROUP BY column indices.
- Changelog already exists: `emit_changelog` appends `WEIGHT_COLUMN` (`aggregate_state.rs:831`,
  `laminar_core::changelog::WEIGHT_COLUMN`); dirty-only emit path at `aggregate_state.rs:~1300`
  (`dirty_keys`). `EMIT CHANGES` is the working precedent — reuse it; don't reinvent dirty-emit.
- Output → MV store: `streaming_coordinator.rs` `update_mv_stores(&out.results)` →
  `mv_store.rs::MvStore::update(name, batch)` → `MvEntry::update` (`mv_store.rs:56`).
- MV read path: `table_provider.rs` `MvTableProvider::scan` over `mv_store.to_record_batch`.
- Checkpoint/restore of MV store: `mv_store.rs` `checkpoint_states` / `restore_from_ipc`.
- The cross-layer snag: `operator_graph.rs::route_output` swaps the operator **output** into the
  name-keyed live provider (`live_handles`) for chained MVs; under incremental emit that output is
  the changelog (`__weight`), so chained readers must instead see the **snapshot** — and the snapshot
  lives in `mv_store` (pipeline_callback layer), not in `route_output` (operator_graph layer). This
  is the hard part of Stage 1.

**Slices (each flag-gated default-OFF; new `incremental_emit` config, plumbed like `delta_chain_max`):**
- **1a — keyed upsert MV store mode.** `MvStorageMode::Upsert { key_cols: Vec<usize> }`; `MvEntry`
  keeps a keyed snapshot (key via `RowConverter` over `key_cols`); `update(changelog)` applies
  `+WEIGHT → upsert`, `−WEIGHT → delete`, storing rows **without** the weight column;
  `to_record_batch` materializes the plain snapshot. Checkpoint = materialize → IPC; restore = replay
  into the keyed map. **Unit tests**: feed changelog batches, assert snapshot == full-recompute.
- **1b — wire the agg emit.** In `ddl.rs:1153`, under `incremental_emit`, register non-windowed agg
  MVs as `Upsert` (key = GROUP BY col indices) and make the agg operator `emit_changelog=true` so it
  emits dirty-only. MV provider schema stays plain (no `__weight`).
- **1c — terminality guard (anti-sprawl; the cross-layer fix is deliberately OUT of Stage 1).**
  Do NOT try to serve snapshots to chained readers in Stage 1 — that's the sprawl trap (cross-layer:
  snapshot in `mv_store`/callback vs `route_output`/live-provider in operator_graph vs the `input_bufs`
  edge path). Instead, **Stage 1 incremental emit applies only to *terminal* agg MVs** (read solely
  via `SELECT * FROM mv`). Implement as a **local DDL guard** at `ddl.rs`:
  - agg MV with no consumer at creation → incremental (`Upsert` + changelog);
  - any query / sink / `SUBSCRIBE` that references an incremental MV → **DDL error** (no
    back-compat needed → clean reject, no compat shim);
  - every other MV → full-emit, unchanged.
  This is a catalog check (mode of each MV + table-refs of the new query), ~30 lines, no cross-layer
  plumbing. **Serving the snapshot to chained readers** (so they *can* consume an incremental MV —
  feeding `route_output`'s live provider from the MV-store snapshot) is a **separate later stage**,
  not Stage 1. Keep it out to bound the work.
- **1d — recovery same-epoch invariant** (ADR-007's named gate): the chain-recovered accumulators
  (A1-capture) and the manifest-recovered MV snapshot must reflect the same epoch.

**Validation (mirror A1-capture):** a deterministic `cluster_integration` test that feeds an agg over
many cycles and asserts `SELECT * FROM mv` == full-recompute, then a crash+restart asserting the
snapshot is correct; plus the `profile_agg_emit_vs_capture` before/after showing emit is now O(dirty).

### IMPLEMENTED — Stage 1 (2026-06-27, default-OFF `incremental_emit`)

All four slices landed and green on this box; default-OFF so existing behaviour is unchanged.

- **Config:** `incremental_emit: bool` on `StreamCheckpointConfig` (laminar-core) + server
  `[checkpoint] incremental_emit` (TOML), default false — co-located with `delta_chain_max` (so it
  requires a `[checkpoint]` block to enable).
- **1a — keyed upsert MV store** (`mv_store.rs`): `MvStorageMode::Upsert { key_cols }`; `UpsertState`
  keeps an `OwnedRow`-keyed snapshot (`RowConverter` over `key_cols`), `apply(changelog)` does
  `+weight`→upsert / `−weight`→delete storing rows **without** the weight column, `to_record_batch`
  materializes the plain snapshot; checkpoint = materialize→IPC, restore = `load_snapshot` replay.
  `create_mv` is now fallible (RowConverter build). 3 new unit tests (apply, snapshot==recompute,
  checkpoint round-trip).
- **1b — wiring (single source of truth):** the decision is `incremental_emit && non-windowed agg
  WITH a GROUP BY` (global aggregates stay full-emit — already single-row), computed **once** in
  `ddl.rs::incremental_mv_key_cols`. It drives BOTH the MV-store mode (`Upsert` in
  `register_mv_provider`) and the operator's changelog emit, threaded as one `incremental: bool`
  through `StreamRegistration` → `ControlMsg::AddStream` → `OperatorGraph::add_query` →
  `create_operator` (`emit_changelog = incremental || EMIT CHANGES`). The two layers cannot disagree.
  MV provider schema stays plain (no `__weight`); the changelog rides operator→MV-store only.
- **1c — terminality guard** (`ddl.rs`): `incremental_mv_consumer_error` (`[LDB-1300]`). A chained MV
  / `CREATE STREAM` / sink (`FROM <mv>`) / `SUBSCRIBE` that references an incremental MV is rejected;
  ad-hoc `SELECT * FROM mv` snapshot reads stay allowed. The cross-layer snapshot-serving for chained
  readers stays OUT (Stage 2).
- **1d — recovery:** validated single-node by `incremental_emit_survives_checkpoint_restart` (the MV
  snapshot recovers from the manifest `mv:` entry; upsert restore is idempotent so a post-restart
  re-emit of unchanged groups can't corrupt it).
- **Tests:** `crates/laminar-db/tests/incremental_emit.rs` (4: snapshot==recompute, restart survival,
  guard rejection, flag-off regression) all green; 777 db lib tests + 12 `mv_store` tests green;
  clippy (`--features cluster --tests`) + fmt clean.
- **Deferred / caveats:** (a) the `profile_agg_emit_vs_capture` before/after isn't re-run yet (the
  profiler asserts emit-path cost, not consumer contract; O(dirty) follows from emitting only
  `dirty_keys`). (b) **Cluster composition is opt-in but unvalidated** — with the flag on in a sharded
  cluster each node's `Upsert` store would hold only its owned-vnode groups (the changelog already
  shuffles for `EMIT CHANGES`) and `DistributedTableProvider` would union them, but rebalance must
  move the MV snapshot with the vnodes (ADR-007 open Q4), and the cluster kill-9 soak is blocked on
  the pre-existing shuffle-barrier-after-kill bug. Keep `incremental_emit` to single-node until that
  is fixed and a cluster soak is run.

**Stage 2/3 (after Stage 1 — each lifts one part of the 1c guard's reject set):**
- **chained-MV reads of an incremental MV** — the cross-layer snapshot-serving deferred from 1c
  (feed `route_output`'s live provider / chained `input_bufs` from the MV-store snapshot instead of
  the changelog). This is the hard cross-layer piece; its own stage.
- **upsert / transactional sinks** via the existing changelog-collapse path
  (`docs/plans/changelog-collapse-for-upsert-sinks.md`).
- **`SUBSCRIBE`** = snapshot-then-changelog.
- **append-only sink** of a running aggregate rejected at DDL (or opt-in changelog-append).

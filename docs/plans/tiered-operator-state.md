# Tiered Operator State — Implementation Plan

Implements ADR-005 (in-memory hot tier, fjall cold tier, object-store truth).
Plan written 2026-06-12 against `main` @ `6a397a13` (sub-second checkpointing
landed, PR #428).

Convention note: **no ADR references in code**. Source, comments, config keys,
metric names, and log messages describe behavior in their own terms
(`state tier`, `cold tier`, `memory budget`, `demote`/`promote`). ADR citations
live only in `docs/`.

---

## 1. Where the codebase actually is (survey results)

| ADR-005 assumption | What landed | Consequence |
|---|---|---|
| ADR-003 phases 1–3 (restorable epochs, delta artifacts, staging window) | Landed in PR #428 — but "incremental snapshots" are **vnode-level byte-identity reference partials**, not group-level deltas | See §2 — this changes the safe demotion granularity for v1 |
| `estimated_state_bytes` hook exists | Yes — `GraphOperator::estimated_state_bytes` (`operator_graph.rs:41`), implemented by agg (`sql_query.rs:713`), joins, lookup-enrich, AI op | Budget precursor builds on it |
| Budget → backpressure | Only a per-operator **hard-fail** limit exists: `max_state_bytes_per_operator` → `DbError::Pipeline` at 100%, warn at 80% (`operator_graph.rs:1675-1691`) | Precursor must add node-level budget + backpressure instead of error |
| Async-decoupling pattern reusable | Yes — `LookupEnrichOperator` (`operator/lookup_enrich.rs`): `PendingBatch` slots, bounded `mpsc::channel(256)`, worker on the main runtime handle, `try_recv` drain per cycle, `watermark_hold()` + `wants_input()` backpressure, pending-row checkpoint/replay | Promotion path copies this wholesale |
| Rebalance hand-off needs no new mechanism | Confirmed — `merge_groups` is additive (`aggregate_state.rs:1376`), `apply_vnode_state` (`sql_query.rs:767`) handles live + pending-restore | Holds for the chosen v1 granularity |

Other load-bearing facts found:

- **Per-epoch capture serializes ALL groups.** Every barrier,
  `capture_vnode_states` (`pipeline_callback.rs:1054`) →
  `checkpoint_by_vnode` (`sql_query.rs:740`) →
  `checkpoint_groups_by_vnode` (`aggregate_state.rs:1308`) rkyv-encodes every
  group, every vnode. Dedup happens later in the coordinator by byte
  comparison. At 100 ms cadence this is O(total state) CPU per epoch — an
  independent scaling ceiling that the v1 design happens to improve (demoted
  vnodes are no longer serialized each epoch).
- **The coordinator pins last-upload bytes in RAM.**
  `last_vnode_uploads: HashMap<vnode, (epoch, HashMap<op, Bytes>)>`
  (`checkpoint_coordinator.rs:771-829`) retains the full serialized slice per
  vnode to do the byte-identity comparison. For large state this is a second
  full copy of state in memory; tiering must remove this pin for cold vnodes.
- **References are forced back to full before their base leaves the
  `max_retained` prune window** (`checkpoint_coordinator.rs:742-747`,
  `max_ref_age` check at `:767,:781`). Any demotion design must be able to
  produce full bytes on demand for that re-upload.
- **`estimated_state_bytes` for agg is O(groups) per call**
  (`aggregate_state.rs:1181-1190` walks every group summing `acc.size()`).
  Fine as an occasional probe; too expensive to evaluate per cycle as a
  budget. Needs an incrementally-maintained estimate.
- **Per-vnode partials are `cluster`-feature code** (`sql_query.rs:740` is
  `#[cfg(feature = "cluster")]`), and `cluster` is in the server's default
  feature set. v1 tiering rides the per-vnode path and is therefore gated the
  same way; non-cluster lib builds simply never tier.
- The aggregation state shape: `AHashMap<arrow::row::OwnedRow, GroupEntry>`
  (`aggregate_state.rs:241`), `GroupEntry { accs, last_updated_ms }`
  (`:316`). `last_updated_ms` already gives an idle signal per group; per-vnode
  idle is derivable.
- Restorable-epoch knowledge lives in the checkpoint coordinator / barrier
  path; nothing currently publishes "epoch N is restorable" toward the
  operator graph. Small plumbing needed (an atomic/watch is enough).

## 2. The one real design decision: demotion granularity

ADR-005 says "demote clean **groups**" and claims delta epochs are unaffected
because "demoted groups are clean, so deltas never include them". That
reasoning assumed group-level delta artifacts. What actually landed is
different: each epoch re-serializes the **whole vnode slice** from the
in-memory map and the coordinator emits a reference partial only if the bytes
are identical to the last full upload.

Under that format, group-granularity demotion is **unsafe as-is**: dropping
cold groups from the map changes the serialized vnode bytes, so the next full
upload for that vnode would silently omit the demoted groups — truth (the
object store) loses state. Making group granularity safe requires group-level
delta artifacts (base + changed-group chains), which is exactly the "artifact
format change" the ADR promised to avoid.

Decision for v1: **demote at (operator, vnode)-slice granularity** — the unit
at which byte-identity already proves cleanliness.

- A slice that the coordinator has been emitting reference partials for is, by
  construction, byte-identical to a durable full upload at a restorable epoch.
  That is precisely the ADR's "clean" condition, proven by machinery that
  already exists.
- The cold tier stores the exact uploaded bytes, so the forced full re-upload
  (base aging out of `max_retained`) streams those bytes from fjall on Ring 1
  without promoting anything.
- No artifact format change, no recovery-path change, no prune-logic change.
- Bonus wins: demoted vnodes drop out of the per-epoch O(state) serialization,
  and their `last_vnode_uploads` byte pin moves to disk.

Cost: granularity is coarse — one hot key keeps its whole vnode slice
resident. For skewed/idle-heavy workloads (the realistic huge-cardinality
case) this captures most of the win. True group granularity is deferred to a
v2 that introduces group-level delta artifacts — which is also the fix for the
per-epoch O(state) serialization ceiling, so it will likely be wanted on its
own merits. ADR-005 should get a short amendment recording this v1/v2 split.

## 3. Phases

### Phase 0 — Benchmark gate (ADR precondition; no engine changes)

> **Status: harness built 2026-06-12** (`tools/state-tier-bench`, standalone
> crate with its own lockfile — fjall stays out of the main workspace until
> the gate passes). Both modes smoke-tested on Windows; **gate numbers still
> owed from target-class Linux NVMe with a beyond-RAM dataset** — see the
> tool's README for the exact invocations.

Build a standalone harness (e.g. `tools/state-tier-bench`, not in the default
workspace build) that exercises fjall with our workload shape:

- Keys: Zipfian-distributed `(operator, vnode, group-key)` byte strings.
- Values: rkyv-encoded accumulator states, 100 B – 4 KiB.
- Two access patterns: (a) slice-granularity (one value per (op, vnode), tens
  of KB – tens of MB, matching v1) and (b) group-granularity point upserts
  (matching v2) — measure both so the v2 decision is informed now.
- Measure on target NVMe: cold-read p99 from a non-compute thread under
  sustained write load (gate: ≤ 1 ms), compaction CPU and whether fjall's
  background threads can be pinned/limited away from compute cores, write
  amplification at sustained upsert rates.

Exit: numbers recorded in the ADR (flip to Accepted, or pivot to the named
hash-log fallback). Also verify here: fjall's sync API behaves from
`spawn_blocking`, keyspace-per-operator works, point-in-time snapshot/iterator
semantics, crash-recovery behavior without per-write fsync.

### Phase 1 — State memory budget (independently shippable, ships first)

> **Status: implemented 2026-06-12** on `feat/state-memory-budget`. Notes vs.
> the plan below: the agg estimate is a generation-counter + min-2s-rewalk
> cache (not a running counter — fewer mutation-path hooks, same O(1) read);
> the budget gate reuses the coordinator's existing skip-drain backpressure
> (intake throttles to one message per cycle so checkpoint barriers keep
> flowing — a full intake halt would starve them) and additionally skips
> `tick_idle_watermark()` while paused so a budget-paused source is not
> idle-demoted (which would late-drop its queued rows on resume). Config:
> `state_memory_budget_bytes` (builder + `[server]`). Metrics: `state_bytes`,
> `operator_state_bytes{operator}`, `state_memory_budget_bytes`,
> `state_over_budget`, `state_budget_paused_cycles_total`.

1. **Cheap state-size accounting.** Make the agg estimate incrementally
   maintained (running byte counter updated on group insert/remove plus a
   periodic accumulator resample), so reading it per cycle is O(1). Keep the
   trait hook signature unchanged.
2. **Node-level budget.** New config: `DbConfig::state_memory_budget_bytes`
   (server: `[state] memory_budget_bytes`). The graph sums
   `estimated_state_bytes` across nodes once per cycle.
3. **Backpressure, not failure.** Over budget → stop ingesting source input
   (the graph stops offering new batches; sources pause naturally), hold
   watermarks at the gate, raise `state_over_budget` gauge + rate-limited
   error log. Existing per-operator hard limit stays as the kill switch.
4. **Metrics**: `state_bytes{operator}`, `state_memory_budget_bytes`,
   `state_over_budget`, time-over-budget counter.

Exit: a workload exceeding budget throttles instead of OOM-killing; metrics
visible in `/metrics`; soak with a tiny budget shows stable RSS.

### Phase 2 — Cold-tier service (no operator wiring yet)

New module `crates/laminar-db/src/state_tier/` behind a new `state-tier`
feature (implies `cluster`); fjall is the only new dependency, major version
pinned.

- `StateTierStore`: owns the fjall `Keyspace` under
  `<data_dir>/state-tier/`, one fjall partition per operator. Sync API only,
  called exclusively from Ring 1 (`spawn_blocking` / worker task).
- **Lifecycle**: on open, validate a marker file (engine version + clean-flag
  + per-instance nonce). Missing/mismatched/unclean → wipe directory and start
  empty (state rehydrates from checkpoints exactly like today's recovery).
  Clean shutdown writes the marker; runtime operation never fsyncs per write.
- `StateTierWorker`: Ring-1 task (spawned on the main runtime handle, the
  `set_runtime_handle` pattern at `operator_graph.rs:355`), bounded mpsc
  request/response channels, request types: `Demote{key, bytes}`,
  `Fetch{key}`, `Drop{key}`, `SnapshotRead{...}`.
- Metrics: tier size bytes, keys, demote/promote counters, fetch latency
  histogram, wipe events.

Exit: unit tests for lifecycle (clean reuse, unclean wipe, version-mismatch
wipe), round-trip, concurrent fetch-under-write.

### Phase 3 — Demotion (agg operator + coordinator)

1. **Restorable-epoch visibility.** Coordinator publishes the latest
   restorable epoch into a shared atomic readable by the pipeline callback /
   graph (one `Arc<AtomicU64>`, no new channels).
2. **Cold-slice tracking in the coordinator.** Extend the pending-state
   contract so a vnode-operator slice can be staged as `Unchanged` (no bytes)
   instead of full bytes; the coordinator emits the reference partial from its
   recorded base as it does today. When `max_ref_age` forces a full re-upload
   of a cold slice, the coordinator requests the bytes from the tier worker
   (Ring 1, during the existing staging window) instead of from memory.
   `last_vnode_uploads` keeps `(epoch, hash)` for cold slices — exact bytes
   live in fjall; full bytes are retained in RAM only for hot slices.
3. **Demotion trigger.** When the Phase-1 budget crosses a demote watermark
   (default 80%), the graph asks tier-capable operators for demotion
   candidates: vnode slices ranked by idle time (max `last_updated_ms` in the
   slice), eligible only if the coordinator's records show the slice is
   currently in reference state (byte-identical to a durable upload at a
   restorable epoch ≤ the published atomic).
4. **Demotion execution** (two-step, crash-safe in either order because the
   object store stays truth): Ring 1 writes the slice bytes to fjall →
   confirms → next cycle the operator drops those groups from the map and
   marks the vnode cold. New `GraphOperator` default-method hooks:
   `demotion_candidates()`, `demote_vnode(vnode)`, `cold_vnodes()` — only the
   agg operator implements them in v1.
5. **Capture-path change.** `checkpoint_groups_by_vnode` skips cold vnodes;
   `capture_vnode_states` stages them as `Unchanged`.

Exit: unit tests — demoted slice keeps emitting references; forced full
re-upload streams from fjall and matches byte-for-byte; recovery from a
checkpoint taken while slices were cold restores all groups; budget falls
after demotion.

### Phase 4 — Promotion (async, never blocking)

Copy the lookup-enrich pattern into the agg path:

1. On `process_batch`, partition input rows by vnode (`key_hash % vnode_count`,
   `state/vnode.rs:302`). Rows hitting cold vnodes go into a `PendingBatch`
   (slots + `ingest_watermark`); a `Fetch` is submitted for each cold vnode
   (deduped while in flight).
2. Worker fetches slice bytes from fjall, returns them; next cycle the
   operator decodes `AggStateCheckpoint` and `merge_groups` it back in
   (`aggregate_state.rs:1376` — additive, but the slice was dropped from
   memory so the merge is into an empty key range; assert no overlap), marks
   the vnode hot, notifies the coordinator the slice is hot again (bytes
   retained in RAM resume; fjall entry dropped), and replays deferred rows.
3. Backpressure identical to lookup-enrich: `watermark_hold()` = min pending
   ingest watermark; `wants_input()` caps in-flight deferred rows.
4. Checkpoint while promotion is in flight: serialize pending batches +
   replay queue with their ingest watermark, exactly as lookup-enrich does
   (`lookup_enrich.rs:740-779`). A barrier does not wait for promotion.
5. Promotion storms: cap concurrent in-flight promotions; over cap defers via
   `wants_input()`.

Exit: unit tests — deferred rows produce identical results to never-demoted
baseline (golden diff); checkpoint/restore mid-promotion; watermark held while
rows pending; thrash test (alternating hot/cold access) stays correct.

### Phase 5 — Recovery, rebalance, soaks

- **Unclean restart**: tier wiped, vnode set rehydrates from last restorable
  epoch via the existing path — verify with the kill -9 exactly-once diff soak
  extended to run with `state-tier` on and a small budget (forces demotion
  under load).
- **Rebalance**: releasing a vnode drops its fjall entries; acquiring one goes
  through the existing object-store rehydration into memory (it arrives hot,
  may demote later). Extend `cluster_integration` with a rebalance while
  slices are cold on the donor.
- **Bounded-RAM soak**: open-key-space workload (high-cardinality GROUP BY)
  with state ≫ budget; assert stable RSS, no OOM, correct final answers, and
  demotion/promotion counters moving.
- Grafana: panel set for tier size, promotions/sec, fetch p99, over-budget.

### Deferred v2 — group granularity (separate ADR amendment + plan)

Group-level delta artifacts (`VnodePartial` gains a delta form: base epoch +
changed groups), dirty-group tracking in `AggregateState`, chain-resolving
recovery and prune, compaction epochs streaming the cold portion from a fjall
snapshot. Unlocks: per-group demotion (skew-proof), per-epoch capture cost
O(dirty) instead of O(state). Do not start until v1 telemetry shows vnode
granularity leaving real memory on the table, and price it against the ADR's
stated evolution path (point-readable artifacts + cache-over-truth) before
committing — group-level deltas are a step toward SST-like artifacts anyway.

## 4. Risks / open items

- **fjall behavior under our shape is unproven** — that's what Phase 0 is
  for; the fallback (purpose-built hash-log) is named in the ADR.
- **Compaction CPU on a pinned engine**: verify thread-count/affinity control
  in Phase 0; if fjall can't bound it, that's a gate failure.
- **Uniform-access workloads don't demote at vnode granularity**: accepted v1
  limitation; budget backpressure (Phase 1) is the safety net; v2 fixes it.
- **Coordinator/operator cold-state handshake** (who believes a slice is
  cold) must be single-writer: the graph cycle drives all transitions;
  coordinator only reads staged markers. Keep the state machine in one place
  (operator side) to avoid split-brain between `pending_vnode_states` and the
  map.
- **Dirty-set pinning** (ADR negative): a workload touching every vnode every
  epoch never has clean slices → nothing demotes → backpressure. Surfaced by
  the over-budget metric; correct by design but worth a docs note.
- **Windows dev environment**: fjall is pure Rust (no perl/openssl issues),
  but Phase-0 numbers must come from target-like Linux NVMe, not the dev box.

## 5. Suggested PR slicing

1. PR-1: Phase 1 (budget + accounting + metrics) — no fjall, no feature flag.
2. PR-2: Phase 0 harness + recorded results + ADR status flip/amendment.
3. PR-3: Phase 2 tier service (feature-gated, dormant).
4. PR-4: Phase 3 demotion + coordinator `Unchanged` staging.
5. PR-5: Phase 4 promotion + correctness tests.
6. PR-6: Phase 5 soaks/integration + Grafana + docs.

# Performance & Correctness Audit — 2026-07-03

Five-subsystem audit (execution hot path, checkpoint/EO, cluster/shuffle, connectors,
state tier) of main @ `f4abe07b`. All P0/P1 findings below were verified against source;
file:line references are to that commit. Findings the audit explicitly checked and found
sound are listed at the end so they aren't re-audited.

Two findings likely explain the known "EO-Kafka soak flaky on baseline" mystery: CP-1
(multi-source barrier leak → duplicates) and CP-3 (commit-marker-before-Kafka-txn-commit
→ loss window).

---

## Phase 1 — Exactly-once / at-least-once protocol holes (single node)

Small, surgical fixes; each independently testable. Do these first.

### CP-1 (P0, verified) Multi-source barrier alignment leaks epoch-N+1 data into epoch-N state
`crates/laminar-db/src/pipeline/streaming_coordinator.rs:500,507-515,873-880`
`barrier_seen.clear()` runs at the top of every cycle, then `post_barrier_buf` is
re-processed through `process_msg`, which no longer defers those batches (the
`barrier_seen.contains` check at :873 is now empty). With ≥2 sources, an aligned source's
post-barrier batch is folded into operator state (and the open EO sink txn, and
`pending_offsets` at :882) one cycle later, while the other source's barrier is still
pending. Manifest offsets = at-barrier; state/sink = past-barrier → duplicates on every
recovery. Single-source pipelines are unaffected (alignment completes in-cycle).
**Fix:** while `pending_barrier.active`, seed `barrier_seen` from
`pending_barrier.sources_aligned` at cycle start instead of clearing unconditionally.
Also skip the `pending_offsets` overwrite for deferred batches.
**Test:** 2-source EO kill-9 soak with barriers forced to span cycles (slow second
source); assert zero dups after recovery.

### CP-2 (P0, verified) `reconcile_prepared_on_init` runs before any sink is registered → recovery re-drive is a production no-op
`crates/laminar-db/src/pipeline_lifecycle.rs:621` (reconcile) vs `:1248` (`register_sink`)
At reconcile time `coordinator.sinks` is empty: marker-committed Pending epochs are never
re-driven, unmarked Pending epochs are never actually rolled back. This is the concrete
bug behind the standing "coordinated_recovery re-drive" recommendation. Test harnesses
register sinks before reconciling, masking it.
**Fix:** move the reconcile call after sink registration (before `begin_initial_epoch`
at ~:1491), or lazily run it on the first checkpoint with sinks present.

### CP-3 (P0, verified mechanism) Kafka EO: commit marker durable before a non-resumable txn commit → loss window
`crates/laminar-db/src/checkpoint_coordinator.rs:2513-2537`;
`crates/laminar-connectors/src/kafka/sink.rs` (`pre_commit` = flush only; `open` →
`init_transactions` aborts orphaned txns)
Order is `record_committed(epoch)` → `commit_sinks_tracked(epoch)`. Kill-9 in between:
restart aborts the open Kafka txn (records gone), but the marker exists so recovery
restores manifest N with offsets past the lost output. The marker design assumes commits
are re-drivable after restart — true for coordinated sinks, false for the Kafka producer
txn. Related, same root:
- `Failed` sink statuses are terminal — `reconcile_prepared_on_init` only re-drives
  `Pending` (`checkpoint_coordinator.rs:1451-1455`), contradicting the comment at
  :2555-2557.
- `drive_follower_commit` (:2172-2186) rolls back on partial commit failure *after* the
  global commit decision.
**Fix:** for non-re-drivable sinks, record the marker AFTER `commit_sinks_tracked`
(recovery already treats no-marker-Pending as abort, and an aborted Kafka txn is
invisible → rewind+replay preserves EO). Treat `Failed` like `Pending` in reconcile.
Remove the follower rollback-after-decision path.

### CP-4 (P0) EO sink write failure poisons the epoch, aborts the txn, and the pipeline keeps going
`crates/laminar-db/src/sink_task.rs:496-560`,
`crates/laminar-db/src/pipeline_callback.rs:1144-1160`,
`streaming_coordinator.rs:1021-1026`
Write error → batch dropped + epoch poisoned → next pre-commit fails → real
`abort_transaction` → fresh txn, pipeline continues from current position → next
checkpoint seals offsets past the discarded rows. Unlike SQL cycle errors
(`fault_on_cycle_error`), sink failures never fault the pipeline, so the only replay
mechanism never runs.
**Fix:** under EO, escalate `WriteError`/`WriteTimeout`/poisoned-epoch to a pipeline
fault (same path as cycle errors).

### CP-5 (P0, verified) At-least-once sinks are never flushed at checkpoint — buffered rows lost while offsets commit
`crates/laminar-db/src/checkpoint_coordinator.rs:902` (`filter(|s| s.exactly_once)`);
`sink_task.rs:68-71` (on-demand Flush is `#[cfg(test)]`);
Mongo/PG sinks buffer until batch_size/flush_interval.
Checkpoint manifests record offsets past rows still sitting in sink memory; crash before
the periodic 5s flush = at-most-once under the ALO contract.
**Fix:** in `pre_commit_sinks_inner`, drive `pre_commit` (default impl = flush) for ALL
sinks; EO sinks keep 2PC semantics, ALO sinks just flush before `save_manifest`.

### CP-6 (P1) ALO: sink `WriteError`/`ChannelClosed` only bump metrics while offsets advance
`pipeline_callback.rs:1144-1160,1462-1469,1710-1715`; `streaming_coordinator.rs:579-582`
Only `WriteTimeout` skips one checkpoint (which restores nothing — the next interval
commits anyway); a dead sink channel silently `break`s. Loss-not-dup under ALO.
**Fix:** treat WriteError like WriteTimeout at minimum; properly: hold offset
advancement or fault until the failed batch is rewritten; add bounded sink-side retry.

### EX-1 (P0, verified) `MvStorageMode::Aggregate` keeps only the LAST batch of a multi-batch cycle
`crates/laminar-db/src/mv_store.rs:329-333` (clear-per-update) +
`pipeline_callback.rs:1360-1363` (update called per batch in a loop)
Any non-incremental GROUP BY MV (CachedPlan/CachedPhysical path — join+aggregate, or
failed `IncrementalAggState::try_from_sql` introspection) whose output exceeds one
DataFusion output batch (8192 rows, or >1 partition in cluster) silently truncates:
SELECTs and checkpoints see only the final chunk.
**Fix:** clear once per cycle — pass the cycle's `&[RecordBatch]` to `MvStore::update`,
or track a cycle epoch in `MvEntry` and clear on first batch of a new cycle.
**Test:** MV with >8192 groups via the fallback path; assert full group count.

### EX-2 (P1) Graph-level deferral advances the output watermark past buffered input
`crates/laminar-db/src/operator_graph.rs:1989-2028,2069-2092`
`ShuffleNotReady`/`depends_on_stream` deferral preserves input for retry, but
`propagate_operator_watermark` already advanced `output_watermarks[node_id]`; downstream
EOWC/window operators close windows and late-drop the replayed rows. (The backpressure
Skip path freezes the watermark correctly; the state-tier deferrals hold correctly via
`watermark_hold` — only graph deferral is broken.)
**Fix:** pin `output_watermarks[node_id]` at its previous value while a node holds
deferred input.

### EX-3 (P2) `emit_changelog_delta` loses the dirty set on mid-emit error
`crates/laminar-db/src/aggregate_state.rs:1390,1435-1436`
`std::mem::take(&mut self.dirty_keys)` is only restored on success; a mid-loop `?`
permanently drops all other pending groups → stale downstream state.
**Fix:** drop-guard that restores/merges the taken set on the error path.
Same family: shared-source isolation replay double-applies into stateful operators that
fail after mutating state (`operator_graph.rs:2032-2040` + agg/interval-join emit paths)
— only preserve-for-replay for errors known to precede state mutation.

---

## Phase 2 — Connector delivery semantics

### CN-1 (P0, verified) PG CDC advances the replication slot on every poll, not at durable checkpoint
`crates/laminar-connectors/src/cdc/postgres/source.rs:803-818`;
`streaming_coordinator.rs:299` calls `connector.checkpoint()` per successful poll
`checkpoint()` side-effects `confirmed_lsn_tx.send(polled_lsn)` → WAL reader calls
`update_applied_lsn` → PG reclaims WAL for data that is only in-pipeline. Crash →
recovery needs LSN range PG already reclaimed → loss (or failed resume). KafkaSource
does this correctly via `notify_epoch_committed`.
**Fix:** delete the send from `checkpoint()`; override `notify_epoch_committed` and send
the durably-committed epoch's LSN there.

### CN-2 (P0, verified) Composite-PK changelog DELETE deletes the cross-product
`crates/laminar-connectors/src/postgres/sink.rs:237-256`
`id = ANY($1) AND name = ANY($2)` with deletes for (1,'a') and (2,'b') also deletes
(1,'b') and (2,'a'). `test_build_delete_sql_composite_key` asserts the broken shape.
**Fix:** tuple-wise: `DELETE FROM t USING (SELECT UNNEST($1) AS id, UNNEST($2) AS name) d
WHERE t.id = d.id AND t.name = d.name` (UNNEST zips positionally).

### CN-3 (P0) PG sink CDC (`_op`) path: delete-then-reinsert of a key in one flush ends deleted
`postgres/sink.rs:513-580` — applies all upserts then all deletes; D(k)→I(k,v) nets to
deleted. Kafka-upsert and Mongo route CDC through `collapse_cdc`; PG doesn't.
**Fix:** run changelog collapse on the `_op` path so each key contributes one terminal
U/D.

### CN-4 (P1) Kafka source records offsets before deserialization succeeds
`kafka/source.rs:1307,1535-1543` — whole-batch decode failure / poison-pill escalation
returns Err after `offsets.update_arc` already ran; shutdown-drain and barrier-capture
paths persist those offsets → the escalation designed to stop-before-loss loses the batch.
**Fix:** stage per-poll offsets; fold into the tracker only after deser succeeds.

### CN-5 (P1) Kafka sink: DLQ routing failure reported as success
`kafka/sink.rs:799-830` — with a DLQ configured, `write_batch` returns Ok even when the
DLQ send itself failed (broker likely down = correlated). Record exists nowhere; epoch
commits.
**Fix:** count DLQ failures as write failures (poison/retry the epoch).

### CN-6 (P1) PG sink EO commits externally in phase 1 → duplicate rows in append mode
`postgres/sink.rs:860-898` — pre_commit does BEGIN…COMMIT before the manifest persists;
crash-in-window + epoch-id never reused → dedup check passes and replays. Upsert mode is
PK-idempotent; append (COPY) mode duplicates.
**Fix:** `PREPARE TRANSACTION` (commit in commit_epoch), or store source-offset
high-water in `_laminardb_sink_offsets` for content dedup, or reject EO+append at open().

### CN-7 (P2) PG CDC poll error drops already-drained WAL payloads
`cdc/postgres/source.rs:759-761` — decode failure on payload k drops k+1..n from the
local Vec; later polls advance write_lsn past them.
**Fix:** retain undecoded payloads in a deque across polls.

---

## Phase 3 — Cluster: give the shuffle a delivery contract

Root cause shared by CL-1/CL-2 (found independently by two audits): layers above assume
exactly-once channel semantics between barriers; the transport is at-most-once
fire-and-forget with no acks, sequencing, or epoch fencing.

### CL-1 (P0, verified) Partial remote shuffle send + cycle retry double-counts pre-agg rows
`crates/laminar-db/src/operator/sql_query.rs:1074-1108` (per-peer sends with `?`
mid-loop; per-batch unassigned pre-check) + `operator_graph.rs:2012-2028`
(preserve-for-retry). Send to A succeeds, send to B fails → whole input retried next
cycle → A folds a second copy into SUM/COUNT; repeats every cycle until membership sheds
B. No receiver dedup; no cluster rewind under ALO → permanent corruption.
`lookup_enrich.rs:561-602` has the same pattern.

### CL-2 (P0, verified) Shuffle transport silently drops queued/in-flight frames on connection break
`crates/laminar-core/src/shuffle/transport.rs:222-232,291-317,354-363`
`send_to` = Ok-on-enqueue; driver does `let _ = client.shuffle(...)`; `Drop for
PeerConn` aborts the driver with up to 1024 queued messages; next send reconnects
transparently. A transient TCP break between two live nodes (no membership change → no
recovery trigger) = permanent under-count that the next barrier commits around.

**Fix for both:** small frame header (sender id, epoch, per-connection sequence) +
receiver-side dedup + cumulative ack with sender retention until acked — or the cheaper
epoch-fencing variant: any mid-epoch reconnect or partial send poisons the epoch and
forces domain replay from the last checkpoint (all-or-nothing staging: resolve/validate
every target and build all outbound frames before the first `send_to`).

### CL-3 (P1) No gRPC message-size limits and no byte-chunking of shuffle frames
No `max_decoding_message_size`/`max_encoding_message_size` anywhere; tonic default 4MB;
`slice_batch_by_targets` emits one frame per (node, batch) unchunked. Oversized frame →
stream break → CL-2 silent loss. `RemoteScan` chunks by rows only (8192) — wide rows can
exceed 4MB too.
**Fix:** explicit size limits both sides + byte-based chunking (the per-stage
`BatchStreamEncoder` already supports multi-chunk).

### CL-4 (P1) A moved group with no post-move input vanishes from the distributed union
`aggregate_state.rs:2109-2117` (merge restores `last_emitted`), `:1404-1427` (emit gated
on `!= last_emitted`), `:2199-2228` (loser retraction)
Loser retracts g from its MV; gainer rehydrates with `last_emitted[g]` set → first emit
suppressed → no node's MV has g until g gets new input. The soak test pushes post-move
input, masking it.
**Fix:** don't restore `last_emitted` for vnodes acquired via rebalance (vs same-cycle
reacquire), or mark merged groups force-emit.

### CL-5 (P1) One quiet node pins the cluster-min watermark forever
`pipeline_callback.rs:741,470-480`; `cluster/control/barrier.rs:80-90`
Only never-saw-data (i64::MIN) is treated as infinity; a node whose partition goes quiet
after any traffic pins the cluster min → windows/EOWC/idle-TTL stall cluster-wide.
**Fix:** per-node idleness timeout — report `local_watermark_ms: None` in BarrierAck
after N ms without progress (cross-node equivalent of Flink withIdleness).

### CL-6 (P2) Resume-gate (10s const) vs configurable quorum_timeout coupling
`pipeline_callback.rs:822` vs `checkpoint_coordinator.rs:152` — safety of
fold-during-alignment rests on gate > quorum; user-set quorum_timeout ≥ 10s reopens the
epoch-contamination window. **Fix:** derive the gate from quorum_timeout or assert the
relation at startup.

### CL-7 (P2) Stale staged rehydration chains survive rapid successive moves
`db.rs:723-728`, `operator_graph.rs:756-772` — acquire→lose→re-acquire can drain an old
chain and resurrect retracted state; also an unbounded map.
**Fix:** `retain(|v,_| owned.contains(v))` on each adoption/drain.

### CL-8 (P2) Distributed SELECT silently returns partial results when a peer is skipped
`distributed_scan.rs:306-319` — skipped peer = smaller result + warn metric only.
**Fix:** error by default (or a result-level partial flag) for a database read path.

### CL-9 (P2, perf) Shuffle data-plane nits
Per-row `registry.owner(v)` RwLock reads ×2 per batch (pre-check + slicing — snapshot
once per batch); RowConverter rebuilt per batch and rows hashed up to 3×/cycle;
receiver-side per-vnode re-fragmentation (`transport.rs:700-737`) when the vnode id is
discarded anyway outside state-tier deferral; queues bounded by count not bytes;
unbounded `Holdover.staged` for stages with no live drainer.

---

## Phase 4 — Checkpoint-path performance (gates the 100ms cadence)

### CK-1 (P1) Full-state capture on the pipeline thread with ~3× transient copies
`pipeline_callback.rs:1072-1102`; `operator_graph.rs:2570-2587,2733-2737`;
`aggregate_state.rs:1529-1571`; `incremental_join.rs:105-113,605-632,1312-1345`
Every epoch, synchronously on the compute thread: all accumulators re-encoded, per-group
`last_emitted` as 2 tiny IPC streams per group, join Z-sets deep-cloned then re-encoded
cell-by-cell, MV stores rematerialized under the store read lock; then the per-operator
bytes are re-embedded into a rkyv `GraphCheckpoint` (copy 2) and `AlignedVec::to_vec()`
(copy 3). With EO the whole durable tail (manifest/partial PUTs, gate, txn commit) also
runs inline → checkpoint interval becomes the throughput ceiling.
**Fix:** hand `pack_operator_states` the per-operator Bytes map directly (drop the
GraphCheckpoint wrapper); finish the delta-checkpoint Lever-2 wiring for agg captures;
serialized side-cache for join snapshots invalidated per dirty key; resume the pipeline
after pre-commit+capture and gate only the EO sink's next begin_epoch on commit
completion; write the manifest once (statuses live in a sidecar), not twice.

### ST-1 (P0 for Lever-2 enablement) Delta-chain checkpoint loses one epoch of changes after a failed partial write
`aggregate_state.rs:1858-1866` (dirty sets cleared + chain len advanced at capture) +
`checkpoint_coordinator.rs:1269-1275` (parent link only on success)
Failed epoch N → next delta chains to N−1 structurally but its content only covers
N→N+1; recovery silently misses N−1→N changes and lost tombstones resurrect groups.
Default-OFF today; MUST fix before enabling delta checkpoints.
**Fix:** on epoch failure notify operators to restore dirty sets or force
`delta_chain_len.remove(v)` / dirty_all → next capture re-bases FULL.

### ST-2 (P1) Promotion `DropGroup` races the in-flight epoch's cold-group fetch → repeated epoch failures
`incremental_join.rs:1109-1122`, `sql_query.rs:774-830` vs
`checkpoint_coordinator.rs:577-586` — barrier stages cold keys; ALO durable tail fetches
them in the background; a cycle promoting a cold key issues DropGroup on the same
single-flight worker → fetch returns None → epoch failed. Demotion is gated on
checkpoint_in_flight==0; promotion drops are not gated at all.
**Fix:** defer tier drops until no epoch that staged the group is in flight (queue drops
keyed by epoch, release on completion).

### ST-3 (P1) Whole cold tier re-fetched sequentially + O(N²)-merged + re-uploaded every epoch
`checkpoint_coordinator.rs:87-89` (ColdGroups never matches reference dedup),
`:599-603` (one-at-a-time fetch), `aggregate_state.rs:445-528` (`append_disjoint` →
`concat_columnar_ipc` decode+re-encode per merge step)
Checkpoint cost scales with cold-tier size, not hot-delta size — the opposite of what
tiering buys. With EO this lands inline on the pipeline task.
**Fix:** cache/reuse the merged cold-only partial per vnode (cold set unchanged ⇒
byte-identical; key on group-key-set hash); batch tier fetches; make `append_disjoint`
accumulate and encode once (join merge already does).

### ST-4 (P1) Per-group cold-blob framing is 10-100× payload; agg state encoded twice per epoch
`aggregate_state.rs:2385-2413,236-282,2426-2430`; `incremental_join.rs:911-931,605-632`
One full IPC stream (schema + padding) per key, per accumulator, per last_emitted entry
— a ~40-byte group becomes multiple KB. And in the single-node group-demotion config the
entire agg state is columnar-encoded twice per epoch (manifest + per-vnode partials),
with last_emitted as 2N tiny IPC streams both times.
**Fix:** compact per-group codec with operator-invariant schema hoisted out (schema
registry or raw-row encoding); derive the manifest blob from the per-vnode captures.

### ST-5 (P2) Tier store: read-before-write doubles fjall I/O; extra blob copy on get
`state_tier/mod.rs:94-115,128,131-147` — every put/remove does a full vlog get first,
only for the byte gauges; `get_key` copies the fjall Slice.
**Fix:** in-memory per-key length map (tier is wiped on restart) or approximate gauges.

### ST-6 (P2) Budget under-counting + demotion starvation
`aggregate_state.rs:1516-1522`; `incremental_join.rs:1383-1385`;
`checkpoint_coordinator.rs:359-362`; `pipeline_callback.rs:2020`
`last_emitted` map, cold-key indexes (`Vec<ScalarValue>` per demoted key), deferred
batches, and the coordinator's pinned last-full-upload bytes are all outside
`estimated_size_bytes`; demote loop compares serialized bytes freed against an in-memory
target; demotion requires checkpoint_in_flight==0, which pipelined sub-second epochs can
starve → budget response degrades to pausing intake.
**Fix:** account the misses; demote-loop compares like units; allow demotion of
clean-since-captured groups while an epoch is in flight.

### ST-7 (P2) Dead tier worker wedges operators silently
`incremental_join.rs:772-787` + agg equivalents — `TryRecvError::Closed` discards the
in-flight fetch without counting a miss; deferred queue never drains, watermark_hold
pins forever, no log. **Fix:** treat Closed as a miss → LDB-3005 escalation.

### ST-8 (P2) O(cold) idle-TTL scan every cycle; demotion of deferred-referenced keys; heal leaks tier entries
`sql_query.rs:836-847` → `aggregate_state.rs:2535-2542` (full cold-map scan per cycle —
use a min-heap by frozen last_updated_ms); `incremental_join.rs:1436` (demotes keys the
deferred queue is about to promote → churn); `aggregate_state.rs:2322-2336` (#450 heal
drops the marker but never issues DropGroup).

---

## Phase 5 — Hot-path performance

### HP-1 (P1) Per-cycle O(all groups/keys) expiry scans
`aggregate_state.rs:1033-1038` (`evict_idle` iterates the full group map every cycle,
even empty-input ones); `interval_join.rs:108-122,719-733,800-813` (`evict_before`
split_offs every key's BTree + full index retain per watermark advance; LEFT/FULL walk
every key in emit_unmatched). 1M groups × 100 cycles/s = 10⁸ visits/s dwarfing delta
work. **Fix:** time-bucketed expiry index (BTreeMap<bucket, Vec<key>> or wheel), scan
only expired buckets.

### HP-2 (P1) Keyed-aggregate take storm
`aggregate_state.rs:1248-1310,1133-1187` — per touched group per batch: fresh
UInt32Array + one `take` per agg input + boxed update_batch; 8192 rows × 4096 groups × 3
cols ≈ 16K kernel calls/batch. Plus `dirty_keys.insert(row_ref.owned())` allocs when
already present. **Fix:** lexsort indices by group, process contiguous runs via slice
(one sort per batch); probe before owned insert; longer-term GroupsAccumulator-style
vectorized path for non-retractable aggs.

### HP-3 (P1) Subscribed changelog MV rebuilds + broadcasts its FULL snapshot every cycle
`pipeline_callback.rs:1370-1390`; `mv_store.rs:179-202,277-292` — full O(rows)
rematerialization (per-cell ScalarValue clone) + full snapshot into the broadcast log
per update cycle. **Fix:** coalesce/throttle snapshots, or send per-cycle deltas (Upsert
store knows changed keys) with snapshot-on-subscribe.

### HP-4 (P1) ChangelogEnrichOperator re-plans physical EACH cycle
`operator_graph.rs:346-356` — SessionState clone + create_physical_plan (~0.5–2ms) +
task_ctx per cycle per enriched MV. The OnceAsync freeze only requires a fresh
HashJoinExec, not a fresh plan. **Fix:** plan once; per cycle rebuild only stateful
nodes via `with_new_children` (fresh OnceAsync in µs); cache task_ctx.

### HP-5 (P1) Incremental join is ScalarValue row-at-a-time with double per-cell clones
`incremental_join.rs:224-257,342-375,377-396,49-80` — 2 Vec<ScalarValue> per delta row
in, clone-into-proj + clone-again-in-iter_to_array per output cell. 10-20× ceiling vs
Arrow-native. **Fix (incremental):** build output columns from &[ScalarValue] refs
(single clone); hoist downcasts; longer-term arrow-row-encoded rows like the agg.
Related: `demotable_keys` deep-clones the entire two-sided Z-set per ≤256-key demotion
pass (`incremental_join.rs:1001-1026`) — iterate keys() instead; and
`cold_groups_touched` allocs an OwnedRow per input row when ≥1 group is cold
(`aggregate_state.rs:2577-2585`) — probe by bytes.

### HP-6 (P1) Mongo sink: one awaited RTT per doc in upsert/replace/CDC modes
`mongodb/sink.rs:708-877` — 500-doc flush = 500 sequential replace_one round-trips
(~1K docs/s ceiling at 1ms RTT). **Fix:** `Client::bulk_write` (driver v3, server 8.0+)
or bounded-concurrency pipelining; drop the metric-only per-doc JSON serialization
(`:127`).

### HP-7 (P2) Kafka sink full-producer flush every 1000 records mid-batch
`kafka/sink.rs:769-773,547-551`; default `flush_batch_size: 1000` — Producer::flush
waits for ALL outstanding deliveries ~16×/16K-row batch. Durability point is
pre_commit's flush; QueueFull retry already bounds memory. **Fix:** default the
mid-batch flush off; rely on QueueFull backpressure.

### HP-8 (P2) mv_store: SELECT materializes under the store-wide read lock; errors → empty batch
`db.rs:1655` → `mv_store.rs:179-202,277-292` (query blocks the pipeline's write lock —
priority inversion) and `:197-201,288-291` (`unwrap_or_else(|_| new_empty)` returns zero
rows to SQL on conversion error). **Fix:** Arc-swapped immutable snapshot per cycle;
propagate the error. NOTE: the standing "mv_store snapshot-under-lock race"
recommendation is DOWNGRADED — capture and mutation share the single pipeline task; no
torn read is reachable. These two are the real issues.

### HP-9 (P2) Kafka source per-message alloc + double payload copy; JSON encoder per-row re-copy
`kafka/source.rs:735-742,1304` (payload.to_vec per record, then copied again into
poll_payload_buf; per-message header JSON when include_headers);
`schema/json/encoder.rs:56-60` (columnar encode then per-row Vec re-split). **Fix:**
pooled length-prefixed buffer through the reader channel; offsets-into-one-buffer
contract for the encoder.

### HP-10 (P2) Designated committer LISTs the entire state store every second
`state/object_store.rs:329-349,418-437` + poll loop `pipeline_lifecycle.rs:1280-1297` —
O(store) LIST per second against S3 even when idle. **Fix:** `sealed/` marker prefix
(O(epochs)) or leader-side seal high-water-mark.

### Misc P2/P3 (tracked, not planned)
- Barrier injector truncates checkpoint_id to u32 in release (`barrier.rs:191-204`) —
  make it a hard error; sanity-bound the manifest-seeded next_checkpoint_id.
- `latest.json` pointer update is get-then-put, not CAS (`checkpoint_store.rs:1117-1139`)
  — prefer max(list_ids()) on recovery or conditional PUT.
- Decision-store read failure defaults to Abort in reconcile but uncommitted in recovery
  (`checkpoint_coordinator.rs:1462-1471` vs `recovery_manager.rs:678-684`) — retry/fail
  startup instead of guessing.
- Build-flag downgrade (state-tier → without) warns and drops checkpointed deferred join
  batches (`incremental_join.rs:1363-1377`) — make it a hard error.
- `InMemoryJoinState::upsert` clamps negative multiplicities silently
  (`incremental_join.rs:56-63`) — store negatives or count clamps in a metric.
- Agg tier deferral can reorder batches for order-sensitive accumulators
  (`sql_query.rs:868-896`) — guard if first/last_value ever reaches the tier path.
- fjall: no physical-disk gauge/bound; single-flight worker serializes all tier I/O —
  allow concurrent independent reads.
- Multiset `to_record_batch` expands multiplicity into physical rows at read time.

---

## Suggested execution order

1. **Phase 1** (CP-1..6, EX-1..3): small surgical fixes, each with a targeted test;
   CP-1 + CP-3 first — they likely explain the baseline EO-soak flakiness. Re-run the
   EO-Kafka kill-9 soak (now with 2 sources) as the gate.
2. **Phase 2** (CN-1..7): connector semantics; CN-1/CN-2/CN-3 are user-visible data
   corruption/loss. Gate: PG CDC kill-9 soak + composite-PK changelog test + D→I
   collapse test.
3. **Phase 3** (CL-1..3 as one design: shuffle delivery contract): epoch-fenced frames +
   all-or-nothing staging (cheap) or seq/ack (complete). CL-4/CL-5 are independent
   smaller fixes. Gate: the already-planned cluster kill-9 EO-Kafka soak + a
   connection-break (no membership change) chaos test.
4. **Phase 4** (CK-1, ST-1..4): checkpoint cost proportional to delta, not state; this
   is what makes the 100ms cadence + tiering deliver. ST-1 gates Lever-2 enablement.
5. **Phase 5** (HP-1..6 first): expiry index, take-storm, SUBSCRIBE deltas, enrich
   re-plan, join Arrow-native, Mongo bulk — benchmark each (`cargo bench`, no >5%
   regression elsewhere).

## Audited and found sound (don't re-audit)
Watermark core (min-across-active, idle exclusion, skew guard, late-filter);
emit_changelog_delta NaN short-circuit + retract-before-insert ordering; bounded
channels/backpressure on the cycle path; manifest write atomicity (temp+fsync+rename,
sidecar-before-manifest, torn-manifest walk-back); source barrier ordering
(offsets-at-delivery, advisory broker commits via notify_epoch_committed); shuffle
barrier alignment protocol at defaults incl. #447 pre-staged-row folding; _COMMIT CAS +
committer identity verification; leader lease CAS/fencing; revoke-retraction persistence
+ same-cycle reacquire cancel; state-tier C1-C4 fixes present (C4 has the ST-7 Closed
gap); delta-partial parent-link-after-write + reference-partial forcing; pgwire/WS
SUBSCRIBE laggard disconnect.

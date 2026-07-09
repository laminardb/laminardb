# Cluster kill-9 vnode recovery — root cause + fix (revisions 5–20)

## Revision 20 — the round must also quiesce OWNERSHIP (assignment settle + monotonic gen)

The rev-19 diagnostic (`b7da188f`) attributed the small bidirectional ±3/±6 in one soak run.
Skew (#2) and cut-slip (#1) came back **negative** (`from_handoff=false` count 0 everywhere;
chain-apply epochs aligned with the round target; a wide 1.9s release spread on a round that was
nonetheless *clean*). The assignment race (#3) came back **positive**, with a decisive refinement:
`assignment_version` **agrees** across nodes — the cluster releases the gate at a *stale,
unconverged* assignment.

- eo_kill / eo_kill_delta, gen=1/target=7: one node owns all 32 vnodes, the other two own **zero**
  (`av=1`). Both are clean at gen=2 once `av=2` (13/8/11).
- notier (PASS): `av=2`, 13/8/11 at both gens.
- Corroboration: eo_kill applies a chain at a **stray, non-target epoch** *after* the gen-1
  release — the rotation re-adopting vnodes mid-stream.

Root cause: the round's quiet period covers **state and offsets but not ownership**. A kill →
rejoin is a membership change; the rebalancer rotates on a separate path (`rebalance_debounce`
5s + `watcher_poll` 2s) that is **not** source-gated and does **not** consult `is_recovering`. The
round completes in ~1–2s, releases the gate, and *then* the rotation lands mid-stream: a gainer
double-folds a rehydrated chain over records it already counted (**+3/+6**), and records shuffled
to the old owner during the move are dropped (**−3/−6**). notier passed only because its
assignment already matched membership, so no rotation followed.

Fix A — `await_assignment_settled` before releasing the gate on both the leader and follower
paths. `assignment_reflects_membership` (every vnode owned, every owner assignable, every live
node holding a share when vnodes ≥ nodes) cleanly separates the cases: notier releases
immediately (no pause), eo_kill holds until the rotation lands and is adopted. The rebalancer and
snapshot watcher are not source-gated, so the rotation happens **inside** the no-data window,
where the vnode move + rehydration are trivially exact. Bounded (30s > 5s+2s); releases anyway on
timeout. The recovery fence is dropped *before* the wait, since the rotation's pre-rotation
checkpoint is gated on `is_recovering` and would otherwise deadlock it.

Fix B — monotonic round generation. The soak also showed `gen=1/target=13` with **nodes=1/3**:
two rounds reusing `gen=1`, so every node that already applied gen 1 skipped the second `Start`
(`gen > applied_gen`) and only the driving leader restored. A gossip-KV `max + 1` collides when
the leader that recorded the previous gen dies and its slot vanishes. `gen_id` is now
`max(kv_max + 1, unix_nanos())` — monotonic across leader changes, with the KV max still winning
if a new leader's clock lags. Same class of bug as the rev-19 fault-seq collision.

## Revision 19 — every kill fires a round (boot-unique fault nonce)

## Revision 19 — every kill fires a round (boot-unique fault nonce)

Rev-18 soak (two runs) proved the gate: **follower 0/0 and eo_kill_delta 0/0 rock-solid**, and
notier went 1213→0 across the two runs *purely by getting its second round* (no code change) —
`complete=2` everywhere collapses toward 0. The remaining nondeterminism was round-FIRING: a
second kill occasionally didn't escalate to a round (`complete=1`), falling back to the old
orphaned-offset loss (notier 1213, delta_kill 975, eo_kill 1214 on the miss runs).

Root cause: the fault report used `FAULT_SEQ: AtomicU64 = 0`, a **per-process counter that
resets to 1 on each restart**. A kill-9 makes a fresh process, so the second kill reports the
same `seq=1` the leader already recorded in `handled_faults[node]` from the first kill → looks
already-handled → no round. It was nondeterministic because it also relied on the leader
observing the transient `0` (written by `clear_fault_report` after the first round) over an
eventually-consistent gossip KV before the second kill overwrote it.

Fix (`report_local_fault`): the fault value is now a **boot-unique nonce** (`SystemTime` nanos at
first report, memoized per process in a `OnceLock`, reused within the boot). Every kill of a node
reports a distinct value, so the leader's change-detection (`handled_faults.get(node) != Some(&v)`)
triggers deterministically for every kill and no longer depends on catching the transient `0`.
Within a boot the nonce is stable, so repeated reports still dedup to one round.

Remaining (rev 20): eo_kill (EO + 16KB + **non-delta**) holds a ~155 residual even at
`complete=2` — the deferred `Offset::Stored` fallback on a round-restart re-acquire (an owned
partition absent from the manifest+handoff@T resumes at the broker committed offset instead of
the truncated seal cut). eo_kill_delta (delta chains) is clean 0/0, so it is specific to the
non-delta re-acquire path. Needs its own repro before touching the soak-proven offset machinery.

## Revision 18 — the round's receive-readiness gate (the uniform +3 was a shuffle-replay drop)

## Revision 18 — the round's receive-readiness gate (the uniform +3 was a shuffle-replay drop)

Rev-17 soak: notier 892→3 and delta_kill 1386→5 collapsed (rev-17's self-contained cold
captures fixed the ALO/tier state path), but a **uniform +3-under** persisted on the
fold/round-recovered runs — follower 31→1313 (`+3:1249`), eo_kill 826, eo_kill_delta 1505 — and
it is **bimodal** across identical builds (the follower was 0/633/716/1026 at rev-10; the 4 MB
no-tier follower had nothing change in rev-17). A 5-reader adversarially-verified investigation
(27 facts) placed it precisely, and ruled OUT the operator fold (both routes converge to the
chain head; test proves it) and the cut (`collect_chain` and the offset resume both key off
`recovered.epoch()`).

Root cause — a cross-node shuffle-replay drop during the concurrent round restart:
- The GROUP BY uses the AGG shuffle path (`sql_query.rs:1190-1268`). Shuffle data frames carry
  **no epoch/sequence** (`proto/shuffle.proto:41-49`) and delivery is fire-and-forget
  (`shuffle.proto:11-17`); `send_to` returns Ok on **enqueue** (`transport.rs:228`) and a later
  driver connect/stream failure **drops the queued frames** with no error (CL-2, still open).
- The receiver is bound once at process start and is NOT dropped by `stop_pipeline`; nothing
  drains it while the pipeline is stopped; `purge_shuffle_receiver_buffers` is total.
- `drive_round` ordering: purge → `announce_recover_start` → **leader self-restores inline**
  (re-reads its source from O_T and re-shuffles the replay) → *then* waits the recovered quorum.
  Followers react only on their 200 ms poll. So the leader (restarts strictly first) re-shuffles
  its window into the still-booting recovering node, whose **purge-on-Start** (`coordinated_
  recovery.rs:107`) discards exactly those frames — and fire-and-forget never re-sends. Loss =
  the leader's one partition-slice per group = the uniform +3. Restart-timing dependence is the
  bimodality; the second survivor restarts ~concurrently and lands after the purge (folds fine).

Fix (rev 18) — a **receive-readiness gate on the round** (Flink/RisingWave "all tasks RUNNING
before data flows"): a `source_gate: Arc<AtomicBool>` on `db`, checked at the top of every
source-task poll loop (`streaming_coordinator.rs`). `restore_and_ack` sets it closed **before**
the restart so sources come up paused; both the leader (after its existing restore-quorum wait)
and every follower (new symmetric wait) release it only once the quorum confirms every node has
restarted and rebound its receiver. During the gate no node reads or re-shuffles — the round is
a global quiet period, so no frame can be dropped. Released on timeout too (never wedge sources);
released in the abandon/orphan paths. Recovery-path only; zero hot-path cost. Guard test
`source_gate_holds_intake_until_released`.

Secondary mechanism found (NOT the soak's dominant +3; sequenced to rev 19 with its own repro to
avoid a speculative change to the soak-proven offset path): on a round restart the source is
rebuilt with `open()` before `restore()`, so an owned partition absent from BOTH the manifest and
the handoff@T falls back to `Offset::Stored` (broker group offset, possibly ahead of T) and the
seek block never pulls it back (`recovery_source_checkpoint` topic-scoping + `to_seek_tpl`
omission + `startup_default_offset(GroupOffsets)=Stored`). A real edge for a newly-acquired-but-
unconsumed partition; a contributor to the EO/16KB residual (+5). Fix direction: on a recovery
restart, an owned partition with no staged offset must resume at the truncated seal/handoff cut,
never the broker Stored offset.

## Revision 17 — the tier cold-capture hole (the stuck 16KB rows) + coordinated recovery by default

## Revision 17 — the tier cold-capture hole (the stuck 16KB rows) + coordinated recovery by default

The three tier-backed 16KB scenarios (eo_kill / eo_kill_delta / delta_kill) sat at ~1300–1450
across revs 13–16 while notier and the follower collapsed — the round protocol was never their
mechanism. Code-verified hole (matches the rev-10 fold=72/112-vs-806 diagnostic):

- The tier store is **node-local and wiped on every open** (`state_tier/mod.rs` `remove_dir_all`),
  so cold bytes are durable ONLY if folded into a checkpoint partial at capture/upload time.
- The coordinator's force-fetch fold (`resolve_full_with_cold_groups`, the C2 mechanism) was
  reachable only from the **delta-primary** capture (`VnodeCapture::FullWithColdGroups` at the
  chain re-base).
- The **non-delta authoritative** capture (`checkpoint_by_vnode`, cluster + `skip_whole_node_agg`)
  SKIPPED the resident partial for any cold-bearing vnode on the stale single-node assumption
  that the manifest carries resident — false in cluster since rev 5 — and staged a cold-only
  `ColdGroups` slice. Net: a cold-bearing vnode's durable partial held cold bytes but **dropped
  its resident groups**; the fold restores only what's durable; the tier that held everything
  else died with the node.

Fix: in authoritative mode the non-delta capture stages `FullWithColdGroups { resident,
group_keys }` for a mixed vnode (the coordinator's existing fold makes the durable partial
self-contained); embedded keeps cold-only (resident rides the manifest there — including it
would double-apply). Guard test `authoritative_capture_stages_resident_with_cold_groups`
(vnode_count=1 forces a mixed vnode; old code fails it).

Also (owner directive): **coordinated recovery is now always on in cluster mode** — the
`[supervision] coordinated_recovery` knob and the cluster local-restart fault path are removed
(embedded/single-node keep supervised restart). Cluster pipelines always escalate sink/cycle
faults (`in_cluster()`), since the round replays them.

Companion doc: `docs/plans/checkpoint-restore-production-matrix.md` — the full contract ×
failure-condition matrix across (embedded | single-node | cluster) × (EO | ALO) × delta chains,
with the remaining gaps sequenced (in-repo deterministic recovery matrix next).

## Revision 16 — no formation-time rounds; genesis must out-rank the broker's committed offsets

Rev-15 soak: protocol mechanically healthy (prepare == start == complete, orphan/qtimeout 0),
follower 1720→31, seed 336→892 with the decisive trace `gen1→0, gen2→0, gen3→7, gen4→13` and a
round firing BEFORE the first kill. Decode: gen3/gen4 (epochs 7/13 ≈ kills at 35s/70s × 5s
checkpoints) are the two kills with CORRECT targets — the genesis rounds are both **spurious
formation-time rounds**: at cluster startup, later-booting nodes find the assignment snapshot the
first node just CAS-created, and rev-12's `found_existing_snapshot` heuristic fires
`report_rejoin_fault` from a node that never lost anything. Each such round truncates all state
(nothing committed yet → target 0) while the Kafka sources resume from the **broker's committed
group offsets** — which survive `truncate_after` because they live in Kafka, outside the engine's
durable state. The pre-round window is never re-read: the seed's +5 : 527.

Three fixes, all recovery-path:

1. **Rejoin faults require prior local state.** `report_rejoin_fault` no-ops when the boot
   restored no local checkpoint (`last_recovery_epoch` recorded at every start): a fresh joiner
   (formation, scale-out, wiped disk) lost no in-flight window. The killed node's restart still
   reports — its local manifests survive the container kill.
2. **Genesis out-ranks every surviving offset store.** New `SourceConnector::reset_to_initial`
   (default no-op), called for replayable sources when the armed rewind target is 0. The Kafka
   impl clears staged offsets and arms `force_initial_position`: the open-path assignment AND the
   rotation-rebind fallback bind offset-less partitions at `genesis_default_offset` (never
   `Offset::Stored` — the committed group offsets are the abandoned timeline), cleared after the
   first successful acquisition wave (later acquisitions carry post-genesis handoffs; a stale
   flag would rewind a genuinely-new partition into replayed state).
3. **A round that raced the boot absorbs the rejoin report.** `coordinated_restores` counts
   rounds applied by this process; `report_rejoin_fault` no-ops once a round already restored the
   node (the report used to land after `restore_and_ack` cleared it → the extra 3rd round).

Soak expectations: no round before the first kill; exactly one round per kill with target = the
pre-kill decided epoch; seed collapses toward the follower's residual. The follower's +3 : 25
(the §E16 Uninit-fold class) is the remaining suspect if it survives these fixes — chase the fold
path only if a clean-formation run still shows it.

## Revision 15 — the standard model: one authority, stop-the-world, truncate the abandoned timeline

Rev-14 soak: seed 1463→336 (blob-probe fail-closed works) but the probe defers FOREVER when a
kill lands before the first blob-backed epoch (`decided=1 sealed=1 blobs=0` — epoch 1 sealed at
t≈0 before sources read anything, so its blob write no-ops on the empty offset map: not pruning,
an empty-at-genesis cut). Follower 1→1720 chaotic ±11: `max(decided, sealed)` rewound to
sealed-but-not-sink-committed epochs (3/8/12). And 2 kills still made 3 rounds.

All three findings, and every prior regression in this saga (#3/#4, rev 6, rev 9, rev 14), share
one shape: **two recovery paths reading two different notions of the committed cut** (decision
markers, the durable seal, local manifests, blob presence). The deepest instance:
`checkpoint_coordinator.rs` `allocator.advance_to(epoch+1)` means a rewind to `T` REUSES epoch
numbers `T+1…` while the abandoned timeline's artifacts at those numbers survive in the shared
backend — so `latest_committed_epoch` keeps returning the pre-kill seal (the adopt path resumes
offsets ahead of the rewound state = the +3/+6 UNDER), reused epochs find a foreign `_COMMIT`
marker, and stale partials/blobs collide with the new timeline (the ±11 chaos). No target choice
can fix that.

Fix — the Flink/RisingWave-class recovery model, all recovery-path (zero hot-path cost):

1. **One commit authority.** The round target is the 2PC decision store's `highest_committed()`,
   full stop. The seal is node-local durability, never a rewind target. `Err` defers the round
   (transient I/O must not flap a stop-the-world cycle); `Ok(None)` means genesis.
2. **Two-phase stop-the-world round.** Leader announces `Prepare(gen)` → every node stops, purges
   shuffle buffers, acks (`control:recovery-stopped`) → leader waits the stop quorum (30s,
   best-effort) → **computes the target against the now-quiescent store** (the read IS the cut;
   no probe, no fallback, no race) → truncates → announces `Start(target, gen)` → nodes purge
   stragglers, rewind, restart, ack. Closes the in-flight window (the original ±3 and the
   follower's ±1 residual). A node stopped by `Prepare` whose `Start` never arrives (leader died
   mid-round) restarts plainly after 60s rather than staying wedged.
3. **Truncate the abandoned timeline.** After the stop quorum, the leader deletes every backend
   artifact above the target (`StateBackend::truncate_after`: partials, seals, descriptors,
   srcoff blobs). Every existing reader — adopt, boot, seal, handoff — becomes consistent by
   environment; the soak-proven adopt path is untouched. `recover_to_epoch` breaks same-epoch
   manifest ties by checkpoint id (two timelines can leave two manifests at one epoch; the higher
   id is the live one).
4. **Genesis is a valid cut.** No committed epoch → target 0: truncate everything, every node
   restarts fresh (no manifest ≤ 0 → "starting fresh", sources at initial offsets, full
   recompute). The early-kill case needs no special-casing: rewind-to-1 restores the epoch-1
   manifest's near-initial offsets; empty blobs at that cut are simply "nothing consumed yet".
5. **Faults delay-and-recheck, never drop.** The fault watcher waits out any active round plus a
   3s settle, then reports only if still Faulted. (In coordinated mode a round is a Faulted
   node's ONLY recovery path — rev-14's skip was unsafe as well as ineffective.) Round churn
   self-heals; real faults still trigger; cascading rounds stop.
6. **Retention pin.** The prune horizon is clamped to the highest recorded decision marker so a
   rewind target's artifacts can never be pruned even when sink commits stall behind the
   retention window.

Known residual risks (accepted, logged): a marker-write failure after successful sink commits
(LDB-6038) can under-read the cut by one epoch — the decided epoch is replayed, duplicates only
for non-transactional append sinks; folding the marker into the manifest write (single atomic
commit pointer) is the long-term close. A partitioned node that misses `Prepare` and keeps
writing during truncation is fenced only by the stop-quorum timeout — full close needs
generation-stamped artifact paths.

Soak expectations: one round per kill; `leader announced recovery start` with target = the
pre-kill decided epoch (or 0 on an early kill, followed by full replay and exact totals); every
scenario collapses to 0 — there is no remaining seam that can move counts: no in-flight window
(stop quorum), no stale artifacts (truncation), no offset/state cut divergence (single
authority + adopt reads the truncated seal).

## Revision 14 — never rewind to a pruned cut; no cascading rounds

Rev-13 soak (§E18): seed rounds COMPLETE (coord_complete 0→3) and the **follower collapsed
1097 → 1** (a single −1 group = the pre-declared no-stop-barrier window: one record shuffled into
an already-rewound peer). The rewind protocol is exact when the target is good. The seed regressed
(+3 UNDER on 1256/2000) because its round announced **target_epoch=1** — a pruned cut: the offset
restore found no handoff blobs, fell back to the startup default, and resumed AHEAD of the
deep-rewound state, so the purged in-flight buffers were never re-sent by their senders. Also
2 kills → 3 rounds: round churn (peers restarting → shuffle send failures) reported fresh faults.

Fix (`4a6d382a`):
- `compute_target_epoch` = max(decision cut, durable seal), then **probe the target's handoff
  blobs and defer the round if absent** — a coordinated rewind fails closed rather than resuming
  at the startup default. Logs `decided/sealed/target/blobs`.
- A fault raised while a recovery announcement is active is not reported (the round covers it).

Soak expectations: `coordinated recovery target decided=.. sealed=.. target=..` shows a sane
target (the pre-kill seal, not 1) on seed kills; ±3 collapses to the follower's sub-quantum
residual (±1-class, no-stop-barrier seam — two-phase round if it must go to zero).
Open question if target=1 recurs in the log despite intact artifacts at 7+: the decided/sealed
fields now say WHICH read returned 1 — chase that store's keyspace.

## Revision 13 — the round must survive a killed leader and stale buffers

Rev-12 soak (§E17): the rejoin fault fires (`reported local fault`, `leader announced recovery`)
but seed kills STALL the round (`coord_complete=0` — "no committed epoch yet") and the follower's
COMPLETED rounds double the quantum (±3 → ±6, 825 → 1097). Three seams fixed (`6d1ffccc`):

1. **Skipped rounds consumed the fault** — `take_new_fault` marked the sequence handled before
   `drive_round` bailed on a missing target → no retry, ever. Pending faults are now recorded
   only when a round actually runs; a deferred round retries each 200ms poll.
2. **`compute_target_epoch` had no durable fallback** — DB-level decision store only; on the
   freshly-restarted reclaimed-leader seed it yields nothing while the seal exists. Falls back to
   `state_backend.latest_committed_epoch()`.
3. **The rewind kept the long-lived `ShuffleReceiver`'s buffers** — pre-rewind slices stashed
   there are ALSO replayed by their rewound senders; folding the buffered copy after the rewind
   double-counts (the ±3→±6 compounding). `restore_pipeline` now purges queued slices, staged
   holdovers, and stashed barriers between stop and start.

Known remaining seam (if a sub-quantum residual survives): the round has no stop-barrier — a
not-yet-rewound node keeps shuffling for the announce-to-observe window (~200–400ms) into an
already-rewound peer before its own rewind replays those records. Fix shape if needed: a
two-phase round (stop+ack quorum, then start), or gate source intake on a gossiped
`is_recovering` flag.

Soak expectations: seed kills now log "coordinated recovery complete" (target from the durable
seal); the ±6 collapses; the ±3 collapses with it (the rewind now replays the window exactly
once cluster-wide).

## Revision 12 — the in-flight shuffle window; rejoin triggers the coordinated rewind

Rev-11 soak (§E16): 0/5, and the notier per-group delta histogram decoded the residual —
quantized at ±3 (−3×326 over, +3/6/9×546 under), affecting ~872 groups > the 806-group vnode
share. 3 = the killed node's 3 re-acquired partitions × 1 record each per 12-record round-robin
burst: **the miscounted unit is the killed node's partition slice of every group**, i.e. the
in-flight cross-node shuffle window between the last seal and the kill:

- **+3 over**: the restarted node replays its partitions from the sealed cut and re-shuffles the
  window; survivors' surviving in-memory state already folded the pre-kill sends → double-fold.
- **−3/6/9 under**: records survivors consumed in the window and shuffled TO the dead node died
  with its memory; their offsets moved on, nobody re-sends → per-boundary loss for its groups.

This is the audit's "shuffle no-delivery-contract" P0 made concrete. Exactness requires the
cluster-wide rewind to the sealed cut that coordinated recovery already implements; the missing
piece was the **trigger** — a kill-9'd process cannot report a fault at death and its restart
resets the fault sequence. Fix (`5bb05bd5`): `LaminarDB::report_rejoin_fault` (no-op unless
`coordinated_recovery` on) called by `start_cluster` when a boot finds an existing assignment
snapshot → the leader announces → every node `recover_to_epoch(seal)` → the window replays
consistently on all nodes (the rev-5 boot staging is load-bearing inside the round's restore).

**HARNESS REQUIREMENT (OBSOLETE as of 2026-07-06): coordinated recovery is now always on in
cluster mode — the `[supervision] coordinated_recovery` knob was removed (an existing
`coordinated_recovery = true` line in the TOML is ignored harmlessly).**
Soak expectations: seed logs "reported local fault for coordinated cluster recovery" on rejoin;
leader logs "leader announced recovery" + "coordinated recovery complete"; all nodes rewind; the
±3 histogram collapses. Alternative long-term fix if rewind latency is unacceptable: offset-tagged
shuffle slices + receiver dedup (new protocol; parked).

## Revision 11 — the reader-spawn race: a missed rebind freezes the share

Rev-10 diagnostics (§E15) cross-cut: on the clean witness (`eo_kill_notier`, 4 MB, no demotion) the
Uninit fold restored the COMPLETE share (`groups=806, reemit=13`, every chain applied per the
Agg/drain logs) at both kills — yet the run lost ~the whole share, with per-group finals frozen at
the recovery cut. Complete state + complete emit + total loss ⇒ **post-recovery input never
arrived**.

Mechanism (`9495cf6c`): the Kafka reader task initialized `last_assignment_version` to the registry
version at its own spawn ("open() already assigned at the current version"). Rev-7 broke that
assumption: a restart boots unassigned@v0, `open()` assigns nothing, and the startup adopt (right
after `db.start()`) bumps the version. If the bump lands before the spawned reader runs its init,
the rotation check never fires and the node **never assigns its re-acquired partitions** — every
group frozen at the restored baseline. Seed-biased: only the seed's startup adopt is the acquiring
bump (its shed races its rejoin); a follower's acquiring bump arrives via the watcher seconds after
the reader initialized. Fix: initialize `last_assignment_version`/`last_drain_gen` to 0 — a
spurious first pass reconciles current-vs-owned and no-ops; no race window remains.

Confirmation grep (works on the OLD failing logs): the seed after each kill previously shows NO
`acquired partition resume offset` / `rebound partitions after vnode rotation` lines; with the fix
they must appear at the staged handoff cut. Residual candidate if 16 KB runs still lag the notier
result: cold/demoted groups absent from dirty-vnode captures (fold=72/112 vs share 806 — hot
residue), healed lazily by promotion on the Agg path but not by the fold — fix would be capture-side
(force-fetch cold bytes into FULL partials, as re-base already does).

## Revision 10 — rev-9 reverted; instrumentation for a decisive next run

Rev-9 soak (§E14): follower held 0 ✅; seed REGRESSED (901/859/677/866, over 2→8) — the
deferred-revoke cancel reverted (`a802374c`); **368060c6 (rev 8) remains the code high-water mark.**
The regression falsifies the flap model: under it the cancel was strictly state-preserving, so the
soak's dominant path differs from the unit-reproducible deferred-revoke inversion (that defect stays
real — its correct fix likely needs a per-vnode purge of the earlier fold, not a blanket cancel —
but it is not the soak's mechanism). Rather than a sixth blind fix, `8b275c3b` adds cold-path
instrumentation at the audit's discriminating points.

**Decision tree for the next seed-kill run (grep the seed's logs):**
1. `applied rehydrated vnode chain … groups=G` per vnode at each re-acquire —
   G≈0 → the chain content is short (write side: partials/reference/delta capture);
   G≈full (~50/vnode) but finals half → post-apply loss (fold drop or emission).
2. `rehydration_epoch` at restart-1 vs restart-2 — equal → sealing wedged between the kills →
   state-behind-offsets at restart-2 is the mechanism.
3. `source-offset handoff staged for acquire epoch=E` vs the same adopt's `rehydration_epoch` —
   any skew → the two seal reads diverged inside one adopt.
4. WARNs: `lazy_init fold: deferred revoke intersects re-acquired vnodes` (the flap IS live);
   `rehydration chain has no FULL base`; `no live operator for rehydrated slice`;
   `acquired partition has no handoff or local offset`.
5. `acquired partition resume offset` per partition — compare the replay start against the
   rehydrated epoch's expected cut; `from_handoff=false` = the local-snapshot fallback fired.

**Status:** implemented (2026-07-05), soak-gated. Fixes the cluster kill-9 aggregate under-count the
external per-group state-tier soak surfaced (harness doc §E5–E9). Branch
`fix/cluster-source-offset-handoff-recovery`.

## Revision 9 — a re-acquired vnode's chain cancels its deferred revoke

Rev-8 soak (§E13): follower 0 ✅ robust; seed still 558–726 pure loss ≈ its whole 13-vnode share,
per-group finals at exactly one burst (36/72), `rehydrated=13 rehydration_epoch=Some(9)` logged at
staging. Root cause (deterministic, found via a 5-way parallel code audit):

A restarted node's agg operator stays **Uninit until its first input**, which only arrives after a
rotation assigns partitions. The killed seed's rejoin races the shed publication (survivors need
lease takeover + phi + debounce vs a ~5s restart), so three rotations queue before the first cycle:
startup adopt of the stale owning snapshot, the late shed (revoke), and the give-back (chains).
Cycle 1 then runs the STALE revoke against the FRESH chains: `apply_revoked` before
`apply_rehydrated` (right for an initialized op), the Uninit op defers the revoke into
`deferred_revoke_vnodes`, the give-back chains fold into `pending_restore` — and **nothing removed
a re-acquired vnode from the deferred set**. `lazy_init` restores the sealed baseline, the
force-emit skips every deferred vnode, and the deferred drop destroys the just-restored state of
all 13 vnodes. Offsets resume at the sealed cut → the sealed burst is never replayed; the next
burst rebuilds from zero; later checkpoints seal the loss. The follower never hits it (its shed
completes while it is down — no acquire→revoke→re-acquire prefix); steady runs never revoke.

Fix (`852ea8e9`): `apply_vnode_chain` removes the vnode from `deferred_revoke_vnodes` (a chain
apply IS a re-acquire; logged "re-acquire supersedes deferred revoke"). Idempotent under the flap's
duplicate fold (`restore_groups`/`decode_last_emitted` are keyed last-wins; `apply_delta` is per-key
REPLACE). Guard test `uninit_reacquire_after_deferred_revoke_keeps_restored_state` reproduces the
first-cycle ordering and the 36-vs-72 signature (fails when the fix is neutered).

Audit residuals noted for later hardening (all verified real, none the soak's mechanism):
suppression-after-dedup-commit in the emit tail (self-heals via force-reemit today); the drain
consuming chains on apply-error/no-operator while still marking Active (loud-log candidate);
`is_recently_unresponsive` 60s TTL widening the shed race; empty FULL partials written for owned
vnodes while Uninit; ALO recovery accepting an unsealed manifest.

## Revision 8 — acquired partitions resume from the handoff, not the local snapshot

Rev-7 soak (§E12): follower 69 (over=10/under=6), seed 677–1117 mostly loss. Root cause found in the
vnode re-assign path (`kafka/source.rs`): an acquired partition resumed from the **local offset
snapshot first**, staged handoff second. The local snapshot is always stale for an acquired
partition — `restore()`'s recovered-manifest cut on a restart, or a pre-revoke position from an
earlier ownership stint whose operator state was dropped at revocation. Behind the seal it
double-folds replayed records (the follower's over residual); ahead of the seal it skips records in
neither the rehydrated state nor the replay (the seed's pure loss: its delayed shed races its
rejoin, so the revoke→re-acquire flap leaves the local snapshot ahead of the sealed cut). This also
explains the saga: fix #2 "fixed" the follower by making the local fallback coincidentally correct,
and every revision that shifted the restore cut nicked it again.

Fix (`c0f2bf8d`): `acquired_resume_offset` — handoff first, local as first-rotation fallback, WARN
on the silent startup-default fallback; prune revoked partitions from the local snapshot on
unassign (`retain_assigned`) so a stale stint position can't shadow a later handoff; skip the
manifest restore of keyed changelog MVs on cluster nodes (one writer's slice → ghost keys the
unfiltered distributed union double-counts; adopt's force-emit rebuilds them — Append MVs keep the
restore).

## Revision 7 — restarts boot unassigned; ownership only via adopt

Rev-6 soak (§E11): the follower guard failed (0 → 706 mixed) — that run's follower restarts happened
to **boot owning** (staged=8 at epochs 8 and 13), a path the rev-5 follower run never took (it booted
∅ both kills — the shed race decides). Every path ever measured exact flows through
`adopt_assignment_snapshot`; every failure (rev-5 stale under-count, rev-6 exposed mixed) lives on
the boot-owning path, where a node trusts its possibly-stale persisted snapshot and acts on assumed
ownership — consuming, folding, and emitting outside the adopt protocol (and split-braining with a
survivor that adopted a shed).

Fix: delete the boot-owning path. `resolve_vnode_assignment` boots a restart/joiner **unassigned at
version 0** (`VnodeRegistry::new_unassigned`); after `db.start()`, `start_cluster` re-loads the
stored snapshot and explicitly adopts it — offsets + chains at the sealed cut, Restoring gate, CL-4
force re-emit — with a bounded retry (static discovery has no snapshot watcher to re-drive a
deferred adoption). A full-cluster restart also re-acquires everything through adopt. First-boot
(CAS-create) still pre-owns: no state exists to recover. The rev-5 boot staging becomes dormant in
production (kept: in-repo harness configs still pre-own registries); rev-6's Uninit force-emit stays
load-bearing — the startup adopt can land before the first cycle.

## Revision 6 — force-emit chain-restored groups after the Uninit fold

Rev-5 soak (§E10): follower 0 ✅, EO over-counts gone, seed still under (839–1107). The staging log
proved chains were staged and applied — the loss was **emission**, not state: a chain applied while
the operator is still `Uninit` (boot staging always is — chains drain at the cycle top, before the
first `process`/`lazy_init`) folds via `restore_groups`, which restores `last_emitted` and clears
dirty. That contract ("groups == last_emitted, nothing pending") is right for the embedded manifest
restore but wrong for the cluster chain restore: this process's MV snapshot came from the
per-node-incomplete manifest, so the restored groups' MV rows are stale/absent, and the restored
`last_emitted` suppresses their first emit until NEW input — groups whose bursts already passed stay
stale forever (pure loss, no dups — the observed signature). The initialized (`Agg`-arm) applies
already call `force_reemit_acquired_vnode` — the reason the adopt path is exact; the delta-off chain
path even reached that call, but `if let QueryState::Agg` silently no-ops while Uninit.

Fix: vnodes whose chain lands while Uninit are recorded (`pending_restore_reemit`); after
`lazy_init` folds base+deltas, each is `force_reemit_acquired_vnode`d — skipping vnodes with a
deferred revoke, whose `last_emitted` the drop needs for retractions. Covers boot staging (delta on
and off) and an adopt that lands before first init. Guarded by
`uninit_chain_restore_reemits_groups_after_init` (fails without the fix).

## Soak trend (mismatches; 0 = pass)

| Scenario | #2 `793c9caa` | #3 `0fb07a90` | #4 `e78ef9e5` |
|---|---|---|---|
| follower kill (node-1) | **0 ✅** | 106 | 864 |
| eo_kill (node-0 seed/leader) | 661 | 2000 | 1489 |
| eo_kill_delta | 696 | 1998 | 840 |
| eo_kill_notier | 1736 | 878 | 865 |
| delta_kill | 989 | 598 | 534 |

#3 and #4 changed the adopt/re-acquire cut and regressed the follower case #2 had exact; both are
**reverted** (`74f92d5f`, `6ab5e04e`). #2 is the baseline; revision 5 is additive on top of it and
does not touch the adopt path.

## Root cause (code-verified)

Cluster boot recovery restores aggregate state from the **global first-writer-wins manifest**
(`ObjectStoreCheckpointStore` with empty prefix, `PutMode::Create` — one winner per epoch), which
holds only the **writing node's** vnode slices. Per-vnode partials, by contrast, are globally
complete: the durability gate seals an epoch only when every vnode has one.

The asymmetry that made this leader-kill-specific:

- **Follower killed:** the surviving leader sheds its vnodes before it rejoins. It boots owning
  nothing; its vnodes come back via `adopt_assignment_snapshot`, which rehydrates from per-vnode
  chains + resumes offsets from the handoff union at the seal. That path is exact (soak-proven).
- **Seed/leader killed:** no surviving controller sheds its vnodes before it restarts (election +
  rebalance debounce outlast a container restart), so it **boots still owning them**. With delta OFF
  its aggregate state comes only from the recovered manifest — empty for its vnodes whenever another
  node won that epoch's manifest race — while fix #2 stages complete source offsets. Result: a clean,
  share-sized (~1/3 of groups) under-count that varies run-to-run with the manifest race.
- **Delta ON** avoided the manifest dependency (`stage_owned_vnodes_for_delta_primary`) but read
  chains at `latest_committed_epoch` while offsets restored at `recovered.epoch()` — a divergent cut
  when a leader dies between seal and decision.

`sources_restored=0` is a red herring (coordinator recovers with an empty source slice).

## Fix (boot path only; adopt path untouched)

Per-vnode partials become the authoritative aggregate checkpoint in cluster mode, delta on or off:

1. **Write side:** `SqlQueryOperator::set_vnode_partials_authoritative` — whole-node agg capture into
   the manifest is skipped when cluster-sharded + durable backend (wired in `pipeline_lifecycle`).
   The manifest keeps non-agg operator state; embedded/single-node unchanged.
2. **Boot recovery:** `stage_owned_vnodes_from_chains(recovered.epoch())` runs for every cluster
   recovery with a durable backend (previously delta-ON only), staging each boot-owned vnode's chain
   at the **recovered manifest's epoch** — the same committed cut the source offsets restore at.
   `VnodeRehydrator::rehydrate_at(vnodes, epoch)` pins the read (a decision-committed manifest epoch
   is always sealed, so chains exist at that cut).
3. No overlap/double-apply: manifests written by this build carry no agg state in cluster, so the
   additive `merge_groups` chain apply lands on empty slices.

## Deliberately not done

- Adopt/re-acquire path: untouched (#3's lesson — it is exact for the follower case).
- Ingest barrier for still-Restoring vnodes: deferred; emission gating suffices for the current soak.
- Windowed (non-changelog) aggregates still ride the manifest and keep the per-node-incomplete
  exposure on a boot-owning restart — known residual, separate work.

## Acceptance (owner-run soak)

Cluster kill-9 per-group: 0 mismatches at **both** victims (node-0 seed/leader and node-1 follower),
delta ON and OFF, EO and ALO; follower case must stay 0 (guard against #3-style regression);
steady/no-kill gates stay green.

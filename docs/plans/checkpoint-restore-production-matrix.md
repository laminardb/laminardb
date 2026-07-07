# Checkpoint & Restore — Production Contract Matrix

2026-07-06. The single reference for what checkpoint/restore must guarantee in every
deployment mode, what implements it, which failure conditions each path must survive, what
is closed, and what remains. Supersedes scattered status across the recovery saga
(`cluster-kill9-reacquired-vnode-recovery.md`) and the audit
(`perf-correctness-audit-2026-07-03.md`).

## 1. The contract

| Mode | ALO (`at_least_once`) | EO (`exactly_once`) |
|---|---|---|
| **Embedded** (in-process, no server) | No committed input lost: every source offset advanced past is durable in a manifest only after downstream state + sinks flushed. Duplicates allowed on replay. | State, source offsets, and sink transactions commit atomically per epoch (2PC: prepare → decision marker → commit). Recovery re-drives or rolls back to the marker. |
| **Single-node server** | Same as embedded + supervised restart replays from the last manifest. | Same as embedded + supervised restart resumes the 2PC (reconcile-on-init re-drives marker-committed epochs, rolls back unmarked). |
| **Cluster** | Above per node, plus: the recovery cut is **cluster-wide consistent** — no node's state may include another node's un-replayed shuffle output. | Above, plus: one global commit authority (decision store), barrier-aligned cross-node cuts, coordinated rewind on any fatal fault. |

**Delta chains (state-tier vnode FULL+delta captures)** are an encoding of the same state,
not a different contract: every chain must decode to exactly the state a FULL capture would
hold at that epoch, including cold (demoted) groups.

**Hot path**: none of this may add per-event allocation or locking. Checkpoint capture runs
on the cycle boundary (dirty-set drain, pre-sized buffers); persistence, 2PC, recovery, and
coordinated rounds run on background threads/tasks. Any change to capture must keep
`cargo bench` state-lookup/emit within targets (`.claude/rules/performance.md`).

## 2. Mechanism inventory (what implements the contract)

- **Epoch 2PC**: `checkpoint_coordinator.rs` — pre-commit (flush/prepare all sinks) →
  manifest save → per-vnode partials + durability gate (`epoch_complete` seals) → decision
  marker (`checkpoint_decision.rs`, `checkpoint-decisions/epoch=N/commit`) → sink commit.
- **Recovery**: `recovery_manager.rs` — newest viable manifest (id-tiebreak within an
  epoch), sidecar/state restore, sink reconcile against the decision store.
- **Cluster cut**: barrier alignment (`barrier.rs` + shuffle barrier forwarding), global
  first-writer manifest + per-vnode partials (globally complete via the durability gate),
  per-node source-offset handoff blobs (`epoch=N/srcoff/`).
- **Coordinated recovery** (`coordinated_recovery.rs`, revs 12–16): two-phase
  stop-the-world round — `Prepare(gen)` → all nodes stop+purge+ack → target read from the
  quiesced decision store (genesis 0 if none) → `truncate_after(target)` deletes the
  abandoned timeline → `Start(target,gen)` → rewind+restart+ack → restore quorum. Triggers:
  pipeline fault watcher (settle-recheck) and rejoin faults (prior-local-state-gated).
  **Cluster-default and the only cluster fault path** — the `[supervision]
  coordinated_recovery` knob and the cluster local-restart path were removed 2026-07-06;
  embedded/single-node keep supervised restart (no controller → no rounds).
- **State tier** (feature `state-tier`): vnode/group demotion to a KV tier, FULL+delta
  chain captures, cold-vnode rehydration, promotion-fetch with LDB-3005 escalation.

## 3. Failure-condition matrix

Conditions every mode must handle; ✓ = covered with the closing change; open items → §5.

| Failure | Embedded/Single | Cluster |
|---|---|---|
| kill-9 mid-epoch (before seal) | ✓ epoch unsealed → recovery ignores it; sources rewind to last manifest | ✓ rejoin fault → round rewinds all nodes to the decided cut (revs 12–16) |
| kill-9 between decision marker and sink commit | ✓ reconcile re-drives (CP-2 fix) or, for non-re-drivable Kafka txns, marker ordering (CP-3 fix) | ✓ same + follower rollback-after-decision removed (CP-3) |
| kill-9 of the leader | n/a | ✓ lease/election + reclaim dedup (`a47bebbb`); mid-round leader death → orphan-stop fallback restarts nodes plainly after 60s |
| sink write failure under EO | ✓ escalates to pipeline fault (CP-4) | ✓ same; cluster always faults (`in_cluster()` escalation) |
| sink buffered rows at checkpoint under ALO | ✓ all sinks flushed at pre-commit (CP-5); WriteError ≥ WriteTimeout handling (CP-6) | ✓ same |
| multi-source barrier spanning cycles | ✓ barrier_seen seeded from aligned set (CP-1) | ✓ same + cross-node alignment |
| fault before ANY committed epoch | ✓ restart fresh, sources at configured start | ✓ genesis round: truncate-all + `reset_to_initial` (never broker `Offset::Stored`) (rev 16) |
| in-flight cross-node shuffle at kill | n/a | ✓ stop-quorum before target read + receiver-buffer purge + truncation (revs 13–15) |
| node rejoin after kill | n/a | ✓ adopt path (offsets handoff-first, rev 8) + rejoin round; formation joins excluded (rev 16) |
| rewind reusing epoch numbers | n/a | ✓ `truncate_after` removes the abandoned timeline (rev 15) |
| retention prunes the recovery target | ✓/✓ horizon pinned at commit floor + highest decision (rev 15) | same |
| checkpoint store slow/unavailable | ✓ epoch fails, pipeline continues, next interval retries; decision-store read error defers a round (never poisons a target) | same |
| demoted (cold) state at capture/restore | partially — see §5 gap G1 | **open — G1, prime suspect for the stuck tier soak rows** |
| duplicate/lost shuffle frames on reconnect | n/a | partially — CL-1/3/4 landed; full seq/ack contract = G3 |

## 4. Closed (validated by lib suites + clippy on base/cluster/state-tier; soak where noted)

- Audit Phase 1 CP-1..6, EX-1..3 (`e8baedb1`); Phase 2 CN-1..7 (`014a729c`+`cebe91c8`);
  Phase 3 CL-1,3,4,6,7,8 (`904d408b`); Phase 4 correctness ST-1,2,7 (`e0080251`); Phase 5
  HP-8 error propagation. All on this branch (ancestors of HEAD).
- Recovery revisions 5–16 (see `cluster-kill9-reacquired-vnode-recovery.md`): follower path
  exact since rev 8 (soak 0), coordinated round protocol exact with a good target (rev 13
  soak: follower 1097→1), poisoned targets impossible (quiescent decision-only read, rev
  15), abandoned-timeline truncation (rev 15), genesis semantics incl. broker-offset
  override (rev 16), no formation-time rounds (rev 16).
- Tier correctness C1–C4 (`ec647bd5`, `d80c1b40`, `4b55a096`, `768d3153`).

## 5. Open gaps, sequenced by soak leverage

- **G1 — tier cold-group recovery hole (P0, in progress).** The three stuck soak rows
  (eo_kill / eo_kill_delta / delta_kill, all 16KB tier-backed, ~1400 across revs 13–16
  while notier and follower collapsed) match the rev-10 diagnostic: chain folds restore
  72–112 hot-residue groups vs the ~806 share — cold groups are not in the FULL captures
  and no cluster fold path fetches them. Fix direction: make chains self-contained (embed
  or force-fetch cold bytes at capture/fold), mirroring the C2 re-base mechanism.
- **G2 — deterministic in-repo recovery matrix (breaks the soak-iteration circle).**
  Extend `cluster_soak.rs` / `cluster_integration.rs`: 3 in-process nodes, per-group COUNT
  oracle, abrupt node abort (no final checkpoint) + coordinated round, matrix over
  {EO, ALO} × {tier 16KB, no-tier} × {delta chains on/off}. Every invariant this document
  names gets a red test before the external soak ever runs.
- **G3 — shuffle seq/ack delivery contract (CL-2, deferred).** Coordinated rounds mask
  single-frame loss today (whole-window replay); the per-frame contract remains the
  long-term close for reconnect edges.
- **G4 — Lever-2 delta checkpoints (default-OFF).** State-layer emission + chain recovery
  done; per-operator chain mixing unwired. Gate: G2 matrix green with chains forced on.
- **G5 — accepted residual risks (documented, low likelihood):** LDB-6038 marker-write
  failure after sink commit under-reads the cut by one epoch (fold marker into the manifest
  write long-term); partitioned node writing during truncation (generation-stamped artifact
  paths long-term); per-node watermark idleness (CL-5).

## 6. Validation gates

1. Lib suites + `clippy -D warnings` on base / cluster / state-tier — every change.
2. G2 in-repo matrix — every recovery-path change (target: minutes, deterministic).
3. External harness soak (per-group oracle, docker kill-9) — acceptance: 0 mismatches for
   both victims × all five scenarios, steady/no-kill gates green.
4. `cargo bench` state/emit + Linux-NVMe p99 — any capture-path change (hot-path budget).

# 1B Phase 2 — Increment 2: per-domain isolation under exactly-once / coordinated recovery

Status: **design / not started.** Branch `feat/shuffle-barrier-after-kill-recovery`. Parent:
`docs/plans/pipeline-failure-recovery-architecture.md`. Increment 1 (Slices 1, 2a; Decision A on 2b)
is committed and gives shared- and disjoint-source isolation **on the at-least-once / per-sink-EO
path** (online, with replay). Increment 2 targets the **`fault_on_cycle_error` paths** — full
pipeline `ExactlyOnce` and cluster `coordinated_recovery` — which today **rewind the whole pipeline
on any domain fault**, ignoring domains.

## What "Increment 2" means against the code

`fault_on_cycle_error` (`pipeline_callback.rs:1566`) = `delivery_guarantee == ExactlyOnce ||
coordinated_recovery`. On those paths the coordinator, on *any* domain fault, does
`discard_pending_offsets` + fault → whole-pipeline restart to the last global epoch
(`streaming_coordinator.rs:575`, `coordinated_recovery.rs`). Three coupled pieces would make that
per-domain:
1. **Per-domain barrier alignment** — today one global `PendingBarrier` commits when *all* sources
   align (`streaming_coordinator.rs:955`). Needs a `PendingBarrier` per domain so domain D commits
   when D's sources align, independent of siblings.
2. **Per-domain 2PC / checkpoint** — `checkpoint_with_barrier` (`pipeline_callback.rs:1753`) captures
   **all** operator state, builds **one** checkpoint request, runs **one** leader-tail 2PC
   (`run_leader_tail`) and one cluster shuffle alignment. Needs per-domain capture + per-domain
   epoch + 2PC over only that domain's sinks.
3. **Scoped (region) recovery** — a fault re-seeks **all** sources and rewinds to the global epoch
   (`pipeline_lifecycle.rs` restart, `coordinated_recovery.rs`). Needs to restart only the faulted
   domain's operators + re-seek only its sources.

## Findings that change the value/scope calculus (verified)

1. **The practical mode is ALO + per-sink EO, which Increment 1 already covers.** The server/DDL path
   defaults `delivery_guarantee` to the config default (ALO) and "the server never sets pipeline-level
   `delivery_guarantee`"; full pipeline `ExactlyOnce` is reachable only via the **builder** API
   (`builder.rs:305,582`). So Increment 2 improves a **supported-but-less-common** mode; the common
   EO path (per-sink, ALO pipeline) is Increment 1 territory and is done.
2. **Shared-source EO isolation is fundamentally limited by the single shared cursor.** A shared
   source has one consumer/cursor feeding many domains; per-domain offsets collapse to the global min
   (established in Increment 1 — in-memory replay is lost on crash, recovery is global-min). So
   *scoped recovery of one domain cannot re-seek a shared source without disturbing its siblings.*
   Clean region failover is therefore **disjoint-source only**; shared-source domains under full EO
   keep whole-pipeline recovery (or Increment 1's online isolation when not full-EO).
3. **This is the most delicate, already-flaky core.** The EO barrier-alignment + cross-node 2PC +
   coordinated recovery is exactly where the **EO-Kafka soak is flaky on baseline** (per the parent
   plan and memory), and where the hard-won lessons live (reuse `BarrierCoordinator`, don't ship
   unsoaked distributed recovery, the lock-twice deadlock, etc.). Per-domain restructuring of it is
   XL and high-risk.

## Recommendation (decide before implementing)

**Option A — defer/close Increment 2.** Increment 1 covers the practical failure-isolation scope
(ALO + per-sink EO, shared + disjoint, online + replay). Full-pipeline-EO region failover is a
high-risk change to the flakiest core for a less-used mode. Mark 1B Phase 2 complete at Increment 1;
record Increment 2 as a documented future option. **Recommended** unless full-pipeline EO region
failover is an explicit requirement.

**Option B — scope Increment 2 to disjoint-source region failover.** Implement per-domain barriers +
per-domain 2PC + scoped recovery **only for domains whose sources are not shared** (Finding 2 makes
shared-source EO isolation infeasible without per-domain consumers / a durable log). Reuse
`BarrierCoordinator` for the cluster path; flag-gated default-OFF; soak the disjoint kill-9 EO case
heavily. Large, but the cleanest correct subset.

**Option C — full Increment 2** (disjoint + shared via per-domain consumers or a durable log). Pulls
in the Increment-1 design fork we deferred (durable shared log) to make shared-source offsets
crash-durable and per-domain-seekable. Largest; only if shared-source full-EO isolation is required.

## If proceeding (B), sub-slices (each flag-gated, soaked)

- **2-1** Per-domain `PendingBarrier` + alignment (disjoint sources): a domain commits when its own
  sources align; barrier injection unchanged (per source), but completion tracked per domain.
- **2-2** Per-domain checkpoint capture + 2PC: per-domain operator-state capture, per-domain epoch in
  the manifest, leader-tail 2PC over only the domain's sinks. (Biggest manifest/coordinator change.)
- **2-3** Scoped recovery: restart only the faulted domain's operators + re-seek its sources; leave
  healthy domains running. Cluster path via `BarrierCoordinator` (per-domain recover target).
- **2-4** Soak: disjoint-source EO kill-9 (one domain faults, the other keeps committing; no
  gap/dup on either sink), once the broker env is healthy (≥8 runs).

## Open questions for B/C
- Manifest schema for per-domain epochs (no backward-compat constraint).
- Cluster: per-domain recover targets on `BarrierCoordinator` without per-cycle gossip on the hot
  path (the documented alignment-timeout regression risk).
- Interaction with the convergence gate (`assignment_ready_for_checkpoint`) and the single
  `is_recovering` fence — both currently whole-pipeline.

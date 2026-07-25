# Distributed keyed state Cycle 29 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Protocol commit:** `94a12d52`
- **Cycle outcome:** `TARGET_PREFLIGHT_SEMANTICS_FROZEN_SCHEMAS_BLOCKED`
- **Bounded memory:** reference/conformance-only; no current product or soak profile
- **Maintenance-health v2:** direction approved; candidate mapping remains gated
- **Current product target:** local spill; backend not selected
- **Candidate or backend execution authorized or performed:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**

## Outcome

Cycle 29 freezes the semantic layer beneath the redb actual-target and initial-preflight schemas. It
adds one ordered preflight-observation skeleton, candidate-neutral scalar encodings, exact
fact-source/cause precedence, 29 native and 21 Docker source requirements, their fact projections,
and minimum predicates for all 27 native and 12 Docker checks. It also defines the maximum-width and
cap-plus-one proof required before any final schema or global cap.

Docker v1 now has one topology: `container_per_process`. The alternative local-pidfd grammar is
superseded and no v1 parser may accept it. This is a protocol selection, not a feasibility claim.
Docker remains unsupported until all canonical blockers close, including locale-independent
Windows/WSL/Desktop identity, the complete proxy/backend/runtime epoch chain, dedicated Engine/VM and
exclusive-client authority, stable `/version`/`/info` projection, numeric resource policy, approved
image/executable bindings, container security and barrier recipes, lifecycle/emptiness and volume-
exclusivity proofs, fixture storage-version authority and raw-source retention. Events remain
advisory; their timestamps and the Engine ID are not a daemon epoch or durable cursor.

Native passage remains blocked on protected native-host authority, libc/ABI identity, an operations
device lease, redb-free write-attribution probe, complete thermal/kernel/block/NVMe error sources,
the exact error counter/bit registry, marker/barrier transports, raw-source retention, machine policy
and supported-host inventory. Therefore no target/preflight schema, collector, fixture cap, backend
dependency or candidate path was added.

## Six-pass cycle review

### 1. AI slop, evidence, and contract consistency

**Pass after three independent adversarial reviews and correction.** The first reviews rejected the
draft for substantive reasons: volatile Docker `/info` equality, ambiguous broker/socket authority,
an event-stream completeness implication, a normative rejected topology branch, missing Docker
lifecycle ambiguity, duplicated/missing native sources, incomplete cgroup ancestor and
`memory_localevents` handling, lossy cpufreq grouping, ambiguous I/O attribution and stale Cycle 27
language.

The final contract retains raw volatile Engine bodies but compares only a future closed stable
projection; makes events advisory; binds exact Docker request/response receipts; reserves blocked
`D20,D21` epoch/authority requirements; removes local-spawn frames from v1; binds the native cgroup
mount and opened ancestor chain; represents empty adverse CPU sets; preserves per-policy cpufreq
identity; gives topology and block counters one authority each; and requires both `wbytes` and
`wios` growth for the attribution probe. Final native, Docker and hygiene reviews all returned
commit-ready.

### 2. Overengineering, hot path, and latency

**Pass for a preflight control-path contract.** No generic collector/plugin system, errno taxonomy,
arbitrary sysfs snapshot, reusable backend layer or guessed cap was introduced. Fixed source IDs are
decision coordinates, not extension points. Blocked authorities stay blocked rather than receiving
placeholder adapters.

Nothing in this cycle enters LaminarDB's data path. Docker container startup is excluded only from
backend-latency evidence; it still counts against harness deadlines and reliability. A later backend
must still prove bounded service lanes, admission, batching and telemetry outside per-record hot
paths under open-loop p99/p99.9 maintenance interference.

### 3. Unused code and dependencies

**Pass.** The cycle is documentation-only. No Cargo manifest/lockfile, runtime feature, schema,
fixture, collector, Docker launcher, candidate profile, backend adapter or public API changed. There
is no unused implementation or dependency to carry.

### 4. Production readiness, delivery, exactly once, and independent soak

**NO-GO, correctly fail-closed.** No candidate transaction ran, no crash/cache-loss fault was
injected, and no endurance or independent production soak exists. No local state engine supplies the
missing vnode ownership, checkpoint seal/restore-before-activate, rebalance fencing, retention-safe
cleanup or connector delivery protocol by itself.

The first distributed-state release remains at-least-once. A later exactly-once claim still requires
an exact-certified replay-stable and assignment-fenced source, sealed state/timer/output checkpoint
cut, recoverable coordinator decision, and checkpoint-committable external sink transaction that
atomically consumes the predecessor cursor and is fenced by deployment, pipeline/sink namespace,
checkpoint attempt and live leader term, including ambiguous-commit recovery. Production readiness
also requires the separately run independent soak; this cycle supplies none of that evidence.

### 5. Documentation and research hygiene

**Pass.** The contract uses official Linux cgroup, block-stat, mountinfo and XFS geometry semantics
and official Docker Engine/API/security semantics. Vendor or local observations are not promoted to
authority. Cycle 28's inaccurate phrase implying an already reviewed Docker topology was corrected
to the actual topology-selection prerequisite.

No now-irrelevant RocksDB, Fjall, redb, SurrealKV or TidesDB decision record was found; each remains
dated decision history. No Claude-memory assertion was imported as evidence, and no redundant
parallel ADR/report was added.

### 6. Tests and empirical boundary

**The validation-only repository slice is green; no backend evidence was produced.**

- `cargo fmt --all -- --check`: pass.
- `cargo fmt --manifest-path tools/state-backend-qual/Cargo.toml -- --check`: pass.
- Isolated `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed, one explicitly ignored non-gating parser throughput/RSS observation.
- Staged `git diff --check`, registry cardinality checks and relative Markdown-link validation: pass.
- Read-only local inventory found Docker Engine 29.6.2/API 1.55, Docker Desktop 4.83.0 on a WSL2
  6.18.33.2 Linux VM with cgroup v2, overlayfs and `LiveRestoreEnabled=false`; WSL was 2.7.11.0.
  These values are development inventory, not reviewed policy or an epoch/exclusivity witness.
- No container, redb, TidesDB, RocksDB, Fjall, native-target or other candidate workload ran.
- Candidate-bearing CI was not invoked because current authority forbids candidate execution.

## Cycle 30 entry boundary

Continue validation-only authority and inventory closure:

1. select or explicitly reject a provider-backed dedicated native host and define the protected
   native-host/device-lease authorities before any native schema;
2. decide whether formal Docker smoke gets a dedicated Linux Engine/VM satisfying `D20,D21`, or
   remains development-only on Docker Desktop/WSL;
3. freeze every remaining canonical source/authority requirement, including libc, attribution,
   marker/barrier, thermal/error, Windows/Desktop identity, stable Engine projection, numeric
   resources, image/executable bindings, fixture-storage authority, container security and raw-source
   retention, plus the supported-host/source inventory;
4. derive source/list/string/document caps and hand-authored maximum/cap-plus-one fixtures only after
   that inventory; and
5. add no runtime backend, candidate dependency/execution, cluster-admission change, backend
   selection, production claim or soak claim.

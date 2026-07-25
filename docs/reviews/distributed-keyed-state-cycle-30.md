# Distributed keyed state Cycle 30 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Protocol commit:** `c77783f2`
- **Cycle outcome:** host-class/authority direction frozen; Docker and native providers unselected
- **Bounded memory:** reference/conformance-only; no current product or soak profile
- **Maintenance-health v2:** direction approved; candidate mapping remains gated
- **Current product target:** local spill; backend not selected
- **Candidate or backend execution authorized or performed:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**

## Outcome

Cycle 30 rejects Docker Desktop/WSL as a formal smoke producer. The documented Desktop surfaces do
not provide the `D03` Windows/Desktop/VM-to-Engine identity binding, `D20` complete daemon epoch and
backend/runtime process chain, or `D21` exclusive Engine-client authority required by the frozen
contract. Local Desktop/WSL remains development-only and cannot emit a formal smoke result or
satisfy the native prior-smoke prerequisite.

A dedicated native-Linux VM with a host-native protected broker is now the formal Docker successor
host-class direction. `container_per_process` remains the candidate-process topology. The exact
Engine/runtime and sole-client mechanisms are deliberately unselected: a private `dockerd` and
`containerd` takeover on a fresh VM is only a redb-free feasibility hypothesis. Any successor must
version or explicitly supersede the complete transitive Desktop/broker-container artifact, fact,
check, provenance and prior-smoke surface. GitHub-hosted standard `ubuntu-24.04` is the preferred
validation-only inventory subject, not an eligible provider or VM lease.

No native redb target exists. The unapproved `linux-nvme-v3` Fjall/RocksDB proposal cannot provide
redb policy, profile values, source caps or authority. AWS I4i Dedicated Host is only a plausible
inventory subject; no account, placement, host, image, package, instance or device is selected.
Provider selection now requires an operations-owned dedicated-host allocation, complete
`N01`--`N29` supported-host/source inventory, live `N20` device lease and single-use `N29`
collection/run authority. Existing profiles, WSL observations and validator test patterns were
classified for permissible reuse without importing any target value, source/document cap,
schema/status projection or authority.

No schema, fixture, collector, workflow, provider resource, runtime dependency, backend adapter,
candidate run, cluster-admission change or production claim was added.

## Six-pass cycle review

### 1. AI slop, evidence and contract consistency

**Pass after three independent adversarial reviews and multiple correction rounds.** Initial reviewers
rejected the draft for a narrow native inventory gate, incomplete `N29` lifecycle, private-daemon
mechanism overreach, impossible pre-target shim binding, undeclared status-like vocabulary,
inaccurate Cycle 25 classification, event-retention overclaim, incomplete source accounting,
unsupported validator reuse, non-transitive successor versioning, incomplete
`dockerd`-to-`containerd` proof, a fail-open sole-client claim and ambiguous GitHub image/resource
authority. The final review also caught wording that would have made normal reconciled candidate
exit unfinalizable.

The committed text requires all `N01`--`N29` dependencies, makes `N29` live and non-serializable,
treats private-daemon takeover as a hypothesis, binds runtime helpers only after they exist, keeps
normal reconciled exit legal, distinguishes `SO_PEERCRED` from sole-client proof, and leaves every
provider/mechanism blocker explicit. Native, Docker and hygiene reviewers returned commit-ready.

### 2. Overengineering, hot path and latency

**Pass for a control-plane decision.** This cycle adds no generic provider abstraction, Docker
launcher, collector framework, guessed helper population, target cap or reusable runtime layer. It
selects the minimum host-class direction needed to avoid an impossible Desktop contract and leaves
the private-daemon construction subject to a redb-free feasibility proof.

Nothing enters LaminarDB's record, batch, checkpoint or maintenance hot path. No latency number is
inferred from a VM, Docker volume, provider SKU or backend marketing result. A later backend still
must prove open-loop p99/p99.9 latency, bounded maintenance interference, memory admission, service
lane isolation and telemetry outside per-record critical sections on the selected production host.

### 3. Unused code and dependencies

**Pass.** The protocol slice is documentation-only. No Cargo manifest/lockfile, feature, runtime
API, schema, fixture, workflow, provider integration, Docker image, backend dependency or generated
artifact changed. It creates no unused implementation or dependency.

### 4. Production readiness, delivery, exactly once and independent soak

**NO-GO, correctly fail-closed.** No state engine was selected or executed; no target, fault,
recovery, endurance or independent soak evidence exists. `[LDB-4007]` and `[LDB-0013]` remain
unchanged. A local state engine alone still cannot supply vnode ownership, checkpoint seal,
restore-before-activate, rebalance fencing or retention-safe cleanup.

The first distributed-state release remains at-least-once. An exactly-once claim still requires a
replay-stable assignment-fenced source, one sealed state/timer/output checkpoint cut, a recoverable
coordinator decision, and a checkpoint-committable sink transaction fenced by deployment,
pipeline/sink namespace, checkpoint attempt and live leader term, including ambiguous-commit
recovery. Source and sink capability certification remains separate from backend selection. An
independent team/environment must run the production soak before any production-ready claim; this
cycle supplies no substitute.

### 5. Documentation and research hygiene

**Pass.** Current claims use official Docker Desktop/Engine, GitHub hosted-runner and AWS EC2
documentation. Vendor lifecycle and inventory facts are explicitly weaker than leases,
attestations, daemon epochs and sole-client evidence. The checked-in v1/v2 profiles remain immutable
regressions, v3 remains unapproved and Fjall/RocksDB-specific, and the WSL report remains dated
development history. No Claude-memory assertion was treated as evidence.

No existing research/report was removed: the backend audits, redb mechanism note, WSL capability
report, maintenance-health work and Fjall/RocksDB/redb/SurrealKV/TidesDB decision records remain
relevant dated decision history. Their ineligible values are not imported into the current target.
No redundant ADR or parallel authority report was added; the decision lives in the canonical
prescreen document.

### 6. Tests and empirical boundary

**The validation-only repository slice is green; no backend evidence was produced.**

- `cargo fmt --all -- --check`: pass.
- `cargo fmt --manifest-path tools/state-backend-qual/Cargo.toml -- --check`: pass.
- Isolated `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed, one explicitly ignored non-gating parser throughput/RSS observation.
- Staged `git diff --check`, 29-native/21-Docker source-registry cardinality checks and relative
  Markdown-link validation: pass.
- No Docker container, workflow, provider call, redb, TidesDB, RocksDB, Fjall, native target or other
  candidate workload ran.

## Cycle 31 entry boundary

Continue validation-only protocol closure without selecting or executing a backend:

1. freeze provider-neutral `N19` shared-marker and `N28` broker/barrier recipes, including exact
   ownership, state machines, ordering, deadlines, loss/restart behavior and cleanup, but no target
   values or executable implementation;
2. build the Docker successor compatibility map across every affected identity and define the
   redb-free feasibility-probe questions for Engine/runtime peer binding and sole-client enforcement;
3. keep GitHub-hosted `ubuntu-24.04` and AWS I4i as inventory subjects only until their distinct
   provider/authority contracts pass review;
4. do not derive source/document caps until supported-host inventories and raw-retention authority
   exist; and
5. add no runtime backend, candidate dependency/execution, cluster-admission change, backend
   selection, production claim or soak claim.

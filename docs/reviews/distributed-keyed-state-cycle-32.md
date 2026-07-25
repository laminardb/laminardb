# Distributed keyed state Cycle 32 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Protocol commit:** `7685a9ba`
- **Cycle outcome:** N19 publication ABI and dedicated-VM D20/D21 mechanism directions frozen
- **Bounded memory:** reference/conformance-only; no current product or soak profile
- **Maintenance-health v2:** direction approved; candidate mapping remains gated
- **Current product target:** local spill; backend not selected
- **Candidate, container, provider workflow or backend execution authorized or performed:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**

## Outcome

The [canonical redb prescreen](../testing/state-backend-redb-prescreen-v1.md) now rejects direct
Rust shared-memory publication as an interprocess correctness contract. The selected eligibility
direction is a separately assembled, non-inline Linux/x86-64 System V shim that owns every access to
the shared sequence and payload slots. The writer uses payload `movq` followed by one locked
compare-and-exchange; the read-only observer uses sequence `movq`, conditional `lfence`, then payload
`movq`. Exact source, object/final bytes, optimized LLVM IR, call sites, disassembly, toolchain,
CPU/microcode/kernel/hypervisor and live-host identities are mandatory. No Rust/C/C++ reference or
plain/atomic access may touch those slots.

That target-specific ABI still needs explicit owner acceptance and a separately authorized,
independently run, redb-free cross-process campaign on every admitted target class. Every iteration
uses a fresh never-reset memfd. One observation of published sequence with stale payload rejects the
target tuple; zero occurrences only corroborate the hardware/build argument. Miri, sanitizers and
same-process threads are not substitutes. If the ABI is rejected or another architecture is needed,
the whole N19 and crash-marker contract must be versioned around normative POSIX/kernel IPC; there
is no runtime fallback. The later crash frame now uses the same scalar shim slots while its complete
intent and timestamps remain supervisor-owned, closing the ordinary shared-payload gap.

For `D20,D21`, Cycle 32 conditionally selects a fresh dedicated native-Linux VM with one private
containerd/dockerd pair, exact mutable roots and an independently identified preloaded content
source. A retained BPF LSM/lifecycle ledger admits exact executables, records fork/exec/exit and
Unix-capability transitions, and poisons on any loss. Multi-call shim/runc gates and an inert
container PID 1 hold candidate release until dynamic bootstrap/OCI inputs, historical helpers and
the complete live runtime graph reconcile. Pre-hashing is explicitly insufficient unless the same
configuration/mount objects are protected through their official consumer.

The Engine listener is dockerd-created, private and pathname-only. A separate exact broker process
precreates a fixed raw-HTTP socket set, installs a TSYNC seccomp confinement filter before any
connect, and consumes an exact BPF generation/token budget. The final allowed connect atomically
seals the kernel state; only afterward may `sock_diag` reconcile pending/accepted sockets and the
supervisor unlink the pathname. Thus a racy snapshot/unlink pair is not authority. Descriptor
duplication, ancillary-message syscalls, fork/exec, new socket creation, `pidfd_getfd` capability
duplication, BPF and io_uring holder routes are denied; a single nonce-bound verifier exception for
a protected `pidfd_getfd` observation is consumed and revoked. The complete
dockerd/containerd/shim connection
graph, including planned lazy edges, remains continuously observed. Reconnect or any gap makes the
attempt unfinalizable.

This is a conditional GO only for a later redb-free, dummy-only feasibility probe. Exact kernel
hooks, Docker/containerd/shim/runc pins, provider lease, trusted privileged population, API/pool
plan, wire/cardinality/caps, schemas, goldens and hostile fixtures remain absent. Docker Desktop and
this workstation's WSL environment remain non-evidentiary development surfaces, not formal storage
or Engine-authority targets.

Approval payload `/v2` remains compatible under a strict placement rule. Broker/helper/gate and
assembly/BPF code are modes or embedded objects of the already listed supervisor binary and are
closed by existing source/lock/SBOM/build rows. Target policy owns expected VM, kernel/BTF/module/JIT,
Docker/runtime/ELF/DSO and OCI content identities. Execution plan and candidate configuration own
only static bytes, templates and predicates. All runtime FD/PID/mount/cgroup/socket/bootstrap/OCI
values belong to versioned live evidence. Any new standalone approval-input object requires payload
`/v3`; old Desktop target/result bytes remain rejected. The pre-run receipt `/v2`, protected-review
schema `/v1` and protocol ID `/v1` otherwise retain their exact meanings. GitHub protected-review
identity is not confused with the separate compute-provider lease.

No schema, fixture, workflow, provider resource, runtime dependency, backend adapter, candidate
run, cluster-admission change or production claim was added.

## Six-pass cycle review

### 1. AI slop, evidence and contract consistency

**Pass after three independent adversarial reviews and correction rounds.** N19 review initially
found contradictory zero initialization, object reuse in the litmus, shared multi-byte crash intent,
stale Rust fence language, overbroad relocation checks and incomplete VM/placement identity. The
final text uses ftruncate zero extension without userspace slot writes, a fresh object per iteration,
supervisor-owned intent/timestamps, exact shim observations and a complete target tuple.

Docker review rejected an earlier socket-activation direction, snapshot/unlink authority, late
seccomp, one sampled containerd connection, a blanket file-receive rule that blocked its own
verifier, static/dynamic identity conflation, preloaded-root contradiction, ambiguous network/reload
claims, containerd legacy fallback and pre-hash TOCTOU. The final text uses a dockerd-created
listener, pre-connect TSYNC confinement, atomic BPF admission seal, correct `UDIAG_SHOW_*` request
versus `UNIX_DIAG_*` response vocabulary, complete runtime connection graph, one-shot exception,
static template/live evidence separation, explicit mutable/preloaded roots, exact API/egress/signal
controls and consumer-bound dynamic inputs. N19, Docker and compatibility/hygiene reviewers all
returned commit-ready.

### 2. Overengineering, hot path and latency

**Pass for a validation control-plane design; implementation remains gated.** The assembly shim is
used only by preflight/crash markers. BPF, seccomp, runtime gates and broker controls operate on
process, exec, socket and lifecycle events, not LaminarDB records, batches, timers, joins or state
lookups. The design selects one conjunctive authority instead of implementing ptrace, fanotify,
seccomp user notification or a generic container supervisor. Its complexity follows from proving
historical short-lived runtimes and sole Engine capability rather than from a reusable framework.

This does not make cost free. A later dummy-only probe must measure daemon/gate startup, recovery,
observer CPU/memory/ring pressure and broker API latency on the exact target. Backend qualification
still requires open-loop p99/p99.9 tails, hot-key/Zipf pressure, maintenance interference, service-
lane isolation and telemetry outside per-record critical sections. No vendor benchmark or control-
plane latency is treated as state-backend evidence.

### 3. Unused code and dependencies

**Pass.** Only the canonical protocol document changed before this review. No Cargo manifest or
lockfile, feature, runtime API, schema, fixture, workflow, provider integration, Docker image,
assembly/BPF object, gate, backend dependency or generated artifact changed. There is no unused
implementation or dependency.

### 4. Production readiness, delivery, exactly once and independent soak

**NO-GO, correctly fail-closed.** No state engine, provider, exact Docker/runtime tuple or target was
selected or executed; no target, fault, recovery, endurance or independent-soak evidence exists.
`[LDB-4007]` and `[LDB-0013]` remain unchanged. A local state engine alone still cannot supply vnode
ownership, checkpoint seal, restore-before-activate, rebalance fencing or retention-safe cleanup.

The first distributed-state release remains at-least-once. Exactly once still requires a replay-
stable assignment-fenced source, one sealed state/timer/output checkpoint cut, a recoverable
coordinator decision and a checkpoint-committable sink transaction fenced by deployment,
pipeline/sink namespace, checkpoint attempt and live leader term, including ambiguous-commit
recovery. Source and sink capability certification remains separate from backend selection. An
independent team/environment must run the full production soak before any production-ready claim;
the N19 mechanism campaign and later D20/D21 dummy probe do not replace it.

### 5. Documentation and research hygiene

**Pass.** The large edit replaces ambiguous direct-Rust and Docker-snapshot prose in one canonical
protocol rather than adding a parallel ADR or speculative implementation. Primary/versioned Rust,
LLVM, POSIX, Intel/AMD, Linux, Docker and containerd sources support the mechanism boundaries. Gaps
are recorded as blockers rather than filled with marketing or Claude-memory claims.

No research document was removed. The dated WSL capability boundary, backend audits, redb mechanism
note, maintenance-health work and Fjall/RocksDB/redb/SurrealKV/TidesDB decision records remain
relevant decision history; none supplies current target or production evidence. Bounded memory
remains reference-only and maintenance-health v2 remains an approved direction, not implemented
backend behavior.

### 6. Tests and empirical boundary

**The validation-only repository slice is green; no backend evidence was produced.**

- `cargo fmt --all -- --check`: pass.
- `cargo fmt --manifest-path tools/state-backend-qual/Cargo.toml -- --check`: pass.
- Isolated `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed, one explicitly ignored non-gating parser throughput/RSS observation.
- `git diff --check`, exact `N01`--`N29` and `D01`--`D21` registry checks, relative Markdown-link
  validation and 21 added primary/versioned external-reference resolutions: pass.
- No Docker daemon/container, WSL workload, workflow/provider call, redb, TidesDB, RocksDB, Fjall,
  native target, backend candidate, mechanism campaign or soak ran.

## Cycle 33 entry boundary

Continue validation-only protocol closure without selecting or executing a backend:

1. freeze the trusted privileged-actor/threat boundary and compare provider lease, attestation,
   renewal, preemption and fencing APIs for a fresh dedicated Linux VM without provisioning one;
2. source-audit and propose one exact Docker/containerd/shim/runc compatibility tuple plus the
   closed Engine API/fixed-connection plan; no binary download, daemon start or container run;
3. freeze the redb-free BPF LSM/lifecycle event, map/poison, atomic-connect-seal, shim/runc gate,
   dynamic-input TOCTOU and complete runtime-connection predicates and hostile fixtures;
4. prepare the explicit owner decision packet for the Linux/x86-64 N19 ABI versus a versioned
   POSIX/kernel-IPC redesign, without implementing or running either;
5. derive raw roles/cardinalities and caps only after those populations close; do not start a
   successor target/preflight schema prematurely; and
6. preserve all bounded-memory, maintenance-health, source/sink, exactly-once, hot-path, latency and
   independent-soak gates, with no runtime backend, candidate dependency/execution, cluster-
   admission change, backend selection or production claim.

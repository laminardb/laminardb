# Distributed keyed state Cycle 31 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Protocol commit:** `78f2f6ea`
- **Cycle outcome:** provider-neutral `N19`/`N28` semantics and Docker-successor compatibility frozen
- **Bounded memory:** reference/conformance-only; no current product or soak profile
- **Maintenance-health v2:** direction approved; candidate mapping remains gated
- **Current product target:** local spill; backend not selected
- **Candidate or backend execution authorized or performed:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**

## Outcome

`N19` now has one representative shared-marker semantic recipe: a fresh non-executable, fixed-size
and sealed `memfd`, one atomic sequence plus one plain publication payload, a single reviewed helper,
read-only observer mapping, exact live/post-exit observation, and bounded process/socket/cgroup/object
cleanup. The decision boundary is deliberately wider than atomic instruction support. Owners must
approve the exact target-qualified process-shared publication/memory-model ABI, including ordinary
payload happens-before, for the pinned Rust target/build; otherwise the marker and later crash frame
need an audited-shim or kernel-IPC redesign. The recipe remains unimplemented and cannot currently
satisfy preflight.

`N28` now has a one-shot anonymous `AF_UNIX`/`SOCK_SEQPACKET` barrier recipe between the protected
outer runner, collector and inert helper. It binds exact process and endpoint provenance, a RAW total
deadline beginning before collector spawn, an isolated preflight-probe cgroup outside the campaign
parent, armed-without-release state, planned pidfd `SIGKILL`, parent-thread death handling, explicit
stop/guard-release/final receipts, and complete descriptor/socket/task/cgroup cleanup. Success
requires the probe parent's enumerated descendant count and zero dying descendants after removal.
The probe runs before final quiet observations, so its own activity cannot make them stale. It too
remains unimplemented and pass-incapable.

Every Docker Desktop-era protocol surface now has an explicit conditional/version/replace/reject
action for a host-native dedicated-Linux-VM successor. The `D20,D21` matrix requires a complete
private Engine/runtime chain, lossless process history, inert-until-joined candidate bootstrap,
exclusive VM authority, a no-new-client barrier or continuous loss-detecting observation, and
continuous privileged-holder closure. These are validation questions, not a selected mechanism.
No historical Desktop byte can be promoted by relabelling its producer.

No schema, collector, fixture, workflow, provider resource, runtime dependency, backend adapter,
candidate run, cluster-admission change or production claim was added.

## Six-pass cycle review

### 1. AI slop, evidence and contract consistency

**Pass after three independent adversarial reviews and repeated correction rounds.** The first
specialist drafts were not commit-ready. Review found a read-only mapping acknowledgement write,
non-neutral initial marker handling, a marker that failed to test payload publication, incomplete
reserved-byte checks, executable-memfd ambiguity, uncovered setup deadlines, an undeclared N19
runner channel, parent-thread-specific death-signal gaps, incomplete N28 broker-loss/task/socket
closure, overclaimed credentials, impossible pre-creation leaf identity, incomplete retained
transcripts, stale post-quiet N28 activity, missing helper-pidfd/cgroup-handle closure, dying-cgroup
ABA, and a pre-handoff broker-loss window.

The committed text closes each issue explicitly. It also prevents an atomicity-only approval from
satisfying the full process-shared publication contract and changes Docker runtime supervision from
an impossible pre-effect observation to approval-bound inputs, lossless creation/exec history and an
inert candidate release barrier. N19, N28/Docker and hygiene reviewers independently returned
commit-ready.

### 2. Overengineering, hot path and latency

**Pass for a validation control-plane contract.** The detailed state machines exist because process,
descriptor, cgroup and deadline ownership must be loss-complete; they add no generic runtime
framework. Nothing enters LaminarDB's record, batch, checkpoint, timer, join, maintenance or storage
hot path. No candidate/backend latency is inferred from protocol mechanics or vendor claims.

A later backend decision still requires open-loop p99/p99.9 latency, bounded maintenance
interference, memory admission, service-lane isolation and telemetry outside per-record critical
sections on the selected production host.

### 3. Unused code and dependencies

**Pass.** Only the canonical prescreen document changed. No Cargo manifest/lockfile, feature, runtime
API, schema, fixture, workflow, provider integration, Docker image, backend dependency or generated
artifact changed. There is no unused implementation or dependency.

### 4. Production readiness, delivery, exactly once and independent soak

**NO-GO, correctly fail-closed.** No state engine was selected or executed; no target, fault,
recovery, endurance or independent soak evidence exists. `[LDB-4007]` and `[LDB-0013]` remain
unchanged. A local state engine alone still cannot supply vnode ownership, checkpoint seal,
restore-before-activate, rebalance fencing or retention-safe cleanup.

The first distributed-state release remains at-least-once. Exactly once still requires a replay-
stable assignment-fenced source, one sealed state/timer/output checkpoint cut, a recoverable
coordinator decision, and a checkpoint-committable sink transaction fenced by deployment,
pipeline/sink namespace, checkpoint attempt and live leader term, including ambiguous-commit
recovery. Source and sink capability certification remains separate from backend selection. An
independent team/environment must run the production soak before any production-ready claim.

The socket recipes distinguish semantic resend/replay from bounded continuation of a syscall that
has not committed a packet. Unknown send state, duplicate delivery, authority loss or incomplete
cleanup cannot produce a singleton or pass.

### 5. Documentation and research hygiene

**Pass.** The 409 added lines are dense but required to freeze ownership, state, loss and cleanup
semantics plus the transitive Docker compatibility boundary; the independent hygiene review found no
redundant parallel ADR. Linux and Rust claims use relevant versioned or primary documentation, and
the Rust documentation's missing interprocess guarantee is treated as a blocker rather than filled
by inference. No Claude-memory assertion or backend marketing claim was imported as evidence.

No existing research/report was removed. The backend audits, redb mechanism note, WSL capability
report, maintenance-health work and Fjall/RocksDB/redb/SurrealKV/TidesDB decision records remain
relevant dated decision history. Their ineligible values are not imported into a current target.

### 6. Tests and empirical boundary

**The validation-only repository slice is green; no backend evidence was produced.**

- `cargo fmt --all -- --check`: pass.
- `cargo fmt --manifest-path tools/state-backend-qual/Cargo.toml -- --check`: pass.
- Isolated `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed, one explicitly ignored non-gating parser throughput/RSS observation.
- Staged `git diff --check`, exact `N01`--`N29` and `D01`--`D21` registry checks, relative Markdown-
  link validation and eight added external-reference resolutions: pass.
- No Docker container, workflow, provider call, redb, TidesDB, RocksDB, Fjall, native target or other
  candidate workload ran.

## Cycle 32 entry boundary

Continue validation-only protocol closure without selecting or executing a backend:

1. define the acceptance-evidence decision for the target-qualified N19 publication/memory-model
   ABI, including the exact compiler/build/codegen audit and the limits of cross-process litmus
   corroboration; do not treat a litmus as a language-level proof;
2. narrow the redb-free `D20,D21` feasibility mechanisms for a dedicated Linux VM, including exact
   daemon/runtime supervision, inert candidate release and Engine-client capability closure, without
   starting Docker or a candidate;
3. decide where immutable host broker, `dockerd`, `containerd`, configuration, shim/runtime and
   provider identities belong, then determine whether approval payload/protocol `/v2` is compatible
   or must be versioned;
4. preserve all source/sink, exactly-once, latency, hot-path, bounded-memory and independent-soak
   gates; and
5. add no runtime backend, candidate dependency/execution, cluster-admission change, backend
   selection, production claim or soak claim.

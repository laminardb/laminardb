# Distributed keyed state — Core Cycle 2 review

- **Date:** 2026-07-27
- **Scope:** fail-closed containment of the existing vnode transition path across assignment
  adoption, boot restore, graph callbacks, transport authority, and control-only completion
- **Code commits:** `cb2cd4c8`, `966ff725`, `9cbdd79a`
- **Slice verdict:** **APPROVE WITH OWNED FOLLOW-UPS**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Implemented boundary

Assignment adoption stages revoke and rehydration work while holding the rotation write fence.
Boot staging validates the exact current assignment, checkpoint attempt, and owned vnode roster;
when an installed source handoff exists, its attempt and assignment version must also match. A
no-checkpoint fresh start rejects committed or staged vnode state. The watcher preserves predecessor
authority across a not-ready successor and can reconstruct the exact predecessor certificate from
durable assignment history.

At the start of a graph cycle, the transition path pins assignment and transport authority,
validates the exact locally owned/restoring vnode roster, decodes and resolves every recovery chain
before callbacks, and executes revoke and restore callbacks in deterministic order. It activates
owned target vnodes and consumes staging only after all callbacks and post-callback authority
revalidation succeed. Unowned stale restore input may be discarded during collection. A callback
failure has an indeterminate live result, retains owned target staging and pending revokes, and
poisons that graph generation. A fenced coordinator can run this control transition without
admitting source rows.

This is containment around the existing runtime path, not the Phase 1 lifecycle and not operator
admission. The private Core Cycle 1 managed-state reference is still not a runtime consumer.

## Review plan and result

- **AI slop — pass for the touched slice:** transition phases and data carriers have one runtime
  owner; claims distinguish local vnode-roster validation from an authoritative operator/table
  roster and containment from exactly-once operation.
- **Overengineering/hot path — pass with measurement debt:** no backend adapter, generic state
  trait, or speculative inline annotation was added. An O(1) atomic restoring count avoids a vnode
  scan; the empty cluster cycle still takes two staging mutexes and performs no allocation. These
  costs must be benchmarked before admission.
- **Unused code and naming — pass for the touched slice:** the unused permissive
  `GraphOperator::apply_vnode_state` hook and a redundant test helper were removed. The transition
  moved from the already broad graph module into one cohesive cluster-only module; lifecycle,
  recovery, buffering, and test-probe names now describe their actual role. Per-vnode success logs
  are `debug`, with one batch-level `info` event.
- **Production readiness — block:** the transition record lacks complete durable identity and an
  authoritative operator/state-table roster; callbacks are not prepare/publish/abort; the SQL
  operator's `QueryState::Uninit` path has no atomic semantic install boundary; decode has no
  bounded-memory/pause protocol; final-vnode-loss revoke cleanup lacks valid authority;
  source/state/sink exactly-once composition is unproven; TidesDB is selected but not qualified or
  integrated; tail latency and independent release-binary soak evidence do not exist.
- **Documentation — pass:** ADR, current plan, execution plan, and validation report state the new
  boundary and retain **NO-GO**. No backend or certification claim was inferred from this slice.
- **Tests — pass:** formatting, diff checks, both warnings-denied feature matrices, focused vnode,
  graph, boot, watcher, adoption, recovery-suspension, and control-only suites pass on Windows.

## Verification record

| Command or suite | Result |
|---|---:|
| `laminar-core state::vnode::tests` | 29 passed |
| `operator_graph::tests` with cluster | 104 passed |
| boot vnode recovery | 8 passed |
| watcher predecessor-cache/authority | 2 passed |
| DB unapplied-transition gates | 2 passed |
| lost-after-preflight stale restore cleanup; pending revoke retained | 1 passed |
| recovery suspension | 1 passed |
| coordinator control-only completion | 1 passed |
| `cargo check`, no default features | passed |
| `cargo check`, cluster | passed |
| Clippy `-D warnings`, no default features | passed |
| Clippy `-D warnings`, cluster | passed |
| `cargo fmt --all -- --check` and `git diff --check` | passed |

## Maintainability cleanup gate

The vnode-transition module extraction is complete for this cycle. The roughly 3,500-line parent
graph module and 5,450-line graph test module remain too large. Transition tests are interleaved
with execution and checkpoint fixtures, so moving them mechanically now would create a noisy,
high-risk diff. Continue splitting both by runtime and fixture ownership when those areas change.

**DKS-CLEANUP-001** in the current implementation plan assigns the final gate to the distributed
keyed-state feature maintainer. After the core lifecycle and backend implementation are complete,
and before stateful admission or production qualification, run one bounded cleanup cycle that:

1. removes or explicitly product-owns paused soak/certification-only runtime surfaces, including
   kill gates, trigger-file polling, raw evidence endpoints/ledgers, and cluster-soak helpers;
2. removes obsolete backend candidate jobs and tools, then consolidates historical narrative from
   the oversized ADR/plans after preserving current decisions and relying on Git for chronology;
3. wires or deletes the release-dead `artifact_v1`, `managed_v1`, and `vnode_partial/v2` modules
   under **DKS-P1-001**; and
4. reruns dead-code search, both feature matrices, Clippy, focused fault tests, and an independent
   human review for naming, file ownership, latency hazards, and operator-facing error semantics.

This cleanup may delete code; it must not silently move test hooks behind a default production
feature or change admission behavior.

## Next core slice

Define one complete transition identity bound to predecessor/target assignment certificates,
checkpoint attempt, artifact identity, and the authoritative operator/state-table roster. Add
operator prepare/publish/abort tokens and a real SQL semantic install boundary, then make local
revoke cleanup valid when this node is absent from the target owner set. Bound restore decode bytes
and pause time as part of that design. Keep backend integration and `[LDB-4007]` changes out of the
slice.

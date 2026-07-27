# Distributed keyed state — Core Cycle 3 review

- **Date:** 2026-07-27
- **Scope:** authorize graceful cleanup when a live process with the committed predecessor boot
  incarnation loses its final vnode, without inventing target ownership or weakening recovery and
  adoption fences
- **Code commit:** `c5cb47db`
- **Slice verdict:** **APPROVE WITH OWNED FOLLOW-UPS**
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

## Implemented boundary

An audited, durably committed assignment drain may now mint an opaque final-owner-exit capability.
It binds the predecessor and target assignment versions and owner-map digests, the local process
incarnation, predecessor membership, target absence, and the exact set of locally owned vnodes lost
to the target. Recovery decisions, direct/no-store adoption, abort, skipped versions, and unaudited
transitions cannot mint it and fail before intake, transport, registry, lifecycle, or staging
mutation.

The graph accepts that capability only on its control-only completion path while the rotation
execution fence is held, both local transport endpoints are inactive, the target registry and
certificate still match exactly, and neither staged restore nor a `Restoring` lifecycle exists.
Callbacks run deterministically. Callback failure or post-callback drift in assignment, transport,
restore staging, lifecycle, or capability poisons the graph and retains the staged revoke. Success
alone consumes it. Normal execution and checkpoint drain retain their explicit final-exit guard.

The multi-node Windows test reproduced a cluster compute-worker stack overflow under the platform
default. Cluster compute threads now use an explicit 4 MiB stack, matching the existing cluster I/O
workers. That is a correctness fix for the reproduced environment, not evidence that one 4 MiB
stack per running pipeline meets production RSS or latency limits.

This slice does not consume the managed-state reference, add TidesDB, install state into the SQL
operator, introduce prepare/publish/abort, change delivery guarantees, or admit any keyed, window,
join, or materialized-view shape.

## End-of-cycle review

- **AI slop — pass after adversarial review:** two independent correctness reviews found an
  unsealed capability, missing exact predecessor-version binding, missing post-callback restore
  revalidation, and optional destructive-path fencing. The implementation and regressions now
  close each issue. Claims remain limited to graceful committed final-owner cleanup.
- **Overengineering and hot path — pass with measurement debt:** the capability is a private
  control-plane value rather than a backend-neutral framework. A redundant staging mutex on every
  normal cycle was removed. Owner-map scans and clones occur only during assignment transition.
  Empty-cycle locking, the 4 MiB per-pipeline stack, and per-batch shuffle codec construction remain
  explicit benchmark/resource work.
- **Unused code and naming — pass for the touched slice, repository gate open:**
  `pending_revoke_vnodes` is now `staged_vnode_revocation`, the revoke callback setter describes
  vnode revocation, and the final helper names committed authority. Production sibling modules can
  no longer construct staging batches. One invalid concurrency test was removed because its lock
  blocked startup before the phase it claimed to observe; a future race test needs a deterministic
  scheduler or owned seam.
- **Production readiness — block:** general restore/partial-revoke records still lack one complete
  transition identity and authoritative operator/table roster; callbacks lack
  prepare/publish/abort and SQL semantic installation; restore decode lacks byte and pause bounds;
  source/operator/sink exactly-once composition is unproved; no backend is qualified or integrated;
  tail-latency/RSS evidence and the independent release soak do not exist.
- **Documentation — pass:** ADR, phased plan, Phase 0 execution plan, and validation report record
  the exact boundary, stack finding, cleanup gate, and unchanged **NO-GO**.
- **Tests — pass for the affected matrices:** focused unit and multi-node integration coverage,
  both compile/lint feature matrices, formatting, and diff checks pass. The full DB library suite
  was not rerun, so this review does not claim it.

## Verification record

| Command or suite | Result |
|---|---:|
| `operator_graph::tests` with `cluster` | 111 passed |
| `final_owner_exit` focused filter | 8 passed |
| exact version/capability constructor regression | 1 passed |
| durable recovery-authority rejection | 1 passed |
| assignment-adoption staging race | 1 passed |
| partial lost-vnode restoring cleanup | 1 passed |
| recovery flag cannot discard staged work | 1 passed |
| startup restore reset | 1 passed |
| `snapshot_watcher_handles_draining_phase` multi-node integration | 1 passed |
| `cargo check -p laminar-db --no-default-features` | passed |
| `cargo check -p laminar-db --features cluster` | passed |
| Clippy `-D warnings`, both feature matrices | passed |
| `cargo fmt --all -- --check` and `git diff --check` | passed |

The integration test proves the stable zero-vnode result: no additional source polling, intake
still fenced, inactive transport endpoints, absence from the committed target certificate, and no
later authority after the leader opens intake. Public hooks cannot deterministically observe the
transient nonzero staging count; callback unit tests supply the direct execution evidence. This is
not an independent soak.

## Maintainability follow-up

The normative ordered inventory and exit checks live only in
[DKS-CLEANUP-001](../plans/distributed-keyed-stateful-operators.md#core-workstream-reset), avoiding a
second policy copy in cycle history. For this slice, misleading revoke names and the forgeable
batch API were corrected. The DB façade, DB tests, and graph tests remain oversized; split their
assignment and transition ownership after lifecycle semantics stabilize. The independent inventory
also confirms that parked backend/observer scaffolding and release-dead state models need deletion
or explicit product ownership before admission. Git, rather than maintained runtime code or repeated
cycle prose, preserves discarded experiments.

## Next core slice

Define one complete identity for acquire and partial-revoke work, including assignment attempt,
predecessor/target certificates, pipeline and artifact identity, and an authoritative
operator/state-table roster. Use it to introduce prepare/publish/abort shadows and the SQL semantic
install boundary, with bounded restore bytes and pause time. Keep backend construction, admission
changes, and paused soak/certification tooling outside that slice.

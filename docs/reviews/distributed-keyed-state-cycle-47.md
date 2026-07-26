# Distributed keyed state Cycle 47 review

- **Date:** 2026-07-25
- **Scope:** checkpoint-drain output-publication cancellation ownership
- **Cycle outcome:** the production drain remains non-cancellable as one owner transaction; no
  checkpoint-only runtime fence was added
- **Admission/backend outcome:** unchanged; no TidesDB dependency, runtime backend, or cluster
  capability was added
- **Production verdict:** **NO-GO** pending managed vnode ownership, delivery, backend, fault, and
  independent-soak gates

## Result and exact boundary

One completed checkpoint-drain graph pass does not finish the pass transaction. The callback still
has to publish materialized-view output, stream subscriptions, and sink commands before it can
record the pass and continue. Sink filter compilation and bounded sink-actor admission are async.
The graph's Cycle 46 guard has already disarmed when these awaits run because graph input/state and
the returned batches are internally consistent at that point.

A transient deterministic red probe exercised this exact boundary through the real callback and
sink actor:

1. a test output node began with three one-row graph-owned batches;
2. a checkpoint-drain pass consumed all three and returned them for publication;
3. the first sink command entered a connector blocked by a semaphore;
4. the second command filled the actor's one-slot queue;
5. the third `write_batch_until` remained pending, proved by polling the drain future; and
6. dropping that borrowed future left the retained callback without a pipeline fault.

The red assertion failed in 0.02 seconds at
`pipeline_callback::tests::cancelled_checkpoint_drain_sink_publication_is_a_sticky_pipeline_fault`.
The probe was then removed: retaining an executable test that blesses unsupported private-API use
would require a test-only graph injection seam while enforcing no production behavior. It proved
graph-owned input consumption and partial sink-command transfer, not stateful-operator mutation,
source-offset publication, durable checkpoint capture, replay, or exactly-once.

The code audit found no retained-owner production cancellation:

| Path | Ownership behavior | Disposition |
|---|---|---|
| leader aligned checkpoint | directly awaits the complete callback drain | explicit result |
| source-less checkpoint | directly awaits the complete callback drain | explicit result |
| follower checkpoint capture | directly awaits the complete callback drain | explicit result |
| checkpoint deadline or process-lease loss in replay-required/cluster sink publication | nested future is cancelled, callback regains control | recorded sink/checkpoint fault and explicit `Recovery` |
| local best-effort sink-enqueue deadline | nested future is cancelled, callback regains control | recorded `sink_timed_out`; later fence blocks capture: `Skipped` after successful FIFO sync, otherwise `Failed`; overall drain-deadline expiry is `Recovery` |
| compute-root panic or root task destruction | callback and graph generation are destroyed | supervisor rebuild/restore |
| private borrowed drain future dropped while callback is retained | no supported owner cleanup | unsupported; reproduced above |

Sink enqueue acknowledges actor admission, not external durability. Before capture, the FIFO sync
waits for every accepted command to finish. Source positions are kept outside the durable manifest
through graph drain, that sink fence, and operator-state capture. The durable tail later performs
the contract-required phase-one flush or pre-commit before persisting the cut; checkpoint-
committable sinks finalize only after the durable decision. A recorded or returned sink-publication
disposition therefore cannot seal a newer source cut. Under at-least-once recovery, the first two
accepted batches in the red choreography may duplicate; no exactly-once claim is made.

## Decision

No checkpoint-drain-only poison was added. It would cover neither the equivalent hypothetical
normal-cycle MV/stream/sink publication drop nor coordinator-owned source-barrier and exact-attempt
cleanup. Extending graph poison across external delivery would also conflate an internally
consistent graph with a larger input-cut/publication transaction without giving the graph authority
to resolve that transaction.

`PipelineCallback::drain_checkpoint_edges_until` now states the actual owner contract: await its
whole future to an explicit result, or destroy the complete callback/coordinator generation. Nested
deadline and process-lease cancellation remains supported only because the callback catches it and
records or returns a disposition that prevents capture. Publication in replay-required and cluster
modes returns `CycleError::Recovery`; local best-effort enqueue loss records a timeout and later
reaches `Skipped` only after successful FIFO sync, otherwise `Failed`; overall drain-deadline expiry
returns recovery. Before any future outer timeout, `select!`, or task abort is introduced, its owner
must provide one attempt transaction covering the frozen input cut, graph/MV/stream/sink
publication, source-barrier ownership, offsets, and exact-attempt cleanup.

This is separate from a native working-state request that may complete after its Rust caller is
gone. Such a backend still needs a backend-owned generation, retired root/directory, late-completion
fence, and portable fresh restore. Cluster best-effort and exactly-once remain rejected; the current
admitted cluster delivery guarantee is at-least-once only.

## AI slop review

**Pass.** The cycle began with a concrete red reproduction, then removed its 177-line fixture and
test-only graph seam after production reachability and ownership were audited. It adds no generic
transaction type, second poison bit, timeout, task, rollback mechanism, backend trait, or TidesDB
surface. The unsupported borrowed call and supported production paths are reported separately.

## Overengineering and hot-path review

**Pass.** A per-drain-pass `Arc` guard, whole-drain guard, sink/batch guard, and checkpoint-fault
guard were rejected. The first three cover the wrong owner scope; the last is consumable and does
not resolve callback reuse or attempt cleanup. The committed change is Rust documentation plus
design/validation records, so normal cycles, rows, operators, checkpoints, and sinks gain no branch,
atomic, clone, allocation, task, lock, or I/O.

## Unused-code review

**Pass.** The temporary output-node helper, gated connector, and failing regression were removed.
No production or test symbol was added.

## Production-readiness review

**NO-GO.** Current owners await the drain without retained-owner cancellation, and supported inner
interruptions record or return a disposition that blocks capture. This cycle does not make a future
cancellable publication path safe, eliminate at-least-once duplicates, provide transactional sink
commit, qualify a backend, add byte-bounded managed state, publish vnode acquire/revoke atomically,
relax `[LDB-4007]`/`[LDB-0013]`, or run the independent multi-process soak.

## Documentation review

**Pass.** The callback contract, ADR-008, phased plan, Phase 0 execution order, and validation report
now distinguish graph-cycle poison, callback publication ownership, classified nested cancellation,
best-effort checkpoint skip, coordinator attempt ownership, and future native-root poison.
Historical backend research remains
gate/reference evidence. No research document became false or irrelevant, so none was removed.

## Test review

**Pass for the documentation-only disposition.** The transient red probe deterministically proved
the private retained-owner ambiguity before it was removed. Final validation results are:

- transient red probe: one expected failure at the absent pipeline fault after the third sink
  enqueue was proved pending;
- `cargo test -p laminar-db --lib`: 1,290 passed and one profiling test ignored;
- `cargo test -p laminar-db --lib --all-features`: 1,774 passed and two explicit tests ignored;
- `cargo clippy -p laminar-db --lib --all-features -- -D warnings`: pass; and
- formatting, diff, local-link, and dependency hygiene: pass.

Three independent read-only reviews pass after correcting the BestEffort disposition and sink-
ordering descriptions. This remains local regression evidence, not an independent product soak.

## Cycle 48 review plan

Determine whether the already-proven sequential vnode-transition partial-apply boundary is reachable
through the currently admitted vnode-0 global aggregate path, and close only the live safety gap:

1. **AI slop:** reuse the Cycle 3 partial-apply regression; add a new probe only if needed to prove
   admitted global-path reachability, not to rediscover the generic loop;
2. **Overengineering/hot path:** keep any transition certification/fence on assignment rotation and
   reuse the existing rotation fence and recovery mapping;
3. **Unused code:** require any new transition status to block execution and checkpoint capture and
   to be observed by the current coordinator/supervisor;
4. **Production readiness:** trace assignment authority, source handoff, shuffle alignment, vnode-0
   revoke/restore-before-activate, output publication, and last-committed-cut recovery as one order;
5. **Documentation:** distinguish a correction required by today's admitted global aggregate from
   the future authoritative-roster/shadow-publish design for keyed, window, join, and MV state; and
6. **Tests:** cover the admitted multi-operator/global shape if reachable, explicit partial failure,
   checkpoint exclusion, fresh-cut recovery, unchanged successful rotation, and zero row-hot-path
   cost.

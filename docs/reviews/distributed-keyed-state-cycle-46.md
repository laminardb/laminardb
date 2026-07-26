# Distributed keyed state Cycle 46 review

- **Date:** 2026-07-25
- **Scope:** operator-graph cancellation/panic ownership and checkpoint exclusion
- **Cycle outcome:** a dropped or unwinding graph cycle permanently fences that in-memory graph
  generation; fresh restore from the last committed cut is required
- **Admission/backend outcome:** unchanged; no TidesDB dependency, runtime backend, or cluster
  capability was added
- **Production verdict:** **NO-GO** pending managed vnode ownership, delivery, backend, fault, and
  independent-soak gates

## Result and boundary

The audit distinguished three paths that earlier reviews grouped together:

| Attempt outcome | Current owner behavior | Safe continuation |
|---|---|---|
| explicit `Ok`/`Err` | graph cleanup/classification completes; recovery and halt errors keep their existing disposition | follow that returned disposition |
| panic in the production compute root | unwind destroys the callback and graph before the outer lifecycle reports `Faulted` | rebuild and restore through the supervisor |
| drop/caught panic of a borrowed graph future | before this cycle, future-local input/results were lost while mutated operator state remained reusable | now poison that graph generation and require fresh restore |

Production does not currently cancel and retain an in-flight graph pass. Normal execution and
shutdown drain await `callback.execute_cycle` directly. Checkpoint deadlines are checked between
whole passes, and the callback contract explicitly forbids cancelling an in-progress graph pass.
An operator that never becomes ready can therefore stall compute/checkpoint/shutdown; adding a
timeout at this await is not a safe liveness fix.

The borrowed API ambiguity was real. `execute_single_operator` removes graph input buffers before
awaiting the operator. The red regression checkpointed an ASOF right-side cut, submitted a newer
quote and trade, waited until the real ASOF output reached a downstream pending operator, and
dropped the pinned cycle. The same graph then accepted a checkpoint. Catching a downstream panic
after the same stateful output had the same result.

`OperatorGraph` now owns a sticky `Arc<AtomicBool>`. One RAII guard per graph cycle arms after the
cluster rotation read fence is acquired but before input/state admission. Every explicit result
disarms it; cancellation or unwind performs one release store. A poisoned generation rejects
normal cycles, checkpoint-drain cycles, whole-graph snapshots, and per-vnode snapshots with
`StatefulOperatorPartialApply`. The existing callback recovery mapping remains authoritative, and
`take_pipeline_fault` observes the same permanent graph condition. There is no clear operation.

The green regression proves the poisoned graph cannot execute or checkpoint and one explicit replay
invocation on a newly built graph restored from the preceding checkpoint emits the newer ASOF match.
It does not exercise a source cursor, callback publication, sink epoch, or end-to-end exactly-once.
A cluster-only regression proves cancellation while waiting behind vnode rotation does not poison:
the guard has not armed because no input or state has been admitted.

This is an in-memory graph-generation fence, not the future TidesDB root lifecycle. A native
operation may outlive the Rust request/graph and needs a backend-owner generation, late-completion
fence, retired root/directory, and portable-checkpoint restore. Cancellation after a checkpoint-
drain graph pass returns while asynchronous sink publication is pending is also a later callback-
transaction boundary. Current production does not perform either cancellation. Cluster remains
at-least-once only; best-effort and exactly-once cluster configurations remain rejected.

## AI slop review

**Pass.** The change follows two red tests at the exact borrowed-owner boundary. It adds no backend
trait, generic cancellation framework, operator hook, timeout, task spawn, rollback path, admission
flag, or TidesDB type. The production panic behavior and the latent direct/future-refactor hazard are
reported separately.

## Overengineering and hot-path review

**Pass, subject to normal performance regression monitoring.** The graph is the resource whose
inputs and operator state diverge, so its generation owns one allocation and one sticky bit. A
normal cycle performs one acquire load plus one `Arc` clone/drop; no per-row or per-operator branch,
lock, allocation, scheduler hop, or I/O was added. The callback supervisor poll performs one further
atomic read after its existing sink/checkpoint-fault checks. The abnormal path alone stores poison
and formats a recovery error. Unsafe self-pointers and per-operator guards were rejected.

## Unused-code review

**Pass.** Normal execution and checkpoint drain consume the entry check; both checkpoint capture
paths consume it; guard `Drop` is exercised by cancellation and panic tests; explicit completion is
exercised by the existing successful and returned-error suites; and `take_pipeline_fault` is the
live supervisor consumer. There is no reset API or disconnected future backend surface.

## Production-readiness review

**NO-GO.** A cancelled graph pass returns no partial cycle result, and that graph can no longer
produce a later successful cycle result or checkpoint. The change does not directly fence callback
MV/stream/sink publication after a graph pass has already returned, and no current cluster caller
exercised retained-owner cancellation before the fix. Managed byte-bounded working state, vnode
acquire/revoke publication, portable artifacts, checkpoint-delivery cancellation, term-fenced
source handoff, exactly-once external sink commits, a qualified official TidesDB package,
crash/resource qualification, and the independent multi-process soak remain open. `[LDB-4007]`
and `[LDB-0013]` remain unchanged.

## Documentation review

**Pass.** ADR-008, both implementation plans, and the validation report now distinguish explicit
errors, production-root panic, retained-owner cancellation, checkpoint delivery, and native-root
poison. Historical backend research remains labeled as reference/gate evidence. No research file
became false or irrelevant in this cycle, so none was removed.

## Test review

**Pass after three independent read-only audits.** Before the fence, both cancellation and caught-
panic tests failed because the old graph accepted `snapshot_state`. Review then found that the probe
proved only row count, callback fault consumption was untested, and “replay once” wording could be
misread as exactly-once certification. The probe now requires the newer `bid=20`, a callback test
observes the sticky graph fault twice, and the documents explicitly exclude source/sink delivery.
Final results are:

- default focused graph cancellation/panic: two passed;
- default focused callback poison consumer: one passed;
- all-feature focused graph cancellation/panic: two passed;
- all-feature cluster pre-rotation cancellation: one passed;
- all-feature focused callback poison consumer: one passed;
- `cargo test -p laminar-db --lib`: 1,290 passed and one profiling test ignored;
- `cargo test -p laminar-db --lib --all-features`: 1,774 passed and two explicit tests ignored;
- `cargo clippy -p laminar-db --lib --all-features -- -D warnings`: pass;
- `cargo fmt --all -- --check`, `git diff --check`, and local-document-link validation: pass; and
- Cargo manifests and lockfile contain no TidesDB dependency.

This is deterministic local regression evidence, not independent product-soak evidence.

## Cycle 47 review plan

Audit the complete checkpoint-drain publication await before adding a callback guard:

1. **AI slop:** reproduce cancellation only after a graph drain pass has advanced state and while a
   real asynchronous sink publication remains pending; do not add a production sleep/fault hook;
2. **Overengineering/hot path:** keep any guard on the checkpoint cold path and reuse the graph or
   callback's live recovery consumer; do not add normal-cycle task spawning or timeouts;
3. **Unused code:** require the cancellation signal to prevent later output, offset, and checkpoint
   publication and to be consumed by the coordinator;
4. **Production readiness:** preserve halt versus recovery dispositions, sink epoch poison, source
   cursor ordering, and the current non-cancellable shutdown contract;
5. **Documentation:** keep callback delivery poison separate from the future native TidesDB root and
   from the still-rejected cluster exactly-once mode; and
6. **Tests:** cover cancellation during sink publication, no checkpoint/source-offset advance,
   fresh-cut replay, ordinary returned sink errors, and unchanged completed checkpoint drain.

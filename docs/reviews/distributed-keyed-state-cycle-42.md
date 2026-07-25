# Distributed keyed state Cycle 42 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle outcome:** current incremental aggregates fail recovery-closed when state application may
  have started; output and due-checkpoint exclusion is tested through the live coordinator path
- **Runtime dependency, backend, adapter, workflow, or admission change:** none
- **TidesDB status:** official Cargo package `tidesdb v0.11.1` remains stopped at T0; not added,
  built, linked, or executed
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Outcome

Normal incremental aggregate execution previously propagated an ordinary `DbError::Pipeline` if a
later input batch or output construction failed after an earlier batch had already changed the
aggregate map. Failure-domain isolation could then retain and replay the input against changed
state. Cycle 42 maps errors from `IncrementalAggState::process_batch` and aggregate output
construction to the existing `StatefulOperatorPartialApply` recovery disposition. Errors already
classified for recovery or halt retain that stronger disposition.

The existing result boundary supplies the three execution outcomes without a speculative public
enum: successful processing is complete; ordinary projection/planning errors proved to precede
aggregate mutation retain their prior disposition; and any otherwise ordinary error after
aggregate mutation may begin is an indeterminate outcome requiring recovery. Existing stronger
pre-apply dispositions are preserved, including recovery after a partial shuffle send and halt for
a terminal structural error. The coordinator only publishes outputs after `execute_cycle`
succeeds. A recovery result stops every delivery mode, discards staged source offsets, and bypasses
both output publication and checkpoint admission.

This is deliberately not a TidesDB adapter or the future working-state root poison. No live hot-
state backend owns a native root yet, so a generic owner lane, publication token, process-fence
atomic, or qualification-only trait would be unused and could encode the wrong package semantics.
An admitted backend must still make an ambiguous native root permanently unusable, restore into a
fresh root under current ownership, and cover cancellation/panic/timeout. That remaining contract
cannot be claimed by this aggregate correction.

## Six-part cycle review

### 1. AI slop, evidence, and consistency

**Pass after independent review.** The implementation follows an observed failure path and
uses an existing recovery disposition. It does not claim atomic backend success, sticky native-root
poison, cluster exactly-once, qualification, or production readiness. “Partial apply” is documented
as an indeterminate outcome rather than proof that exactly a prefix landed.

### 2. Overengineering, hot path, and latency

**Pass after independent review.** The successful path adds no lock, atomic, allocation,
I/O, task, state scan, or per-record branch. It replaces iterator error propagation with a direct
per-batch loop and invokes classification only on error. Projection, shuffle, state update, and
output construction remain batch-scoped. No verified readback or per-key TidesDB work was added.

### 3. Unused code and scope

**Pass after independent review.** The only production helper is called by both fallible
post-mutation aggregate phases. The group-limit setter is test-only and creates a deterministic
late-batch failure. No new trait, enum, module, dependency, feature, backend, adapter, or fallback
exists. The checkpoint-artifact `StateBackend` remains unchanged.

### 4. Production readiness, delivery, and soak

**Pass only as a bounded correctness improvement; production remains NO-GO.** Recovery-class errors
stop best-effort as well as replay-guaranteed modes, so an indeterminate state mutation cannot be
dropped and followed by later work. This does not upgrade at-least-once to exactly-once: source
replay/fencing, one coordinator-admitted state/output cut with durable terminal Commit, and a
transactional or durably idempotent fenced sink remain separate requirements. Backend
qualification, rebalance lifecycle, resource/health admission, endurance, and the independent
unchanged-release soak remain open.

### 5. Documentation and research hygiene

**Pass after independent review.** Current authority documents record only the narrow
runtime correction and preserve the Cycle 41 upstream stop. No prior backend report was removed:
the dated Fjall, RocksDB, redb, and TidesDB records remain relevant decision/evidence history. No
new research dump or duplicate backend design was added.

### 6. Tests and reproducibility

**Pass for this bounded correction.** Tests prove:

- an ordinary post-mutation error becomes recovery-required while existing recovery/halt errors
  retain their disposition;
- batch one mutates an aggregate and batch two fails deterministically, producing
  `StatefulOperatorPartialApply`;
- callback mapping converts the indeterminate apply into coordinator-owned recovery without
  terminal-shutdown signaling; and
- even in best-effort mode with a checkpoint already due, a recovery cycle reaches neither stream
  publication, sink writes, checkpoint drain/capture, nor barrier publication.

The output-construction branch uses the same unit-tested classifier and was inspected directly; a
separate end-to-end emission fault injector was not added solely for this slice. Final commands:

- `cargo test -p laminar-db --lib`: 1,267 passed; one profiling test ignored;
- `cargo test -p laminar-db --lib --all-features`: 1,750 passed; two explicit external/profiling
  tests ignored;
- `cargo clippy -p laminar-db --lib --all-features -- -D warnings`: pass;
- focused `stateful_apply`, `later_aggregate_batch_failure`, and
  `recovery_cycle_error_faults_best_effort` filters: four passed;
- `cargo fmt --all -- --check`: pass;
- `git diff --check`: pass; and
- local links in the six changed/new Markdown documents: zero missing.

## Cycle 43 review plan

Before the next backend-neutral slice is committed, review:

1. **AI slop:** bind every claimed contract to a current consumer and executable negative case;
2. **Overengineering/hot path:** add no per-record allocation, lock, task, I/O, full-state scan, or
   metrics polling, and measure any new batch-level work;
3. **Unused code:** reject future traits, poison states, or capability flags without a runtime
   caller and failure test;
4. **Production readiness:** preserve fresh-root/owner fencing, source/sink delivery matrices,
   fail-closed admission, resource governance, and independent-soak gates;
5. **Documentation:** update one authority path and this review without duplicating the TidesDB T0
   source report; and
6. **Tests:** include the successful boundary, pre-apply rejection, ambiguity/cancellation path,
   checkpoint/rebalance exclusion, and unchanged hot-path behavior appropriate to the selected
   consumer.

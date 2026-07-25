# Distributed keyed state Cycle 44 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle outcome:** returned ASOF errors distinguish failed-before-apply from changed right state
- **Runtime dependency, backend, adapter, workflow, or admission change:** none
- **TidesDB status:** official Cargo package `tidesdb v0.11.1` remains stopped at T0; not added,
  built, linked, or executed
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Outcome

ASOF right-buffer ingest previously returned only `()`. After it succeeded, join construction,
residual projection, and watermark eviction could return ordinary errors even though retained state
had changed. Operator-graph isolation could then preserve and replay the same input against that
changed state.

`AsofRightBuffer::ingest` now returns a batch-level applied fact. Every fallible filter, concat,
key/time extraction, and buffer-merge step completes before the index, retained batch, or ingest
counter changes; a returned ingest error is therefore failed-before-apply. Learning the right
schema also counts as logical state change. Join/projection errors are recovery-required only when
that cycle installed rows or learned the schema. A left-only failure against unchanged prior state
remains ordinary. Eviction errors are always recovery-required because both eviction methods prune
their indexes before their only fallible compaction step. Existing recovery and halt dispositions
are preserved.

The classifier observes returned errors only. Cancellation or panic after synchronous ingest but
before `process` returns does not pass through `map_err`; attempt/root poisoning and fresh restore
remain open. A second existing gap is now explicit: `right_schema` is retained logical state but is
not present in the ASOF checkpoint when the right buffer is empty, so restore after complete
eviction cannot yet reconstruct null-extended left output. Cycle 44 does not claim complete ASOF
checkpoint/replay or distributed readiness.

## Six-part cycle review

### 1. AI slop, evidence, and consistency

**Pass after independent review.** Each classification is tied to an inspected mutation boundary.
The recovery diagnostic says state “may have changed,” which is correct for both new-row admission
and eviction-only mutation. The result is not generalized to temporal-probe, lookup, or every join,
and it does not claim cancellation safety, backend atomicity, or cluster exactly-once.

### 2. Overengineering, hot path, and latency

**Pass after independent review.** Ingest returns a boolean fact already known at its commit point;
the caller combines it with schema learning once per Arrow batch. Error formatting and disposition
branches execute only on error. There is no new per-record branch, allocation, clone, state scan,
lock, atomic, task, I/O, readback, or recovery action on successful processing.

### 3. Unused code and scope

**Pass after independent review.** Both local classifiers have live ASOF callers. The applied fact
is consumed by the operator and tests cover both values. No graph-wide wrapper, public trait, enum,
feature, backend, adapter, dependency, fault framework, or qualification-only seam was added.

### 4. Production readiness, delivery, and soak

**Pass only as a bounded embedded-mode correctness improvement; production remains NO-GO.** The
existing graph/callback/coordinator path propagates `StatefulOperatorPartialApply` as recovery and
admits neither that cycle's output nor a checkpoint. A unit test calls checkpoint directly only for
forensic inspection of retained state; it does not model or authorize checkpoint publication after
recovery.

Panic/cancellation poisoning, right-schema checkpoint completeness, vnode ownership, durable
working state, fenced rebalance, source/sink delivery composition, and exactly-once terminal commit
remain open. Backend qualification, hot-key/Zipf tail gates, endurance, and the independently
operated unchanged-release soak remain mandatory.

### 5. Documentation and research hygiene

**Pass after independent review.** The authority documents record the returned-error correction and
its two ASOF lifecycle limits. Backend research is unchanged; dated Fjall, RocksDB, redb, and
TidesDB documents remain relevant decision history. No obsolete approval workflow or new research
dump was added.

### 6. Tests and reproducibility

**Pass after independent review.** Focused tests cover:

- malformed first right input returns an ordinary error and leaves schema, buffer, and index empty;
- malformed right input preserves byte-identical prior buffer state and equal canonicalized index
  entries;
- a left-only error against unchanged prior right state remains ordinary;
- invalid projection after right-state admission returns recovery-required partial apply, while
  forensic decode proves two right index entries were retained; and
- `Checkpoint`, `ShufflePartialSend`, `BackpressureFail`, and `ShuffleTerminal` retain their stronger
  dispositions.

Final commands and results:

- `cargo test -p laminar-db --lib asof_state` and the `asof_partial_apply` filter: five passed;
- `cargo test -p laminar-db --lib`: 1,273 passed; one profiling test ignored;
- `cargo test -p laminar-db --lib --all-features`: 1,756 passed; two explicit
  external/profiling tests ignored;
- `cargo clippy -p laminar-db --lib --all-features -- -D warnings`: pass;
- `cargo fmt --all -- --check` and `git diff --check`: pass;
- local links in the five changed/new Markdown documents: zero missing; and
- Cargo manifests/lockfiles contain no TidesDB dependency.

## Cycle 45 review plan

Before ASOF checkpoint completeness is changed, review:

1. **AI slop:** prove the empty-buffer/right-schema failure with a red test and define one canonical
   schema authority; do not conflate schema retention with row-state qualification;
2. **Overengineering/hot path:** serialize schema only at checkpoint boundaries, with no per-batch
   encoding, remote I/O, or schema clone beyond existing ownership;
3. **Unused code:** version only the live ASOF checkpoint envelope and keep compatibility logic
   bounded to supported prior versions;
4. **Production readiness:** cap and validate schema bytes, restore atomically, preserve corruption
   as recovery-required, and keep cancellation/root poisoning, source/sink, backend, and soak gates
   open;
5. **Documentation:** update the authority chain without claiming full stateful-join recovery; and
6. **Tests:** cover ingest, checkpoint/restore, complete eviction, left-null output, v1 compatibility
   or explicit fail-closed version policy, corrupt/truncated schema, and unchanged nonempty-buffer
   restore behavior.

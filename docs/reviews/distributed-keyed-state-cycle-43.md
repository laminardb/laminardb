# Distributed keyed state Cycle 43 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle outcome:** analytic window-frame history commits only after residual projection succeeds
- **Runtime dependency, backend, adapter, workflow, or admission change:** none
- **TidesDB status:** official Cargo package `tidesdb v0.11.1` remains stopped at T0; not added,
  built, linked, or executed
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain fail-closed

## Outcome

`WindowFrameOperator` previously replaced its retained history before awaiting its residual SQL
projection. If that projection failed, the operator graph could preserve and replay the input after
the history had already advanced, appending the same rows twice.

The operator already had a natural commit point, so Cycle 43 does not promote the error to an
indeterminate apply. It computes the candidate tail exactly as before, awaits projection, and
installs the tail only after projection succeeds. A projection error, or cancellation while the
projection is pending, leaves the exact prior history in place and remains an ordinary
failed-before-apply result. There is no fallible step between history assignment and successful
return.

Read-only audits also identified separate mutation-boundary work in EOWC window aggregates,
temporal filters, ASOF and temporal-probe joins, lookup enrichment, and AI inference. Those paths
are not changed or claimed safe here. Incremental IVM joins already stage/roll back ordinary output
failures; interval joins already prevalidate and recovery-classify post-admission failures; raw
EOWC stages before commit.

## Six-part cycle review

### 1. AI slop, evidence, and consistency

**Pass after independent review.** The change is tied to one observed assignment-before-error path
and its deterministic replay consequence. It does not introduce a generic `ApplyOutcome`, claim
that all window operators are fixed, qualify TidesDB, or relax cluster admission. The distinction
between failed-before-apply and indeterminate apply remains explicit.

### 2. Overengineering, hot path, and latency

**Pass after independent review.** The successful path performs the same concatenation, enrichment,
tail slice, and projection. Only assignment order changes. No additional allocation, clone, scan,
lock, atomic, task, I/O, metric poll, or per-record branch was added. The correction avoids the
larger cost and operational disruption of coordinated recovery when no logical state was committed.

### 3. Unused code and scope

**Pass after independent review.** Production Rust changes touch one live operator, with one new
regression test. There is no helper, trait, enum, feature, dependency, backend, adapter, fault
framework, or unused production seam. Existing projection and checkpoint facilities are reused.

### 4. Production readiness, delivery, and soak

**Pass only as a bounded embedded-mode correctness improvement; production remains NO-GO.** The
change prevents duplicate frame history during ordinary replay, but supplies none of the missing
vnode ownership, managed durable working state, fenced rebalance, source/sink delivery composition,
or exactly-once commit protocol. Backend qualification, hot-key/Zipf tail gates, endurance, and the
independently operated unchanged-release soak remain mandatory before production admission.

### 5. Documentation and research hygiene

**Pass after independent review.** The ADR, phased plan, execution plan, and baseline validation
report record only this narrow commit-order result. Backend research remains unchanged: dated Fjall,
RocksDB, redb, and TidesDB documents are still relevant decision history. No obsolete workflow or
new research dump was added.

### 6. Tests and reproducibility

**Pass after independent review.** The deterministic regression first commits frame-history rows
`[1, 2]`, injects an invalid residual projection while processing row `3`, and proves the error is
ordinary and retained history remains `[1, 2]` rather than advancing to `[2, 3]`. Existing analytic
tests cover the successful cross-cycle and bounded-tail paths.

Final commands and results:

- `cargo test -p laminar-db --lib projection_failure_does_not_advance_frame_history`: one passed;
- `cargo test -p laminar-db --lib`: 1,268 passed; one profiling test ignored;
- `cargo test -p laminar-db --lib --all-features`: 1,751 passed; two explicit
  external/profiling tests ignored;
- `cargo clippy -p laminar-db --lib --all-features -- -D warnings`: pass;
- `cargo fmt --all -- --check` and `git diff --check`: pass;
- local links in the five changed/new Markdown documents: zero missing; and
- Cargo manifests/lockfiles contain no TidesDB dependency.

## Cycle 44 review plan

Before the next correction is committed, review:

1. **AI slop:** bind the selected operator's exact pre-apply and post-admission boundaries to code
   and executable failures; do not generalize from one join to all joins;
2. **Overengineering/hot path:** prefer staging or rollback; if recovery classification is required,
   keep it on the cold error path with no successful-path allocation, lock, I/O, or scan;
3. **Unused code:** add no graph-wide outcome wrapper or future backend trait without a live caller;
4. **Production readiness:** preserve stronger recovery/halt dispositions, output/checkpoint
   exclusion, source/sink delivery gates, fail-closed admission, and independent-soak requirements;
5. **Documentation:** update the authority chain concisely without duplicating backend source
   reports; and
6. **Tests:** prove a pre-apply rejection leaves state unchanged, a post-admission failure cannot be
   isolated/retried against changed state, successful behavior is unchanged, and any recovery result
   reaches neither publication nor checkpoint.

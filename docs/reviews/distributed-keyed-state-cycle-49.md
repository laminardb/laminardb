# Distributed keyed state Cycle 49 review

- **Date:** 2026-07-26
- **Scope:** follower checkpoint/assignment fence coverage, serialization-permit versus rebalance
  latency, and checkpoint-versus-rotation independent-soak specification
- **Cycle outcome:** all leader/follower mutable-capture routes are mechanically covered; an
  overlapping capture now fails before mutation instead of waiting under the assignment fence
- **Admission/backend outcome:** unchanged; no TidesDB dependency, runtime backend, or cluster
  capability was added
- **Production verdict:** **NO-GO** pending managed vnode ownership, delivery, qualified local
  working state, fault/latency evidence, and an independently operated release-candidate soak

## Result and exact boundary

Cycle 48 established that the production leader capture holds the existing assignment-rotation read
token from after the sink FIFO fence, through shuffle alignment and whole/vnode mutable capture, and
releases it before encoding and durable tail I/O. Cycle 49 drives both follower entry paths through
their real callbacks:

1. the source-less `service_checkpoint_control -> maybe_follower_checkpoint -> CaptureNow` route;
2. the source-barrier `checkpoint_with_barrier -> route_follower_checkpoint_barrier ->
   run_follower_checkpoint_deferred` route; and
3. the already-covered leader/source-less route shared by sourceful leader capture.

The follower probes install one operator that attempts assignment-write acquisition from both its
whole-state and per-vnode snapshot callbacks. Both attempts must fail. They then block the durable
tail on the real coordinator mutex, wait until `checkpoint_in_flight` proves tail handoff, acquire
and retain the assignment write token while the tail remains blocked, release the coordinator, and
let the route complete while the writer is still held. This proves the read token is absent from the
tail and cleanup rather than merely showing that it is eventually released after callback return.
The deferred route also retains its pre-existing pending-attempt cleanup assertion.

## Serialization-permit latency correction

The fence audit exposed a recovery-path priority inversion. Mutable capture acquired the rotation
read token and then awaited the sole serialization permit until the checkpoint/serialization
deadline, 120 seconds by default. Assignment publication needs the opposing write token and its
default rebalance deadline is 15 seconds. A timed-out blocking encoder is intentionally
non-abortable and retains its permit, so a later capture could prevent assignment publication from
meeting its shorter deadline without doing useful work.

Healthy admission allows one checkpoint tail, and the callback requires exclusive `&mut self`
ownership during capture. Permit contention therefore means that an old non-abortable encoder still
owns the image or that the admission invariant has been breached; it is not a valid queue. Capture
now uses `try_acquire_owned()` at the existing pre-snapshot point:

- `NoPermits` returns `[LDB-6017]` synchronously before graph, MV, or table capture;
- the rejected capture neither mutates operator state nor creates a second sticky fault;
- the original encoder continues to own the permit and its already-established failure fence until
  it exits; and
- unwinding the capture route drops the rotation token so a waiting assignment writer can proceed.

The capture helpers became synchronous and their redundant Tokio timeout wrappers were removed.
This does not weaken preemption: after the former semaphore await, all snapshot calls were already
synchronous, so `timeout_at` could not interrupt them. The explicit pre-capture deadline checks and
the separately bounded asynchronous encoder remain. Synchronous snapshot duration itself remains a
latency and independent-soak gate.

## Hot path, delivery, and backend disposition

The normal row/Arrow-batch path is unchanged. The checkpoint-only contended path replaces an await,
timer, and potential long fence hold with one semaphore try-acquire. Successful capture retains the
same permit ownership through `CapturedOperatorState` and the non-abortable blocking encoder.

Sink FIFO fencing still precedes mutable capture. A rejected attempt publishes no new operator
image or source cut and follows the existing leader/follower cleanup classification. Cluster mode
remains at-least-once: already accepted output can duplicate after recovery. Exactly-once remains
rejected with `[LDB-0013]`; unsupported source handoff, sink/output combinations, grouped/windowed/
join state, and materialized views retain their existing fail-closed gates.

TidesDB through the official `tidesdb/tidesdb-rs` binding remains the selected but T0-stopped
worker-local candidate line. This cycle did not add, download, compile, link, execute, qualify, or
admit TidesDB, Fjall, RocksDB, redb, or another backend. Bounded memory remains a reference and
conformance subject only.

## Independent-soak disposition

The production-soak charter now freezes a minimum checkpoint-versus-rotation family:
`CVR-SL-F-ALO-v1`, `CVR-SL-L-ALO-v1`, `CVR-SF-F-ALO-v1`, `CVR-SF-L-ALO-v1`, and
`CVR-EO-REJECT-v1`. It defines externally witnessed overlap, permanent `INVALID` handling for a
missed overlap, exact checkpoint/assignment/recovery gates, the narrow legal ALO replay boundary,
the `[LDB-0013]` negative case, and per-node/process-generation latency distributions.

The existing `cluster_soak.rs` remains useful engineering scaffolding but builds a test binary,
uses test-only gates, and lacks the source-less route, complete output provenance/FIFO oracle,
rotation-wait distributions, and exactly-once negative case. The standalone independent-soak tool
is schema/fixture-only. WSL 2/Docker availability can support a labelled local engineering smoke,
not independent evidence. No soak ran in Cycle 49 and `certification_eligible` remains `false`.

## AI slop review

**Pass.** The implementation reuses the existing semaphore, assignment fence, callback fixtures,
and audit operator. It adds no retry manager, cancellation abstraction, checkpoint transaction,
backend interface, production-only metric, or duplicate route implementation. The independent soak
specification lives in its existing charter rather than another design document.

## Overengineering and hot-path review

**Pass.** Fail-fast is justified by the existing one-tail admission invariant and is smaller than
reordering permit ownership through every asynchronous alignment path. It removes work from a cold
failure path and adds nothing to row processing. Three route probes share two test helpers. No
production helper exists solely for testing.

## Unused-code review

**Pass.** Every changed production function is called by the leader or follower checkpoint route.
The synchronous cascade removes, rather than preserves, unused async state. Both test helpers have
multiple consumers, and every added scenario or assertion proves a named lifecycle boundary.

## Production-readiness review

**NO-GO.** Local deterministic evidence closes the known retained-encoder/rebalance inversion and
the follower route-audit gap. It does not bound synchronous snapshot duration, qualify distributed
keyed ownership, provide spill-backed byte bounds, add output provenance/fencing, widen delivery,
or substitute for faulted multi-process latency distributions and the independent soak.

## Documentation review

**Pass.** The validation report, ADR-008, phased plan, Phase 0 execution order, and production-soak
charter now agree on the exact fail-fast behavior, the non-preemptible synchronous boundary, the
unchanged source/sink/delivery/backend status, and the unexecuted soak. No research or evidence
document became false or irrelevant, so none was removed.

## Test review

**Pass with one investigated non-causal flake.** Final validation is:

- focused leader, immediate-follower, deferred-follower, held-permit, deadline, and whole-state
  request tests: pass;
- `cargo test -p laminar-db --lib`: 1,290 passed and one profiling test ignored;
- initial `cargo test -p laminar-db --lib --all-features`: 1,778 passed, two ignored, and the
  existing `asof_join_in_materialized_view_emits_backward_match` timing probe failed after more than
  60 seconds;
- that exact embedded/no-checkpoint test then passed 3/3 in 0.07–0.08 seconds; its Git history
  records suite-load/CPU-starvation mitigations and Cycle 49's checkpoint callback path is
  unreachable;
- the required complete all-feature rerun passed: 1,779 passed and two ignored;
- `cargo clippy -p laminar-db --lib --all-features -- -D warnings`: pass; and
- formatting, diff, local-link, and dependency hygiene: pass.

The first failed all-feature run is retained in the record rather than silently discarded. The
passing rerun closes this cycle's regression gate; neither run is an independent soak or keyed-state
production evidence. Two independent read-only code reviews approve the synchronous permit/follower
changes after checking ownership, failure classification, deadline semantics, hot-path cost, and
test simplicity.

## Cycle 50 review plan

Audit the largest blocker to an independent at-least-once oracle without widening runtime scope:

1. **AI slop:** map existing checkpoint, connector, and sink identities before proposing another
   envelope or sequence;
2. **Overengineering/hot path:** keep provenance off the row path unless a concrete external oracle
   consumes it; prefer batch/writer-interval metadata over per-row coordination;
3. **Unused code:** add no production field, marker, or metric in an audit-only cycle;
4. **Production readiness:** determine whether Kafka append output exposes stable operation
   identity, assignment generation, writer interval, admission sequence, and successor fencing
   sufficient to distinguish legal ALO replay from stale-owner output;
5. **Documentation:** assign each missing fact to Laminar, the connector, or the independent
   controller and state what can be observed through public surfaces; and
6. **Tests:** use read-only/code-level evidence and existing connector tests; if a contract gap is
   proven, specify the smallest deterministic regression before implementation. Do not run a
   backend candidate, relax `[LDB-4007]`, claim exactly-once, or claim production readiness.

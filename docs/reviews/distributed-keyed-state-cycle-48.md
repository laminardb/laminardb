# Distributed keyed state Cycle 48 review

- **Date:** 2026-07-26
- **Scope:** admitted global-aggregate transition reachability and checkpoint/assignment capture
  atomicity
- **Cycle outcome:** staged vnode transitions now enter checkpoint drain; assignment publication is
  excluded through shuffle alignment and whole/vnode mutable capture
- **Admission/backend outcome:** unchanged; no TidesDB dependency, runtime backend, or cluster
  capability was added
- **Production verdict:** **NO-GO** pending managed vnode ownership, delivery, backend, fault,
  latency, and independent-soak gates

## Result and exact boundary

The current global-aggregate exception is per stream query, not per cluster graph. The admission
regression now creates two separate global aggregate streams, so multiple vnode-0 stateful
operators can coexist. The existing rehydration regression already proves that a later operator can
fail after an earlier operator has applied a vnode image. That returned checkpoint-class error is
contained today: it maps to coordinated recovery, reaches neither output publication nor checkpoint
capture, and the callback/graph generation is destroyed before the coordinator returns. Recovery
builds a fresh graph from the last committed cut. A second sticky poison for explicit returned
errors would not improve that production lifecycle and was not added.

The audit did find a separate live checkpoint defect. `checkpoint_is_quiescent()` previously saw
only queued graph input. A stable staged acquire/revoke, or a registry assignment that graph
execution had not yet observed, could therefore be omitted by an idle, source-less, or follower
checkpoint. A transient red probe removed the fix and failed in 0.02 seconds at
`checkpoint_quiescence_requires_staged_vnode_transitions_to_apply`: the staged acquire did not make
the graph non-quiescent. The fix was immediately restored.

Checkpoint quiescence and both snapshot APIs now require empty revoke/rehydration staging maps and
the graph's last executed assignment version to equal the registry version. The two maps are sampled
under both mutexes in adoption order. A checkpoint drain pass applies the transition before capture;
whole and vnode snapshots independently fail closed if a caller bypasses that drain.

The final quiescence sample still had a time-of-check/time-of-use gap. Assignment adoption could
publish after that sample and before mutable capture. All production capture routes now reuse the
existing rotation fence:

1. source barriers and the checkpoint graph drain establish the local cut;
2. accepted sink commands complete their FIFO synchronization;
3. the callback acquires the deadline-bounded assignment read token and revalidates the admitted
   assignment certificate plus graph quiescence;
4. shuffle barriers align under the token, followed by another certificate check;
5. whole and vnode images are captured under the same token and the certificate is checked again;
6. the token is dropped before encoding, durable checkpoint-tail I/O, or awaited cleanup.

Assignment adoption takes the opposing write token before staging maps or publishing ownership.
Follower vnode capture was removed from `follower_tail_future` and is now completed beside the
whole image. Immediate-follower awaited failure cleanup explicitly drops the read token first;
leader and deferred-follower failure returns drop it synchronously. Deadline-bounded shuffle
transport and authority-settlement reads remain inside the token because staged channel state is
part of the same cut.

A source-less leader regression drives the real `checkpoint_with_barrier` callback. Its operator
asserts that assignment write acquisition fails inside both `checkpoint()` and
`checkpoint_by_vnode()`, then the test proves write acquisition succeeds after callback capture.
Sourceful leader shares this path. Code and ownership review confirms both follower routes retain
already-captured images and release the token before their tails.

## Delivery, latency, and backend disposition

The ordering correction does not widen delivery claims. Cluster mode remains at-least-once only;
already accepted output may duplicate after recovery. Exactly-once, best-effort cluster mode,
unsupported source handoff, incompatible sink modes, and retraction output without a compatible
sink remain fail-closed. The sink FIFO fence precedes state capture, and source offsets remain out
of the durable manifest until the operator cut is captured.

The normal row/graph path is unchanged. Checkpoint polling adds two short staging-map reads and one
assignment-version comparison. Capture adds a deadline-bounded read acquisition on the existing
rotation lock; no new lock, atomic, allocation, task, backend, or state abstraction was introduced.
Alignment deliberately holds the token across bounded network/authority work. The serialization
permit is also acquired under it, but healthy admission permits one in-flight checkpoint, so it is
normally immediately available. A retained non-abortable encoder is recovery-only and bounded by
the shared deadline. Cycle 49 must test this anomalous contention and measure rebalance-writer wait;
the independent checkpoint-versus-rotation soak remains mandatory before any production claim.

TidesDB through the official `tidesdb/tidesdb-rs` binding remains the selected but T0-stopped
candidate line. This cycle does not add, download, compile, link, execute, qualify, or admit it. It
also does not add Fjall, RocksDB, redb, or another runtime backend. Bounded memory remains a
reference/conformance model only.

## AI slop review

**Pass.** The implementation reuses the current graph transition markers, staging handles,
assignment certificate, and rotation fence. It adds no generic checkpoint transaction, state trait,
rollback layer, backend adapter, retry task, or speculative capability. The one callback audit test
exercises production behavior rather than duplicating four route fixtures.

## Overengineering and hot-path review

**Pass.** Three independent reviews found the read-token scope mechanically correct and the
checkpoint-only checks to have zero normal row-path cost. Moving serialization-permit acquisition
across every route was rejected for this cycle because normal one-in-flight admission makes it
immediately available; a deterministic Cycle 49 contention probe must precede any such plumbing.
The explicit immediate-follower drops are retained because its cleanup awaits.

## Unused-code review

**Pass.** Every new production method is called by the checkpoint callback, both snapshot guards
are exercised, and the follower tail consumes its newly explicit vnode image. The test-only audit
operator and assignment-version accessor are used. No backend or disconnected future API exists.

## Production-readiness review

**NO-GO.** Today's admitted global state can no longer be captured across an unapplied assignment
transition, and explicit partial-apply errors recover the whole generation. This does not provide
byte-bounded managed keyed state, authoritative roster/off-side publication, window timers, join
indexes, transactional sink composition, exactly-once, a qualified local backend, rescale fault
evidence, or the independently operated multi-process soak. Alignment/rotation and anomalous
encoder contention still need latency evidence.

## Documentation review

**Pass.** The validation report, ADR-008, phased plan, and Phase 0 execution order now distinguish
current global-vnode containment from the future managed-state lifecycle, state the capture fence's
bounded alignment I/O truthfully, preserve source/sink and at-least-once constraints, and keep the
backend/admission verdict unchanged. No research document became false or irrelevant, so none was
removed.

## Test review

**Pass for this cycle's correction.** Final validation is:

- transient red probe: one expected failure in 0.02 seconds when staged acquire was deliberately
  removed from checkpoint quiescence; the fix was then restored;
- focused transition, capture-fence, admission, partial-failure, and callback-destruction tests:
  pass;
- `cargo test -p laminar-db --lib`: 1,290 passed and one profiling test ignored;
- `cargo test -p laminar-db --lib --all-features`: 1,777 passed and two explicit tests ignored;
- `cargo clippy -p laminar-db --lib --all-features -- -D warnings`: pass; and
- formatting, diff, local-link, and dependency hygiene: pass.

The first default-suite invocation reached its 120-second command wrapper during a rebuild; the
identical five-minute invocation completed successfully. Three independent read-only reviews
approve after adding the production callback capture-span regression and correcting the I/O
comment. These tests are local regression evidence, not the independent product soak.

## Cycle 49 review plan

Close the remaining route/latency evidence without widening the implementation:

1. **AI slop:** extend the existing follower fixtures with the same fence-audit operator; do not add
   a test-only production abstraction or duplicate the full cluster harness;
2. **Overengineering/hot path:** deterministically hold the serialization permit while assignment
   publication queues, then choose permit-first reservation, fail-fast, or no change from measured
   behavior rather than anticipation;
3. **Unused code:** add no metric or helper unless a runtime decision consumes it and a test proves
   its lifecycle;
4. **Production readiness:** cover immediate and deferred follower capture spans, source-less and
   sourceful leader equivalence, checkpoint deadline, rotation cancellation, sink FIFO ordering,
   and at-least-once recovery disposition;
5. **Documentation:** record exact lock/wait distributions and keep exactly-once/backend/admission
   claims closed; and
6. **Tests:** add deterministic immediate-follower and deferred-follower writer-exclusion probes,
   an anomalous held-permit/rebalance-writer probe, and the corresponding independent-soak scenario
   specification. Run no backend candidate and claim no production readiness.

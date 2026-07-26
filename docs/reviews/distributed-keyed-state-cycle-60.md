# Distributed keyed state Cycle 60 review

- **Date:** 2026-07-26
- **Scope:** coherent observed-cut retry proof, direct-route test disposition, and checkpoint-
  instrumentation A/B design
- **Code outcome:** accepted in `fbb8fae8`; test-harness code only
- **Empirical outcome:** no A/B, backend trial, or new real-process soak ran
- **Runtime/backend outcome:** no keyed operator, backend, admission flag, evidence endpoint,
  source/sink contract, or delivery guarantee changed
- **Production verdict:** **NO-GO**; `[LDB-4007]`, `[LDB-0013]`, backend qualification, complete
  checkpoint authority, a prospectively powered instrumentation equivalence run, and the
  independent immutable release-binary soak remain open

## Coherent-cut retry proof

`CheckpointBarrierTimingEvidence::finalize_node` now delegates its existing test-target logic to a
private two-closure helper. The production harness wrapper still performs the same sequence:
capture exact pages, read metrics, capture, read metrics, capture, compare the three exact cuts and
two metric snapshots, reconcile, flush, and finalize before the same deadline. No trait, mock HTTP
server, clock abstraction, library API, or runtime branch was added.

The scripted test uses three attempts:

1. exact cursor and metadata remain stable while the two metric snapshots differ;
2. both metric snapshots already describe two records while the third exact capture appends record
   two, so only exact cursor/metadata instability can force the retry; and
3. exact and metric observations remain stable and the two-record cut finalizes.

It requires nine capture calls and six metric reads and accepts only the final two-record latency
snapshot. Removing either equality gate finalizes early and violates those assertions. An
independent review found the initial script accidentally protected the exact-only case with metric
reconciliation; the metric sequence was corrected before commit and the focused test passed.

## Direct nonempty HTTP pagination disposition

The deterministic direct-route gap is retained, not relabelled as closed. Existing coverage is
layered:

- the ledger proves exclusive cursor pagination and continuation;
- the DB API proves that continuation is bound to the live process;
- HTTP tests prove query parsing, bearer and serving gates, status mapping, empty response headers
  and envelope, the byte cap, and serialization of the maximum 64-record page;
- the collector deterministically proves multi-page application and reconciliation; and
- Cycle 59 used the real protected route for 392 records, including process generations with 139,
  139, and 110 records, proving nonempty process-bound continuation across polls but not that one
  HTTP snapshot returned more than 64 pending records.

There is no production record-seeding API. A valid direct test would need a live one-node cluster
with process and leader leases, barrier service, assignment and catalog authority, verified
storage, a custom replayable/splittable source, an admitted pipeline, and 65 successful durable
checkpoints. Duplicating that lifecycle in the HTTP unit suite would add a connector dependency,
slow/flaky Windows CI work, and little new fault-detection value. A hidden injector, widened DB API,
or fake route branch would weaken the evidence boundary and was rejected. Revisit the direct test
only if a reusable fast production cluster fixture exists, the cursor contract changes, or a
pagination regression appears.

## Frozen A/B v1

The canonical protocol is in the
[production-soak charter](../testing/distributed-state-production-soak-charter.md#cycle-60-instrumentation-ab-protocol).
It defines two named contrasts:

- recorder treatment versus recorder control, using `40e3637b` and a hashed derived tree containing
  only the patch content of `6084462b` and `3909f4d4`, with no runtime/config/feature switch and no
  HTTP route; and
- polling treatment versus polling control, using the same immutable `3909f4d4` server binary with
  one separately pinned observer process issuing or suppressing the frozen request schedule.

The three-node topology is held constant while input becomes a content-addressed 80,000-record
trace paced at 400 records/s inside a fixed 290-second window. The common driver owns manual
checkpoint slots and kills the leader only at one-based ordinal 80 after the existing debug gate
proves `Snapshotting`; a predeclared recovery gap precedes the remaining slots. Diagnostic
observations cannot change those anchors. Eight four-arm temporal blocks use a precomputed balanced
Williams order; warm-ups are per arm, a run is the unit of replication, invalid external slots
invalidate their whole block, and replacements preserve the same sequence. Product failures are
never excluded.

Common run-level estimands use only pre-existing Prometheus and external resource surfaces. Neither
recorder arm exposes comparable exact evidence, and the polling control can lose the killed
generation before a final read, so exact records/maxima are excluded from every v1 A/B contrast.
`B - A` estimates recorder installation without the route; `D - C` estimates polling conditional on
recorder plus route. They cannot be summed, and B remains an intent-to-install comparison rather
than an empirical ledger-activation proof. Producer acknowledgements are workload diagnostics;
distinct sink-output completion/backlog is the SUT throughput outcome.

This v1 protocol estimates effects only. The current `cluster_soak` harness cannot execute it
because diagnostic evidence affects control flow. No A/B ran. Closing the perturbation gate needs
a separate common driver/observer, owner-approved equivalence margins, and a prospectively sized v2.
The earlier Cycle 57 failures remain historical red results, not A/B samples.

## Cycle review

- **AI slop — pass:** a deterministic direct-route checkbox was not manufactured through a fake
  seed hook, the earlier red runs were not pooled with a different experiment, and exact records
  unavailable to the recorder control are excluded from its estimands.
- **Overengineering and hot path — pass:** the only code seam is private to the integration-test
  target. No row/checkpoint runtime branch, generic telemetry abstraction, extra endpoint, test
  feature, connector dependency, or 65-checkpoint HTTP fixture was added.
- **Unused code — pass:** the helper is called by the real soak finalizer and its scripted test; the
  wrapper remains the only production-harness entry point.
- **Production readiness — NO-GO:** the protocol has not run and cannot itself certify production.
  TidesDB remains stopped before runtime integration; exact full-checkpoint/restorable-gate and
  same-snapshot durable outcome/capsule authority, legal ALO replay evidence, exactly-once
  composition, admitted keyed state, and independent soak remain absent.
- **Documentation — pass:** the soak charter owns the A/B protocol. This review records the decision
  and does not copy the ledger schema or promote engineering artifacts to certification evidence.
  No research document became obsolete in this cycle.
- **Tests — pass for the implemented slice:** the focused coherent-cut test passed 1/1; the full
  harness passed 34/34 non-ignored tests with two real-process tests still ignored by default;
  warnings-denied Clippy, formatting, diff checks, and 100 relative links across seven changed
  documents passed. The direct nonempty HTTP route test remains the explicitly accepted low-risk
  composition gap.

## Cycle 61 review plan

1. **AI slop:** do not run the coupled `cluster_soak` target and label it A/B evidence. Prove the
   common driver and observer cannot make treatment-dependent correctness or fault decisions.
2. **Overengineering/hot path:** prototype only the smallest prebuilt-server launcher and separate
   observer boundary. Do not add a server runtime toggle or compile-time checkpoint branch.
3. **Unused code:** any arm/schedule manifest validator must be consumed by a deterministic dry run;
   otherwise do not add it.
4. **Production readiness:** keep v1 effect estimation distinct from a future powered equivalence
   v2 and from the independent release-binary soak.
5. **Documentation:** retain exact derived-tree, binary, driver, observer, schedule, and environment
   identities in one execution record; do not duplicate the charter.
6. **Tests:** prove named contrasts, balanced primary/reserve order, process-generation metric reset
   handling, and external-`INVALID` versus product-`FAIL` classification before any long run.

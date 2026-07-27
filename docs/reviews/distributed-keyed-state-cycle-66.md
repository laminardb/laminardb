# Distributed keyed state Cycle 66 review

- **Date:** 2026-07-27
- **Scope:** fake-only sealed-driver ingress, observer supervision, post-end result consumption,
  replay binding, bounded cleanup, and non-interference tests
- **Implementation:** `4a95a3db`, `76c43064`, and `8108966c`
- **Empirical outcome:** owned loopback fake servers and local child processes only; no LaminarDB
  request, workload, fault campaign, backend trial, latency measurement, A/B estimate, or soak ran
- **Component verdict:** **APPROVE** the sealed driver's bounded fake-only consuming-supervisor path
- **Live/production verdict:** **NO-GO**; the accelerated protocol is not a live-paced observer,
  loopback is not process attestation, durable timing-tail continuity is absent, and every runtime,
  backend, delivery, multi-host, A/B, and independent-soak gate remains open

## Completed boundary

The root-workspace-excluded driver now has an explicit `fake-protocol` command. Its stdin accepts
only a canonical endpoint DTO and one canonical 32-byte diagnostic secret. The driver—not its
caller—constructs and binds sanitized plan v2 to the sealed common plan. Neither endpoints nor the
secret can enter the manifest, base plan, trace, arguments, general environment, files, artifacts,
logs, or fallback configuration. Both driver and observer bound bootstrap acquisition at two
seconds without requiring EOF. IPv6 loopback endpoints require zero flowinfo and scope ID so
distinctness and serialized identity cannot disagree.

After resolving the manifest and common plan, the driver rechecks the observer's declared regular-
file identity immediately before spawn. The child verifies its own executable and the sanitized-
to-base-plan binding. The fake child runs with an empty environment except for the already reviewed
absolute Windows `SystemRoot` requirement. This is operational substitution resistance, not an
atomic executable attestation or signed-binary claim.

Bootstrap v4 includes a driver-generated UUIDv4 invocation identity. Result v3 canonically echoes
that identity as well as the exact plan, base-plan, schedule, and arm bindings. A result from a prior
otherwise identical invocation is rejected. The diagnostic bearer remains separate and is never
copied into the result. UUID generation adds `getrandom` and UUID serde only to this isolated tool;
the root workspace and LaminarDB runtime dependency graphs remain unchanged.

The driver executes, persists, syncs, rereads, and consumes the exact common trace end seal before
it inspects child status, bootstrap delivery, captured output, or protocol disposition. Success,
incomplete exit code 2, other nonzero exit, malformed result, completion deadline, status failure,
oversize output, capture failure, and termination failure have distinct fail-closed outcomes.
Timeout classification stays sticky even if cancellation, kill, or reap completes later. One
bounded cleanup retry precedes a fixed manual-cleanup event and stderr marker if termination still
cannot be confirmed.

Stdout and stderr are drained concurrently into fixed in-memory caps. Fake mode persists neither
raw stream; it persists only byte counts, hashes, fixed classifications, and a validated safe DTO.
Capture buffers, HTTP bodies, request buffers, bootstrap secret scratch after completed reads, and
owned secret storage are zeroized. The cancellation event records only that the supervisor wrote
the frame; it does not claim an observer acknowledgement.

Result validation now enforces the producer's checked aggregate relations for 174 node-slots:
attempt/retry/response bounds, mutually exclusive transient/deferral totals, full six-page
deferrals, timing-page occupancy bounds, unresolved-node support, process-transition limits, and
the legal retained prefix of process events followed by at most one terminal event. The first 32
events have exact coordinate/order validation. Coordinates for dropped details are not present, so
their per-slot placement is not proven by aggregate counters; a later untrusted-producer contract
would need a compact full-slot vector or transcript. Here the fresh result is consumed only from
the manifest-pinned child under the fake harness boundary.

## Empirical verification

The local Windows matrix proves:

- C and D produce byte-identical base plans and 104-action traces through the same
  290-second logical end seal, while using distinct fresh invocation identities.
- C completes without a network connection; D completes with 348 parsed fake responses and exactly
  116 authenticated requests to each owned listener.
- Incomplete D is accepted only with canonical result plus exact exit code 2; wrong bearer,
  malformed output, injected exit, hang, stalled read, invalid/empty/partial driver input, and open
  or partial-secret observer bootstrap all reject within their bounds. Abrupt supervisor EOF
  cancels a stalled observer instead of allowing it to run to the whole-run deadline.
- Outcome and arm changes cannot alter common plan/trace bytes, action order/count, or end seal.
- Only `base-plan.json`, `driver-trace.json`, and `fake-protocol-run-record.json` are created; raw
  child output, both diagnostic test secrets, and an environment sentinel are absent from console
  and artifact bytes.
- Hostile driver/bootstrap matrices cover bad, zero, oversized, and truncated framing; schema,
  notice, eligibility, ordinal, address, UUID, secret, and terminator substitutions. Result
  mutations cover replay, unknown/oversized JSON, counter equations, event ordering, and page
  semantics.

Serial verification passes formatting, all-target check, warnings-denied all-target Clippy, and
`cargo test --all-targets -- --test-threads=1`: 38 tests pass and one parked-child fixture is
intentionally ignored and invoked by its parent. A post-test process inspection found no surviving
tool, test, driver, or observer process.

The two timeout wrappers necessarily abandon a thread blocked in a hostile partially supplied
stdin frame; portable synchronous `Read` offers no cancellation primitive. The executable then
exits, so the OS reclaims its partial stack scratch. Dedicated-process tests hold both a driver and
an observer pipe open through partial secret delivery and prove bounded process exit. This is not a
claim of explicit thread join/zeroization on the timeout branch and would be unacceptable in a
long-lived in-process secret reader.

## Independent review

Independent protocol and source passes initially returned `BLOCK` for weak result equations, replay
acceptance, a reachable-looking but impossible public kill disposition, late-success timeout
reclassification, non-exact incomplete exit handling, unbounded-looking cleanup ambiguity, and
partial-secret timeout coverage. Final passes also found noncanonical IPv6 scope/flow identities and
non-cancelling parent-pipe EOF. The implementation now uses a fresh invocation binding, exact
producer inequalities, canonical endpoints, fail-closed EOF, sticky deadline/status outcomes,
exit code 2 for incomplete, no unused fake kill disposition, a second cleanup attempt, hostile
frame/result matrices, and dedicated-process partial-secret tests. Final review approved the
fake-only component and retained the claim limits in this document. Live polling, the experiment,
runtime admission, and production remain blocked.

## Cycle review

- **AI slop — pass:** public outcomes are reachable and tested or represent source-reviewed I/O
  failures; the removed `killed_and_reaped` fake disposition no longer advertises an impossible
  schema branch. Result and bootstrap contracts were versioned for freshness.
- **Overengineering/hot path — pass:** the added code is confined to an unpublished, root-excluded
  engineering tool. It adds no async runtime, broker, generic HTTP stack, runtime hook, row-path
  branch, state access, checkpoint work, or production dependency.
- **Unused code — pass:** the fake command, ingress, bootstrap, result, classifications, cleanup,
  and record fields are consumed by the real driver/observer child matrix. Warnings-denied
  all-target Clippy passes.
- **Production readiness — NO-GO:** this is accelerated fake-server supervision. It does not honor
  `at_ns` or the server's rolling rate limit, attest fake ownership of loopback ports, close the
  between-poll old-process tail gap, secure multi-host diagnostics, measure latency, qualify
  TidesDB, implement distributed keyed state, widen delivery, or run the independent immutable
  release-binary soak.
- **Documentation — pass:** this review carries the component disposition; the ADR, validation
  report, plans, and soak charter receive concise reconciliations. No obsolete tracked research was
  found. Parked redb material remains decision provenance, and ignored `.claude/` memory is not
  project evidence.
- **Tests — pass for this component:** success and principal hostile process paths plus strict unit
  matrices pass. Termination/status/capture OS-error branches remain source-reviewed because Rust's
  concrete `Child` and pipe types have no safe deterministic fault injection. No test here can
  substitute for live paced A/B or the independent soak.

## Cycle 67 review plan

1. Freeze a separately versioned **design-only** live-paced observer contract; make no live request
   and do not reuse accelerated result v3 as live evidence.
2. Specify monotonic schedule anchoring, missed-slot behavior, jitter accounting, non-burst catch-up,
   the server's per-process rolling-start limit, total duration, cancellation, and bounded shutdown.
3. Decide the evidence model for the between-poll process-local timing tail: add durable handoff/
   continuity authority or formally bound and approve a weaker observation claim. Do not silently
   call the current ledger complete.
4. Separate co-located loopback engineering topology from multi-host transport. Freeze the latter's
   listener, authentication, encryption, rotation, and process/deployment identity requirements
   before implementation.
5. Define which paced outputs could feed non-feedback A/B, which remain diagnostic, and what sample
   size/power, clock, fault, warm-up, cooldown, invalid-run, and raw-evidence rules apply.
6. Repeat AI-slop, overengineering, unused-code, hot-path, production-readiness,
   overdocumentation, and test review. Keep live execution, backend qualification, admission,
   delivery claims, and production soak blocked.

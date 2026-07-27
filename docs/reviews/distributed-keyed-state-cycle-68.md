# Distributed keyed state Cycle 68 review

- **Date:** 2026-07-27
- **Scope:** paced owned-fake plan/start/clock/rate primitives only
- **Decision outcome:** approved in `1b6a06ed` as a partial, library-only fake boundary
- **Empirical outcome:** deterministic and hostile Windows tests only; no socket, LaminarDB process,
  workload, fault, latency sample, backend trial, or soak ran
- **Runtime/backend outcome:** no runtime workspace dependency, backend, route, listener,
  configuration, operator, checkpoint path, connector, admission rule, or guarantee changed
- **Production verdict:** **NO-GO**; `[LDB-4007]`, `[LDB-0013]`, evidence/result framing, executable
  paced lanes, release-process identity, live A/B, backend qualification, exactly-once composition,
  and the independent release-binary soak remain open

## Implemented boundary

The root-workspace-excluded `distributed-state-ab` tool now has an isolated
`paced_observer` module. It deliberately uses `paced-owned-fake-*` schemas and frame magic rather
than claiming the future live `paced-observer-*` or `diagnostic-request-response/v2` contracts.
Every serialized object remains `execution_eligible = false`, and the module has no API which can
open a socket.

The plan is authorized by external context, not by its own hashes. A trusted-side expectation binds
the sealed base plan, arm, fresh UUIDv4 invocation, exact observer schedule, three distinct fixture
descriptor identities, and server/configuration artifact digests. Only byte-canonical input
equal to that complete expectation becomes validated. Fixture READY rebinds that plan and process
set; it is only a fake-process protocol acknowledgement, not inherited-listener adoption or process
attestation.

START and ACK are exact fixed-size binary frames bound to invocation, plan digest, and a nonzero
128-bit start nonce. START parsing is private. The one public receipt transition validates the
entire frame and only then samples the injected monotonic clock; ACK can be constructed only from
that anchored state. This makes the required ordering representable without trusting a caller-
supplied anchor.

The clock and classification layer implements inclusive 50-ms start-ACK and release bounds,
absolute target-derived 4.5-second work and 4.75-second quiescence cuts, the 289.75-second final-lane
and 290-second result constants, cancellation, regression detection, and checked arithmetic. The
rolling shaper retains physical-start history across logical slots and atomically admits and records
a start. It therefore cannot pass a rate check and then write a socket after the absolute deadline.
Its trailing interval is exactly `(now - 1 second, now]`: seven starts are accepted and an eighth
waits until the oldest timestamp is on the excluded boundary.

## Review corrections

Three independent read-only reviews initially requested changes:

- plan parsing trusted self-declared base, arm, invocation, and descriptor bindings;
- a limits-only object incorrectly claimed the future request/response-v2 schema, fixture READY
  looked stronger than it was, and a caller could supply an arbitrary ACK anchor; and
- the split shaper check/record API had a deadline time-of-check/time-of-use gap, while canonical,
  frame, acknowledgement, deadline, and rolling-boundary tests were incomplete.

The final code adds the external exact expectation, renames every incomplete surface as owned-fake,
removes the premature result schema, couples complete START decode to anchor capture, requires an
anchored token for ACK, and replaces split shaping with atomic admission. Tests now include byte-
golden START/ACK frames; all truncation lengths; extension, magic, invocation, digest, nonce,
cross-plan, duplicate, unknown, noncanonical, replay, cap, regression, deadline, and overflow cases;
49/50/51-ms boundaries; seven simultaneous starts; exact one-second expiry; and cross-slot history.
All three reviewers then returned **APPROVE** for this partial boundary and explicitly excluded the
deferred executable/evidence work.

## Cycle review

- **AI slop — pass:** names and APIs distinguish a fake fixture acknowledgement from future live
  readiness. A structural hash, loopback address, descriptor re-hash, or monotonic sample is not
  relabelled as process attestation, complete evidence, or an experiment result.
- **Overengineering/hot path — pass:** one synchronous, dependency-free module was added outside the
  runtime workspace. There is no async runtime, generic scheduler, HTTP framework, state backend,
  checkpoint-path I/O, durable timing journal, cloud adapter, or production listener. Pacing code
  never enters LaminarDB's row or checkpoint hot path.
- **Unused code — pass for the staged boundary:** all public primitives are inputs to the next
  owned-fake framing/executor gates, and their behavior is directly tested. Nothing is wired into a
  command prematurely. The accelerated v2/v3 implementation is unchanged.
- **Production readiness — NO-GO:** there is no result/transcript validator, three-lane coordinator,
  delivery-stage transport, quarantine state machine, child/supervisor spool, actual-limiter run,
  290-second pair, inherited listener, diagnostic v2, mTLS transport, A/B, keyed runtime state, or
  independent soak.
- **Documentation — pass:** the normative Cycle 67 contract remains authoritative. ADR, report,
  plans, and charter record only this narrower implementation and preserve the later gates. No
  stale `docs/research` or `.claude` material is present.
- **Tests — pass for the bounded component:** 39 library tests, three active supervisor tests, and
  16 CLI tests pass on Windows (58 active total); one subprocess fixture remains intentionally
  ignored. Formatting, warnings-denied Clippy, diff checks, local Markdown links, and the research/
  memory audit pass. These tests are not a real-time, Linux, network, or product soak claim.

## Superseded Cycle 69 review plan

Owner direction on 2026-07-27 pauses this plan and all later soak, A/B, observer, transcript, and
certification-tooling work. The completed Cycle 68 code and commits remain preserved; none of the
items below is active work.

1. Add only domain-separated owned-fake evidence/result schemas; do not use the reserved live
   `paced-observer-result/v1` or `diagnostic-request-response/v2` names yet.
2. Implement a small sequenced, length-prefixed, hash-chained frame codec suitable for continuous
   supervisor spooling. Enforce per-frame, 128-MiB, 1,392-attempt, and 66,816-record bounds before
   allocation; require one header, contiguous sequence numbers, one seal, and exact EOF.
3. Validate a complete canonical 58-by-3 node-slot vector in `(slot,node)` order. Recompute all
   totals, require treatment attempt/transcript joins and zero control network evidence, and reject
   sparse, reordered, duplicate, truncated, or aggregate-only results.
4. Model process generations as sampled open prefixes or unsealed transitions, with numeric gap
   bounds only for generation seal, independent predecessor reap, or the measurement-window cut.
   Keep hidden completed-record count explicitly unknown and require full assignment-fence anchors.
5. Test every framing boundary, count/cap overflow, chain mutation, seal/trailing data, slot order,
   timing hidden tail, old-process survival, bounded/unbounded gap, and classification/totals
   tampering. Keep the module unable to contact LaminarDB.
6. Repeat AI-slop, overengineering, unused-code, hot-path, production-readiness,
   overdocumentation, and test review. HTTP extraction, lanes, supervisor IPC, the 290-second pair,
   live release preflight, backend, admission, delivery/exactly-once, and independent soak remain
   blocked.

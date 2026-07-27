# Distributed keyed state Cycle 65 review

- **Date:** 2026-07-27
- **Scope:** standalone observer bootstrap, direct loopback fake-server protocol, response state
  machine, completeness reporting, cancellation, and real-child tests
- **Implementation:** `46b9c3fd`, `b68515a8`, `1006c0ee`, `5fb9b213`, `f294effc`, `00fc419f`,
  `fe3693f8`, and `553c933d`
- **Empirical outcome:** owned loopback fake servers only; no LaminarDB request, common-driver A/B,
  workload, fault campaign, backend trial, latency measurement, or soak ran
- **Component verdict:** **APPROVE** the bounded standalone fake-protocol component
- **Live/production verdict:** **NO-GO**; the sealed driver does not consume this protocol, and
  `[LDB-4007]`, `[LDB-0013]`, backend qualification, multi-host diagnostic transport, powered
  equivalence, exact source/state/sink composition, and the independent immutable soak remain open

## Completed boundary

The root-workspace-excluded `tools/distributed-state-ab` observer now accepts canonical sanitized
plan v2 plus one canonical 32-byte diagnostic secret through self-delimiting stdin bootstrap v3.
The secret has no argument, general-environment, URL/query, plan, file, manifest, log, result, or
console fallback. Secret decode scratch and request storage are zeroized. On Windows the child
allows only a nonempty absolute `SystemRoot`, which the subprocess investigation found necessary for
Winsock after `env_clear`; all other environment names and values are rejected without disclosure.
Non-Windows remains empty-environment only.

D connects directly to three unique literal loopback `SocketAddr` values. It performs no DNS or
proxy discovery, refuses redirects and transfer encoding, constructs only the two exact origin-form
GETs, and uses one bearer header. Frozen connect, write, idle-read, request, node/slot, retry, page,
record, byte, header, event-retention, and whole-run bounds are serialized in the sanitized plan and
covered by a drift test. Strict DTOs and the raw HTTP parser reject unknown, malformed, truncated,
oversized, or inconsistent evidence.

Each node retains stable identity, every seen boot UUID, and per-node assignment/checkpoint
high-water marks across restart. Run-global bounded maps deliberately separate local owner-map/ABI
identity (digest plus vnode count) from timing's complete assignment-certificate digest; same-domain
conflicts across nodes fail while the expected cross-domain digest inequality is accepted. Timing
cursors are process-bound; new processes strictly advance assignment version, boot reuse and
version/checkpoint regression fail, and a restart fails when the last observed page advertised
unread old-generation evidence. A process-local record appended after that poll and lost before the
next poll is unknowable to this client; this component does not prove durable tail continuity.
Result v2 binds the exact canonical sanitized-plan hash. Exhausted collection returns explicit
`incomplete` disposition with aggregate transient, page-deferral, unresolved-node, and dropped-event
counters; the executable serializes it and then exits nonzero.

Bootstrap acquisition is bounded at two seconds by the executable wrapper and does not require
stdin EOF. A post-bootstrap stdin control frame cancels active work. Reads observe cancellation on a
50 ms poll; connect/write can defer it up to their 250 ms caps. Any complete same-length control
word cancels fail-closed, so the control word is not an authenticated command channel. The generic
framed-reader type itself has no clock.

## Empirical verification

The Windows subprocess matrix uses only owned loopback listeners:

- C completes while the supervisor pipe remains open, returns the expected sanitized-plan hash,
  and opens zero connections.
- D returns `complete` after 348 parsed responses: exactly 116 connections per endpoint and exact
  alternating local-evidence/timing request bytes.
- Two initial `503` responses create one exhausted logical probe without changing the total 348
  starts; result v2 is `incomplete`, is serialized, and the child exits nonzero.
- An open but empty supervisor pipe reaches the two-second bootstrap deadline.
- The public cancel frame interrupts the first stalled read before either other endpoint is used.
- An unsupported environment sentinel is rejected without its name or value appearing in output.

`cargo test --all-targets -- --test-threads=1` passes 17 library tests, two driver tests, and 12 CLI
tests; one parked-child fixture is intentionally ignored and invoked by its parent test. Formatting,
warnings-denied all-target Clippy, duplicate-dependency inspection, and diff hygiene pass. The root
workspace, LaminarDB crates, runtime dependencies, and data/checkpoint hot paths are unchanged.

## Independent review

The first independent passes returned `BLOCK`. They found EOF-dependent bootstrap, an unwired
cancellation token, no D subprocess, successful incomplete collection, capped events without a
loss counter, no sanitized-plan provenance, incomplete cross-response assignment history, reusable
older boot identities, an inaccurate failure marker, and unchanged wire versions. A later pass
also found unread-generation loss, assignment/checkpoint regressions across restart, conflated
owner-map and certificate digest domains, and missing cross-node identity reconciliation. Those
defects were corrected with deliberately unequal-domain fixtures and regressions. The final
independent review returned `APPROVE` for this bounded component with the claim limits below retained.

The sealed common driver still launches only the legacy `dry-run` observer. Its existing success,
exit, hang, and malformed-output non-feedback proof therefore does not cover `fake-protocol`.
Independent review correctly keeps that as a separate blocker to live authorization rather than
inflating this component result into an integrated observer claim.

The fake protocol accelerates all 58 slots without waiting for `at_ns`. It does not implement the
0..285-second wall-clock schedule and would not be compatible with the real server's eight-starts-
per-process/second limit. Live polling therefore also requires a separately versioned paced path;
consuming the current accelerated result is not live authorization.

Live polling also requires durable continuity/handoff evidence or an explicitly reviewed bounded
interpretation for the between-poll restart gap. The process-local timing ledger cannot tell the
client that an old process appended and then lost records after its last observed page.

## Cycle review

- **AI slop — pass:** every new result field, state transition, deadline, and subprocess assertion
  maps to a named failure found by review. Wire contracts were bumped instead of silently changing
  v1/v2 semantics.
- **Overengineering/hot path — pass:** one synchronous bounded client and explicit state machine
  are appropriate for a low-rate standalone observer. No async framework, generic HTTP client,
  broker, runtime hook, row path, or checkpoint-capture branch was added.
- **Unused code — pass:** public framing, cancellation, result, and plan types are consumed by the
  executable and subprocess tests. Test servers are owned, bounded, joined, and confined to the
  integration target.
- **Production readiness — NO-GO:** fake identity is operational rather than attested; the main
  diagnostic listener is loopback/plaintext; the consuming sealed supervisor, live preflight,
  paced polling, durable timing-tail continuity (or a bounded observation interpretation), powered
  A/B, backend, keyed operator runtime, exactly-once connector matrix, and independent soak are
  absent.
- **Documentation — pass:** this review summarizes disposition while the
  [soak charter](../testing/distributed-state-production-soak-charter.md#cycle-65-standalone-loopback-observer-protocol)
  owns numerical protocol detail. No obsolete tracked research was found. The parked redb record is
  retained decision provenance, not an active backend plan; ignored `.claude/` memory is not project
  evidence.
- **Tests — pass for this component:** hostile unit coverage plus six bounded real-child scenarios
  cover control, complete, incomplete, timeout, cancel, and environment paths. They do not contact
  LaminarDB or substitute for an independent immutable release-binary soak.

## Cycle 66 review plan

1. Add a fake-only consuming supervisor path to the sealed driver; do not authorize a live
   LaminarDB address or run an A/B.
2. Provision bootstrap v3 and the diagnostic secret without arguments, general environment, files,
   manifests, artifacts, or logs; preserve only the reviewed Windows `SystemRoot` exception.
3. Bind and validate result v2, including sanitized-plan hash and `complete` disposition, only after
   the exact common trace has been validated, file-synced, and end-sealed.
4. Prove success, early exit, hang, malformed output, incomplete result, bootstrap failure, and
   cancellation cannot change common C/D plan or trace bytes, action count/order, or end seal.
5. Bound stdout/stderr capture, bootstrap, cancellation, kill, wait, and reap; reject substitutions
   and stale/mismatched protocol identities without exposing authority.
6. Keep the accelerated protocol fake-only and freeze a separately versioned paced observer contract
   before any live request; do not infer rate-limit, 0..285-second schedule compatibility, or
   durable between-poll timing-tail continuity here.
7. Repeat AI-slop, overengineering, unused-code, hot-path, production-readiness,
   overdocumentation, and exhaustive-test review. Keep live polling blocked until independent
   review approves the consuming-supervisor proof.

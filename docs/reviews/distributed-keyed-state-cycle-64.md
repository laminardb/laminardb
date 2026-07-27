# Distributed keyed state Cycle 64 review

- **Date:** 2026-07-27
- **Scope:** diagnostic-read configuration, authorization/routing, availability bounds, parse-error
  redaction, and restart-only reload publication
- **Implementation:** `3a0d3b5c` (`security: isolate cluster diagnostic reads`) and `cf0f5aa4`
  (`security: redact malformed diagnostic queries`)
- **Empirical outcome:** in-process configuration/router/race tests only; no socket-level observer,
  real cluster, A/B, backend trial, workload, fault campaign, or soak ran
- **Production verdict:** **NO-GO**; `[LDB-4007]`, `[LDB-0013]`, backend qualification, multi-host
  diagnostic transport, powered instrumentation equivalence, source/state/sink atomicity, and the
  independent immutable release-binary soak remain open

## Completed boundary

Cycle 64 implements the Cycle 63 contract without adding an observer. A shared startup validator
enforces cluster mode, loopback bind, distinct console/diagnostic secrets, and canonical 32-byte
unpadded base64url credentials before programmatic startup side effects. The immutable HTTP policy
also snapshots cluster mode, avoiding a mutable-config read on each diagnostic request.

The two local evidence GETs now live on a separate router outside console CORS. One bearer header
authenticates either a console administrator or a private diagnostic-read principal. The narrower
credential is denied from every registered console, mutation, pipeline, reload, checkpoint, SQL,
and WebSocket route. Exact target/method tests reject duplicate, comma-joined, short, oversized,
query, cookie, absolute-form, alias, and non-GET substitutions. Single-node and feature-disabled
handlers retain ready-state `404`; startup/recovery/fence checks remain authoritative.

One non-queuing permit is shared by both routes, an eight-slot allocation-free rolling window counts
only authenticated handler starts, and a two-second timeout bounds each admitted future. Integrated
tests cover auth-before-accounting, cross-route contention, ninth-start rejection, success, timeout,
and cancellation release. Request logs use matched route templates or `<unmatched>` rather than raw
targets. None of these objects is referenced by a row, state, source, sink, or checkpoint-capture
path.

TOML parse errors detach substituted input before leaving `load_config`. Both POST and watcher reload
paths publish only the four supported live sections after complete DDL success. Pure restart-only
changes are ignored with warnings; mixed success updates only live sections; failed DDL publishes
neither the proposed live nor restart-only configuration. The active HTTP policy and retained
server configuration therefore remain aligned with the database's startup checkpoint-forwarding
credential.

## Verification

- `cargo test -p laminar-server --no-default-features --bin laminardb`: **238/238 passed**.
- `cargo test -p laminar-server --no-default-features --features cluster --bin laminardb`:
  **316/316 passed**.
- `cargo clippy -p laminar-server --no-default-features --bin laminardb -- -D warnings`: passed.
- `cargo clippy -p laminar-server --no-default-features --features cluster --bin laminardb -- -D
  warnings`: passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.

The tests prove the error sentinel absent from parse-error `Display`, `Debug`, source chains, and
the reload API body. The watcher invokes and logs that same redacted error object; this is source and
in-process formatting evidence, not an external log-collector test. A separate captured-log
regression proves an attacker-controlled timing-query field name is absent while the fixed rejection
event and route-template access event are emitted. No claim is made that the two-second cooperative
timeout can preempt a synchronously blocked executor thread.

## Independent review

The first independent pass returned `BLOCK` on four substantive gaps: cluster mode was still read
from mutable configuration, raw unmatched paths could carry logged credential material, both reload
entry paths lacked the required pure/mixed success/failure matrix, and limiter tests mostly exercised
primitives rather than middleware order. It also requested the complete route matrix. The final
implementation snapshots mode, logs route templates, extracts one directly testable watcher-change
boundary, and adds integrated route/reload/limiter coverage. The post-correction reviewer returned
`APPROVE` with later observer/transport/soak work kept separate.

## Cycle review

- **AI slop — pass:** implementation claims map to named fields, middleware, handlers, and passing
  tests. In-process request tests are not called a live sample or latency result.
- **Overengineering/hot path — pass:** one existing workspace base64 dependency, one small policy,
  one semaphore, and one fixed array implement the frozen boundary. There is no generic RBAC,
  proxy/broker, remote-state call, measurement hook, or data/checkpoint hot-path branch.
- **Unused code — pass:** both reload entry points use the publication helper; both diagnostic
  handlers consume the typed principal; old live-config credential reads and repeated handler token
  comparisons are gone. The watcher helper exists to process each production file event and is not
  a test-only production seam.
- **Production readiness — NO-GO:** transport remains plaintext loopback on the main HTTP/
  checkpoint-RPC listener, no observer exists, and no backend, distributed keyed operator,
  exactly-once connector composition, multi-host run, A/B, or independent soak changed status.
- **Documentation — pass:** the soak charter remains the normative detailed contract; ADR, plans,
  and validation report carry only concise disposition summaries. `.claude/` is ignored local
  tooling/memory rather than tracked project evidence and was preserved. The parked redb protocol is
  linked historical decision provenance explaining its rejection, not an active candidate plan;
  no obsolete tracked research document was identified for deletion.
- **Tests — pass for this boundary:** complete server feature matrices, warnings-denied Clippy,
  formatting, diff hygiene, and relative-link validation pass. The independent immutable soak is a
  later mandatory gate and has not run.

## Cycle 65 review plan

1. **Scope:** build only the standalone observer's loopback fake-server protocol; make no LaminarDB
   request and run no A/B.
2. **Authority:** accept one typed diagnostic secret source from the supervisor, never the console
   secret, URL/query/arguments/environment, or an observer fallback.
3. **Network safety:** pin one loopback origin, use exact origin-form GETs, disable redirects and
   proxy discovery, bound DNS/connect/read/total time, retries, pages, bytes, and retained errors.
4. **State machine:** validate both response schemas, process identity and timing cursors, restart/
   eviction transitions, malformed/truncated/oversized bodies, cancellation, and deterministic exit.
5. **Non-interference:** prove arm C opens zero connections and observer success, exit, hang, or
   malformed output cannot change the already sealed common schedule.
6. **Review:** repeat AI-slop, overengineering, unused-code, hot-path, production-readiness,
   overdocumentation, and exhaustive-test checks before any live request is authorized.

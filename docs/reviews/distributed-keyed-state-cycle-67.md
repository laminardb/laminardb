# Distributed keyed state Cycle 67 review

- **Date:** 2026-07-27
- **Scope:** paced observer schedule, server-rate compatibility, process-local timing coverage, and
  single-host versus multi-host diagnostic identity
- **Decision outcome:** accepted in `97254753` as documentation only; no executable contract is
  authorized
- **Empirical outcome:** source inspection and independent design review only; no HTTP request,
  LaminarDB process, workload, fault, A/B sample, backend trial, or soak ran
- **Runtime/backend outcome:** no crate, dependency, route, listener, configuration, checkpoint path,
  state backend, admission rule, connector, or delivery guarantee changed
- **Production verdict:** **NO-GO**; `[LDB-4007]`, `[LDB-0013]`, backend qualification, live paced
  polling, powered equivalence, multi-host diagnostic transport, exact restart-spanning timing, and
  the independent immutable release-binary soak remain open

## Accepted decision

The accelerated fake protocol cannot be made live by adding sleeps. Its three nodes execute
serially, while each node can consume 4.5 seconds of a five-second slot. Its eight-start cap is per
node-slot, but the server enforces one rolling one-second history per process across all slots and
both diagnostic routes. Late work from one slot can therefore collide with the next. Fake result v3
also retains only a 32-event prefix and cannot support independently auditable live evidence.

The separately versioned paced v1 design uses a driver monotonic `t0`, a bounded start
acknowledgement, 58 absolute five-second targets, and three persistent lanes bound to distinct
verified server processes. Each lane ends at its target plus 4.5 seconds and must quiesce or force
observer termination by plus 4.75 seconds. Late slots are skipped, never caught up; slot release and
first socket start are distinct evidence. A cross-slot per-process history admits a proposed client
start only when no more than seven starts would occupy its trailing second. Requests are sequential
only after a complete terminal response. Ambiguous post-delivery I/O quarantines that process lane
for the rest of the run, and paced v1 never retries an unclassified `429`.

The result has three independent decisions: protocol completion, timing coverage, and A/B attempt
classification. It contains all 174 node-slot entries and a sealed content-addressed transcript,
bounded to 1 MiB of canonical result JSON, 128 MiB of transcript, 1,392 attempt rows, and 66,816
timing records. Aggregates are derived checks, not a replacement for slot/attempt coordinates.
Only Cycle 60's external metrics and correctness outcomes may feed `D - C`; exact timing records
remain arm-specific diagnostics, and eight-block v1 remains effect-estimation-only.

## Timing evidence boundary

The in-memory ledger proves a mutex-coherent, loss-reporting prefix through one sampled process cut.
It does not prove a generation tail. After the last old-process page, a record or loss update can be
added, or an already-created timing guard can remain in flight. That guard writes on drop without
revalidating current assignment/process authority, so a successor response does not prove the
predecessor stopped writing. `durable_tail_handoff` means state capture reached checkpoint-tail
work; it is not timing-ledger persistence.

Each generation is consequently `sampled_open_prefix`, `unsealed_transition`, or—only after a
future real authority—`authoritatively_sealed`. A numeric unobserved-tail interval may end only at
an authoritative generation seal, independently timestamped predecessor termination/reap, or the
fixed measurement cut for claims limited to that window. Otherwise it is unknown. Hidden record
count is always unknown today. Coverage distinguishes observed prefixes with bounded gaps,
unbounded gaps, and invalid evidence. Every assignment referenced by an accepted record or
assignment-specific conclusion must resolve to the exact audited canonical assignment fence.

The maximum honest claim is: **no SLO violation was found in captured, completed barrier
observations through the reported process-local cursors**. It is not a statement about every
attempt, the killed generation's tail, or the complete window.

No timing persistence is added now. Exact abrupt-restart coverage, if required, needs a separate
fenced journal with a durable intent before an observation can become invisible, immutable terminal
records, and an authoritative generation seal. Periodic/graceful flush or worker-local Fjall/
TidesDB state cannot prove node-loss continuity. Synchronous intent durability is checkpoint-
control-path I/O and needs an explicit outage policy, latency A/B through p99.9, crash-boundary and
stale-writer tests, and independent soak approval.

## Transport boundary

Co-located engineering may use literal loopback only after a future launcher pre-binds and passes
the listener handle to each manifest-pinned child, retains its process identity, and receives a
post-serving descriptor plus nonce-bound v2 responses. The current server binds its own listener,
so this seam does not exist yet. The association trusts the launcher, pinned child, same-user host,
and kernel; it is not
cryptographic attestation. Observer and server must share the exact network namespace. Published,
bridged, NATed, forwarded, mirrored, proxied, or port-forwarded Docker/WSL localhost is ineligible;
native Linux host networking may qualify only after namespace verification.

Multi-host use requires a separate two-route diagnostics listener and independent restart-bound
auth configuration. Direct TLS 1.3 mTLS uses no 0-RTT or session resumption in v1. A generation-
specific node leaf has `serverAuth`, exact dial-target DNS/IP SAN, and one deployment/run/node/boot/
term URI identity; the observer leaf has `clientAuth` and one deployment/run/observer URI. A
pre-`t0` pinned run-controller key signs an exact-version, predecessor-hash-linked roster. Rollback,
fork, duplicate/skip, wrong signer, address reuse, shared/wildcard identity, and predecessor leaf or
session all fail closed. The current shared-name cluster mTLS does not terminate Axum diagnostics
and is not this evidence.

Both profiles require capped `diagnostic-request-response/v2` headers/envelopes which bind and echo
deployment, run, invocation, slot, route, nonce, and process identity. Current v1 bodies cannot do
that and are ineligible. Trust material enters through a provider-neutral in-memory identity
interface, with file-backed PEM/PKCS#8 as baseline and an optional non-exporting SPIFFE adapter.
Cloud/Kubernetes/service-mesh adapters are optional; S3, TidesDB object storage, and LaminarDB's
checkpoint `object_store` providers are not identity authorities.

## Independent review

Three independent read-only passes initially returned `BLOCK`:

- the protocol audit found cross-slot limiter collision, unsafe retry after ambiguous delivery,
  server/process gate aliasing, an overclaim that client cancellation ended the server handler,
  unclassified `429`, and missing exact accounting/result fields;
- the timing audit found that successor evidence does not end predecessor writes, assignment
  anchoring was vague, unbounded gaps were missing, and the transcript had no numerical cap; and
- the transport audit rejected child-reported port ownership, blanket Docker/WSL claims, one leaf
  across process restart, use of current v1 response identity, underspecified roster rollback/fork
  rules, and a file-only reading of SPIFFE.

The final contract adds terminal-response-or-quarantine semantics, exact request ceilings and
classifications, process-bound pacing, bounded full evidence, neutral unsealed transitions with
authoritative gap endpoints, exact assignment-fence anchoring, launcher-prebound inherited sockets,
network-namespace precision, a distinct capped v2 envelope, generation leaves, hash-chained signed
rosters, and a provider-neutral in-memory identity interface. Final reviewers approved those
substantive corrections; their only remaining block before this file was added was this previously
missing review target.

## Cycle review

- **AI slop — pass:** current source behavior, planned behavior, and empirical evidence are kept
  separate. Successor authority, loopback reachability, a child descriptor, `durable_tail_handoff`,
  current cluster mTLS, and an aggregate counter are not relabelled as stronger proofs.
- **Overengineering/hot path — pass:** the immediate choice is an observed-prefix interpretation and
  an isolated engineering-tool design. No row/state/source/sink path, checkpoint persistence,
  generic RBAC, service mesh, cloud IAM, object-store identity, or runtime backend is added. The
  durable journal is deferred behind an explicit need/latency decision.
- **Unused code — pass:** this cycle adds no code. Every planned schema, lane, transcript, listener,
  roster, and test belongs to a named later gate; no speculative runtime abstraction landed.
- **Production readiness — NO-GO:** no paced implementation, inherited listener, diagnostic v2,
  mTLS listener, live A/B, backend qualification, distributed keyed lifecycle, source/state/sink
  atomicity, exactly-once certification, or independent soak exists.
- **Documentation — pass:** the soak charter is normative; ADR/report/plans carry concise status
  reconciliations. `git ls-files` and the untracked-file audit find no project `docs/research` or
  `.claude` material to remove. This review is the linked cycle disposition rather than a second
  normative protocol copy.
- **Tests — pass for a design-only cycle:** source audits cover the observer loop, server gate,
  timing ledger/guard, HTTP authority, listener, and cluster TLS boundary. `git diff --check`, root
  formatting, and 110 local Markdown targets across six changed/new documents pass; tracked and
  untracked research/memory counts are zero. Cargo tests are not rerun because no Rust, manifest,
  lockfile, configuration, or executable fixture changed.

## Cycle 68 review plan

1. Implement only paced owned-fake contracts and keep `execution_eligible = false`; do not contact a
   LaminarDB process or add the inherited production-listener seam yet.
2. Add a small injectable monotonic clock, absolute slot coordinator, three persistent process
   lanes, server-terminal-or-quarantine state, and cross-slot seven-start shaper. Avoid a generic
   scheduler, HTTP framework, or async runtime unless source evidence makes one necessary.
3. Emit the complete capped slot vector and append-only transcript through a supervisor-created
   typed handle. Recompute every total, bind the diagnostic contract, and reject truncation,
   ambiguous delivery reuse, unclassified-429 retry, process aliasing, or schema replay.
4. Model timing generations as open/unsealed prefixes with authoritative/unknown gap endpoints and
   exact assignment anchors. Test hidden record/loss/guard tails, old-process survival after
   successor evidence, multiple transitions, unbounded gaps, and final-window cuts.
5. Use deterministic-clock boundary tests for 49/50/51-ms release, rolling-window expiry, absolute
   4.5/4.75-second cuts, rate deferral, no catch-up/overlap, cancellation, and finalization. Add
   owned-server ambiguous-I/O and actual-limiter tests, then one manual real-time 290-second C/D pair
   before accepting the 250-ms post-end grace.
6. Repeat AI-slop, overengineering, unused-code, hot-path, production-readiness,
   overdocumentation, and test review. Keep live release-process preflight, multi-host transport,
   A/B, backend, admission, delivery, exactly-once, and independent soak blocked.

# Distributed keyed state Cycle 63 review

- **Date:** 2026-07-27
- **Scope:** diagnostic-read authority, transport/reload threat model, and implementation bounds
- **Decision outcome:** accepted in `8793e54a`; documentation only
- **Empirical outcome:** no SUT, HTTP request, A/B, backend trial, cluster run, or soak ran
- **Runtime/backend outcome:** no crate, route, credential, state backend, admission rule, connector,
  checkpoint path, or delivery guarantee changed
- **Production verdict:** **NO-GO**; `[LDB-4007]`, `[LDB-0013]`, backend qualification, complete
  checkpoint authority, powered instrumentation equivalence, exactly-once composition, multi-host
  diagnostic transport, and the independent immutable release-binary soak remain open

## Accepted decision

The current console bearer is not least authority for an observer: one protected router accepts it
for the two local evidence GETs and for checkpoint, SQL, reload, pipeline start/stop, other console
reads, and WebSockets. A hashed broker would still possess that unrestricted credential; binary
identity cannot constrain what an exploited broker sends to the same origin. Cycle 63 therefore
selects a server-enforced `server.diagnostic_read_token` and rejects the broker.

The diagnostic value is a startup-bound canonical 43-character unpadded base64url encoding of 32
bytes. Enabling it also requires a distinct console token with the same strength, cluster mode, and
a loopback HTTP bind. The diagnostic token is valid only as one bearer header on exact origin-form
`GET` requests to `local-evidence` and `local-checkpoint-barrier-timings`. It is invalid for every
console/mutation route, query-token/cookie/WebSocket authentication, method substitution, alias, or
unknown path. The console bearer remains the administrator superset on both GETs for compatibility;
least authority depends on the later supervisor having only a typed diagnostic-secret source and no
console fallback.

The loopback constraint is deliberately not called production transport. LaminarDB advertises the
main HTTP port for inter-node checkpoint RPC, so this v1 can run only a co-located single-host
engineering cluster. Multi-host A/B and the production soak still require a separately reviewed
local diagnostic listener or native TLS/mTLS.

The selected route also has one shared non-queuing permit, an allocation-free eight-starts-per-
rolling-second bound, and a two-second server deadline. These controls execute only after
authentication and only on the diagnostic router. They are fixed control-plane bounds, not a
generic policy/rate-limit framework, and add nothing to row/state/checkpoint-capture/source/sink hot
paths.

## Prerequisites found by source audit

Two existing behaviors must be fixed in the implementation cycle:

1. `[server]` changes are labelled restart-only, but explicit reload republishes the whole new
   configuration and the watcher does likewise when a reloadable DDL change is present. This can
   rotate or remove HTTP auth while checkpoint forwarding retains its startup console token.
2. `Secret` redacts only after successful deserialization. A TOML parse error currently retains
   substituted source input, and reload logs and returns that error, so a malformed secret line can
   disclose its value.

The approved fix removes TOML input from parse errors, commits only the four reloadable named
sections after successful reload, and snapshots one immutable auth policy at startup. File loading
and programmatic `run_server` both invoke the same auth validator before side effects. Rotation has
no grace overlap and requires restart. Immutable policy also removes the old rationale for repeated
post-capture token comparison: a private typed principal authenticates once, while final serving and
process-fence checks remain.

## Independent review

Two independent reviewers initially blocked the draft on unbounded token/header work, absence of
availability controls, ambiguous absolute-form routing, a console-compatibility tradeoff, missing
programmatic validation, secret-leak/reload test coverage, and an incorrect implication that
loopback could support ordinary multi-host clusters. The final revision freezes canonical token
size/strength for both credentials, pre-comparison size rejection, route-local concurrency/rate/
deadline bounds, origin-form enforcement, console-as-admin plus typed observer provenance, both
reload paths, full parse-error surfaces, and the single-host limit. Both reviewers then returned
`APPROVE`.

## Cycle review

- **AI slop — pass:** every current-router, token, reload, parse-error, and advertised-RPC claim was
  checked against source. Planned behavior is labelled future behavior; no request declaration is
  called an observation, A/B sample, or production result.
- **Overengineering and hot path — pass:** the decision selects one token and a split router, not
  generic RBAC, a proxy, token service, remote-state mechanism, or live client. The small permit,
  fixed rate window, and deadline are required availability bounds and remain off data/checkpoint
  hot paths. A second listener and TLS are deferred because this cycle runs nothing multi-host.
- **Unused code — pass:** this cycle adds no code or configuration field. Every normative matrix row
  is assigned to the next implementation tests or a named later transport/client gate.
- **Production readiness — NO-GO:** v1 transport is single-host, no observer exists, and no A/B,
  source/state/sink atomicity, exactly-once, backend, failover, memory, skew, latency, or restore
  evidence changed. The independent immutable release-binary soak remains mandatory and unrun.
- **Documentation — pass:** the soak charter owns the detailed contract; ADR, phase plans, and
  validation report link to it. A relative-link check passed 104 targets across five changed files.
  `git ls-files -- docs/research .claude` returned no tracked research or Claude-memory artifact, so
  there was no obsolete tracked research to remove.
- **Tests — pass for a design-only cycle:** `git diff --check` and the relative-link check passed.
  No Cargo suite was run because no executable source, manifest, lockfile, or runtime configuration
  changed. Source-grep evidence covered the router, handler rechecks, reload commits, parse path,
  listener, advertised RPC address, and startup checkpoint-forwarding token.

## Cycle 64 review plan

1. **AI slop:** implement the exact field, canonical decoder, shared startup validator, typed
   principal, two-route allowlist, and status precedence; invent no generic permission vocabulary.
2. **Overengineering/hot path:** use one small immutable policy and fixed route-local limiter; prove
   no row/state/checkpoint-capture/source/sink dependency or measurement hook is introduced.
3. **Unused code:** replace the old current-config token reads and repeated handler comparisons;
   retain no parallel auth path, unused token format, or test-only production seam.
4. **Production readiness:** keep the observer and all live runs blocked; preserve console admin
   compatibility, single/no-feature `404`, startup/recovery/fence precedence, bounded/no-store
   responses, and the explicit single-host-only limitation.
5. **Documentation:** reconcile names/statuses with the implementation once, record any necessary
   deviation, and do not duplicate the full matrix outside the charter/review.
6. **Tests:** cover canonical/weak/equal/missing/non-loopback/programmatic configuration; parse
   sentinel redaction; pure and mixed explicit/watcher reload success/failure; both credentials;
   duplicate/oversize/query/cookie/absolute/method/alias/unknown/CORS requests; zero mutation side
   effects; immutable policy; permit/rate/deadline success, rejection, timeout, cancellation, and
   release; cluster and no-cluster feature matrices; warnings-denied Clippy, formatting, links, and
   diff checks.

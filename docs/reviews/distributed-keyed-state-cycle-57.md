# Distributed keyed state Cycle 57 review

- **Date:** 2026-07-26
- **Scope:** the stable-serving local-assignment evidence portion of Phase 0 delivery item 6
- **Code outcome:** accepted for the existing engineering harness; core, HTTP, consumer, and
  post-read authority-revalidation changes are committed as `175a208e`, `8b44575d`, `fafc6e6c`,
  and `b87b5322`
- **Empirical outcome:** the new kill/rejoin evidence assertions passed twice, but both complete
  Windows/WSL2 engineering runs failed an existing latency-profile terminal gate
- **Runtime/backend outcome:** no keyed runtime, state backend dependency, TidesDB execution,
  admission, source/sink contract, or delivery mode changed
- **Production verdict:** **NO-GO**; `[LDB-4007]` and `[LDB-0013]` remain unchanged, and no
  independent release-binary soak has run

## Exact accepted contract

Authenticated `GET /api/v1/cluster/local-evidence` returns schema
`laminardb-local-authority-evidence/v1` with exactly three evidence fields:
`participant`, nonzero `process_term`, and concrete `adopted_assignment`. A successful response is
at most 4096 bytes, has `Cache-Control: no-store`, and means all of the following were true during
capture:

- the process lease was sampled live before and after one bounded checked-KV operation;
- the lease-bound node, boot incarnation, and process term did not change;
- the local stable-node slot contained a 1..=1024-byte canonical adoption from this boot; and
- that adoption matched the same canonical, locally audited assignment fence on both samples.

Missing adoption, prior-boot adoption, a cleared local fence, or different-version evidence is
unavailable (`503`). A logical value
successfully returned by checked storage that is malformed, non-canonical, oversized, or conflicts
with a same-version local fence is invalid (`500`). The current `ObjectStoreClusterKv` error type
cannot distinguish a malformed outer control envelope from I/O failure, so either is conservatively
`503`; this is fail-closed but diagnostically weaker than the independent soak eventually needs.

The route remains behind normal startup, coordinated-recovery, and terminal process-lease serving
gates. It additionally requires a configured console token, accepts only the bearer header, and
samples the current token both before capture and immediately before `200`. Responses contain no
raw object-store path, owner vector, storage error, or secret.

Cycle 57 deliberately removed two initially proposed fields during review. `process_lease_live`
could only be true on success and conveyed no information. The retained successful-`Start`
acknowledgement is historical and can remain unchanged across a later recovery; exposing it would
have added another checked-KV operation and large-round hashing without proving current phase or
committed-`Release` consumption. No substitute recovery state was invented.

## Engineering consumer

The existing three-node real-binary harness now:

1. reads the durable assignment from one live node;
2. obtains authenticated local evidence from every expected live owner;
3. rereads the durable assignment and retries if the two complete snapshots differ or are draining;
4. rejects local versions ahead of the durable head and same-version digest/roster conflicts, while
   treating a trailing local version as pending; and
5. requires the exact local participant set to equal the durable fence roster.

Across hard kill/rejoin it also requires a strictly newer survivor assignment with the killed
participant removed, followed by another strictly newer assignment binding the same stable node to
a different boot, a higher process term, and at least one vnode.

The raw client caps headers at 16 KiB, local bodies at 4 KiB, and durable assignment bodies at 4
MiB. Connect, write, and every 8-KiB receive operation recompute the same absolute recovery
deadline. Polling is 500 ms. A `503` is pending only until that deadline; invalid framing, schema,
identity, or content is immediately contradictory. These rules prevent an unavailable or slow
endpoint from producing a pass.

## Empirical results

The subject was a Windows optimized soak-profile test binary talking to Docker Desktop 4.83.0's
WSL2 Linux engine (Docker 29.6.2), MinIO `RELEASE.2024-11-07T00-52-20Z`, and Redpanda v26.1.13.
Both runs used static discovery, default 64 key groups, default 96 Kafka input partitions, a 400-rps
producer, shared S3-compatible checkpoint/state prefixes, and one in-checkpoint leader hard kill.
This is not the independent release subject in the production-soak charter.

| Check | Result | Boundary/evidence |
|---|---:|---|
| Core local-authority tests | PASS, 5/5 | Exact schema/round trip, cleared and conflicting live fence, discriminating prior-boot case, malformed/oversized logical value, and lease loss during the durable read |
| Core warnings-denied Clippy | PASS | `laminar-core`, cluster tests |
| HTTP local-evidence tests | PASS, 6/6 | Configured/current bearer, query rejection, exact bounded/no-store envelope, live-fence suspension, missing/prior-boot/malformed evidence, and serving gates |
| HTTP non-cluster case | PASS, 1/1 | Route remains 404 without cluster support |
| Server warnings-denied Clippy | PASS | Cluster and no-cluster test matrices |
| Exact-cut classifier | PASS, 1/1 | Stable success; changing/draining head, trailing/missing local evidence, ahead/conflicting adoption, and duplicate process identity branches |
| Harness warnings-denied Clippy | PASS | `cluster,kafka` integration-test target |
| Real run r1: `kills=1`, `seconds=0` | **FAIL terminal profile gate** | New assertions passed: survivor exact convergence in 44.95 s and rejoin exact convergence in 34.21 s; all 43,473 source IDs reached the ALO sink, with 2,303 duplicates. The run then correctly rejected only 72 node0 latency samples versus the existing minimum 100 |
| Real run r2: `kills=1`, `seconds=90` | **FAIL terminal profile gate** | New assertions again passed: survivor exact convergence in 43.46 s and rejoin exact convergence in 37.53 s; all 80,260 source IDs reached the ALO sink, with 2,611 duplicates. Node1 met the 1024-ms stall bound for 166/168 observations (98.81%), below the required 99.00% |

The second failure is retained as latency evidence, not repeatedly rerun in hope that random
sampling dilutes it. The
current aggregate histograms cannot identify the exact two violating attempts or correlate their
stages with endpoint polling, assignment rotation, checkpoint identity, and terminal disposition.
That diagnostic gap was already a Phase 0 blocker and is now empirically material. Neither run is a
green engineering soak, and neither changes the production verdict.

The 98.81% miss has no demonstrated cause, but the new oracle cannot be declared latency-neutral.
During convergence it polls each local endpoint plus two durable-assignment views every 500 ms.
Each local logical checked-KV operation can expand into several MinIO metadata/object requests, and
the unthrottled route permits concurrent probes. That shared control-plane load is a plausible
measurement perturbation until exact attempt/stage timing or a controlled A/B run can separate it.

The Kafka sink remained explicitly at-least-once. Duplicate counts are expected diagnostics, not
exactly-once failures, and no transaction composed source position, state/checkpoint authority, and
sink publication.

## AI-slop review

**Pass.** The final change has one fixed schema, one controller method, one protected route, and one
existing-harness consumer. Review removed a generic partial-evidence shape, a tautological field,
and weak recovery history. No event framework, duplicate lifecycle state, disconnected allocator,
backend trait, or speculative recovery phase remains.

## Overengineering and hot-path review

**Pass for implementation scope; latency effect unresolved.** The product path performs one bounded
logical checked-KV operation only when this diagnostic route is called. The object-store
implementation may issue several physical metadata and object requests while validating the
lease-bound control envelope. It adds no row, shuffle, state mutation, checkpoint, rebalance, or
sink code, but shared object-store/control-plane contention can still affect those paths. The larger
harness diff is the bounded HTTP parser and exact sandwich oracle, both consumed by the real process
test. The built-in listener has no route rate limit, so this endpoint is not high-frequency
monitoring: use loopback/a trusted network or TLS ingress and a bounded polling cadence.

## Unused-code review

**Pass.** All three response fields are checked by the HTTP layer and harness. Both error variants,
the response bound, auth failures, serving gates, stale/missing paths, and classifier branches have
focused coverage. No dependency, lockfile, backend, feature flag, admission branch, or unused
recovery field was added.

## Production-readiness review

**NO-GO.** A successful response is bearer-protected, but startup/serving middleware can return a
state-specific `503` before authentication; it reads no evidence, and readiness state is already
public. The built-in HTTP listener is plaintext and unthrottled. The outer-envelope/I/O error
distinction is unavailable. The 1-KiB logical adoption limit is applied after the storage read; the
adapter permits a 1-MiB control value in an envelope capped at about 6 MiB. The default 64-vnode
fixture assigned work
to all three processes in both runs, but `LAMINAR_SOAK_KEY_GROUPS >= 3` alone cannot mathematically
guarantee that under rendezvous hashing; a low/unlucky override can time out rather than satisfy this
oracle. There is no deterministic injected test for a fence change specifically between the two
local fence samples or for a token reload during the checked read, although the implementation
resamples both and static suspension/mismatch/authentication cases are covered. The raw harness HTTP
reader also lacks direct slow-drip, exact-cap, oversized-header/body, and malformed-framing tests.

More importantly, the real subject failed its existing latency terminal twice and was neither an
independent controller nor an immutable release artifact. TidesDB remains stopped before execution;
there is no qualified hot-state backend, distributed keyed restore/rebalance, transactional writer
interval, source/state/sink atomicity, exact checkpoint-attempt evidence, production cloud matrix,
or independent soak. Admission and delivery remain fail-closed.

## Documentation and research review

**Pass with repetition debt.** ADR-008 owns the contract; the validation report and this review own
current evidence; the plans own sequencing; the soak charter owns certification. Endpoint mechanics
and the red-run disposition are repeated across those documents more than ideal, although their
boundaries agree. Do not add another summary in Cycle 58; plans and the charter should link to the
normative ADR/evidence record when they next change. No tracked `docs/research` file exists, the
ignored research junction is empty, and the obsolete private files named by the original Claude
material are absent. Existing backend reports remain reverse-linked decision/regression provenance,
so none was deleted merely for preferring a different candidate.

## Test review

**Pass for the bounded implementation; RED for the complete engineering run.** Unit, route,
classifier, formatting, diff, and warnings-denied lint gates pass. The exact local convergence and
kill/rejoin assertions passed twice against real processes, Kafka, and MinIO. The encompassing soak
did not pass, first for insufficient samples and then for a real 98.81%-versus-99.00% latency miss.
No production, independent-soak, exactly-once, backend, or keyed-operator test is claimed.

## Cycle 58 review plan

Use the latency miss to take the next smallest authority-first slice: audit and freeze a bounded,
versioned checkpoint-attempt outcome/capsule and exact stage-timing projection before implementing
another endpoint. If exact attempt identity, terminal disposition, or stage maxima are not retained,
stop at the authority gap rather than reconstructing them from logs or histograms.

1. **AI slop:** reject a generic event/log API; freeze only fields with named durable or exact
   in-process authority.
2. **Overengineering/hot path:** add no synchronous row/checkpoint work merely for observability;
   measure retention and read costs before choosing history bounds.
3. **Unused code:** require the existing engineering controller to consume every exposed field in
   the same cycle.
4. **Production readiness:** preserve both latency failures, source/sink delivery limits,
   `[LDB-4007]`, `[LDB-0013]`, backend qualification, and independent-soak gates.
5. **Documentation:** keep one normative schema and distinguish immutable outcomes/capsules from
   aggregate metrics and human logs.
6. **Tests:** cover missing/malformed/oversized authority, terminal outcome conflicts, exact attempt
   correlation, retention bounds, deadline exhaustion, auth/serving gates, and a real consumer.

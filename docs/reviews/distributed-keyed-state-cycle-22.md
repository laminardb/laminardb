# Distributed keyed state Cycle 22 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle commits:** `14ed0e02`, `083fc256`, `a5469ec8`
- **Cycle outcome:** `REDB_PRE_RUN_CONTENT_VALIDATED_AUTHORITY_UNVERIFIED`
- **Bounded memory:** reference/conformance-only; no current product or soak profile
- **Maintenance-health v2:** direction approved; exact runner-v2 contract and implementation remain gated
- **Current product target:** local spill; backend not selected
- **Candidate construction or execution authorized:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**

## Outcome

Cycle 22 implements the smallest validation-only successor from the
[redb 4.1.0 prescreen protocol](../testing/state-backend-redb-prescreen-v1.md). Three closed Draft
2020-12 schemas and a bytes-only semantic validator now check the protected-review policy, approval
payload and pre-run protected-review receipt. The validator enforces exact cross-document byte
length/SHA-256 bindings, two ordered role records, the exact 28 artifact role/locator/media tuples,
the native-only prior-smoke tuple, the exact redb 4.1.0 archive pin, per-role and aggregate declared
size caps, canonical scalar domains, and the prescreen-only scope with every eligibility field
false.

Success remains deliberately ineligible. The public authority type has only `Unverified`, and both
`execution_authorized()` and `result_sealing_authorized()` return false unconditionally. The sole
redb-prescreen CLI command reports
`VALID_INELIGIBLE_REDB_PRESCREEN_CONTENT ... authorization=authorization_unverified`. Unknown
commands, including a synthetic `run-redb-prescreen`, return usage status 64. No disposition or
`DEFER` vocabulary is available through this API.

The API accepts three exact byte strings. It is not a formal packet reader: it does not open any of
the 28 nominated descriptor targets, follow locators, query GitHub, resolve group membership,
authenticate reviews, dispatch a workflow, start a process, load redb, classify a candidate, or
seal retained evidence. The checked-in fixture is synthetic copied content and proves only the
validator's fail-closed behavior.

The repository's current branch protection requires one CODEOWNERS approval. GitHub protected
environments also advance when any one listed required reviewer approves. Neither mechanism alone
proves the protocol's two distinct role memberships and review events. A future default-branch
trusted dispatcher must export and verify those live provider facts over the exact head and payload
bytes; local JSON can never substitute for that step.

The protocol's final SHA-256 is
`3dcdd1887c6039045d43743e2eef87acfc3d7e541e3bfe9df1cf8805c8fe49f2`.

## Six-pass cycle review

### 1. AI slop and contract consistency

**Pass after independent correction.** Independent reviews caught masked and nonspecific semantic
tests. Descriptor-order and exact-source mutations had retained stale receipt hashes, so they could
leave a generic `is_err()` assertion green from the unrelated digest error even if the intended
semantic enforcement regressed; those tests now regenerate the receipt and assert the specific
semantic failure. Role-cap tests also assert their semantic failures, and the
base-4g cap-plus-one case can now pass the generic schema ceiling to reach the exact 12-GiB role
limit. Schema-valid post-run content reaches the explicit pre-run-only guard, and all three JSON byte
caps are exercised with valid content at the inclusive cap and one byte above it.

The review also removed an unused public `run_class` result field and added a direct native
prior-smoke 256-MiB cap-plus-one assertion. The approval schema's generic descriptor ceiling is 16
GiB only so the 12-GiB fixture-role cap can be reached by semantic validation; fixed tuple-specific
caps and the checked 20-GiB aggregate still reject wider content. Governance, protocol and
code-quality re-reviews returned PASS.

The protocol keeps four states distinct: content-valid, provider-authenticated,
execution-authorized and result-sealed. No field, output or local success path aliases one into the
next. `APPROVE_MAINTENANCE_HEALTH_V2_DIRECTION` approves that separate design direction only; it
does not instantiate a runner-v2 identity, parser, workflow, candidate adapter or execution path.

### 2. Overengineering, hot path and latency

**Pass for control-plane validation scope.** The implementation adds three schemas, one semantic
module, one CLI verb and hostile tests. It adds no provider abstraction, PKI, workflow framework,
packet filesystem layer, artifact store, candidate adapter or backend trait. The fixed 28-row table
is the frozen contract rather than a generic descriptor DSL.

This validator is off the LaminarDB data path. Its bounded JSON parsing and per-call schema
compilation therefore do not affect record, Arrow-batch, checkpoint or state-backend latency. No
runtime lock, allocation, I/O, network call, fsync or transaction was added. Candidate hot-path
latency and maintenance-health behavior remain unmeasured; validation-tool speed is not evidence
for them.

### 3. Unused code and dependencies

**Pass.** The unused `run_class` summary field was removed. Cycle 22 changes no Cargo manifest or
lockfile and adds no redb, Fjall, RocksDB, network, provider or process dependency to the
qualification tool or product runtime. No runtime backend, feature flag, adapter, dispatch command,
result classifier or dormant execution abstraction was added.

The schemas are embedded by the validator, the module is exported, the CLI path is tested, and the
synthetic fixtures are consumed by contract and CLI tests. The reserved result-payload identity has
no schema or code yet and is not represented as implemented.

### 4. Production readiness, delivery, exactly once and independent soak

**NO-GO, correctly fail-closed.** This cycle creates no backend evidence. No C1/C2/C3 campaign,
native persistence or recovery run, fault trial, latency/resource measurement, Docker/WSL smoke,
endurance run, or independent production soak executed. Bounded memory remains reference-only and
no local-spill engine has been selected.

Vnode ownership epochs, checkpoint freeze/export/seal, restore-before-activate, rebalance fencing,
retention-safe cleanup and state-transfer lifecycle are still absent from the product. Grouped
aggregates, windows and stateful joins remain rejected in cluster mode under `[LDB-4007]`.

Exactly once still requires a compatible source replay/offset cut, sealed vnode state, recovered
coordinator decision, and a transactional or idempotent fenced sink. This validator changes no
source or sink capability and cannot satisfy `[LDB-0013]`. The independently operated 24/72-hour
production-like soak remains a later release veto after an immutable implementation candidate
exists; prescreen validation cannot replace it.

### 5. Documentation, stale research and overdocumentation

**Pass.** The successor wire contract is consolidated in the existing redb protocol rather than a
second ADR. It now cites the provider's actual protected-environment behavior and states the gap
between the repository's one-approval configuration and the required two-role proof. The closeout
does not duplicate all 28 tuples or schema fields; it records outcomes and review evidence.

No research document became obsolete in this cycle. Earlier backend source, mechanism, rejection
and decision reports remain audit provenance, not current authority. No Claude-memory claim was
accepted without code, primary-source or empirical corroboration, and no document was deleted only
to reduce file count.

### 6. Tests and empirical boundary

**Validation passes; native and production evidence remains absent.** The complete
`state-backend-qual` suite reports 142 passed and one explicitly ignored non-gating parser
throughput/RSS observation. `cargo fmt --all -- --check`,
`cargo clippy --all-targets --all-features -- -D warnings`, and `git diff --check` pass. The
synthetic success command exits 0 with only ineligible/unverified vocabulary; an unknown run-like
command exits 64. Cycle 22 changes no Cargo manifest or lockfile.

Hostile coverage includes duplicate/unknown fields, JSON depth/node and byte caps, non-`u64`
numbers, placeholders, hashes, exact source pins, descriptor order and tuple mutations, fixed role
and aggregate caps, native prior-smoke shape/cap, stage swaps, time/order/head/reviewer mutations,
cross-document repinning, post-run rejection and hard-false authority accessors. These are local
synthetic validation observations. No descriptor target was dereferenced and no candidate result
exists.

## Cycle 23 entry boundary

The next useful slice remains validation-only:

1. Freeze the smallest post-run/result content contract: exact result-payload fields, retained
   artifact-index root, storage-version/retention binding, precedence and byte/accounting rules.
2. Only after independent contract review, add closed schemas and a redb-free bytes-only validator
   with an always-unsealed/ineligible success type and hostile synthetic fixtures.
3. Keep the formal race-safe packet reader, live provider verification, trusted dispatcher,
   workflow, artifact construction/dereference, result evidence classifier and native execution out
   of that slice unless separately authorized.
4. Keep maintenance-health v2 at approved direction. Its exact contract still needs the existing
   owner freeze before any v2 schema, observer, adapter or executable implementation.

Do not add a runtime backend, execute redb or another candidate, select a backend, relax
`[LDB-4007]`/`[LDB-0013]`, treat bounded memory as a product profile, or claim production readiness
from content validation.

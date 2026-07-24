# Distributed keyed state Cycle 21 review

- **Date:** 2026-07-24
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle commits:** `a1d76b28`, `3e100687`, `fba10d5b`, `1c788d87`, `9ecb52e8`, `a91f78ce`
- **Cycle outcome:** `OWNER_DIRECTION_AND_VALIDATION_CONTRACTS_REVIEWED_EXECUTION_BLOCKED`
- **Bounded memory:** reference/conformance-only; no current product or soak profile
- **Current product target:** local spill; backend not selected
- **Candidate construction or execution authorized:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**

## Outcome

Cycle 21 records the owner's two decisions without widening their authority. Bounded memory remains
the semantic/conformance reference only. Local spill remains the sole current cluster product target,
and no engine has been selected for it. The maintenance-health v2 direction is approved, but the
[consolidated runner v2 freeze candidate](../architecture-decisions/state-backend-qualification-runner-v2-draft.md)
is still unapproved. Its reserved identities, schemas, parsers, formulas, observers, adapters and
execution path do not exist.

The v2 draft is standalone relative to the v1 runner and keeps Fjall 3.1.8/RocksDB 10.4.2 as the
comparison scope. It uses common correctness, latency, resource, persistence and failure gates plus
candidate-native maintenance-health vetoes. It does not rank native metrics, invent a common debt
number, or include redb. Its exact profile-use declaration cannot mutate v4's retained
`qualification_eligible=false` provenance; only a later detached two-owner qualification approval
could make the profile/declaration pair consumable.

The [redb 4.1.0 prescreen protocol](../testing/state-backend-redb-prescreen-v1.md) is now a reviewed
validation-contract candidate, not a runnable backend test. The two old descriptor-root schemas and
fixtures are permanently synthetic, fail-closed regression shapes. The successor protocol fixes
packet locators, deterministic fixtures and schedules, bounded raw evidence, crash/liveness
classification, resource and five-hour safety bounds, and result precedence. It reuses the
repository's protected-review provider rather than adding a private PKI. A copied receipt can report
only `authorization_unverified`; live protected-provider verification remains mandatory before any
future dispatch or result sealing.

No runtime backend, adapter, native observer, candidate source, successor redb schema, verifier,
harness, fixture base, execution command or result was added. The existing construction-only redb
workspace was not executed and cannot emit a disposition.

## Six-pass cycle review

### 1. AI slop and contract consistency

**Pass after independent blocking review and correction.** The first v2 review rejected an unsafe
implicit override of v4's false provenance, incomplete latency/resource formulas, weakened
space-amplification handling, missing cadence/cut chronology, target-device binding and witness
drift, an overbroad hot-path no-I/O statement, incomplete fault fairness, first-initialization
ambiguity and a missing retained stall-wire cap. The corrected exact draft, SHA-256
`af152cecd0bcd12d964eaa7e8e83fe9c86c1b11066e77feda4b0c27f1c8388e9`, received an independent
PASS.

The redb review rejected circular payload/signature wording, ambiguous trust stages, incomplete
fixture construction, a whole-state digest in the candidate path, inconsistent timeouts, adaptive
schedule ambiguity and a `u64` overflow that would invalidate every nonzero lane. The corrected
protocol masks generation before addition, evaluates slot arithmetic in `u128`, and is independently
PASS at SHA-256 `d11fb8b889befa107885eefb5161dd4f7f9927d1e9fc1b7fc9b50192a6d04434`.

A separate slop review then caught stale bounded-memory soak obligations and an overengineered custom
Ed25519 registry/revocation hierarchy. Those obligations were removed from current authority, the
Cycle 20 text was marked historical, and the custom hierarchy was replaced by the repository's
protected-review policy/receipt boundary. Final governance, redb and slop re-reviews all returned
PASS.

### 2. Overengineering, hot path and latency

**Pass for validation-contract scope.** V2 adds no metrics DSL, weighted score, dynamic plugin,
remote metrics service or per-signal timestamp rows. Candidate health remains a conjunctive veto.
The measured request observer/sample update is preallocated, bounded and free of allocation, locks,
logging, merge work and I/O; backend locking and I/O remain correctly inside measured service. The
target-device design stores bounded per-shard summaries instead of a multi-gigabyte per-I/O stream,
and exact tracer capacity/overhead remains a pre-execution gate.

The redb prescreen times the database-wide writer and full transaction service rather than hiding
them. Full-state expected digests are precomputed by a redb-free oracle, not scanned on the candidate
hot path. The protocol has no optional post-failure diagnostic repetition, no private security
platform and no candidate execution path. Its 287.5-minute allocation leaves 12.5 minutes of the
five-hour hard watchdog as slack; safety caps are not decision thresholds.

### 3. Unused code and dependencies

**Pass.** The only code change hardens existing validation tests so fabricated legacy redb records
cannot claim approval, execution or a native disposition. No Cargo manifest or lockfile changed. No
redb, RocksDB, Fjall or other engine dependency entered a LaminarDB runtime crate. No v2 schema,
parser, observer, adapter, feature flag, runtime trait or unused backend abstraction was added.

The isolated existing `tools/state-backend-redb-prescreen` workspace remains construction-only and
all of its qualification, selection, production, delivery and soak fields remain false.

### 4. Production readiness, delivery, exactly once and independent soak

**NO-GO, correctly fail-closed.** No working-state backend has passed C1/C2/C3, native persistence,
fault, recovery, resource, latency or 24/72-hour endurance qualification. Vnode ownership epochs,
checkpoint freeze/export/seal, restore-before-activate, rebalance fencing and retention-safe cleanup
are not implemented in the product. Grouped aggregates, windows and stateful joins therefore remain
rejected in cluster mode.

Exactly once remains a topology-specific composition over source replay/offset handoff, the sealed
state cut, coordinator decision recovery, and a transactional or idempotent fenced sink. A local
engine cannot create capabilities absent from the source or sink. No connector combination has
passed that matrix.

The current production-soak charter targets local spill only. It still requires an immutable release
candidate, external oracle, production-like connectors, faults/rebalances, progress and latency
gates, leak slopes and an operator independent of implementation. No such soak ran. Backend
qualification or the bounded redb prescreen cannot substitute for it.

### 5. Documentation, stale research and overdocumentation

**Pass after subtraction.** One standalone v2 draft owns the consolidated normative candidate; ADRs,
plans and mapping reports link to it rather than copying another schema. Its length is justified by
the requirement that v1 have no interpretive authority, but its wire rules must not be duplicated in
new documents. Independent review found and corrected two such retained-wire omissions before close.

One redb protocol owns the prescreen contract. The rejected private PKI was removed rather than
documented further. No tracked research document became irrelevant in this cycle: the Cycle 16-20
source, construction, rejection and decision reports remain audit provenance, while the Cycle 20
review is explicitly marked historical where Cycle 21 supersedes it. No Claude-memory assertion was
accepted as evidence and no document was deleted merely to reduce file count.

### 6. Tests and empirical boundary

**Validation passes; native and production evidence remains absent.** The full qualification-tool
suite reports 128 passed and one explicitly ignored non-gating observation test. `cargo fmt --check`,
`cargo clippy --all-targets --all-features -- -D warnings`, `git diff --check`, changed-document
relative-link checks, domain/header byte-count checks and runtime-manifest scope checks pass.
Legacy-schema tests prove both committed synthetic records remain ineligible and reject fabricated
native/prescreen-shaped authority.

No redb candidate binary, fixture generator, Docker smoke, native target campaign, fault trial,
latency run, recovery comparison or endurance run executed. No Fjall/RocksDB campaign, connector
matrix or independent product soak ran. Document and synthetic-validator PASS results are not
backend or production evidence.

## Cycle 22 entry boundary

Work can proceed without choosing a backend, but only inside existing authority:

1. Validation-only redb work may implement strict successor approval/result payload,
   protected-review-receipt, plan, raw-wire and result schemas plus a redb-free bounded semantic
   verifier/classifier. Local receipt validation must never become execution authorization.
2. Formal native redb source, generator, fixture bases, supervisor, child, actuator and oracle still
   require a separate construction authorization. Candidate execution additionally requires the
   complete provider-authenticated pre-run packet; neither is authorized now.
3. V2 owners must resolve every listed pre-final decision and issue the exact
   `APPROVE_STATE_BACKEND_RUNNER_CONTRACT_V2` record before any v2 identity or validator is
   implemented. Candidate-specific source work and execution remain later approvals.
4. Backend selection waits for comparable complete evidence, C3 and owner review. Distributed
   checkpoint/source/sink/rebalance implementation, delivery claims and the independent release soak
   remain later vetoes regardless of which local engine is carried.

Do not add a runtime backend, execute a candidate, select by elimination, start Phase 1, relax
`[LDB-4007]`/`[LDB-0013]`, or claim production readiness from validation-only work.

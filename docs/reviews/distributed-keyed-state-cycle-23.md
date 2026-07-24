# Distributed keyed state Cycle 23 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle commits:** `844ca13c`, `19cc44eb`
- **Cycle outcome:** `REDB_POST_RUN_BINDING_VALIDATED_AUTHORITY_UNVERIFIED`
- **Bounded memory:** reference/conformance-only; no current product or soak profile
- **Maintenance-health v2:** direction approved; exact runner-v2 contract and implementation remain gated
- **Current product target:** local spill; backend not selected
- **Candidate construction or execution authorized:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**

## Outcome

Cycle 23 adds only the independently reviewed outer post-run binding slice in the
[redb 4.1.0 prescreen protocol](../testing/state-backend-redb-prescreen-v1.md). The new bytes-only
validator consumes the exact policy, approval payload, pre-run receipt, opaque result bytes, opaque
artifact-index bytes and post-run receipt. It reuses the complete pre-run validator, bounds the two
opaque inputs, hashes their actual bytes, and checks the post receipt's policy/change/head/configured
workflow lineage, exact descriptors, copied review bindings, cross-stage copied event-ID non-reuse,
and timestamp interval.

Success has one meaning: structurally coherent copied bindings whose authority is unverified. The
public summary contains only `RedbPrescreenAuthorization::Unverified`; execution and result-sealing
accessors are hard false. After the standard repository notice, the CLI's sole status line is
`VALID_INELIGIBLE_REDB_PRESCREEN_BINDING stage=post_run
authorization=authorization_unverified`. It exposes no payload ID, run class, disposition, reviewer,
provider, storage, backend-selection or qualification state.

The result and artifact-index bytes are intentionally opaque. The validator does not establish that
they are JSON, validate a result/index schema, interpret a disposition, inspect index entries,
dereference an indexed artifact, check retained-byte accounting, authenticate a review/provider,
verify storage/retention, classify redb, or seal evidence. A fully repinned synthetic chain that
contains `PRESCREEN_PASS` or `immutable=true` text still returns only `Unverified`.

The strict result/classifier slice was not implemented because its contract is incomplete. Missing
normative pieces include the successor result layout and descriptor hierarchy, Docker outcome
vocabulary, artifact-index entry/destruction schema, raw manifest and binary wires/goldens, final
index lifecycle, distinct derived-outcome/terminal-latch/final-seal states, and a trusted immutable
storage version/object-set/retention protocol. The obsolete result identity in the exact-source
mechanism note was corrected; the legacy result schema remains synthetic-only.

The protocol's final SHA-256 is
`e10a2434b37199ffb809b1a001f1f87028b1ce471f1f1eae33e598c0280c2f10`.

## Six-pass cycle review

### 1. AI slop and contract consistency

**Pass after independent blocking review and subtraction.** The initial audit rejected a strict
result/classifier implementation because it would have invented unspecified fields, evidence rows,
Docker results, binary formats and storage semantics. Work was narrowed to exact outer binding. The
first freeze draft also exposed many fixed-false provider/artifact/accounting/storage/backend axes;
review removed them in favor of the existing single `Unverified` authority and short ineligible
output.

Implementation review found that early result/index mutation tests could be masked by length errors.
They now use same-length one-byte mutations and assert SHA-specific failures. Independent mutations
also reach the post policy digest, copied review payload length/SHA, and reviewed-head checks. Final
governance, protocol and code-quality reviews returned PASS.

Copied account and event string inequality is never described as proof of real distinct principals
or events. The exact content relationship is `pre_verified <= each post_review <= post_verified`;
the post event IDs must differ from both pre event IDs, while owner account strings may repeat.
Configured workflow/job/environment and change/head are stable across stages; copied workflow run,
attempt and job IDs may differ.

### 2. Overengineering, hot path and latency

**Pass for validation-only scope.** The cycle reuses the existing receipt schema and pre-run
validator. It adds no premature result/index schema, generic evidence DSL, classifier framework,
provider plugin, storage abstraction, formal packet reader or trusted-state conversion. The larger
diff is dominated by hostile tests and explicit CLI input/error handling for six independently
bounded byte strings.

All new work is outside LaminarDB's record, Arrow-batch, state, checkpoint and rebalance paths. The
largest operation is bounded sequential hashing of a caller-supplied 16-MiB opaque index; the CLI
reads only cap plus one byte. No runtime lock, async hop, transaction, database I/O, fsync, network
call or per-record allocation was added. This validation latency says nothing about candidate or
product latency.

### 3. Unused code and dependencies

**Pass.** Both new constants, the one-state summary, validator, CLI path, bounded-input helper and
synthetic fixtures are exercised. No Cargo manifest or lockfile changed. No product runtime crate,
backend adapter, feature flag, redb/Fjall/RocksDB dependency, network/provider client,
process-launch path, artifact reader, classifier, sealer or storage client was added.

No successor result-payload or artifact-index schema exists. That absence is deliberate and tested:
opaque bytes that are not JSON can bind successfully but cannot produce any authority or result
state.

### 4. Production readiness, delivery, exactly once and independent soak

**NO-GO, correctly fail-closed.** No redb candidate or other backend executed. No Docker/WSL smoke,
native campaign, C1/C2/C3 run, persistence/recovery trial, latency/resource result, fault/endurance
run or independent production soak occurred. Bounded memory remains reference-only, local spill
remains the current product target, and no backend has been selected.

The product still lacks vnode ownership epochs, checkpoint freeze/export/seal,
restore-before-activate, rebalance fencing and retention-safe cleanup. Keyed aggregates, windows and
stateful joins therefore remain rejected in cluster mode under `[LDB-4007]`.

Exactly once still depends on a compatible source replay/offset cut, the sealed vnode-state cut,
coordinator decision recovery, and a transactional or idempotent fenced sink. This cycle changes no
source or sink capability and cannot satisfy `[LDB-0013]`. A later immutable release candidate must
still pass the independently operated production-like 24/72-hour soak; a prescreen byte binding can
never substitute for it.

### 5. Documentation, stale research and overdocumentation

**Pass after correction.** The exact Cycle 23 boundary is part of the existing redb protocol rather
than another ADR. It states every checked relation and every intentionally opaque/unverified fact so
the API cannot be relabelled as result validation. The mechanism note's obsolete
`state-backend-redb-prescreen-result/v1` reference now points to the reserved successor while saying
that its schema and classifier do not exist.

No research document became irrelevant. The mechanism note remains useful exact-source provenance
after correction; earlier backend source, rejection and decision reports remain audit history, not
execution authority. No Claude-memory assertion was accepted as evidence, and no document was
deleted merely to reduce file count.

### 6. Tests and empirical boundary

**Binding validation passes; candidate and production evidence remains absent.** The complete
`state-backend-qual` suite reports 148 passed and one explicitly ignored non-gating parser
throughput/RSS observation. `cargo fmt --all -- --check`,
`cargo clippy --all-targets --all-features -- -D warnings`, and `git diff --check` pass. The checked-in
opaque fixtures are LF-pinned and their actual lengths/SHA-256 values match the synthetic post
receipt.

Hostile coverage includes empty, exact-cap and cap-plus-one result/index bytes; same-length digest
mutations; wrong stage, policy, provider, change, workflow, role and locator; copied account/event
collisions; pre-event reuse; wrong decision, head, result length/SHA and chronology; a completely
repinned chain; over-cap CLI reads; and unavailable run/dispatch/approve/accept/verify/classify/seal/
select/qualify verbs. Empirically, valid synthetic bytes exit 0 only as ineligible/unverified and a
classifier-like command exits 64.

No result/index content was validated, no descriptor locator was resolved, no indexed artifact was
opened, no candidate result was derived, and no provider or storage fact was authenticated. These
tests are not backend or production evidence.

## Cycle 24 entry boundary

Further validation-only work must close contracts before code:

1. Separate the classifier-derived outcome, early terminal-rejection latch, owner-reviewed result,
   and final stored seal; resolve the current pass/post-review circular wording and Docker outcome
   vocabulary.
2. Freeze the successor result descriptor hierarchy and exact artifact-index entry,
   destroyed-database, count, ordering, uniqueness, exclusion, cap and sum model, including the
   intermediate-to-final index and cleanup lifecycle.
3. Define the raw-run manifest, validator/oracle/mechanism reports, binary wire registry/framing and
   literal goldens before any strict result classifier exists.
4. Keep immutable storage provider/version/object-set/atomicity/retention authentication in a later
   separately trusted verifier; packet-supplied hashes or `immutable=true` remain claims only.

Do not add a result/index implementation before its exact contract passes independent review. Do
not add a runtime backend, formal packet reader, trusted dispatcher, provider/storage integration,
candidate construction or execution, backend selection, or any relaxation of `[LDB-4007]` or
`[LDB-0013]`.

# Distributed keyed state Cycle 26 review

- **Date:** 2026-07-25
- **Branch:** `feature/distributed-keyed-state-adr`
- **Contract commit:** `98a1f6fa`
- **Validation commit:** `adfbeec8`
- **Cycle outcome:** `REDB_FORMAL_INPUT_V2_CONTENT_VALIDATED_EXECUTION_BLOCKED`
- **Bounded memory:** reference/conformance-only; no current product or soak profile
- **Maintenance-health v2:** direction approved; exact runner-v2 contract and implementation remain
  gated
- **Current product target:** local spill; backend not selected
- **Candidate or backend execution authorized or performed:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Independent production soak:** not run
- **Production verdict:** **NO-GO**

## Outcome

Cycle 26 freezes and implements one redb-free copied-content slice: the additive
`state-backend-redb-prescreen-approval-payload/v2` and matching
`state-backend-redb-prescreen-protected-review-receipt/v2` pre-run contract. The legacy `/v1`
28-row contract remains an ineligible synthetic regression contract; it cannot describe the formal
29-object set or authorize dispatch. The `/v2` payload
uses the first 25 legacy rows unchanged, inserts the 64-MiB fixture role at row 26, and moves the
256-MiB, 1-GiB and 4-GiB roles to rows 27--29.

The bytes-only validator decodes the bounded payload once with duplicate-key rejection, selects only
the exact `/v1` or `/v2` payload schema, and uses that payload version to select both the receipt
schema and semantic role registry. A receipt cannot nominate or substitute its own version. The
semantic layer enforces every role/locator/media tuple, the exact supplied policy-byte binding and
fixed redb archive descriptor pin (length and SHA-256), the 1/1/4/12-GiB fixture-role caps, and
checked 20-GiB aggregate including native prior
smoke. Fully repinned cross-version receipt pairs still fail.

The public API surface, CLI command set, and ineligible output/authority vocabulary are unchanged;
the existing content command now accepts `/v2`. Success returns only the existing single-variant
`Unverified` authority, prints `VALID_INELIGIBLE_REDB_PRESCREEN_CONTENT`, and keeps execution and
result-sealing accessors unconditionally false. The Cycle 23 outer post-run binding remains
`/v1`-only; a content-valid `/v2` pre-run packet cannot enter it. The structurally reserved
`post_run` receipt branch is explicitly rejected by pre-run semantics.

The LF-pinned synthetic `/v2` payload is 8,369 bytes with SHA-256
`2d354f07af5fc18b408b72b7ccb91a565746977ff52de5a25872adaa25f207c9`; its receipt is 2,433
bytes with SHA-256 `b79b6f508b5324762faaf7b3597d782056b8ecf1f8ce8ba498c0c14c83526719`.
Those JSON bytes do not contain or prove an independently constructed redb fixture. Whitespace or
line-ending drift with the fixed receipt fails the exact-byte binding.

The approval-input storage-version binding is placed only as future nested run-start content. It has
no role, locator, retained/indexed leaf, schema, fixture, public type or validator. Its provider,
version, retention, freshness and TOCTOU contract remains unresolved, and no serialized content can
construct `APPROVAL_INPUT_STORAGE_VERSION_VERIFIED`.

## Six-pass cycle review

### 1. AI slop and contract consistency

**Pass after three independent reviews and adversarial repair.** The review found and corrected a
stale reference to a nonexistent result-payload schema, ambiguous treatment of the copied
approval-input version as a possible standalone retained artifact, and wording that could equate
structural JSON Schema validity with semantic conformance. The final contract makes the binding
nested, labels the receipt's post-run shape reserved, and requires semantic registry and byte checks
before even an ineligible success exists.

The implementation uses one private two-variant selector and two explicit registries instead of a
generic version DSL. The `/v2` registry is mechanically composed from `/v1` rows 1--25, one new
64-MiB row, and the existing final three rows. Independent mutation tests cover role, locator and
media type at every position in both versions. No approval word, provider-state string, descriptor,
receipt or schema branch creates authority.

One intentionally accepted compatibility detail is that a multiply-invalid legacy input may now
report a placeholder/non-`u64` diagnostic before its schema diagnostic. Accepted/rejected behavior,
exit codes, error prefixes and valid `/v1` output remain unchanged; exact internal error ordering was
never a wire contract.

### 2. Overengineering, hot path and latency

**Pass for the validation-only scope.** The change extends the existing parser, type and CLI rather
than adding a packet framework, provider abstraction, capability library, process runner or storage
backend. The explicit 29-row array is smaller and easier to audit than a registry language.

This code is in the standalone qualification tool, not the LaminarDB record, Arrow batch, keyed-state,
timer, join, checkpoint, source, sink or rebalance path. Inputs remain capped at 32/64 KiB and 4,096
nodes before semantic checks; descriptor lengths never drive allocation. No candidate writer
acquisition, commit, recovery, p99/p999 latency or resident-state memory behavior was measured.

### 3. Unused code and dependencies

**Pass.** Both new schemas are embedded and meta-schema tested. The two static fixtures are consumed
by a byte-hash integration golden and CLI test. The private version selector, `/v2` registry and
role-cap constant are exercised by positive and hostile tests.

No Cargo manifest or lockfile changed. The isolated dependency tree contains no LaminarDB, Fjall,
redb, RocksDB, librocksdb, Arrow or DataFusion package. CI's qualification-tool dependency deny-list
now explicitly includes `redb`. No runtime feature, adapter, public capability, filesystem packet
reader, provider client, process launcher, Docker client or deletion path was added.

### 4. Production readiness, delivery, exactly once and independent soak

**NO-GO, correctly fail-closed.** Cycle 26 did not create or open a redb database, run the
construction candidate, invoke Docker/WSL, collect native target evidence, inject a fault, run an
endurance campaign or perform an independent soak. It produced no writer-rate, latency, recovery,
resource, C1/C2/C3, fault-endurance or production evidence and selected no backend.

LaminarDB still lacks vnode ownership epochs, checkpoint freeze/export/seal,
restore-before-activate, rebalance fencing and retention-safe cleanup. Cluster keyed aggregates,
windows and stateful joins therefore remain rejected by `[LDB-4007]`.

Exactly once still requires a replayable source/offset cut, sealed vnode-state cut, recoverable
coordinator decision, and transactional or idempotent fenced sink. This cycle changes no source or
sink capability and cannot satisfy `[LDB-0013]`. Any selected immutable release candidate must
still pass backend endurance and an independently operated production-like soak.

### 5. Documentation, stale research and overdocumentation

**Pass.** The normative additions stay in the existing prescreen protocol and correct, rather than
duplicate, its historical schema inventory. The contract distinguishes structural schemas,
redb-free semantic validation, live provider/storage authority, candidate execution and final
sealing. The storage-version schema is deliberately deferred instead of inventing provider fields.

No external research or Claude-memory claim was used as evidence in this slice. Existing research
and backend documents remain relevant decision history; none was removed merely to reduce file
count. The exact fixture lengths and hashes are kept in one focused integration golden rather than
repeated through the protocol.

### 6. Tests and empirical boundary

**The isolated validation slice is green; the root Windows all-target gate remains red.**

- `cargo fmt --manifest-path tools/state-backend-qual/Cargo.toml -- --check`: pass.
- `cargo clippy --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features -- -D warnings`: pass.
- Isolated `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets
  --all-features`: 157 passed, one explicitly ignored non-gating throughput/RSS observation.
- Root `cargo fmt --all -- --check`: pass.
- Root `cargo clippy --all-targets --all-features -- -D warnings`: pass.
- `git diff --check`: pass, apart from benign Windows LF-to-CRLF checkout warnings.

Root `cargo test --all-targets --all-features` was rerun and failed during compilation with the
same Windows missing-rlib/`E0463` class recorded in Cycle 25. Failures included unresolved
`laminar_db` in integration tests and benches, `laminar_connectors` and other rlibs, and unresolved
DataFusion/SQL dependencies in the LaminarDB lib-test target. Cycle 26 changed only the isolated tool
and schemas, fixtures, protocol/review documentation, attributes and CI dependency guard; it did
not change those targets. The root gate is
recorded as failing, not reclassified.

The repository's full CI workflow was not invoked because its required
`redb-prescreen-construction` job executes the construction-only redb candidate, which the current
instruction forbids. No candidate, backend, native or Docker workload ran.

## Cycle 27 entry boundary

Continue validation-only design without candidate or backend execution:

1. freeze exact tagged native and Docker actual-target/preflight identities, field sets, reason codes,
   byte/depth/node caps and positive/negative examples before adding schemas;
2. keep the approval-input storage-version schema blocked until a provider/version/retention,
   freshness and TOCTOU contract is independently selected;
3. do not freeze run-start or raw-manifest caps until the 105-row native and ten-row Docker
   role/process/cardinality registries prove their maxima;
4. require a Docker-tagged durable launch ledger or equivalent retained broker-receipt chain before
   any attempt-cut implementation; and
5. add no runtime backend, redb dependency, candidate execution, deletion path, cluster admission
   change, backend selection or production claim.

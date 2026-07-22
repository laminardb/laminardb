# Distributed keyed state — Cycle 4 review

- **Date:** 2026-07-22
- **Scope:** first grouped-aggregate semantic contract, managed-artifact/restore boundaries,
  source/sink delivery proof, and the executable but ineligible numerical qualification profile
- **Verdict:** **APPROVE WITH OWNED FOLLOW-UPS** for this Phase 0 slice only
- **Production/admission verdict:** **BLOCK**; keyed aggregates, windowed aggregates, stateful joins,
  and cluster materialized views remain fail-closed with `[LDB-4007]`, and cluster exactly-once
  remains separately blocked by `[LDB-0013]`

This review approves contracts and qualification scaffolding, not a distributed state runtime. No
managed aggregate codec, `VnodePartialV2`, working-state service, ownership lifecycle, LSM adapter,
certified connector composition, production benchmark result, or independent soak result exists.

## Reviewed changes and review history

Cycle 4 covers `3519be7d..9aec6933`, followed by the review correction that distinguishes Kafka
transactions used for writer fencing from checkpoint-coupled transactional/exactly-once sink
commits. The principal commits are:

- `3519be7d`, `ba3e4050`: narrow the first grouped aggregate and define replay across writer terms;
- `73b54e07`, `536f9634`, `fd83e374`: introduce the explicitly ineligible numerical profile and
  bounded standalone validator;
- `1b0fa2af`: close schema, arithmetic, evidence-ownership, CI, dependency-isolation, and bounded
  input gaps; and
- `9aec6933`: reconcile aggregate arithmetic, V2 directory, restore charging/spooling, source
  ledger, Kafka fencing, and current-versus-proposed documentation.

The first independent review was **BLOCK**, despite green unit tests. It found batch-dependent SUM
overflow, ambiguous V2 absence/reference semantics, allocation before restore charging, an
observational rather than provider-enforced stale-writer test, acknowledgement-based source
membership, ambiguous operation identity, mixed evidence ownership, and incoherent numerical caps.
The profile, ADR, plans, report, charter, and CI were corrected and re-reviewed. Targeted profile,
workflow, and documentation reviewers then approved their scopes; a fresh six-pass reviewer returned
**APPROVE WITH OWNED FOLLOW-UPS** for the complete cycle.

## Review passes

### 1. AI slop and evidence

**Result: pass.**

- Current behavior and future design remain separate. The validation report says the first artifact
  is *specified* to use a bounded Laminar codec; it does not claim that codec exists.
- The first candidate is exact rather than aspirational: one append-only `COUNT(*)`, one `SUM` of a
  direct `Int64` column, direct partition-ABI-v1 group columns, and positively replay-deterministic
  upstream expressions. Unsupported variants retain `[LDB-4007]`.
- COUNT/SUM durable arithmetic is signed-`Int64` compatible and checked at every group-local input
  prefix. A whole Arrow batch preflights before one atomic state/output mutation, so coalescing
  cannot hide `[MAX, +1, -1]` overflow or partially apply a later-group failure.
- The profile prints `NOT QUALIFICATION EVIDENCE`, fixes `qualification_eligible=false`, forbids
  measured/result fields, and does not disguise proposed thresholds as benchmark results.
- Exact Fjall `=3.1.8` and RocksDB wrapper `=0.24.0`/engine `10.4.2` pins are comparison inputs, not a
  backend selection. No claim relies on the rejected obsolete Claude/private research material.

### 2. Over-engineering, hot path, and latency

**Result: pass for this slice; production latency remains unproven.**

- Cycle 4 changes no Laminar runtime crate. The validator is a standalone tool with no Laminar,
  Arrow, DataFusion, Fjall, or RocksDB dependency and cannot enter the record path.
- The design keeps contract derivation, codec selection, sorting, hashing setup, checksumming,
  artifact parsing, object-store I/O, and restore work off the record/event-loop path. The future
  executor uses one grouped read and one atomic batch mutation/output enqueue, not per-row futures,
  fsyncs, transactions, or LSM operations.
- Whole-transition row preflight uses a disk-governed immutable spool before operator callbacks.
  This deliberately adds restore I/O; its RTO, disk-pressure, and p99/p99.9 effects must be measured
  rather than optimized away before correctness is proven.
- Kafka transactions are required only for externally auditable old-writer fencing in the initial
  at-least-once scenario. Transaction batching and marker activation latency remain numerical gates.
- Hosted-runner Criterion data is explicitly an advisory trend smoke, not qualification evidence.

### 3. Unused and dead code

**Result: pass.**

- The profile library, CLI, schema, candidate, and public size cap are exercised by unit/CLI tests
  and by a required CI-success dependency.
- CI rejects path/workspace dependencies and all-feature/all-target Laminar, Fjall, RocksDB,
  `librocksdb-*`, Arrow, and DataFusion dependency families.
- No unused runtime adapter, alternate LSM, permissive admission flag, archived live Rust type, or
  abandoned generic IPC helper was added. The future qualification plan still requires deletion of
  the losing Fjall/RocksDB adapter after selection.

### 4. Production readiness, delivery, and independent soak

**Result: BLOCK, correctly fail-closed.**

The contracts now require:

1. a manifest-selected, allocation-bounded `VnodePartialV2` with canonical `BODY`/`REFERENCE`
   entries, authoritative `EMPTY`, exact body coverage, resolved FULL/EMPTY bases, and no fallback;
2. inventory and artifact encoded-byte charging before GET/buffering, separate task/global scratch,
   checked disk-spool reservation, atomic completion, retry identity, and crash reclamation;
3. a pure plan/codec contract before fetch, followed by whole-transition row preflight, abortable
   shadow prepare, and one fenced infallible graph publication boundary;
4. pipeline incarnation in operation identity plus deterministic-expression admission;
5. durable producer intents reconciled with every actual Kafka source record through frozen broker
   high-watermarks, including physical retries and lost acknowledgements;
6. stable Kafka transactional identity per bounded sink-writer shard, broker fencing, one confirmed
   predecessor/successor marker transaction, transactional output, read-committed capture, and
   forced rejection of predecessor writes; and
7. a separate later exactly-once certification because source cursor, managed state, and Kafka
   transactions are not one atomic commit.

None is implemented or certified. The candidate profile lacks owner approval and immutable runner
identity; neither LSM has run the common workload/fault/endurance suite; no source/state/sink
composition has delivery evidence; and no independently operated release-candidate soak has run.
Backend soak, integration tests, benchmarks, canaries, and self-review cannot substitute for that
soak.

### 5. Documentation and over-documentation

**Result: pass with an owned consolidation follow-up.**

- The validation report owns current evidence; ADR-008 owns normative architecture; the Phase 0
  plan owns near-term execution; the master plan owns phase sequencing; and the soak charter owns
  independent certification.
- BODY/REFERENCE/EMPTY, plan-before-preflight ordering, zero-row versus zero-count rejection,
  encoded versus scratch charging, broker-derived input membership, and old-interval output after a
  partition marker are consistent across those authorities.
- The normative artifact and marker binary layouts do not yet exist. Their implementation slices
  must publish field offsets, widths, byte order, digest ranges, compatibility goldens, and hostile
  vectors instead of treating prose or a Rust type as wire ABI.
- Detailed contracts are still repeated across five documents. Before Phase 0 closes, consolidate
  normative format/lifecycle text into ADR-008 and retain only executable acceptance work in Phase 0.

### 6. Tests and checks

**Result: pass for the changed scope, with the runtime rerun limitation disclosed.**

| Command/check | Result |
|---|---|
| `cargo fmt --manifest-path tools/state-backend-qual/Cargo.toml -- --check` | PASS |
| `cargo clippy --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets -- -D warnings` | PASS |
| `cargo test --locked --manifest-path tools/state-backend-qual/Cargo.toml --all-targets` | PASS, 13/13: 8 library, 1 binary, 4 CLI |
| Candidate CLI exact two-line protocol | PASS; `VALID_INELIGIBLE_PROFILE`, never qualification |
| All-feature/all-target dependency isolation | PASS |
| Modified workflow YAML parse and `ci-success` dependency | PASS |
| Touched-document local-link validation | PASS |
| `git diff --check` | PASS |

A default-feature `laminar-db --lib` invocation compiled but selected zero copies of the cluster-only
admission test and is not evidence. The corrected cluster-feature retry remained compile-bound and
timed out after 304 seconds without an assertion or compiler failure. The independent review also
observed concurrent build-lock and unrelated integration-target missing-rlib noise; none is counted
as a pass. Cycle 4 changes no runtime crate, and Cycle 3 retains the passing exact `[LDB-4007]`
admission result. The full workspace, Docker/Kafka/MinIO, multi-process chaos, backend qualification,
endurance, and independent production soak were not run and are not represented as green.

## Owned follow-ups before the next implementation gate

- **Workload and operations owners:** approve the unchanged candidate hash, thresholds, exact runner
  image/package identity, and a separate approved-profile schema/status. Do not edit the ineligible
  profile into evidence after results exist.
- **Artifact/restore owner:** publish the normative row/V2 layouts and goldens; implement declared
  versus actual byte enforcement, partial-download handling, spool cleanup, and disk-pressure tests.
- **Storage/performance owner:** build the backend-neutral model, run equivalent exact-pin Fjall and
  RocksDB durability/crash/compaction/resource/endurance evidence, select one, and delete the loser.
- **Connector/delivery owner:** freeze transactional-ID and marker schemas; test ambiguous commits,
  forced predecessor writes, read-committed capture, lost source acknowledgements, physical retries,
  and operation-ID conflicts.
- **Independent soak owner:** operate the immutable release-candidate black-box soak after all other
  gates pass; retain every failed and invalid attempt.

## Next-cycle review plan

Cycle 5 may add the admission-neutral aggregate row codec and `VnodePartialV2` contract tests, but it
must not wire live restore or relax admission. Its closing independent reviewer must repeat:

1. **AI slop:** reject invented compatibility, vague field semantics, result-derived limits,
   copy-pasted live Rust/rkyv ABI, and claims unsupported by primary evidence or executable vectors.
2. **Over-engineering/hot path:** prove no codec/reflection/sort/hash/object-store/LSM work enters the
   per-row event loop; measure or explicitly defer the restore-spool and Kafka-transaction costs.
3. **Unused code:** require every DTO/decoder/registry entry to be consumed by goldens/fuzz tests;
   remove alternate formats, speculative codecs, and unused adapters.
4. **Production readiness:** keep managed state, LSM selection, delivery, exactly-once, and production
   blocked; audit reserve-before-fetch, shadow abort, spool cleanup, and ownership fences.
5. **Documentation:** maintain one normative binary specification and update plans by reference;
   remove stale or duplicated claims rather than accumulating another design narrative.
6. **Tests:** require fresh/populated/EMPTY/REFERENCE, null-only, prefix-overflow,
   split/coalesced, late-group rollback, maximum/max-plus-one, malformed range/digest/ancestry,
   N/N-1, arbitrary bounded input, and no-panic/no-amplification vectors, plus unchanged admission
   guards.

Any correctness, resource, compatibility, delivery, owner, or independent-soak gap keeps production
and admission blocked.

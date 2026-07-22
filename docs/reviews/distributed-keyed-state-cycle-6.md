# Distributed keyed state — Cycle 6 review

- **Date:** 2026-07-22
- **Scope:** provisional backend-neutral model, deterministic workload and result oracle,
  occurrence-addressed lifecycle fault cuts, literal fixtures, and validation-only CLI
- **Cycle verdict:** **APPROVE** for the provisional C1 engineering slice
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

C1 is implemented against the provisional v1 contract, but every result is explicitly ineligible
for qualification. This review does not select Fjall or RocksDB, approve the candidate profile,
produce backend evidence, establish a source/operator/sink delivery guarantee, or make distributed
state production-ready. No independent production soak has run.

## Reviewed changes and evidence boundary

Cycle 6 covers `2bb77a0e..42ffe66e`:

- `2bb77a0e` and `2be0f423` specify the provisional model and due-scan row identity;
- `e9e4db50` adds the bounded deterministic workload, semantic oracle, result schema, and strict
  result generation and validation;
- `f2ecbbc2` adds immutable lifecycle cuts for persistence, snapshot/export, restore, and cleanup,
  plus the literal wire and result fixtures;
- `ca2ec061` closes the semantic and resource boundary matrix and avoids whole-state cloning on
  ordinary replay; and
- `42ffe66e` exposes only bounded profile/result validation and checks that path in CI.

The tool has its own workspace and lockfile. It depends on no LaminarDB runtime crate, Arrow,
DataFusion, async runtime, Fjall, or RocksDB. Its CLI has no backend execution command. Candidate
performance, resource, fault, endurance, and selection runs remain prohibited until named human
owners approve both the final profile and candidate-neutral runner.

A separate reviewer recomputed the model hashes, literal request and observation bytes, aggregate,
timer/window, due-scan, fire/delete, and join counters and digests from the ADR algorithms. That
calculation did not call the Rust encoders or `generate_model_result`. It matched the checked-in
vectors, including request SHA-256
`123ed9013922ea3c89c11d4a3eadfb24a98b510aaf0ab152ed5529f52721f0a4`, observation SHA-256
`a76c83d79219e9ab92991f18f9b597122cf51c1a0083957aab4ebaf67f652174`, and all four aggregate
fixture digests. This is evidence against a circular golden, not backend qualification evidence.

## Six review passes

### 1. AI slop and evidence

**Result: pass for C1.** The generator is counter-addressed and directly comparable with sequential
generation. The model result binds exact profile bytes, ordered model inputs, case, counters, four
digests, and fixed identities. Literal and independently recomputed vectors do not derive expected
bytes by calling the implementation under test. Claims are limited to offline logical semantics.

Review fixes included late-allocation preflight, post-dedup aggregate accounting, deterministic
lowest-row value provenance, a distinct timer due-row identity, immutable snapshots, and exact
raw-profile line endings for the profile hash. No candidate-profile number is presented as an
observed performance result.

### 2. Over-engineering, hot path, and latency

**Result: pass for this isolated tool; production latency remains unproven.** The reference model is
not linked to the runtime or record hot path. Ordinary replay validates and observes a pre-cut,
then installs already-validated mutations without cloning the whole state or placing a fallible
hook inside the install. A maximum 4,096-request case guards the bounded path. Snapshot and restore
still copy off-side state because immutability and atomic replacement are their tested semantics.

No executor, cache, background thread, tuning knob, LSM abstraction, or async runtime was added.
Candidate service time, queue time, p99/p99.9, write stalls, compaction, checkpoint overlap, and
restore RTO remain C2/C3 evidence requirements. Nothing here establishes production hot-path cost.

### 3. Unused code

**Result: pass.** The model, lifecycle API, result validator, schemas, fixtures, and CLI path are all
exercised. The CLI deliberately cannot generate results or invoke a backend. There is no dormant
Fjall/RocksDB adapter, runtime feature, admission option, alternate state trait, or production
dependency to remove.

### 4. Production readiness, delivery, and soak

**Result: BLOCK, correctly fail-closed.** C1 proves neither disk durability nor process-failure
atomicity. Real kill, torn write, corruption, `ENOSPC`, file-descriptor pressure, fencing, N/N-1,
endurance, compaction/resource slopes, and portable recovery remain unexecuted. Fjall is still the
incumbent library to qualify, not a selected distributed-state backend; RocksDB remains the planned
same-contract comparison.

No source cut, checkpoint decision, sink fence, ambiguous-commit reconciliation, or connector
capability matrix has been exercised with state. At-least-once is not established by a storage
model, and exactly-once remains separately blocked by `[LDB-0013]`. Most importantly, an independent
operator has not run an immutable release artifact through the frozen black-box soak charter.
Backend endurance, self-run chaos, unit tests, benchmarks, and canaries cannot replace that soak.

### 5. Documentation and over-documentation

**Result: pass.** The model ADR is the single authority for C1 encoding and semantics. The Phase 0
plan owns sequencing and approval gates; the existing soak charter owns production certification.
Future-tense and stale “boundary matrix incomplete” text was removed. The documents consistently
say provisional, ineligible, no backend selected, no admission change, and no exactly-once claim.
No duplicate research diary or backend-selection ADR was added.

### 6. Tests and checks

**Result: pass for C1.** Exact Rust 1.95 checks after the closing fixes were:

| Command/check | Result |
|---|---|
| `cargo +1.95.0 test --locked --all-targets` | PASS, 67/67 |
| `cargo +1.95.0 clippy --locked --all-targets -- -D warnings` | PASS |
| `cargo +1.95.0 fmt --all -- --check` | PASS |
| fully qualified cluster admission regression | PASS, 1/1; 1,660 filtered out |
| `git diff --check` | PASS |

The admission command was:

```text
cargo +1.95.0 test -p laminar-db --lib --no-default-features --features cluster \
  db::tests::cluster_query_shape_admission_is_pre_mutation_and_mode_derived -- --exact
```

Two earlier shells timed out while stale Cargo children from overlapping review checks held the
build lock; they produced no test result and are not counted as evidence. After those exact stale
processes were stopped, one clean cold build completed and the named regression passed. It confirms
that cluster query-shape admission is still pre-mutation and mode-derived; keyed, windowed, and
stateful shapes remain fail-closed.

The matrix covers all three scenarios; direct/sequential generation; aggregate deduplication;
timer mutation, populated due scan, and atomic fire/delete; both join sides and 0/1/8/64 fanout;
strict ordering and duplicates; exact/max-plus-one scan and restore limits; all four restore budget
dimensions; row, width, batch, replay, and canonical-byte ceilings; multi-mutation pre/post cuts;
independent per-phase fault ordinals; zero-hook invalid/empty operations; immutable snapshot retry;
persist/reopen; bounded file reads; strict schema/replay comparison; literal wire bytes; and frozen
result tuples. These are semantic-model tests, not candidate crash, latency, resource, or soak tests.

## Next-cycle implementation and review plan

Cycle 7 should add only the candidate-neutral C2 runner and evidence schema first: exact case
matrix, pacing and warmup, service-versus-queue latency, mergeable raw histograms, resource formulas,
invalid-run rules, fault schedules, environment/binary/lock/profile identity, and immutable evidence
retention. It must still contain no runtime dependency and may not relax admission.

Before an adapter consumes the model API, make validated `ModelProfile` fields opaque. Add explicit
result rejection tests for negative, floating, and above-`u64` numbers, a nonzero batch-fault retry,
and empty-stream replacement of a populated vnode. These are owned C2 hardening tasks, not evidence
gaps in the approved C1 semantic matrix. Keep lifecycle full-map copies oracle-only; the logical
64 MiB replay bound is not an RSS or hot-path claim.

Qualification execution remains blocked on named workload and operations owner approval. In
particular, **DKS-Q2-001** requires a reviewed cross-platform deterministic Zipf sampler and explicit
assignment of hot-mix versus Zipf cases; changing the generator requires a new identity and
goldens. Only after the profile and runner are approved may equivalent exact-pin Fjall and RocksDB
candidate performance, fault, resource, and endurance runs begin.

The next closing review repeats the six passes: independently derive evidence; challenge runner
complexity and measurement perturbation; remove unused fields and paths; audit production, delivery,
and independent-soak blockers; keep one authority per contract; and test deterministic evidence,
invalid attempts, exact limits, and fault schedules. Production readiness cannot be claimed until
the separately owned independent soak passes on the immutable release artifact.

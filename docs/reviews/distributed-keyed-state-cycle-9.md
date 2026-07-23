# Distributed keyed state — Cycle 9 review

- **Date:** 2026-07-23
- **Scope:** non-circular C2 workload identity and first long-stream matrix audit
- **Identity-framing verdict:** **APPROVE** as admission-neutral design input
- **M1 matrix verdict:** **REJECT**; no case body, expectations, plan, or result may be created
- **Candidate execution verdict:** **BLOCK** on DKS-Q2-001 through DKS-Q2-008
- **Backend selection verdict:** **BLOCK** additionally on DKS-Q2-009/C3
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` remain unchanged

Cycle 9 separates C2 long-stream identity from C1, closes the direction of the evidence hash graph,
and subjects a concrete six-case workload sketch to arithmetic, lifecycle, scope, and identity
review. The review rejected M1 rather than turning internally consistent numbers into an invalid
qualification plan. This cycle adds no workload encoder, schema, benchmark command, backend
adapter, engine call, performance result, product runtime path, or admission change.

## Reviewed changes and outcome

This review covers `a7e069a3..01c44d9e`:

- `9b4bf81a` gives C2 its own case body, counter coordinates, request/observation/state identities,
  seeded stream identity, and result/evidence boundary without consuming C1's multi-seed
  `model_input_sha256`;
- `9dff7e61` removes derived values and generated artifacts from upstream identity inputs, separates
  deterministic expectations from operational preflight provenance, and fixes source-archive and
  mathematical-source scopes; and
- `01c44d9e` records and rejects M1 after independent arithmetic, lifecycle, product-scope, and
  identity review.

The evidence dependency direction is now case body → case ID → selected seed/stream instance →
expectations/preflight → plan → detached approval → result → evidence manifest → completion. No
result, candidate, gate, approval, or generated artifact points back into stream identity. C1
`ModelCase`, workload-v1, its fixed-seed vector, and its goldens remain unchanged.

M1's useful arithmetic passed: exact logical key-plus-value totals/bands, timer `/5` and join `/125`
phase alignment, 99,921,420 scheduled runtime requests, 12,789,941,760 raw rows, about 3,000 GiB of
fresh setup logical key-plus-value materialization, the timer-ring scan counts, and the
189/253/317 join output bands. M1 was nevertheless rejected because arithmetic consistency is not
lifecycle validity.

Its blockers are concrete:

- its setup/final persistence shorthand cannot be represented by the case-body union;
- its modulo width policy makes rank zero cheapest and correlates width with vnode placement;
- several logical shapes do not demonstrate compatibility with artifact/transition row and byte
  caps, while timer/join codecs and transition rosters do not yet exist;
- every 96-GiB case is Zipf, so none proves cold-read spill behavior;
- the timer's sampled companion records are not ordered timer reschedules;
- the join is an immutable opposite-side range probe, not a dynamic two-sided join with expiry;
- there is no candidate-observed full-state witness at the warmup boundary;
- service, runner/interference, join write-amplification, warmup-debt, schedule, and campaign-resource
  gates remain open; and
- the candidate-neutral runtime preflight alone would process 49,960,710 requests and
  6,394,970,880 rows without an approved bounded oracle design.

Artifact-limit findings are deliberately conditional. The initial managed aggregate codec is
frozen, but mapping an M1 synthetic record to an artifact row, future timer/join codecs, and putting
all vnodes in one transition have not been frozen. M1 therefore fails to prove compatible
dimensions—it does not prove the layout of nonexistent artifacts.

## Six review passes

### 1. AI slop and evidence integrity

**Result: pass for identity framing; M1 rejected.** Three independent read-only AI roles challenged
hash dependencies, arithmetic, lifecycle, and scope. They are not substitutes for named human
workload and operations owners. Review removed the circular dependency on C1's complete seed vector,
separated deterministic and operational artifacts, fixed phase/runtime/sample ordinal meanings,
and kept raw rows distinct from post-dedup backend operations.

The first matrix was not accepted merely because its byte totals were exact. Review exposed an
unencodable persistence policy, implicit class mapping, conditional product-limit assumptions,
static-fixture join masquerading as a join lifecycle, non-timer metadata masquerading as timer
rescheduling, an atomic-read-modify-write implication not provided by the adapter, and a missing
actual warmup-state witness. All remain fail-closed.

### 2. Overengineering, hot path, and latency

**Result: pass after pruning; performance remains unproven.** Rejected pseudo-implementation detail
was reduced to the arithmetic needed to explain the failure. No Cartesian matrix was introduced.
Short semantic/boundary controls, finite latency attempts, 24/72-hour endurance, C3 concurrency,
product recovery, and independent soak remain separate evidence populations.

The proposed resident timer rate would return about 819 MiB/s before ring/oracle copies. The full
campaign is at least 45 scheduled hours before setup, cooldown, resource tail, and failure retries.
Finite 2,700-second cases cannot populate slope gates beginning at 21,600 seconds. M2 must bound
counter/sampler, scheduler, queue, result-ring, oracle, memory-bandwidth, warmup compaction debt,
direct-I/O conditioning, and null-adapter interference before offered rates are approved. No
observed overhead may be subtracted from service latency.

### 3. Unused code and API

**Result: pass.** This cycle changes design/review documents only. There is no public or private C2
encoder, scenario registry, schema, result type, workload runner, backend adapter, feature flag,
CLI subcommand, benchmark entry point, or product dependency. Synthetic C1 and Zipf feasibility
code remains isolated exactly as reviewed in prior cycles.

### 4. Production readiness, delivery, backend selection, and soak

**Result: BLOCK, correctly fail-closed.** No Fjall or RocksDB operation ran and neither backend is
selected. M1 cannot establish checkpoint/restore formatting, vnode ownership, source offset cuts,
sink prepare/commit/fencing/reconciliation, watermark behavior, delivery guarantees, rebalance,
or cluster admission. DKS-Q2-001 through DKS-Q2-009 remain open.

The independent production soak still requires its own manifest, driver/counter domain, fresh
precommitted seed, immutable release artifact, real source and sink coordinates, external oracle,
fault schedule, and independent operator. It may reuse an approved Zipf specification and
independent numerical goldens, but not the backend generator implementation/binary or C2 evidence
identity. No production-ready claim is possible before that soak completes independently.

### 5. Documentation and over-documentation

**Result: pass after consolidation.** One ADR owns the C2 identity graph and records why M1 is
ineligible. Detailed rejected lifecycle pseudo-code was removed; only audited arithmetic and the
specific rejection lessons remain. The runner ADR remains the authority for gates and execution
blockers, while the soak charter remains the production-certification authority.

No tracked research document was removed. The previously reviewed sources remain relevant; Claude
memory and ignored external material were not treated as authority. No new survey or duplicated
state-of-the-art narrative was added in this cycle.

### 6. Tests and checks

**Result: scoped pass; root all-target compilation blocked by host paging capacity.** The cycle
changes Markdown only. From `tools/state-backend-qual`, the captured commands were:

```powershell
$env:CARGO_BUILD_JOBS='1'
cargo +1.95.0 test --locked --all-targets
cargo +1.95.0 test --locked --all-targets --features zipf-feasibility
cargo +1.95.0 clippy --locked --all-targets -- -D warnings
cargo +1.95.0 clippy --locked --all-targets --features zipf-feasibility -- -D warnings
cargo +1.95.0 fmt --all -- --check
```

From the repository root, the captured commands were:

```powershell
$env:CARGO_BUILD_JOBS='1'
cargo +1.95.0 test -p laminar-db --lib --no-default-features --features cluster `
  db::tests::cluster_query_shape_admission_is_pre_mutation_and_mode_derived -- --exact
git diff --cached --check
```

| Check | Result |
|---|---|
| qualification-tool default tests | PASS, 85 tests |
| qualification-tool Zipf-feature tests | PASS, 94 tests |
| default and Zipf-feature clippy | PASS |
| qualification-tool formatting | PASS |
| fully qualified cluster admission regression | PASS, 1/1; 1,660 filtered out |
| staged whitespace check | PASS |

A root-workspace `cargo +1.95.0 test --locked --all-targets` attempt did **not** pass: the Windows
host exhausted its paging file while compiling connector integration targets, rustc reported
allocation/mmap failures, and compilation stopped before tests. An earlier 120-second harness also
timed out while OpenSSL was compiling. Neither attempt is counted as evidence. The relevant
standalone tool and cluster-admission checks passed with `CARGO_BUILD_JOBS=1`; the broad workspace
suite must be repeated on a sufficiently provisioned host/CI rather than relabelled as a product
failure or a pass.

## Next-cycle implementation and review plan

Cycle 10 remains specification/qualification work and does not expose a measurement command:

1. derive and independently review an M2 width/placement policy and exact resident/spill dimensions
   that avoid M1's hottest-rank and vnode-correlation defects while preserving any claimed timer
   scan population;
2. replace the static probe with an exact dynamic two-sided arrival/expiry/replenishment lifecycle,
   and define real timer rescheduling, complete entity/key/value encodings, range byte/lookahead,
   canonical dedup/conflicts, and serialized-read then atomic-mutation semantics;
3. choose a representable setup/final persistence policy, an interference-qualified warmup-boundary
   witness or explicitly narrower correctness claim, a bounded independent oracle, and at least one
   true cold-read spill case; and
4. freeze the service/runner/schedule/resource gates and statically audit pinned Fjall/RocksDB
   mappings before any owner-approved conformance or performance execution.

The next cycle again performs AI-slop, overengineering/hot-path, unused-code, production-readiness,
over-documentation, and test review. Any missing lifecycle, cap proof, gate, exact build,
workload/operations owner, or independent evidence keeps candidate execution blocked. Backend
selection remains additionally blocked on C3, and production remains blocked on the complete
source/sink/exactly-once/rebalance lifecycle plus independent release soak.

# Distributed keyed state Cycle 20 review

> **Historical boundary:** Cycle 21's
> [owner decision record](../reports/distributed-state-cycle-21-owner-decisions-2026-07-24.md)
> supersedes this review's bounded-memory soak description and Cycle 21 entry questions. Bounded
> memory is now reference/conformance-only; the current product-soak charter targets local spill
> only. The findings below remain historical review provenance, not current execution authority.

- **Date:** 2026-07-24
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle commits:** `ca369bfc`, `fa9a6aac`
- **Cycle outcome:** `WORKING_STATE_PLACEMENT_ANALYZED_PRODUCT_AND_BACKEND_DECISIONS_PENDING`
- **Production backend selected:** none
- **Bounded-memory cluster profile approved:** no; reference/conformance use only
- **Candidate construction or execution authorized:** no
- **Cluster admission:** unchanged and fail-closed under `[LDB-4007]` and `[LDB-0013]`
- **Production verdict:** **NO-GO**

## Outcome

Cycle 20 answers the architecture question without selecting an engine. LaminarDB needs a managed
mutable working-state capability, but correctness, checkpointing, rebalance and exactly-once do not
require RocksDB, Fjall, an LSM, or durable local disk by name. The existing `StateBackend` remains
the immutable cluster-shared artifact/seal authority; source positions, checkpoint decisions,
ownership epochs and sink commits remain separate authorities.

The reviewed [placement analysis](../reports/state-working-state-options-2026-07-24.md) retains one
qualified embedded local-spill backend as the intended general production profile. The in-memory
implementation remains required for semantics and conformance. Turning it into a separately
supported hard-bounded cluster profile is an optional product and operations decision with no
implementation/admission schedule in this cycle. Remote-primary/object-store state is a future
subsystem decision, not another adapter in the current bake-off.

The active backend set does not expand. RocksDB remains at owner-gated source closure, Fjall needs an
explicit fork/upstream ownership decision, and redb 4.1.0 remains at its bounded native prescreen.
`heed`/LMDB is considered only after a redb-specific implementation/lifecycle failure; a redb
single-writer C3/tail failure rejects that shared architecture rather than triggering repeated
single-writer probes. SQLite stays a paper fallback, SlateDB stays remote-primary research, and the
source-supported sled/ParityDB/Tonbo/SurrealKV dispositions do not advance.

Cycle 20 does not split the current Phase 0 gate. Phase 1 remains blocked until every existing Phase
0 exit condition, including selection of the general local-spill backend, passes or a later accepted
ADR/plan amendment defines a smaller named owner-approved gate.

## Six-pass cycle review

### 1. AI slop and contract consistency

**Pass after independent correction.** The first review rejected an undefined early Phase 1 entry,
a prematurely committed second product profile, stale SlateDB 0.13.x, an unauditable combined-engine
row, an overclaimed libMDBX fork consequence, and language that could reinterpret disk-oriented
DKS-Q2-006 for memory. Corrections restore the current Phase 0 authority, pin SlateDB 0.14.1, give
each retained alternative an exact reason/source, limit the LMDB trigger, and require a separately
versioned applicability contract before any memory profile evidence.

Three independent final reviews returned PASS for architecture/facts, backend fairness and
authority/phase/soak consistency. Facts, LaminarDB inferences, missing evidence and owner decisions
remain distinct.

### 2. Overengineering and hot path

**Pass.** The cycle adds no candidate merely to broaden comparison and does not turn remote state or
a custom service into Phase 0 work. Memory certification is opt-in because it adds an admission,
support and independent-soak matrix; otherwise the implementation remains the already-required
reference. The hot-path contract still forbids per-row database transactions, futures, remote calls
or fsync, coalesces work per Arrow batch, and moves applicable cold/blocking work off event loops.

### 3. Unused code and dependencies

**Pass.** Cycle 20 changes tracked documentation only. It adds no engine crate, C/C++ library,
adapter, feature flag, cache layer, service, schema implementation, workflow or unused runtime
abstraction. The isolated redb prescreen dependency remains confined to its existing tool workspace.

### 4. Production readiness, delivery and independent soak

**NO-GO, correctly fail-closed.** No working-state backend has passed common native-host
correctness, tail, resource, persistence, recovery, fault or 24/72-hour endurance gates. Ownership
epochs, checkpoint freeze/export/seal, restore-before-activate, rebalance fencing and per-vnode
cleanup are not implemented for the new service. Grouped aggregates, windows and stateful joins
therefore remain rejected in cluster mode.

Exactly-once remains a per-combination source/state/coordinator/sink claim. Local durability cannot
manufacture replay-stable source handoff or transactional/idempotent fenced sink publication, and a
non-replayable source still needs durable ingress or stays excluded.

The [production-soak charter](../testing/distributed-state-production-soak-charter.md) now binds each
attempt to one exact `(scenario, working_state_profile)` and applicability hash. Common faults and
thresholds remain mandatory; bounded memory adds near-capacity skew/timer/join growth, allocator/RSS,
controlled exhaustion without cursor/output advance and remote restore/source-replay RTO; local
spill adds cold cache, maintenance, disk fill/corruption and complete local-volume loss. One
profile's evidence cannot widen another. No independent release-candidate soak has run.

### 5. Documentation, stale research and overdocumentation

**Pass.** One report owns the placement/engine delta; the ADR, plans and soak charter carry only the
normative consequences. Tracked Cycle 16–19 reports remain because they preserve exact-source,
construction, rejection and decision provenance; deleting them would discard evidence rather than
remove duplication.

The ignored, untracked obsolete local drafts `docs/research/extensible-schema-traits.md` and
`docs/research/schema-inference-design.md` were removed from this workspace. The ignored local
Claude prompts `.claude/agents/lookup-table-validator.md` and `.claude/agents/state-auditor.md` were
corrected to discover active runtime stores/caches rather than assume redb/foyer. These are local
workspace hygiene and cannot be represented as tracked deletion/content commits under the current
`.gitignore`; the tracked report records that limitation explicitly. No unrelated ignored ADR was
bulk-deleted.

### 6. Tests and empirical limits

**Documentation checks pass; production evidence remains absent.** `git diff --check`, relative
links across all changed tracked documents, docs-only scope, obsolete-local-file absence and stale
Claude-prompt searches pass. Independent reviewers checked the corrected plan/report against the
current tree and primary Flink, Arroyo, SlateDB, LMDB/heed, SQLite, libMDBX, sled, ParityDB and Tonbo
sources.

No runtime code, backend build, workload, latency, persistence, crash, recovery, endurance,
connector, exactly-once or product-soak test ran in this cycle. Source review is not qualification.

## Cycle 21 entry boundary

The next decisions are explicit rather than inferred from continuation:

1. product and operations owners decide whether bounded-memory cluster support is worth its second
   support/soak matrix or remains reference-only;
2. the named owners approve, retain or defer the maintenance-health v2 direction; and
3. only the already-authorized candidate-specific gates may then proceed: redb's detached native
   prescreen approval, RocksDB source closure after the final contract, or an explicit Fjall fork/
   upstream ownership commitment.

Until the existing Phase 0 gate completes or is formally amended, do not add a runtime backend,
start Phase 1, run a gate-bearing candidate, select by elimination, change delivery claims, or relax
cluster admission.

# Distributed keyed state — Cycle 10 review

- **Cycle status:** complete for static backend screening and M2 feasibility; qualification work continues
- **M2 verdict:** reviewed components **PASS feasibility**; complete matrix and executable schemas **BLOCK**
- **Candidate execution verdict:** **BLOCK** on DKS-Q2-001 through DKS-Q2-008
- **Backend selection verdict:** **BLOCK** additionally on DKS-Q2-009/C3
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` are unchanged

Cycle 10 did not implement a backend, add a dependency, expose a measurement command, or weaken
cluster admission. It exact-source audited the retained Fjall/RocksDB candidates, screened redb
4.1.0, and replaced M1's rejected lifecycle sketch with independently challenged M2 feasibility
components. Those components are inputs to a future canonical workload, not qualification evidence.

## Reviewed changes and outcome

The cycle contains three separately committed decisions:

- `3c7ff85b` audits the exact Fjall 3.1.8/lsm-tree 3.1.8 and RocksDB wrapper/engine sources. It
  fails unmodified Fjall on stable pressure/stall telemetry and blocks the current RocksDB binding
  on an uncovered write-buffer-manager stall path. Neither result selects a backend.
- `bd391aef` adds the requested redb 4.1.0 screen. redb can implement bounded C1 atomic/range
  semantics, but is deferred pending an approved non-LSM telemetry/cache/persistence mapping and a
  bounded writer/commit/recovery microprobe. It is not a measured C3 failure and was not added to
  the profile, dependency graph, or lockfiles.
- `cf2a6557` records balanced width/placement constraints and corrected under-target aggregate,
  timer, and dynamic two-sided join shapes. It also freezes enough abstract timer/J2 lifecycle to
  prove feasibility while leaving canonical policy tags, codecs, oracle, schedule, gates, and owner
  approvals blocked.

The source audit pins exact archive hashes and upstream revisions. Its disposition is static design
evidence only: Fjall **FAIL DKS-Q2-006 as published**, RocksDB **BLOCK DKS-Q2-006**, and redb
**DEFER** before admission to the comparison. DKS-Q2-007 and C3 remain open for any retained or
newly admitted candidate.

M2 corrects the rejected M1 defects without pretending to close the matrix:

- hash-rotated Latin quartets remove rank/vnode width correlation and provide a separate hot-wide
  control without putting SHA-256 in candidate service timing;
- every resident/spill logical byte target is now below 4 GiB/96 GiB and every reviewed per-vnode
  row/key/state total is numerically below the current artifact caps;
- timer state uses an explicit `C` window/output population, timer-bearing `D` subset, dynamic
  due-roster rescheduling, bounded 512-entity scans, and a five-operation atomic fire roster;
- the spill timer increases actively exercised timer bundles from 5,347,799,040 bytes to
  21,391,011,840 bytes, but still requires external cache-miss/device-read evidence; and
- J2 is a real dynamic two-sided arrival/expiry/replenishment lifecycle. Its corrected period-200
  classifier exactly preserves 0/1/8/64 fanout over `W=12,800Q`; it deliberately does not claim
  late/idling semantics, concurrent atomic RMW, sink commit, or product join correctness.

The remaining M2 blocker is substantive, not clerical. There is still no closed v2 scenario/body
registry, exact timer/join codec, literal golden set, bounded oracle, feasible finite preflight,
setup/reopen/final-persist encoding, true cold-I/O gate, approved rate/schedule/interference/resource
contract, or named owner approval. Consequently no case body, expectations object, runner plan, or
candidate result may be created.

## Six review passes

### 1. AI slop and evidence integrity

**Result: pass after corrections; executable M2 remains blocked.** Independent read-only roles
challenged source claims, arithmetic, lifecycle, product scope, and documentation. The review found
and corrected issues that would otherwise have become plausible-looking false evidence:

- RocksDB's stopped-state wording originally overstated ticker coverage; the final report identifies
  the verified write-buffer-manager gap instead.
- redb was initially labelled a static failure because of its sole writer and missing internal
  counters. Review correctly observed that C3 is empirical, external observations are authoritative,
  and non-LSM debt semantics are not frozen; the disposition became **DEFER**, not failure.
- the first J2 classifier had a period that did not divide `W`; the period-200 replacement was
  independently recomputed before acceptance.
- initial aggregate/timer spill counts exceeded 96 GiB by 516,096/577,536 bytes; the checked-in
  dimensions are the corrected lower balanced shapes.
- the first timer role sketch attempted to reschedule entities without timer rows. The final `D`
  subset and dynamic due-roster lifecycle removes that contradiction.
- the timer framing proxy was initially described as a cap proof and 5.35 GB as GiB. Both claims
  were narrowed/corrected before cycle close.

No AI review is represented as workload-owner approval, production review, independent-soak
operation, or empirical candidate evidence. The static reports cite exact packages and separate
facts, inferences, proposed adapter duties, and unproved target-host behavior.

### 2. Overengineering, hot path, and latency

**Result: pass for scope; latency qualification remains blocked.** The project still retains only
two proposed comparison candidates. redb gets a cheap, bounded pre-screen before any third adapter;
there is no speculative dependency or abstraction. The losing retained adapter is still removed
after equivalent evidence.

M2 requires width hashes to use bounded precomputation or a reviewed closed-form path outside
candidate service timing, and J2's greedy load proof cannot become runtime routing. Generator,
scheduler, oracle, result-ring, and observation costs remain charged and require null/control
gates. Timer scans use a balanced 512-entity roster; arbitrary all-wide fire is explicitly rejected
at 12,058,864 logical mutation bytes. Neither logical state size nor an 8-GiB cache comparison is
treated as cold-I/O proof.

The source audit also blocks unsafe latency assumptions: Fjall sorted ingest holds its global journal
mutex; RocksDB calls are synchronous and its database write controller is shared; redb has one
blocking database writer and synchronous durable commit/repair/compaction. None is assumed to be an
online rebalance hot path without targeted evidence.

### 3. Unused code and API

**Result: pass.** Cycle 10 is documentation-only. It adds no runtime service, adapter, schema,
feature flag, dependency, generated artifact, measurement command, or dead compatibility layer.
The redb archive was read as audit input but is absent from project manifests and locks. M1 remains
only as explicitly rejected audit history needed to explain M2 constraints; it has no encoder or
runner path.

The two temporary production-reader dead-code allowances in `aggregate_state/artifact_v1.rs` and
`vnode_partial/v2.rs` remain explicitly owned by **DKS-P1-001**. They are due for removal by
2026-08-31 or the first trusted production consumer, whichever comes first; Cycle 10 added no new
allowance and did not close that follow-up.

### 4. Production readiness, delivery, backend selection, and soak

**Result: BLOCK, correctly fail-closed.** C2 can at most qualify backend-local storage traffic.
DKS-Q2-009 must still prove shared-database disjoint-vnode lanes, hot-writer/victim tails, snapshots,
restore activation, cleanup, and global resources. Backend selection also needs physical cache-loss,
N/N-1, fault, and an owner-approved 24–72-hour backend endurance matrix.

Production additionally needs vnode ownership/fencing, checkpoint generation freeze/export/restore,
source offset cuts, sink capability negotiation and prepare/commit/fencing/reconciliation, delivery
semantics, rebalance, and operator verticals. Exactly-once remains separately blocked; stronger
local persistence does not make a source/sink transaction. An independent release-candidate soak
is still mandatory, but its duration and event floor remain unfrozen and unapproved. Before it runs,
they must be precommitted along with its own operator, driver, manifest, seeds, external oracle,
source and sink coordinates, and immutable release artifact; only then can its evidence support a
production-ready claim.

### 5. Documentation and over-documentation

**Result: pass with an explicit consolidation trigger.** One workload ADR owns C2 identity, the
rejected M1 record, and the reviewed M2 feasibility constraints; the backend source findings remain
in one report linked from the main ADR/runner contract. The added detail is retained because it
contains exact counterexamples, cap arithmetic, and lifecycle boundaries that stopped invalid code.

When a canonical M2 body and goldens are accepted, the rejected M1 arithmetic is moved to this
review lineage or a compact appendix so it cannot compete with the normative registry. Superseded
research notes must be removed rather than left as a second source of truth. No tracked research
document was found both superseded and unnecessary to the decision trail in this cycle. The local
`.claude` path is an untracked junction to private environment configuration outside the repository;
it was not treated as project content or mutated.

### 6. Tests and checks

**Result: pass for the affected qualification/admission surface.** With `CARGO_BUILD_JOBS=1`:

| Check | Result |
|---|---|
| qualification-tool default `test --locked --all-targets` | PASS, 85 tests |
| qualification-tool `zipf-feasibility` tests | PASS, 94 tests |
| default and `zipf-feasibility` clippy with `-D warnings` | PASS |
| qualification-tool formatting | PASS |
| exact cluster admission regression | PASS, 1/1; 1,660 filtered out |
| working-tree whitespace check before commits | PASS |

A mistaken invocation using nonexistent feature name `zipf-reference` failed immediately and is not
evidence; the configured `zipf-feasibility` command above passed. The root all-target workspace
suite was not repeated for documentation-only changes after Cycle 9 established that this Windows
host exhausts its paging file; sufficiently provisioned CI still owns that broad run.

## Cycle 11 implementation and review plan

Cycle 11 remains qualification-contract work and exposes no C2 candidate measurement command:

1. freeze a bounded candidate-independent M2 oracle with exact heap/scratch/artifact ceilings,
   analytic or finite-period preflight, cleanup, and fail-closed deadline;
2. close Z3/Z1 for the reviewed aggregate/timer/J2 roles and policies, including exact keys/values,
   timer/join codecs, constant-time routing, range bounds/lookahead, conflicts, schemas, and literal
   independent goldens;
3. encode setup/reopen/final persistence and the narrower setup/final equality claim, then freeze
   rates, counts, schedule, cold-I/O, service, runner, observation, and resource gates for owner
   review;
4. freeze a separate bounded, non-gating redb writer/Immediate-versus-2PC/quick-repair/crash-open
   pre-screen protocol. Run it on the target Linux/XFS/NVMe class only if the protocol is reviewed
   and the target host plus operations owner are available; otherwise defer execution to a later
   cycle. Its result cannot enter C2 evidence or expose the C2 runner; and
5. decide whether Fjall gets an audited telemetry patch/upstream path and whether RocksDB gets a
   complete stall source; reject any candidate that cannot meet DKS-Q2-006 without invented zeros.

The cycle again ends with AI-slop, overengineering/hot-path, unused-code, production-readiness,
overdocumentation, and test review. Candidate execution remains blocked until DKS-Q2-001 through
DKS-Q2-008 and owner approval close; selection additionally requires C3; production additionally
requires delivery/rebalance/fault/endurance work and the independently operated release soak.

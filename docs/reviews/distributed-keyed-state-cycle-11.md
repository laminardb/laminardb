# Distributed keyed state Cycle 11 review

- **Date:** 2026-07-23
- **Branch:** `feature/distributed-keyed-state-adr`
- **Cycle verdict:** **APPROVE** the validation and provisional contract work
- **Candidate execution verdict:** **BLOCK** on DKS-Q2-001 through DKS-Q2-008 and owner approval
- **Backend selection verdict:** **BLOCK** additionally on DKS-Q2-009/C3
- **Production/admission verdict:** **BLOCK**; `[LDB-4007]` and `[LDB-0013]` are unchanged
- **Independent soak:** not run; no production-ready claim is permitted

## Outcome

Cycle 11 did not implement or select a state backend. It established what this Windows/WSL host can
honestly test, corrected the LSM-shaped resource contract additively, rejected unmodified SurrealKV,
defined a bounded non-gating redb screen, and replaced an infeasible 6.39-billion-row M2 preflight
in the provisional contract with a bounded static proof plus one-generation runtime commitments.

The cycle commits are:

- `1652fb0b` classifies local WSL/Docker capability. Linux builds, deterministic wires, connector
  integration, and process-`SIGKILL`/reopen smoke are possible; XFS quota, native-NVMe latency and
  write attribution, cache-loss/power-loss, endurance, qualification, and independent soak are not;
- `9cea31c2` moves the repository Docker build to the workspace's Rust 1.95 floor;
- `389b94e7` rejects unmodified SurrealKV 0.21.2 after exact-source snapshot-retention and range-
  snapshot bookkeeping defects, plus unresolved drain/durability/telemetry risks;
- `e7b04c69` adds candidate-neutral profile/resource v2 while retaining v1 byte-for-byte as an
  immutable regression fixture;
- `4c392ebc` records the exact Rust 1.95 Docker image/toolchain command and excludes an accidental
  moving-stable Rust 1.97.1 run from pinned evidence; and
- `6b05135f` records the bounded M2 oracle/lifecycle decision and proposed redb 4.1.0 prescreen,
  while explicitly preserving every execution and production block.

The exact-source disposition is still: unmodified Fjall 3.1.8 **FAILS** its applicable DKS-Q2-006
observability obligation; the current RocksDB 10.4.2 binding is **BLOCKED** on complete stall-path
coverage; redb 4.1.0 is **DEFERRED** pending a separately approved prescreen and additive profile;
unmodified SurrealKV 0.21.2 is **REJECTED**. This is not selection by elimination.

## Material decisions

### Local WSL/Docker boundary

Docker Desktop is usable here for pinned Linux build and functional smoke work. The confirmed path
is a Docker-managed ext4 VHDX on NTFS and shared Windows storage, not the target Debian/XFS/
dedicated-NVMe machine. Named volumes bypass the container root overlay for database files but not
the VHDX/NTFS/device-virtualization boundary. The exact Rust 1.95 Linux default and
`zipf-feasibility` matrices pass; neither is storage-qualification evidence.

### DKS-Q2-006 resource contract

`distributed-state-qual/v2` and common resource wire v2 remove mandatory LSM-specific debt/stall
fields. Candidate mechanisms now require typed `observed | not_applicable` mappings; N/A needs exact
source/configuration proof plus a bounded corroborating probe, and missing APIs remain unsupported.
Common cgroup, XFS, device, process, lifecycle, queue, service, and end-to-end observations remain
mandatory.

Two positive profile-v2 values are deliberately non-evaluable today. Background-maintenance debt
still needs an approved byte normalization/sample/aggregation/maximum formula, and “unexplained
storage pause” needs an objective event population and deterministic attribution rule. Plan
validation must reject both until DKS-Q2-006 adds strict mechanism artifacts and formulas. No value
is treated as zero or inferred from prose.

### Bounded M2 oracle and lifecycle

An exact digest of an expanded Zipf stream cannot be precomputed without expanding the stream. M2
therefore separates a pre-approved expectation contract from runtime-exact result fields:

- static work is structural/analytical plus proven finite-cycle enumeration, capped at 8,388,608
  visits, 256 MiB heap, zero disk scratch, 16 MiB total artifacts, and 600 seconds;
- the generator creates each immutable request once; candidate-independent reference and adapter
  paths consume the same frame through bounded rings, with lead/lag and interference charged to
  runner evidence;
- request/observation/state commitments are one-pass, domain-separated SHA-256 streams with complete
  roots only for complete streams;
- setup/final equality uses an independent expected iterator and candidate logical export in an
  exact no-spool streaming merge, capped at 67,108,864 rows and 96 GiB logical state; and
- post-warmup records a recipe root with `candidate_equality_claimed=false`, avoiding a cache- and
  compaction-perturbing full candidate scan.

After measured drain, the write-stop boundary starts the resource tail and mandatory final persist
is its first action. The same measurement database process/cgroup stays alive until the stable/
deadline cut. Clean close, fresh-child reopen, and final exact merge occur only afterward and are
retained as separate lifecycle evidence. This prevents process replacement from hiding maintenance
debt while retaining persist I/O in the tail.

M2 remains blocked on exact aggregate hot values, timer recurrence/codec, J2 routing/signed bounds,
payload codecs and goldens, expectation/result schemas, preparation/ring topology, rates/schedule,
cold-I/O proof, campaign budget, and named approvals.

### redb prescreen

The proposed `state-backend-redb-prescreen/v1` is a five-hour investment screen, not qualification.
It separates 256-MiB cross-table atomicity trials from nine 4-GiB cold-recovery comparisons and adds
steady one-writer, two-lane, hot-writer/victim, and controlled-holder probes. Mode-specific priming
prevents redb's clean-drop quick-repair commit from making I1/I2 crash recovery look artificially
cheap. Final post-exit release/acquire markers distinguish pre-commit, in-commit, returned, and
acknowledged outcomes without a signal/return race.

No prescreen command exists or is authorized. Any Docker smoke or native run first needs strict
schemas/harness and detached approval by the workload and operations owners binding source/build,
schedule, target/noise rules, triggers, caps, and all-false qualification fields. Even
`PRESCREEN_PASS` only funds a mechanism note, additive profile/schema proposal, and adapter review.

## Six-pass cycle review

### 1. AI slop

**Result: pass after corrections.** Independent read-only reviews found and corrected several
plausible-looking but invalid shortcuts: pre-approving future runtime hashes; describing a 6.39B-row
replay as merely “streamed”; mixing expected and actual hash roles; allowing result-ring failure to
mask candidate backpressure; using a racy holder-return marker; failing to prime redb after its
clean-drop allocator-state commit; classifying crash state before final post-exit markers; equating
mutation rate with source-row throughput; authorizing an unapproved “diagnostic” prescreen; and
replacing the database process inside a tail that required it alive.

The resource audit also caught that two renamed v2 thresholds had no formula. They are now explicit
blockers rather than impressive-sounding fields. Arithmetic for the J2 vector, state framing cap,
and five-hour redb campaign was independently recomputed. No generated prose is treated as evidence.

### 2. Overengineering and hot path

**Result: pass with open owner gates.** The redb screen is isolated and bounded instead of creating a
third qualification adapter. Small atomicity fixtures provide timing coverage; only nine trials pay
for 4-GiB recovery. It intentionally omits the C2 resource wire and cannot add a project dependency.

M2 uses formulas, finite cycles, streaming hashes, and a no-spool merge rather than a second full
generation/replay or an oracle database. Reference work is outside candidate service time but not
free: preparation CPU, reference lead/comparison lag, queue age, page faults, and null-control
interference remain gates. Per-row width hashing is prohibited on the measured service path. Exact
ring topology and headroom remain blocked rather than being guessed.

### 3. Unused code and dependencies

**Result: pass.** No Fjall, RocksDB, redb, SurrealKV, or adapter dependency was added. The only Rust
source change in the cycle is additive profile/resource-v2 validation. Both v1 and v2 paths have
direct unit, CLI, malformed-input, incompatibility, and hand-authored golden coverage. V1 remains by
design as a byte-stable regression fixture, not dead compatibility code. No backend execution
command or unwired runtime feature was introduced.

### 4. Production readiness, delivery, and soak

**Result: BLOCK, as required.** Backend-local atomic batches and persistence cannot establish vnode
ownership/fencing, watermark/timer semantics, portable checkpoint generation, source offset cuts,
sink prepare/commit/fencing/reconciliation, delivery mode, exactly once, rebalance safety, or
operator admission. DKS-Q2-001 through DKS-Q2-009 remain open. Physical fault, cache loss, 24/72-hour
endurance, N/N-1 recovery, and the independently operated release-candidate soak remain unexecuted.

The production soak must still precommit its independent operator, workload manifest/driver, fresh
seed, source and sink coordinates, external oracle, duration/event floor, immutable release artifact,
failure schedule, and evidence retention. It cannot reuse the backend generator binary or inherit a
backend prescreen result.

### 5. Documentation and over-documentation

**Result: pass with consolidation triggers.** The WSL boundary has one capability report, backend
source findings remain in one static audit, M2 remains in its workload ADR, and redb's exceptional
non-gating protocol has one testing document linked from the ADR/runner/plan. Detail retained in the
redb document is decision-bearing: setter modes, crash oracle, process-drop trap, counts, deadlines,
and disposition rules.

When exact M2 codecs and goldens land, move the rejected M1 history into a compact review appendix so
it cannot compete with the normative registry. Superseded candidate research must be removed when a
selection report exists; v1 wire fixtures remain. No tracked research document was found both stale
and unnecessary in this cycle. The local untracked `.claude` junction was not treated as project
research or modified.

### 6. Tests and checks

**Result: pass for the affected qualification/admission surface.** `CARGO_BUILD_JOBS=1` was used on
Windows; Linux used the exact pinned Docker image and Rust toolchain.

| Check | Result |
|---|---|
| qualification tool format | PASS |
| Windows default `test --locked --all-targets` | PASS, 94 tests (84 library tests) |
| Windows `zipf-feasibility` all-targets | PASS, 103 tests (93 library tests) |
| default and `zipf-feasibility` clippy, `-D warnings` | PASS |
| Docker/Linux Rust 1.95 default all-targets | PASS, 94 tests |
| Docker/Linux Rust 1.95 `zipf-feasibility` all-targets | PASS, 103 tests |
| exact cluster admission regression | PASS, 1/1; 1,660 filtered out |
| changed-document relative links | PASS |
| `git diff --check` before commits | PASS |

The exact admission command was:

```text
cargo +1.95.0 test -p laminar-db --lib --no-default-features --features cluster \
  db::tests::cluster_query_shape_admission_is_pre_mutation_and_mode_derived -- --exact
```

The root all-target workspace suite was not repeated: Cycle 11 runtime code is confined to the
isolated qualification tool, and this Windows host previously exhausted its paging file on that
suite. Provisioned CI remains responsible for the broad matrix. This limitation prevents any broad
production claim and is not hidden by the Docker smoke results.

## Cycle 12 implementation and review plan

Cycle 12 remains fail-closed contract/tooling work; it does not expose candidate measurement or
relax cluster admission:

1. resolve DKS-Q2-006's two non-evaluable fields. Freeze an objective maintenance-debt byte formula
   and storage-pause event population, or replace them additively in profile v3; implement the strict
   mechanism-map schema/validator and literal goldens without a backend adapter;
2. close the next M2 Z3/Z1 slice: aggregate hot-value semantics, timer recurrence and codec, J2
   constant-time routing/signed bounds/alignment, canonical request/observation/state codecs, and
   independent literal binaries under the existing proof caps;
3. freeze DKS-Q2-004/005 schedule, preparation topology, request/result ownership, timestamps,
   reference-lead/comparison-lag, ring/sample ceilings, null controls, offered rates, cold-I/O gate,
   and total campaign/endurance budget;
4. for redb, implement only the detached approval/result schemas and exact-source mechanism note
   needed to make a later human-authorized prescreen reviewable. Do not add a runtime/project
   dependency or run Docker/native probes without the two named owner signatures;
5. make an explicit Fjall telemetry patch/upstream/reject decision and a RocksDB complete-stall-
   source/patch/reject decision. Do not advance either with invented zero or N/A; and
6. rerun independent AI-slop, overengineering/hot-path, unused-code/dependency, production/delivery/
   soak, overdocumentation/stale-research, and test/CI review before closing the cycle.

Candidate execution remains blocked until DKS-Q2-001 through DKS-Q2-008 and owner approval close;
selection additionally requires DKS-Q2-009/C3; production additionally requires the distributed
checkpoint/source/sink/rebalance protocol, fault/endurance evidence, and independent soak.

# Distributed keyed state Cycle 12 review

- **Date:** 2026-07-23
- **Branch:** `feature/distributed-keyed-state-adr`
- **Contract-only cycle verdict:** **GO** after independent six-pass review
- **Candidate execution verdict:** **BLOCK** on DKS-Q2-001 through DKS-Q2-008 and named-owner approval
- **Backend selection verdict:** **BLOCK** additionally on DKS-Q2-009/C3
- **Cluster admission verdict:** unchanged and fail-closed under `[LDB-4007]`
- **Production verdict:** **NO-GO**; no distributed implementation, candidate run, native fault/endurance
  evidence, or independent soak exists

## Outcome

Cycle 12 delivered fail-closed qualification contracts, not a backend or distributed-state
implementation:

- `2b311078` freezes the candidate mechanism mapping and bounded observation wires, adds truthful
  profile v3, and keeps every synthetic record execution-ineligible;
- `22f64fa1` narrows the bounded M2 aggregate/timer/J2 semantics and explicitly refuses to freeze an
  unmeasured request codec on the hot path; and
- `d9f70889` adds strict detached redb 4.1.0 prescreen approval/result schemas, synthetic fixtures,
  adversarial contract tests, and an exact-source mechanism note without adding redb or a harness;
  and
- `003a4c72` applies the independent-review correction that structurally limits every synthetic
  redb result to `DEFER`, so a fixture cannot masquerade as a smoke or native prescreen decision.

No candidate command was added or run. Docker Desktop/WSL remains available for pinned Linux build
and functional smoke work, but cannot produce native XFS/NVMe, power-loss, qualification, selection,
or production evidence. The redb protocol additionally requires two named pre-run owners and a
fail-closed harness that do not exist, so no redb Docker or native probe was attempted.

## Material decisions

### DKS-Q2-006 observation contract

Profile v2 asked the causal question `unexplained_storage_pause_max_ms`, which a block-device trace
cannot answer. Profile v3 is additive and replaces it with objective
`target_device_io_latency_max_ms`. It also replaces the ambiguous coordinated-omission boolean with
`open_loop_due_to_return=true` and `synthetic_coordinated_omission_correction=false`. V1 and v2 remain
immutable regression fixtures; v3 remains unapproved and execution-ineligible.

The mechanism map now has strict `observed | not_applicable` arms. N/A requires exact source,
configuration, and bounded-probe descriptors; it is never numeric zero. Maintenance debt is a
checked sum of pairwise-disjoint direct byte gauges followed by a maximum. Stall time is the union of
complete half-open intervals, including deterministic measurement-end censoring. Resource and
mechanism offsets share one pre-warmup `CLOCK_MONOTONIC_RAW` origin, so pre-measurement stalls and
measured-phase latency offsets cannot acquire conflicting meanings.

Target-device evidence is a bounded per-shard summary rather than a per-I/O event stream. Completion
updates stay on the completion CPU; no global issue sequence or remote issue-shard update is added to
the block hot path. Known loss/overflow/lifecycle flags, errors, and incomplete requests remain in
the summary for later INVALID/FAIL classification. Successful decoding is not a verdict.

DKS-Q2-006 remains open. There is no real mapping, collector/tracer, cross-artifact plan validator,
or approved source proof. Unmodified Fjall 3.1.8 still **fails** because its applicable debt/stall
signals are absent. The current RocksDB binding remains **blocked** on complete database-scope stall
coverage. redb remains **deferred** pending its separately approved native probe and later additive
profile/mapping.

### Bounded M2 semantics and hot-path boundary

The aggregate backend arm uses stable identity-derived hot values and a closed-form rotating cold
slot. Multiplicity remains in canonical request/counter evidence but does not claim cumulative SQL
aggregate arithmetic. Product aggregate correctness, checkpoint recovery, and independent soak
remain separate obligations.

The timer arm now has an exact positive-even-`K` recurrence, five-request cycle, lane indices, and
two-ring fairness. The spill static proof uses 7,340,032 visits under the 8,388,608 cap. Warmup and
measurement counts must be divisible by `10L`; incompatible provisional counts are rejected rather
than rounded.

J2 now has checked signed event time, Euclidean group/side/slot equations, a side-distinct fanout-zero
namespace, exact prefix routing for only `Q={197,4737}`, signed setup/final inversion, and literal
allocation hashes. It is state-balanced, not service-balanced: the resident shape has a 1.5616x
per-vnode matched-row spread, which future expectations must retain. Equal state bytes cannot be
reported as equal service work.

No workload-v2 encoder or binary golden landed. The direct timer frame is about 7.56 MiB and would
move roughly 0.93 GB/s at the provisional rate. The 4.72-MiB dictionary idea still moves about
0.582 GB/s before copies/filler generation and is only a spike candidate. Identity-derived
compression-resistant filler, dictionary rules, preparation cost, ring caps, runtime result unions,
lifecycle registration, owners, and literal proofs keep DKS-Q2-003 blocked.

### Campaign controls

The campaign contract fixes adjacent two-slot candidate pairs, one slot per candidate and
repetition, fixed repetition seeds, AB/BA imbalance at most one, no replacement of failed/invalid
slots, open-loop due-to-return conservation, bounded ownership automata, terminal precedence, and
never-subtracted null/observer controls.

DKS-Q2-004/005 remain open because exact counts, rates, order vector, worker topology, ring sizes,
lead/lag caps, scheduler/observer gates, target identity, cold-I/O evidence, and total fault/endurance
budget cannot be selected honestly before the codec/calibration work and DKS-Q2-008.

### redb 4.1.0 boundary

The exact cached archive is 188,200 bytes with SHA-256
`8e925444704b5f17d32bf42f5b6e2df050bceebc3dcd6e71cc73dafe8092e839`. Static source confirms a
single blocking database writer, synchronous commit/cache/shrink/abort/open/repair/close work, and no
redb-managed asynchronous maintenance thread in the frozen no-feature build. Disabled cache metrics
return literal zeros that mean unsupported, not observed zero.

The detached schemas structurally prevent Docker smoke from carrying `PRESCREEN_PASS`, a native
mechanism probe, or production authority. Native records require a reviewed prior smoke descriptor
and bounded mechanism-probe descriptor. Qualification, admission, selection, C1/C2/C3,
fault/endurance, exactly-once, source/sink, production, and independent-soak eligibility are all
fixed false. Schema validity verifies neither referenced bytes nor owner attestations; the external
semantic verifier and harness remain absent. Synthetic results are additionally restricted to
`fixture_ineligible=true` and `DEFER`; future semantic verification must still reject them as
decision inputs rather than relying on downstream callers to notice the marker.

## Six-pass cycle review

### 1. AI slop and contract consistency

**Result: pass after correction.** Independent reviews found and corrected the causal v2 storage
field, the inherited coordinated-omission contradiction, half-open endpoint/origin ambiguity,
per-shard/global device accounting, aggregate multiplicity contradiction, timer recurrence gaps,
signed J2 prefix/inverse errors, duplicate-prefix routing, false service-balance implication, and
stale redb-schema status prose. Named arithmetic and hashes were independently recomputed. No
remaining correctness or misleading-contract blocker was found for contract-only closure. The final
audit also found that a native-shaped synthetic redb record could carry `PRESCREEN_PASS`; the schema,
fixture, and adversarial tests now force all synthetic results to `DEFER`.

### 2. Overengineering and hot path

**Result: pass with explicit stop conditions.** The target-device artifact is at most one 112-byte
record per shard rather than a multi-gigabyte event log. The completion path has no global sequence
or forced remote issue-shard update. M2 codec work stopped when byte movement and preparation cost
were not proved. The redb slice adds schemas/tests only—no PKI, backend dependency, harness, or
execution path. Mechanism sample validators deliberately have no CLI route until a trusted plan can
supply their cross-artifact parameters. This schema surface is now frozen. The current artifact APIs
also accept a complete bounded byte slice of up to 256 MiB; their eventual file integration must
stream, cap memory, and be benchmarked before it can be called production-grade tooling.

### 3. Unused code and dependencies

**Result: pass for the contract-only scope, with integration debt explicit.** No Cargo manifest or
lockfile changed, and redb/Fjall/RocksDB remain absent from the runtime and qualification dependency
graphs. The three public mechanism artifact validators are directly unit-tested but have no callable
CLI or cross-artifact consumer. The redb schemas are compiled only by contract tests and have no
semantic validator or harness. Neither is represented as implemented qualification capability;
Cycle 13 must integrate the mechanism path before adding more detached schema surface.

### 4. Production readiness, delivery, and soak

**Result: NO-GO, correctly fail-closed.** This cycle supplies no vnode ownership, checkpoint,
rebalance, source-offset, sink-commit, delivery/exactly-once, connector capability, or recovery
implementation. No candidate backend ran. DKS-Q2-001 through DKS-Q2-009 remain open at their stated
boundaries. Native physical faults, cache loss, 24/72-hour endurance, and the independent production
soak are absent. WSL/Docker smoke can never substitute for those results.

### 5. Documentation, stale research, and overdocumentation

**Result: pass with a consolidation trigger.** Current-contract links now target unapproved profile
v3 and identify v1/v2 as immutable regression fixtures. The new redb note is exact-pin source proof,
not duplicated product design. No touched research document was stale enough to delete; historical
cycle reviews remain audit history, and unrelated Claude project memories were not repurposed as
evidence. The runner/workload ADRs are now long: a future executable schema/golden must replace—not
repeat—the frozen prose, and a new document is justified only for a distinct evidence identity. No
further redb schema design is justified until a verifier consumes the existing records.

### 6. Tests, CI, and empirical limits

**Result: targeted pass; production evidence absent.** On Windows the isolated qualification tool
passes 112 default-feature tests and 121 all-feature/all-target tests; doc tests contain zero cases
and pass. Formatting, all-feature/all-target `cargo clippy -D warnings`, diff checks, schema
meta-validation, adversarial redb union mutations, local-link checks, exact profile/archive hashes,
and independent timer/J2 arithmetic checks pass. An independent read-only WSL/Docker rerun with the
pinned Linux Rust 1.95 toolchain also passes all 121 tests.

Committed CI still has two low-level coverage gaps: its clippy command uses default features, and the
standalone qualification-tool job is Ubuntu-only. Mechanism wire tests are generated by same-module
helpers rather than a detached hand-authored corpus. These are Cycle 13 test-tooling tasks, not
evidence that the current contracts failed.

The broad workspace matrix was not rerun: prior execution on this Windows host exhausted the paging
file, and Cycle 12 changes are isolated qualification tooling/docs. Provisioned CI still owns the
broad matrix. No native Linux/XFS/NVMe candidate, fault, endurance, or soak test ran; this limitation
prevents any production claim.

## Cycle 13 implementation and review plan

Cycle 13 should stop expanding prose-first contracts and integrate one bounded path:

1. implement one fail-closed `validate-mechanism-bundle` CLI behind a complete, pre-approved plan.
   Stream and bound file reads; bind profile v3, mapping, common-resource cuts, debt, stalls, and
   target-device artifacts; enforce hashes, clocks, counts, mapping arms, and zero error/incomplete/
   anomaly disposition; emit only `VALID_INELIGIBLE`;
2. add detached, hand-authored mechanism binary goldens plus every-truncation, exact-boundary,
   cross-artifact, and adversarial cases. The semantic consumer must reject synthetic records as
   decisions even where the schema already does so;
3. change qualification-tool CI clippy to all features, add a Windows validator lane, and benchmark
   parser peak memory and throughput. WSL/Docker remains build/functional evidence only;
4. close DKS-Q2-001's sampler decision with independent cross-platform vectors, error/interference
   evidence, and named-owner choice between the bounded retry contract and a new total sampler;
5. only after those integrations, run a non-authoritative M2 codec microbenchmark. Do not freeze an
   encoder until preparation, copy, filler, ring-memory, and null-control caps pass; use measured
   bounds to propose, not approve, DKS-Q2-004/005 schedule values; and
6. obtain explicit Fjall-telemetry and RocksDB-stall-source investment decisions, then rerun the six
   independent review passes. Keep redb deferred without its owners/verifier/harness. No production
   readiness claim is allowed until a separate team executes and reviews the production soak charter.

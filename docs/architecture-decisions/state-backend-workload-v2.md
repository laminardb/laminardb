# State backend long-stream workload and identity v2

- **Status:** provisional C2 identity plus reviewed M2 feasibility components; executable cases remain blocked
- **Scope:** standalone `tools/state-backend-qual` qualification tooling only
- **Not runtime code:** this does not change cluster admission, checkpointing, or operator execution
- **Approval required before candidate execution:** workload owner and operations owner
- **Related decisions:** [qualification model v1](state-backend-qualification-model-v1.md),
  [runner/evidence contract v1](state-backend-qualification-runner-v1.md), and the
  [deterministic Zipf decision](state-backend-zipf-generator-v1.md)

## Decision and correction to the provisional counter

C2 uses a new, typed long-stream workload. It does not extend, reinterpret, or deserialize through
the C1 `ModelCase`, `state-backend-workload/v1`, or `state-backend-model-result/v1` types. C1 remains
the bounded empty-start semantic oracle and keeps all existing domains and goldens unchanged.

The complete generation input for one C2 case is its canonical case body plus one selected seed.
C2 does **not** consume C1's `model_input_sha256`. That digest includes every workload vector and the
complete `measurement.fixed_seeds` vector; using it would make adding or reordering an unrelated
repetition silently re-key all existing streams. Profile validation may prove that case values and
the selected seed are approved members, but replay reads only the typed case body and selected seed.

The previously proposed Zipf counter is therefore amended before it becomes executable. The Zipf
mathematical transform retains identity `state-backend-zipf-ri-hd99-softf64/v1`; the phase-aware
addressing around it uses the new C2 stream and counter identities below.

This decision freezes the non-circular framing and dependency direction, but the body identity is
not semantically closed and it does not authorize a backend run. No body is accepted until the
closed scenario/policy registry is frozen. No workload-v2 encoder or result can be qualification
evidence until the exact scenario policies, named matrix, schemas, literal goldens, independent
numerical evidence, and owner approvals are complete.

## Identity graph

The dependency direction is fixed:

```text
typed generation inputs --canonical encoding--> case_stream_id
case_stream_id + selected seed ----------------> stream_instance_id
bounded static/finite-cycle analysis ----------> expectation contract
expectation contract + preflight procedure ----> preflight provenance artifact
independent literal producer ------------------> literal corpus
literal corpus + exact generator build --------> actual-build conformance result
independent MPFR/interval implementation ------> numerical-audit result
independent analysis --------------------------> retry-bound report
case + expectation/preflight + Zipf objects + schedule/gates -> runner plan
runner plan + source/builds/environment --------> detached approval
one generated request -> reference interpreter + adapter -> runtime commitments
expected iterator + candidate export ----------> setup/final streaming merge
attempt results and raw objects ----------------> evidence manifest
closed manifest --------------------------------> detached completion record
```

No arrow points back upward. In particular, generated counters, state digests, observed shares,
candidate identities, plan hashes, approvals, and evidence cannot contribute to either stream ID.

V2 reserves the following printable identities and exact hash domains. Rows labelled “encoding
identity” are printable registry values, not an assertion that the string itself prefixes its future
wire payload. Rows labelled “domain” include the shown terminating NUL byte:

| Item | Identity |
|---|---|
| workload generator | `state-backend-workload/v2` |
| workload counter | `state-backend-counter/v2` |
| case JSON wrapper | `state-backend-workload-case/v2` |
| canonical case body | `state-backend-case-body/v2` |
| case stream ID | `state-backend-case-stream-id/v2` |
| seeded stream instance | `state-backend-stream-instance/v2` |
| request encoding identity | `LDB-SBQ-REQUEST-V2` |
| observation encoding identity | `LDB-SBQ-OBSERVATION-V2` |
| entity encoding identity | `LDB-SBQ-ENTITY-V2` |
| request-leaf domain | `LDB-SBQ-REQUEST-LEAF-V2\0` |
| observation-leaf domain | `LDB-SBQ-OBSERVATION-LEAF-V2\0` |
| request sequence/root domains | `LDB-SBQ-REQUEST-SEQUENCE-V2\0` / `LDB-SBQ-REQUEST-STREAM-V2\0` |
| observation sequence/root domains | `LDB-SBQ-OBSERVATION-SEQUENCE-V2\0` / `LDB-SBQ-OBSERVATION-STREAM-V2\0` |
| logical-state-leaf domain | `LDB-SBQ-STATE-LEAF-V2\0` |
| logical-state sequence/root domains | `LDB-SBQ-STATE-SEQUENCE-V2\0` / `LDB-SBQ-STATE-STREAM-V2\0` |
| post-warmup state-recipe domain | `LDB-SBQ-STATE-RECIPE-V2\0` |
| expectation contract | `state-backend-workload-expectations/v2` |
| preflight provenance | `state-backend-workload-preflight/v2` |
| observed workload result | `state-backend-workload-result/v2` |
| evidence-manifest binding | `state-backend-workload-evidence/v2` |

The already reserved `state-backend-runner-plan/v1`, `state-backend-evidence-manifest/v1`, approval,
attempt, and completion identities remain unchanged. No artifact under those identities exists yet,
so binding an explicitly versioned workload-v2 record does not reinterpret existing bytes.

## Canonical case body

The case body contains only bounded, typed, pre-result inputs. It is a manual binary encoding, not
canonical JSON. Unsigned integers are big-endian. `id` is `u32 length || exact ASCII bytes`, with a
1--128 byte lowercase identity matching `[a-z0-9][a-z0-9._/-]{0,127}`. A vector is `u32 count ||
elements`; a Boolean is exactly `0x00` or `0x01`; a rate is a positive reduced
`u64 numerator || u64 denominator`; and a band is `u64 low || u64 target || u64 high`, with
`low <= target <= high`. Counts and encoded lengths are checked before allocation. The complete
body is at most 65,536 bytes.

Its exact top-level field order is:

```text
"LDB-SBQ-CASE-BODY-V2\0"
id("state-backend-workload/v2")
id("state-backend-counter/v2")
scenario_u8
id(scenario_semantics)
residency_u8
contention_u8
active_vnode_count_u32
vnode_placement_union
distribution_union
width_policy_union
batch_rows_u32
target_batch_bytes_u64
hard_batch_bytes_u64
setup_policy_union
id(churn_policy)
retention_policy_union
persistence_policy_union
lifecycle_verification_union
warmup_phase
measured_phase
live_state_bytes_band
live_record_count_band
distinct_live_rank_band
timer_record_count_band
scenario_parameters_union
generator_limits
```

The tags and payloads are:

- scenario: `0x01 aggregate`, `0x02 timer_window`, or `0x03 join`;
- residency: `0x01 resident` or `0x02 spill`;
- contention: `0x01 normal` or `0x02 hot_vnode`;
- vnode placement: `0x01 identity_mod_vnodes`, or
  `0x02 forced_vnode || target_vnode_u32`;
- distribution:
  - `0x01 || id("hot_mix_v1") || N_u64 || one_key_u16 || nine_keys_u16 || uniform_u16`;
  - `0x02 || id("zipf_ri_hd99_softf64_v1") || N_u64 || exponent_milli_u32 ||
    id("state-backend-zipf-ri-hd99-softf64/v1") || id(math_source) ||
    math_source_sha256[32] || retry_limit_u8`; or
  - `0x03 || id("uniform_rank_v1") || N_u64`;
- width policy:
  - `0x01 || key_bytes_u32 || value_bytes_u32`; or
  - `0x02 || id(selection_policy) || u32 pair_count || (key_bytes_u32 || value_bytes_u32)*`;
- setup: `id(policy) || materialization_u8 || materialized_rank_count_u64`, where materialization
  is `0x01 full` or `0x02 sparse`;
- retention: `0x01 none`, or `0x02 request_horizon || horizon_u64`;
- persistence during warmup/measurement: `0x01 no_explicit_persist`, or
  `0x02 periodic || every_requests_u64 || at_phase_end_bool`;
- lifecycle verification: the provisional sole arm is
  `0x01 || id("setup-persist-close-reopen-verify-final-v1")`; it remains unregistered until owner
  review;
- each phase: its fixed tag, `request_count_u64`, and reduced requests-per-second rate; warmup is
  `0x01` and measurement is `0x02`; and
- generator/verifier limits: request bytes, read rows, read bytes, mutation bytes, encoded
  observation bytes, static-proof entity visits, static-proof artifact bytes, state-verifier rows,
  state-verifier logical bytes, and state-verifier canonical encoded bytes as ten `u64` values in
  that order.

`N` is a count: valid ranks are exactly `0..N-1`, so `N >= 1`. Full materialization requires
`materialized_rank_count = N`; sparse materialization requires
`1 <= materialized_rank_count < N`. Hot-mix weights sum to 1,000.
Zipf v1 fixes exponent 990 and retry limit 64. A forced vnode must be below the active nonzero vnode
count. Width pairs are ordered, nonempty, and unique; they are never an implicit Cartesian product.
Warmup/measurement requests are full batches, so each phase's raw logical rows are derived by
checked `request_count * batch_rows` and are not redundantly encoded. A rate is requests per second.
Periodic persistence runs after every `every_requests` successful requests independently within
warmup and measurement, whose cadence counters each reset to zero; `at_phase_end` adds one call
after each nonempty phase and does not apply to setup. The warmup call follows its last terminal.
The measured call occurs only after drain, immediately after the logical event at
`write_stop_offset_ns`; it is the lifecycle arm's mandatory final persist, not an additional
consecutive call. When `at_phase_end` is false or the
interval policy is `no_explicit_persist`, the lifecycle arm still performs exactly one final persist
at that point. All selected widths and batch limits must
also satisfy the approved profile, checked analytical maxima, bounded finite-cycle analysis, and
exact validation of each emitted post-deduplication request before the adapter sees it.

The ten generator/verifier maxima apply, in order, to one canonical request's encoded bytes, one returned
observation's logical rows, one returned observation's logical key/value bytes, one request's
logical mutation key/value bytes, one encoded observation's bytes, bounded static-proof entity
visits, the externally serialized expectation contract's bytes, and one setup/final verifier's
logical rows, logical key/value bytes, and canonical encoded record bytes. Each is an inclusive
`u64` ceiling checked before allocation or iteration; the artifact value is a predeclared cap, never
a derived self-length field.

For the current Zipf candidate, `math_source` identifies the immutable upstream
`libm 0.2.16` crate package, and `math_source_sha256` is SHA-256 over the exact raw `.crate` package
bytes—the Cargo registry checksum scope. It is not a digest of the LaminarDB/runner source archive,
lockfile, toolchain, target, feature set, port source, object file, or generator binary. Those
downstream identities belong to actual-build conformance, preflight, plan, and approval records.
Z4 must verify both scopes and reject any aliasing or self-inclusion.

The scenario-parameter union is deliberately not an opaque map. Its final tags and payloads and the
closed, versioned registries for every scenario, setup, churn, width-selection, retention, and
persistence identity must be frozen alongside the aggregate, timer, and join lifecycle policies
before any case body is accepted. Regex validity alone never registers a policy. Until then, the
case schema and encoder remain unavailable. Adding a default, unknown-policy pass through,
free-form extension object, or ignored trailing field is prohibited.

### Included and excluded values

The body binds everything that can change generated storage traffic: scenario and policy versions,
distribution and `N`, vnode placement, exact width pairs, batch shape and byte ceilings,
setup/materialization, churn/retention/persistence, phase counts/rates, declared live-state bands,
scenario parameters, and generator/preflight limits.

It excludes the human slug, derived IDs, selected seed, repetition and schedule slot, candidate and
order, machine placement, latency/throughput/resource pass gates, owner approvals, expected
counters/digests, observations, timestamps, status, plan hashes, and evidence. The included
low/target/high bands are generator state-control inputs; they are not candidate performance gates.
Slugs remain unique lowercase ASCII labels in the runner plan, and the plan rejects either one slug
mapped to multiple bodies or duplicate canonical bodies hidden behind multiple slugs.

Analytical setup/final facts and bounds live in the expectation contract. Exact expanded request,
observation, and setup/final state roots; content-dependent post-deduplication counters; realized
hottest-key/vnode shares; timer backlog; and join return counts are runtime values. Putting either
kind in the body would make the stream depend on its own result. The expectation contract declares
which fields are `analytical` and which are `runtime_exact`; it never fabricates a runtime value.

## Case and seeded-stream derivation

The canonical body includes its domain above. IDs are:

```text
case_stream_id = SHA-256(
  "LDB-SBQ-CASE-STREAM-ID-V2\0" ||
  body_length_u64_be ||
  canonical_case_body
)

stream_instance_id = SHA-256(
  "LDB-SBQ-STREAM-INSTANCE-V2\0" ||
  case_stream_id[32] ||
  selected_seed_u64_be
)
```

`case_stream_id` alone never identifies generated bytes. Every expectations, result, plan, and
evidence record carries both IDs and the selected seed. Before expectations generation, proposed
plan validation checks that the seed is the unique raw-profile `fixed_seeds[repetition]` value for
the proposed slot. The completed plan then binds it; execution validates it against the detached
approval. Neither the raw profile nor an approved plan is an input to stream derivation, so the body
does not acquire a hidden dependency on the complete seed vector.

## Coordinate and encoding separation

Three ordinals have different meanings and must never be substituted:

1. `phase_request_ordinal` resets to zero independently in setup, warmup, and measurement and
   addresses counter output; setup's exact request count is derived from its record stream;
2. `runtime_event_ordinal` is `warmup_phase_request_ordinal` in warmup and
   `warmup_request_count + measured_phase_request_ordinal` in measurement, using checked `u64`;
   it is monotonic across the warmup/measurement boundary and drives logical time, retention, timer
   and join lifecycle; and
3. `sample_ordinal` is the measured phase ordinal and is the contiguous key in the finite latency
   stream. Setup and warmup do not create measured samples.

Setup has a separate `setup_record_ordinal` over the candidate-neutral logical prefill stream. It
does not pretend to be an earlier timer or join event. A setup request packs the next bounded slice
of records, permits only a final partial batch, and reports its actual logical row count. Scenario
policy validation derives the exact setup record/request counts with checked arithmetic.

The C2 counter word is the first eight bytes, interpreted big-endian, of:

```text
SHA-256(
  "LDB-SBQ-COUNTER-V2\0" ||
  stream_instance_id[32] ||
  phase_tag_u8 ||
  scenario_tag_u8 ||
  phase_request_ordinal_u64_be ||
  row_ordinal_u32_be ||
  lane_u32_be
)
```

The Zipf proposal word for `0 <= attempt <= 63` is the first eight bytes of:

```text
SHA-256(
  "LDB-SBQ-ZIPF-RI-HD99-SOFTF64-V2\0" ||
  stream_instance_id[32] ||
  phase_tag_u8 ||
  scenario_tag_u8 ||
  N_u64_be ||
  phase_request_ordinal_u64_be ||
  row_ordinal_u32_be ||
  attempt_u8
)
```

Setup, warmup, and measured tags are `0x00`, `0x01`, and `0x02`. Scenario tags remain aggregate
`0x01`, timer/window `0x02`, and join `0x03`. Row ordinal addresses the raw logical input row before
deduplication. Request-level choices reserve row `0xffffffff`. A failure to obtain a valid Zipf
sample in 64 attempts invalidates any bounded finite-cycle analysis that evaluates that coordinate
and makes an executing attempt INVALID. Static preflight does not expand the complete stream to
search for it. There is no reseed, fallback, or consumption of another row's coordinates; accepting
the analytical retry bound plus runtime fail-closed handling remains a named-owner decision under
DKS-Q2-001.

C2 entity suffixes, requests, observations, logical-state records/digests, and request and
observation streams use their explicit v2 domains and bind the stream instance plus phase
where applicable. Reusing a C1 item layout as an inner logical-operation payload is permitted only
if the v2 envelope explicitly length-prefixes it; phase-aware bytes are never labelled with a v1
identity alone. The closed scenario registry must freeze exact v2 item field order before an encoder
exists. Any lifecycle identity based on time uses `runtime_event_ordinal`, not the resetting counter
or sample ordinal.

## Expectations, plan, result, and evidence boundary

One candidate-neutral expectation contract is created per seeded stream instance before a runner
plan can be approved. It has identity `state-backend-workload-expectations/v2`, the notice
`NOT QUALIFICATION EVIDENCE`, and contains the exact case body, recomputed case/instance IDs, selected
seed, all producer/verifier/math/policy identities, static limits, and one closed mode for every
expected field:

- `analytical` carries the exact value plus the approved proof identity and witness digest; or
- `runtime_exact` carries the exact producer, canonical encoding, comparison role, and bound, but no
  value or digest that is unknowable before execution.

Phase request/raw-row counts, structural bands, finite-cycle facts, and formula-derived bounds may be
`analytical`. Expanded request/observation roots, content-dependent deduplication/share counters, and
setup/final expected state roots are `runtime_exact`. The contract has no candidate, plan, approval,
machine-result, or status field. The implementation under test cannot serve as its semantic oracle.

A separate `state-backend-workload-preflight/v2` provenance artifact binds the expectation-contract
length/digest, static-proof source archive/binary/lock/toolchain identities, start/end UTC timestamps,
elapsed wall and CPU time, peak memory, host identity, exit/failure classification, and bounded
stdout/stderr descriptors. It proves structure and checked maxima; it does not replay all 6.39
billion logical input rows. The runner plan and detached approval bind the accepted expectation and
preflight-provenance digests plus the exact runtime generator/reference builds.

During an approved attempt, the deterministic generator writes each immutable canonical request
frame exactly once into a bounded preallocated ring. The candidate-independent reference
interpreter and adapter consume that same sealed frame; neither calls or re-runs the generator. The
driver and reference path independently hash the frame before decode/dispatch. The reference path
computes expected observations and state transitions without calling the adapter or reading
candidate state. Candidate latency, results, errors, and storage state never influence later request
bytes. A bounded lead is allowed so reference work does not enter candidate service latency; lead,
lag, preparation CPU, affinity, ring occupancy, and interference remain gate-bearing runner
evidence. Generation/validation failure, reference-lead/comparison-lag overflow, or cross-candidate expected-root
disagreement makes the attempt or pair INVALID, never candidate FAIL.

Each result records driver/reference request roots, reference expected-observation roots, adapter
actual-observation roots, and independently derived expected versus candidate setup/final state
roots in distinct fields. Expected and actual payloads are compared in ordinal order before their
equal root values can be asserted. The result also carries separately derived counters and bounded
mismatch witnesses. The plan binds the producer algorithms and builds, not these future values.

### One-pass commitment framing

All displayed integer fields below—including lengths, ordinals, counts, and applied-request
counts—are unsigned big-endian integers. Phase tags are setup `0x00`, warmup
`0x01`, and measurement `0x02`; state cut tags are setup `0x00` and final `0x02`. For request,
observation, or state payload type `X`, its leaf is:

```text
SHA-256(
  X_leaf_domain_with_NUL ||
  stream_instance_id[32] ||
  phase_or_cut_tag_u8 ||
  ordinal_u64 ||
  canonical_payload_length_u64 ||
  canonical_payload
)
```

Its single-pass sequence digest and root are:

```text
sequence_digest = SHA-256(
  X_sequence_domain_with_NUL ||
  repeated(ordinal_u64 || canonical_payload_length_u64 || item_leaf[32])
)

stream_root = SHA-256(
  X_stream_domain_with_NUL ||
  stream_instance_id[32] ||
  phase_or_cut_tag_u8 ||
  actual_item_count_u64 ||
  sequence_digest[32]
)
```

Request and observation ordinals are `phase_request_ordinal`. State record ordinals are zero-based,
contiguous positions in the canonical state order and reset independently for each producer and cut.
A state root inserts
`applied_runtime_request_count_u64` after the cut tag so setup and final cannot alias at an ambiguous
ordinal. The domains are the exact terminating-NUL domains reserved above. Expected and actual
observation fields use the same observation algorithm so equal canonical streams have equal roots;
their artifact roles remain distinct. Expected and actual state fields follow the same rule.

A complete root exists only for a complete successful stream. Each driver, reference, expected, or
actual producer has its own failure role. Its failure record contains that producer's completed
prefix count, prefix sequence digest, closed error tag, first failing ordinal, and a bounded witness;
it never populates the full-root field or borrows a run-ahead peer's prefix. The strict result-schema union, canonical request and
observation payload field order, and literal binaries remain blockers, so this framing does not yet
authorize an encoder.

### Canonical operations and state equality

Scenario-specific folding consumes every raw source row before generic canonicalization. In an
aggregate group, multiplicity contributes to the replacement value; the least source ordinal only
selects representative provenance. Generic canonicalization is separate per collection: point reads
sort by logical key, ranges by their complete `(start,end,row_cap,byte_cap)` tuple, and mutations by
logical key and mutation kind. Each range endpoint is a complete logical key including role/table,
vnode, and opaque key bytes. Byte-identical entries collapse to the least provenance ordinal. Two
puts of different values, or a put and delete, for one key are INVALID. Timer and J2 lifecycle
rosters are stricter: a duplicate entity/key is INVALID rather than deduplicated.

Ranges are half-open `[start,end)` in logical key order and carry inclusive row and logical
key/value-byte caps. A first row that alone exceeds the byte cap returns the closed oversized-first-
row error and no rows. Otherwise the adapter may inspect exactly one unreturned row beyond the caps
to set `has_more`; that I/O and service time remain charged. Endpoints, both caps, returned rows, and
`has_more` are canonical payload fields. Exact duplicate ranges include their caps and collapse;
ranges with different caps coexist. Lifecycle requests carry request-bound expected precondition
values derived by the generator and independently checked by the reference interpreter. The adapter
reads the pre-mutation cut and must abort before its one atomic mutation batch if storage does not
match those request preconditions. A later supervisor comparison mismatch cannot undo a returned
call; it fails the attempt and final-state verification remains mandatory.

A canonical logical-state record is exactly:

```text
role_u8 || vnode_u32 || key_length_u32 || key || value_length_u32 || value
```

It is ordered strictly by `(role, vnode, unsigned opaque key)`. The role registry is closed before
encoding. The independent expected iterator cannot inspect candidate export; the adapter exports
logical, not physical engine records. Setup and final verification stream both iterators through an
exact merge and reject unordered, duplicate, missing, extra, or unequal records while producing
separate expected/actual roots. No side is spooled. Post-warmup has only a separately identified
recipe/provenance root and literal `candidate_equality_claimed=false`; it is never compared with a
candidate state root.

The provisional lifecycle arm means: write setup, explicitly persist, cleanly close, reopen in a
fresh child, exact-merge the setup cut, run warmup and measurement, drain, perform the mandatory
final persist immediately after the `write_stop_offset_ns` event, then keep that measurement child
and its cgroup alive through the resource-tail stable/deadline cut. Only after that cut does it
cleanly close; a fresh
verification child reopens and exact-merges the final cut, then cleans up. Post-tail close/reopen/
merge work is separately retained lifecycle evidence, not part of resource-tail/write-amplification
gates. This clean reopen is not cache loss, process crash, checkpoint restore, source-offset
recovery, sink commit, or exactly once; those remain DKS-Q2-007/product gates.

Four Zipf objects remain separate: a literal corpus produced without calling the implementation
under test; actual-build conformance results from running that corpus against each exact generator
binary/target/build; a finite-precision MPFR/interval audit independently implemented without
calling the generator; and an analytical retry-bound report. Their exact lengths/digests,
source/binary/toolchain/precision/rounding identities, declared target/build coverage, and reviewer
provenance are plan and approval inputs. None enters generated case-body or expectation bytes.

All generator, preflight, and independent-audit source archives and binaries are finalized before
their generated artifacts. Their explicit inclusion manifests exclude case wrappers, expectations,
preflight provenance, literal/conformance/numerical/retry results, runner plans, approvals, attempt
results, evidence manifests, and completion records. Generated objects are never copied back into a
source archive whose digest they bind.

The complete runner plan binds exact case wrapper bytes, every required case/seed expectation and
preflight-provenance length/digest, schedule, candidates, gates, result-ring sizes, machine policy,
and artifact limits. The detached approval binds that complete plan plus exact
source/build/environment inputs.
An observed `state-backend-workload-result/v2` then binds the approved plan, detached approval,
slot/candidate, and matching expectation digest while recording reference-expected and
candidate-actual counters/digests plus artifact descriptors.
The evidence manifest references results; it does not repeat or mutate case semantics.

Within the evidence manifest, a closed `state-backend-workload-evidence/v2` binding contains only
the slot index, case and stream-instance IDs, selected seed, and length/SHA-256 descriptors for the
approved expectation, preflight provenance, observed workload result, and required raw phase
objects. It contains no copied thresholds or case parameters. The manifest validator requires those
references to equal the plan/approval/result identities and rejects unknown roles.

This ordering prevents plan/result and stream/result hash cycles. A changed body, seed, expectation,
gate, binary, or environment creates the appropriate new downstream identity; it cannot be patched
inside a closed campaign.

## Fail-closed schema and migration rules

The future `workload-case-v2`, `workload-expectations-v2`, `workload-preflight-v2`,
`workload-result-v2`, and embedded `workload-evidence-v2` JSON schemas are strict envelopes around
the binary/body identities above. Every object rejects additional properties; duplicate JSON keys,
floating/negative numbers, unknown tags/policies, uppercase or malformed digests, unreduced/zero
rates, invalid bands, missing scenario arms, and trailing binary bytes are errors. Durations are
unsigned integer nanoseconds, memory and object sizes are unsigned bytes, CPU time is unsigned
nanoseconds, timestamps are normalized UTC strings, status is a closed enum, and every object
descriptor is role, byte length, lowercase SHA-256, and media type. Semantic validation follows
schema validation and recomputes every submitted ID.

There is no automatic conversion from C1, version guessing, default insertion, ignored extension,
or compatibility flag. Importing a C1-shaped scenario means constructing an explicit C2 body and
new expectations. It never preserves the v1 generator or result identity. A semantic or wire change
requires a new identity.

The CLI remains validation-only. Synthetic fixtures must say `NOT QUALIFICATION EVIDENCE` and may
not be accepted by a runner approval path. No real runner plan exists until DKS-Q2-001 through
DKS-Q2-008 close and independent review plus named owner approval succeeds.

## Rejected M1 arithmetic sketch

M1 is a reviewed non-Cartesian arithmetic and campaign-cost sketch. It is **not** a lifecycle
candidate, an executable plan, or measurement evidence, and it does not close Z3 or
DKS-Q2-002/003. Its exact numbers are retained so an M2 replacement cannot silently repeat the
same infeasible state shapes or lose the useful phase-alignment arithmetic.

All rows below assume 256 vnodes, one foreground state worker, 128 raw logical rows per request,
full logical prefill, no explicit persist during warmup/measurement, and the shorthand
`setup_final_persist_v1`: persist, close/reopen, and verify setup before warmup; preserve warm state
across the measurement boundary; then persist and verify after measured drain with writes charged
to the resource tail. That shorthand has no tag in the current case-body persistence union, so no
canonical M1 case body can be encoded. M2 must either add one exact lifecycle tag or choose an
existing representable policy; the exact Fjall/RocksDB calls remain DKS-Q2-007.

| Slug | Scenario | Distribution | Residency / placement | Width | Logical state |
|---|---|---|---|---|---|
| `agg-r-c-hm-normal-128` | aggregate | `hot_mix_v1` | resident / identity-mod | compact | `N=17,716,740`, `M=178,957`, `C=17,895,697`, `B=4,294,967,280` |
| `agg-s-v-zipf-normal-128` | aggregate | Zipf v1 | spill / identity-mod | bounded variable | `N=60,028,485`, `M=606,349`, `C=60,634,834`, `B=103,079,214,800` (artifact-compatibility risk) |
| `timer-r-v-hm-hot-128` | timer/window | `hot_mix_v1` for mutations | resident / forced vnode 0 | bounded variable | `N=1,244,027`, `M=12,800`, `C=1,256,827`, `R=2,526,454`, `B=4,294,964,960` (artifact-compatibility risk) |
| `timer-s-c-zipf-normal-128` | timer/window | Zipf v1 for mutations | spill / identity-mod | compact | `N=211,543,116`, `M=2,136,832`, `C=213,679,948`, `R=429,496,728`, `B=103,079,214,720` (artifact/transition-compatibility risk) |
| `join-r-c-hm-mix-128` | static range probe | `hot_mix_v1` | resident / identity-mod | compact | `N=245,140`, `B=[4,294,928,880, 4,294,944,240, 4,294,959,600]` |
| `join-s-c-zipf-mix-128` | static range probe | Zipf v1 | spill / identity-mod | compact | `N=5,883,510`, `B=[103,079,171,280, 103,079,186,640, 103,079,202,000]` (artifact/transition-compatibility risk) |

`C` is the stable entity/record count named by each scenario, `M` is its churn/TTL pool, `R` is
total live records, and `B` is logical key-plus-value bytes. A three-value `B` is low/setup/high
over the join output cycle. These are declarative generation inputs or arithmetic
consequences, not allocated filesystem bytes, RSS claims, or observed residency. In particular,
calling a case `spill` from its live-byte target does not prove a cold-I/O working set.

Compact records are 32-byte keys plus 208-byte values. `bounded_var_mod4_v1` selects by stable
logical record class, never request order:

| Class | Key bytes | Value bytes | Logical record bytes |
|---:|---:|---:|---:|
| 0 | 16 | 64 | 80 |
| 1 | 64 | 256 | 320 |
| 2 | 256 | 1,024 | 1,280 |
| 3 | 4,096 | 1,024 | 5,120 |

For M1 arithmetic only, aggregate sampled rank `r` uses global ordinal `r` and cold slot `s` uses
global ordinal `N+s`; both width and identity-mod placement consume that ordinal. A timer entity
uses its entity ordinal modulo four for all of its records, and a TTL slot uses `slot mod 4` across
successors. Both join cases are compact. This is not a registered width-selection policy.
The modulo-four sketch maps rank zero—the hottest hot-mix and Zipf rank—to the cheapest 80-byte
class. It therefore does not qualify hot-wide latency. M2 must use a reviewed identity-hash class
mapping with exact balance and a named hot-wide control, or explicitly narrow the claimed width
coverage.

An all-distinct 128-row request is therefore at most 655,360 logical bytes for one record per row.
The profile's 65,536-byte value remains in one-record semantic/storage conformance and an explicit
all-distinct negative preflight: `128 * (16 + 65,536) = 8,390,656` already exceeds the 8 MiB hard
limit before framing. It is not smuggled into a positive case through expected hot-key deduplication.

### Rates, counts, and campaign floor

M1's offered rates are the minimum at which 950-permille achieved rows equal the current absolute
scenario throughput gate. They are provisional owner inputs, not demonstrated headroom:

| Scenario | Offered requests/s | Offered raw rows/s | Warmup requests | Measured requests |
|---|---:|---:|---:|---:|
| aggregate | `15,625 / 19` | `2,000,000 / 19` | 740,132 | 1,480,264 |
| timer/window | `46,875 / 76` | `1,500,000 / 19` | 555,100 | 1,110,200 |
| join | `15,625 / 38` | `1,000,000 / 19` | 370,125 | 740,250 |

Counts meet the 900/1,800-second and 200,000-request minima. Timer counts are multiples of five;
join counts are multiples of 125, so 128 rows/request completes an integer number of 1,000-row
fanout cycles. Across six cases, five repetitions, and two candidates, schedule time alone is at
least 162,000 seconds (45 hours), before setup, reopen, stabilization, drain, resource tail,
cooldown, validation, or failures. Fresh setup writes approximately 3,000 GiB of logical live state
across the 60 slots before write amplification. The two candidates together schedule 99,921,420
warmup-plus-measured requests and 12,789,941,760 raw logical rows. Plan approval must add measured
setup throughput, cooldown, disk endurance, and a bounded campaign deadline; state reuse is still
forbidden.

The candidate-neutral finite runtime preflight alone would replay 49,960,710 requests and
6,394,970,880 raw rows over 30 case/seed streams, before its full setup streams. Calling that replay
"streamed" is not a resource bound. M2 needs a bounded, deterministic, candidate-independent oracle
design with memory/scratch/time/artifact ceilings, isolated storage, cleanup rules, and a
fail-closed deadline before its feasibility can be approved. External run/sort/merge is one option,
not a decision. Fjall and RocksDB cannot serve as the independent oracle.

### M2 bounded preflight and runtime-oracle decision

M2 rejects that expanded replay. Static preflight performs only schema/structural validation,
checked analytical maxima, and enumeration of a mathematically proven finite cycle. It does not
generate every Zipf request, observation, or post-warmup state. Across the complete proposed matrix,
the static proof is limited to 8,388,608 entity visits, 256 MiB heap, zero disk scratch, 8 MiB per
expectation artifact, 16 MiB of total static artifacts/logs, 600 seconds wall clock, and 60 seconds
cleanup. Overflow, deadline, proof-closure, retry-bound, publication, or cleanup failure is
fail-closed; a partially published object is not plan input.

The setup/final verifier is a different runtime population. Each exact expected/candidate merge is
limited to 67,108,864 logical records, 103,079,215,104 logical key/value bytes (96 GiB), and
103,951,630,336 canonical record bytes (logical bytes plus the maximum 13-byte framing per record).
It gets 256 MiB heap, zero disk scratch, a 4-MiB mismatch-witness ring, 64 KiB diagnostics, one
combined 1,800-second deadline for both iterators and the merge at each cut, and 60 seconds cleanup.
The full state is never retained as a digest input file. A case whose exact formula or codec cannot
prove these bounds is not encodable.

The aggregate cold slots admit a closed form. For cold slot `j`, request `t` selects `j=t mod M`,
deletes its current generation, and inserts its successor, so after `T` requests its generation is
`0` when `T<=j`, otherwise `1+floor((T-1-j)/M)`. This bounds exact setup/final iteration without
expanded request replay. It does not decide stable hot-rank values or prove aggregate multiplicity;
those policies remain open.

The timer proof may enumerate only its `256K` deadline ring and affine generation deltas, jumping
complete cycles and replaying a bounded remainder. It must prove that the exact, still-unfrozen
reschedule permutation returns the roster shape to closure; the 8,388,608-visit ceiling applies.
J2 may use its 200-row classifier period and 3,200-row/25-request batching superperiod only when a
phase starts at `t mod 3200 = 0`; the exact
128-row return vector is
`[316,252,253,309,252,260,245,309,260,252,246,316,252,254,309,253,261,246,309,261,253,246,324,260,254]`.
It sums to 6,752, has mean 270.08, and range 245--324. A differently aligned phase needs a frozen
rotation rule and new literal proof.

At runtime, every emitted request is validated against its exact post-fold/post-dedup row, byte,
framing, fanout, and batch ceilings before the adapter sees it. A generator/reference failure makes
the attempt INVALID. Candidate observation overflow is candidate FAIL only when the approved
analytical proof and the runtime expected observation both establish that the correct response fits;
otherwise it is runner INVALID. Zipf uses the approved analytical retry bound plus runtime
fail-closed/no-reseed semantics unless owners choose a total sampler under a new identity.

This closes only the infeasible-replay design choice. Exact payload codecs, field-mode schema,
aggregate hot-value policy, timer recurrence proof, J2 routing and signed bounds, runtime ring/lead/
lag limits, binary goldens, and named approvals remain blockers. No workload plan or backend run is
authorized.

Raw input rows are not backend operations. Each expectation contract/result pair separately binds requests/s,
raw rows/s, post-dedup point/range/mutation operations/s, logical mutation bytes/s, and returned
rows/bytes/s. A hot-mix case cannot claim LSM throughput from its raw row count.

### Reviewed arithmetic retained from M1

- Aggregate uses `N` sampled ranks plus `M=ceil(C/100)` disjoint cold slots. Request
  `G=runtime_event_ordinal` replaces each distinct sampled group and rotates slot `G mod M` from
  generation `floor(G/M)` to its successor. This keeps the sketched count/bytes stable, but M1 did
  not freeze complete entity/key/value encodings or conflict rules.
- Timer uses a five-request cycle: three mutations, one due scan, one fire. Its count identity is
  `R=2*C+M`; before request `G`, fired rows are `F=128*floor(G/5)`. The `M`-slot ring and
  `D=8,192`/`D=1,048,832` frontiers yield 4,096 rows with `has_more=true`; the resident variable
  scan is 6,963,200 logical bytes. One 128-row control request emits one range operation, not 128
  scans. Only the ring tests ordered timer fire; the `N` mutation companions are not timer
  reschedules.
- Static join-range setup has 73 immutable fixtures per rank. The reviewed M1 fanout sketch gives
  exact 500/350/140/10 weights for 0/1/8/64 matches per 1,000 rows and 189--317 live outputs per
  125-request cycle (253 at setup/final, mean 270.08). Live-record bands are
  17,895,537/17,895,601/17,895,665 resident and
  429,496,547/429,496,611/429,496,675 spill.

These are arithmetic facts, not accepted lifecycle semantics. M1 never froze timer/join key and
interval encodings, row/byte lookahead, setup-placeholder identity, canonical dedup/conflicts, or a
dynamic two-sided expiry schedule. Reads followed by one atomic mutation batch are not an atomic
read-modify-write transaction. The static probe cannot qualify a stateful join, and its
bookkeeping token proves neither watermark behavior nor external exactly-once delivery.

### Proposed gate mapping and explicit gaps

Aggregate maps to its resident/spill end-to-end gate. Timer mutation/fire maps to hot-vnode or
spill, while due scans map to timer/join-range. The static join sketch maps only to range-probe
gates. Every finite case maps to state-queue, scenario throughput, achieved-rate, CPU,
memory/disk/FD/stall/debt maxima, and applicable finite write/space-amplification gates. RSS/disk
slope gates whose sampling starts at 21,600 seconds cannot consume these 2,700-second attempts;
they belong only to the separately frozen DKS-Q2-008 endurance population.

This mapping cannot be approved yet. The profile has no candidate-service thresholds and no
join-specific write-amplification gate; neither may be inferred from an end-to-end threshold or
silently borrowed from timer state. DKS-Q2-005 must also freeze scheduler, runner-preparation,
oracle-lag, result-ring, observation-skew, and sampler/null interference gates. Maximum 1,000/8,192
batch shapes remain semantic/null/capacity controls unless owners deliberately add a gate-bearing
case and accept its campaign cost.

M1 also fails to prove compatibility with the current product limits. If one logical record becomes
one managed row and one transition carries all 256 vnodes, the compact spill timer has
1,677,721--1,677,723 rows per vnode and 429,496,728 rows total; the compact spill join has
1,677,686--1,677,759 fixture rows per vnode and 429,496,547--429,496,675 live rows total. Those would
exceed 1,048,576 rows per complete per-vnode artifact and 268,435,456 rows per transition.
Forced-vnode resident timer state would place 2,526,454 logical records and 4,294,964,960 logical
bytes in vnode zero. Aggregate spill also correlates `rank mod 4` width with `rank mod 256`
placement: a wide-class vnode reaches 236,855 records, 1,212,697,600 logical bytes, and 970,158,080
logical key bytes, above the numerical 512-MiB artifact caps if encoded directly.

Only the initial aggregate artifact codec is currently frozen; timer/join codecs, logical-to-row
mapping, and exact transition rosters are not. These calculations are therefore incompatibility
risks, not claims about nonexistent physical artifacts. The documented format still has no
within-vnode chunk identity and the transition contract has no state-shape splitting rule. M2 must
prove compatible encoded placement/width/cardinality shapes or first freeze and test an explicit
format/runtime split.

All three 96-GiB cases use Zipf, so size alone does not prove cold-read I/O. M2 needs at least one
uniform/all-distinct spill case or a pre-approved candidate-neutral device-read/cache-miss floor.
The resident timer sketch's one due scan returns 6,963,200 logical bytes at roughly 123.36 scans/s,
about 819 MiB/s before result-ring and oracle copies. Its offered rate cannot be approved until
paired counter-only/null controls bound sampler, memory-bandwidth, ring, oracle-lag, and observation
interference. A frozen warmup-end debt/stall baseline must also prevent delayed warmup writes from
entering the measured physical-write numerator without corresponding logical mutation bytes.

Short positive boundary trials must cover the limits that M1 otherwise leaves as prose: supported
large values, maximum accepted input/output batches, byte-bound scans, and near-limit atomic
mutations. They remain distinct from the five-repetition latency population and cannot donate
samples. Any untested maximum is lowered in the certified profile instead of inferred from a
negative or smaller case. Direct-I/O preflight order and cooldown are also frozen so it cannot
thermally or physically condition one candidate's subsequent slots.

The setup expected/candidate roots are produced by the exact merge and charged to setup before
warmup. Final persistence runs at resource-tail start and remains charged to that tail; close/reopen
and final merge run only after its stable/deadline cut and are charged to separate lifecycle
verification evidence. At post-warmup, only the reference recipe/provenance root is retained: a full
candidate scan/export would perturb cache, I/O, snapshot, and maintenance state. A separately
classified conformance replay may cover that deterministic boundary but cannot prove equality for
the actual performance attempt or donate samples. M2 explicitly narrows its claim to setup/final
full-state equality plus ordinal in-service read/observation validation and records
`candidate_equality_claimed=false` for post-warmup.

## Why M1 is rejected and what M2 must close

M1's state-byte, phase-alignment, fanout-cycle, and timer-ring arithmetic passed independent
read-only review. M1 as a whole failed review because its persistence shorthand is unencodable,
four state shapes have unresolved product row/byte compatibility, its width mapping makes the
hottest rank cheapest and correlates width with vnode, its timer scope is narrower than required
for qualification, and its join is not a dynamic two-sided lifecycle. No M1 body, expectations
object, plan, or backend result may be created.

The dependency order remains DKS-Q2-001/Z3 (independent review and closed binary union/registry),
then Z1 (body encoder and independent goldens), then Z2 (strict schemas and runner/evidence
bindings). Z6 and DKS-Q2-002/003 require named workload and operations owners to approve M2. At a
minimum, closure must freeze:

- aggregate full-domain versus sparse materialization, stable replacement, natural Zipf pressure,
  forced-vnode placement, and broad/all-distinct storage pressure;
- timer insertion time, monotonic frontier, half-open due ranges, row/byte pagination, serialized
  validation followed by one atomic mutation batch, TTL/backlog control, and separate
  window/timer/due/output/live-byte bands;
- deterministic setup order and complete entity/key/value encodings; canonical sort, deduplication,
  and conflict rules; exact timer slot/generation/vnode mapping; and checked live-count bands;
- deterministic dynamic two-sided join prefill and arrivals, exact event-time interval/key bounds,
  row and byte lookahead, stable arrival/output identities, expiry/replenishment, per-side
  cardinality, and mismatch-before-mutation behavior; and
- exact fixed/variable width pairs, compatible fanout/batch limits, raw rows/s, requests/s,
  post-dedup candidate operations/s, logical/returned bytes/s, rational rates/counts, gate mapping,
  and total campaign wall-clock/storage/endurance/preflight cost; and
- either a candidate-observed, interference-qualified post-warmup state witness or an explicitly
  narrower correctness claim that does not present the oracle digest as candidate equality.

Every decision uses only ordinal, seed, and oracle state—not candidate latency, compaction, or
results. The current candidate's impossible `128 * (16 + 65,536)` write and 1,000-row compact
fanout-64 join remain mandatory negative preflight cases, not matrix entries.

## M2 reviewed feasibility components

M2 is not an accepted matrix or case body. Independent read-only cross-review found no arithmetic
or lifecycle feasibility blocker in the components below and caught three defects before encoding:
the M1 rank-correlated width rule, an over-target aggregate/timer sizing attempt, and a join fanout
period that did not divide its retention horizon. This is not workload-owner approval, production
review, or independent-soak evidence. DKS-Q2-002/003, Z3, owner approval, and every execution gate
remain open. No policy name below is registered and no expectations, plan, or result object may be
emitted from these numbers.

### Balanced width and placement constraint

For stable entity ordinal `e`, let `v=e mod 256`, `local=floor(e/256)`,
`quartet=floor(local/4)`, and `lane=local mod 4`. A role- and stream-separated SHA-256 rotation
selects the two-bit value `r`; width class is `(lane+r) mod 4`. Thus every complete quartet in every
vnode and role has exactly one 80-, 320-, 1,280-, and 5,120-byte record, with exact mean 1,700
bytes. The canonical domain separator, length framing, role registry, and integer widths still
belong to Z3/Z1; concatenating informal fields is not an encoding.

Width hashes are generation decisions, not candidate service work. Their implementation must use
a bounded setup/precomputed or reviewed closed-form path; runner CPU remains charged and paired
null controls must expose interference. Per-row SHA-256 cannot silently enter the measured service
hot path.

A hot-wide control may swap each of ranks 0 through 9 with the class-3 lane in its own `(v,
quartet)`. Under identity-mod placement those ranks occupy distinct vnodes, and each swap preserves
the quartet's class multiset. This fixes M1's “hottest means cheapest” defect without changing
state bytes. It remains a reviewed transform, not a registered `selection_policy`.

Aggregate and timer dimensions use complete quartets per vnode. Join has two tail rows per vnode;
its tail rotation is `(vnode+stream_offset) mod 4`, so 64 vnodes receive each rotation and every
class receives the same total tail count. Independently hashing each tail is forbidden: it can move
the nominal total by up to 768,000 bytes.

### Corrected under-target shapes

All byte totals are logical key-plus-value bytes, not encoded artifacts, RSS, disk allocation, or
proof of cache misses. The aggregate uses `C=N+M`, where `M=ceil(C/100)` is a disjoint rotating-cold
slot domain. Timer `C` is the entity count, `D` is its timer-bearing subset, and its records are
`C` window plus `C` output/bookkeeping plus `D` timer rows and 256 compact frontier rows. Join
uses `R=W=12,800Q` live rows.

| Shape | Exact dimensions | Logical records | Logical bytes | Below target |
|---|---|---:|---:|---:|
| Aggregate resident | `C=2,526,208`; `M=25,263`; `N=2,500,945` | 2,526,208 | 4,294,553,600 | 413,696 |
| Aggregate spill | `C=60,634,112`; `M=606,342`; `N=60,027,770` | 60,634,112 | 103,077,990,400 | 1,224,704 |
| Timer resident | `C=1,131,520`; `D=262,144` | 2,525,440 | 4,292,874,240 | 2,093,056 |
| Timer spill | `C=28,219,392`; `D=4,194,304` | 60,633,344 | 103,076,311,040 | 2,904,064 |
| Join resident | `Q=197`; `W=2,521,600` | 2,521,600 | 4,286,720,000 | 8,247,296 |
| Join spill | `Q=4,737`; `W=60,633,600` | 60,633,600 | 103,077,120,000 | 2,095,104 |

The corrected aggregate shapes place 9,868 and 236,852 records per vnode. Their worst logical
bytes per vnode are 16,775,600 and 402,648,400; key bytes are 10,933,744 and 262,432,016. The timer
shapes place 9,865 and 236,849 records per vnode. Their logical bytes per vnode are 16,769,040 and
402,641,840; spill key/state bytes are 262,427,616/140,214,224. Join's reviewed allocator yields
exactly `50Q` rows per vnode: 9,850 and 236,850. Its worst tail placement gives 16,748,000 and
402,648,000 logical bytes, with 10,915,936 and 262,431,936 key bytes. All raw counts fit the current
per-artifact and transition numerical ceilings, but an actual timer/join codec plus framing and
roster proof is still required.

Aggregate requests apply the 128 source rows in source order and fold repeated point updates by
logical key, then rotate one disjoint cold slot. That gives stable cardinality while separating
natural distribution pressure from guaranteed churn. Exact values, generations, conflict rules,
distribution assignments, operation counts, and an all-distinct cold-read case remain to be
frozen; a Zipf-labelled 96-GiB shape alone is not cold-I/O evidence.

### Timer lifecycle candidate

The reviewed `2C+D+256` M2 mapping makes `D` a subset of `C`, with one window and one
output/bookkeeping row for every entity, one timer row for every `D` entity, and one compact
frontier row per vnode. Every reschedule and fire must select from `D`; the earlier suggestion to
exclude `D` from the mutation pool is rejected because those entities would have no timer row.

One five-request cycle matches the profile's 600/200/200 timer mix: three reschedule requests, one
bounded due scan, then one validated atomic fire. Each reschedule swaps 64 pairs from distinct
not-yet-fired deadline cohorts in the same vnode, covering 128 entities and incrementing their
generations. It selects 16 pairs of quartets and makes four same-lane deadline swaps per pair,
preserving vnode ownership and every role's balanced width population.

Deadlines use due-scan ordinals, not raw request ordinals. Let `K=D/(256*512)`, `v=q mod 256`, and
`bucket=floor(q/256) mod K`. At setup only, cohort entity `i` is
`v+256*(512*bucket+i)` for `0<=i<512`; rescheduling deliberately changes later rosters while
preserving 512 timers per deadline. Resident has `K=2` and a `256K=512`-scan ring; spill has `K=32`
and an 8,192-scan ring. Fire consumes the due scan's canonical ordered roster, re-reads and validates
it rather than regenerating setup membership, requeues each timer at `q+256K`, and advances the
vnode frontier in the same mutation batch.

A balanced 512-entity cohort has 870,400 row bytes and 567,296 key bytes. Fire validates five
old-present/successor-absent records per entity plus the frontier: 2,561 reads and 4,352,240 bytes of
read capacity. Its mutation roster is window put, old-timer delete/new-timer put, old-output
delete/new-output put, and one frontier put, for 3,746,032 logical mutation bytes. A narrower
C1-style proxy estimates 6,637,428 encoded bytes, leaving 1,751,180 bytes below 8 MiB, but cannot
prove the unfrozen v2 framing or encoded-request cap. An all-wide five-operation roster plus
frontier needs
`3*512*5,120 + 2*512*4,096 + 240 = 12,058,864` mutation bytes and is a mandatory negative; the
balanced-cohort invariant is required.

The spill timer's active three-row `D` bundles plus frontiers are 21,391,011,840 logical bytes,
above the configured 8-GiB block cache and therefore a stronger cache-pressure basis than the
rejected 5,347,799,040-byte (4.98-GiB) active shape. It still cannot prove physical cold reads in
the presence of OS cache, compression, prefetch, or locality. A pre-approved external device-read
or verified cache-miss floor remains mandatory.

### Dynamic two-sided join candidate J2

For integer `Q`, the live horizon is `W=12,800Q`. A signed event ordinal `t` uses Euclidean
arithmetic and `p=(11*(t mod 200)) mod 200`; bands `[0,100)`, `[100,170)`, `[170,198)`, and
`[198,200)` select fanout 0, 1, 8, and 64. This is a permutation with exact 500/350/140/10 weights.
Its class group counts are `n0=3,200Q`, `n1=2,240Q`, `n8=112Q`, and `n64=Q`. A class-event ordinal
selects group modulo `n`, alternates side by occurrence, and rotates through `max(1,fanout)` slots.
Fanout zero uses side-distinct join generations.

`W` contains exactly `64Q` complete classifier periods, so advancing by `W` shifts a group's
occurrence by exactly 2/2/16/128. The row at `t-W` is therefore the same class, group, side, and
slot as the successor at `t`; the opposite-side scan returns exactly 0/1/8/64 rows and cardinality
stays constant. Across aligned 128-row requests the reviewed return band is 245--324 rows with mean
270.08. The smallest fanout-64 group population is 197, so no group repeats within one request; all
reads may use the pre-mutation snapshot before one atomic state batch.

Those reads plus a later batch are not an atomic read-modify-write transaction. C2 relies on its
single serialized foreground state worker and must abort on any validation mismatch; this shape
does not establish C3 concurrency or serializability.

The interval is `[t-W,t)`: read first while the lower-bound row is visible, then atomically delete
the same-side `t-W` key, insert the distinct timestamped `t` key, and append output observations.
Both input next-time frontiers start a request `[a,b]` at `a`; after all arrivals through `b` and
the state batch commit, both advance to `b+1`, then cleanup may run. Synthetic progress controls
are not workload rows or samples. Endpoint sentinels cover exact lower inclusion and upper
exclusion. This case deliberately does not qualify lateness, idleness, watermark-only idle-group
eviction, expiry retractions, or sink commits.

Whole groups of 128, 16, or 2 rows can be assigned to the least-loaded vnode and the 2-row groups
fill every vnode to exactly `50Q`; the arithmetic is feasible. The canonical routed-group ID,
constant-time routing formula, signed timestamp/key padding, range bounds, and globally balanced
tail-rule stream-offset derivation remain unfrozen. The proposed setup uses negative logical
history and an order-preserving sign-bit-biased key timestamp; it must not ambiguously apply a
second epoch offset. The greedy allocator is a setup feasibility proof, not a measured hot-path
operation; runtime routing needs the still-unfrozen constant-time formula, with generator overhead
charged and controlled.
J2 is therefore a pass for logical lifecycle feasibility only, not an accepted workload or product
join semantic.

### Remaining M2 blockers

The complete matrix remains **BLOCK**. It still needs registered scenario/role/width/churn/retention
identities and exact binary encodings; complete timer/join logical-to-physical codecs; literal
goldens implementing the canonical dedup/range rules; aggregate hot-value semantics; the exact timer
recurrence/closure proof; J2 routing/signed bounds/alignment; the runtime expectation/result union
and ring/lead/lag caps; an encodable lifecycle arm; cold-I/O evidence; rates, counts, schedule,
service/runner/interference/resource gates; campaign time/endurance budget; and named workload/
operations approvals. The bounded static-proof/runtime-commitment direction is accepted only as an
M2 design component, not as a closed oracle or executable schema.

`no_explicit_persist` may describe only the same-open warmup/measurement interval. It does not by
itself encode setup durability, close/reopen verification, final persistence, cache-loss behavior,
or a checkpoint. A full candidate scan at the warmup boundary remains prohibited from donating
performance samples. Until the remaining blockers close, the CLI stays validation-only and every
candidate execution, backend selection, admission, exactly-once claim, and production-readiness
claim remains fail-closed.

## Product boundary

This is backend-local storage traffic. Even a perfect C2 result cannot establish production
watermark behavior, vnode co-partitioning, checkpoint coordination, source offset cuts, sink
prepare/commit/fencing/reconciliation, at-least-once or exactly-once delivery, rebalance safety, or
cluster admission. `[LDB-4007]` and `[LDB-0013]` remain fail-closed.

The independent production soak keeps its own workload manifest, driver/counter domain, fresh
precommitted seed, real source and sink coordinates, immutable release artifact, external oracle,
and independent operator. It may reuse the specification and numerical goldens of an approved Zipf
mathematical transform, but not the backend generator implementation or binary; it does not inherit
this backend case body, counter, stream ID, seed rule, preflight, or result schema.

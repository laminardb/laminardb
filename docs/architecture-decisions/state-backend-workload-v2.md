# State backend long-stream workload and identity v2

- **Status:** provisional C2 identity decision; lifecycle cases and executable schemas remain blocked
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
case_stream_id + selected seed -------------> stream_instance_id
stream instance replay ----------------------> expectations artifact
expectations + preflight procedure ----------> preflight provenance artifact
independent literal producer ----------------> literal corpus
literal corpus + exact generator build ------> actual-build conformance result
independent MPFR/interval implementation ----> numerical-audit result
independent analysis ------------------------> retry-bound report
case entries + expectations + preflight + four Zipf objects + schedule/gates -> runner plan
runner plan + source/builds/environment ------> detached approval
approved attempt -----------------------------> workload/attempt results
attempt results and raw objects --------------> evidence manifest
closed manifest ------------------------------> detached completion record
```

No arrow points back upward. In particular, generated counters, state digests, observed shares,
candidate identities, plan hashes, approvals, and evidence cannot contribute to either stream ID.

V2 reserves these printable identities:

| Item | Identity |
|---|---|
| workload generator | `state-backend-workload/v2` |
| workload counter | `state-backend-counter/v2` |
| case JSON wrapper | `state-backend-workload-case/v2` |
| canonical case body | `state-backend-case-body/v2` |
| case stream ID | `state-backend-case-stream-id/v2` |
| seeded stream instance | `state-backend-stream-instance/v2` |
| request encoding | `LDB-SBQ-REQUEST-V2` |
| observation encoding | `LDB-SBQ-OBSERVATION-V2` |
| entity encoding | `LDB-SBQ-ENTITY-V2` |
| logical state encoding/digest | `LDB-SBQ-STATE-V2` |
| request stream digest | `LDB-SBQ-REQUEST-STREAM-V2` |
| observation stream digest | `LDB-SBQ-OBSERVATION-STREAM-V2` |
| request/observation trace digest | `LDB-SBQ-TRACE-V2` |
| precomputed expectations | `state-backend-workload-expectations/v2` |
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
- persistence: `0x01 no_explicit_persist`, or
  `0x02 periodic || every_requests_u64 || at_phase_end_bool`;
- each phase: its fixed tag, `request_count_u64`, and reduced requests-per-second rate; warmup is
  `0x01` and measurement is `0x02`; and
- generator limits: request bytes, read rows, read bytes, mutation bytes, encoded observation
  bytes, preflight rows, and preflight artifact bytes as seven `u64` values in that order.

`N` is a count: valid ranks are exactly `0..N-1`, so `N >= 1`. Full materialization requires
`materialized_rank_count = N`; sparse materialization requires
`1 <= materialized_rank_count < N`. Hot-mix weights sum to 1,000.
Zipf v1 fixes exponent 990 and retry limit 64. A forced vnode must be below the active nonzero vnode
count. Width pairs are ordered, nonempty, and unique; they are never an implicit Cartesian product.
Warmup/measurement requests are full batches, so each phase's raw logical rows are derived by
checked `request_count * batch_rows` and are not redundantly encoded. A rate is requests per second.
Periodic persistence runs after every `every_requests` successful requests independently within
warmup and measurement, whose cadence counters each reset to zero; `at_phase_end` adds one call
after each nonempty phase and does not apply to setup. All selected widths and batch limits must
also satisfy the approved profile and exact per-ordinal post-deduplication preflight.

The seven generator maxima apply, in order, to one canonical request's encoded bytes, one returned
observation's logical rows, one returned observation's logical key/value bytes, one request's
logical mutation key/value bytes, one encoded observation's bytes, all finite-preflight raw logical
input rows, and the externally serialized expectations artifact's bytes. Each is an inclusive
`u64` ceiling checked before allocation or iteration; the final value is a predeclared cap, never a
derived self-length field.

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

Expected setup/post-warmup/final digests, exact post-deduplication counters, hottest-key/vnode
shares, timer backlog, join return counts, and realized bands are generated values. Putting them in
the body would make the stream depend on its own result. They instead live in the expectations
artifact described below.

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
sample in 64 attempts is a preflight failure for a finite stream and INVALID if encountered in an
executing attempt; it never consumes another row's coordinates.

C2 entity suffixes, requests, observations, logical-state records/digests, request and observation
streams, and combined traces use their explicit v2 domains and bind the stream instance plus phase
where applicable. Reusing a C1 item layout as an inner logical-operation payload is permitted only
if the v2 envelope explicitly length-prefixes it; phase-aware bytes are never labelled with a v1
identity alone. The closed scenario registry must freeze exact v2 item field order before an encoder
exists. Any lifecycle identity based on time uses `runtime_event_ordinal`, not the resetting counter
or sample ordinal.

## Expectations, plan, result, and evidence boundary

One candidate-neutral expectations artifact is created per seeded stream instance before a runner
plan can be approved. It has identity `state-backend-workload-expectations/v2`, the notice
`NOT QUALIFICATION EVIDENCE`, and contains:

- the exact case body bytes and recomputed case/instance IDs plus selected seed;
- generator, counter, request/observation/entity/state/stream/trace, lifecycle-policy, sampler, and
  math-source identities;
- phase request/raw-row counts and request/observation/state stream digests;
- setup, post-warmup, and final logical-state digests;
- exact post-dedup read/range/mutation/returned-row and byte counters per phase;
- realized low/target/high checks, distinct touched/live ranks, timer backlog/due rows, join-side
  cardinalities, exact join return/fanout counts, and expected hottest-key/vnode shares; and
- bounded-preflight input rows and deterministic canonical request/observation/state payload-byte
  counters. The expectations artifact's own serialized length is excluded.

The artifact has no candidate, plan, approval, machine-result, or status field. The implementation
under test cannot serve as its independent numerical oracle. Preflight is streamed once into a
content-addressed expectations artifact and reused for both candidates; it is not regenerated
inside measured service time.

A separate `state-backend-workload-preflight/v2` provenance artifact binds the expectations length
and digest, preflight source archive/binary/lock/toolchain identities, start/end UTC timestamps,
elapsed wall and CPU time, peak memory, host identity, exit/failure classification, and stdout/stderr
object descriptors. Operational measurements are kept out of deterministic expectations so two
correct reference replays can produce identical expectation bytes. The runner plan and detached
approval bind the accepted expectations and preflight-provenance digests together.

Four Zipf objects remain separate: a literal corpus produced without calling the implementation
under test; actual-build conformance results from running that corpus against each exact generator
binary/target/build; a finite-precision MPFR/interval audit independently implemented without
calling the generator; and an analytical retry-bound report. Their exact lengths/digests,
source/binary/toolchain/precision/rounding identities, declared target/build coverage, and reviewer
provenance are plan and approval inputs. None enters generated case-body or expectations bytes.

All generator, preflight, and independent-audit source archives and binaries are finalized before
their generated artifacts. Their explicit inclusion manifests exclude case wrappers, expectations,
preflight provenance, literal/conformance/numerical/retry results, runner plans, approvals, attempt
results, evidence manifests, and completion records. Generated objects are never copied back into a
source archive whose digest they bind.

The complete runner plan binds exact case wrapper bytes, every required case/seed expectations and
preflight-provenance length/digest, schedule, candidates, gates, result-ring sizes, machine policy,
and artifact limits. The detached approval binds that complete plan plus exact
source/build/environment inputs.
An observed `state-backend-workload-result/v2` then binds the approved plan, detached approval,
slot/candidate, and matching expectations digest while recording actual counters/digests and
artifact descriptors.
The evidence manifest references results; it does not repeat or mutate case semantics.

Within the evidence manifest, a closed `state-backend-workload-evidence/v2` binding contains only
the slot index, case and stream-instance IDs, selected seed, and length/SHA-256 descriptors for the
approved expectations, preflight provenance, observed workload result, and required raw phase
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

## Remaining lifecycle and matrix work

This framing decision intentionally does not guess the scenario union. The dependency order is
DKS-Q2-001/Z3 (closed semantics and registry), then Z1 (body encoder and independent goldens), then
Z2 (strict schemas and runner/evidence bindings); Z6 and DKS-Q2-002/003 additionally require one
non-Cartesian named matrix. Those policies must prove:

- aggregate full-domain versus sparse materialization, stable replacement, natural Zipf pressure,
  forced-vnode placement, and broad/all-distinct storage pressure;
- timer insertion time, monotonic frontier, half-open due ranges, row/byte pagination, exact atomic
  fire/delete replacement, TTL/backlog control, and separate window/timer/due/live-byte bands;
- deterministic opposite-side join prefill, exact returned fanout with `has_more=false`, side
  schedule, event interval, stable identities, two-sided expiry, per-side cardinality, and output
  bookkeeping; and
- exact fixed/variable width pairs, compatible fanout/batch limits, raw rows/s, requests/s,
  post-dedup candidate operations/s, logical/returned bytes/s, rational rates/counts, gate mapping,
  and total campaign wall-clock/storage cost.

Every decision uses only ordinal, seed, and oracle state—not candidate latency, compaction, or
results. The current candidate's impossible `128 * (16 + 65,536)` write and 1,000-row compact
fanout-64 join remain mandatory negative preflight cases, not matrix entries.

## Product boundary

This is backend-local storage traffic. Even a perfect C2 result cannot establish production
watermark behavior, vnode co-partitioning, checkpoint coordination, source offset cuts, sink
prepare/commit/fencing/reconciliation, at-least-once or exactly-once delivery, rebalance safety, or
cluster admission. `[LDB-4007]` and `[LDB-0013]` remain fail-closed.

The independent production soak keeps its own workload manifest, driver/counter domain, fresh
precommitted seed, real source and sink coordinates, immutable release artifact, external oracle,
and independent operator. It may reuse an approved Zipf mathematical transform and its numerical
goldens, but it does not inherit this backend case body, counter, stream ID, seed rule, preflight, or
result schema.

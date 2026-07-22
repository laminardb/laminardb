# Deterministic Zipf workload generator v1

- **Status:** provisional C2 decision; algorithm selected, qualification use still blocked
- **Scope:** standalone state-backend qualification workload only
- **Not runtime code:** this does not change cluster admission or operator execution
- **Approval required:** workload owner and operations owner
- **Related decisions:** [qualification model v1](state-backend-qualification-model-v1.md) and
  [runner/evidence contract v1](state-backend-qualification-runner-v1.md)

## Decision

Use a bounded, counter-addressed rejection-inversion sampler for the proposed Zipf cases. Its
reserved identity is `state-backend-zipf-ri-hd99-softf64/v1`. The implementation is based on the
Hörmann--Derflinger rejection-inversion method and the stabilized construction in Apache Commons
RNG 1.7.

This sampler is a subordinate transform, not an amendment to `state-backend-workload/v1`.
Integrating it reserves `state-backend-workload/v2` plus a versioned C2 plan/result schema that
binds sampler identity, math-source identity, domain, counter identity, and case-stream identity.
No v1 model result can claim Zipf behavior. Those C2 encodings are deliberately not invented in
this ADR and block implementation beyond isolated numerical feasibility tests.

The sampler is not approved for qualification execution yet. It becomes usable only if the same
setup constants and output goldens are bit-identical on every declared qualification target, an
independent high-precision audit accepts its numerical error, and null-adapter tests show that
generation does not corrupt the offered-load schedule. Failure of any gate leaves DKS-Q2-001 open;
there is no target-specific output, native-libm fallback, or modulo fallback.

This decision deliberately separates two workload families:

- `hot_mix_v1` retains the existing adversarial 50/30/20 one-key, nine-key, and uniform-remainder
  mix; and
- `zipf_ri_hd99_softf64_v1` represents broad rank skew.

A case selects exactly one family. The profile's `zipf_exponent_milli` field does not silently
change existing C1 requests or goldens.

## Why this design

The performance cases can contain more than 1.2 billion candidate keys. YCSB's generator computes
an O(N) zeta sum and uses mutable runtime randomness. A full CDF or alias table would add roughly
10--20 GB at that domain before accounting for construction scratch, alter the memory-pressure
experiment, and make the lookup structure part of the cache workload. Capping a table to a small
hot set would make a nominal spill case mostly resident.

Rejection-inversion has O(1) setup and memory and supports the full planned domain. The expectation
that it needs close to one proposal is a hypothesis to verify, not an admitted workload property.
Counter addressing contains a numerical or rejection difference to one logical row instead of
shifting every later row. Its weakness is binary floating-point
reproducibility, so software math, exact operation order, cross-target goldens, and fail-closed
preflight are part of the decision rather than implementation details.

## Mathematical and numerical contract

For domain size `N`, output rank `r` is in `0..N`. The ideal reference distribution is

```text
P(r) = (r + 1)^(-99/100) / sum(j = 1..N, j^(-99/100)).
```

V1 accepts only `zipf_exponent_milli = 990` and `1 <= N <= 2_147_483_647`. The runner plan stores
`N` explicitly; it is not inferred from bytes during execution. A different exponent, domain
limit, math implementation, operation order, random-bit mapping, or retry limit requires a new
sampler identity.

The ideal rational exponent is provenance, not a claim that binary64 implements real arithmetic.
The executable approximation freezes these binary64 constants:

| Constant | Bits |
|---|---:|
| `q` | `0x3fefae147ae147ae` |
| `one_minus_q` | `0x3f847ae147ae1480` |
| `taylor_threshold` | `0x3e45798ee2308c3a` |
| `one_half` | `0x3fe0000000000000` |
| `one_third` | `0x3fd5555555555555` |
| `one_quarter` | `0x3fd0000000000000` |
| `two_pow_minus_53` | `0x3ca0000000000000` |

Constants are constructed with `f64::from_bits`; parsing decimal text is not normative. The only
v1 math candidate is `libm = 0.2.16` with default features disabled and
`force-soft-floats` enabled for `log`, `exp`, `log1p`, and `expm1`. It does not become the v1 math
source until an exact Cargo package checksum, source archive, lockfile, Rust 1.95.0 artifact,
target/CPU features, build flags, and feature set are frozen by approval. Changing any of them
requires a new identity. Rust `std` transcendental methods and the host C math library are
prohibited. The executable uses round-to-nearest-even binary64 basic operations, no fast-math and
no fused replacement of a specified multiply followed by add.

Pure Rust is not accepted as proof of reproducibility. Only the cross-target gates below may admit
this math source.

### Frozen functions

The pseudocode uses one assignment per rounded binary64 operation. Calls are not evaluated when
their branch is not taken:

```text
helper1(t):
  if abs(t) > taylor_threshold:
    a = log1p(t)
    return a / t
  a = one_quarter * t
  b = one_third - a
  c = t * b
  d = one_half - c
  e = t * d
  return 1 - e

helper2(t):
  if abs(t) > taylor_threshold:
    a = expm1(t)
    return a / t
  a = one_quarter * t
  b = 1 + a
  c = t * one_third
  d = c * b
  e = 1 + d
  f = t * one_half
  g = f * e
  return 1 + g

H(x):
  log_x = log(x)
  t = one_minus_q * log_x
  helper2(t) * log_x

h(x):
  a = log(x)
  b = (-q) * a
  return exp(b)

H_inverse(x):
  t = x * one_minus_q
  if t < -1: t = -1
  a = helper1(t)
  b = a * x
  return exp(b)
```

Setup for a domain caches, as exact binary64 bit patterns:

```text
H1 = H(1.5) - 1
HN = H(N + 0.5)
R  = H1 - HN
S  = 2 - H_inverse(H(2.5) - h(2))
```

`N` converts exactly to binary64 at the admitted bound. Construction evaluates setup in the order
above and requires every returned function value and `H1`, `HN`, `R`, and `S` to be finite;
`H1 < HN`; `R < 0` and not negative zero; and `0 < S < 1`. Failure is
`zipf_invalid_setup`, a plan-validation error. It allocates no per-sample memory. Exact
intermediate-bit conformance remains a gate because a prose arithmetic contract is not a substitute
for testing the built artifact.

## Counter and sampling contract

Each planned case will have a 32-byte `case_stream_id`. It is derived as SHA-256 over a separate
domain and the length-prefixed canonical C2 case body, excluding the derived ID, schedule slots,
candidates, approvals, results, and evidence. The body must bind distribution, sampler/math
identities, `N`, scenario semantics, widths, batch shape, setup/churn/retention policies, phase
counts/rates, cardinality bands, and all generator limits. DKS-Q2-002/003 must freeze that body's
domain, field order, widths, and encoding before counter code exists; this rule prevents a
self-hash but is not yet an executable derivation. Paired candidates use the same ID and seed.

Finite-run phase tags are `0x00 setup`, `0x01 warmup`, and `0x02 measured`. Request and row
ordinals are unsigned, zero-based, and reset to zero at each phase; a row ordinal addresses the
logical input row before deduplication. Scenario tags reuse the frozen C1 values `0x01 aggregate`,
`0x02 timer_window`, and `0x03 join`. Setup/prefill row semantics remain a DKS-Q2-003 blocker.
Adding or changing a tag, coordinate scope, or reset rule requires a new counter identity.

For attempts `a = 0, 1, ..., 63`, the random word is the first eight bytes, interpreted big-endian,
of:

```text
SHA-256(
  "LDB-SBQ-ZIPF-RI-HD99-SOFTF64-V1\0" ||
  model_input_sha256[32] ||
  case_stream_id[32] ||
  seed_u64_be ||
  phase_tag_u8 ||
  scenario_tag_u8 ||
  N_u64_be ||
  request_ordinal_u64_be ||
  row_ordinal_u32_be ||
  attempt_u8
)
```

The proposal uses the following rounded temporaries. `word >> 11` is converted exactly to
binary64. `b = x + one_half` must satisfy `0 <= b < 2^63`; conversion to signed 64-bit truncates
toward zero. A negative inverse is deliberately invalid even though the real-valued method cannot
produce one. Any violation is `zipf_invalid_sample`, which makes finite plan validation fail and an
executing attempt INVALID. Every evaluated function result and temporary through `g` must be
finite; a non-finite value is the same error rather than an ordinary rejection.

```text
u_bits = word >> 11
u_int = exact_f64(u_bits)
u = u_int * two_pow_minus_53                 # exactly [0, 1)
a = u * R
z = HN + a                                   # separate multiply and add
x = H_inverse(z)
b = x + one_half
k_i64 = truncate_toward_zero_to_i64(b)
k_i64 = clamp(k_i64, 1, exact_i64(N))        # normative numerical boundary clamp
k_f64 = exact_f64(k_i64)
c = k_f64 - x

if c <= S:
  accept                                      # do not evaluate the second branch
else:
  d = k_f64 + one_half
  e = H(d)
  f = h(k_f64)
  g = e - f
  accept if z >= g

output exact_u64(k_i64 - 1)
```

The first accepted proposal wins. Sixty-four rejected proposals are
`zipf_rejection_limit`, a deterministic generator failure. Exact plan preflight rejects a stream
containing that outcome for finite setup, warmup, and measured coordinates; encountering it during
execution makes the attempt INVALID. Clamp above is the method's normative boundary correction and
its mass is included in the numerical audit. There is no post-rejection clamp, uniform/modulo
fallback, seed change, or consumption of another row's coordinates. Forced cap tests inject words
at the sampler boundary; they do not search for a convenient SHA-256 coordinate.

Rank maps directly to logical group identity. This correlates popularity with key order and gives
the hottest key's vnode natural Zipf pressure. Every case reports expected and observed hottest-key
and hottest-vnode shares. It is distinct from a forced all-to-one-vnode case, and results cannot be
generalized to scrambled popularity. Any future permutation must be a specified bijection and a
new identity.

## Qualification and hot-path gates

DKS-Q2-001 remains blocked until all of the following are checked into immutable review evidence:

1. **Platform matrix.** Identical setup-bit and output goldens on every declared target, at minimum
   `x86_64-unknown-linux-gnu` and `aarch64-unknown-linux-gnu`, in debug and the exact release build.
   Windows development builds may be supported only after passing the same corpus. An unlisted or
   failing target cannot emit qualification evidence. Tests run inside the actual generator process
   after floating-point initialization; build review rejects FMA/reassociation in the relevant
   object code or equivalent compiler artifact.
2. **Separate conformance and numerical audits.** Independent literal vectors first test exact bits
   of the frozen implementation. A separately implemented MPFR/interval reference then checks
   boundary stability and the executed finite-precision law—including the 53-bit uniform grid,
   boundary clamp, binary64 decisions, and retry cap—against the ideal distribution for
   `N = 1, 2, 3, 10`, powers of two plus/minus one, every planned domain, and the maximum domain.
   Review freezes exact reference version/precision/rounding, corpus encoding, and acceptable head,
   tail, CDF, and total-variation error before candidate results. It never calls the code under
   test. Binary64 output is an audited approximation, never “exact Zipf”.
3. **Retry bound.** Review states its uniform-word/random-oracle assumptions and derives a
   conservative acceptance lower bound and 64-rejection probability. A million-sample observation
   is secondary evidence, not the proof.
4. **Independent goldens.** Hand-authored word-to-rank vectors cover `u` endpoints, the reachable
   upper clamp, both acceptance branches, rejection, and injected cap exhaustion. Separately
   injected numerical-boundary vectors cover the lower clamp and invalid negative/non-finite
   inverse values; no valid-word vector is claimed for an unreachable edge. Separate tooling
   produces bounded corpus digests without calling the implementation under test; it need not
   regenerate an entire endurance run.
5. **Schedule and hardware isolation.** Before results, every approved batch shape and peak raw
   rows/s gets sampler-on versus counter-only/null gates for preparation p50/p99/p99.9/max,
   scheduler lateness, queue age/occupancy, controller CPU, attempts/sample, and sustained
   throughput with explicit headroom. The plan rejects any shape that grows its queue. It freezes
   core/SMT/LLC placement, frequency/throttle evidence, and a paired interference control; measured
   interference is never subtracted. An invariant SHA-256 prefix midstate may be reused because it
   preserves the canonical 127-byte message, but that exact build is benchmarked. Sampler time is
   runner preparation and offered end-to-end latency, never candidate service.
6. **Named case assignment.** The workload owner approves the exact non-Cartesian cases using
   `hot_mix_v1` and `zipf_ri_hd99_softf64_v1`, including timer/join semantics, domain sizes, live
   cardinality bands, phase ordinals, raw rows/s, post-dedup candidate operations/s, expected
   hottest-vnode share, and resident/spill behavior. Every rank in `0..N` must map to a valid
   logical identity; the plan states whether that universe is prefilled or sparsely materialized and
   binds expected distinct-touched/live-cardinality bands relative to `N`. It need not touch or
   retain every rank unless the named setup policy says so. Exponent 0.99 is one synthetic point,
   not a claim to represent every production workload.

The independent production soak has its own canonical workload-manifest and driver identity. It may
reuse the approved Zipf transform and its goldens, but it does not reuse this backend counter,
profile seed rule, or `case_stream_id` by implication. Its finite duration, fresh seed, phase
coordinates, scheduled/sent/terminal conservation, null-sink headroom, sampler failures, and
preparation latency must be bound by the soak contract. Analytical retry evidence plus runtime
fail-closed handling is permitted when full endurance-stream precomputation is not affordable; a
short rank file cannot be looped silently.

## Remaining DKS-Q2-001 sub-blockers

This ADR may be committed as a provisional selection, but the reserved identities cannot be
implemented or approved until these are closed:

| ID | Required closure |
|---|---|
| `Z1` | Freeze the non-circular canonical C2 case body and `case_stream_id` encoding. |
| `Z2` | Add workload-v2 plan/result/evidence identities and schema bindings without changing C1. |
| `Z3` | Freeze per-scenario setup, phase, row, lifecycle, domain, and cardinality semantics. |
| `Z4` | Freeze the exact math package checksum/build and prove actual-artifact conformance. |
| `Z5` | Approve the independent finite-precision error metrics, thresholds, corpus, and retry bound. |
| `Z6` | Approve named hot-mix/Zipf cases and all schedule/interference gates before backend data. |
| `Z7` | Add the separate soak workload identity and seed/coordinate/evidence rules. |
| `Z8` | Record Apache/libm attribution, source hashes, licenses, and SBOM entries before distribution. |

## Rejected and deferred alternatives

- **YCSB ZipfianGenerator:** rejected as the contract. It uses an O(N) floating zeta sum,
  `ThreadLocalRandom`, and a floating approximation whose output is not directly addressable.
- **Full CDF or alias table:** rejected for the current billion-key boundary because its memory and
  cache footprint would materially change the experiment.
- **Small-domain alias table:** rejected for spill cases because limiting Zipf to a resident hot
  set does not exercise the declared full-state distribution.
- **Native or `std` floating math:** rejected because Rust does not promise invariant
  transcendental results across platforms and builds.
- **Precomputed rank stream:** retained only as a future separately reviewed fallback if software
  math cannot pass. Large streams can perturb memory/I/O and are unsuitable for a long finite
  endurance soak; they must not appear behind the v1 identity.
- **Bespoke fixed-point or segmented approximation:** deferred. It adds a new numerical method and
  distribution-error proof without current evidence that the simpler O(1) sampler fails.

## Sources and licensing

- W. Hörmann and G. Derflinger, “Rejection-Inversion to Generate Variates from Monotone Discrete
  Distributions,” 1996, [DOI record](https://doi.org/10.57938/4f886353-ad5e-4148-a6ae-fbe162fdacf6).
- Apache Commons RNG 1.7,
  [`RejectionInversionZipfSampler`](https://github.com/apache/commons-rng/blob/rel/commons-rng-1.7/commons-rng-sampling/src/main/java/org/apache/commons/rng/sampling/distribution/RejectionInversionZipfSampler.java),
  Apache-2.0. A source-derived port must preserve required attribution and identify modifications.
- YCSB 0.17.0,
  [`ZipfianGenerator`](https://github.com/brianfrankcooper/YCSB/blob/0.17.0/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java),
  Apache-2.0; used only for comparison.
- Rust [`f64` precision documentation](https://doc.rust-lang.org/stable/std/primitive.f64.html) and
  [`libm` 0.2.16](https://docs.rs/crate/libm/0.2.16), published under MIT.

No paper text or third-party source is vendored by this ADR.

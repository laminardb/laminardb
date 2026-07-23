# Deterministic Zipf workload generator v1

- **Status:** provisional C2 decision; algorithm selected, qualification use still blocked
- **Scope:** standalone state-backend qualification workload only
- **Not runtime code:** this does not change cluster admission or operator execution
- **Approval required:** workload owner and operations owner
- **Related decisions:** [qualification model v1](state-backend-qualification-model-v1.md),
  [runner/evidence contract v1](state-backend-qualification-runner-v1.md), and the
  [long-stream workload/identity v2](state-backend-workload-v2.md)

## Decision

Use a bounded, counter-addressed rejection-inversion sampler for the proposed Zipf cases. Its
reserved identity is `state-backend-zipf-ri-hd99-softf64/v1`. The implementation is based on the
Hörmann--Derflinger rejection-inversion method and the stabilized construction in Apache Commons
RNG 1.7.

This sampler is a subordinate transform, not an amendment to `state-backend-workload/v1`.
Integrating it uses `state-backend-workload/v2` and the separate C2 case/stream identity contract,
which binds sampler identity, math-source identity, domain, counter identity, and case-stream
identity without importing C1's complete seed vector. No v1 model result can claim Zipf behavior.
The C2 scenario schemas and named cases remain blocked beyond isolated numerical feasibility tests.

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

For domain size `N`, output rank `r` satisfies `0 <= r < N`. The ideal reference distribution is

```text
P(r) = (r + 1)^(-99/100) / sum(j = 1,...,N, j^(-99/100)).
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
target/CPU features, build flags, and feature set are frozen by approval. A package, algorithm,
math, operation-order, bit-mapping, or retry semantic change requires a new sampler identity. A
source, lock, toolchain, target, CPU-feature, flag, feature-set, object, or binary change requires
new build/approval identities and fresh actual-build conformance; cross-target builds retain the
same sampler identity only when the frozen corpus is bit-identical. Rust `std` transcendental
methods and the host C math library are prohibited. The executable uses round-to-nearest-even
binary64 basic operations, no fast-math and no fused replacement of a specified multiply followed
by add.

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

Each planned case has the 32-byte `case_stream_id` and each selected seed has the 32-byte
`stream_instance_id` defined by the [long-stream workload/identity v2](state-backend-workload-v2.md).
The body binds distribution, sampler/math identities, `N`, scenario semantics, widths, batch shape,
setup/churn/retention policies, phase counts/rates, cardinality bands, and generator limits. It
excludes derived expectations as well as the derived ID, seed, schedule slots, candidates,
approvals, results, and evidence. Paired candidates use the same seeded stream instance.

The [long-stream workload/identity v2](state-backend-workload-v2.md) is the single normative owner
of phase/scenario tags, case and seeded-stream derivation, ordinal scopes, and the SHA-256 proposal
word address. For attempts `a = 0, 1, ..., 63`, this sampler consumes that address's first eight
bytes as an unsigned big-endian word. Adding or changing a tag, coordinate scope, reset rule, or
counter message requires a new workload-counter identity; changing the word-to-rank transform
requires a new sampler identity.

The proposal uses the following rounded temporaries. `word >> 11` is converted exactly to
binary64. `b = x + one_half` must satisfy `0 <= b < 2^63`; conversion to signed 64-bit truncates
toward zero. A negative inverse is deliberately invalid even though the real-valued method cannot
produce one. Any violation is `zipf_invalid_sample`: it fails a bounded static/finite-cycle proof if
that proof evaluates the coordinate and makes an executing attempt INVALID. Static plan validation
does not expand every setup/warmup/measurement row to search for the outcome. Every evaluated
function result and temporary through `g` must be finite; a non-finite value is the same error rather
than an ordinary rejection.

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

The first accepted proposal wins. Sixty-four rejected proposals are `zipf_rejection_limit`, a
deterministic generator failure. Encountering it in a bounded proof fails that proof; encountering it
during execution makes the attempt INVALID. The approved analytical retry report must bound the
complete planned population under its stated random-oracle assumptions, and workload/operations
owners must explicitly accept runtime fail-closed handling. Otherwise closure requires a total
sampler under a new identity. Clamp above is the method's normative boundary correction and its mass
is included in the numerical audit. There is no post-rejection clamp, uniform/modulo fallback, seed
change, or consumption of another row's coordinates. Forced cap tests inject words at the sampler
boundary; they do not search for a convenient SHA-256 coordinate.

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
   hottest-vnode share, and resident/spill behavior. Every rank satisfying `0 <= r < N` must map to a valid
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

Cycle 13 extracts the pre-existing setup/proposal literals into a detached JSON corpus whose own
fields state `qualification_eligible=false` and `independently_generated=false`. The feature-gated
tests consume that corpus in Windows x86_64 and Linux x86_64 debug/release checks. Required CI now
also provisions Windows release and native Linux arm64 debug/release checks, but those jobs are not
evidence until they pass on the branch. This reduces same-function test coupling and target
coverage debt; it does not turn transcribed literals into the independent goldens or numerical
audit required above.

Cycle 14 adds `tools/state-backend-zipf-oracle`, an implementation-isolated CPython 3.13 prototype
using gmpy2 2.3.1 and MPFR 4.2.2 directed intervals. Its fixed 92-domain search emits 471 canonical
NDJSON records (865,397 bytes, SHA-256
`8ad14317bdb1f12d67b9f823bea0759d33034e4c01164c2dbac90ad870f2474b`) identically on the local
Windows x86_64 host and a pinned Linux x86_64 container. It imports neither the Rust candidate nor
its literal corpus, escalates unresolved decisions through 4,096-bit precision, and marks every
record ineligible and non-authorizing. CI is configured for Linux x86_64, Windows x86_64, and native
Linux arm64.

Cycle 15 [CI run 30047503740, attempt
2](https://github.com/laminardb/laminardb/actions/runs/30047503740) exercises all three hosted paths
at commit `1cc095bc`. Linux x86_64 passes the 14 oracle tests plus an explicit CLI byte-count/hash
check. Native Linux arm64 passes 111 Rust Zipf tests in both debug and release and all 14 oracle
tests. Hosted Windows passes the standalone validator, 111 Rust Zipf tests in both modes, and all
14 oracle tests. This closes the prototype's configured-platform execution debt; it does not make
the same-tool canonical hash an independent distribution or candidate-conformance result.

This prototype is decision input, not the independent audit or qualification corpus. It has no
candidate-output comparator, approved head/CDF/tail/total-variation thresholds, finite-grid
rejection or retry bound, observation-bound dependency-installation receipt, workload-owner
operation, or sampler/case approval. Exact-equality and one-interval-step adversarial tests are
also required before promotion. Z4/Z5, the interference harness, workload-v2 registry, complete
licensing/SBOM record, and named-owner sampler/case decision therefore remain open.

This ADR may be committed as a provisional selection, but the reserved identities cannot be
implemented or approved until these are closed:

| ID | Required closure |
|---|---|
| `Z1` | After Z3, implement and independently golden-test the closed non-circular C2 case body, case ID, and seeded stream encoding. |
| `Z2` | Implement the frozen workload-v2 case/expectations/result schemas and runner/evidence bindings without changing C1. |
| `Z3` | First freeze the closed per-scenario setup, phase, row, lifecycle, domain, cardinality, and policy-ID registry required to make Z1 injective. |
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
- [gmpy2 2.3.1](https://pypi.org/project/gmpy2/2.3.1/) is LGPL-3.0-or-later and supplies the
  prototype's MPFR-backed directed-rounding contexts. The exact permitted wheel hashes are pinned
  in the tool rather than inferred from a version label.
- [MPFR 4.2.2](https://www.mpfr.org/mpfr-current/) is LGPL-3.0-or-later. Its use here does not
  substitute for the Z4 artifact receipt, complete attribution record, or SBOM.

No paper text or third-party source is vendored by this ADR.

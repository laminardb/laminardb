# Standalone Zipf MPFR oracle prototype

This directory is an isolated numerical screening tool for the provisional state-backend Zipf
transform. It is **not qualification evidence** and cannot run a workload or backend. It does not
import LaminarDB, the Rust candidate implementation, or the candidate literal corpus.

The tool evaluates the ideal exponent `99/100` with independent formulas:

```text
H(x)          = expm1((1-q) * log(x)) / (1-q)
h(x)          = exp(-q * log(x))
H_inverse(y)  = exp(log1p((1-q) * y) / (1-q))
```

Every operation propagates an MPFR interval using downward rounding for the lower endpoint and
upward rounding for the upper endpoint. A decision that straddles an interval is retried at
256, 512, 1,024, 2,048, then 4,096 bits. It fails as `ORACLE_UNRESOLVED` rather than guessing after
the final precision. Precision, rounding, exponent bounds, subnormal behavior, traps, and complex/
rational context options are constructed explicitly; an inherited ambient MPFR context is not used.

The deterministic NDJSON observation contains:

- all in-range `2^k-1`, `2^k`, and `2^k+1` domains for `k=1..30`, plus `1, 2, 3, 10`, the
  provisional `1,288,490,188` domain, and the maximum `2,147,483,647` domain;
- the 53-bit grid endpoints and midpoint, including a lower-word ignored-bit control;
- standalone deterministic-search adjacent grid points around ideal squeeze acceptance, boundary
  acceptance, and rejection; and
- a synthetic 64-repeat rejection control whose probability and policy remain unapproved.

It does **not** calculate or approve head/CDF/tail/total-variation error thresholds, finite-grid
rejection mass, retry probability, candidate differences, or a sampler decision. Those fields are
`not_computed`. The generated SHA-256 asserted by the tests is only a same-tool stability guard.

## Reproducible invocation

Use CPython 3.13. Hash-pinned wheels are permitted for Windows x86-64, Linux x86-64, and Linux
arm64:

```text
python -m venv .venv
.venv/Scripts/python -m pip install --only-binary=:all: --no-deps \
    --require-hashes -r requirements.txt
.venv/Scripts/python -m unittest discover -s . -p "test_*.py" -v
.venv/Scripts/python zipf_oracle.py > observations.ndjson
```

On Linux, replace `.venv/Scripts/python` with `.venv/bin/python`. The notice is written to stderr;
NDJSON alone is written to stdout. `requirements.txt` pins the exact CPython 3.13 wheels for the
three declared platforms. This prototype has been observed only on Windows x86-64 and Linux
x86-64; the arm64 wheel and configured CI path have not run. Runtime checks additionally require
gmpy2 2.3.1, MPFR 4.2.2, GMP 6.3.0, and MPC 1.4.0.

## Dependency and evidence boundary

gmpy2 2.3.1 was released on 2026-06-24 under LGPL-3.0-or-later. Its wheels bundle MPFR 4.2.2
(LGPL-3.0-or-later), GMP 6.3.0 (dual GPL-2.0-or-later/LGPL-3.0-or-later), and MPC 1.4.0
(LGPL-3.0-or-later). No wheel or third-party source is vendored here; the permitted wheel hashes are
recorded in both `requirements.txt` and every output header. The runtime version checks do not
attest which permitted wheel was installed, so the output records
`dependency_installation_attested=false`. See the
[gmpy2 2.3.1 release](https://pypi.org/project/gmpy2/2.3.1/),
[gmpy2 rounding contexts](https://gmpy2.readthedocs.io/en/latest/contexts.html), and
[MPFR 4.2.2 release](https://www.mpfr.org/mpfr-current/).

This is a prototype dependency record, not the complete Z4/Z8 build provenance, attribution, or
SBOM required before distribution or qualification. It has not been independently operated or
reviewed by the workload and operations owners.

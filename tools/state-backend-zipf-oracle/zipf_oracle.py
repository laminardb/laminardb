#!/usr/bin/env python3
"""Bounded MPFR interval observations for the provisional Zipf transform.

This standalone tool does not import LaminarDB, the Rust sampler, or its fixtures. Its output is
decision input only: it has no thresholds, candidate comparison, approval, or execution authority.
"""

from __future__ import annotations

import json
import platform
import sys
import time
from dataclasses import dataclass
from typing import Callable, Iterable

import gmpy2


NOTICE = "NOT QUALIFICATION EVIDENCE"
SCHEMA = "state-backend-zipf-mpfr-observations/v1alpha1"
GMPY2_VERSION = "2.3.1"
MPFR_VERSION = "MPFR 4.2.2"
GMP_VERSION = "GMP 6.3.0"
MPC_VERSION = "MPC 1.4.0"
PRECISIONS = (256, 512, 1024, 2048, 4096)
MAX_DOMAIN = 2_147_483_647
MAX_RECORDS = 1_024
MAX_OUTPUT_BYTES = 1_048_576
MAX_SEARCH_STEPS = 65_536
MAX_GENERATION_SECONDS = 60.0
GRID_BITS = 53
GRID_SIZE = 1 << GRID_BITS
GRID_MASK = GRID_SIZE - 1
GRID_STEP = 0x1E37_79B9_7F4A_7C15 & GRID_MASK

PYPI_HASHES = {
    "cp313-manylinux-x86_64": "32e599e078234fd6d0e59c3ae7e6f78fe033394b69018a1b80c5c7d57a3c057e",
    "cp313-manylinux-aarch64": "b4e14b32adf4a69e7755833ac795ae70b5932a50d75d2106f1cbaadf80fb642c",
    "cp313-windows-x86_64": "4e1496e1a40c87dccb13163bebb265fdb7d0579726f197d59a93b94441e4e509",
}

EVIDENCE_BOUNDARY = {
    "notice": NOTICE,
    "qualification_eligible": False,
    "validation_authorizes_execution": False,
    "independently_reviewed": False,
    "candidate_results_present": False,
    "candidate_inputs_imported": False,
}


class OracleError(RuntimeError):
    """A fail-closed oracle construction or resolution error."""


class OracleUnresolved(OracleError):
    """An interval still straddles a required decision at maximum precision."""


def require_runtime() -> None:
    if sys.implementation.name != "cpython" or sys.implementation.cache_tag != "cpython-313":
        raise OracleError("oracle requires CPython 3.13")
    if sys.version_info[:2] != (3, 13):
        raise OracleError("oracle requires Python 3.13")
    system = platform.system()
    machine = platform.machine().lower()
    supported = (
        (system == "Windows" and machine in {"amd64", "x86_64"})
        or (system == "Linux" and machine in {"amd64", "x86_64", "aarch64", "arm64"})
    )
    if not supported:
        raise OracleError("oracle runtime platform has no permitted wheel hash")
    if gmpy2.version() != GMPY2_VERSION:
        raise OracleError(f"oracle requires gmpy2 {GMPY2_VERSION}")
    if gmpy2.mpfr_version() != MPFR_VERSION:
        raise OracleError(f"oracle requires {MPFR_VERSION}")
    if gmpy2.mp_version() != GMP_VERSION:
        raise OracleError(f"oracle requires {GMP_VERSION}")
    if gmpy2.mpc_version() != MPC_VERSION:
        raise OracleError(f"oracle requires {MPC_VERSION}")


def _context(bits: int, rounding: int) -> gmpy2.context:
    return gmpy2.context(
        precision=bits,
        round=rounding,
        emax=1_000_000,
        emin=-1_000_000,
        subnormalize=False,
        trap_underflow=True,
        trap_overflow=True,
        trap_invalid=True,
        trap_erange=True,
        trap_divzero=True,
        trap_inexact=False,
        allow_complex=False,
        rational_division=False,
        allow_release_gil=False,
    )


def _rounded(bits: int, rounding: int, operation: Callable[[], gmpy2.mpfr]) -> gmpy2.mpfr:
    with _context(bits, rounding):
        return +operation()


@dataclass(frozen=True)
class Interval:
    lower: gmpy2.mpfr
    upper: gmpy2.mpfr
    bits: int

    def __post_init__(self) -> None:
        if self.bits not in PRECISIONS:
            raise OracleError("interval precision is outside the fixed ladder")
        if not (gmpy2.is_finite(self.lower) and gmpy2.is_finite(self.upper)):
            raise OracleError("interval endpoint is not finite")
        if self.lower.precision != self.bits or self.upper.precision != self.bits:
            raise OracleError("interval endpoint precision does not match its label")
        if self.lower > self.upper:
            raise OracleError("interval endpoints are inverted")

    @classmethod
    def integer(cls, value: int, bits: int) -> "Interval":
        lower = _rounded(bits, gmpy2.RoundDown, lambda: gmpy2.mpfr(value))
        upper = _rounded(bits, gmpy2.RoundUp, lambda: gmpy2.mpfr(value))
        return cls(lower, upper, bits)

    @classmethod
    def rational(cls, numerator: int, denominator: int, bits: int) -> "Interval":
        if denominator <= 0:
            raise OracleError("rational denominator must be positive")
        value = gmpy2.mpq(numerator, denominator)
        lower = _rounded(bits, gmpy2.RoundDown, lambda: gmpy2.mpfr(value))
        upper = _rounded(bits, gmpy2.RoundUp, lambda: gmpy2.mpfr(value))
        return cls(lower, upper, bits)

    def _same(self, other: "Interval") -> None:
        if self.bits != other.bits:
            raise OracleError("mixed interval precisions")

    def add(self, other: "Interval") -> "Interval":
        self._same(other)
        lower = _rounded(self.bits, gmpy2.RoundDown, lambda: self.lower + other.lower)
        upper = _rounded(self.bits, gmpy2.RoundUp, lambda: self.upper + other.upper)
        return Interval(lower, upper, self.bits)

    def subtract(self, other: "Interval") -> "Interval":
        self._same(other)
        lower = _rounded(self.bits, gmpy2.RoundDown, lambda: self.lower - other.upper)
        upper = _rounded(self.bits, gmpy2.RoundUp, lambda: self.upper - other.lower)
        return Interval(lower, upper, self.bits)

    def multiply(self, other: "Interval") -> "Interval":
        self._same(other)
        pairs = (
            (self.lower, other.lower),
            (self.lower, other.upper),
            (self.upper, other.lower),
            (self.upper, other.upper),
        )
        lower = min(
            _rounded(self.bits, gmpy2.RoundDown, lambda a=a, b=b: a * b)
            for a, b in pairs
        )
        upper = max(
            _rounded(self.bits, gmpy2.RoundUp, lambda a=a, b=b: a * b)
            for a, b in pairs
        )
        return Interval(lower, upper, self.bits)

    def divide(self, other: "Interval") -> "Interval":
        self._same(other)
        if other.lower <= 0 <= other.upper:
            raise OracleError("interval division crosses zero")
        pairs = (
            (self.lower, other.lower),
            (self.lower, other.upper),
            (self.upper, other.lower),
            (self.upper, other.upper),
        )
        lower = min(
            _rounded(self.bits, gmpy2.RoundDown, lambda a=a, b=b: a / b)
            for a, b in pairs
        )
        upper = max(
            _rounded(self.bits, gmpy2.RoundUp, lambda a=a, b=b: a / b)
            for a, b in pairs
        )
        return Interval(lower, upper, self.bits)

    def negate(self) -> "Interval":
        lower = _rounded(self.bits, gmpy2.RoundDown, lambda: -self.upper)
        upper = _rounded(self.bits, gmpy2.RoundUp, lambda: -self.lower)
        return Interval(lower, upper, self.bits)

    def log(self) -> "Interval":
        if self.lower <= 0:
            raise OracleError("log interval is not positive")
        lower = _rounded(self.bits, gmpy2.RoundDown, lambda: gmpy2.log(self.lower))
        upper = _rounded(self.bits, gmpy2.RoundUp, lambda: gmpy2.log(self.upper))
        return Interval(lower, upper, self.bits)

    def log1p(self) -> "Interval":
        if self.lower <= -1:
            raise OracleError("log1p interval is outside its domain")
        lower = _rounded(self.bits, gmpy2.RoundDown, lambda: gmpy2.log1p(self.lower))
        upper = _rounded(self.bits, gmpy2.RoundUp, lambda: gmpy2.log1p(self.upper))
        return Interval(lower, upper, self.bits)

    def exp(self) -> "Interval":
        lower = _rounded(self.bits, gmpy2.RoundDown, lambda: gmpy2.exp(self.lower))
        upper = _rounded(self.bits, gmpy2.RoundUp, lambda: gmpy2.exp(self.upper))
        return Interval(lower, upper, self.bits)

    def expm1(self) -> "Interval":
        lower = _rounded(self.bits, gmpy2.RoundDown, lambda: gmpy2.expm1(self.lower))
        upper = _rounded(self.bits, gmpy2.RoundUp, lambda: gmpy2.expm1(self.upper))
        return Interval(lower, upper, self.bits)

    def wire(self) -> dict[str, object]:
        return {
            "lower": _ratio_wire(self.lower),
            "upper": _ratio_wire(self.upper),
            "precision_bits": self.bits,
        }


@dataclass(frozen=True)
class Setup:
    domain: int
    h_integral_x1: Interval
    h_integral_domain: Interval
    proposal_range: Interval
    squeeze: Interval


def _ratio_wire(value: gmpy2.mpfr) -> dict[str, str]:
    numerator, denominator = value.as_integer_ratio()
    return {"numerator": str(numerator), "denominator": str(denominator)}


def _constants(bits: int) -> tuple[Interval, Interval, Interval]:
    q = Interval.rational(99, 100, bits)
    one = Interval.integer(1, bits)
    return q, one.subtract(q), one


def h_integral(value: Interval) -> Interval:
    _, one_minus_q, _ = _constants(value.bits)
    # Independent ideal formula: expm1((1-q) * ln(x)) / (1-q).
    return one_minus_q.multiply(value.log()).expm1().divide(one_minus_q)


def density(value: Interval) -> Interval:
    q, _, _ = _constants(value.bits)
    return q.negate().multiply(value.log()).exp()


def h_integral_inverse(value: Interval) -> Interval:
    _, one_minus_q, _ = _constants(value.bits)
    # Independent ideal formula: exp(log1p((1-q) * y) / (1-q)).
    return one_minus_q.multiply(value).log1p().divide(one_minus_q).exp()


def setup(domain: int, bits: int) -> Setup:
    if not 1 <= domain <= MAX_DOMAIN:
        raise OracleError("domain is outside the fixed boundary")
    one = Interval.integer(1, bits)
    two = Interval.integer(2, bits)
    x_one_half = Interval.rational(3, 2, bits)
    domain_half = Interval.rational(2 * domain + 1, 2, bits)
    h_integral_x1 = h_integral(x_one_half).subtract(one)
    h_integral_domain = h_integral(domain_half)
    proposal_range = h_integral_x1.subtract(h_integral_domain)
    inverse_input = h_integral(Interval.rational(5, 2, bits)).subtract(density(two))
    squeeze = two.subtract(h_integral_inverse(inverse_input))
    if not (
        h_integral_x1.upper < h_integral_domain.lower
        and proposal_range.upper < 0
        and squeeze.lower > 0
        and squeeze.upper < 1
    ):
        raise OracleUnresolved("setup inequalities are unresolved")
    return Setup(domain, h_integral_x1, h_integral_domain, proposal_range, squeeze)


def resolve_setup(domain: int) -> Setup:
    for bits in PRECISIONS:
        try:
            return setup(domain, bits)
        except OracleUnresolved:
            continue
    raise OracleUnresolved(f"setup domain={domain} unresolved at 4096 bits")


def proposal(reference: Setup, word: int) -> dict[str, object] | None:
    if not 0 <= word <= 0xFFFF_FFFF_FFFF_FFFF:
        raise OracleError("word is outside u64")
    bits = reference.proposal_range.bits
    uniform_bits = word >> 11
    uniform = Interval.rational(uniform_bits, GRID_SIZE, bits)
    z = reference.h_integral_domain.add(uniform.multiply(reference.proposal_range))
    inverse = h_integral_inverse(z)
    rounded_input = inverse.add(Interval.rational(1, 2, bits))
    preclamp_lower = int(gmpy2.floor(rounded_input.lower))
    preclamp_upper = int(gmpy2.floor(rounded_input.upper))
    preclamp_resolution = "interval_floor"
    if uniform_bits == 0:
        # H_inverse(H(N+1/2)) is exactly N+1/2 in the ideal law. Interval dependency
        # otherwise leaves this algebraic grid endpoint straddling the integer boundary.
        exact_inverse = Interval.rational(2 * reference.domain + 1, 2, bits)
        if not (
            inverse.lower <= exact_inverse.lower and inverse.upper >= exact_inverse.upper
        ):
            raise OracleError("grid-zero symbolic inverse identity is outside its interval")
        preclamp_lower = reference.domain + 1
        preclamp_upper = reference.domain + 1
        preclamp_resolution = "symbolic_grid_zero_inverse_identity"
    if preclamp_lower != preclamp_upper:
        return None
    rank_one_based = min(max(preclamp_lower, 1), reference.domain)
    clamp = "none"
    if preclamp_upper < 1:
        clamp = "lower"
    elif preclamp_lower > reference.domain:
        clamp = "upper"
    rank_interval = Interval.integer(rank_one_based, bits)
    distance = rank_interval.subtract(inverse)
    if distance.upper <= reference.squeeze.lower:
        disposition = "accept_squeeze"
        threshold = None
    elif distance.lower > reference.squeeze.upper:
        endpoint = Interval.rational(2 * rank_one_based + 1, 2, bits)
        threshold = h_integral(endpoint).subtract(density(rank_interval))
        if z.lower >= threshold.upper:
            disposition = "accept_boundary"
        elif z.upper < threshold.lower:
            disposition = "reject"
        else:
            return None
    else:
        return None
    return {
        "disposition": disposition,
        "rank": rank_one_based - 1 if disposition != "reject" else None,
        "preclamp_floor_range": [preclamp_lower, preclamp_upper],
        "preclamp_resolution": preclamp_resolution,
        "clamp": clamp,
        "uniform_grid_integer": uniform_bits,
        "z": z.wire(),
        "inverse": inverse.wire(),
        "distance": distance.wire(),
        "boundary_threshold": None if threshold is None else threshold.wire(),
        "precision_bits": bits,
    }


def resolve_proposal(domain: int, word: int) -> dict[str, object]:
    for bits in PRECISIONS:
        try:
            reference = setup(domain, bits)
        except OracleUnresolved:
            continue
        result = proposal(reference, word)
        if result is not None:
            return result
    raise OracleUnresolved(f"proposal domain={domain} word={word:016x} unresolved at 4096 bits")


def domain_set() -> tuple[int, ...]:
    domains = {1, 2, 3, 10, 1_288_490_188, MAX_DOMAIN}
    for exponent in range(1, 31):
        power = 1 << exponent
        for value in (power - 1, power, power + 1):
            if 1 <= value <= MAX_DOMAIN:
                domains.add(value)
    return tuple(sorted(domains))


def fixed_words() -> tuple[int, ...]:
    return (
        0,
        0x0000_0000_0000_07FF,
        0x8000_0000_0000_0000,
        0xFFFF_FFFF_FFFF_FFFF,
    )


def _searched_boundary_words(deadline: float) -> dict[str, int]:
    found: dict[str, int] = {}
    grid = 0
    for _ in range(MAX_SEARCH_STEPS):
        if time.monotonic() > deadline:
            raise OracleError("boundary search exceeded its wall-clock cap")
        grid = (grid + GRID_STEP) & GRID_MASK
        word = grid << 11
        result = resolve_proposal(MAX_DOMAIN, word)
        disposition = str(result["disposition"])
        found.setdefault(disposition, word)
        if {"accept_squeeze", "accept_boundary", "reject"}.issubset(found):
            return found
    raise OracleUnresolved("bounded search did not find both acceptance paths and rejection")


def _header(domains: tuple[int, ...]) -> dict[str, object]:
    return {
        "record_type": "header",
        "schema_version": SCHEMA,
        "corpus_class": "standalone_mpfr_interval_prototype",
        **EVIDENCE_BOUNDARY,
        "thresholds_approved": False,
        "python_abi": "cp313",
        "gmpy2_version": GMPY2_VERSION,
        "mpfr_version": MPFR_VERSION,
        "gmp_version": GMP_VERSION,
        "mpc_version": MPC_VERSION,
        "permitted_gmpy2_wheel_sha256": PYPI_HASHES,
        "dependency_installation_attested": False,
        "precision_ladder_bits": list(PRECISIONS),
        "rounding": "MPFR_RNDD_lower_MPFR_RNDU_upper",
        "uniform_grid_bits": GRID_BITS,
        "domain_count": len(domains),
        "metrics": {
            "head_relative_error": "not_computed",
            "cdf_ks_error": "not_computed",
            "tail_mass_error": "not_computed",
            "total_variation_interval": "not_computed",
            "rejection_mass": "not_computed",
            "retry_probability_bound": "not_computed",
        },
    }


def generate_records() -> list[dict[str, object]]:
    require_runtime()
    started = time.monotonic()
    deadline = started + MAX_GENERATION_SECONDS
    domains = domain_set()
    records: list[dict[str, object]] = [_header(domains)]
    for domain in domains:
        if time.monotonic() > deadline:
            raise OracleError("generation exceeded its wall-clock cap")
        reference = resolve_setup(domain)
        records.append(
            {
                "record_type": "setup_interval",
                "domain": domain,
                "h_integral_x1": reference.h_integral_x1.wire(),
                "h_integral_domain": reference.h_integral_domain.wire(),
                "proposal_range": reference.proposal_range.wire(),
                "squeeze": reference.squeeze.wire(),
                "precision_bits": reference.proposal_range.bits,
            }
        )
        for word in fixed_words():
            records.append(
                {
                    "record_type": "proposal_interval",
                    "source": "fixed_grid_boundary",
                    "domain": domain,
                    "word": f"{word:016x}",
                    **resolve_proposal(domain, word),
                }
            )

    searched = _searched_boundary_words(deadline)
    for disposition in ("accept_squeeze", "accept_boundary", "reject"):
        center_grid = searched[disposition] >> 11
        for delta in (-1, 0, 1):
            grid = center_grid + delta
            if not 0 <= grid < GRID_SIZE:
                continue
            word = grid << 11
            records.append(
                {
                    "record_type": "proposal_interval",
                    "source": f"searched_{disposition}_adjacent_grid",
                    "domain": MAX_DOMAIN,
                    "word": f"{word:016x}",
                    **resolve_proposal(MAX_DOMAIN, word),
                }
            )
    records.append(
        {
            "record_type": "retry_control",
            "domain": MAX_DOMAIN,
            "word": f"{searched['reject']:016x}",
            "repetitions": 64,
            "expected_ideal_observation": "rejection_limit",
            "probability_bound": "not_computed",
            "policy_approved": False,
        }
    )
    for record in records:
        record.update(EVIDENCE_BOUNDARY)
    if len(records) > MAX_RECORDS:
        raise OracleError("record cap exceeded")
    return records


def encode_records(records: Iterable[dict[str, object]]) -> bytes:
    encoded = b"".join(
        json.dumps(
            record,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        + b"\n"
        for record in records
    )
    if len(encoded) > MAX_OUTPUT_BYTES:
        raise OracleError("encoded output cap exceeded")
    return encoded


def main() -> int:
    if len(sys.argv) != 1:
        print(NOTICE, file=sys.stderr)
        print("ORACLE_INVALID this command accepts no arguments", file=sys.stderr)
        return 64
    try:
        output = encode_records(generate_records())
    except OracleUnresolved as error:
        print(NOTICE, file=sys.stderr)
        print(f"ORACLE_UNRESOLVED {error}", file=sys.stderr)
        return 2
    except (OracleError, ArithmeticError, ValueError) as error:
        print(NOTICE, file=sys.stderr)
        print(f"ORACLE_INVALID {error}", file=sys.stderr)
        return 2
    sys.stderr.write(f"{NOTICE}\n")
    sys.stdout.buffer.write(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

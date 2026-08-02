import ast
import contextlib
import hashlib
import io
import json
import pathlib
import re
import sys
import unittest
from unittest import mock

import gmpy2

import zipf_oracle as oracle


class IntervalTests(unittest.TestCase):
    def test_directed_arithmetic_contains_exact_rationals(self) -> None:
        bits = oracle.PRECISIONS[0]
        one_third = oracle.Interval.rational(1, 3, bits)
        three = oracle.Interval.integer(3, bits)
        product = one_third.multiply(three)
        self.assertLessEqual(product.lower, 1)
        self.assertGreaterEqual(product.upper, 1)

        negative = oracle.Interval.integer(-2, bits)
        four = oracle.Interval.integer(4, bits)
        multiplied = negative.multiply(four)
        self.assertEqual(multiplied.lower, -8)
        self.assertEqual(multiplied.upper, -8)

        quotient = four.divide(negative)
        self.assertEqual(quotient.lower, -2)
        self.assertEqual(quotient.upper, -2)

    def test_division_and_transcendental_domains_fail_closed(self) -> None:
        bits = oracle.PRECISIONS[0]
        minus_one = oracle.Interval.integer(-1, bits)
        one = oracle.Interval.integer(1, bits)
        crossing_zero = oracle.Interval(minus_one.lower, one.upper, bits)
        with self.assertRaisesRegex(oracle.OracleError, "crosses zero"):
            one.divide(crossing_zero)
        with self.assertRaisesRegex(oracle.OracleError, "not positive"):
            crossing_zero.log()
        with self.assertRaisesRegex(oracle.OracleError, "outside its domain"):
            minus_one.log1p()

    def test_endpoint_precision_must_match_the_interval_label(self) -> None:
        endpoint = gmpy2.mpfr(1)
        self.assertEqual(endpoint.precision, 53)
        with self.assertRaisesRegex(oracle.OracleError, "precision does not match"):
            oracle.Interval(endpoint, endpoint, oracle.PRECISIONS[0])


class OracleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.records = oracle.generate_records()
        cls.encoded = oracle.encode_records(cls.records)

    def test_runtime_and_dependency_identity_are_exact(self) -> None:
        oracle.require_runtime()
        self.assertEqual(gmpy2.version(), "2.3.1")
        self.assertEqual(gmpy2.mpfr_version(), "MPFR 4.2.2")
        self.assertEqual(gmpy2.mp_version(), "GMP 6.3.0")
        self.assertEqual(gmpy2.mpc_version(), "MPC 1.4.0")
        with mock.patch.object(oracle.gmpy2, "version", return_value="2.3.0"):
            with self.assertRaisesRegex(oracle.OracleError, "requires gmpy2 2.3.1"):
                oracle.require_runtime()
        requirements = pathlib.Path(oracle.__file__).with_name("requirements.txt").read_text(
            encoding="ascii"
        )
        self.assertEqual(
            set(re.findall(r"--hash=sha256:([0-9a-f]{64})", requirements)),
            set(oracle.PYPI_HASHES.values()),
        )

    def test_ambient_mpfr_context_cannot_change_an_observation(self) -> None:
        expected = oracle.resolve_proposal(10, 0x8000_0000_0000_0000)
        hostile = gmpy2.context(
            gmpy2.get_context(),
            precision=80,
            round=gmpy2.RoundToZero,
            emax=100,
            emin=-100,
            allow_complex=True,
        )
        with hostile:
            actual = oracle.resolve_proposal(10, 0x8000_0000_0000_0000)
        self.assertEqual(actual, expected)

    def test_header_is_explicitly_ineligible_and_has_no_thresholds(self) -> None:
        header = self.records[0]
        self.assertEqual(header["notice"], oracle.NOTICE)
        self.assertFalse(header["qualification_eligible"])
        self.assertFalse(header["validation_authorizes_execution"])
        self.assertFalse(header["independently_reviewed"])
        self.assertFalse(header["candidate_results_present"])
        self.assertFalse(header["candidate_inputs_imported"])
        self.assertFalse(header["thresholds_approved"])
        self.assertTrue(all(value == "not_computed" for value in header["metrics"].values()))
        for record in self.records:
            for field, value in oracle.EVIDENCE_BOUNDARY.items():
                self.assertEqual(record[field], value)

    def test_domain_and_grid_coverage_is_bounded_and_deterministic(self) -> None:
        domains = oracle.domain_set()
        self.assertEqual(len(domains), 92)
        self.assertEqual(domains[0:4], (1, 2, 3, 4))
        self.assertIn(1_288_490_188, domains)
        self.assertEqual(domains[-1], oracle.MAX_DOMAIN)
        self.assertLessEqual(len(self.records), oracle.MAX_RECORDS)
        self.assertLessEqual(len(self.encoded), oracle.MAX_OUTPUT_BYTES)
        self.assertEqual(len(self.records), 471)
        self.assertEqual(len(self.encoded), 865_397)
        self.assertEqual(
            hashlib.sha256(self.encoded).hexdigest(),
            "8ad14317bdb1f12d67b9f823bea0759d33034e4c01164c2dbac90ad870f2474b",
        )

    def test_low_eleven_word_bits_do_not_change_the_ideal_grid_point(self) -> None:
        lower = oracle.resolve_proposal(10, 0)
        lower_with_ignored_bits = oracle.resolve_proposal(10, 0x7FF)
        self.assertEqual(lower, lower_with_ignored_bits)
        self.assertEqual(lower["clamp"], "upper")

    def test_search_covers_both_acceptance_paths_rejection_and_cap_control(self) -> None:
        dispositions = {
            record["disposition"]
            for record in self.records
            if record["record_type"] == "proposal_interval"
        }
        self.assertEqual(dispositions, {"accept_squeeze", "accept_boundary", "reject"})
        controls = [record for record in self.records if record["record_type"] == "retry_control"]
        self.assertEqual(len(controls), 1)
        self.assertEqual(controls[0]["repetitions"], 64)
        self.assertEqual(controls[0]["probability_bound"], "not_computed")
        self.assertFalse(controls[0]["policy_approved"])

    def test_precision_exhaustion_and_output_cap_fail_closed(self) -> None:
        with mock.patch.object(oracle, "proposal", return_value=None):
            with self.assertRaises(oracle.OracleUnresolved):
                oracle.resolve_proposal(10, 0)
        oversized = [{"payload": "x" * oracle.MAX_OUTPUT_BYTES}]
        with self.assertRaisesRegex(oracle.OracleError, "output cap"):
            oracle.encode_records(oversized)
        with self.assertRaises(ValueError):
            oracle.encode_records([{"not_a_number": float("nan")}])

    def test_setup_escalation_clamp_ambiguity_and_symbolic_identity_fail_closed(self) -> None:
        valid = oracle.setup(10, oracle.PRECISIONS[0])
        with mock.patch.object(
            oracle,
            "setup",
            side_effect=[oracle.OracleUnresolved("first precision"), valid],
        ):
            self.assertIs(oracle.resolve_setup(10), valid)

        valid_at_512 = oracle.setup(10, oracle.PRECISIONS[1])
        with mock.patch.object(
            oracle,
            "setup",
            side_effect=[oracle.OracleUnresolved("first precision"), valid_at_512],
        ) as setup_mock:
            resolved = oracle.resolve_proposal(10, 0x8000_0000_0000_0000)
        self.assertIn(resolved["disposition"], {"accept_squeeze", "accept_boundary", "reject"})
        self.assertEqual(
            [call.args[1] for call in setup_mock.call_args_list],
            [oracle.PRECISIONS[0], oracle.PRECISIONS[1]],
        )

        bits = oracle.PRECISIONS[0]
        zero = oracle.Interval.integer(0, bits)
        two = oracle.Interval.integer(2, bits)
        ambiguous_inverse = oracle.Interval(zero.lower, two.upper, bits)
        with mock.patch.object(oracle, "h_integral_inverse", return_value=ambiguous_inverse):
            self.assertIsNone(oracle.proposal(valid, 0x8000_0000_0000_0000))

        shifted = valid.h_integral_domain.add(oracle.Interval.integer(1, bits))
        corrupted = oracle.Setup(
            valid.domain,
            valid.h_integral_x1,
            shifted,
            valid.proposal_range,
            valid.squeeze,
        )
        with self.assertRaisesRegex(oracle.OracleError, "symbolic inverse identity"):
            oracle.proposal(corrupted, 0)

    def test_cli_rejects_arguments_and_distinguishes_unresolved(self) -> None:
        stderr = io.StringIO()
        with mock.patch.object(sys, "argv", ["zipf_oracle.py", "unexpected"]):
            with contextlib.redirect_stderr(stderr):
                self.assertEqual(oracle.main(), 64)
        self.assertIn("ORACLE_INVALID this command accepts no arguments", stderr.getvalue())

        stderr = io.StringIO()
        with mock.patch.object(sys, "argv", ["zipf_oracle.py"]):
            with mock.patch.object(
                oracle, "generate_records", side_effect=oracle.OracleUnresolved("boundary")
            ):
                with contextlib.redirect_stderr(stderr):
                    self.assertEqual(oracle.main(), 2)
        self.assertIn("ORACLE_UNRESOLVED boundary", stderr.getvalue())

    def test_ndjson_is_canonical_and_has_one_final_newline(self) -> None:
        self.assertTrue(self.encoded.endswith(b"\n"))
        self.assertFalse(self.encoded.endswith(b"\n\n"))
        lines = self.encoded.splitlines()
        self.assertEqual(len(lines), len(self.records))
        for line, record in zip(lines, self.records, strict=True):
            self.assertEqual(json.loads(line), record)
            self.assertEqual(
                line,
                json.dumps(
                    record,
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("ascii"),
            )

    def test_oracle_source_has_no_file_import_path(self) -> None:
        source_path = pathlib.Path(oracle.__file__)
        source = source_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imported_modules = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        }
        imported_modules.update(
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        )
        self.assertTrue(imported_modules.isdisjoint({"subprocess", "pathlib", "os"}))
        called_names = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        self.assertNotIn("open", called_names)
        called_attributes = {
            node.func.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        }
        self.assertTrue(called_attributes.isdisjoint({"open", "read_bytes", "read_text"}))


if __name__ == "__main__":
    unittest.main()

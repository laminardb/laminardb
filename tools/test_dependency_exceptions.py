"""Exercise the fail-closed boundary of the temporary advisory policy."""

from datetime import date
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from check_dependency_exceptions import validate


class DependencyExceptions(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        source = Path(__file__).resolve().parent.parent
        for name in ("security", ".cargo", "vendor/quick-xml"):
            shutil.copytree(source / name, self.root / name, ignore=shutil.ignore_patterns("target"))
        for name in ("Cargo.toml", "Cargo.lock", "deny.toml"):
            shutil.copy2(source / name, self.root / name)

    def check(self):
        validate(self.root, date(2026, 9, 21))

    def test_current_and_expiration_boundary(self):
        self.check()
        validate(self.root, date(2026, 10, 20))
        with self.assertRaisesRegex(ValueError, "expired"):
            validate(self.root, date(2026, 10, 21))

    def test_scanner_disagreement(self):
        for config in (".cargo/audit.toml", "deny.toml"):
            with self.subTest(config=config):
                path = self.root / config
                original = path.read_text()
                path.write_text(original.replace('    "RUSTSEC-2023-0071",\n', ""))
                with self.assertRaisesRegex(ValueError, "approved set"):
                    self.check()
                path.write_text(original)

    def test_version_source_checksum_and_duplicate_drift(self):
        path = self.root / "Cargo.lock"
        original = path.read_text()
        start = original.index('name = "rsa"')
        end = original.index("[[package]]", start)
        section = original[start:end]
        for old, new in (
            ('version = "0.9.10"', 'version = "0.9.11"'),
            ("registry+https://github.com/rust-lang/crates.io-index", "git+https://example.invalid/rsa"),
            ('checksum = "', 'checksum = "00'),
        ):
            with self.subTest(change=old):
                path.write_text(original[:start] + section.replace(old, new) + original[end:])
                with self.assertRaisesRegex(ValueError, "drifted"):
                    self.check()
        path.write_text(original + '\n[[package]]\nname = "rsa"\nversion = "0.9.11"\n')
        with self.assertRaisesRegex(ValueError, "drifted"):
            self.check()

    def test_backport_replacement_and_source_tampering(self):
        manifest = self.root / "Cargo.toml"
        original = manifest.read_text()
        manifest.write_text(original.replace('path = "vendor/quick-xml"', 'version = "0.39.4"'))
        with self.assertRaisesRegex(ValueError, "local backport"):
            self.check()
        manifest.write_text(original)
        source = self.root / "vendor/quick-xml/src/name.rs"
        source.write_bytes(source.read_bytes() + b"\n// unreviewed change\n")
        with self.assertRaisesRegex(ValueError, "reviewed digest"):
            self.check()

    def test_new_build_script_is_not_outside_source_pin(self):
        (self.root / "vendor/quick-xml/build.rs").write_text("fn main() {}")
        with self.assertRaisesRegex(ValueError, "reviewed digest"):
            self.check()

    def test_missing_owner(self):
        path = self.root / "security/dependency-exceptions.json"
        policy = json.loads(path.read_text())
        policy["owner"] = ""
        path.write_text(json.dumps(policy))
        with self.assertRaisesRegex(ValueError, "owner"):
            self.check()

    def test_publication_requires_explicit_xml_risk_acceptance(self):
        self.check()
        validate(self.root, date(2026, 10, 20), publication=True)
        with self.assertRaisesRegex(ValueError, "expired"):
            validate(self.root, date(2026, 10, 21), publication=True)
        path = self.root / "security/dependency-exceptions.json"
        policy = json.loads(path.read_text())
        for acceptance in ({}, {"reason": "accepted", "advisories": ["RUSTSEC-2026-0194"]}):
            policy["publication"] = acceptance
            path.write_text(json.dumps(policy))
            with self.assertRaisesRegex(ValueError, "publication is blocked"):
                validate(self.root, date(2026, 9, 21), publication=True)


if __name__ == "__main__":
    unittest.main()

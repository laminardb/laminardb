#!/usr/bin/env python3
"""Fail closed on expired exceptions, scanner disagreement or dependency/source drift."""

import argparse
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
import sys
import tomllib


def read_toml(path):
    return tomllib.loads(path.read_text(encoding="utf-8"))


def source_digest(directory):
    digest = hashlib.sha256()
    for path in sorted(directory.rglob("*")):
        relative = path.relative_to(directory).as_posix()
        # Cargo.lock controls standalone upstream tests, not the workspace build.
        if relative == "Cargo.lock" or "target" in path.relative_to(directory).parts:
            continue
        if path.is_symlink():
            raise ValueError(f"backport contains a symbolic link: {relative}")
        if path.is_file():
            digest.update(relative.encode("utf-8") + b"\0")
            # Git may use CRLF on Windows; hash the same source on either platform.
            digest.update(path.read_bytes().replace(b"\r\n", b"\n") + b"\0")
    return digest.hexdigest()


def validate(root, today, publication=False):
    policy = json.loads((root / "security/dependency-exceptions.json").read_text())
    if not policy["owner"] or not date.fromisoformat(policy["approved"]) <= today:
        raise ValueError("dependency exceptions lack a current approval/owner")
    if today >= date.fromisoformat(policy["expires"]):
        raise ValueError(f"dependency exceptions expired on {policy['expires']}")

    exceptions = policy["exceptions"]
    expected = sorted(entry["advisory"] for entry in exceptions)
    if len(set(expected)) != len(expected) or not all(e["reason"] for e in exceptions):
        raise ValueError("duplicate or unexplained dependency exception")
    for config in (".cargo/audit.toml", "deny.toml"):
        actual = read_toml(root / config)["advisories"].get("ignore", [])
        if sorted(actual) != expected:
            raise ValueError(f"{config}: advisory exceptions differ from the approved set")

    names = {entry["crate"] for entry in exceptions}
    packages = policy["packages"]
    if {package["name"] for package in packages} != names:
        raise ValueError("every excepted crate must have pinned packages")
    identities = {(p["name"], p["version"]) for p in packages}
    if any((e["crate"], e["version"]) not in identities for e in exceptions):
        raise ValueError("exception version is not pinned")
    locked = read_toml(root / "Cargo.lock")["package"]
    keys = ("name", "version", "source", "checksum")
    actual = [{key: p[key] for key in keys if key in p} for p in locked if p["name"] in names]
    order = lambda p: (p["name"], p["version"])
    if sorted(actual, key=order) != sorted(packages, key=order):
        raise ValueError("excepted dependency version, source or checksum drifted")

    backport = policy["backport"]
    patch = read_toml(root / "Cargo.toml")["patch"]["crates-io"]["quick-xml"]
    if patch != {"path": backport["path"]}:
        raise ValueError("quick-xml must resolve to the reviewed local backport")
    directory = root / backport["path"]
    if directory.resolve() != (root / "vendor/quick-xml").resolve():
        raise ValueError("unexpected quick-xml backport location")
    if directory.is_symlink() or source_digest(directory) != backport["sha256"]:
        raise ValueError("quick-xml backport source differs from its reviewed digest")
    if publication:
        accepted = policy.get("publication", {})
        xml_advisories = sorted(e["advisory"] for e in exceptions if e["crate"] == "quick-xml")
        if not accepted.get("reason") or sorted(accepted.get("advisories", [])) != xml_advisories:
            raise ValueError(
                "crate publication is blocked: Cargo drops the quick-xml workspace patch; "
                "an explicit, current exception for the unpatched XML advisories is required"
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publication", action="store_true", help="also check registry publication safety")
    args = parser.parse_args()
    try:
        validate(Path(__file__).resolve().parent.parent, datetime.now(timezone.utc).date(), args.publication)
    except (ValueError, KeyError, OSError, TypeError) as error:
        sys.exit(f"Dependency exception check failed: {error}")
    print("Dependency exceptions: approval, expiry, scanner lists and pinned sources verified")
    if args.publication:
        print("Registry publication allowed under the temporary unpatched XML risk exception")

#!/usr/bin/env python3
import json
import re
import subprocess
import sys


EXPECTED_GENERATIONS = (
    ("Arrow", r"^arrow(?:$|-)", ("58.4.0",)),
    ("Parquet", r"^parquet$", ("58.4.0",)),
    ("DataFusion", r"^datafusion(?:$|-)", ("53.1.0",)),
    ("object_store", r"^object_store$", ("0.13.2",)),
    (
        "Delta Lake",
        r"^deltalake(?:$|-)",
        ("0.15.0", "0.16.0", "0.16.1", "0.32.4", "1.0.0"),
    ),
    ("Delta kernel", r"^buoyant_kernel$", ("0.22.2",)),
    ("Iceberg", r"^iceberg(?:$|-)", ("0.10.1",)),
    ("OpenDAL", r"^opendal(?:$|-)", ("0.57.0",)),
    ("SQLParser", r"^sqlparser$", ("0.61.0",)),
)


def load_packages() -> list[dict[str, object]]:
    command = [
        "cargo",
        "metadata",
        "--locked",
        "--format-version",
        "1",
        "--all-features",
    ]
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)
    return json.loads(result.stdout)["packages"]


def main() -> int:
    packages = load_packages()
    failed = False

    git_sources = sorted(
        {
            str(package["source"])
            for package in packages
            if str(package["source"]).startswith("git+")
        }
    )
    if git_sources:
        print(f"Git dependencies are not publishable: {git_sources}", file=sys.stderr)
        failed = True
    else:
        print("Git dependencies: none")

    for family, pattern, expected in EXPECTED_GENERATIONS:
        matching_packages = [
            package
            for package in packages
            if re.match(pattern, str(package["name"]))
        ]
        versions = sorted(
            {
                str(package["version"])
                for package in matching_packages
            }
        )
        unexpected_sources = sorted(
            {
                str(package["source"])
                for package in matching_packages
                if not str(package["source"]).startswith("registry+")
            }
        )
        generation_matches = tuple(versions) == expected
        if not generation_matches:
            print(
                f"{family}: found {versions or ['<absent>']}, expected {list(expected)}",
                file=sys.stderr,
            )
            failed = True
        if unexpected_sources:
            print(
                f"{family}: non-registry sources are not publishable: {unexpected_sources}",
                file=sys.stderr,
            )
            failed = True
        if generation_matches and not unexpected_sources:
            print(f"{family}: {', '.join(versions)}")

    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())

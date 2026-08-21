#!/usr/bin/env python3
import json
import re
import subprocess
import sys


EXPECTED_GENERATIONS = (
    ("Arrow", r"^arrow(?:$|-)", ("58.4.0",)),
    ("Parquet", r"^parquet$", ("58.4.0",)),
    ("DataFusion", r"^datafusion(?:$|-)", ("54.1.0",)),
    ("object_store", r"^object_store$", ("0.13.2",)),
    ("Delta Lake", r"^deltalake(?:$|-)", ("1.0.0",)),
    ("Delta kernel", r"^buoyant_kernel$", ("0.24.0",)),
    ("Iceberg", r"^iceberg(?:$|-)", ("0.10.1",)),
    ("OpenDAL", r"^opendal(?:$|-)", ("0.57.0",)),
    # delta-rs owns 0.61 for Delta predicates; DataFusion and LaminarDB use 0.62.
    ("SQLParser", r"^sqlparser$", ("0.61.0", "0.62.0")),
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

    for family, pattern, expected in EXPECTED_GENERATIONS:
        versions = sorted(
            {
                str(package["version"])
                for package in packages
                if re.match(pattern, str(package["name"]))
            }
        )
        if tuple(versions) != expected:
            print(
                f"{family}: found {versions or ['<absent>']}, expected {list(expected)}",
                file=sys.stderr,
            )
            failed = True
        else:
            print(f"{family}: {', '.join(versions)}")

    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())

"""Start a bound worker with only declared imports and runtime-owned stdlib paths."""

import json
from pathlib import Path
import runpy
import sys

runtime_root = Path(sys.argv[1]).resolve(strict=True)


def within_runtime(path: str) -> bool:
    resolved = Path(path).resolve()
    # File identity handles Windows canonical paths with and without the \\?\ prefix.
    return any(parent.is_dir() and parent.samefile(runtime_root)
               for parent in (resolved, *resolved.parents))


if not within_runtime(sys.executable):
    raise ValueError("Python executable is outside the runtime root")
if any(not within_runtime(path) for path in sys.path):
    raise ValueError("Python standard-library path is outside the runtime root")
sys.path[:0] = json.loads(sys.argv[2])
sys.argv = sys.argv[3:]
runpy.run_module("laminardb_process.worker", run_name="__main__", alter_sys=True)

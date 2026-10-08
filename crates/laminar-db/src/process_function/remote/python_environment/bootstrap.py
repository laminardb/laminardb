"""Start a bound worker with only declared imports and runtime-owned stdlib paths."""

import sys

if sys.pycache_prefix is None or not sys.dont_write_bytecode:
    raise ValueError("bound Python requires disabled source bytecode caches")

import json
from pathlib import Path
import runpy

runtime_root = Path(sys.argv[1]).resolve(strict=True)

# Replay launches have a cleared environment and a fixed interpreter hash seed.
if not sys.flags.isolated:
    import os
    status = Path("/proc/self/status").read_text().splitlines()
    if (sys.platform != "linux" or os.geteuid() == 0 or sys.flags.hash_randomization
            or "NoNewPrivs:\t1" not in status
            or "CapEff:\t0000000000000000" not in status):
        raise ValueError("replay-bound Python requires an unprivileged Linux process with no_new_privs")


def within_runtime(path: str) -> bool:
    resolved = Path(path).resolve()
    # File identity handles Windows canonical paths with and without the \\?\ prefix.
    return any(parent.is_dir() and parent.samefile(runtime_root)
               for parent in (resolved, *resolved.parents))


if not within_runtime(sys.executable):
    raise ValueError("Python executable is outside the runtime root")
if not Path(sys.pycache_prefix).samefile(sys.executable):
    raise ValueError("Python bytecode cache prefix must be the interpreter file")
if any(not within_runtime(path) for path in sys.path):
    raise ValueError("Python standard-library path is outside the runtime root")
sys.path[:0] = json.loads(sys.argv[2])
sys.argv = sys.argv[3:]
runpy.run_module("laminardb_process.worker", run_name="__main__", alter_sys=True)

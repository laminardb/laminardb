"""Run the existing process-soak target and retain a unique, hash-indexed evidence bundle.

This records single-node Kafka ALO projection evidence. It never certifies S12, another
composition, or a release. Supply numerical limits in the spec for per-run SLO checks.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import signal
import subprocess
import tarfile
import time


ROOT = Path(__file__).resolve().parents[1]
TEST = "workload_qualification::single_node_kafka_workload"


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def capture(args):
    return subprocess.check_output(args, cwd=ROOT, text=True, encoding="utf-8").strip()


def source_identity():
    names = capture(["git", "ls-files", "-co", "--exclude-standard"]).splitlines()
    names = sorted(set(name for name in names if (
        name in ("Cargo.toml", "Cargo.lock", "tools/run_workload_qualification.py")
        or name.startswith(("crates/", ".cargo/", "tests/qualification/", "vendor/"))
    ) and (ROOT / name).is_file()))
    return {name: digest(ROOT / name) for name in names}


def execute(args, log_path, timeout, env):
    started = time.time()
    with log_path.open("w", encoding="utf-8") as log:
        options = {"start_new_session": True} if os.name != "nt" else {
            "creationflags": subprocess.CREATE_NEW_PROCESS_GROUP
        }
        process = subprocess.Popen(args, cwd=ROOT, env=env, stdout=log,
                                   stderr=subprocess.STDOUT, **options)
        try:
            code = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            # Only the process group/tree created above belongs to this run.
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                               capture_output=True, timeout=30, check=False)
            else:
                os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=30)
            code = 124
    return {"args": args, "exit_code": code, "started_unix": started,
            "elapsed_seconds": time.time() - started, "log": log_path.name}


def build(output, features, offline, env):
    args = ["cargo", "test", "--profile", "release", "-p", "laminar-server",
            "--test", "cluster_soak", "--no-default-features", "--features", features,
            "--locked", "-j1", "--no-run", "--message-format=json"]
    if offline:
        args.append("--offline")
    print("Building release server and process harness", flush=True)
    result = execute(args, output / "build.log", 7200, env)
    write_json(output / "build.json", result)
    if result["exit_code"]:
        raise RuntimeError(f"build failed; see {output / 'build.log'}")
    artifacts = {}
    for line in (output / "build.log").read_text(encoding="utf-8").splitlines():
        if not line.startswith("{"):
            continue
        item = json.loads(line)
        if item.get("reason") != "compiler-artifact" or not item.get("executable"):
            continue
        name = item["target"]["name"]
        if name in ("laminardb", "cluster_soak"):
            original = Path(item["executable"])
            destination = output / (name + original.suffix)
            shutil.copy2(original, destination)
            artifacts[name] = {"path": str(destination), "sha256": digest(destination)}
    if set(artifacts) != {"laminardb", "cluster_soak"}:
        raise RuntimeError("build did not produce both the server and the process test")
    write_json(output / "executables.json", artifacts)
    return artifacts


def run(args, output):
    spec = json.loads(args.spec.read_text(encoding="utf-8"))
    write_json(output / "requested-spec.json", spec)
    identity = source_identity()
    write_json(output / "source-files.json", identity)
    with tarfile.open(output / "source.tar.gz", "w:gz") as archive:
        for name in identity:
            archive.add(ROOT / name, arcname=name, recursive=False)
    (output / "working-tree.patch").write_bytes(subprocess.check_output(
        ["git", "-c", "core.safecrlf=false", "diff", "--binary"], cwd=ROOT))
    write_json(output / "identity.json", {
        "head": capture(["git", "rev-parse", "HEAD"]), "platform": platform.platform(),
        "machine": platform.machine(), "logical_cpus": os.cpu_count(),
        "features": args.features, "default_features": False, "profile": "release",
        "rustc": capture(["rustc", "-Vv"]), "cargo": capture(["cargo", "-V"]),
        "lockfile_sha256": digest(ROOT / "Cargo.lock"), "clock": "one monotonic observer process",
        "build_environment": {key: value for key, value in os.environ.items()
                              if key.startswith("CARGO_PROFILE_RELEASE_") or key in (
                                  "RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_TARGET_DIR", "RUSTUP_TOOLCHAIN")},
        "s12_qualified": False,
    })
    for path in (Path("/proc/cpuinfo"), Path("/proc/meminfo")):
        if path.exists():
            shutil.copyfile(path, output / path.name)
    env = os.environ.copy()
    env.update(CARGO_BUILD_JOBS="1", RUST_MIN_STACK="8388608")
    artifacts = build(output, args.features, args.offline, env)
    if source_identity() != identity:
        raise RuntimeError("source changed during the build; evidence is invalid")
    env["LAMINAR_SOAK_LAMINARDB_EXE"] = artifacts["laminardb"]["path"]
    env["LAMINAR_SOAK_LAMINARDB_SHA256"] = artifacts["laminardb"]["sha256"]
    env["LAMINAR_QUALIFICATION_SPEC"] = str(output / "requested-spec.json")
    outcomes = []
    for repetition in range(1, args.repetitions + 1):
        label = f"run-{repetition:02}"
        env["LAMINAR_QUALIFICATION_OUTPUT"] = str(output / label)
        command = [artifacts["cluster_soak"]["path"], TEST, "--ignored", "--exact",
                   "--nocapture", "--test-threads=1", "--color", "never"]
        print(f"{label}: starting {spec['seconds']}s workload", flush=True)
        outcome = execute(command, output / f"{label}.log",
                          spec["seconds"] + spec["drain_seconds"] + 180, env)
        outcomes.append(outcome)
        write_json(output / "runs.json", outcomes)
        if outcome["exit_code"] or "test result: ok. 1 passed; 0 failed;" not in (
            output / f"{label}.log"
        ).read_text(encoding="utf-8"):
            raise RuntimeError(f"{label} failed; see its log and retained evidence")
        if source_identity() != identity:
            raise RuntimeError("source changed during the workload; evidence is invalid")
        print(f"{label}: completed; scoped evidence only", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--features", default="cluster,kafka")
    parser.add_argument("--repetitions", type=int, choices=range(1, 4), default=1)
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    if not os.environ.get("LAMINAR_SOAK_KAFKA_SOURCE_BROKERS"):
        parser.error("set LAMINAR_SOAK_KAFKA_SOURCE_BROKERS to a real test broker")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    try:
        run(args, output)
        write_json(output / "outcome.json", {"status": "runs_passed", "s12_qualified": False})
    except Exception as error:
        write_json(output / "outcome.json", {"status": "failed", "error": str(error), "s12_qualified": False})
        raise
    finally:
        # Append-only runs and the final digest index detect later alteration; this is not a signature.
        write_json(output / "sha256.json", {
            str(path.relative_to(output)): digest(path)
            for path in sorted(output.rglob("*")) if path.is_file() and path.name != "sha256.json"
        })


if __name__ == "__main__":
    main()

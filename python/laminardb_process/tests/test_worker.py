"""Local validation of manifest and IPC boundaries without a database process."""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
import os
import py_compile
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import grpc
import pyarrow as pa

from laminardb_process import Manifest
from laminardb_process import process_worker_pb2 as wire
from laminardb_process.worker import ProtocolError, _decode_batch, _encode_batch, _load_bound_module, serve


def manifest_bytes() -> bytes:
    def field(name: str, type_name: str) -> dict:
        return {"name": name, "nullable": False, "data_type": {"type": type_name}}

    return json.dumps({
        "version": 1,
        "protocol_version": 1,
        "runtime": "remote_python",
        "function_id": "unit_test",
        "pipeline_state_id": "unit_test_pipeline",
        "implementation_digest": "a" * 64,
        "partitioning_abi": 2,
        "state_codec_version": 2,
        "late_event_policy": "reject",
        "input_changelog": "append_only",
        "output_changelog": "append_only",
        "value_state_name": "value",
        "determinism": "undeclared",
        "key_columns": ["key"],
        "event_time_column": "ts",
        "output_event_time_column": "ts",
        "input_schema": [field("key", "utf8"), field("ts", "timestamp_microsecond_utc")],
        "output_schema": [field("key", "utf8"), field("ts", "timestamp_microsecond_utc")],
        "timer_names": ["flush"],
        "limits": {"max_keys": 2, "max_timers": 1, "max_state_bytes": 1024,
                   "max_batch_rows": 2, "max_input_rows": 2, "max_input_bytes": 1024,
                   "max_output_rows": 2, "max_output_bytes": 1024,
                   "max_timer_callbacks_per_step": 2},
    }).encode()


class WorkerBoundaryTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "requires POSIX SIGTERM")
    def test_sigterm_drains_an_active_call_and_rejects_new_calls(self) -> None:
        with tempfile.TemporaryDirectory() as directory, ThreadPoolExecutor(max_workers=1) as reader:
            handler = Path(directory) / "handler.py"
            handler.write_text(
                "import sys\nfrom laminardb_process import ActivationResult\n"
                "def handle(activations):\n"
                "    print('ENTERED', flush=True)\n"
                "    if sys.stdin.buffer.read(1) != b'x':\n"
                "        raise RuntimeError('missing release')\n"
                "    return tuple(ActivationResult(a.id) for a in activations)\n",
                encoding="utf-8",
            )
            data = json.loads(manifest_bytes())
            data["implementation_digest"] = sha256(handler.read_bytes()).hexdigest()
            manifest_file = Path(directory) / "manifest.json"
            manifest_file.write_text(json.dumps(data), encoding="utf-8")
            manifest = Manifest.from_bytes(manifest_file.read_bytes())
            process = subprocess.Popen(
                [sys.executable, "-m", "laminardb_process.worker", "--manifest", str(manifest_file),
                 "--handler", "handler:handle", "--handler-file", str(handler), "--max-in-flight", "1"],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0,
            )
            try:
                ready = reader.submit(process.stdout.readline).result(timeout=5)
                self.assertTrue(ready.startswith(b"READY "), ready)
                with grpc.insecure_channel(f"127.0.0.1:{int(ready.split()[1])}") as channel:
                    grpc.channel_ready_future(channel).result(timeout=5)
                    exchange = channel.stream_stream(
                        "/laminar.process.v1.ProcessWorker/Exchange",
                        request_serializer=wire.HostFrame.SerializeToString,
                        response_deserializer=wire.WorkerFrame.FromString,
                    )
                    frames = [
                        wire.HostFrame(open=wire.Open(
                            protocol_version=1, descriptor_sha256=manifest.digest, operator_id="test",
                            vnode_count=1, batch_id=b"a" * 16, attempt_id=b"b" * 16,
                            deadline_unix_ms=time.time_ns() // 1_000_000 + 10_000,
                        )),
                        wire.HostFrame(activation=wire.Activation(
                            id=1, canonical_key=b"key", key_text="key", event_time_us=1,
                            timer_name="flush", absent=True,
                        )),
                        wire.HostFrame(end=wire.End(count=1)),
                    ]
                    call = exchange(iter(frames), timeout=10)
                    self.assertEqual(reader.submit(process.stdout.readline).result(timeout=5), b"ENTERED\n")
                    process.send_signal(signal.SIGTERM)
                    for _ in range(20):
                        try:
                            list(exchange(iter(frames), timeout=0.1))
                            self.fail("worker admitted a call while draining")
                        except grpc.RpcError as error:
                            if error.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.CANCELLED):
                                break
                            self.assertEqual(error.code(), grpc.StatusCode.RESOURCE_EXHAUSTED)
                        time.sleep(0.05)
                    else:
                        self.fail("worker did not stop admission")
                    process.stdin.write(b"x")
                    response = list(call)
                    self.assertEqual([frame.WhichOneof("kind") for frame in response], ["ack", "result", "complete"])
                    self.assertEqual(response[-1].complete.result_count, 1)
                self.assertEqual(process.wait(timeout=5), 0)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
                process.stdin.close()
                process.stdout.close()

    def test_worker_rejects_handler_from_another_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "manifest.json"
            manifest.write_bytes(manifest_bytes())
            result = subprocess.run(
                [sys.executable, "-m", "laminardb_process.worker", "--manifest", str(manifest),
                 "--handler", "laminardb_process.worker:main", "--handler-file", __file__],
                capture_output=True, text=True, timeout=5, check=False,
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("handler module differs", result.stderr)

    def test_worker_rejects_changed_handler_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "manifest.json"
            manifest.write_bytes(manifest_bytes())
            handler = Path(directory) / "handler.py"
            handler.write_text("def handle(_):\n    return ()\n", encoding="utf-8")
            result = subprocess.run(
                [sys.executable, "-m", "laminardb_process.worker", "--manifest", str(manifest),
                 "--handler", "handler:handle", "--handler-file", str(handler)],
                capture_output=True, text=True, timeout=5, check=False,
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("handler file digest differs", result.stderr)

    def test_bound_handler_executes_verified_source_even_with_stale_bytecode(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            handler = Path(directory) / "bound_handler_fixture.py"
            handler.write_bytes(b"VALUE = 1\n")
            timestamp = handler.stat()
            py_compile.compile(str(handler), doraise=True)
            source = b"VALUE = 2\n"
            handler.write_bytes(source)
            os.utime(handler, ns=(timestamp.st_atime_ns, timestamp.st_mtime_ns))
            try:
                module = _load_bound_module(handler.stem, handler, sha256(source).digest())
                self.assertEqual(module.VALUE, 2)
            finally:
                sys.modules.pop(handler.stem, None)

    def test_manifest_rejects_wrong_runtime_and_duplicate_timer(self) -> None:
        raw = manifest_bytes()
        manifest = Manifest.from_bytes(raw)
        self.assertEqual(len(manifest.digest), 32)
        self.assertEqual(Manifest.from_bytes(raw + b"\n").digest, manifest.digest)
        self.assertEqual(Manifest.from_bytes(raw + b"\r\n").digest, manifest.digest)
        invalid = json.loads(raw)
        invalid["runtime"] = "trusted_native_rust"
        with self.assertRaises(ValueError):
            Manifest.from_bytes(json.dumps(invalid).encode())
        invalid["runtime"] = "remote_python"
        invalid["timer_names"] = ["flush", "flush"]
        with self.assertRaises(ValueError):
            Manifest.from_bytes(json.dumps(invalid).encode())
        invalid = json.loads(raw)
        invalid["unexpected"] = True
        with self.assertRaises(ValueError):
            Manifest.from_bytes(json.dumps(invalid).encode())

    def test_plaintext_worker_rejects_nonloopback_bind(self) -> None:
        manifest = Manifest.from_bytes(manifest_bytes())
        with self.assertRaises(ValueError):
            serve(manifest, lambda _: (), "0.0.0.0:0")

    def test_replay_contract_requires_environment_and_changes_binding(self) -> None:
        data = json.loads(manifest_bytes())
        data["determinism"] = "replay_safe"
        with self.assertRaises(ValueError):
            Manifest.from_bytes(json.dumps(data).encode())
        data["python_environment"] = {
            "version": 1, "executable": "bin/python3.13", "handler": "handler:handle",
            "runtime_sha256": "b" * 64, "import_roots_sha256": ["c" * 64],
        }
        replay = Manifest.from_bytes(json.dumps(data).encode())
        self.assertTrue(replay.replay_safe)
        data["determinism"] = "undeclared"
        self.assertNotEqual(replay.digest, Manifest.from_bytes(json.dumps(data).encode()).digest)
        data["determinism"] = "unknown"
        with self.assertRaises(ValueError):
            Manifest.from_bytes(json.dumps(data).encode())

    def test_manifest_binds_optional_python_environment_and_rejects_invalid_fields(self) -> None:
        original = json.loads(manifest_bytes())
        original["python_environment"] = {
            "version": 1, "executable": "python.exe", "handler": "handler:handle", "runtime_sha256": "b" * 64,
            "import_roots_sha256": ["c" * 64, "d" * 64],
        }
        bound = Manifest.from_bytes(json.dumps(original).encode())
        self.assertNotEqual(bound.digest, Manifest.from_bytes(manifest_bytes()).digest)
        self.assertEqual(bound.environment_handler, "handler:handle")
        for field, value in (("version", True), ("version", 2), ("executable", "../python"),
                             ("executable", "/python"), ("executable", "C:\\python.exe"),
                             ("handler", "handler:1invalid"), ("handler", "handler"),
                             ("runtime_sha256", "B" * 64), ("import_roots_sha256", []),
                             ("import_roots_sha256", ["c" * 64] * 17), ("unexpected", True)):
            invalid = json.loads(json.dumps(original))
            invalid["python_environment"][field] = value
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                Manifest.from_bytes(json.dumps(invalid).encode())
        original["python_environment"] = None
        with self.assertRaises(ValueError):
            Manifest.from_bytes(json.dumps(original).encode())

    def test_environment_bootstrap_rejects_uncontained_standard_library_path(self) -> None:
        repository = Path(__file__).resolve().parents[3]
        bootstrap = repository / "crates/laminar-db/src/process_function/remote/python_environment/bootstrap.py"
        with tempfile.TemporaryDirectory() as directory:
            code = f"import sys\nsys.path.append({directory!r})\n" + bootstrap.read_text(encoding="utf-8")
            result = subprocess.run(
                [sys.executable, "-I", "-S", "-B", "-X", f"pycache_prefix={sys.executable}", "-c", code,
                 str(Path(sys.executable).resolve().parent), "[]", "laminardb_process.worker"],
                capture_output=True, text=True, timeout=5, check=False,
            )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("standard-library path is outside", result.stderr)

    def test_environment_bound_worker_requires_declared_entrypoint_and_source_file(self) -> None:
        data = json.loads(manifest_bytes())
        data["python_environment"] = {
            "version": 1, "executable": "python.exe", "handler": "handler:handle",
            "runtime_sha256": "b" * 64, "import_roots_sha256": ["c" * 64],
        }
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "manifest.json"
            manifest.write_bytes(json.dumps(data).encode())
            for arguments in (("--handler", "handler:different", "--handler-file", __file__),
                              ("--handler", "handler:handle")):
                result = subprocess.run(
                    [sys.executable, "-m", "laminardb_process.worker", "--manifest", str(manifest),
                     *arguments], capture_output=True, text=True, timeout=5, check=False,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("requires the declared handler and verified handler file", result.stderr)

    def test_ipc_accepts_one_exact_batch_and_rejects_truncation(self) -> None:
        schema = Manifest.from_bytes(manifest_bytes()).input_schema
        batch = pa.record_batch([
            pa.array(["k"], type=pa.utf8()),
            pa.array([123], type=pa.timestamp("us")),
        ], schema=schema)
        raw = _encode_batch(batch, schema)
        self.assertEqual(_decode_batch(raw, schema, 1, 1024).num_rows, 1)
        with self.assertRaises(ProtocolError) as caught:
            _decode_batch(raw[:len(raw) // 2], schema, 1, 1024)
        self.assertEqual(caught.exception.code, grpc.StatusCode.INVALID_ARGUMENT)


if __name__ == "__main__":
    unittest.main()

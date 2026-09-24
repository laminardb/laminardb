"""Local validation of manifest and IPC boundaries without a database process."""

from __future__ import annotations

import json
import unittest

import grpc
import pyarrow as pa

from laminardb_process import Manifest
from laminardb_process.worker import ProtocolError, _decode_batch, _encode_batch, serve


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
    def test_manifest_rejects_wrong_runtime_and_duplicate_timer(self) -> None:
        raw = manifest_bytes()
        manifest = Manifest.from_bytes(raw)
        self.assertEqual(len(manifest.digest), 32)
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

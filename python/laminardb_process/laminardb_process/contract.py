"""Typed handler values and the v1 manifest's Arrow schema boundary."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
from typing import Literal, TypeAlias

import pyarrow as pa


@dataclass(frozen=True)
class ValueState:
    """A managed Int64 value. ``present=True, value=None`` is SQL NULL."""

    present: bool
    value: int | None = None


@dataclass(frozen=True)
class Mutation:
    """An explicit proposed transition for the managed value."""

    kind: Literal["unchanged", "clear", "set_null", "set"] = "unchanged"
    value: int | None = None

    @classmethod
    def set(cls, value: int) -> Mutation:
        return cls("set", value)


@dataclass(frozen=True)
class TimerSet:
    name: str
    at_us: int


@dataclass(frozen=True)
class TimerCancel:
    name: str


TimerOperation: TypeAlias = TimerSet | TimerCancel


@dataclass(frozen=True)
class Activation:
    """One host-assigned key and immutable pre-transition state snapshot."""

    id: int
    key: bytes
    key_text: str
    event_time_us: int
    input: pa.RecordBatch | None
    timer_name: str | None
    state: ValueState


@dataclass(frozen=True)
class ActivationResult:
    """One proposed result. The host validates a complete invocation before applying it."""

    activation_id: int
    output: tuple[pa.RecordBatch, ...] = ()
    mutation: Mutation = Mutation()
    timers: tuple[TimerOperation, ...] = ()


def _schema(fields: list[dict]) -> pa.Schema:
    if not isinstance(fields, list) or len(fields) > 256:
        raise ValueError("invalid process schema field count")
    result = []
    names = set()
    simple_types = {
        "boolean": pa.bool_(),
        "int8": pa.int8(),
        "int16": pa.int16(),
        "int32": pa.int32(),
        "int64": pa.int64(),
        "float32": pa.float32(),
        "float64": pa.float64(),
        "utf8": pa.utf8(),
        "binary": pa.binary(),
        "timestamp_microsecond_utc": pa.timestamp("us"),
    }
    for item in fields:
        if not isinstance(item, dict) or set(item) != {"name", "nullable", "data_type"}:
            raise ValueError("invalid process field")
        name = item["name"]
        if not isinstance(name, str) or not name or name in names:
            raise ValueError("invalid or repeated process field name")
        names.add(name)
        nullable = item["nullable"]
        if not isinstance(nullable, bool):
            raise ValueError("invalid process field nullability")
        type_spec = item["data_type"]
        if not isinstance(type_spec, dict) or "type" not in type_spec:
            raise ValueError("invalid process field type")
        kind = type_spec["type"]
        if not isinstance(kind, str):
            raise ValueError("invalid process field type name")
        if kind == "decimal128" and set(type_spec) == {"type", "precision", "scale"}:
            if type(type_spec["precision"]) is not int or type(type_spec["scale"]) is not int:
                raise ValueError("invalid process decimal parameters")
            data_type = pa.decimal128(type_spec["precision"], type_spec["scale"])
        elif set(type_spec) == {"type"} and kind in simple_types:
            data_type = simple_types[kind]
        else:
            raise ValueError(f"unsupported process field type: {kind}")
        result.append(pa.field(name, data_type, nullable=nullable))
    return pa.schema(result)


@dataclass(frozen=True)
class Manifest:
    """Canonical descriptor bytes bind invocations, source and optional environment identity."""

    digest: bytes
    implementation_digest: bytes
    input_schema: pa.Schema
    output_schema: pa.Schema
    key_column: str
    event_time_column: str
    output_event_time_column: str
    timer_names: frozenset[str]
    limits: dict[str, int]
    environment_handler: str | None = None

    @classmethod
    def from_bytes(cls, raw: bytes) -> Manifest:
        if not raw or len(raw) > 64 * 1024:
            raise ValueError("process manifest must be at most 64 KiB")
        # A checked-in manifest may have one platform line ending. The host binds the
        # canonical JSON bytes, which do not include that terminator.
        raw = raw.removesuffix(b"\r\n").removesuffix(b"\n")
        data = json.loads(raw)
        fields = {
            "version", "protocol_version", "runtime", "function_id", "pipeline_state_id",
            "implementation_digest", "input_schema", "output_schema", "key_columns",
            "partitioning_abi", "event_time_column", "output_event_time_column",
            "late_event_policy", "input_changelog", "output_changelog", "value_state_name",
            "state_codec_version", "timer_names", "determinism", "limits",
        }
        if not isinstance(data, dict) or set(data) not in (fields, fields | {"python_environment"}):
            raise ValueError("process manifest fields mismatch")
        if "python_environment" in data:
            _validate_environment(data["python_environment"])
        if type(data["version"]) is not int or data["version"] != 1:
            raise ValueError("unsupported process manifest version")
        if (type(data["protocol_version"]) is not int or data["protocol_version"] != 1
                or data["runtime"] != "remote_python"):
            raise ValueError("worker requires the remote_python protocol v1 manifest")
        if (type(data["partitioning_abi"]) is not int or data["partitioning_abi"] != 2
                or type(data["state_codec_version"]) is not int or data["state_codec_version"] != 2
                or data["late_event_policy"] != "reject" or data["determinism"] != "undeclared"):
            raise ValueError("unsupported process state or late-event policy")
        if data["input_changelog"] != "append_only" or data["output_changelog"] != "append_only":
            raise ValueError("unsupported process changelog mode")
        identity = (data["function_id"], data["pipeline_state_id"], data["value_state_name"])
        digest = data["implementation_digest"]
        if (any(not isinstance(value, str) or not value for value in identity)
                or not isinstance(digest, str) or len(digest) != 64
                or any(character not in "0123456789abcdefABCDEF" for character in digest)):
            raise ValueError("invalid process identity or implementation digest")
        key_columns = data.get("key_columns")
        if not isinstance(key_columns, list) or len(key_columns) != 1 or not isinstance(key_columns[0], str):
            raise ValueError("worker requires one key column")
        input_schema = _schema(data["input_schema"])
        output_schema = _schema(data["output_schema"])
        key_column = key_columns[0]
        time_column = data["event_time_column"]
        output_time = data["output_event_time_column"]
        if (key_column not in input_schema.names
                or input_schema.field(key_column).type != pa.utf8()
                or input_schema.field(key_column).nullable):
            raise ValueError("worker requires one non-null UTF-8 key")
        for schema, name in ((input_schema, time_column), (output_schema, output_time)):
            if (name not in schema.names or schema.field(name).type != pa.timestamp("us")
                    or schema.field(name).nullable):
                raise ValueError("worker requires non-null UTC microsecond event time")
        timers = data["timer_names"]
        if not isinstance(timers, list) or any(not isinstance(x, str) or not x for x in timers):
            raise ValueError("invalid process timer names")
        if len(timers) != len(set(timers)):
            raise ValueError("repeated process timer name")
        limits = data["limits"]
        limit_fields = {
            "max_keys", "max_timers", "max_state_bytes", "max_batch_rows", "max_input_rows",
            "max_input_bytes", "max_output_rows", "max_output_bytes", "max_timer_callbacks_per_step",
        }
        if (not isinstance(limits, dict) or set(limits) != limit_fields
                or any(type(value) is not int or value < 0 for value in limits.values())):
            raise ValueError("invalid process limits")
        if any(limits[name] == 0 for name in limit_fields - {"max_timers"}):
            raise ValueError("process row and byte limits must be positive")
        needed = ("max_batch_rows", "max_input_bytes", "max_output_rows", "max_output_bytes", "max_timers")
        return cls(
            digest=sha256(raw).digest(),
            implementation_digest=bytes.fromhex(digest),
            environment_handler=data.get("python_environment", {}).get("handler"),
            input_schema=input_schema,
            output_schema=output_schema,
            key_column=key_column,
            event_time_column=time_column,
            output_event_time_column=output_time,
            timer_names=frozenset(timers),
            limits={name: limits[name] for name in needed},
        )


def _validate_environment(environment: dict) -> None:
    if (not isinstance(environment, dict)
            or set(environment) != {"version", "executable", "handler", "runtime_sha256", "import_roots_sha256"}
            or type(environment["version"]) is not int or environment["version"] != 1):
        raise ValueError("invalid Python environment binding")
    handler = environment["handler"]
    if (not isinstance(handler, str) or handler.count(":") != 1
            or any(not name.isascii() or not name.isidentifier() for name in handler.split(":"))):
        raise ValueError("invalid Python environment handler")
    executable = environment["executable"]
    if (not isinstance(executable, str) or not executable or len(executable.encode("utf-8")) > 1024
            or any(character in executable for character in ("\\", ":", "\0"))
            or any(part in ("", ".", "..") for part in executable.split("/"))):
        raise ValueError("invalid Python environment executable")
    roots = environment["import_roots_sha256"]
    if not isinstance(roots, list) or not 1 <= len(roots) <= 16:
        raise ValueError("invalid Python environment import roots")
    for digest in (environment["runtime_sha256"], *roots):
        if (not isinstance(digest, str) or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)):
            raise ValueError("invalid Python environment tree digest")

"""Bounded, loopback-only gRPC worker for Python process functions."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import importlib
import importlib.util
from hashlib import sha256
import ipaddress
from pathlib import Path
import signal
import sys
import threading
import time
from types import ModuleType
from typing import Callable, Iterator, Sequence

import grpc
import pyarrow as pa
import pyarrow.compute as pc

from .contract import Activation, ActivationResult, Manifest, Mutation, TimerCancel, TimerSet, ValueState
from . import process_worker_pb2 as wire


MAX_FRAME_BYTES = 8 * 1024 * 1024
MAX_INVOCATION_WIRE_BYTES = 32 * 1024 * 1024
GRPC_MESSAGE_BYTES = MAX_FRAME_BYTES + 64 * 1024
Handler = Callable[[tuple[Activation, ...]], Sequence[ActivationResult]]


class ProtocolError(Exception):
    def __init__(self, code: grpc.StatusCode, message: str):
        super().__init__(message)
        self.code = code


def _require(condition: bool, code: grpc.StatusCode, message: str) -> None:
    if not condition:
        raise ProtocolError(code, message)


def _decode_batch(raw: bytes, schema: pa.Schema, max_rows: int, max_bytes: int) -> pa.RecordBatch:
    _require(0 < len(raw) <= MAX_FRAME_BYTES, grpc.StatusCode.INVALID_ARGUMENT, "invalid Arrow IPC frame size")
    try:
        with pa.ipc.open_stream(pa.py_buffer(raw)) as reader:
            _require(reader.schema.equals(schema, check_metadata=True), grpc.StatusCode.INVALID_ARGUMENT, "Arrow IPC schema mismatch")
            batch = reader.read_next_batch()
            try:
                reader.read_next_batch()
            except StopIteration:
                pass
            else:
                raise ProtocolError(grpc.StatusCode.INVALID_ARGUMENT, "Arrow IPC must contain one batch")
    except (pa.ArrowException, StopIteration) as exc:
        raise ProtocolError(grpc.StatusCode.INVALID_ARGUMENT, "invalid or empty Arrow IPC stream") from exc
    _require(batch.num_rows <= max_rows and batch.nbytes <= max_bytes,
             grpc.StatusCode.RESOURCE_EXHAUSTED, "decoded Arrow batch exceeds budget")
    return batch


def _encode_batch(batch: pa.RecordBatch, schema: pa.Schema) -> bytes:
    _require(isinstance(batch, pa.RecordBatch) and batch.schema.equals(schema, check_metadata=True),
             grpc.StatusCode.INVALID_ARGUMENT, "handler output schema mismatch")
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, schema, options=pa.ipc.IpcWriteOptions(compression=None)) as writer:
        writer.write_batch(batch)
    raw = sink.getvalue().to_pybytes()
    _require(len(raw) <= MAX_FRAME_BYTES, grpc.StatusCode.RESOURCE_EXHAUSTED, "handler output IPC frame exceeds budget")
    return raw


def _timestamp_values(batch: pa.RecordBatch, column: str) -> list[int]:
    return pc.cast(batch.column(batch.schema.get_field_index(column)), pa.int64()).to_pylist()


def _decode_activation(raw: wire.Activation, manifest: Manifest) -> Activation:
    _require(bool(raw.canonical_key) and len(raw.canonical_key) <= MAX_FRAME_BYTES,
             grpc.StatusCode.INVALID_ARGUMENT, "invalid canonical key")
    state_kind = raw.WhichOneof("state")
    if state_kind == "absent" and raw.absent:
        state = ValueState(False)
    elif state_kind == "null_value" and raw.null_value:
        state = ValueState(True)
    elif state_kind == "value":
        state = ValueState(True, raw.value)
    else:
        raise ProtocolError(grpc.StatusCode.INVALID_ARGUMENT, "invalid process state view")
    callback = raw.WhichOneof("callback")
    if callback == "input_ipc":
        batch = _decode_batch(raw.input_ipc, manifest.input_schema, 1, manifest.limits["max_input_bytes"])
        _require(batch.num_rows == 1, grpc.StatusCode.INVALID_ARGUMENT, "input activation must contain one row")
        key = batch.column(batch.schema.get_field_index(manifest.key_column))[0].as_py()
        times = _timestamp_values(batch, manifest.event_time_column)
        _require(key == raw.key_text and times == [raw.event_time_us],
                 grpc.StatusCode.INVALID_ARGUMENT, "activation metadata differs from input row")
        return Activation(raw.id, raw.canonical_key, raw.key_text, raw.event_time_us, batch, None, state)
    if callback == "timer_name" and raw.timer_name in manifest.timer_names:
        return Activation(raw.id, raw.canonical_key, raw.key_text, raw.event_time_us,
                          None, raw.timer_name, state)
    raise ProtocolError(grpc.StatusCode.INVALID_ARGUMENT, "invalid process callback")


def _validate_open(open_frame: wire.Open, manifest: Manifest) -> None:
    _require(open_frame.protocol_version == 1 and open_frame.descriptor_sha256 == manifest.digest,
             grpc.StatusCode.FAILED_PRECONDITION, "process protocol or descriptor mismatch")
    _require(0 < len(open_frame.operator_id) <= 128 and open_frame.vnode_count > 0
             and open_frame.vnode < open_frame.vnode_count and len(open_frame.batch_id) == 16
             and len(open_frame.attempt_id) == 16 and open_frame.batch_id != open_frame.attempt_id,
             grpc.StatusCode.INVALID_ARGUMENT, "invalid process invocation scope")
    remaining_ms = open_frame.deadline_unix_ms - time.time_ns() // 1_000_000
    _require(0 < remaining_ms <= 30_000, grpc.StatusCode.DEADLINE_EXCEEDED,
             "process invocation deadline expired or exceeds 30 seconds")


def _read_request(requests: Iterator[wire.HostFrame], manifest: Manifest) -> tuple[wire.Open, tuple[Activation, ...]]:
    try:
        first = next(requests)
    except StopIteration as exc:
        raise ProtocolError(grpc.StatusCode.INVALID_ARGUMENT, "missing process open frame") from exc
    _require(first.WhichOneof("kind") == "open", grpc.StatusCode.INVALID_ARGUMENT, "process open must be first")
    open_frame = first.open
    _validate_open(open_frame, manifest)
    total_wire = first.ByteSize()
    input_bytes = 0
    activations = []
    ids = set()
    keys = set()
    for frame in requests:
        _require(time.time_ns() // 1_000_000 < open_frame.deadline_unix_ms,
                 grpc.StatusCode.DEADLINE_EXCEEDED, "process request deadline expired")
        total_wire += frame.ByteSize()
        _require(total_wire <= MAX_INVOCATION_WIRE_BYTES,
                 grpc.StatusCode.RESOURCE_EXHAUSTED, "process request wire budget exceeded")
        kind = frame.WhichOneof("kind")
        if kind == "end":
            _require(frame.end.count == len(activations) and bool(activations),
                     grpc.StatusCode.INVALID_ARGUMENT, "process end count mismatch")
            try:
                next(requests)
            except StopIteration:
                return open_frame, tuple(activations)
            raise ProtocolError(grpc.StatusCode.INVALID_ARGUMENT, "process frame after end")
        _require(kind == "activation", grpc.StatusCode.INVALID_ARGUMENT, "invalid process request frame")
        _require(len(activations) < manifest.limits["max_batch_rows"],
                 grpc.StatusCode.RESOURCE_EXHAUSTED, "process batch row limit exceeded")
        activation = _decode_activation(frame.activation, manifest)
        _require(activation.id not in ids and activation.key not in keys,
                 grpc.StatusCode.INVALID_ARGUMENT, "repeated activation ID or key")
        ids.add(activation.id)
        keys.add(activation.key)
        if activation.input is not None:
            input_bytes += activation.input.nbytes
            _require(input_bytes <= manifest.limits["max_input_bytes"],
                     grpc.StatusCode.RESOURCE_EXHAUSTED, "process input byte budget exceeded")
        activations.append(activation)
    raise ProtocolError(grpc.StatusCode.INVALID_ARGUMENT, "process invocation ended without completion")


def _encode_mutation(mutation: Mutation, target: wire.Result) -> None:
    _require(isinstance(mutation, Mutation), grpc.StatusCode.INVALID_ARGUMENT, "invalid handler mutation")
    if mutation.kind == "set" and type(mutation.value) is int and -(1 << 63) <= mutation.value < (1 << 63):
        target.set_value = mutation.value
    elif mutation.value is None and mutation.kind == "unchanged":
        target.unchanged = True
    elif mutation.value is None and mutation.kind == "clear":
        target.clear = True
    elif mutation.value is None and mutation.kind == "set_null":
        target.set_null = True
    else:
        raise ProtocolError(grpc.StatusCode.INVALID_ARGUMENT, "invalid handler mutation")


def _encode_results(open_frame: wire.Open, activations: tuple[Activation, ...],
                    results: Sequence[ActivationResult], manifest: Manifest) -> list[wire.WorkerFrame]:
    _require(len(results) == len(activations), grpc.StatusCode.INVALID_ARGUMENT, "handler result count mismatch")
    frames = []
    wire_bytes = 0

    def append(frame: wire.WorkerFrame) -> None:
        nonlocal wire_bytes
        size = frame.ByteSize()
        _require(size <= GRPC_MESSAGE_BYTES and wire_bytes + size <= MAX_INVOCATION_WIRE_BYTES,
                 grpc.StatusCode.RESOURCE_EXHAUSTED, "process response wire budget exceeded")
        wire_bytes += size
        frames.append(frame)

    append(wire.WorkerFrame(ack=wire.OpenAck(protocol_version=1,
                                            descriptor_sha256=manifest.digest,
                                            attempt_id=open_frame.attempt_id)))
    expected = {activation.id: activation for activation in activations}
    seen = set()
    output_count = 0
    output_rows = 0
    output_bytes = 0
    timer_count = 0
    for result in results:
        _require(isinstance(result, ActivationResult) and result.activation_id in expected
                 and result.activation_id not in seen,
                 grpc.StatusCode.INVALID_ARGUMENT, "handler returned invalid activation ID")
        seen.add(result.activation_id)
        output_count += len(result.output)
        timer_count += len(result.timers)
        _require(output_count <= max(1, manifest.limits["max_output_rows"])
                 and timer_count <= manifest.limits["max_timers"],
                 grpc.StatusCode.RESOURCE_EXHAUSTED, "handler result exceeds count budget")
        wire_result = wire.Result(activation_id=result.activation_id, output_count=len(result.output))
        _encode_mutation(result.mutation, wire_result)
        for operation in result.timers:
            _require(operation.name in manifest.timer_names,
                     grpc.StatusCode.INVALID_ARGUMENT, "handler used undeclared timer")
            if isinstance(operation, TimerSet):
                if open_frame.HasField("input_watermark_us"):
                    _require(operation.at_us > open_frame.input_watermark_us,
                             grpc.StatusCode.INVALID_ARGUMENT, "timer must exceed input watermark")
                wire_result.timers.add(set=wire.TimerSet(name=operation.name, at_us=operation.at_us))
            elif isinstance(operation, TimerCancel):
                wire_result.timers.add(cancel=operation.name)
            else:
                raise ProtocolError(grpc.StatusCode.INVALID_ARGUMENT, "invalid handler timer operation")
            _require(wire_result.ByteSize() <= GRPC_MESSAGE_BYTES,
                     grpc.StatusCode.RESOURCE_EXHAUSTED, "process timer result frame exceeds budget")
        append(wire.WorkerFrame(result=wire_result))
        for batch in result.output:
            _require(isinstance(batch, pa.RecordBatch), grpc.StatusCode.INVALID_ARGUMENT, "handler output must be a RecordBatch")
            _require(batch.schema.equals(manifest.output_schema, check_metadata=True),
                     grpc.StatusCode.INVALID_ARGUMENT, "handler output schema mismatch")
            output_rows += batch.num_rows
            output_bytes += batch.nbytes
            _require(output_rows <= manifest.limits["max_output_rows"]
                     and output_bytes <= manifest.limits["max_output_bytes"],
                     grpc.StatusCode.RESOURCE_EXHAUSTED, "handler output exceeds byte or row budget")
            _require(all(value is not None and value >= expected[result.activation_id].event_time_us
                         for value in _timestamp_values(batch, manifest.output_event_time_column)),
                     grpc.StatusCode.INVALID_ARGUMENT, "handler emitted before activation time")
            append(wire.WorkerFrame(output=wire.Output(
                activation_id=result.activation_id, arrow_ipc=_encode_batch(batch, manifest.output_schema))))
    append(wire.WorkerFrame(complete=wire.Complete(
        result_count=len(seen), output_count=output_count)))
    return frames


class ProcessWorker:
    def __init__(self, manifest: Manifest, handler: Handler, max_in_flight: int):
        self.manifest = manifest
        self.handler = handler
        self.credits = threading.BoundedSemaphore(max_in_flight)

    def exchange(self, requests: Iterator[wire.HostFrame], context: grpc.ServicerContext) -> Iterator[wire.WorkerFrame]:
        remaining = context.time_remaining()
        if remaining is None or not 0 < remaining <= 30:
            context.abort(grpc.StatusCode.DEADLINE_EXCEEDED,
                          "process invocation requires a deadline of at most 30 seconds")
        if not self.credits.acquire(blocking=False):
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "process worker invocation slots full")
        try:
            open_frame, activations = _read_request(requests, self.manifest)
            results = self.handler(activations)
            frames = _encode_results(open_frame, activations, results, self.manifest)
        except ProtocolError as exc:
            context.abort(exc.code, str(exc))
        except Exception as exc:
            message = f"process handler failed: {type(exc).__name__}: {exc}"[:512]
            yield wire.WorkerFrame(failure=wire.Failure(message=message))
        else:
            yield from frames
        finally:
            self.credits.release()


def serve(manifest: Manifest, handler: Handler, bind: str, max_in_flight: int = 2) -> None:
    """Serve one immutable package on an explicit loopback IP address."""
    host, separator, port_text = bind.rpartition(":")
    if not separator:
        raise ValueError("bind must be an explicit loopback IP and port")
    address = ipaddress.ip_address(host.strip("[]"))
    if not address.is_loopback:
        raise ValueError("plaintext process worker must bind to loopback")
    port = int(port_text)
    if not 0 <= port <= 65535 or not 1 <= max_in_flight <= 32:
        raise ValueError("invalid worker port or concurrency")
    pa.set_cpu_count(max_in_flight)
    pa.set_io_thread_count(max_in_flight)
    worker = ProcessWorker(manifest, handler, max_in_flight)
    with ThreadPoolExecutor(max_workers=max_in_flight) as pool:
        server = grpc.server(
            pool,
            options=(("grpc.max_receive_message_length", GRPC_MESSAGE_BYTES),
                     ("grpc.max_send_message_length", GRPC_MESSAGE_BYTES),
                     ("grpc.so_reuseport", 0)),
            maximum_concurrent_rpcs=max_in_flight,
        )
        service = wire.DESCRIPTOR.services_by_name["ProcessWorker"].full_name
        method = grpc.stream_stream_rpc_method_handler(
            worker.exchange,
            request_deserializer=wire.HostFrame.FromString,
            response_serializer=wire.WorkerFrame.SerializeToString,
        )
        server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(service, {"Exchange": method}),))
        bound_port = server.add_insecure_port(f"{host}:{port}")
        if bound_port == 0:
            raise OSError("failed to bind process worker listener")
        server.start()
        previous_sigterm = None
        if threading.current_thread() is threading.main_thread():
            previous_sigterm = signal.signal(signal.SIGTERM, lambda _signal, _frame: server.stop(5))
        try:
            print(f"READY {bound_port}", flush=True)
            server.wait_for_termination()
        finally:
            if previous_sigterm is not None:
                signal.signal(signal.SIGTERM, previous_sigterm)
            if not server.stop(0).wait(timeout=5):
                raise TimeoutError("process worker failed to stop")


def _load_bound_module(module_name: str, handler_file: Path, digest: bytes) -> ModuleType:
    if handler_file.suffix != ".py" or module_name.rpartition(".")[2] != handler_file.stem:
        raise ValueError("handler module differs from verified handler file")
    if module_name in sys.modules:
        raise ValueError("handler module was loaded before verification")
    source = handler_file.read_bytes()
    if sha256(source).digest() != digest:
        raise ValueError("handler file digest differs from process manifest")
    spec = importlib.util.spec_from_file_location(module_name, handler_file)
    if spec is None or spec.loader is None:
        raise ValueError("handler file is not a Python source module")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        # Execute the exact bytes just hashed; importlib may otherwise reuse stale .pyc code.
        exec(compile(source, str(handler_file), "exec"), module.__dict__)
    except BaseException:
        if sys.modules.get(module_name) is module:
            del sys.modules[module_name]
        raise
    return module


def main() -> None:
    parser = argparse.ArgumentParser(description="LaminarDB loopback Python process worker")
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--handler", required=True, help="module:callable")
    parser.add_argument("--handler-file", type=Path, help="verified local handler module file")
    parser.add_argument("--bind", default="127.0.0.1:0")
    parser.add_argument("--max-in-flight", type=int, default=2)
    args = parser.parse_args()
    module_name, separator, name = args.handler.partition(":")
    if not separator or not module_name or not name:
        parser.error("handler must be module:callable")
    manifest = Manifest.from_bytes(args.manifest.read_bytes())
    if manifest.environment_handler is not None and (args.handler != manifest.environment_handler or args.handler_file is None):
        parser.error("environment-bound worker requires the declared handler and verified handler file")
    if args.handler_file is None:
        module = importlib.import_module(module_name)
    else:
        try:
            module = _load_bound_module(module_name, args.handler_file, manifest.implementation_digest)
        except (OSError, ValueError) as exc:
            parser.error(str(exc))
    handler = getattr(module, name)
    if not callable(handler):
        parser.error("handler is not callable")
    serve(manifest, handler, args.bind, args.max_in_flight)


if __name__ == "__main__":
    main()

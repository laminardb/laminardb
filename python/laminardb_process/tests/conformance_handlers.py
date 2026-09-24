"""Stateless handler fixtures used by the Rust/Python wire tests."""

from __future__ import annotations

import os
from pathlib import Path
import time

import pyarrow as pa

from laminardb_process import Activation, ActivationResult, Mutation, TimerCancel, TimerSet


OUTPUT_SCHEMA = pa.schema([
    pa.field("key", pa.utf8(), nullable=False),
    pa.field("total", pa.int64(), nullable=False),
    pa.field("ts", pa.timestamp("us"), nullable=False),
])


def accumulate(activations: tuple[Activation, ...]) -> tuple[ActivationResult, ...]:
    results = []
    for activation in activations:
        if activation.timer_name is not None:
            results.append(ActivationResult(
                activation.id,
                mutation=Mutation("clear"),
                timers=(TimerCancel(activation.timer_name),),
            ))
            continue
        assert activation.input is not None
        amount = activation.input.column(1)[0].as_py()
        prior = activation.state.value if activation.state.value is not None else 0
        total = prior + amount
        output = ()
        if amount != 0:
            row = pa.record_batch([
                pa.array([activation.key_text], type=pa.utf8()),
                pa.array([total], type=pa.int64()),
                pa.array([activation.event_time_us], type=pa.timestamp("us")),
            ], schema=OUTPUT_SCHEMA)
            output = (row, row)
        results.append(ActivationResult(
            activation.id,
            output=output,
            mutation=Mutation.set(total),
            timers=(TimerSet("flush", activation.event_time_us + 100),),
        ))
    return tuple(results)


def echo_types(activations: tuple[Activation, ...]) -> tuple[ActivationResult, ...]:
    return tuple(ActivationResult(item.id, output=(item.input,)) for item in activations)


def state_variants(activations: tuple[Activation, ...]) -> tuple[ActivationResult, ...]:
    results = []
    for item in activations:
        if not item.state.present:
            mutation = Mutation("set_null")
        elif item.state.value is None:
            mutation = Mutation("clear")
        else:
            mutation = Mutation.set(item.state.value + 1)
        results.append(ActivationResult(item.id, mutation=mutation))
    return tuple(results)


def blocking(activations: tuple[Activation, ...]) -> tuple[ActivationResult, ...]:
    marker = os.environ.get("LAMINAR_PROCESS_TEST_BLOCK_MARKER")
    if marker is not None:
        with Path(marker).open("a", encoding="ascii") as output:
            output.write(f"{activations[0].id}\n")
    time.sleep(1.0)
    return tuple(ActivationResult(item.id) for item in activations)

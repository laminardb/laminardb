"""Vectorized account monitor; all authoritative state and timers stay in LaminarDB."""

import pyarrow as pa
import pyarrow.compute as pc

from laminardb_process import Activation, ActivationResult, Mutation, TimerSet


THRESHOLD = 100
INACTIVITY_US = 10_000
OUTPUT_SCHEMA = pa.schema([
    pa.field("account", pa.utf8(), nullable=False),
    pa.field("kind", pa.utf8(), nullable=False),
    pa.field("total", pa.int64(), nullable=False),
    pa.field("ts", pa.timestamp("us"), nullable=False),
])


def handle(activations: tuple[Activation, ...]) -> tuple[ActivationResult, ...]:
    inputs = []
    results = []
    for activation in activations:
        if activation.state.present and activation.state.value is None:
            raise ValueError("account total is null")
        if activation.timer_name is None:
            if activation.input is None or activation.input.num_rows != 1:
                raise ValueError("invalid amount row")
            inputs.append(activation)
            continue
        if activation.timer_name != "inactive":
            raise ValueError("unknown account timer")
        output = pa.record_batch([
            pa.array([activation.key_text], type=pa.utf8()),
            pa.array(["inactive"], type=pa.utf8()),
            pa.array([activation.state.value or 0], type=pa.int64()),
            pa.array([activation.event_time_us], type=pa.timestamp("us")),
        ], schema=OUTPUT_SCHEMA)
        results.append(ActivationResult(activation.id, output=(output,)))
    if not inputs:
        return tuple(results)

    prior = pa.array([item.state.value or 0 for item in inputs], type=pa.int64())
    amounts = pa.concat_arrays([item.input.column("amount") for item in inputs])
    if amounts.type != pa.int64() or amounts.null_count:
        raise ValueError("invalid amount row")
    totals = pc.add_checked(prior, amounts)
    times = pa.array([item.event_time_us for item in inputs], type=pa.int64())
    deadlines = pc.add_checked(times, INACTIVITY_US)
    crossed = pc.and_(pc.less(prior, THRESHOLD), pc.greater_equal(totals, THRESHOLD))
    output = pa.record_batch([
        pa.array([item.key_text for item in inputs], type=pa.utf8()),
        pc.if_else(crossed, "threshold", "running"),
        totals,
        times.cast(pa.timestamp("us")),
    ], schema=OUTPUT_SCHEMA)
    for row, activation in enumerate(inputs):
        results.append(ActivationResult(
            activation.id,
            output=(output.slice(row, 1),),
            mutation=Mutation.set(totals[row].as_py()),
            timers=(TimerSet("inactive", deadlines[row].as_py()),),
        ))
    return tuple(results)

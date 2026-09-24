"""Small stateless Python handler; LaminarDB owns the running total and timer."""

import pyarrow as pa

from laminardb_process import Activation, ActivationResult, Mutation, TimerCancel, TimerSet


OUTPUT_SCHEMA = pa.schema([
    pa.field("key", pa.utf8(), nullable=False),
    pa.field("total", pa.int64(), nullable=False),
    pa.field("ts", pa.timestamp("us"), nullable=False),
])


def handle(activations: tuple[Activation, ...]) -> tuple[ActivationResult, ...]:
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
            output = (row,)
        results.append(ActivationResult(
            activation.id,
            output=output,
            mutation=Mutation.set(total),
            timers=(TimerSet("flush", activation.event_time_us + 100),),
        ))
    return tuple(results)

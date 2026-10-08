"""Pure recovery oracle: callback identities are returned as ordinary output data."""

import pyarrow as pa

from laminardb_process import ActivationResult, Mutation, TimerSet


SCHEMA = pa.schema([
    pa.field("account", pa.utf8(), nullable=False),
    pa.field("kind", pa.utf8(), nullable=False),
    pa.field("total", pa.int64(), nullable=False),
    pa.field("crossed", pa.bool_(), nullable=False),
    pa.field("ts", pa.timestamp("us"), nullable=False),
])


def handle(activations):
    results = []
    for activation in activations:
        prior = activation.state.value if activation.state.present else 0
        if prior is None:
            raise ValueError("null account state")
        timer = activation.timer_name is not None
        if timer and activation.timer_name != "inactive":
            raise ValueError("unknown account timer")
        total = prior if timer else prior + activation.input.column("amount")[0].as_py()
        crossed = not timer and prior < 100 <= total
        kind = f"{'inactive' if timer else 'running'}:{activation.id}"
        output = pa.record_batch([
            pa.array([activation.key_text], type=pa.utf8()),
            pa.array([kind], type=pa.utf8()),
            pa.array([total], type=pa.int64()),
            pa.array([crossed], type=pa.bool_()),
            pa.array([activation.event_time_us], type=pa.timestamp("us")),
        ], schema=SCHEMA)
        results.append(ActivationResult(
            activation.id, output=(output,),
            mutation=Mutation() if timer else Mutation.set(total),
            timers=() if timer else (TimerSet("inactive", activation.event_time_us + 10_000),),
        ))
    return tuple(results)

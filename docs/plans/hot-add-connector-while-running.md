# Hot-add / hot-remove connectors on a running pipeline

Deferred follow-up. Today connector DDL is rejected while `Running`
(`connector_ddl_rejected`) because connectors are built only in `start()`, so
adding a source/sink needs stop → CREATE → start (a full rebuild). Streams/MVs
already hot-mutate via the coordinator control channel (`ControlMsg::DropStream`).

Approach: extend that channel with acked `AddSource`/`RemoveSource` (+ sink)
messages; the coordinator builds the connector, joins it at a checkpoint barrier
boundary, registers its offset, and (cluster) applies the vnode assignment. DDL
sends the message and awaits the ack instead of erroring.

Main risk is checkpoint/exactly-once correctness for a source that appears
mid-stream — get the barrier-boundary join reviewed against Flink/RisingWave
first. Phasing: non-cluster at-least-once → checkpoint integration → cluster →
sinks/DROP.

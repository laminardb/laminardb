# Hot-Add / Hot-Remove Connectors on a Running Pipeline — Plan

Follow-up to `fix/dynamic-source-ddl` (env vars in dynamic SQL + durable
connector replay). Written against `main` after that branch.

## 1. Problem

Connector sources and sinks are instantiated **only** inside `start()` →
`start_inner` (`pipeline_lifecycle.rs:237`), which reads the full
`ConnectorManager` registration set once and builds every connector. There is
no path to add or remove a single connector on a *running* coordinator, so the
DDL guard (`connector_ddl_rejected()`, `ddl.rs:37/240/520/629`) rejects
`CREATE`/`DROP SOURCE`/`SINK` while `Running`/`ShuttingDown` with *"Stop the
pipeline first."*

Consequence for the console/HTTP workflow: adding one Kafka source requires
**stop → CREATE → start**, which:
- drives the node out of `Running` (readiness 503 in the gap), and
- rebuilds the **entire** pipeline — every source re-opens, all operator state
  rehydrates from checkpoint — to add one connector.

Streams and MVs do **not** have this problem: they are internal operators the
coordinator mutates live via the control channel
(`control_tx`/`ControlMsg::DropStream`, `ddl.rs:780`, bounded crossfire(64)
created at `pipeline_lifecycle.rs:1656`). Hot-add extends that same mechanism to
external connectors.

## 2. Goal

Apply `CREATE SOURCE`/`CREATE SINK`/`DROP SOURCE`/`DROP SINK` (connector forms)
to a `Running` pipeline without a full stop/start, with a synchronous success/
failure result back to the API caller, and without breaking exactly-once.

## 3. Design

### 3.1 Control messages
Extend `ControlMsg` (`pipeline/mod.rs` / wherever `DropStream` lives) with:

```rust
enum ControlMsg {
    // …existing…
    AddSource    { reg: SourceRegistration, ack: oneshot::Sender<Result<(), String>> },
    RemoveSource { name: String,            ack: oneshot::Sender<Result<(), String>> },
    AddSink      { reg: SinkRegistration,   ack: oneshot::Sender<Result<(), String>> },
    RemoveSink   { name: String,            ack: oneshot::Sender<Result<(), String>> },
}
```

The `ack` oneshot lets `execute()` block on the coordinator's result so the
HTTP call returns a real 200/4xx instead of fire-and-forget. (Today's
`DropStream` is fire-and-forget; the connector variants must be acked because
connector construction can fail — bad broker, unknown type, etc.)

### 3.2 Coordinator handling
The `StreamingCoordinator` owns the live `sources` set and per-source reader
tasks / watermark trackers. On `AddSource`:
1. `connector_registry.create_source(&config, prom)` (same call as
   `start_inner:704`); on error, send `Err` on the ack and stop.
2. `set_vnode_assignment(registry, self_id)` for cluster mode
   (`start_inner:714-723`).
3. Open the connector, register it in the running source map + scheduler, wire
   watermark/event-time columns (`start_inner:732-760`).
4. **Join at a checkpoint boundary** (see §3.3); then ack `Ok`.

`RemoveSource`: quiesce the reader, drain in-flight, deregister from the source
map + checkpoint set, drop the connector, ack.

### 3.3 Checkpoint / exactly-once correctness (the hard part)
- A newly-added source has no prior offset. It must enter the topology **at a
  barrier boundary**: the coordinator should fold `AddSource` into the start of
  the next epoch so the source's first records are bracketed by a clean barrier
  pair (no records straddling the add). Reuse the aligned-barrier machinery
  from the cross-node shuffle work (`cross-node-shuffle-barrier-alignment.md`).
- The checkpoint coordinator's source-offset map must learn the new source so
  recovery after the add restores it; until the first checkpoint that includes
  it, the source is effectively at-least-once.
- `RemoveSource` must complete a final checkpoint (or be excluded atomically) so
  a recovery doesn't resurrect a dropped source from a stale manifest. Pairs
  with the manifest `store_ddl`/replay path already in place.

### 3.4 Cluster
- Partition→vnode assignment for the new source uses the existing
  `set_vnode_assignment` + partition-aware Kafka `assign()`
  (`partition-aware-kafka-gap2`). The add must respect `owned_partitions`.
- The catalog manifest already persists the DDL (post `fix/dynamic-source-ddl`),
  so a peer that boots later replays it. No manifest change needed.

### 3.5 DDL wiring
In `handle_create_source`/`handle_create_sink`, when `connector_ddl_rejected()`
is true, instead of erroring: build the `SourceRegistration`, send
`ControlMsg::AddSource{reg, ack}`, and `await` the ack; map `Err` →
`DbError::Connector`. Symmetric for drops. When **not** running (Created/
Starting), keep today's path (register + let `start_inner` build it).

## 4. Phasing
1. **Plumbing + non-cluster at-least-once** — `ControlMsg` variants, coordinator
   add/remove for sources, acked DDL wiring. Sinks next.
2. **Checkpoint integration** — barrier-boundary join, offset registration,
   exactly-once; remove-with-final-checkpoint.
3. **Cluster** — vnode assignment + partition-aware assign for live-added
   sources; multi-node soak.
4. **Sinks + DROP** — connector teardown, 2PC sink drain on remove.
5. **Tests/soak** — add-under-load, add+kill -9 exactly-once diff, add/remove
   churn, partial-add failure rollback.

## 5. Risks
- Barrier/epoch ordering for a source that appears mid-stream is the core
  correctness risk; get §3.3 reviewed against Flink/RisingWave semantics first.
- Partial-add failure must fully roll back (no half-registered source the next
  checkpoint captures).
- Crash safety: a panic while applying `AddSource` must not corrupt the running
  topology — apply changes only after the connector is fully constructed.

## 6. Out of scope
Per-query/per-operator dynamic graph rewrites beyond source/sink connectors;
those already work for streams/MVs via the existing control channel.

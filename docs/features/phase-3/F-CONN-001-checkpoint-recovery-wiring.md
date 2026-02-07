# F-CONN-001: Connector Pipeline Checkpoint Recovery

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-CONN-001 |
| **Status** | 📝 Draft |
| **Priority** | P0 |
| **Phase** | 3 |
| **Effort** | L (3-5 days) |
| **Dependencies** | F022 (Incremental Checkpointing), F025/F026 (Kafka Connectors), F-DAG-004 (DAG Checkpointing) |

## Summary

Wire end-to-end checkpoint and recovery into the connector pipeline
(`start_connector_pipeline()` in `db.rs`). Currently the Kafka connector
pipeline runs with no periodic checkpointing, no source offset persistence,
and no recovery on restart. Sources always start from `offset_reset` regardless
of prior progress.

## Motivation

Without checkpoint recovery:
- Restarting the consumer reprocesses all data from `earliest` or loses data from `latest`
- No exactly-once semantics across restarts
- Kafka consumer offsets committed to Kafka are not coordinated with internal state
- Operator state (window aggregations, join buffers) is lost on restart
- Cannot demonstrate production-grade durability

With checkpoint recovery:
- Consumer resumes from last committed offset
- Operator state restored from checkpoint
- Exactly-once via coordinated Kafka transactions + state snapshots
- Sub-10-second recovery time

## Current State

| Component | Status |
|-----------|--------|
| `StreamCheckpointConfig` type | Done — `interval_ms`, `data_dir`, `wal_mode`, etc. |
| `LaminarDbBuilder.checkpoint()` | Done — accepts config |
| `db.checkpoint()` manual trigger | Done — calls `StreamCheckpointManager` |
| `ConnectorBridgeRuntime` (F-DAG-006) | Done — full DAG checkpoint/recovery |
| `KafkaSource.checkpoint()` | Done — returns `SourceCheckpoint` with per-partition offsets |
| `KafkaSource.restore()` | Done — seeks consumer to checkpointed offsets |
| `KafkaSink.commit_epoch()` | Done — Kafka transaction commit |
| `KafkaSink.rollback_epoch()` | Done — Kafka transaction abort |
| **Periodic checkpoint trigger in pipeline** | **NOT WIRED** |
| **Source offset recovery on startup** | **NOT WIRED** |
| **Operator state persistence in pipeline** | **NOT WIRED** |
| **Sink epoch coordination in pipeline** | **NOT WIRED** |

## Design

### Option A: Wire Into Existing Pipeline (Minimal Change)

Add checkpoint coordination directly to `start_connector_pipeline()`:

#### Startup Recovery

```rust
// In start_connector_pipeline(), after opening sources:
if self.checkpoint_config.is_some() {
    if let Ok(checkpoint) = self.restore_checkpoint() {
        // Restore source offsets
        for (name, source, _) in &mut sources {
            if let Some(offsets) = checkpoint.source_offsets.get(name) {
                source.restore(offsets).await?;
            }
        }
        // Restore operator state (window accumulators, join buffers)
        executor.restore_state(&checkpoint.operator_states)?;
        // Rollback uncommitted sink epochs
        for (_, sink, _, _) in &mut sinks {
            sink.rollback_epoch(checkpoint.epoch).await?;
        }
    }
}
```

#### Periodic Checkpointing in Pipeline Loop

```rust
let checkpoint_interval = self.checkpoint_config
    .as_ref()
    .and_then(|c| c.interval_ms)
    .map(Duration::from_millis);
let mut last_checkpoint = Instant::now();

// Inside the main poll loop:
if let Some(interval) = checkpoint_interval {
    if last_checkpoint.elapsed() >= interval {
        // 1. Capture source offsets
        let mut source_offsets = HashMap::new();
        for (name, source, _) in &sources {
            source_offsets.insert(name.clone(), source.checkpoint());
        }

        // 2. Capture operator state
        let operator_states = executor.checkpoint_state()?;

        // 3. Build and persist checkpoint
        let epoch = self.checkpoint_manager.lock().next_epoch();
        let checkpoint = PipelineCheckpoint {
            epoch,
            source_offsets,
            operator_states,
            watermarks: current_watermarks.clone(),
            timestamp_ms: now_ms(),
        };
        self.checkpoint_manager.lock().persist(&checkpoint)?;

        // 4. Commit sink epochs (exactly-once)
        for (_, sink, _, _) in &mut sinks {
            if sink.capabilities().exactly_once {
                sink.commit_epoch(epoch).await?;
            }
        }

        // 5. Truncate WAL up to checkpoint
        if let Some(wal) = &self.wal {
            wal.truncate_before(epoch)?;
        }

        last_checkpoint = Instant::now();
    }
}
```

### Option B: Switch to ConnectorBridgeRuntime (Full DAG)

Replace `start_connector_pipeline()` internals with `ConnectorBridgeRuntime`:

```rust
let mut runtime = ConnectorBridgeRuntime::new(
    dag,
    executor,
    sources.into_iter().map(|(n, s, _)| DagSourceBridge::new(n, s)).collect(),
    sinks.into_iter().map(|(n, s, _, _)| DagSinkBridge::new(n, s)).collect(),
    BridgeRuntimeConfig {
        checkpoint_interval: checkpoint_interval,
        exactly_once: true,
        ..Default::default()
    },
);

// Recovery
if runtime.recovery_manager().has_snapshots() {
    runtime.recover().await?;
}

// Main loop
loop {
    let result = runtime.process_cycle().await?;
    // Push results to stream subscriptions...
    runtime.maybe_trigger_checkpoint().await?;
}
```

This gives us barrier-based checkpointing, coordinated recovery, and
exactly-once out of the box.

### Recommended: Option B

Option B is the production-grade path. All the components exist:
- `ConnectorBridgeRuntime` has `process_cycle()`, `trigger_checkpoint()`, `recover()`
- `DagCheckpointCoordinator` handles barrier alignment
- `DagRecoveryManager` handles snapshot persistence
- Source/Sink bridges handle offset capture and epoch management

The main work is wiring `ConnectorBridgeRuntime` into `start_connector_pipeline()`
and ensuring stream subscription delivery still works.

### Checkpoint Storage

```
{storage_dir}/
├── checkpoints/
│   ├── epoch_000042/
│   │   ├── metadata.json        # epoch, timestamp, source list
│   │   ├── source_offsets.json  # per-source, per-partition offsets
│   │   ├── operator_state.bin   # serialized operator state (rkyv)
│   │   └── watermarks.json      # per-source watermark positions
│   └── epoch_000041/
│       └── ...
├── wal/
│   └── ...
└── latest -> checkpoints/epoch_000042  # symlink to latest
```

Retain `max_retained` checkpoints (default 3). Delete older ones after
successful new checkpoint.

### PipelineCheckpoint Type

```rust
pub struct PipelineCheckpoint {
    pub epoch: u64,
    pub timestamp_ms: u64,
    pub source_offsets: HashMap<String, SourceCheckpoint>,
    pub operator_states: HashMap<String, Vec<u8>>,
    pub watermarks: HashMap<String, i64>,
    pub sink_epochs: HashMap<String, u64>,
}
```

## Test Plan

| Module | Tests | What |
|--------|-------|------|
| `pipeline_checkpoint` | 5 | Serialize/deserialize, multi-source offsets, operator state |
| `recovery` | 6 | Startup recovery, source seek, operator restore, sink rollback |
| `periodic_trigger` | 4 | Interval triggering, manual trigger, disabled checkpoint |
| `integration` | 5 | Full Kafka pipeline: run, checkpoint, kill, recover, verify no duplicates |
| `exactly_once` | 3 | Sink transaction commit, rollback on failure, epoch sequencing |

## Files

- `crates/laminar-db/src/db.rs` — Wire checkpoint into `start_connector_pipeline()`
- `crates/laminar-db/src/pipeline_checkpoint.rs` — NEW: PipelineCheckpoint type, persistence
- `crates/laminar-db/src/connector_manager.rs` — Checkpoint-aware source/sink management
- `crates/laminar-connectors/src/bridge/runtime.rs` — Integration point (if Option B)

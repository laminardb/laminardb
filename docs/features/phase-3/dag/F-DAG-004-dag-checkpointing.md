# F-DAG-004: DAG Checkpointing

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-DAG-004 |
| **Status** | 📝 Draft |
| **Priority** | P1 |
| **Phase** | 3 |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-DAG-003 (Executor), F022 (Incremental Checkpointing), F063 (Changelog), F062 (Per-Core WAL) |
| **Blocks** | F-DAG-007 (Performance Validation) |
| **Owner** | TBD |
| **Created** | 2026-01-30 |
| **Crate** | `laminar-core` |
| **Module** | `laminar-core/src/dag/checkpoint.rs`, `laminar-core/src/dag/recovery.rs` |

## Summary

Chandy-Lamport asynchronous barrier snapshot algorithm adapted for DAG topologies. Checkpoint barriers are injected at source nodes (Ring 1) and flow through DAG edges alongside data events. At fan-in nodes, barriers are aligned before snapshotting. Includes recovery manager for parallel state restoration from checkpoints.

## Goals

- `CheckpointBarrier` type (checkpoint_id, timestamp, barrier_type)
- `BarrierType::Aligned` (Chandy-Lamport) and `BarrierType::Unaligned` (Flink 1.11+)
- `BarrierAligner` for fan-in nodes: buffer data from aligned channels, trigger snapshot when all inputs aligned
- `DagCheckpointCoordinator` (Ring 1): trigger_checkpoint(), on_node_snapshot_complete(), finalize_checkpoint()
- `CheckpointProgress` tracking: completed_nodes, pending_nodes, node_snapshots, source_offsets
- `DagCheckpointConfig`: interval, barrier_type, alignment_timeout, incremental, max_concurrent
- `DagRecoveryManager`: load latest checkpoint, restore operator state per node, return source offsets
- Integration with `IncrementalCheckpointManager` (F022) for state persistence
- Integration with `ChangelogBuffer` (F063) for delta tracking

## Non-Goals

- Unaligned barrier implementation (defer to future - aligned first)
- Distributed checkpoint coordination across nodes
- Checkpoint storage backends (S3/GCS) - use local FS first

## Technical Design

See [Full Design Spec, Sections 9-10](../F-DAG-001-dag-pipeline.md#9-dag-aware-checkpoint-coordination) for detailed implementation.

### Barrier Flow

```
Source_1 ──[barrier]──> Op_A ──[barrier]──> Merge ──[barrier]──> Sink
Source_2 ──[barrier]──> Op_B ──[barrier]──┘
                                          ^
                                    BarrierAligner waits
                                    for both barriers
```

### Recovery Sequence

1. Load latest valid checkpoint (metadata.json)
2. Restore operator state for each node (parallel)
3. Return source offsets for exactly-once replay (F023)

### Module Structure

```
crates/laminar-core/src/dag/
+-- checkpoint.rs   # CheckpointBarrier, BarrierAligner, DagCheckpointCoordinator
+-- recovery.rs     # DagRecoveryManager, RecoveryConfig
```

## Test Plan

### Unit Tests (12+)

- [ ] `test_barrier_aligner_single_input` - Immediate alignment
- [ ] `test_barrier_aligner_two_inputs` - Wait for both barriers
- [ ] `test_barrier_aligner_three_inputs` - Wait for all three
- [ ] `test_barrier_aligner_partial` - One barrier arrives, not yet aligned
- [ ] `test_barrier_aligner_cleanup` - complete_checkpoint clears state
- [ ] `test_coordinator_trigger` - Inject barriers at source nodes
- [ ] `test_coordinator_progress` - Track node completion
- [ ] `test_coordinator_finalize` - All nodes complete, checkpoint finalized
- [ ] `test_coordinator_partial_completion` - Some nodes complete, not finalized
- [ ] `test_checkpoint_config_defaults` - Verify default config values
- [ ] `test_recovery_no_checkpoint` - No checkpoint found, error
- [ ] `test_recovery_restore_state` - Restore operator state from checkpoint

### Integration Tests

- [ ] `test_checkpoint_linear_dag` - Checkpoint/recover linear DAG
- [ ] `test_checkpoint_fan_out_dag` - Barrier propagation through fan-out
- [ ] `test_checkpoint_diamond_dag` - Barrier alignment at fan-in + fan-out

## Completion Checklist

- [ ] `CheckpointBarrier`, `BarrierAligner` implemented
- [ ] `DagCheckpointCoordinator` implemented
- [ ] `DagRecoveryManager` implemented
- [ ] Integration with F022 `IncrementalCheckpointManager`
- [ ] 12+ unit tests passing
- [ ] Integration tests passing
- [ ] `cargo clippy` clean

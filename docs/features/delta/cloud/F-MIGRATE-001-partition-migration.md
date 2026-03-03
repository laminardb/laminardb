# F-MIGRATE-001: Partition Migration Protocol

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-MIGRATE-001 |
| **Status** | Done |
| **Priority** | P1 |
| **Phase** | 7c |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-DISAGG-001, F-XAGG-004 |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-03-02 |
| **Crate** | `laminar-core` |
| **Module** | `crates/laminar-core/src/delta/partition/migration.rs`, `coordination/orchestrator.rs` |

## Summary

Safe partition ownership transfer during scale-out/scale-in with epoch fencing. No data shuffle: new owner reads state from S3 (disaggregated backend). Old owner's partition guard is invalidated by epoch bump.

## Goals

- `MigrationProtocol`: freeze partition -> flush changelog to S3 -> bump epoch -> transfer ownership via Raft -> new owner loads from S3
- Zombie fencing: old owner's partition guard invalidated by epoch bump
- `DeltaManager::rebalance()`: triggered on membership change
- Consistent hash ring update -> compute migration plan -> execute serially (one partition at a time)

## Non-Goals

- Parallel partition migration (serial is safer, bounds recovery time)
- Cross-region migration

## Technical Design

### Migration Flow

```
1. Raft leader detects membership change (join/leave/crash)
2. Compute new consistent hash assignment
3. For each partition to migrate:
   a. Source node: freeze partition (stop accepting writes)
   b. Source node: flush changelog to S3
   c. Source node: write final checkpoint
   d. Raft: commit ownership transfer (epoch bump)
   e. Target node: load state from S3 checkpoint
   f. Target node: activate partition guard with new epoch
   g. Source node: release old partition guard
4. Verify all partitions assigned and active
```

### Core Types

- **`MigrationError`** — enum: `PartitionNotFound`, `WrongOwner`, `StaleEpoch`, `ExecutorFull`, `NoActiveNodes`, `AlreadyTerminal`
- **`MigrationAction`** — caller-facing action for each phase (DrainPartition, CreateCheckpoint, UploadCheckpoint, RaftRelease, DownloadCheckpoint, RestoreState, SeekSources, Activate, None)
- **`PhaseResult`** — success (with optional checkpoint path) or failure
- **`MigrationProtocol`** — orchestrates the migration flow, generates actions per phase
- **`MigrationPlan`** — ordered list of `MigrationTask`s from a rebalance operation
- **`MigrationPhase`** — 11-state FSM: Planned → Draining → Checkpointing → Uploading → Released → Downloading → Restoring → Seeking → Active | Failed | Cancelled

### Caller-Driven Protocol

The protocol produces `MigrationAction` values telling the caller what to do (actual I/O is external). The caller reports `PhaseResult` to advance:

```rust
let protocol = MigrationProtocol::new();
let mut task = MigrationTask::new(0, NodeId(1), NodeId(2), 1, 2);

loop {
    let action = protocol.next_action(&task);
    match action {
        MigrationAction::None => break,
        action => {
            // Caller performs the action (drain, checkpoint, S3, Raft, etc.)
            let result = perform_action(action);
            protocol.complete_phase(&mut task, result)?;
        }
    }
}
```

### DeltaManager Integration

- **`rebalance(metadata, nodes)`** — computes `MigrationPlan` using `ConsistentHashAssigner`
- **`should_rebalance(event)`** — determines if a membership event warrants rebalancing
- **`apply_migration_result()`** — updates `PartitionGuardSet` (removes source guard, inserts target guard)
- **`transition(phase)`** — validates lifecycle phase transitions (Discovering → FormingRaft → WaitingForAssignment → RestoringPartitions → Active → Draining → Shutdown)

### Epoch Fencing

- Each migration bumps the epoch: `to_epoch = cluster_epoch + task_index + 1`
- `MigrationTask::release_entry()` generates `MetadataLogEntry::ReleasePartition`
- `MigrationTask::acquire_entry()` generates `MetadataLogEntry::AcquirePartition`
- `validate()` checks epoch monotonicity and owner correctness before migration starts
- Old owner's `PartitionGuard` is fenced via `update_epoch()` then removed

## API

```rust
// Plan rebalance
let plan = protocol.plan_rebalance(&metadata, &nodes, &constraints)?;

// Serial execution
for task in plan.tasks {
    executor.submit(task)?;
    // Drive through phases...
}

// Raft entry generation
let (release, acquire) = MigrationProtocol::raft_entries(&task);
metadata.apply(&release);
metadata.apply(&acquire);

// Guard management on migration complete
manager.apply_migration_result(partition_id, from_node, to_node, new_epoch);
```

## Files Modified

| File | Change |
|------|--------|
| `crates/laminar-core/src/delta/partition/migration.rs` | Extended — `MigrationProtocol`, `MigrationPlan`, `MigrationAction`, `PhaseResult`, `MigrationError`, epoch fencing |
| `crates/laminar-core/src/delta/coordination/orchestrator.rs` | Extended — `rebalance()`, `should_rebalance()`, `apply_migration_result()`, `transition()`, lifecycle phases |
| `crates/laminar-core/src/delta/partition/mod.rs` | Updated re-exports |
| `crates/laminar-core/src/delta/coordination/metadata.rs` | Added `PartialEq, Eq` derive to `MetadataLogEntry` |
| `crates/laminar-core/src/delta/partition/assignment.rs` | Added `Debug` derive to `ConsistentHashAssigner` |
| `docs/features/delta/cloud/F-MIGRATE-001-partition-migration.md` | Updated |
| `docs/features/delta/INDEX.md` | Updated |

## Test Plan

- Phase FSM: advance through all 11 phases, fail, cancel
- Phase sides: source-side vs target-side classification
- Raft entry generation: release + acquire entries match task params
- Executor: max concurrent, cleanup terminal, get_mut, has_active, active_count
- Protocol: next_action for all phases, complete_phase success/failure/terminal
- Full migration flow: walk through all 8 phase transitions successfully
- Rebalance: add node (partitions move), remove node (all move), stable cluster (no moves), epoch monotonic, filter non-active nodes
- Validation: wrong owner, stale epoch, unassigned source
- Metadata integration: release → acquire cycle, epoch fencing rejects stale writes
- Guard integration: source loses guard, target gains guard, neither unaffected
- Full cycle: rebalance → Raft entries → guard updates (source + target managers)
- Lifecycle: valid transitions, invalid transitions, skip restore, operational/terminal checks

## Completion Checklist

- [x] `MigrationProtocol` implemented (plan_rebalance, next_action, complete_phase, validate)
- [x] `MigrationPlan` with ordered tasks and epoch allocation
- [x] `MigrationAction` + `PhaseResult` caller-driven protocol
- [x] `MigrationError` error types
- [x] `DeltaManager::rebalance()` wired via `ConsistentHashAssigner`
- [x] `DeltaManager::should_rebalance()` for membership events
- [x] `DeltaManager::apply_migration_result()` for guard management
- [x] `DeltaManager::transition()` lifecycle phase transitions
- [x] Epoch fencing: release/acquire entry generation, validation
- [x] Metadata integration: release → acquire cycle works correctly
- [x] Serial migration execution via `MigrationExecutor`
- [x] 53 unit tests + 1 doc-test
- [x] Feature INDEX.md updated

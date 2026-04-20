#![allow(clippy::disallowed_types)]
//! End-to-end checkpoint recovery tests.
//!
//! Tests the full checkpoint → shutdown → restart → recovery cycle
//! using the unified checkpoint system.

use std::collections::HashMap;

use laminar_db::checkpoint_coordinator::{
    CheckpointConfig, CheckpointCoordinator, CheckpointRequest,
};
use laminar_db::recovery_manager::RecoveryManager;
use laminar_storage::checkpoint_manifest::{
    CheckpointManifest, ConnectorCheckpoint, OperatorCheckpoint,
};
use laminar_storage::checkpoint_store::{CheckpointStore, FileSystemCheckpointStore};

fn make_store(dir: &std::path::Path) -> FileSystemCheckpointStore {
    FileSystemCheckpointStore::new(dir, 5)
}

async fn make_coordinator(dir: &std::path::Path) -> CheckpointCoordinator {
    let store = Box::new(make_store(dir));
    CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap()
}

// ── Scenario 1: Happy path ──

#[tokio::test]
async fn test_happy_path_checkpoint_and_recovery() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Process and checkpoint
    let mut coord = make_coordinator(dir.path()).await;

    // Perform checkpoint with operator state
    let mut ops = HashMap::new();
    ops.insert(
        "window-agg".into(),
        bytes::Bytes::from_static(b"accumulated-state"),
    );

    let result = coord
        .checkpoint(CheckpointRequest {
            operator_states: ops,
            watermark: Some(5000),
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();

    assert!(result.success);
    assert_eq!(result.checkpoint_id, 1);
    assert_eq!(result.epoch, 1);

    // Phase 2: "Restart" — new coordinator with same store
    let store = make_store(dir.path());
    let mgr = RecoveryManager::new(&store);

    let manifest = mgr.load_latest().await.unwrap().unwrap();
    assert_eq!(manifest.checkpoint_id, 1);
    assert_eq!(manifest.epoch, 1);
    assert_eq!(manifest.watermark, Some(5000));

    // Verify operator state
    let op = manifest.operator_states.get("window-agg").unwrap();
    assert_eq!(op.decode_inline().unwrap(), b"accumulated-state");
}

// ── Scenario 2: Recovery with no prior checkpoint (fresh start) ──

#[tokio::test]
async fn test_recovery_fresh_start() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let mgr = RecoveryManager::new(&store);

    let result = mgr.load_latest().await.unwrap();
    assert!(result.is_none(), "fresh start should return None");
}

// ── Scenario 3: Multiple checkpoints, recover latest ──

#[tokio::test]
async fn test_recover_latest_of_multiple_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    // Create 3 checkpoints
    for i in 1..=3 {
        let mut m = CheckpointManifest::new(i, i);
        m.watermark = Some(i as i64 * 1000);
        store.save(&m).await.unwrap();
    }

    let mgr = RecoveryManager::new(&store);
    let manifest = mgr.load_latest().await.unwrap().unwrap();

    // Should recover the latest (checkpoint 3)
    assert_eq!(manifest.epoch, 3);
    assert_eq!(manifest.watermark, Some(3000));
}

// ── Scenario 4: Checkpoint with source offsets → recovery restores them ──

#[tokio::test]
async fn test_checkpoint_source_offsets_round_trip() {
    let dir = tempfile::tempdir().unwrap();

    // Create checkpoint with source offsets
    let store = make_store(dir.path());
    let mut manifest = CheckpointManifest::new(1, 5);
    manifest.source_offsets.insert(
        "kafka-trades".into(),
        ConnectorCheckpoint {
            offsets: HashMap::from([
                ("partition-0".into(), "1234".into()),
                ("partition-1".into(), "5678".into()),
            ]),
            epoch: 5,
            metadata: HashMap::from([("topic".into(), "trades".into())]),
        },
    );
    manifest.source_offsets.insert(
        "pg-orders".into(),
        ConnectorCheckpoint {
            offsets: HashMap::from([("lsn".into(), "0/ABCDEF".into())]),
            epoch: 5,
            metadata: HashMap::from([("slot".into(), "laminar_slot".into())]),
        },
    );
    store.save(&manifest).await.unwrap();

    // Recover and verify offsets
    let mgr = RecoveryManager::new(&store);
    let manifest = mgr.load_latest().await.unwrap().unwrap();

    let kafka = manifest.source_offsets.get("kafka-trades").unwrap();
    assert_eq!(kafka.offsets.get("partition-0"), Some(&"1234".into()));
    assert_eq!(kafka.offsets.get("partition-1"), Some(&"5678".into()));
    assert_eq!(kafka.metadata.get("topic"), Some(&"trades".into()));

    let pg = manifest.source_offsets.get("pg-orders").unwrap();
    assert_eq!(pg.offsets.get("lsn"), Some(&"0/ABCDEF".into()));
}

// ── Scenario 5: Operator state recovery ──

#[tokio::test]
async fn test_operator_state_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    // Simulate operator state persistence
    let mut manifest = CheckpointManifest::new(1, 10);
    let executor_state = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
    let filter_state = vec![0xDE, 0xAD, 0xBE, 0xEF];

    manifest.operator_states.insert(
        "stream_executor".into(),
        OperatorCheckpoint::inline(&executor_state),
    );
    manifest
        .operator_states
        .insert("filter".into(), OperatorCheckpoint::inline(&filter_state));
    store.save(&manifest).await.unwrap();

    // Recover
    let mgr = RecoveryManager::new(&store);
    let manifest = mgr.load_latest().await.unwrap().unwrap();

    assert_eq!(manifest.operator_states.len(), 2);

    let w = manifest.operator_states.get("stream_executor").unwrap();
    assert_eq!(w.decode_inline().unwrap(), executor_state);

    let f = manifest.operator_states.get("filter").unwrap();
    assert_eq!(f.decode_inline().unwrap(), filter_state);
}

// ── Scenario 7: Table store checkpoint path ──

#[tokio::test]
async fn test_table_store_checkpoint_path_recovery() {
    let dir = tempfile::tempdir().unwrap();

    let mut coord = make_coordinator(dir.path()).await;
    let result = coord
        .checkpoint(CheckpointRequest {
            table_store_checkpoint_path: Some("/data/rocksdb_cp_001".into()),
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();

    assert!(result.success);

    // Recover
    let store = make_store(dir.path());
    let mgr = RecoveryManager::new(&store);
    let manifest = mgr.load_latest().await.unwrap().unwrap();

    assert_eq!(
        manifest.table_store_checkpoint_path.as_deref(),
        Some("/data/rocksdb_cp_001")
    );
}

// ── Scenario 8: Coordinator resumes epoch from stored checkpoint ──

#[tokio::test]
async fn test_coordinator_resumes_epoch_after_recovery() {
    let dir = tempfile::tempdir().unwrap();

    // First run: create two checkpoints
    {
        let mut coord = make_coordinator(dir.path()).await;
        coord
            .checkpoint(CheckpointRequest {
                watermark: Some(1000),
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();
        coord
            .checkpoint(CheckpointRequest {
                watermark: Some(2000),
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();

        assert_eq!(coord.epoch(), 3); // Started at 1, incremented twice
        assert_eq!(coord.next_checkpoint_id(), 3);
    }

    // "Restart": new coordinator picks up from stored state
    let coord2 = make_coordinator(dir.path()).await;
    assert_eq!(coord2.epoch(), 3);
    assert_eq!(coord2.next_checkpoint_id(), 3);
}

// ── Scenario 10: Table offsets round-trip ──

#[tokio::test]
async fn test_table_offsets_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    let mut manifest = CheckpointManifest::new(1, 1);
    manifest.table_offsets.insert(
        "exchange_rates".into(),
        ConnectorCheckpoint {
            offsets: HashMap::from([("lsn".into(), "0/FF00".into())]),
            epoch: 1,
            metadata: HashMap::new(),
        },
    );
    store.save(&manifest).await.unwrap();

    let mgr = RecoveryManager::new(&store);
    let manifest = mgr.load_latest().await.unwrap().unwrap();

    let table_cp = manifest.table_offsets.get("exchange_rates").unwrap();
    assert_eq!(table_cp.offsets.get("lsn"), Some(&"0/FF00".into()));
}

// ── Scenario 11: Checkpoint store prune ──

#[tokio::test]
async fn test_checkpoint_store_prune_keeps_latest() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    // Create 5 checkpoints
    for i in 1..=5 {
        let m = CheckpointManifest::new(i, i);
        store.save(&m).await.unwrap();
    }

    let all = store.list().await.unwrap();
    assert_eq!(all.len(), 5);

    // Prune to keep 2
    let pruned = store.prune(2).await.unwrap();
    assert_eq!(pruned, 3);

    let remaining = store.list().await.unwrap();
    assert_eq!(remaining.len(), 2);

    // Latest should still be loadable
    let latest = store.load_latest().await.unwrap().unwrap();
    assert_eq!(latest.checkpoint_id, 5);
}

// ── Scenario 12: Checkpoint manifest JSON round-trip ──

#[test]
fn test_manifest_full_round_trip() {
    let mut manifest = CheckpointManifest::new(42, 100);
    manifest.watermark = Some(999_999);
    manifest.table_store_checkpoint_path = Some("/tmp/cp".into());

    manifest.source_offsets.insert(
        "kafka".into(),
        ConnectorCheckpoint {
            offsets: HashMap::from([("p0".into(), "100".into())]),
            epoch: 100,
            metadata: HashMap::new(),
        },
    );
    manifest.sink_epochs.insert("pg-sink".into(), 99);
    manifest
        .operator_states
        .insert("0".into(), OperatorCheckpoint::inline(b"state-bytes"));
    manifest.source_watermarks.insert("kafka".into(), 999_000);

    // Serialize
    let json = serde_json::to_string_pretty(&manifest).unwrap();

    // Deserialize
    let restored: CheckpointManifest = serde_json::from_str(&json).unwrap();

    assert_eq!(restored.checkpoint_id, 42);
    assert_eq!(restored.epoch, 100);
    assert_eq!(restored.watermark, Some(999_999));
    assert_eq!(
        restored.table_store_checkpoint_path.as_deref(),
        Some("/tmp/cp")
    );
    assert_eq!(restored.sink_epochs.get("pg-sink"), Some(&99));
    assert_eq!(
        restored
            .operator_states
            .get("0")
            .unwrap()
            .decode_inline()
            .unwrap(),
        b"state-bytes"
    );
    assert_eq!(*restored.source_watermarks.get("kafka").unwrap(), 999_000);
}

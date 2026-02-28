//! End-to-end checkpoint recovery tests.
//!
//! Tests the full checkpoint → shutdown → restart → recovery cycle
//! using the unified checkpoint system.

use std::collections::HashMap;
use std::sync::Arc;

use laminar_db::checkpoint_coordinator::{CheckpointConfig, CheckpointCoordinator};
use laminar_db::recovery_manager::RecoveryManager;
use laminar_storage::checkpoint_manifest::{
    CheckpointManifest, ConnectorCheckpoint, OperatorCheckpoint,
};
use laminar_storage::checkpoint_store::{CheckpointStore, FileSystemCheckpointStore};

fn make_store(dir: &std::path::Path) -> FileSystemCheckpointStore {
    FileSystemCheckpointStore::new(dir, 5)
}

fn make_coordinator(dir: &std::path::Path) -> CheckpointCoordinator {
    let store = Box::new(make_store(dir));
    CheckpointCoordinator::new(CheckpointConfig::default(), store)
}

// ── Scenario 1: Happy path ──

#[tokio::test]
async fn test_happy_path_checkpoint_and_recovery() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Process and checkpoint
    let mut coord = make_coordinator(dir.path());

    // Register a mock source
    let source = laminar_connectors::testing::MockSourceConnector::new();
    let source_handle: Arc<
        tokio::sync::Mutex<Box<dyn laminar_connectors::connector::SourceConnector>>,
    > = Arc::new(tokio::sync::Mutex::new(Box::new(source)));
    coord.register_source("trades", Arc::clone(&source_handle), true);

    // Perform checkpoint with operator state
    let mut ops = HashMap::new();
    ops.insert("window-agg".into(), b"accumulated-state".to_vec());

    let result = coord
        .checkpoint(ops, Some(5000), 1024, vec![100, 200], None, HashMap::new(), None)
        .await
        .unwrap();

    assert!(result.success);
    assert_eq!(result.checkpoint_id, 1);
    assert_eq!(result.epoch, 1);

    // Phase 2: "Restart" — new coordinator with same store
    let store = make_store(dir.path());
    let mgr = RecoveryManager::new(&store);

    let manifest = mgr.load_latest().unwrap().unwrap();
    assert_eq!(manifest.checkpoint_id, 1);
    assert_eq!(manifest.epoch, 1);
    assert_eq!(manifest.watermark, Some(5000));
    assert_eq!(manifest.wal_position, 1024);
    assert_eq!(manifest.per_core_wal_positions, vec![100, 200]);

    // Verify operator state
    let op = manifest.operator_states.get("window-agg").unwrap();
    assert_eq!(op.decode_inline().unwrap(), b"accumulated-state");
}

// ── Scenario 2: Recovery with no prior checkpoint (fresh start) ──

#[test]
fn test_recovery_fresh_start() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());
    let mgr = RecoveryManager::new(&store);

    let result = mgr.load_latest().unwrap();
    assert!(result.is_none(), "fresh start should return None");
}

// ── Scenario 3: Multiple checkpoints, recover latest ──

#[test]
fn test_recover_latest_of_multiple_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    // Create 3 checkpoints
    for i in 1..=3 {
        let mut m = CheckpointManifest::new(i, i);
        m.watermark = Some(i as i64 * 1000);
        store.save(&m).unwrap();
    }

    let mgr = RecoveryManager::new(&store);
    let manifest = mgr.load_latest().unwrap().unwrap();

    // Should recover the latest (checkpoint 3)
    assert_eq!(manifest.epoch, 3);
    assert_eq!(manifest.watermark, Some(3000));
}

// ── Scenario 4: Checkpoint with source offsets → recovery restores them ──

#[test]
fn test_checkpoint_source_offsets_round_trip() {
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
    store.save(&manifest).unwrap();

    // Recover and verify offsets
    let mgr = RecoveryManager::new(&store);
    let manifest = mgr.load_latest().unwrap().unwrap();

    let kafka = manifest.source_offsets.get("kafka-trades").unwrap();
    assert_eq!(kafka.offsets.get("partition-0"), Some(&"1234".into()));
    assert_eq!(kafka.offsets.get("partition-1"), Some(&"5678".into()));
    assert_eq!(kafka.metadata.get("topic"), Some(&"trades".into()));

    let pg = manifest.source_offsets.get("pg-orders").unwrap();
    assert_eq!(pg.offsets.get("lsn"), Some(&"0/ABCDEF".into()));
}

// ── Scenario 5: Operator state recovery ──

#[test]
fn test_operator_state_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    // Simulate DAG with accumulated window state
    let mut manifest = CheckpointManifest::new(1, 10);
    let window_state = vec![1u8, 2, 3, 4, 5, 6, 7, 8]; // Serialized accumulator
    let filter_state = vec![0xDE, 0xAD, 0xBE, 0xEF];

    manifest
        .operator_states
        .insert("0".into(), OperatorCheckpoint::inline(&window_state));
    manifest
        .operator_states
        .insert("3".into(), OperatorCheckpoint::inline(&filter_state));
    store.save(&manifest).unwrap();

    // Recover
    let mgr = RecoveryManager::new(&store);
    let manifest = mgr.load_latest().unwrap().unwrap();

    // Convert to DAG format
    let dag_states = laminar_db::checkpoint_coordinator::manifest_operators_to_dag_states(
        &manifest.operator_states,
    );

    assert_eq!(dag_states.len(), 2);

    let w = dag_states
        .get(&laminar_core::dag::topology::NodeId(0))
        .unwrap();
    assert_eq!(w.data, window_state);

    let f = dag_states
        .get(&laminar_core::dag::topology::NodeId(3))
        .unwrap();
    assert_eq!(f.data, filter_state);
}

// ── Scenario 6: WAL positions recorded and recovered ──

#[tokio::test]
async fn test_wal_positions_recovery() {
    let dir = tempfile::tempdir().unwrap();

    // Checkpoint with WAL positions
    let mut coord = make_coordinator(dir.path());
    let result = coord
        .checkpoint(
            HashMap::new(),
            Some(42_000),
            8192,
            vec![512, 1024, 256, 768],
            None,
            HashMap::new(),
            None,
        )
        .await
        .unwrap();

    assert!(result.success);

    // Recover
    let store = make_store(dir.path());
    let mgr = RecoveryManager::new(&store);
    let manifest = mgr.load_latest().unwrap().unwrap();

    assert_eq!(manifest.wal_position, 8192);
    assert_eq!(manifest.per_core_wal_positions, vec![512, 1024, 256, 768]);
}

// ── Scenario 7: Table store checkpoint path ──

#[tokio::test]
async fn test_table_store_checkpoint_path_recovery() {
    let dir = tempfile::tempdir().unwrap();

    let mut coord = make_coordinator(dir.path());
    let result = coord
        .checkpoint(
            HashMap::new(),
            None,
            0,
            vec![],
            Some("/data/rocksdb_cp_001".into()),
            HashMap::new(),
            None,
        )
        .await
        .unwrap();

    assert!(result.success);

    // Recover
    let store = make_store(dir.path());
    let mgr = RecoveryManager::new(&store);
    let manifest = mgr.load_latest().unwrap().unwrap();

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
        let mut coord = make_coordinator(dir.path());
        coord
            .checkpoint(HashMap::new(), Some(1000), 0, vec![], None, HashMap::new(), None)
            .await
            .unwrap();
        coord
            .checkpoint(HashMap::new(), Some(2000), 0, vec![], None, HashMap::new(), None)
            .await
            .unwrap();

        assert_eq!(coord.epoch(), 3); // Started at 1, incremented twice
        assert_eq!(coord.next_checkpoint_id(), 3);
    }

    // "Restart": new coordinator picks up from stored state
    let coord2 = make_coordinator(dir.path());
    assert_eq!(coord2.epoch(), 3);
    assert_eq!(coord2.next_checkpoint_id(), 3);
}

// ── Scenario 9: Incremental checkpoint flag ──

#[tokio::test]
async fn test_incremental_checkpoint_flag() {
    let dir = tempfile::tempdir().unwrap();

    let config = CheckpointConfig {
        incremental: true,
        ..CheckpointConfig::default()
    };
    let store = Box::new(make_store(dir.path()));
    let mut coord = CheckpointCoordinator::new(config, store);

    let result = coord
        .checkpoint(HashMap::new(), None, 0, vec![], None, HashMap::new(), None)
        .await
        .unwrap();

    assert!(result.success);

    let store = make_store(dir.path());
    let loaded = store.load_latest().unwrap().unwrap();
    assert!(loaded.is_incremental);
}

// ── Scenario 10: Table offsets round-trip ──

#[test]
fn test_table_offsets_round_trip() {
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
    store.save(&manifest).unwrap();

    let mgr = RecoveryManager::new(&store);
    let manifest = mgr.load_latest().unwrap().unwrap();

    let table_cp = manifest.table_offsets.get("exchange_rates").unwrap();
    assert_eq!(table_cp.offsets.get("lsn"), Some(&"0/FF00".into()));
}

// ── Scenario 11: Checkpoint store prune ──

#[test]
fn test_checkpoint_store_prune_keeps_latest() {
    let dir = tempfile::tempdir().unwrap();
    let store = make_store(dir.path());

    // Create 5 checkpoints
    for i in 1..=5 {
        let m = CheckpointManifest::new(i, i);
        store.save(&m).unwrap();
    }

    let all = store.list().unwrap();
    assert_eq!(all.len(), 5);

    // Prune to keep 2
    let pruned = store.prune(2).unwrap();
    assert_eq!(pruned, 3);

    let remaining = store.list().unwrap();
    assert_eq!(remaining.len(), 2);

    // Latest should still be loadable
    let latest = store.load_latest().unwrap().unwrap();
    assert_eq!(latest.checkpoint_id, 5);
}

// ── Scenario 12: Checkpoint manifest JSON round-trip ──

#[test]
fn test_manifest_full_round_trip() {
    let mut manifest = CheckpointManifest::new(42, 100);
    manifest.watermark = Some(999_999);
    manifest.wal_position = 4096;
    manifest.per_core_wal_positions = vec![10, 20, 30, 40];
    manifest.is_incremental = true;
    manifest.parent_id = Some(41);
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
    assert_eq!(restored.wal_position, 4096);
    assert_eq!(restored.per_core_wal_positions, vec![10, 20, 30, 40]);
    assert!(restored.is_incremental);
    assert_eq!(restored.parent_id, Some(41));
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

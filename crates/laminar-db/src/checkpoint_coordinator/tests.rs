use super::*;
use laminar_core::storage::checkpoint_store::FileSystemCheckpointStore;

async fn make_coordinator(dir: &std::path::Path) -> CheckpointCoordinator {
    let store = Box::new(FileSystemCheckpointStore::new(dir, 3));
    CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap()
}

/// Coordinator whose restorable gate gives up quickly — for tests
/// that exercise a gate *miss* (the default 30s poll would stall
/// the suite).
async fn make_coordinator_with_fast_gate(dir: &std::path::Path) -> CheckpointCoordinator {
    let store = Box::new(FileSystemCheckpointStore::new(dir, 3));
    let config = CheckpointConfig {
        restorable_gate_timeout: Duration::from_millis(250),
        ..CheckpointConfig::default()
    };
    CheckpointCoordinator::new(config, store).await.unwrap()
}

#[tokio::test]
async fn test_coordinator_new() {
    let dir = tempfile::tempdir().unwrap();
    let coord = make_coordinator(dir.path()).await;

    assert_eq!(coord.epoch(), 1);
    assert_eq!(coord.next_checkpoint_id(), 1);
    assert_eq!(coord.phase(), CheckpointPhase::Idle);
}

#[tokio::test]
async fn test_coordinator_resumes_from_stored_checkpoint() {
    let dir = tempfile::tempdir().unwrap();

    // Save a checkpoint manually
    let store = FileSystemCheckpointStore::new(dir.path(), 3);
    let m = CheckpointManifest::new(5, 10);
    store.save(&m).await.unwrap();

    // Coordinator should resume from epoch 11, checkpoint_id 6
    let coord = make_coordinator(dir.path()).await;
    assert_eq!(coord.epoch(), 11);
    assert_eq!(coord.next_checkpoint_id(), 6);
}

#[test]
fn test_checkpoint_phase_display() {
    assert_eq!(CheckpointPhase::Idle.to_string(), "Idle");
    assert_eq!(CheckpointPhase::Snapshotting.to_string(), "Snapshotting");
    assert_eq!(CheckpointPhase::PreCommitting.to_string(), "PreCommitting");
    assert_eq!(CheckpointPhase::Persisting.to_string(), "Persisting");
    assert_eq!(CheckpointPhase::Committing.to_string(), "Committing");
}

#[test]
fn test_source_to_connector_checkpoint() {
    let mut cp = SourceCheckpoint::new(5);
    cp.set_offset("partition-0", "1234");
    cp.set_metadata("topic", "events");

    let cc = source_to_connector_checkpoint(&cp);
    assert_eq!(cc.epoch, 5);
    assert_eq!(cc.offsets.get("partition-0"), Some(&"1234".into()));
    assert_eq!(cc.metadata.get("topic"), Some(&"events".into()));
}

#[test]
fn test_connector_to_source_checkpoint() {
    let cc = ConnectorCheckpoint {
        offsets: HashMap::from([("lsn".into(), "0/ABCD".into())]),
        epoch: 3,
        metadata: HashMap::from([("type".into(), "postgres".into())]),
    };

    let cp = connector_to_source_checkpoint(&cc);
    assert_eq!(cp.epoch(), 3);
    assert_eq!(cp.get_offset("lsn"), Some("0/ABCD"));
    assert_eq!(cp.get_metadata("type"), Some("postgres"));
}

#[tokio::test]
async fn test_stats_initial() {
    let dir = tempfile::tempdir().unwrap();
    let coord = make_coordinator(dir.path()).await;
    let stats = coord.stats();

    assert_eq!(stats.completed, 0);
    assert_eq!(stats.failed, 0);
    assert!(stats.last_duration.is_none());
    assert_eq!(stats.duration_p50_ms, 0);
    assert_eq!(stats.duration_p95_ms, 0);
    assert_eq!(stats.duration_p99_ms, 0);
    assert_eq!(stats.current_phase, CheckpointPhase::Idle);
}

#[tokio::test]
async fn test_checkpoint_no_sources_no_sinks() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let result = coord
        .checkpoint(CheckpointRequest {
            watermark: Some(1000),
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();

    assert!(result.success);
    assert_eq!(result.checkpoint_id, 1);
    assert_eq!(result.epoch, 1);

    // Verify manifest was persisted
    let loaded = coord.store().load_latest().await.unwrap().unwrap();
    assert_eq!(loaded.checkpoint_id, 1);
    assert_eq!(loaded.epoch, 1);
    assert_eq!(loaded.watermark, Some(1000));

    // Second checkpoint should increment
    let result2 = coord
        .checkpoint(CheckpointRequest {
            watermark: Some(2000),
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();

    assert!(result2.success);
    assert_eq!(result2.checkpoint_id, 2);
    assert_eq!(result2.epoch, 2);

    let stats = coord.stats();
    assert_eq!(stats.completed, 2);
    assert_eq!(stats.failed, 0);
}

#[tokio::test]
async fn test_checkpoint_with_operator_states() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let mut ops = HashMap::new();
    ops.insert(
        "window-agg".into(),
        bytes::Bytes::from_static(b"state-data"),
    );
    ops.insert("filter".into(), bytes::Bytes::from_static(b"filter-state"));

    let result = coord
        .checkpoint(CheckpointRequest {
            operator_states: ops,
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();

    assert!(result.success);

    let loaded = coord.store().load_latest().await.unwrap().unwrap();
    assert_eq!(loaded.operator_states.len(), 2);

    let window_op = loaded.operator_states.get("window-agg").unwrap();
    assert_eq!(window_op.decode_inline().unwrap(), b"state-data");
}

#[tokio::test]
async fn test_checkpoint_with_table_store_path() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let result = coord
        .checkpoint(CheckpointRequest {
            table_store_checkpoint_path: Some("/tmp/table_store_cp".into()),
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();

    assert!(result.success);

    let loaded = coord.store().load_latest().await.unwrap().unwrap();
    assert_eq!(
        loaded.table_store_checkpoint_path.as_deref(),
        Some("/tmp/table_store_cp")
    );
}

#[tokio::test]
async fn test_load_latest_manifest_empty() {
    let dir = tempfile::tempdir().unwrap();
    let coord = make_coordinator(dir.path()).await;
    assert!(coord.load_latest_manifest().await.unwrap().is_none());
}

#[tokio::test]
async fn test_coordinator_debug() {
    let dir = tempfile::tempdir().unwrap();
    let coord = make_coordinator(dir.path()).await;
    let debug = format!("{coord:?}");
    assert!(debug.contains("CheckpointCoordinator"));
    assert!(debug.contains("epoch: 1"));
}

#[tokio::test]
async fn test_checkpoint_emits_metrics_on_success() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let registry = prometheus::Registry::new();
    let prom = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
    coord.set_metrics(Arc::clone(&prom));

    let result = coord
        .checkpoint(CheckpointRequest {
            watermark: Some(1000),
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();

    assert!(result.success);
    assert_eq!(prom.checkpoints_completed.get(), 1);
    assert_eq!(prom.checkpoints_failed.get(), 0);
    assert_eq!(prom.checkpoint_epoch.get(), 1);

    // Second checkpoint
    let result2 = coord
        .checkpoint(CheckpointRequest {
            watermark: Some(2000),
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();

    assert!(result2.success);
    assert_eq!(prom.checkpoints_completed.get(), 2);
    assert_eq!(prom.checkpoint_epoch.get(), 2);
}

#[tokio::test]
async fn test_checkpoint_without_metrics() {
    // Verify checkpoint works fine without metrics set
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();

    assert!(result.success);
    // No panics — metrics emission is a no-op
}

#[test]
fn test_histogram_empty() {
    let h = DurationHistogram::new();
    assert_eq!(h.len(), 0);
    assert_eq!(h.percentile(0.50), 0);
    assert_eq!(h.percentile(0.99), 0);
    let (p50, p95, p99) = h.percentiles();
    assert_eq!((p50, p95, p99), (0, 0, 0));
}

#[test]
fn test_histogram_single_sample() {
    let mut h = DurationHistogram::new();
    h.record(Duration::from_millis(42));
    assert_eq!(h.len(), 1);
    // 42ms = 42_000μs
    assert_eq!(h.percentile(0.50), 42_000);
    assert_eq!(h.percentile(0.99), 42_000);
}

#[test]
fn test_histogram_sub_millisecond() {
    let mut h = DurationHistogram::new();
    // 500μs — previously truncated to 0 with as_millis()
    h.record(Duration::from_micros(500));
    assert_eq!(h.percentile(0.50), 500);
    assert_eq!(h.percentile(0.99), 500);
}

#[test]
fn test_histogram_percentiles() {
    let mut h = DurationHistogram::new();
    // Record 1..=100ms in order → 1000..=100_000 μs.
    for i in 1..=100 {
        h.record(Duration::from_millis(i));
    }
    assert_eq!(h.len(), 100);

    let p50 = h.percentile(0.50);
    let p95 = h.percentile(0.95);
    let p99 = h.percentile(0.99);

    // Values in μs: 1000..=100_000
    //   p50 ≈ 50_000, p95 ≈ 95_000, p99 ≈ 99_000
    assert!((49_000..=51_000).contains(&p50), "p50={p50}");
    assert!((94_000..=96_000).contains(&p95), "p95={p95}");
    assert!((98_000..=100_000).contains(&p99), "p99={p99}");
}

#[test]
fn test_histogram_wraps_ring_buffer() {
    let mut h = DurationHistogram::new();
    // Write 150 samples — first 50 are overwritten.
    for i in 1..=150 {
        h.record(Duration::from_millis(i));
    }
    assert_eq!(h.len(), 100);
    assert_eq!(h.count, 150);

    // Only samples 51..=150 remain in the buffer (51_000..=150_000 μs).
    let p50 = h.percentile(0.50);
    assert!((99_000..=101_000).contains(&p50), "p50={p50}");
}

#[tokio::test]
async fn test_sidecar_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
    let config = CheckpointConfig {
        state_inline_threshold: 100, // 100 bytes threshold
        ..CheckpointConfig::default()
    };
    let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();

    // Small state stays inline, large state goes to sidecar
    let mut ops = HashMap::new();
    ops.insert("small".into(), bytes::Bytes::from(vec![0xAAu8; 50]));
    ops.insert("large".into(), bytes::Bytes::from(vec![0xBBu8; 200]));

    let result = coord
        .checkpoint(CheckpointRequest {
            operator_states: ops,
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();
    assert!(result.success);

    // Verify manifest
    let loaded = coord.store().load_latest().await.unwrap().unwrap();
    let small_op = loaded.operator_states.get("small").unwrap();
    assert!(!small_op.external, "small state should be inline");
    assert_eq!(small_op.decode_inline().unwrap(), vec![0xAAu8; 50]);

    let large_op = loaded.operator_states.get("large").unwrap();
    assert!(large_op.external, "large state should be external");
    assert_eq!(large_op.external_length, 200);

    // Verify sidecar file exists and has correct data
    let state_data = coord.store().load_state_data(1).await.unwrap().unwrap();
    assert_eq!(state_data.len(), 200);
    assert!(state_data.iter().all(|&b| b == 0xBB));
}

#[tokio::test]
async fn test_all_inline_no_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
    let config = CheckpointConfig::default(); // 1MB threshold
    let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();

    let mut ops = HashMap::new();
    ops.insert("op1".into(), bytes::Bytes::from_static(b"small-state"));

    let result = coord
        .checkpoint(CheckpointRequest {
            operator_states: ops,
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();
    assert!(result.success);

    // No sidecar file
    assert!(coord.store().load_state_data(1).await.unwrap().is_none());
}

// Durability gate tests.

#[tokio::test]
async fn durability_gate_skipped_when_vnode_set_empty() {
    // With no state backend installed AND empty vnode set, the commit
    // path behaves as before. Regression guard: the durability gate
    // must not change single-instance semantics.
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(result.success, "baseline checkpoint must succeed");
}

#[tokio::test]
async fn bridge_writes_markers_and_gate_passes() {
    use laminar_core::state::InProcessBackend;
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let backend = Arc::new(InProcessBackend::new(4));
    coord.set_state_backend(backend.clone());
    coord.set_vnode_set(vec![0, 1, 2, 3]);

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(result.success, "bridge writes markers → gate passes");
    // Every owned vnode has a marker for the completed epoch.
    for v in 0..4 {
        assert!(
            backend.read_partial(v, 1).await.unwrap().is_some(),
            "bridge should have written marker for vnode {v}",
        );
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn reconcile_announces_abort_when_no_decision_store() {
    // Fallback path: if no decision store is wired (e.g. legacy
    // deployments), absence of a marker == Abort. This is the
    // pre-decision-store behavior preserved for compatibility.
    use laminar_core::cluster::control::{
        BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::NodeId;
    use laminar_core::storage::checkpoint_manifest::SinkCommitStatus;
    use tokio::sync::watch;

    let dir = tempfile::tempdir().unwrap();
    let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
    let mut orphan = CheckpointManifest::new(42, 7);
    orphan
        .sink_commit_statuses
        .insert("kafka_out".into(), SinkCommitStatus::Pending);
    store.save_with_state(&orphan, None).await.unwrap();

    let coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let (_tx, rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
    let mut coord = coord;
    coord.set_cluster_controller(controller);

    coord.reconcile_prepared_on_init().await;

    let raw = kv.read_from(self_id, ANNOUNCEMENT_KEY).await.unwrap();
    let ann: BarrierAnnouncement = serde_json::from_str(&raw).unwrap();
    assert_eq!(ann.phase, Phase::Abort);
    assert_eq!(ann.epoch, 7);
    assert_eq!(ann.checkpoint_id, 42);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn reconcile_announces_commit_when_marker_present() {
    use laminar_core::cluster::control::{
        BarrierAnnouncement, CheckpointDecisionStore, ClusterController, ClusterKv, InMemoryKv,
        Phase, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::NodeId;
    use laminar_core::storage::checkpoint_manifest::SinkCommitStatus;
    use object_store::local::LocalFileSystem;
    use tokio::sync::watch;

    let ckpt_dir = tempfile::tempdir().unwrap();
    let decision_dir = tempfile::tempdir().unwrap();
    let store = Box::new(FileSystemCheckpointStore::new(ckpt_dir.path(), 3));
    let mut orphan = CheckpointManifest::new(42, 7);
    orphan
        .sink_commit_statuses
        .insert("kafka_out".into(), SinkCommitStatus::Pending);
    store.save_with_state(&orphan, None).await.unwrap();

    let decision_os: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(decision_dir.path()).unwrap());
    let decision_store = Arc::new(CheckpointDecisionStore::new(decision_os));
    decision_store.record_committed(7).await.unwrap();

    let coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let (_tx, rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
    let mut coord = coord;
    coord.set_cluster_controller(controller);
    coord.set_decision_store(decision_store);

    coord.reconcile_prepared_on_init().await;

    let raw = kv.read_from(self_id, ANNOUNCEMENT_KEY).await.unwrap();
    let ann: BarrierAnnouncement = serde_json::from_str(&raw).unwrap();
    assert_eq!(ann.phase, Phase::Commit);
    assert_eq!(ann.epoch, 7);
    assert_eq!(ann.checkpoint_id, 42);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn reconcile_announces_abort_when_marker_missing() {
    // Decision store is wired but has no marker for this epoch —
    // the "leader crashed before commit point" case.
    use laminar_core::cluster::control::{
        BarrierAnnouncement, CheckpointDecisionStore, ClusterController, ClusterKv, InMemoryKv,
        Phase, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::NodeId;
    use laminar_core::storage::checkpoint_manifest::SinkCommitStatus;
    use object_store::local::LocalFileSystem;
    use tokio::sync::watch;

    let ckpt_dir = tempfile::tempdir().unwrap();
    let decision_dir = tempfile::tempdir().unwrap();
    let store = Box::new(FileSystemCheckpointStore::new(ckpt_dir.path(), 3));
    let mut orphan = CheckpointManifest::new(11, 3);
    orphan
        .sink_commit_statuses
        .insert("out".into(), SinkCommitStatus::Pending);
    store.save_with_state(&orphan, None).await.unwrap();

    let decision_os: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(decision_dir.path()).unwrap());
    let decision_store = Arc::new(CheckpointDecisionStore::new(decision_os));

    let coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let (_tx, rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
    let mut coord = coord;
    coord.set_cluster_controller(controller);
    coord.set_decision_store(decision_store);

    coord.reconcile_prepared_on_init().await;

    let raw = kv.read_from(self_id, ANNOUNCEMENT_KEY).await.unwrap();
    let ann: BarrierAnnouncement = serde_json::from_str(&raw).unwrap();
    assert_eq!(ann.phase, Phase::Abort);
    assert_eq!(ann.epoch, 3);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn reconcile_silent_when_manifest_clean() {
    use laminar_core::cluster::control::{
        ClusterController, ClusterKv, InMemoryKv, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::NodeId;
    use laminar_core::storage::checkpoint_manifest::SinkCommitStatus;
    use tokio::sync::watch;

    let dir = tempfile::tempdir().unwrap();
    let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
    let mut clean = CheckpointManifest::new(5, 3);
    clean
        .sink_commit_statuses
        .insert("out".into(), SinkCommitStatus::Committed);
    store.save_with_state(&clean, None).await.unwrap();

    let coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let (_tx, rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
    let mut coord = coord;
    coord.set_cluster_controller(controller);

    coord.reconcile_prepared_on_init().await;

    // No announcement emitted.
    assert!(kv.read_from(self_id, ANNOUNCEMENT_KEY).await.is_none());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_checkpoint_commits_on_leader_commit() {
    use laminar_core::cluster::control::{
        BarrierAck, BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase, ACK_KEY,
        ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
    use tokio::sync::watch;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let leader_id = NodeId(1);
    let follower_id = NodeId(7);

    // Follower's KV sees both its own writes and a seeded view of the
    // leader's announcements. `members_rx` includes the leader so
    // `current_leader()` picks the lowest id (the leader, not self).
    let kv = Arc::new(InMemoryKv::new(follower_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let leader_info = NodeInfo {
        id: leader_id,
        name: "leader".into(),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let (_tx, rx) = watch::channel(vec![leader_info]);
    let controller = Arc::new(ClusterController::new(follower_id, kv_trait, None, rx));
    coord.set_cluster_controller(controller);

    // Leader has already announced PREPARE and COMMIT (simulates
    // a fast-gossip scenario; follower sees both on its first poll).
    let prepare_json = serde_json::to_string(&BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        phase: Phase::Prepare,
        flags: 0,
        min_watermark_ms: None,
    })
    .unwrap();
    let commit_json = serde_json::to_string(&BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        phase: Phase::Commit,
        flags: 0,
        min_watermark_ms: None,
    })
    .unwrap();
    // Overwrite the prepare with commit — observe_barrier reads the
    // latest value. Real gossip shows both in order; for the unit
    // test, landing on Commit is enough for the decision loop.
    kv.seed(leader_id, ANNOUNCEMENT_KEY, prepare_json);
    kv.seed(leader_id, ANNOUNCEMENT_KEY, commit_json);

    let ann = BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        phase: Phase::Prepare,
        flags: 0,
        min_watermark_ms: None,
    };
    let committed = coord
        .follower_checkpoint(CheckpointRequest::default(), ann, Duration::from_secs(2))
        .await
        .unwrap();
    assert!(committed, "follower should commit on leader's Commit");

    // Follower's ack landed in its own KV.
    let ack_raw = kv.read_from(follower_id, ACK_KEY).await.unwrap();
    let ack: BarrierAck = serde_json::from_str(&ack_raw).unwrap();
    assert_eq!(ack.epoch, 1);
    assert!(ack.ok, "prepare succeeded, ack should be ok");

    // Follower's manifest is on disk at the leader's epoch.
    let stored = coord.store().load_latest().await.unwrap().unwrap();
    assert_eq!(stored.epoch, 1);
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_checkpoint_rolls_back_on_leader_abort() {
    use laminar_core::cluster::control::{
        BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
    use tokio::sync::watch;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let leader_id = NodeId(1);
    let follower_id = NodeId(9);
    let kv = Arc::new(InMemoryKv::new(follower_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let leader_info = NodeInfo {
        id: leader_id,
        name: "leader".into(),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let (_tx, rx) = watch::channel(vec![leader_info]);
    let controller = Arc::new(ClusterController::new(follower_id, kv_trait, None, rx));
    coord.set_cluster_controller(controller);

    let abort_json = serde_json::to_string(&BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        phase: Phase::Abort,
        flags: 0,
        min_watermark_ms: None,
    })
    .unwrap();
    kv.seed(leader_id, ANNOUNCEMENT_KEY, abort_json);

    let ann = BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        phase: Phase::Prepare,
        flags: 0,
        min_watermark_ms: None,
    };
    let committed = coord
        .follower_checkpoint(CheckpointRequest::default(), ann, Duration::from_secs(2))
        .await
        .unwrap();
    assert!(!committed, "follower should roll back on leader's Abort");
}

/// KV that records every announcement written, preserving order —
/// the single-slot `InMemoryKv` only keeps the latest.
#[cfg(feature = "cluster")]
struct RecordingKv {
    inner: laminar_core::cluster::control::InMemoryKv,
    announcements: Arc<parking_lot::Mutex<Vec<String>>>,
}

#[cfg(feature = "cluster")]
#[async_trait::async_trait]
impl laminar_core::cluster::control::ClusterKv for RecordingKv {
    async fn write(&self, key: &str, value: String) {
        if key == laminar_core::cluster::control::ANNOUNCEMENT_KEY {
            self.announcements.lock().push(value.clone());
        }
        self.inner.write(key, value).await;
    }
    async fn read_from(
        &self,
        who: laminar_core::cluster::discovery::NodeId,
        key: &str,
    ) -> Option<String> {
        self.inner.read_from(who, key).await
    }
    async fn scan(&self, key: &str) -> Vec<(laminar_core::cluster::discovery::NodeId, String)> {
        self.inner.scan(key).await
    }
    async fn scan_prefix(
        &self,
        prefix: &str,
    ) -> Vec<(laminar_core::cluster::discovery::NodeId, String, String)> {
        self.inner.scan_prefix(prefix).await
    }
}

/// Two-level completion: the leader must announce `Aligned`
/// (the pipeline resume gate) after the capture quorum and *before*
/// the durable tail's `Commit`.
#[cfg(feature = "cluster")]
#[tokio::test]
async fn leader_announces_aligned_between_prepare_and_commit() {
    use laminar_core::cluster::control::{
        BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase,
    };
    use laminar_core::cluster::discovery::NodeId;
    use tokio::sync::watch;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let self_id = NodeId(1);
    let announcements = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let kv: Arc<dyn ClusterKv> = Arc::new(RecordingKv {
        inner: InMemoryKv::new(self_id),
        announcements: Arc::clone(&announcements),
    });
    let (_tx, rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv, None, rx));
    coord.set_cluster_controller(controller);

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(result.success);

    let phases: Vec<Phase> = announcements
        .lock()
        .iter()
        .map(|json| {
            serde_json::from_str::<BarrierAnnouncement>(json)
                .unwrap()
                .phase
        })
        .collect();
    assert_eq!(
        phases,
        vec![Phase::Prepare, Phase::Aligned, Phase::Commit],
        "two-level completion must announce Aligned between Prepare and Commit",
    );
}

/// The follower acks at capture (before its durable
/// prepare). If the prepare then fails, a best-effort `ok = false`
/// ack overwrites the capture ack so a still-polling leader can
/// fail the quorum fast instead of waiting for its gate timeout.
#[cfg(feature = "cluster")]
#[tokio::test]
async fn follower_prepare_failure_overwrites_capture_ack() {
    use laminar_core::cluster::control::{
        BarrierAck, BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, Phase, ACK_KEY,
    };
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};
    use laminar_core::state::InProcessBackend;
    use tokio::sync::watch;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let leader_id = NodeId(1);
    let follower_id = NodeId(7);
    let kv = Arc::new(InMemoryKv::new(follower_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let leader_info = NodeInfo {
        id: leader_id,
        name: "leader".into(),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let (_tx, rx) = watch::channel(vec![leader_info]);
    let controller = Arc::new(ClusterController::new(follower_id, kv_trait, None, rx));
    coord.set_cluster_controller(controller);
    // Backend sized for 2 vnodes but the follower claims vnode 99 —
    // `write_vnode_partials` (the last prepare step) fails.
    coord.set_state_backend(Arc::new(InProcessBackend::new(2)));
    coord.set_vnode_set(vec![99]);

    let ann = BarrierAnnouncement {
        epoch: 1,
        checkpoint_id: 1,
        phase: Phase::Prepare,
        flags: 0,
        min_watermark_ms: None,
    };
    let result = coord
        .follower_checkpoint(CheckpointRequest::default(), ann, Duration::from_secs(1))
        .await;
    assert!(result.is_err(), "prepare failure must surface as an error");

    let ack_raw = kv.read_from(follower_id, ACK_KEY).await.unwrap();
    let ack: BarrierAck = serde_json::from_str(&ack_raw).unwrap();
    assert_eq!(ack.epoch, 1);
    assert!(!ack.ok, "the failure ack must overwrite the capture ack");
    assert!(
        ack.error.unwrap().contains("vnode partial write failed"),
        "failure ack should carry the prepare error",
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn leader_publishes_cluster_min_watermark_to_controller() {
    // On a solo cluster, `await_prepare_quorum` computes the
    // cluster-wide min as "leader's local watermark" (no followers
    // to fold). This must be mirrored into the controller atomic so
    // the leader's own operators consume the same value that
    // followers pick up via `observe_barrier(Commit)` — otherwise
    // the leader would drive event-time decisions off a watermark
    // that none of its peers have acked yet.
    use laminar_core::cluster::control::{
        CheckpointDecisionStore, ClusterController, ClusterKv, InMemoryKv,
    };
    use laminar_core::cluster::discovery::NodeId;
    use object_store::local::LocalFileSystem;
    use tokio::sync::watch;

    let dir = tempfile::tempdir().unwrap();
    let decision_dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let decision_os: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(decision_dir.path()).unwrap());
    coord.set_decision_store(Arc::new(CheckpointDecisionStore::new(decision_os)));

    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let (_tx, rx) = watch::channel(Vec::new());
    let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
    coord.set_cluster_controller(Arc::clone(&controller));

    // Pre-condition: controller atomic is at its "unset" sentinel.
    assert_eq!(controller.cluster_min_watermark(), None);

    // Seed a local watermark on the coordinator and drive a full
    // checkpoint. Solo cluster → leader's local value *is* the
    // cluster-wide min.
    coord.set_local_watermark_ms(Some(12_345));
    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(result.success, "solo-cluster checkpoint should succeed");

    assert_eq!(
        controller.cluster_min_watermark(),
        Some(12_345),
        "leader must mirror the cluster-wide min into its controller",
    );

    // A subsequent checkpoint with a lower local watermark must
    // NOT regress the published value — event-time progress is
    // monotonic (same invariant the follower path already enforces).
    coord.set_local_watermark_ms(Some(42));
    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(result.success);
    assert_eq!(
        controller.cluster_min_watermark(),
        Some(12_345),
        "stale local watermark must not lower the published cluster min",
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn leader_announces_prepare_and_commit_on_solo_cluster() {
    use laminar_core::cluster::control::{
        CheckpointDecisionStore, ClusterController, ClusterKv, InMemoryKv, Phase, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::NodeId;
    use object_store::local::LocalFileSystem;
    use tokio::sync::watch;

    let dir = tempfile::tempdir().unwrap();
    let decision_dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let decision_os: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(decision_dir.path()).unwrap());
    coord.set_decision_store(Arc::new(CheckpointDecisionStore::new(decision_os)));

    let self_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(self_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let (_tx, rx) = watch::channel(Vec::new()); // solo — no peers
    let controller = Arc::new(ClusterController::new(self_id, kv_trait, None, rx));
    coord.set_cluster_controller(controller);

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(result.success, "solo-cluster checkpoint should succeed");

    // The last announce on the leader's KV is COMMIT (PREPARE was
    // overwritten in the same slot).
    let raw = kv.read_from(self_id, ANNOUNCEMENT_KEY).await.unwrap();
    let ann: laminar_core::cluster::control::BarrierAnnouncement =
        serde_json::from_str(&raw).unwrap();
    assert_eq!(ann.phase, Phase::Commit);
    assert_eq!(ann.epoch, result.epoch);
}

#[tokio::test]
async fn gate_checks_full_registry_not_just_owned() {
    // Leader owns vnodes {0, 1}. Cluster has 4 vnodes total; a
    // follower (simulated by pre-populating half the backend) owns
    // {2, 3}. If the follower's markers are missing, the leader's
    // gate must fail even though the leader wrote its own.
    use laminar_core::state::InProcessBackend;
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator_with_fast_gate(dir.path()).await;
    let backend = Arc::new(InProcessBackend::new(4));
    coord.set_state_backend(backend.clone());
    coord.set_vnode_set(vec![0, 1]); // leader's owned subset
    coord.set_gate_vnode_set(vec![0, 1, 2, 3]); // full cluster registry

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(
        !result.success,
        "gate must fail when follower markers are missing",
    );
    let err = result.error.expect("failure produces an error message");
    assert!(
        err.contains("not all vnodes persisted"),
        "expected full-registry gate miss, got: {err}",
    );
}

/// B1: each node persists its source offsets every checkpoint; once the epoch
/// seals, a node acquiring a partition reads the union of every node's blob and
/// recovers the committed offset instead of falling back to `auto.offset.reset`.
#[cfg(feature = "cluster")]
#[tokio::test]
#[allow(clippy::disallowed_types)]
async fn source_offset_handoff_round_trip() {
    use bytes::Bytes;
    use laminar_core::state::{InProcessBackend, StateBackend};
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let backend = Arc::new(InProcessBackend::new(4));
    coord.set_state_backend(backend.clone());
    coord.set_assignment_version(1); // >0 so the handoff actually writes

    let mut source_offsets = HashMap::new();
    source_offsets.insert(
        "kafka".to_string(),
        ConnectorCheckpoint::with_offsets(
            5,
            HashMap::from([("events-0".to_string(), "100".to_string())]),
        ),
    );
    coord
        .persist_source_offset_handoff(5, &source_offsets)
        .await
        .unwrap();

    // Seal epoch 5 so it becomes the latest committed epoch.
    for v in 0u32..4 {
        backend
            .write_partial(v, 5, 1, Bytes::from_static(b"x"))
            .await
            .unwrap();
    }
    assert!(backend.epoch_complete(5, &[0, 1, 2, 3], &[]).await.unwrap());

    // A node acquiring events-0 on rotation recovers the committed offset.
    let acquired = coord.acquired_source_offsets().await;
    assert_eq!(acquired.get("events-0"), Some(&"100".to_string()));
}

/// Followers ack at capture and upload partials
/// asynchronously, so the leader's restorable gate must *wait* for
/// late partials rather than failing on the first check.
#[tokio::test]
async fn restorable_gate_waits_for_async_follower_uploads() {
    use bytes::Bytes;
    use laminar_core::state::InProcessBackend;
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let backend = Arc::new(InProcessBackend::new(4));
    // Leader's own partials are present; the "follower's" vnodes
    // {2, 3} land only after a delay, simulating its background
    // upload completing while the leader polls.
    backend
        .write_partial(0, 1, 0, Bytes::from_static(b"leader"))
        .await
        .unwrap();
    backend
        .write_partial(1, 1, 0, Bytes::from_static(b"leader"))
        .await
        .unwrap();
    let late = Arc::clone(&backend);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(300)).await;
        for v in [2u32, 3] {
            late.write_partial(v, 1, 0, Bytes::from_static(b"follower"))
                .await
                .unwrap();
        }
    });
    coord.set_state_backend(backend);
    coord.set_vnode_set(vec![0, 1]);
    coord.set_gate_vnode_set(vec![0, 1, 2, 3]);

    let start = std::time::Instant::now();
    coord
        .await_restorable_gate(1, &[])
        .await
        .expect("gate must seal once the late partials land");
    assert!(
        start.elapsed() >= Duration::from_millis(250),
        "gate returned before the late partials could have landed",
    );
}

#[tokio::test]
async fn gate_passes_when_all_registry_markers_present() {
    // Same topology as the previous test, but now the follower's
    // markers are pre-populated — the gate sees a complete set
    // across the full registry and the checkpoint succeeds.
    use bytes::Bytes;
    use laminar_core::state::InProcessBackend;
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let backend = Arc::new(InProcessBackend::new(4));
    // Simulate the follower's prior write on vnodes {2, 3} for the
    // epoch the leader is about to use (fresh store starts at 1).
    backend
        .write_partial(2, 1, 0, Bytes::from_static(b"follower"))
        .await
        .unwrap();
    backend
        .write_partial(3, 1, 0, Bytes::from_static(b"follower"))
        .await
        .unwrap();
    coord.set_state_backend(backend);
    coord.set_vnode_set(vec![0, 1]);
    coord.set_gate_vnode_set(vec![0, 1, 2, 3]);

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(result.success, "gate should pass: every vnode has a marker");
}

#[tokio::test]
async fn marker_write_failure_aborts_checkpoint() {
    use laminar_core::state::InProcessBackend;
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    // Backend is sized for 2 vnodes, but we claim to own vnode 99 →
    // bridge fails its write, checkpoint aborts cleanly.
    coord.set_state_backend(Arc::new(InProcessBackend::new(2)));
    coord.set_vnode_set(vec![0, 99]);

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(
        !result.success,
        "out-of-range vnode must fail the checkpoint"
    );
    let err = result.error.expect("failure produces an error message");
    assert!(err.contains("vnode partial write failed"), "got: {err}");
}

/// A vnode whose slices didn't change uploads a
/// reference to its last full partial instead of the state bytes,
/// and is forced back to full before the base ages out of the
/// prune retention window.
#[tokio::test]
async fn unchanged_vnode_state_becomes_reference_partial() {
    use laminar_core::state::InProcessBackend;

    let dir = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        max_retained: 2, // reference age cap = 2 epochs
        ..CheckpointConfig::default()
    };
    let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
    let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();
    let backend = Arc::new(InProcessBackend::new(2));
    coord.set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>);
    coord.set_vnode_set(vec![0]);

    let slices = || {
        let mut ops = std::collections::HashMap::new();
        ops.insert(
            "agg".to_string(),
            StagedSlice::Bytes(bytes::Bytes::from_static(b"state-v1")),
        );
        std::collections::HashMap::from([(0u32, ops)])
    };

    // Epoch 1: full upload.
    coord.set_pending_vnode_states(slices());
    let r1 = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(r1.success);
    let p1 = crate::vnode_partial::VnodePartial::decode(
        &backend.read_partial(0, r1.epoch).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(p1.base_epoch, None, "first upload must be full");
    assert!(!p1.operators.is_empty());

    // Epoch 2: identical slices → reference to epoch 1.
    coord.set_pending_vnode_states(slices());
    let r2 = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(r2.success);
    let p2 = crate::vnode_partial::VnodePartial::decode(
        &backend.read_partial(0, r2.epoch).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(
        p2.base_epoch,
        Some(r1.epoch),
        "unchanged slice must reference its base"
    );
    assert!(p2.operators.is_empty());

    // Epoch 3: still identical, but the base would hit the age cap —
    // forced back to a full upload (new base).
    coord.set_pending_vnode_states(slices());
    let r3 = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(r3.success);
    let p3 = crate::vnode_partial::VnodePartial::decode(
        &backend.read_partial(0, r3.epoch).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(
        p3.base_epoch, None,
        "reference age cap must force a full re-upload",
    );

    // Changed slices always upload full.
    let mut changed = std::collections::HashMap::new();
    let mut ops = std::collections::HashMap::new();
    ops.insert(
        "agg".to_string(),
        StagedSlice::Bytes(bytes::Bytes::from_static(b"state-v2")),
    );
    changed.insert(0u32, ops);
    coord.set_pending_vnode_states(changed);
    let r4 = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(r4.success);
    let p4 = crate::vnode_partial::VnodePartial::decode(
        &backend.read_partial(0, r4.epoch).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(p4.base_epoch, None);
    assert!(!p4.operators.is_empty());
}

/// A demoted (cold-staged) slice keeps emitting references while its
/// base is fresh, and a forced full re-upload (base hitting the age
/// cap) fetches the bytes back from the tier instead of dropping the
/// slice from recovery truth.
#[cfg(feature = "state-tier")]
#[tokio::test]
async fn cold_slice_references_then_tier_fetch_on_forced_full() {
    use laminar_core::state::InProcessBackend;

    let tier_dir = tempfile::tempdir().unwrap();
    let tier = Arc::new(
        crate::state_tier::StateTierStore::open(tier_dir.path().join("tier"), None).unwrap(),
    );
    tier.put("agg", 0, b"state-v1").unwrap();
    let tier_tx = crate::state_tier::spawn_worker(&tokio::runtime::Handle::current(), tier, 16);

    let dir = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        max_retained: 2, // reference age cap = 2 epochs
        ..CheckpointConfig::default()
    };
    let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
    let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();
    let backend = Arc::new(InProcessBackend::new(2));
    coord.set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>);
    coord.set_vnode_set(vec![0]);
    coord.set_state_tier(tier_tx);

    // Epoch 1: full upload while the slice is still memory-resident.
    let mut ops = std::collections::HashMap::new();
    ops.insert(
        "agg".to_string(),
        StagedSlice::Bytes(bytes::Bytes::from_static(b"state-v1")),
    );
    coord.set_pending_vnode_states(std::collections::HashMap::from([(0u32, ops)]));
    let r1 = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(r1.success);

    // Demote: release the in-memory pin; subsequent captures stage Cold.
    assert_eq!(coord.demotion_candidates(), vec![(0, b"state-v1".len())]);
    let slices = coord.slices_for_demotion(0);
    assert_eq!(slices.len(), 1);
    assert_eq!(slices[0].1.as_ref(), b"state-v1");
    coord.mark_slice_demoted(0, "agg");
    assert!(coord.demotion_candidates().is_empty());

    // Epoch 2: Cold staged → reference to epoch 1, no bytes needed.
    let cold = || {
        let mut ops = std::collections::HashMap::new();
        ops.insert("agg".to_string(), StagedSlice::Cold);
        std::collections::HashMap::from([(0u32, ops)])
    };
    coord.set_pending_vnode_states(cold());
    let r2 = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(r2.success);
    let p2 = crate::vnode_partial::VnodePartial::decode(
        &backend.read_partial(0, r2.epoch).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(p2.base_epoch, Some(r1.epoch), "cold slice must reference");

    // Epoch 3: still cold, base ages out → full re-upload from the tier.
    coord.set_pending_vnode_states(cold());
    let r3 = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(
        r3.success,
        "forced full must fetch from the tier: {:?}",
        r3.error
    );
    let p3 = crate::vnode_partial::VnodePartial::decode(
        &backend.read_partial(0, r3.epoch).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(p3.base_epoch, None);
    assert_eq!(p3.operators.len(), 1);
    assert_eq!(p3.operators[0].0, "agg");
    assert_eq!(
        p3.operators[0].1, b"state-v1",
        "the re-uploaded slice must be the tier's bytes"
    );
}

/// A forced full re-upload whose cold slice is missing from the tier
/// must fail the epoch — writing the partial without it would silently
/// drop the slice from recovery truth.
#[cfg(feature = "state-tier")]
#[tokio::test]
async fn cold_slice_missing_from_tier_fails_epoch() {
    use laminar_core::state::InProcessBackend;

    let tier_dir = tempfile::tempdir().unwrap();
    let tier = Arc::new(
        crate::state_tier::StateTierStore::open(tier_dir.path().join("tier"), None).unwrap(),
    );
    // Deliberately empty tier.
    let tier_tx = crate::state_tier::spawn_worker(&tokio::runtime::Handle::current(), tier, 16);

    let dir = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        max_retained: 2,
        ..CheckpointConfig::default()
    };
    let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
    let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();
    let backend = Arc::new(InProcessBackend::new(2));
    coord.set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>);
    coord.set_vnode_set(vec![0]);
    coord.set_state_tier(tier_tx);

    let mut ops = std::collections::HashMap::new();
    ops.insert(
        "agg".to_string(),
        StagedSlice::Bytes(bytes::Bytes::from_static(b"state-v1")),
    );
    coord.set_pending_vnode_states(std::collections::HashMap::from([(0u32, ops)]));
    let r1 = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(r1.success);
    coord.mark_slice_demoted(0, "agg");

    let cold = || {
        let mut ops = std::collections::HashMap::new();
        ops.insert("agg".to_string(), StagedSlice::Cold);
        std::collections::HashMap::from([(0u32, ops)])
    };
    // Epoch 2 references; epoch 3 forces full → tier miss → failure.
    coord.set_pending_vnode_states(cold());
    assert!(
        coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap()
            .success
    );
    coord.set_pending_vnode_states(cold());
    let r3 = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(
        !r3.success,
        "a tier miss must fail the epoch, not drop state"
    );
    assert!(
        r3.error.unwrap().contains("missing from the state tier"),
        "failure must name the missing slice"
    );
}

/// `InProcessBackend` wrapper with a per-write delay (forces epoch
/// overlap) and injected failures keyed by `(epoch, vnode)`.
struct FaultBackend {
    inner: laminar_core::state::InProcessBackend,
    fail: parking_lot::Mutex<std::collections::HashSet<(u64, u32)>>,
    write_delay: Duration,
}

#[async_trait::async_trait]
impl StateBackend for FaultBackend {
    async fn write_partial(
        &self,
        vnode: u32,
        epoch: u64,
        assignment_version: u64,
        bytes: bytes::Bytes,
    ) -> Result<(), laminar_core::state::StateBackendError> {
        tokio::time::sleep(self.write_delay).await;
        if self.fail.lock().contains(&(epoch, vnode)) {
            return Err(laminar_core::state::StateBackendError::Io(
                "injected write failure".into(),
            ));
        }
        self.inner
            .write_partial(vnode, epoch, assignment_version, bytes)
            .await
    }

    async fn read_partial(
        &self,
        vnode: u32,
        epoch: u64,
    ) -> Result<Option<bytes::Bytes>, laminar_core::state::StateBackendError> {
        self.inner.read_partial(vnode, epoch).await
    }

    async fn write_commit_descriptor(
        &self,
        epoch: u64,
        key: &str,
        assignment_version: u64,
        bytes: bytes::Bytes,
    ) -> Result<(), laminar_core::state::StateBackendError> {
        self.inner
            .write_commit_descriptor(epoch, key, assignment_version, bytes)
            .await
    }

    async fn read_commit_descriptors(
        &self,
        epoch: u64,
    ) -> Result<Vec<(String, bytes::Bytes)>, laminar_core::state::StateBackendError> {
        self.inner.read_commit_descriptors(epoch).await
    }

    async fn epoch_complete(
        &self,
        epoch: u64,
        vnodes: &[u32],
        required_descriptors: &[String],
    ) -> Result<bool, laminar_core::state::StateBackendError> {
        self.inner
            .epoch_complete(epoch, vnodes, required_descriptors)
            .await
    }

    async fn sealed_epochs(
        &self,
        after: u64,
    ) -> Result<Vec<u64>, laminar_core::state::StateBackendError> {
        self.inner.sealed_epochs(after).await
    }

    async fn prune_before(
        &self,
        before: u64,
    ) -> Result<(), laminar_core::state::StateBackendError> {
        self.inner.prune_before(before).await
    }

    async fn latest_committed_epoch(
        &self,
    ) -> Result<Option<u64>, laminar_core::state::StateBackendError> {
        self.inner.latest_committed_epoch().await
    }

    fn set_authoritative_version(&self, version: u64) {
        self.inner.set_authoritative_version(version);
    }

    fn authoritative_version(&self) -> u64 {
        self.inner.authoritative_version()
    }
}

/// Fault injection at pipeline depth > 1. Four
/// epochs are admitted (ids allocated, tails spawned) while the
/// first is still uploading; the third epoch's upload partially
/// fails — one vnode's write lands, the other's is injected to
/// fail. Must hold:
/// - tails complete in admission order (FIFO coordinator mutex);
/// - the failed epoch is abandoned without disturbing successors;
/// - the recovery point is the last successful epoch;
/// - the partial that *landed* for the failed epoch never becomes
///   a reference base (a successor with identical state must
///   re-upload full, or reference an older *successful* epoch).
#[tokio::test]
#[allow(clippy::too_many_lines)] // four-epoch fault sequence reads better unsplit
async fn overlapping_epoch_failure_is_isolated() {
    let dir = tempfile::tempdir().unwrap();
    let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
        .await
        .unwrap();
    let backend = Arc::new(FaultBackend {
        inner: laminar_core::state::InProcessBackend::new(2),
        fail: parking_lot::Mutex::new(std::collections::HashSet::new()),
        write_delay: Duration::from_millis(100),
    });
    coord.set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>);
    coord.set_vnode_set(vec![0, 1]);

    let allocator = coord.epoch_allocator();
    let coordinator = Arc::new(tokio::sync::Mutex::new(Some(coord)));
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<CheckpointResult>();

    // Admit an epoch exactly as the pipeline callback does: claim
    // ids lock-free, spawn the tail; the FIFO mutex serializes the
    // durable work.
    let admit = |tag: &'static [u8]| {
        let (epoch, checkpoint_id) = allocator.allocate();
        let coordinator = Arc::clone(&coordinator);
        let done = done_tx.clone();
        let states = std::collections::HashMap::from([
            (
                0u32,
                std::collections::HashMap::from([(
                    "agg".to_string(),
                    StagedSlice::Bytes(bytes::Bytes::from_static(tag)),
                )]),
            ),
            (
                1u32,
                std::collections::HashMap::from([(
                    "agg".to_string(),
                    StagedSlice::Bytes(bytes::Bytes::from_static(tag)),
                )]),
            ),
        ]);
        tokio::spawn(async move {
            let mut guard = coordinator.lock().await;
            let coord = guard.as_mut().unwrap();
            coord.set_pending_vnode_states(states);
            let result = coord
                .checkpoint_preallocated(
                    CheckpointRequest::default(),
                    epoch,
                    checkpoint_id,
                    QuorumStage::RunInline,
                )
                .await
                .unwrap();
            done.send(result).unwrap();
        });
        epoch
    };

    // All four admitted while epoch A's tail is still uploading
    // (each write sleeps 100ms; admissions are microseconds apart,
    // paced just enough that lock-queue order is admission order).
    let a = admit(b"v1");
    tokio::time::sleep(Duration::from_millis(10)).await;
    let b = admit(b"v1"); // unchanged → reference to A
    tokio::time::sleep(Duration::from_millis(10)).await;
    let (c_epoch, _) = allocator.peek();
    backend.fail.lock().insert((c_epoch, 1)); // vnode 0 lands, vnode 1 fails
    let c = admit(b"v2"); // changed → full attempt, partially fails
    tokio::time::sleep(Duration::from_millis(10)).await;
    let d = admit(b"v2"); // same state as the failed epoch

    let mut results = Vec::new();
    for _ in 0..4 {
        results.push(done_rx.recv().await.unwrap());
    }

    assert_eq!(
        results.iter().map(|r| r.epoch).collect::<Vec<_>>(),
        vec![a, b, c, d],
        "tails must complete in admission order",
    );
    assert_eq!(
        results.iter().map(|r| r.success).collect::<Vec<_>>(),
        vec![true, true, false, true],
        "the failed epoch must not disturb its successors",
    );
    assert!(results[2]
        .error
        .as_deref()
        .is_some_and(|e| e.contains("vnode partial write failed")));

    // Recovery point: the failed epoch was never sealed.
    assert_eq!(
        backend.latest_committed_epoch().await.unwrap(),
        Some(d),
        "the last successful epoch is the recovery point",
    );

    // B was unchanged from A → reference. D matches the FAILED
    // epoch's state, and C's vnode-0 write landed before the
    // injected failure — D must not reference it (bases are
    // recorded only after every write in an epoch lands).
    let p_b = crate::vnode_partial::VnodePartial::decode(
        &backend.read_partial(0, b).await.unwrap().unwrap(),
    )
    .unwrap();
    assert_eq!(p_b.base_epoch, Some(a));
    for vnode in [0u32, 1] {
        let p_d = crate::vnode_partial::VnodePartial::decode(
            &backend.read_partial(vnode, d).await.unwrap().unwrap(),
        )
        .unwrap();
        assert_eq!(
            p_d.base_epoch, None,
            "vnode {vnode}: a successor of a failed epoch must re-upload full, \
                 never reference the failed epoch's stray partial",
        );
        assert_eq!(p_d.operators[0].1, b"v2");
    }
    assert_eq!(c, d - 1, "abandoned epoch's id is burned, not reused");
}

/// A follower persists its manifest before learning the leader
/// aborted, so an aborted epoch's Pending manifest can be the
/// highest on disk at restart. Construction seeds ids from it
/// (high is safe); recovery then restores from the older committed
/// epoch and must NOT walk the ids back down — that would
/// re-allocate the aborted epoch over its stale artifacts.
#[tokio::test]
async fn recovery_never_walks_ids_back_onto_aborted_epochs() {
    use laminar_core::storage::checkpoint_manifest::SinkCommitStatus;

    let dir = tempfile::tempdir().unwrap();
    let store = FileSystemCheckpointStore::new(dir.path(), 5);
    // Committed epoch 3.
    let mut committed = CheckpointManifest::new(3, 3);
    committed
        .sink_commit_statuses
        .insert("out".into(), SinkCommitStatus::Committed);
    store.save(&committed).await.unwrap();
    // Aborted epoch 5: persisted by a follower before the leader's
    // Abort, never committed.
    let mut aborted = CheckpointManifest::new(5, 5);
    aborted
        .sink_commit_statuses
        .insert("out".into(), SinkCommitStatus::Pending);
    store.save(&aborted).await.unwrap();

    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), Box::new(store))
        .await
        .unwrap();
    assert_eq!(coord.epoch(), 6, "seeds from the highest loadable manifest");

    let recovered = coord.recover().await.unwrap().expect("recovers");
    assert_eq!(recovered.epoch(), 3, "restores from the committed epoch");
    assert_eq!(
        coord.epoch(),
        6,
        "ids must stay above the aborted epoch, never re-allocating it",
    );
}

#[test]
fn epoch_allocator_allocates_monotonic_pairs() {
    let a = EpochAllocator::new(5, 9);
    assert_eq!(a.peek(), (5, 9));
    assert_eq!(a.allocate(), (5, 9));
    assert_eq!(a.allocate(), (6, 10));
    assert_eq!(a.peek(), (7, 11));
    a.advance_to(20, 30);
    assert_eq!(a.allocate(), (20, 30));
    // Monotonic: never walks backwards.
    a.advance_to(5, 5);
    assert_eq!(a.peek(), (21, 31));
}

/// Ids are allocated at the start of an attempt: a failed epoch is
/// abandoned (Flink-style), never retried under the same ids.
#[tokio::test]
async fn failed_epoch_is_abandoned_not_retried() {
    let dir = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        max_checkpoint_bytes: Some(16),
        ..CheckpointConfig::default()
    };
    let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 3));
    let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();

    // Oversized state → size-cap rejection.
    let mut ops = HashMap::new();
    ops.insert("big".to_string(), bytes::Bytes::from(vec![0u8; 2_000_000]));
    let failed = coord
        .checkpoint(CheckpointRequest {
            operator_states: ops,
            ..CheckpointRequest::default()
        })
        .await
        .unwrap();
    assert!(!failed.success);

    let ok = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(ok.success);
    assert_eq!(
        ok.epoch,
        failed.epoch + 1,
        "the failed epoch must be abandoned, not reused",
    );
    assert_eq!(ok.checkpoint_id, failed.checkpoint_id + 1);
}

#[tokio::test]
async fn test_stats_include_percentiles_after_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    // Run 3 checkpoints.
    for _ in 0..3 {
        let result = coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        assert!(result.success);
    }

    let stats = coord.stats();
    assert_eq!(stats.completed, 3);
    // After 3 fast checkpoints, percentiles should be > 0
    // (they're real durations, not zero).
    assert!(stats.last_duration.is_some());
}

/// Sink whose `pre_commit` always fails; counts `rollback_epoch` calls.
struct FailingPreCommitSink {
    rollback_count: Arc<std::sync::atomic::AtomicU64>,
    schema: arrow::datatypes::SchemaRef,
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SinkConnector for FailingPreCommitSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        _batch: &arrow::array::RecordBatch,
    ) -> Result<laminar_connectors::connector::WriteResult, laminar_connectors::error::ConnectorError>
    {
        Ok(laminar_connectors::connector::WriteResult::new(0, 0))
    }

    async fn pre_commit(
        &mut self,
        epoch: u64,
    ) -> Result<Option<Vec<u8>>, laminar_connectors::error::ConnectorError> {
        Err(laminar_connectors::error::ConnectorError::TransactionError(
            format!("synthetic pre_commit failure at epoch {epoch}"),
        ))
    }

    async fn rollback_epoch(
        &mut self,
        _epoch: u64,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        self.rollback_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn capabilities(&self) -> laminar_connectors::connector::SinkConnectorCapabilities {
        laminar_connectors::connector::SinkConnectorCapabilities::new(Duration::from_secs(5))
            .with_exactly_once()
            .with_two_phase_commit()
            .with_preserves_pending_on_abandon()
    }
}

/// A pre-commit failure abandons the epoch but must NOT hard-roll
/// back a connector that preserves pending output (see
/// `SinkCommand::RollbackEpoch`): the pending rows ride into the
/// next epoch's commit. Connectors without the capability ARE
/// rolled back.
#[tokio::test]
async fn pre_commit_failure_abandons_without_connector_rollback() {
    use arrow::datatypes::{DataType, Field, Schema};

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;

    let rollback_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let sink = FailingPreCommitSink {
        rollback_count: Arc::clone(&rollback_count),
        schema,
    };
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "failing-sink".into(),
        sink_id: Arc::from("failing-sink"),
        connector: Box::new(sink),
        exactly_once: true,
        coordinated_commit: false,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(5),
        event_tx,
    });
    coord.register_sink("failing-sink", handle, true, false);

    coord.begin_initial_epoch().await.unwrap();

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();

    assert!(!result.success);
    assert!(
        result
            .error
            .as_deref()
            .is_some_and(|e| e.contains("pre-commit failed")),
        "error should mention pre-commit: got {:?}",
        result.error
    );
    assert_eq!(
        rollback_count.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "a healthy sink must keep its pending output on a live abandon"
    );
}

/// Writes fail (poisoning the epoch); `rollback_epoch` hangs
/// forever. The poisoned epoch is what makes the live abandon take
/// the forced connector-rollback path that can hang.
struct StuckRollbackSink {
    schema: arrow::datatypes::SchemaRef,
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SinkConnector for StuckRollbackSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        _batch: &arrow::array::RecordBatch,
    ) -> Result<laminar_connectors::connector::WriteResult, laminar_connectors::error::ConnectorError>
    {
        Err(laminar_connectors::error::ConnectorError::WriteError(
            "synthetic write failure".into(),
        ))
    }

    async fn pre_commit(
        &mut self,
        _epoch: u64,
    ) -> Result<Option<Vec<u8>>, laminar_connectors::error::ConnectorError> {
        Err(laminar_connectors::error::ConnectorError::TransactionError(
            "synthetic pre_commit failure".into(),
        ))
    }

    async fn rollback_epoch(
        &mut self,
        _epoch: u64,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        // Hang until the test runtime drops us.
        std::future::pending::<()>().await;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn capabilities(&self) -> laminar_connectors::connector::SinkConnectorCapabilities {
        laminar_connectors::connector::SinkConnectorCapabilities::new(Duration::from_secs(5))
            .with_exactly_once()
            .with_two_phase_commit()
    }
}

#[tokio::test(start_paused = true)]
async fn test_rollback_sinks_bounded_by_timeout() {
    use arrow::datatypes::{DataType, Field, Schema};

    let dir = tempfile::tempdir().unwrap();
    let config = CheckpointConfig {
        rollback_timeout: Duration::from_millis(100),
        ..Default::default()
    };
    let store = Box::new(
        laminar_core::storage::checkpoint_store::FileSystemCheckpointStore::new(dir.path(), 3),
    );
    let mut coord = CheckpointCoordinator::new(config, store).await.unwrap();

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let sink = StuckRollbackSink { schema };
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "stuck-sink".into(),
        sink_id: Arc::from("stuck-sink"),
        connector: Box::new(sink),
        exactly_once: true,
        coordinated_commit: false,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(5),
        event_tx,
    });
    coord.register_sink("stuck-sink", handle.clone(), true, false);
    coord.begin_initial_epoch().await.unwrap();

    // Poison the epoch with a failing write — only a poisoned sink
    // takes the forced connector-rollback path that can hang.
    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let batch = arrow::array::RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow::array::Int32Array::from(vec![1]))],
    )
    .unwrap();
    handle.write_batch(batch).await.unwrap();
    handle.sync().await.unwrap();

    // Poisoned pre_commit fails → rollback_sinks fires → connector
    // rollback hangs → 100ms rollback_timeout fires → coordinator
    // returns instead of wedging.
    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();

    assert!(!result.success);
    assert!(
        result
            .error
            .as_deref()
            .is_some_and(|e| e.contains("pre-commit failed")),
        "checkpoint result should reflect pre-commit failure: got {:?}",
        result.error
    );
}

/// A sink that declares `coordinated_commit` and returns a descriptor.
struct CoordinatedMockSink {
    schema: arrow::datatypes::SchemaRef,
    descriptor: Vec<u8>,
}

#[async_trait::async_trait]
impl laminar_connectors::connector::SinkConnector for CoordinatedMockSink {
    async fn open(
        &mut self,
        _config: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    async fn write_batch(
        &mut self,
        _batch: &arrow::array::RecordBatch,
    ) -> Result<laminar_connectors::connector::WriteResult, laminar_connectors::error::ConnectorError>
    {
        Ok(laminar_connectors::connector::WriteResult::new(0, 0))
    }

    async fn pre_commit(
        &mut self,
        _epoch: u64,
    ) -> Result<Option<Vec<u8>>, laminar_connectors::error::ConnectorError> {
        // An empty descriptor models an idle epoch (no data produced).
        if self.descriptor.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.descriptor.clone()))
        }
    }

    async fn close(&mut self) -> Result<(), laminar_connectors::error::ConnectorError> {
        Ok(())
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        Arc::clone(&self.schema)
    }

    fn capabilities(&self) -> laminar_connectors::connector::SinkConnectorCapabilities {
        laminar_connectors::connector::SinkConnectorCapabilities::new(Duration::from_secs(5))
            .with_exactly_once()
            .with_two_phase_commit()
            .with_coordinated_commit()
    }
}

/// An idle coordinated sink (no descriptor this epoch) must not stall the
/// gate — regression for the empty-epoch hang.
#[tokio::test]
async fn coordinated_sink_idle_epoch_still_seals() {
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::state::{InProcessBackend, StateBackend};

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    coord.set_state_backend(Arc::new(InProcessBackend::new(2)) as Arc<dyn StateBackend>);

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (event_tx, _rx) = laminar_core::streaming::channel::channel::<crate::sink_task::SinkEvent>(
        crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY,
    );
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "ice".into(),
        sink_id: Arc::from("ice"),
        connector: Box::new(CoordinatedMockSink {
            schema,
            descriptor: Vec::new(), // idle: pre_commit returns None
        }),
        exactly_once: true,
        coordinated_commit: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(5),
        event_tx,
    });
    coord.register_sink("ice", handle, true, true);
    coord.begin_initial_epoch().await.unwrap();

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(
        result.success,
        "idle coordinated epoch must seal, not hang: {:?}",
        result.error
    );
}

/// A coordinated sink's `pre_commit` descriptor is persisted to the state
/// backend and required by the durability gate before the epoch seals.
#[tokio::test]
async fn coordinated_sink_descriptor_persisted_and_gated() {
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::state::{InProcessBackend, StateBackend};

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let backend = Arc::new(InProcessBackend::new(2));
    coord.set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>);

    let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
    let (event_tx, _event_rx) = laminar_core::streaming::channel::channel::<
        crate::sink_task::SinkEvent,
    >(crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY);
    let handle = crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "ice".into(),
        sink_id: Arc::from("ice"),
        connector: Box::new(CoordinatedMockSink {
            schema,
            descriptor: b"datafiles".to_vec(),
        }),
        exactly_once: true,
        coordinated_commit: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(5),
        event_tx,
    });
    coord.register_sink("ice", handle, true, true);
    coord.begin_initial_epoch().await.unwrap();

    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(result.success, "checkpoint failed: {:?}", result.error);

    let descs = backend.read_commit_descriptors(result.epoch).await.unwrap();
    assert_eq!(
        descs,
        vec![("node=0/sink=ice".to_string(), b"datafiles".to_vec().into())]
    );
    assert_eq!(
        backend.latest_committed_epoch().await.unwrap(),
        Some(result.epoch),
        "epoch must seal once the descriptor is durable"
    );
}

/// Connector config for the Dockerized Iceberg REST + `MinIO` stack.
#[cfg(feature = "iceberg")]
fn iceberg_e2e_config(
    table: &str,
    schema: &arrow::datatypes::SchemaRef,
) -> laminar_connectors::config::ConnectorConfig {
    use laminar_connectors::config::{encode_arrow_schema_ipc, ConnectorConfig};
    let mut cc = ConnectorConfig::new("iceberg");
    cc.set("catalog.uri", "http://localhost:8181");
    cc.set("warehouse", "s3://warehouse/wh");
    cc.set("storage.type", "s3");
    cc.set("namespace", "laminar_test");
    cc.set("table.name", table);
    cc.set("auto.create", "true");
    cc.set("writer.id", "w0");
    cc.set("catalog.property.s3.endpoint", "http://localhost:9000");
    cc.set("catalog.property.s3.access-key-id", "minioadmin");
    cc.set("catalog.property.s3.secret-access-key", "minioadmin");
    cc.set("catalog.property.s3.region", "us-east-1");
    cc.set("catalog.property.s3.path-style-access", "true");
    cc.set("_arrow_schema", encode_arrow_schema_ipc(schema));
    cc
}

/// Open a coordinated Iceberg sink and spawn its task handle.
#[cfg(feature = "iceberg")]
async fn spawn_iceberg_sink(
    cc: &laminar_connectors::config::ConnectorConfig,
) -> crate::sink_task::SinkTaskHandle {
    use laminar_connectors::connector::SinkConnector;
    use laminar_connectors::lakehouse::iceberg::IcebergSink;
    use laminar_connectors::lakehouse::iceberg_config::IcebergSinkConfig;

    let mut sink = IcebergSink::new(IcebergSinkConfig::from_config(cc).unwrap(), None);
    sink.open(cc).await.expect("open iceberg sink");
    let (event_tx, _rx) = laminar_core::streaming::channel::channel::<crate::sink_task::SinkEvent>(
        crate::sink_task::SINK_EVENT_CHANNEL_CAPACITY,
    );
    crate::sink_task::SinkTaskHandle::spawn(crate::sink_task::SinkTaskConfig {
        name: "ice".into(),
        sink_id: std::sync::Arc::from("ice"),
        connector: Box::new(sink),
        exactly_once: true,
        coordinated_commit: true,
        channel_capacity: crate::sink_task::DEFAULT_CHANNEL_CAPACITY,
        flush_interval: crate::sink_task::DEFAULT_FLUSH_INTERVAL,
        write_timeout: Duration::from_secs(60),
        event_tx,
    })
}

/// `(snapshot_count, total_rows)` for the Iceberg table.
#[cfg(feature = "iceberg")]
async fn iceberg_state(
    cc: &laminar_connectors::config::ConnectorConfig,
    table: &str,
) -> (usize, usize) {
    use laminar_connectors::lakehouse::iceberg_config::IcebergSinkConfig;
    use laminar_connectors::lakehouse::iceberg_io;
    let catalog = iceberg_io::build_catalog(&IcebergSinkConfig::from_config(cc).unwrap().catalog)
        .await
        .unwrap();
    let loaded = iceberg_io::load_table(catalog.as_ref(), "laminar_test", table)
        .await
        .unwrap();
    let snapshots = loaded.metadata().snapshots().count();
    let rows: usize = iceberg_io::scan_table(&loaded, None, &[])
        .await
        .unwrap()
        .iter()
        .map(arrow::array::RecordBatch::num_rows)
        .sum();
    (snapshots, rows)
}

/// Data files added by the table's latest snapshot (from its summary).
#[cfg(feature = "iceberg")]
async fn iceberg_added_data_files(
    cc: &laminar_connectors::config::ConnectorConfig,
    table: &str,
) -> usize {
    use laminar_connectors::lakehouse::iceberg_config::IcebergSinkConfig;
    use laminar_connectors::lakehouse::iceberg_io;
    let catalog = iceberg_io::build_catalog(&IcebergSinkConfig::from_config(cc).unwrap().catalog)
        .await
        .unwrap();
    let loaded = iceberg_io::load_table(catalog.as_ref(), "laminar_test", table)
        .await
        .unwrap();
    loaded
        .metadata()
        .current_snapshot()
        .expect("a snapshot")
        .summary()
        .additional_properties
        .get("added-data-files")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0)
}

#[cfg(feature = "iceberg")]
fn unique_table(prefix: &str) -> String {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}_{n}")
}

/// Full-wiring e2e: a real Iceberg sink registered as coordinated. A checkpoint
/// persists the descriptor and seals the epoch; the designated committer then
/// commits to the real Iceberg catalog, exactly once. Requires Docker
/// (tests/docker/iceberg-compose.yml).
#[cfg(feature = "iceberg")]
#[tokio::test]
#[ignore = "requires Docker: tests/docker/iceberg-compose.yml"]
async fn coordinated_iceberg_commits_through_committer() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::state::{InProcessBackend, StateBackend};
    use std::sync::Arc;

    if std::net::TcpStream::connect("127.0.0.1:8181").is_err() {
        eprintln!("skipping: Iceberg REST not reachable on 127.0.0.1:8181");
        return;
    }

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let table = unique_table("e2e");
    let cc = iceberg_e2e_config(&table, &schema);
    let handle = spawn_iceberg_sink(&cc).await;

    let dir = tempfile::tempdir().unwrap();
    let mut coord = make_coordinator(dir.path()).await;
    let backend = Arc::new(InProcessBackend::new(2));
    coord.set_state_backend(Arc::clone(&backend) as Arc<dyn StateBackend>);
    coord.register_sink("ice", handle.clone(), true, true);
    coord.begin_initial_epoch().await.unwrap();

    // Three separate batches in one epoch — they must coalesce into ONE Parquet
    // file (one S3 upload), not one file per batch.
    for chunk in [[1i64, 2, 3], [4, 5, 6], [7, 8, 9]] {
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(chunk.to_vec()))],
        )
        .unwrap();
        handle.write_batch(batch).await.unwrap();
    }
    handle.sync().await.unwrap();

    // Checkpoint: pre_commit writes Parquet + descriptor, the gate seals the epoch.
    let result = coord
        .checkpoint(CheckpointRequest::default())
        .await
        .unwrap();
    assert!(result.success, "checkpoint failed: {:?}", result.error);

    // The designated committer commits the sealed epoch to the real catalog.
    let mut committer = coord
        .coordinated_committer()
        .expect("coordinated committer");
    committer.commit_ready().await.unwrap();
    assert_eq!(
        iceberg_state(&cc, &table).await,
        (1, 9),
        "one snapshot, 9 rows"
    );
    assert_eq!(
        iceberg_added_data_files(&cc, &table).await,
        1,
        "all staged batches must land in a single data file"
    );

    // Re-running the committer is idempotent — no new snapshot.
    committer.commit_ready().await.unwrap();
    assert_eq!(
        iceberg_state(&cc, &table).await.0,
        1,
        "re-commit must not add a snapshot"
    );
}

/// Crash-recovery: descriptors persisted to a durable backend survive a restart.
/// Commit epoch 1, then seal epoch 2 without committing (the crash window). A
/// fresh backend + committer over the same storage must commit epoch 2 (no loss)
/// and not re-commit epoch 1 (no duplicate). Requires Docker.
#[cfg(feature = "iceberg")]
#[tokio::test]
#[ignore = "requires Docker: tests/docker/iceberg-compose.yml"]
async fn coordinated_iceberg_recovers_uncommitted_epoch_after_restart() {
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_core::state::{ObjectStoreBackend, StateBackend};
    use object_store::local::LocalFileSystem;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    if std::net::TcpStream::connect("127.0.0.1:8181").is_err() {
        eprintln!("skipping: Iceberg REST not reachable on 127.0.0.1:8181");
        return;
    }

    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let table = unique_table("e2e_recover");
    let cc = iceberg_e2e_config(&table, &schema);

    // Durable state backend so descriptors outlive the simulated crash.
    let state_dir = tempfile::tempdir().unwrap();
    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(state_dir.path()).unwrap());

    let row_batch = |vals: Vec<i64>| {
        arrow::array::RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals))])
            .unwrap()
    };

    // ── pre-crash engine ──
    let ckpt_dir = tempfile::tempdir().unwrap();
    let handle1 = spawn_iceberg_sink(&cc).await;
    let mut coord = make_coordinator(ckpt_dir.path()).await;
    let backend1 = Arc::new(ObjectStoreBackend::new(Arc::clone(&store), "node0", 2));
    coord.set_state_backend(Arc::clone(&backend1) as Arc<dyn StateBackend>);
    coord.register_sink("ice", handle1.clone(), true, true);
    coord.begin_initial_epoch().await.unwrap();

    // Epoch 1: write, checkpoint, commit.
    handle1.write_batch(row_batch(vec![1, 2, 3])).await.unwrap();
    handle1.sync().await.unwrap();
    assert!(
        coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap()
            .success
    );
    let mut committer1 = coord.coordinated_committer().expect("committer");
    committer1.commit_ready().await.unwrap();
    assert_eq!(iceberg_state(&cc, &table).await, (1, 3));

    // Epoch 2: write + checkpoint (descriptor durable), but DO NOT commit — crash.
    handle1.write_batch(row_batch(vec![4, 5, 6])).await.unwrap();
    handle1.sync().await.unwrap();
    assert!(
        coord
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap()
            .success
    );
    drop(committer1);
    drop(coord);
    drop(handle1);

    // ── restart: fresh backend + committer over the SAME durable storage ──
    let backend2 = Arc::new(ObjectStoreBackend::new(Arc::clone(&store), "node0", 2));
    let handle2 = spawn_iceberg_sink(&cc).await;
    let mut committer2 = crate::coordinated_committer::CoordinatedCommitter::new(
        Arc::clone(&backend2) as Arc<dyn StateBackend>,
        vec![("ice".into(), handle2)],
        Arc::new(AtomicU64::new(0)),
    );
    committer2.commit_ready().await.unwrap();

    // Epoch 2 recovered (no loss); epoch 1 not re-committed (no duplicate).
    assert_eq!(
        iceberg_state(&cc, &table).await,
        (2, 6),
        "recovery must commit the uncommitted epoch exactly once"
    );

    // Re-running after recovery is still idempotent.
    committer2.commit_ready().await.unwrap();
    assert_eq!(iceberg_state(&cc, &table).await.0, 2);
}

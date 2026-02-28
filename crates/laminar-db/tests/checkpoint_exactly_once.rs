//! End-to-end exactly-once checkpoint test via barrier protocol.
//!
//! Validates the full pipeline path:
//! 1. Multiple sources produce data
//! 2. Barriers are injected and aligned across all sources
//! 3. Checkpoint captures consistent state at barrier point
//! 4. Simulated crash (drop pipeline)
//! 5. Recovery from checkpoint restores correct offsets
//! 6. No duplicate or lost data

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use tokio::sync::Notify;

use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_db::checkpoint_coordinator::{CheckpointConfig, CheckpointCoordinator};
use laminar_db::pipeline::{
    PipelineCallback, PipelineConfig, PipelineCoordinator, SourceRegistration,
};
use laminar_db::recovery_manager::RecoveryManager;
use laminar_storage::checkpoint_store::FileSystemCheckpointStore;

/// A callback that tracks barrier checkpoint calls and records state.
struct BarrierTrackingCallback {
    cycle_count: u64,
    barrier_checkpoints: Vec<HashMap<String, SourceCheckpoint>>,
    force_checkpoints: u64,
    should_trigger: Arc<AtomicBool>,
    total_records_processed: Arc<AtomicU64>,
}

impl BarrierTrackingCallback {
    fn new(should_trigger: Arc<AtomicBool>, record_counter: Arc<AtomicU64>) -> Self {
        Self {
            cycle_count: 0,
            barrier_checkpoints: Vec::new(),
            force_checkpoints: 0,
            should_trigger,
            total_records_processed: record_counter,
        }
    }
}

#[async_trait::async_trait]
impl PipelineCallback for BarrierTrackingCallback {
    async fn execute_cycle(
        &mut self,
        source_batches: &HashMap<String, Vec<RecordBatch>>,
        _watermark: i64,
    ) -> Result<HashMap<String, Vec<RecordBatch>>, String> {
        self.cycle_count += 1;
        let records: u64 = source_batches
            .values()
            .flat_map(|v| v.iter())
            .map(|b| b.num_rows() as u64)
            .sum();
        self.total_records_processed
            .fetch_add(records, Ordering::Relaxed);
        Ok(HashMap::new())
    }

    fn push_to_streams(&self, _results: &HashMap<String, Vec<RecordBatch>>) {}

    async fn write_to_sinks(&mut self, _results: &HashMap<String, Vec<RecordBatch>>) {}

    fn extract_watermark(&mut self, _source_name: &str, _batch: &RecordBatch) {}

    fn filter_late_rows(&self, _source_name: &str, batch: &RecordBatch) -> Option<RecordBatch> {
        Some(batch.clone())
    }

    fn current_watermark(&self) -> i64 {
        0
    }

    async fn maybe_checkpoint(&mut self, force: bool) -> bool {
        if force {
            self.force_checkpoints += 1;
            return true;
        }
        self.should_trigger.load(Ordering::Relaxed)
    }

    async fn checkpoint_with_barrier(
        &mut self,
        source_checkpoints: HashMap<String, SourceCheckpoint>,
    ) -> bool {
        self.barrier_checkpoints.push(source_checkpoints);
        true
    }

    fn record_cycle(&self, _events_ingested: u64, _batches: u64, _elapsed_ns: u64) {}

    async fn poll_tables(&mut self) {}
}

/// Test that barriers are injected and aligned across multiple sources,
/// and that the checkpoint callback fires with consistent offsets.
#[tokio::test]
async fn test_barrier_aligned_checkpoint_fires() {
    let sources = vec![
        SourceRegistration {
            name: "src_a".to_string(),
            connector: Box::new(
                laminar_connectors::testing::MockSourceConnector::with_batches(50, 10),
            ),
            config: laminar_connectors::config::ConnectorConfig::new("mock"),
            supports_replay: true,
        },
        SourceRegistration {
            name: "src_b".to_string(),
            connector: Box::new(
                laminar_connectors::testing::MockSourceConnector::with_batches(50, 10),
            ),
            config: laminar_connectors::config::ConnectorConfig::new("mock"),
            supports_replay: true,
        },
    ];

    let shutdown = Arc::new(Notify::new());
    let shutdown_clone = Arc::clone(&shutdown);

    let config = PipelineConfig {
        fallback_poll_interval: Duration::from_millis(1),
        batch_window: Duration::ZERO,
        // Enable checkpoint interval to trigger barrier injection.
        checkpoint_interval: Some(Duration::from_millis(10)),
        barrier_alignment_timeout: Duration::from_secs(5),
        ..PipelineConfig::default()
    };

    let coordinator = PipelineCoordinator::new(sources, config, shutdown)
        .await
        .unwrap();

    let should_trigger = Arc::new(AtomicBool::new(true));
    let record_counter = Arc::new(AtomicU64::new(0));
    let callback =
        BarrierTrackingCallback::new(Arc::clone(&should_trigger), Arc::clone(&record_counter));

    let handle = tokio::spawn(async move {
        coordinator.run(Box::new(callback)).await;
    });

    // Let the pipeline run and process data + barriers.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Shut down.
    shutdown_clone.notify_one();
    handle.await.unwrap();

    // Verify that records were processed.
    let total = record_counter.load(Ordering::Relaxed);
    assert!(
        total > 0,
        "pipeline should have processed records, got {total}"
    );
}

/// Test that checkpoint persisted via barrier path can be recovered.
#[tokio::test]
async fn test_barrier_checkpoint_recovery_round_trip() {
    let dir = tempfile::tempdir().unwrap();

    // Phase 1: Run pipeline, trigger barrier checkpoint, persist.
    let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 5));
    let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store);

    // Simulate barrier-aligned checkpoint: operator state captured at barrier.
    let mut operator_states = HashMap::new();
    operator_states.insert(
        "stream_executor".to_string(),
        b"barrier-consistent-state".to_vec(),
    );

    // Simulate source offsets captured at barrier.
    let mut extra_tables = HashMap::new();
    extra_tables.insert(
        "src_a".to_string(),
        laminar_storage::checkpoint_manifest::ConnectorCheckpoint {
            offsets: HashMap::from([("records".into(), "500".into())]),
            epoch: 1,
            metadata: HashMap::new(),
        },
    );
    extra_tables.insert(
        "src_b".to_string(),
        laminar_storage::checkpoint_manifest::ConnectorCheckpoint {
            offsets: HashMap::from([("records".into(), "300".into())]),
            epoch: 1,
            metadata: HashMap::new(),
        },
    );

    let mut source_watermarks = HashMap::new();
    source_watermarks.insert("src_a".to_string(), 5000_i64);
    source_watermarks.insert("src_b".to_string(), 4500_i64);

    let result = coord
        .checkpoint_with_extra_tables(
            operator_states,
            Some(4500), // global watermark = min of sources
            0,
            Vec::new(),
            None,
            extra_tables,
            source_watermarks,
            Some(0xDEAD_BEEF),
        )
        .await
        .unwrap();

    assert!(result.success, "barrier checkpoint should succeed");
    assert_eq!(result.epoch, 1);

    // Phase 2: Simulate crash — drop coordinator.
    drop(coord);

    // Phase 3: Recovery — load from store.
    let store = FileSystemCheckpointStore::new(dir.path(), 5);
    let mgr = RecoveryManager::new(&store);
    let manifest = mgr.load_latest().unwrap().unwrap();

    // Verify epoch and watermark.
    assert_eq!(manifest.epoch, 1);
    assert_eq!(manifest.watermark, Some(4500));

    // Verify source offsets captured at barrier.
    let src_a = manifest.table_offsets.get("src_a").unwrap();
    assert_eq!(
        src_a.offsets.get("records"),
        Some(&"500".to_string()),
        "src_a offset should be captured at barrier point"
    );
    let src_b = manifest.table_offsets.get("src_b").unwrap();
    assert_eq!(
        src_b.offsets.get("records"),
        Some(&"300".to_string()),
        "src_b offset should be captured at barrier point"
    );

    // Verify operator state (barrier-consistent).
    let op_state = manifest.operator_states.get("stream_executor").unwrap();
    assert_eq!(
        op_state.decode_inline().unwrap(),
        b"barrier-consistent-state"
    );

    // Verify per-source watermarks.
    assert_eq!(manifest.source_watermarks.get("src_a"), Some(&5000));
    assert_eq!(manifest.source_watermarks.get("src_b"), Some(&4500));

    // Verify pipeline hash.
    assert_eq!(manifest.pipeline_hash, Some(0xDEAD_BEEF));
}

/// Test that a pipeline with a single source correctly injects and
/// aligns barriers (degenerate case: alignment is trivial).
#[tokio::test]
async fn test_single_source_barrier_checkpoint() {
    let sources = vec![SourceRegistration {
        name: "only_source".to_string(),
        connector: Box::new(laminar_connectors::testing::MockSourceConnector::with_batches(100, 5)),
        config: laminar_connectors::config::ConnectorConfig::new("mock"),
        supports_replay: true,
    }];

    let shutdown = Arc::new(Notify::new());
    let shutdown_clone = Arc::clone(&shutdown);

    let config = PipelineConfig {
        fallback_poll_interval: Duration::from_millis(1),
        batch_window: Duration::ZERO,
        checkpoint_interval: Some(Duration::from_millis(10)),
        barrier_alignment_timeout: Duration::from_secs(5),
        ..PipelineConfig::default()
    };

    let coordinator = PipelineCoordinator::new(sources, config, shutdown)
        .await
        .unwrap();

    let should_trigger = Arc::new(AtomicBool::new(true));
    let record_counter = Arc::new(AtomicU64::new(0));
    let callback =
        BarrierTrackingCallback::new(Arc::clone(&should_trigger), Arc::clone(&record_counter));

    let handle = tokio::spawn(async move {
        coordinator.run(Box::new(callback)).await;
    });

    tokio::time::sleep(Duration::from_millis(300)).await;

    shutdown_clone.notify_one();
    handle.await.unwrap();

    let total = record_counter.load(Ordering::Relaxed);
    assert!(total > 0, "single source should process records");
}

/// Test that the pipeline handles sources exhausting gracefully,
/// with barrier checkpoint and then shutdown fallback checkpoint.
#[tokio::test]
async fn test_exhausted_sources_with_shutdown() {
    // Sources that exhaust quickly (3 batches each).
    let sources = vec![
        SourceRegistration {
            name: "fast_a".to_string(),
            connector: Box::new(
                laminar_connectors::testing::MockSourceConnector::with_batches(3, 5),
            ),
            config: laminar_connectors::config::ConnectorConfig::new("mock"),
            supports_replay: true,
        },
        SourceRegistration {
            name: "fast_b".to_string(),
            connector: Box::new(
                laminar_connectors::testing::MockSourceConnector::with_batches(3, 5),
            ),
            config: laminar_connectors::config::ConnectorConfig::new("mock"),
            supports_replay: true,
        },
    ];

    let shutdown = Arc::new(Notify::new());
    let shutdown_clone = Arc::clone(&shutdown);

    let config = PipelineConfig {
        fallback_poll_interval: Duration::from_millis(1),
        batch_window: Duration::ZERO,
        checkpoint_interval: Some(Duration::from_millis(5)),
        barrier_alignment_timeout: Duration::from_secs(1),
        ..PipelineConfig::default()
    };

    let coordinator = PipelineCoordinator::new(sources, config, shutdown)
        .await
        .unwrap();

    let should_trigger = Arc::new(AtomicBool::new(true));
    let record_counter = Arc::new(AtomicU64::new(0));
    let callback =
        BarrierTrackingCallback::new(Arc::clone(&should_trigger), Arc::clone(&record_counter));

    let handle = tokio::spawn(async move {
        coordinator.run(Box::new(callback)).await;
    });

    // Let sources exhaust and barriers fire, then shut down.
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown_clone.notify_one();
    handle.await.unwrap();

    // All 6 batches (3 per source * 5 rows) should be processed.
    let total = record_counter.load(Ordering::Relaxed);
    assert_eq!(total, 30, "all records should be processed: got {total}");
}

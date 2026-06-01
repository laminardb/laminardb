#![allow(clippy::disallowed_types)]
//! Unified checkpoint integration tests.

mod disk_persistence {
    use laminar_core::storage::checkpoint_store::{CheckpointStore, FileSystemCheckpointStore};
    use laminar_core::streaming::StreamCheckpointConfig;
    use laminar_db::{LaminarConfig, LaminarDB};

    fn config_with_storage(dir: &std::path::Path) -> LaminarConfig {
        LaminarConfig {
            storage_dir: Some(dir.to_path_buf()),
            checkpoint: Some(StreamCheckpointConfig {
                interval_ms: None, // manual only
                ..StreamCheckpointConfig::default()
            }),
            ..LaminarConfig::default()
        }
    }

    #[tokio::test]
    async fn test_manual_checkpoint_writes_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let storage = dir.path().to_path_buf();

        let db = LaminarDB::open_with_config(config_with_storage(&storage)).unwrap();

        db.execute("CREATE SOURCE sensors (ts BIGINT, device VARCHAR, value DOUBLE)")
            .await
            .unwrap();
        db.execute(
            "CREATE STREAM avg_val AS SELECT device, AVG(value) AS avg_v FROM sensors GROUP BY device",
        )
        .await
        .unwrap();
        db.execute("CREATE SINK out FROM avg_val").await.unwrap();

        db.start().await.unwrap();

        // Insert some data
        let source = db.source_untyped("sensors").unwrap();
        let schema = source.schema();
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                std::sync::Arc::new(arrow::array::Int64Array::from(vec![1, 2, 3])),
                std::sync::Arc::new(arrow::array::StringArray::from(vec!["a", "b", "a"])),
                std::sync::Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();
        source.push_arrow(batch).unwrap();

        // Manual checkpoint — this should persist to disk
        let result = db.checkpoint().await.unwrap();
        assert!(result.success, "checkpoint should succeed");
        assert_eq!(result.checkpoint_id, 1);

        // Verify files exist on disk
        let checkpoint_dir = storage.join("checkpoints");
        assert!(
            checkpoint_dir.exists(),
            "checkpoints directory should be created at {checkpoint_dir:?}"
        );

        // Verify the store can load the manifest
        let store = FileSystemCheckpointStore::new(&storage, 3);
        let manifest = store.load_latest().await.unwrap();
        assert!(manifest.is_some(), "manifest should be loadable from disk");

        let manifest = manifest.unwrap();
        assert_eq!(manifest.checkpoint_id, 1);
        assert_eq!(manifest.epoch, 1);

        db.close();
    }

    #[tokio::test]
    async fn test_checkpoint_errors_when_not_enabled() {
        let db = LaminarDB::open().unwrap(); // default config, no checkpoint

        let err = db.checkpoint().await;
        assert!(err.is_err(), "checkpoint should fail when not enabled");
    }

    #[tokio::test]
    async fn test_checkpoint_errors_before_start() {
        let dir = tempfile::tempdir().unwrap();
        let db = LaminarDB::open_with_config(config_with_storage(dir.path())).unwrap();

        // checkpoint enabled but start() not called — coordinator not initialized
        let err = db.checkpoint().await;
        assert!(err.is_err(), "checkpoint should fail before start()");
    }
}

mod exactly_once {
    use rustc_hash::FxHashMap;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::RecordBatch;
    use tokio::sync::Notify;

    use laminar_connectors::checkpoint::SourceCheckpoint;
    use laminar_core::storage::checkpoint_store::FileSystemCheckpointStore;
    use laminar_db::checkpoint_coordinator::{
        CheckpointConfig, CheckpointCoordinator, CheckpointRequest,
    };
    use laminar_db::pipeline::{
        PipelineCallback, PipelineConfig, SourceRegistration, StreamingCoordinator,
    };
    use laminar_db::recovery_manager::RecoveryManager;

    /// A callback that tracks barrier checkpoint calls and records state.
    struct BarrierTrackingCallback {
        cycle_count: u64,
        barrier_checkpoints: Vec<FxHashMap<String, SourceCheckpoint>>,
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

    impl PipelineCallback for BarrierTrackingCallback {
        async fn execute_cycle(
            &mut self,
            source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
            _watermark: i64,
        ) -> Result<FxHashMap<Arc<str>, Vec<RecordBatch>>, String> {
            self.cycle_count += 1;
            let records: u64 = source_batches
                .values()
                .flat_map(|v| v.iter())
                .map(|b| b.num_rows() as u64)
                .sum();
            self.total_records_processed
                .fetch_add(records, Ordering::Relaxed);
            Ok(FxHashMap::default())
        }

        fn push_to_streams(&self, _results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {}

        async fn write_to_sinks(&mut self, _results: &FxHashMap<Arc<str>, Vec<RecordBatch>>) {}

        fn extract_watermark(&mut self, _source_name: &str, _batch: &RecordBatch) {}

        fn filter_late_rows(&self, _source_name: &str, batch: &RecordBatch) -> Option<RecordBatch> {
            Some(batch.clone())
        }

        fn current_watermark(&self) -> i64 {
            0
        }

        async fn maybe_checkpoint(
            &mut self,
            force: bool,
            _source_offsets: rustc_hash::FxHashMap<
                String,
                laminar_connectors::checkpoint::SourceCheckpoint,
            >,
        ) -> Option<u64> {
            if force {
                self.force_checkpoints += 1;
                return Some(self.force_checkpoints);
            }
            if self.should_trigger.load(Ordering::Relaxed) {
                Some(1)
            } else {
                None
            }
        }

        async fn checkpoint_with_barrier(
            &mut self,
            source_checkpoints: FxHashMap<String, SourceCheckpoint>,
        ) -> laminar_db::pipeline::BarrierOutcome {
            let epoch = self.barrier_checkpoints.len() as u64 + 1;
            self.barrier_checkpoints.push(source_checkpoints);
            laminar_db::pipeline::BarrierOutcome::Committed(epoch)
        }

        fn record_cycle(&self, _events_ingested: u64, _batches: u64, _elapsed_ns: u64) {}

        async fn poll_tables(&mut self) {}

        fn apply_control(&mut self, _msg: laminar_db::pipeline::ControlMsg) {}
    }

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
                restore_checkpoint: None,
            },
            SourceRegistration {
                name: "src_b".to_string(),
                connector: Box::new(
                    laminar_connectors::testing::MockSourceConnector::with_batches(50, 10),
                ),
                config: laminar_connectors::config::ConnectorConfig::new("mock"),
                supports_replay: true,
                restore_checkpoint: None,
            },
        ];

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = Arc::clone(&shutdown);

        let config = PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            batch_window: Duration::ZERO,
            checkpoint_interval: Some(Duration::from_millis(10)),
            barrier_alignment_timeout: Duration::from_secs(5),
            ..PipelineConfig::default()
        };

        let (_control_tx, control_rx) =
            crossfire::mpsc::bounded_async::<laminar_db::pipeline::ControlMsg>(64);
        let coordinator = StreamingCoordinator::new(sources, config, shutdown, control_rx)
            .await
            .unwrap();

        let should_trigger = Arc::new(AtomicBool::new(true));
        let record_counter = Arc::new(AtomicU64::new(0));
        let callback =
            BarrierTrackingCallback::new(Arc::clone(&should_trigger), Arc::clone(&record_counter));

        let handle = tokio::spawn(async move {
            coordinator.run(callback).await;
        });

        tokio::time::sleep(Duration::from_millis(500)).await;

        shutdown_clone.notify_one();
        handle.await.unwrap();

        let total = record_counter.load(Ordering::Relaxed);
        assert!(
            total > 0,
            "pipeline should have processed records, got {total}"
        );
    }

    #[tokio::test]
    async fn test_notify_epoch_committed_propagates_to_sources() {
        let src_a = laminar_connectors::testing::MockSourceConnector::with_batches(50, 10);
        let src_b = laminar_connectors::testing::MockSourceConnector::with_batches(50, 10);
        let epochs_a = src_a.committed_epochs_handle();
        let epochs_b = src_b.committed_epochs_handle();

        let sources = vec![
            SourceRegistration {
                name: "src_a".to_string(),
                connector: Box::new(src_a),
                config: laminar_connectors::config::ConnectorConfig::new("mock"),
                supports_replay: true,
                restore_checkpoint: None,
            },
            SourceRegistration {
                name: "src_b".to_string(),
                connector: Box::new(src_b),
                config: laminar_connectors::config::ConnectorConfig::new("mock"),
                supports_replay: true,
                restore_checkpoint: None,
            },
        ];

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = Arc::clone(&shutdown);

        let config = PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            batch_window: Duration::ZERO,
            checkpoint_interval: Some(Duration::from_millis(10)),
            barrier_alignment_timeout: Duration::from_secs(5),
            ..PipelineConfig::default()
        };

        let (_control_tx, control_rx) =
            crossfire::mpsc::bounded_async::<laminar_db::pipeline::ControlMsg>(64);
        let coordinator = StreamingCoordinator::new(sources, config, shutdown, control_rx)
            .await
            .unwrap();

        let should_trigger = Arc::new(AtomicBool::new(true));
        let record_counter = Arc::new(AtomicU64::new(0));
        let callback =
            BarrierTrackingCallback::new(Arc::clone(&should_trigger), Arc::clone(&record_counter));

        let handle = tokio::spawn(async move {
            coordinator.run(callback).await;
        });

        tokio::time::sleep(Duration::from_millis(500)).await;
        shutdown_clone.notify_one();
        handle.await.unwrap();

        for (label, epochs) in [("src_a", &epochs_a), ("src_b", &epochs_b)] {
            let observed = epochs.lock().clone();
            assert!(
                !observed.is_empty(),
                "{label}: expected at least one notify_epoch_committed call, got {observed:?}"
            );
            assert!(
                observed.windows(2).all(|w| w[0] <= w[1]),
                "{label}: epochs must be non-decreasing, got {observed:?}"
            );
        }
    }

    #[tokio::test]
    async fn test_barrier_checkpoint_recovery_round_trip() {
        let dir = tempfile::tempdir().unwrap();

        // Phase 1: Run pipeline, trigger barrier checkpoint, persist.
        let store = Box::new(FileSystemCheckpointStore::new(dir.path(), 5));
        let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();

        let mut operator_states = HashMap::new();
        operator_states.insert(
            "stream_executor".to_string(),
            bytes::Bytes::from_static(b"barrier-consistent-state"),
        );

        let mut source_overrides = HashMap::new();
        source_overrides.insert(
            "src_a".to_string(),
            laminar_core::storage::checkpoint_manifest::ConnectorCheckpoint {
                offsets: HashMap::from([("records".into(), "500".into())]),
                epoch: 1,
                metadata: HashMap::new(),
            },
        );
        source_overrides.insert(
            "src_b".to_string(),
            laminar_core::storage::checkpoint_manifest::ConnectorCheckpoint {
                offsets: HashMap::from([("records".into(), "300".into())]),
                epoch: 1,
                metadata: HashMap::new(),
            },
        );

        let mut source_watermarks = HashMap::new();
        source_watermarks.insert("src_a".to_string(), 5000_i64);
        source_watermarks.insert("src_b".to_string(), 4500_i64);

        let result = coord
            .checkpoint_with_offsets(CheckpointRequest {
                operator_states,
                watermark: Some(4500),
                source_offset_overrides: source_overrides,
                source_watermarks,
                pipeline_hash: Some(0xDEAD_BEEF),
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();

        assert!(result.success, "barrier checkpoint should succeed");
        assert_eq!(result.epoch, 1);

        drop(coord);

        let store = FileSystemCheckpointStore::new(dir.path(), 5);
        let mgr = RecoveryManager::new(&store);
        let manifest = mgr.load_latest().await.unwrap().unwrap();

        assert_eq!(manifest.epoch, 1);
        assert_eq!(manifest.watermark, Some(4500));

        let src_a = manifest.source_offsets.get("src_a").unwrap();
        assert_eq!(
            src_a.offsets.get("records"),
            Some(&"500".to_string()),
            "src_a offset should be captured at barrier point"
        );
        let src_b = manifest.source_offsets.get("src_b").unwrap();
        assert_eq!(
            src_b.offsets.get("records"),
            Some(&"300".to_string()),
            "src_b offset should be captured at barrier point"
        );

        let op_state = manifest.operator_states.get("stream_executor").unwrap();
        assert_eq!(
            op_state.decode_inline().unwrap(),
            b"barrier-consistent-state"
        );

        assert_eq!(manifest.source_watermarks.get("src_a"), Some(&5000));
        assert_eq!(manifest.source_watermarks.get("src_b"), Some(&4500));
        assert_eq!(manifest.pipeline_hash, Some(0xDEAD_BEEF));
    }

    #[tokio::test]
    async fn test_single_source_barrier_checkpoint() {
        let sources = vec![SourceRegistration {
            name: "only_source".to_string(),
            connector: Box::new(
                laminar_connectors::testing::MockSourceConnector::with_batches(100, 5),
            ),
            config: laminar_connectors::config::ConnectorConfig::new("mock"),
            supports_replay: true,
            restore_checkpoint: None,
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

        let (_control_tx, control_rx) =
            crossfire::mpsc::bounded_async::<laminar_db::pipeline::ControlMsg>(64);
        let coordinator = StreamingCoordinator::new(sources, config, shutdown, control_rx)
            .await
            .unwrap();

        let should_trigger = Arc::new(AtomicBool::new(true));
        let record_counter = Arc::new(AtomicU64::new(0));
        let callback =
            BarrierTrackingCallback::new(Arc::clone(&should_trigger), Arc::clone(&record_counter));

        let handle = tokio::spawn(async move {
            coordinator.run(callback).await;
        });

        tokio::time::sleep(Duration::from_millis(300)).await;

        shutdown_clone.notify_one();
        handle.await.unwrap();

        let total = record_counter.load(Ordering::Relaxed);
        assert!(total > 0, "single source should process records");
    }

    #[tokio::test]
    async fn test_exhausted_sources_with_shutdown() {
        let sources = vec![
            SourceRegistration {
                name: "fast_a".to_string(),
                connector: Box::new(
                    laminar_connectors::testing::MockSourceConnector::with_batches(3, 5),
                ),
                config: laminar_connectors::config::ConnectorConfig::new("mock"),
                supports_replay: true,
                restore_checkpoint: None,
            },
            SourceRegistration {
                name: "fast_b".to_string(),
                connector: Box::new(
                    laminar_connectors::testing::MockSourceConnector::with_batches(3, 5),
                ),
                config: laminar_connectors::config::ConnectorConfig::new("mock"),
                supports_replay: true,
                restore_checkpoint: None,
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

        let (_control_tx, control_rx) =
            crossfire::mpsc::bounded_async::<laminar_db::pipeline::ControlMsg>(64);
        let coordinator = StreamingCoordinator::new(sources, config, shutdown, control_rx)
            .await
            .unwrap();

        let should_trigger = Arc::new(AtomicBool::new(true));
        let record_counter = Arc::new(AtomicU64::new(0));
        let callback =
            BarrierTrackingCallback::new(Arc::clone(&should_trigger), Arc::clone(&record_counter));

        let handle = tokio::spawn(async move {
            coordinator.run(callback).await;
        });

        tokio::time::sleep(Duration::from_millis(500)).await;
        shutdown_clone.notify_one();
        handle.await.unwrap();

        let total = record_counter.load(Ordering::Relaxed);
        assert!(
            total >= 15,
            "at least one source should fully drain: got {total}/30"
        );
    }
}

mod performance {
    use async_trait::async_trait;
    use laminar_core::storage::checkpoint_manifest::CheckpointManifest;
    use laminar_core::storage::checkpoint_store::{CheckpointStore, CheckpointStoreError};
    use laminar_db::checkpoint_coordinator::{
        CheckpointConfig, CheckpointCoordinator, CheckpointRequest,
    };
    use std::time::{Duration, Instant};

    struct SlowCheckpointStore {
        delay: Duration,
    }

    impl SlowCheckpointStore {
        fn new(delay: Duration) -> Self {
            Self { delay }
        }
    }

    #[async_trait]
    impl CheckpointStore for SlowCheckpointStore {
        async fn save(&self, _manifest: &CheckpointManifest) -> Result<(), CheckpointStoreError> {
            tokio::time::sleep(self.delay).await;
            Ok(())
        }

        async fn load_latest(&self) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
            Ok(None)
        }

        async fn load_by_id(
            &self,
            _id: u64,
        ) -> Result<Option<CheckpointManifest>, CheckpointStoreError> {
            Ok(None)
        }

        async fn list(&self) -> Result<Vec<(u64, u64)>, CheckpointStoreError> {
            Ok(vec![])
        }

        async fn prune(&self, _keep_count: usize) -> Result<usize, CheckpointStoreError> {
            Ok(0)
        }

        async fn save_state_data(
            &self,
            _id: u64,
            _chunks: &[bytes::Bytes],
        ) -> Result<(), CheckpointStoreError> {
            Ok(())
        }

        async fn load_state_data(&self, _id: u64) -> Result<Option<Vec<u8>>, CheckpointStoreError> {
            Ok(None)
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore]
    async fn test_checkpoint_non_blocking() {
        let delay = Duration::from_millis(200);
        let store = Box::new(SlowCheckpointStore::new(delay));
        let mut coordinator = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();

        let (tx, rx) = crossfire::mpsc::bounded_async::<Duration>(100);

        let ticker = tokio::spawn(async move {
            let mut last_tick = Instant::now();
            loop {
                tokio::time::sleep(Duration::from_millis(10)).await;
                let now = Instant::now();
                let elapsed = now.duration_since(last_tick);
                if tx.send(elapsed).await.is_err() {
                    break;
                }
                last_tick = now;
            }
        });

        let start = Instant::now();
        let result = coordinator
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();
        let duration = start.elapsed();

        assert!(result.success);
        assert!(
            duration >= delay,
            "Checkpoint duration ({:?}) should be at least delay ({:?})",
            duration,
            delay
        );

        ticker.abort();

        let mut max_interval = Duration::ZERO;
        let mut count = 0;
        while let Ok(interval) = rx.try_recv() {
            if interval > max_interval {
                max_interval = interval;
            }
            count += 1;
        }

        println!("Ticks collected: {}", count);
        println!("Max tick interval: {:?}", max_interval);
        println!("Checkpoint duration: {:?}", duration);

        assert!(
            max_interval < Duration::from_millis(100),
            "Checkpoint blocked the runtime! Max interval: {:?} (expected < 100ms)",
            max_interval
        );
    }
}

mod recovery {
    use std::collections::HashMap;

    use laminar_core::storage::checkpoint_manifest::{
        CheckpointManifest, ConnectorCheckpoint, OperatorCheckpoint,
    };
    use laminar_core::storage::checkpoint_store::{CheckpointStore, FileSystemCheckpointStore};
    use laminar_db::checkpoint_coordinator::{
        CheckpointConfig, CheckpointCoordinator, CheckpointRequest,
    };
    use laminar_db::recovery_manager::RecoveryManager;

    fn make_store(dir: &std::path::Path) -> FileSystemCheckpointStore {
        FileSystemCheckpointStore::new(dir, 5)
    }

    async fn make_coordinator(dir: &std::path::Path) -> CheckpointCoordinator {
        let store = Box::new(make_store(dir));
        CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_happy_path_checkpoint_and_recovery() {
        let dir = tempfile::tempdir().unwrap();

        let mut coord = make_coordinator(dir.path()).await;

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

        let store = make_store(dir.path());
        let mgr = RecoveryManager::new(&store);

        let manifest = mgr.load_latest().await.unwrap().unwrap();
        assert_eq!(manifest.checkpoint_id, 1);
        assert_eq!(manifest.epoch, 1);
        assert_eq!(manifest.watermark, Some(5000));

        let op = manifest.operator_states.get("window-agg").unwrap();
        assert_eq!(op.decode_inline().unwrap(), b"accumulated-state");
    }

    #[tokio::test]
    async fn test_recovery_fresh_start() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());
        let mgr = RecoveryManager::new(&store);

        let result = mgr.load_latest().await.unwrap();
        assert!(result.is_none(), "fresh start should return None");
    }

    #[tokio::test]
    async fn test_recover_latest_of_multiple_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        for i in 1..=3 {
            let mut m = CheckpointManifest::new(i, i);
            m.watermark = Some(i as i64 * 1000);
            store.save(&m).await.unwrap();
        }

        let mgr = RecoveryManager::new(&store);
        let manifest = mgr.load_latest().await.unwrap().unwrap();

        assert_eq!(manifest.epoch, 3);
        assert_eq!(manifest.watermark, Some(3000));
    }

    #[tokio::test]
    async fn test_checkpoint_source_offsets_round_trip() {
        let dir = tempfile::tempdir().unwrap();

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

        let mgr = RecoveryManager::new(&store);
        let manifest = mgr.load_latest().await.unwrap().unwrap();

        let kafka = manifest.source_offsets.get("kafka-trades").unwrap();
        assert_eq!(kafka.offsets.get("partition-0"), Some(&"1234".into()));
        assert_eq!(kafka.offsets.get("partition-1"), Some(&"5678".into()));
        assert_eq!(kafka.metadata.get("topic"), Some(&"trades".into()));

        let pg = manifest.source_offsets.get("pg-orders").unwrap();
        assert_eq!(pg.offsets.get("lsn"), Some(&"0/ABCDEF".into()));
    }

    #[tokio::test]
    async fn test_operator_state_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

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

        let mgr = RecoveryManager::new(&store);
        let manifest = mgr.load_latest().await.unwrap().unwrap();

        assert_eq!(manifest.operator_states.len(), 2);

        let w = manifest.operator_states.get("stream_executor").unwrap();
        assert_eq!(w.decode_inline().unwrap(), executor_state);

        let f = manifest.operator_states.get("filter").unwrap();
        assert_eq!(f.decode_inline().unwrap(), filter_state);
    }

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

        let store = make_store(dir.path());
        let mgr = RecoveryManager::new(&store);
        let manifest = mgr.load_latest().await.unwrap().unwrap();

        assert_eq!(
            manifest.table_store_checkpoint_path.as_deref(),
            Some("/data/rocksdb_cp_001")
        );
    }

    #[tokio::test]
    async fn test_coordinator_resumes_epoch_after_recovery() {
        let dir = tempfile::tempdir().unwrap();

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

            assert_eq!(coord.epoch(), 3);
            assert_eq!(coord.next_checkpoint_id(), 3);
        }

        let coord2 = make_coordinator(dir.path()).await;
        assert_eq!(coord2.epoch(), 3);
        assert_eq!(coord2.next_checkpoint_id(), 3);
    }

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

    #[tokio::test]
    async fn test_checkpoint_store_prune_keeps_latest() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        for i in 1..=5 {
            let m = CheckpointManifest::new(i, i);
            store.save(&m).await.unwrap();
        }

        let all = store.list().await.unwrap();
        assert_eq!(all.len(), 5);

        let pruned = store.prune(2).await.unwrap();
        assert_eq!(pruned, 3);

        let remaining = store.list().await.unwrap();
        assert_eq!(remaining.len(), 2);

        let latest = store.load_latest().await.unwrap().unwrap();
        assert_eq!(latest.checkpoint_id, 5);
    }

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

        let json = serde_json::to_string_pretty(&manifest).unwrap();

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
}

mod restart {
    use std::sync::Arc;

    use arrow::array::{Float64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
    use laminar_core::storage::checkpoint_store::{CheckpointStore, FileSystemCheckpointStore};
    use laminar_core::streaming::StreamCheckpointConfig;
    use laminar_db::{LaminarConfig, LaminarDB};

    fn config_for(dir: &std::path::Path) -> LaminarConfig {
        LaminarConfig {
            storage_dir: Some(dir.to_path_buf()),
            checkpoint: Some(StreamCheckpointConfig {
                interval_ms: None,
                ..StreamCheckpointConfig::default()
            }),
            ..LaminarConfig::default()
        }
    }

    fn make_batch(symbol: &str, price: f64, ts_ms: i64) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("symbol", Arc::new(StringArray::from(vec![symbol])) as _),
            ("price", Arc::new(Float64Array::from(vec![price])) as _),
            (
                "ts",
                Arc::new(TimestampMicrosecondArray::from(vec![ts_ms * 1000])) as _,
            ),
        ])
        .unwrap()
    }

    #[tokio::test]
    async fn test_checkpoint_survives_restart() {
        let dir = tempfile::tempdir().unwrap();
        let storage = dir.path().to_path_buf();

        {
            let db = LaminarDB::open_with_config(config_for(&storage)).unwrap();

            db.execute(
                "CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts TIMESTAMP, \
                 WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)",
            )
            .await
            .unwrap();

            db.execute(
                "CREATE STREAM avg_price AS \
                 SELECT symbol, AVG(price) AS avg_p \
                 FROM trades GROUP BY symbol",
            )
            .await
            .unwrap();

            db.start().await.unwrap();

            let source = db.source_untyped("trades").unwrap();
            for i in 0..10 {
                source
                    .push_arrow(make_batch(
                        "AAPL",
                        100.0 + f64::from(i),
                        i64::from(i) * 1000,
                    ))
                    .unwrap();
            }

            tokio::time::sleep(std::time::Duration::from_millis(200)).await;

            let cp = db.checkpoint().await.unwrap();
            assert!(cp.success, "checkpoint must succeed");
            assert!(cp.checkpoint_id > 0);

            db.close();
        }

        {
            let store = FileSystemCheckpointStore::new(&storage, 3);
            let manifest = store.load_latest().await.unwrap();
            assert!(
                manifest.is_some(),
                "manifest must be loadable after restart"
            );
            let manifest = manifest.unwrap();
            assert!(manifest.checkpoint_id > 0);
            assert!(manifest.epoch > 0, "epoch must be > 0 after checkpoint");
        }

        {
            let db = LaminarDB::open_with_config(config_for(&storage)).unwrap();

            db.execute(
                "CREATE SOURCE trades (symbol VARCHAR, price DOUBLE, ts TIMESTAMP, \
                 WATERMARK FOR ts AS ts - INTERVAL '1' SECOND)",
            )
            .await
            .unwrap();

            db.execute(
                "CREATE STREAM avg_price AS \
                 SELECT symbol, AVG(price) AS avg_p \
                 FROM trades GROUP BY symbol",
            )
            .await
            .unwrap();

            db.start().await.unwrap();

            let source = db.source_untyped("trades").unwrap();
            source
                .push_arrow(make_batch("AAPL", 200.0, 20_000))
                .unwrap();

            tokio::time::sleep(std::time::Duration::from_millis(200)).await;

            let metrics = db.metrics();
            assert!(
                metrics.total_cycles > 0,
                "pipeline must have executed cycles after restart"
            );

            db.close();
        }
    }
}

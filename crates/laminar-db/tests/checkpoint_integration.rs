#![allow(clippy::disallowed_types)]
//! Unified checkpoint integration tests.

type DecisionStore = laminar_core::checkpoint_decision::CheckpointDecisionStore;

fn in_memory_decision_store() -> std::sync::Arc<DecisionStore> {
    std::sync::Arc::new(
        laminar_core::checkpoint_decision::CheckpointDecisionStore::new(std::sync::Arc::new(
            object_store::memory::InMemory::new(),
        )),
    )
}

async fn bind_in_memory_decision_store(
    coordinator: &mut laminar_db::checkpoint_coordinator::CheckpointCoordinator,
) {
    coordinator
        .bind_durable_decision_store(in_memory_decision_store())
        .await
        .unwrap();
}

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
        let store = FileSystemCheckpointStore::new(&storage);
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
    use laminar_connectors::connector::{SourceConsistency, SourceContract, SourceTopology};
    use laminar_core::state::CheckpointAttempt;
    use laminar_core::storage::checkpoint_store::{CheckpointStore, FileSystemCheckpointStore};
    use laminar_db::checkpoint_coordinator::{
        CheckpointConfig, CheckpointCoordinator, CheckpointRequest,
    };
    use laminar_db::pipeline::{
        CycleError, CycleOutcome, PipelineCallback, PipelineConfig, SourceRegistration,
        StreamingCoordinator,
    };

    /// A callback that tracks barrier checkpoint calls and records state.
    struct BarrierTrackingCallback {
        cycle_count: u64,
        barrier_checkpoints: Vec<FxHashMap<String, SourceCheckpoint>>,
        should_trigger: Arc<AtomicBool>,
        total_records_processed: Arc<AtomicU64>,
        barrier_counter: Option<Arc<AtomicU64>>,
        next_attempt: u64,
    }

    impl BarrierTrackingCallback {
        fn new(should_trigger: Arc<AtomicBool>, record_counter: Arc<AtomicU64>) -> Self {
            Self {
                cycle_count: 0,
                barrier_checkpoints: Vec::new(),
                should_trigger,
                total_records_processed: record_counter,
                barrier_counter: None,
                next_attempt: 1,
            }
        }

        fn with_barrier_counter(mut self, counter: Arc<AtomicU64>) -> Self {
            self.barrier_counter = Some(counter);
            self
        }
    }

    impl PipelineCallback for BarrierTrackingCallback {
        async fn execute_cycle(
            &mut self,
            source_batches: &FxHashMap<Arc<str>, Vec<RecordBatch>>,
            _watermark: i64,
        ) -> Result<CycleOutcome, CycleError> {
            self.cycle_count += 1;
            let records: u64 = source_batches
                .values()
                .flat_map(|v| v.iter())
                .map(|b| b.num_rows() as u64)
                .sum();
            self.total_records_processed
                .fetch_add(records, Ordering::Relaxed);
            Ok(CycleOutcome::clean(FxHashMap::default()))
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

        async fn service_checkpoint_control(
            &mut self,
            _source_offsets: rustc_hash::FxHashMap<
                String,
                laminar_connectors::checkpoint::SourceCheckpoint,
            >,
        ) -> Option<u64> {
            if self.should_trigger.load(Ordering::Relaxed) {
                Some(1)
            } else {
                None
            }
        }

        async fn reserve_checkpoint_attempt(
            &mut self,
            _attempt_started: std::time::Instant,
        ) -> Result<CheckpointAttempt, String> {
            let id = self.next_attempt;
            self.next_attempt = self
                .next_attempt
                .checked_add(1)
                .ok_or_else(|| "test checkpoint attempt space exhausted".to_string())?;
            Ok(CheckpointAttempt::new(id, id))
        }

        async fn checkpoint_with_barrier(
            &mut self,
            source_checkpoints: FxHashMap<String, SourceCheckpoint>,
            attempt: CheckpointAttempt,
            _attempt_started: std::time::Instant,
        ) -> laminar_db::pipeline::BarrierOutcome {
            self.barrier_checkpoints.push(source_checkpoints);
            if let Some(ref counter) = self.barrier_counter {
                counter.fetch_add(1, Ordering::Relaxed);
            }
            laminar_db::pipeline::BarrierOutcome::Committed(attempt.epoch)
        }

        fn record_cycle(&self, _events_ingested: u64, _batches: u64, _elapsed_ns: u64) {}

        async fn poll_tables(&mut self) {}

        fn apply_control(&mut self, _msg: laminar_db::pipeline::ControlMsg) {}
    }

    // The source-intake gate (held closed during a coordinated round until the restore quorum)
    // must stop the source from producing until released — the invariant that keeps a rewound
    // node from re-shuffling its replay into a peer whose receiver hasn't rebound.
    #[tokio::test]
    async fn source_gate_holds_intake_until_released() {
        let sources = vec![SourceRegistration {
            name: "src".to_string(),
            connector: Box::new(
                laminar_connectors::testing::MockSourceConnector::with_batches(10_000, 10),
            ),
            config: laminar_connectors::config::ConnectorConfig::new("mock"),
            contract: SourceContract::new(
                SourceConsistency::Replayable,
                SourceTopology::Splittable,
            ),
            position: laminar_connectors::connector::SourcePosition::Initial,
        }];
        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = Arc::clone(&shutdown);
        let config = PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            batch_window: Duration::ZERO,
            ..PipelineConfig::default()
        };
        let (_control_tx, control_rx) =
            crossfire::mpsc::bounded_async::<laminar_db::pipeline::ControlMsg>(64);

        // Start with the gate CLOSED.
        let gate = Arc::new(AtomicBool::new(true));
        let coordinator =
            StreamingCoordinator::new(sources, config, shutdown, control_rx, Arc::clone(&gate))
                .await
                .unwrap();
        let record_counter = Arc::new(AtomicU64::new(0));
        let callback = BarrierTrackingCallback::new(
            Arc::new(AtomicBool::new(false)),
            Arc::clone(&record_counter),
        );
        let handle = tokio::spawn(async move { coordinator.run(callback).await });

        // Gated: no records flow.
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            record_counter.load(Ordering::Relaxed),
            0,
            "source produced records while the intake gate was closed"
        );

        // Released: records flow.
        gate.store(false, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            record_counter.load(Ordering::Relaxed) > 0,
            "source produced no records after the gate was released"
        );

        shutdown_clone.notify_one();
        handle.await.unwrap();
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
                contract: SourceContract::new(
                    SourceConsistency::Replayable,
                    SourceTopology::Splittable,
                ),
                position: laminar_connectors::connector::SourcePosition::Initial,
            },
            SourceRegistration {
                name: "src_b".to_string(),
                connector: Box::new(
                    laminar_connectors::testing::MockSourceConnector::with_batches(50, 10),
                ),
                config: laminar_connectors::config::ConnectorConfig::new("mock"),
                contract: SourceContract::new(
                    SourceConsistency::Replayable,
                    SourceTopology::Splittable,
                ),
                position: laminar_connectors::connector::SourcePosition::Initial,
            },
        ];

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = Arc::clone(&shutdown);

        let config = PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            batch_window: Duration::ZERO,
            checkpoint_interval: Some(Duration::from_millis(10)),
            checkpoint_timeout: Duration::from_secs(5),
            ..PipelineConfig::default()
        };

        let (_control_tx, control_rx) =
            crossfire::mpsc::bounded_async::<laminar_db::pipeline::ControlMsg>(64);
        let coordinator = StreamingCoordinator::new(
            sources,
            config,
            shutdown,
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
        .await
        .unwrap();

        let should_trigger = Arc::new(AtomicBool::new(true));
        let record_counter = Arc::new(AtomicU64::new(0));
        let barrier_counter = Arc::new(AtomicU64::new(0));
        let callback =
            BarrierTrackingCallback::new(Arc::clone(&should_trigger), Arc::clone(&record_counter))
                .with_barrier_counter(Arc::clone(&barrier_counter));

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

        let barriers = barrier_counter.load(Ordering::Relaxed);
        assert!(
            barriers > 0,
            "pipeline should have committed at least one barrier-aligned checkpoint, got {barriers}"
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
                contract: SourceContract::new(
                    SourceConsistency::Replayable,
                    SourceTopology::Splittable,
                ),
                position: laminar_connectors::connector::SourcePosition::Initial,
            },
            SourceRegistration {
                name: "src_b".to_string(),
                connector: Box::new(src_b),
                config: laminar_connectors::config::ConnectorConfig::new("mock"),
                contract: SourceContract::new(
                    SourceConsistency::Replayable,
                    SourceTopology::Splittable,
                ),
                position: laminar_connectors::connector::SourcePosition::Initial,
            },
        ];

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = Arc::clone(&shutdown);

        let config = PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            batch_window: Duration::ZERO,
            checkpoint_interval: Some(Duration::from_millis(10)),
            checkpoint_timeout: Duration::from_secs(5),
            ..PipelineConfig::default()
        };

        let (_control_tx, control_rx) =
            crossfire::mpsc::bounded_async::<laminar_db::pipeline::ControlMsg>(64);
        let coordinator = StreamingCoordinator::new(
            sources,
            config,
            shutdown,
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
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

        // Run pipeline, trigger barrier checkpoint, persist.
        let store = Box::new(FileSystemCheckpointStore::new(dir.path()));
        let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();
        super::bind_in_memory_decision_store(&mut coord).await;

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
                metadata: HashMap::new(),
            },
        );
        source_overrides.insert(
            "src_b".to_string(),
            laminar_core::storage::checkpoint_manifest::ConnectorCheckpoint {
                offsets: HashMap::from([("records".into(), "300".into())]),
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
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();

        assert!(result.success, "barrier checkpoint should succeed");
        assert_eq!(result.epoch, 1);

        drop(coord);

        let store = FileSystemCheckpointStore::new(dir.path());
        let manifest = store.load_latest().await.unwrap().unwrap();

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
        assert_eq!(
            manifest.pipeline_identity,
            laminar_core::storage::checkpoint_manifest::PipelineIdentity::empty()
        );
    }

    #[tokio::test]
    async fn test_single_source_barrier_checkpoint() {
        let sources = vec![SourceRegistration {
            name: "only_source".to_string(),
            connector: Box::new(
                laminar_connectors::testing::MockSourceConnector::with_batches(100, 5),
            ),
            config: laminar_connectors::config::ConnectorConfig::new("mock"),
            contract: SourceContract::new(
                SourceConsistency::Replayable,
                SourceTopology::Splittable,
            ),
            position: laminar_connectors::connector::SourcePosition::Initial,
        }];

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = Arc::clone(&shutdown);

        let config = PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            batch_window: Duration::ZERO,
            checkpoint_interval: Some(Duration::from_millis(10)),
            checkpoint_timeout: Duration::from_secs(5),
            ..PipelineConfig::default()
        };

        let (_control_tx, control_rx) =
            crossfire::mpsc::bounded_async::<laminar_db::pipeline::ControlMsg>(64);
        let coordinator = StreamingCoordinator::new(
            sources,
            config,
            shutdown,
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
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
                contract: SourceContract::new(
                    SourceConsistency::Replayable,
                    SourceTopology::Splittable,
                ),
                position: laminar_connectors::connector::SourcePosition::Initial,
            },
            SourceRegistration {
                name: "fast_b".to_string(),
                connector: Box::new(
                    laminar_connectors::testing::MockSourceConnector::with_batches(3, 5),
                ),
                config: laminar_connectors::config::ConnectorConfig::new("mock"),
                contract: SourceContract::new(
                    SourceConsistency::Replayable,
                    SourceTopology::Splittable,
                ),
                position: laminar_connectors::connector::SourcePosition::Initial,
            },
        ];

        let shutdown = Arc::new(Notify::new());
        let shutdown_clone = Arc::clone(&shutdown);

        let config = PipelineConfig {
            fallback_poll_interval: Duration::from_millis(1),
            batch_window: Duration::ZERO,
            checkpoint_interval: Some(Duration::from_millis(5)),
            checkpoint_timeout: Duration::from_secs(1),
            ..PipelineConfig::default()
        };

        let (_control_tx, control_rx) =
            crossfire::mpsc::bounded_async::<laminar_db::pipeline::ControlMsg>(64);
        let coordinator = StreamingCoordinator::new(
            sources,
            config,
            shutdown,
            control_rx,
            Arc::new(AtomicBool::new(false)),
        )
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
        async fn list_ids(&self) -> Result<Vec<u64>, CheckpointStoreError> {
            Ok(vec![])
        }

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

        async fn prune_before(&self, _before_epoch: u64) -> Result<usize, CheckpointStoreError> {
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
        super::bind_in_memory_decision_store(&mut coordinator).await;

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

    #[tokio::test(start_paused = true)]
    async fn checkpoint_uses_one_end_to_end_attempt_deadline() {
        let store = Box::new(SlowCheckpointStore::new(Duration::from_secs(30)));
        let mut config = CheckpointConfig::default();
        config.checkpoint_timeout = Duration::from_millis(100);
        let mut coordinator = CheckpointCoordinator::new(config, store).await.unwrap();
        super::bind_in_memory_decision_store(&mut coordinator).await;

        let started = tokio::time::Instant::now();
        let result = coordinator
            .checkpoint(CheckpointRequest::default())
            .await
            .unwrap();

        assert!(!result.success);
        assert!(result
            .error
            .as_deref()
            .is_some_and(|error| error.contains("end-to-end deadline")));
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a 30s store operation must be cut off by the 100ms attempt deadline"
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

    fn make_store(dir: &std::path::Path) -> FileSystemCheckpointStore {
        FileSystemCheckpointStore::new(dir)
    }

    async fn save_finalized(store: &FileSystemCheckpointStore, manifest: &CheckpointManifest) {
        store.save_with_state(manifest, None).await.unwrap();
        store.finalize(manifest.checkpoint_id).await.unwrap();
    }

    async fn make_coordinator(dir: &std::path::Path) -> CheckpointCoordinator {
        let store = Box::new(make_store(dir));
        let mut coordinator = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();
        super::bind_in_memory_decision_store(&mut coordinator).await;
        coordinator
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
        let manifest = store.load_latest().await.unwrap().unwrap();
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

        let result = store.load_latest().await.unwrap();
        assert!(result.is_none(), "fresh start should return None");
    }

    #[tokio::test]
    async fn test_recover_latest_of_multiple_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let store = make_store(dir.path());

        for i in 1..=3 {
            let mut m = CheckpointManifest::new(i, i);
            m.watermark = Some(i as i64 * 1000);
            save_finalized(&store, &m).await;
        }

        let manifest = store.load_latest().await.unwrap().unwrap();

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
                    ("trades:0".into(), "1234".into()),
                    ("trades:1".into(), "5678".into()),
                ]),
                metadata: HashMap::from([("topic".into(), "trades".into())]),
            },
        );
        manifest.source_offsets.insert(
            "pg-orders".into(),
            ConnectorCheckpoint {
                offsets: HashMap::from([("lsn".into(), "0/ABCDEF".into())]),
                metadata: HashMap::from([("slot".into(), "laminar_slot".into())]),
            },
        );
        save_finalized(&store, &manifest).await;

        let manifest = store.load_latest().await.unwrap().unwrap();

        let kafka = manifest.source_offsets.get("kafka-trades").unwrap();
        assert_eq!(kafka.offsets.get("trades:0"), Some(&"1234".into()));
        assert_eq!(kafka.offsets.get("trades:1"), Some(&"5678".into()));
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
        save_finalized(&store, &manifest).await;

        let manifest = store.load_latest().await.unwrap().unwrap();

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
                table_store_checkpoint_path: Some("/data/table_store_cp_001".into()),
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();

        assert!(result.success);

        let store = make_store(dir.path());
        let manifest = store.load_latest().await.unwrap().unwrap();

        assert_eq!(
            manifest.table_store_checkpoint_path.as_deref(),
            Some("/data/table_store_cp_001")
        );
    }

    #[tokio::test]
    async fn test_coordinator_resumes_epoch_and_durable_id_after_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let reservation_objects: std::sync::Arc<dyn object_store::ObjectStore> =
            std::sync::Arc::new(object_store::memory::InMemory::new());

        {
            let store = Box::new(make_store(dir.path()));
            let mut coord = CheckpointCoordinator::new(CheckpointConfig::default(), store)
                .await
                .unwrap();
            coord
                .bind_durable_decision_store(std::sync::Arc::new(
                    laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
                        std::sync::Arc::clone(&reservation_objects),
                    ),
                ))
                .await
                .unwrap();

            let first = coord
                .checkpoint(CheckpointRequest {
                    watermark: Some(1000),
                    ..CheckpointRequest::default()
                })
                .await
                .unwrap();
            let second = coord
                .checkpoint(CheckpointRequest {
                    watermark: Some(2000),
                    ..CheckpointRequest::default()
                })
                .await
                .unwrap();

            assert_eq!((first.epoch, first.checkpoint_id), (1, 1));
            assert_eq!((second.epoch, second.checkpoint_id), (2, 2));
        }

        // Re-create both coordinator and allocator over their persisted stores. The next
        // successful checkpoint proves recovery resumed the local epoch and the durable ID
        // reservation stream without consuming an ID merely to inspect allocator state.
        let store = Box::new(make_store(dir.path()));
        let mut restarted = CheckpointCoordinator::new(CheckpointConfig::default(), store)
            .await
            .unwrap();
        restarted
            .bind_durable_decision_store(std::sync::Arc::new(
                laminar_core::checkpoint_decision::CheckpointDecisionStore::new(
                    reservation_objects,
                ),
            ))
            .await
            .unwrap();

        let third = restarted
            .checkpoint(CheckpointRequest {
                watermark: Some(3000),
                ..CheckpointRequest::default()
            })
            .await
            .unwrap();
        assert_eq!((third.epoch, third.checkpoint_id), (3, 3));

        let manifest = make_store(dir.path()).load_latest().await.unwrap().unwrap();
        assert_eq!((manifest.epoch, manifest.checkpoint_id), (3, 3));
        assert_eq!(manifest.watermark, Some(3000));
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
                metadata: HashMap::new(),
            },
        );
        save_finalized(&store, &manifest).await;

        let manifest = store.load_latest().await.unwrap().unwrap();

        let table_cp = manifest.table_offsets.get("exchange_rates").unwrap();
        assert_eq!(table_cp.offsets.get("lsn"), Some(&"0/FF00".into()));
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
                metadata: HashMap::new(),
            },
        );
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
            let store = FileSystemCheckpointStore::new(&storage);
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

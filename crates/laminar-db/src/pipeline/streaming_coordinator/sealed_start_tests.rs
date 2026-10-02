//! Exercise the existing owned source actors with a sealed position and held intake.

use super::*;

#[derive(Default)]
struct Probe {
    positions: Mutex<Vec<SourcePosition>>,
    polls: AtomicU64,
    acknowledgements: Mutex<Vec<u64>>,
    control_seen: tokio::sync::Notify,
    poll_seen: tokio::sync::Notify,
    closes: AtomicU64,
}

struct SealedSource(Arc<Probe>, bool);

#[async_trait::async_trait]
impl SourceConnector for SealedSource {
    fn supports_initialized_start(&self) -> bool {
        self.1
    }

    fn contract(
        &self,
        _: &laminar_connectors::config::ConnectorConfig,
    ) -> Result<SourceContract, ConnectorError> {
        Ok(replayable_append_only_source_contract())
    }
    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        self.0.positions.lock().push(request.into_parts().1);
        Ok(())
    }
    async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        self.0.polls.fetch_add(1, Ordering::AcqRel);
        self.0.poll_seen.notify_one();
        Ok(None)
    }
    fn schema(&self) -> Arc<Schema> {
        test_source_schema()
    }
    fn checkpoint(&self) -> SourceCheckpoint {
        checkpoint_at(91)
    }
    fn drive_control_plane(&mut self) {
        self.0.control_seen.notify_one();
    }
    async fn notify_epoch_committed(
        &mut self,
        epoch: u64,
        _: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        self.0.acknowledgements.lock().push(epoch);
        Ok(())
    }
    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.0.closes.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

#[tokio::test]
async fn topology_start_runtime_held_source_does_not_seed_committed_progress_or_ack() {
    let probe = Arc::new(Probe::default());
    let gate = Arc::new(AtomicBool::new(true));
    let runtime = StreamingCoordinatorRuntime::new();
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let (_control, control_rx) = mpsc::bounded_async::<crate::pipeline::ControlMsg>(8);
    let coordinator = StreamingCoordinator::new(
        &runtime,
        vec![SourceRegistration {
            name: "added_source".into(),
            connector: Box::new(SealedSource(Arc::clone(&probe), true)),
            config: laminar_connectors::config::ConnectorConfig::new("sealed-probe"),
            position: SourcePosition::Initialized {
                checkpoint: checkpoint_at(91),
            },
            assignment_scoped: false,
        }],
        PipelineConfig {
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            checkpoint_schedule: CheckpointSchedule::Manual,
            ..PipelineConfig::default()
        },
        Arc::clone(&shutdown),
        control_rx,
        Arc::clone(&gate),
    )
    .await
    .unwrap();
    assert!(coordinator.committed_offsets[0].is_none());
    assert!(
        matches!(&probe.positions.lock()[0], SourcePosition::Initialized { checkpoint }
        if checkpoint.get_offset("test_position") == Some("91"))
    );
    let callback = MockCallback::new();
    let (ready_tx, ready_rx) = crossfire::oneshot::oneshot::<Result<(), String>>();
    let work = tokio::spawn(async move { coordinator.run_with_ready(callback, ready_tx).await });
    ready_rx.await.unwrap().unwrap();
    // Control observations prove the actor is servicing its lifecycle while the gate is held.
    // No wall-clock happy-path sleep is used as evidence that input stayed stopped.
    for _ in 0..2 {
        tokio::time::timeout(Duration::from_secs(2), probe.control_seen.notified())
            .await
            .unwrap();
    }
    assert_eq!(probe.polls.load(Ordering::Acquire), 0);
    assert!(probe.acknowledgements.lock().is_empty());
    // This is a local actor test, not a topology Release authorization.
    gate.store(false, Ordering::Release);
    tokio::time::timeout(Duration::from_secs(2), probe.poll_seen.notified())
        .await
        .unwrap();
    assert!(probe.polls.load(Ordering::Acquire) > 0);
    assert!(probe.acknowledgements.lock().is_empty());
    shutdown.notify_one();
    assert!(matches!(work.await.unwrap(), ExitReason::Shutdown));
    assert_eq!(probe.closes.load(Ordering::Acquire), 1);
    runtime.prune_and_require_idle().unwrap();
}

#[tokio::test(start_paused = true)]
async fn topology_start_runtime_initialized_deadline_closes_without_spawning_actors() {
    let state = Arc::new(StartupSourceState::default());
    let result = startup_result_with_config(
        vec![startup_source_with_delay(
            "added_source",
            Arc::clone(&state),
            false,
            false,
            Duration::from_secs(60),
            SourcePosition::Initialized {
                checkpoint: checkpoint_at(91),
            },
        )],
        PipelineConfig {
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            checkpoint_schedule: CheckpointSchedule::Manual,
            checkpoint_timeout: Duration::from_secs(10),
            ..PipelineConfig::default()
        },
    )
    .await;
    assert!(matches!(result, Err(DbError::Checkpoint(ref message))
        if message.contains("sealed initialization") && message.contains("shared 10s")));
    assert_eq!(state.open_calls.load(Ordering::Acquire), 1);
    assert_eq!(state.close_calls.load(Ordering::Acquire), 1);
    assert_eq!(state.poll_calls.load(Ordering::Acquire), 0);
    assert!(!state.open.load(Ordering::Acquire));
}

#[tokio::test]
async fn topology_start_runtime_owned_initialization_rejects_before_connector_io() {
    let state = Arc::new(StartupSourceState::default());
    let mut checkpoint = checkpoint_at(91);
    checkpoint.bind_assignment_version(std::num::NonZeroU64::MIN);
    let result = startup_result_with_config(
        vec![startup_source(
            "added_source",
            Arc::clone(&state),
            false,
            false,
            SourcePosition::Initialized { checkpoint },
        )],
        PipelineConfig {
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            checkpoint_schedule: CheckpointSchedule::Manual,
            ..PipelineConfig::default()
        },
    )
    .await;
    assert!(
        matches!(result, Err(DbError::Checkpoint(ref message)) if message.contains("unowned global"))
    );
    assert_eq!(state.open_calls.load(Ordering::Acquire), 0);
    assert_eq!(state.poll_calls.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn topology_start_runtime_uncertified_custom_source_cannot_ignore_sealed_position() {
    let probe = Arc::new(Probe::default());
    let result = startup_result_with_config(
        vec![SourceRegistration {
            name: "uncertified_source".into(),
            connector: Box::new(SealedSource(Arc::clone(&probe), false)),
            config: laminar_connectors::config::ConnectorConfig::new("uncertified-probe"),
            position: SourcePosition::Initialized {
                checkpoint: checkpoint_at(91),
            },
            assignment_scoped: false,
        }],
        PipelineConfig {
            delivery_guarantee: DeliveryGuarantee::AtLeastOnce,
            checkpoint_schedule: CheckpointSchedule::Manual,
            ..PipelineConfig::default()
        },
    )
    .await;
    assert!(
        matches!(result, Err(DbError::Config(ref message)) if message.contains("no certified sealed initialization"))
    );
    assert!(probe.positions.lock().is_empty());
    assert_eq!(probe.polls.load(Ordering::Acquire), 0);
    assert!(probe.acknowledgements.lock().is_empty());
}

use super::*;
use arrow::array::{Array, BinaryArray, Float64Array, Int64Array, StringArray, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};

fn test_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("ts", DataType::Int64, false),
    ]))
}

fn test_batch() -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(StringArray::from(vec!["AAPL", "GOOG"])),
            Arc::new(Float64Array::from(vec![150.0, 2800.0])),
            Arc::new(Int64Array::from(vec![1000, 2000])),
        ],
    )
    .unwrap()
}

struct RichFrontierProbe(Arc<parking_lot::Mutex<Vec<InputFrontier>>>);

struct BatchProbe(Arc<parking_lot::Mutex<Vec<RecordBatch>>>);

#[async_trait]
impl GraphOperator for BatchProbe {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        self.0.lock().extend(inputs.iter().flatten().cloned());
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

#[async_trait]
impl GraphOperator for RichFrontierProbe {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        unreachable!("the graph must use the rich frontier entry point")
    }

    async fn process_with_frontiers(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        frontiers: &[InputFrontier],
    ) -> Result<Vec<RecordBatch>, DbError> {
        *self.0.lock() = frontiers.to_vec();
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

struct FrontierHoldProbe {
    watermarks: Arc<parking_lot::Mutex<Vec<i64>>>,
    hold: i64,
}

#[async_trait]
impl GraphOperator for FrontierHoldProbe {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        *self.watermarks.lock() = watermarks.to_vec();
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn output_frontier(&self, input: InputFrontier) -> InputFrontier {
        input.held_at(Some(self.hold))
    }
}

#[tokio::test]
async fn rich_frontiers_exclude_idle_inputs_and_remain_monotone() {
    let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let mut graph = test_graph();
    let left = graph.ensure_source_node("left");
    let right = graph.ensure_source_node("right");
    let output = graph
        .place_operator_node(
            "frontier_probe",
            Box::new(RichFrontierProbe(Arc::clone(&seen))),
            2,
        )
        .unwrap();
    graph.add_edge(left, output, 0);
    graph.add_edge(right, output, 1);

    let sources = FxHashMap::default();
    let mut frontiers = FxHashMap::from_iter([
        (
            Arc::from("left"),
            InputFrontier {
                watermark: Some(100),
                idle: false,
            },
        ),
        (
            Arc::from("right"),
            InputFrontier {
                watermark: Some(50),
                idle: true,
            },
        ),
    ]);
    graph
        .execute_cycle_with_frontiers(&sources, i64::MIN, Some(&frontiers))
        .await
        .unwrap();

    assert_eq!(*seen.lock(), [frontiers["left"], frontiers["right"]]);
    assert_eq!(graph.output_watermarks[output], 100);
    assert!(!graph.output_idle[output]);

    frontiers.get_mut("left").unwrap().idle = true;
    frontiers.get_mut("right").unwrap().watermark = Some(200);
    graph
        .execute_cycle_with_frontiers(&sources, i64::MIN, Some(&frontiers))
        .await
        .unwrap();
    assert_eq!(graph.output_watermarks[output], 200);
    assert!(graph.output_idle[output]);

    frontiers.get_mut("left").unwrap().idle = false;
    frontiers.get_mut("left").unwrap().watermark = Some(75);
    graph
        .execute_cycle_with_frontiers(&sources, i64::MIN, Some(&frontiers))
        .await
        .unwrap();
    assert_eq!(graph.output_watermarks[output], 200);
    assert!(!graph.output_idle[output]);
}

#[tokio::test]
async fn all_idle_frontier_respects_operator_hold() {
    let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let mut graph = test_graph();
    let left = graph.ensure_source_node("left");
    let right = graph.ensure_source_node("right");
    let output = graph
        .place_operator_node(
            "frontier_hold_probe",
            Box::new(FrontierHoldProbe {
                watermarks: Arc::clone(&seen),
                hold: 150,
            }),
            2,
        )
        .unwrap();
    graph.add_edge(left, output, 0);
    graph.add_edge(right, output, 1);

    let frontiers = FxHashMap::from_iter([
        (
            Arc::from("left"),
            InputFrontier {
                watermark: Some(100),
                idle: true,
            },
        ),
        (
            Arc::from("right"),
            InputFrontier {
                watermark: Some(200),
                idle: true,
            },
        ),
    ]);
    graph
        .execute_cycle_with_frontiers(&FxHashMap::default(), i64::MIN, Some(&frontiers))
        .await
        .unwrap();

    assert_eq!(*seen.lock(), [100, 200]);
    assert_eq!(graph.output_watermarks[output], 150);
    assert!(!graph.output_idle[output]);
    assert_eq!(
        InputFrontier {
            watermark: Some(100),
            idle: true,
        }
        .held_at(Some(i64::MIN)),
        InputFrontier {
            watermark: None,
            idle: false,
        }
    );
}

#[cfg(feature = "cluster")]
#[test]
fn default_operator_rejects_ordered_shuffle_staging() {
    let mut operator = SourcePassthrough;

    let error = operator
        .stage_checkpointed_shuffle(
            "unadmitted-join-stage",
            RetainedBatch::local(test_batch()),
            0,
        )
        .expect_err("operators without an admitted shuffle path must fail closed");

    assert!(error
        .to_string()
        .contains("does not accept checkpointed shuffle stage"));

    let error = operator
        .stage_checkpointed_shuffle_frontier(
            "unadmitted-join-stage::right",
            2,
            InputFrontier {
                watermark: Some(10),
                idle: false,
            },
            1,
            0,
        )
        .expect_err("operators without an ordered frontier path must fail closed");
    assert!(error
        .to_string()
        .contains("does not accept ordered shuffle frontier stage"));
}

#[cfg(feature = "cluster")]
struct CheckpointAlignedReplayProbe {
    aligned_replay: Arc<std::sync::atomic::AtomicBool>,
    checkpoint_drain: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(feature = "cluster")]
#[async_trait]
impl GraphOperator for CheckpointAlignedReplayProbe {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn checkpoint_aligned_replay_pending(&self) -> bool {
        self.aligned_replay
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn checkpoint_drain_pending(&self) -> bool {
        self.checkpoint_drain
            .load(std::sync::atomic::Ordering::Acquire)
    }
}

#[cfg(feature = "cluster")]
#[test]
fn handoff_quiescence_includes_checkpoint_aligned_replay() {
    let replay_pending = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let drain_pending = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.push_test_node(
        "replay",
        Box::new(CheckpointAlignedReplayProbe {
            aligned_replay: Arc::clone(&replay_pending),
            checkpoint_drain: Arc::clone(&drain_pending),
        }),
    );
    graph.compute_topo_order();

    assert!(graph.checkpoint_is_quiescent());
    assert!(!graph.handoff_is_quiescent());
    graph.record_cycle_deferrals();
    assert!(graph.take_cycle_deferrals().0);

    replay_pending.store(false, std::sync::atomic::Ordering::Release);
    drain_pending.store(true, std::sync::atomic::Ordering::Release);
    assert!(!graph.checkpoint_is_quiescent());
    assert!(!graph.handoff_is_quiescent());
    graph.record_cycle_deferrals();
    assert!(graph.take_cycle_deferrals().0);

    drain_pending.store(false, std::sync::atomic::Ordering::Release);
    assert!(graph.handoff_is_quiescent());
}

struct RestoreProbe(Arc<std::sync::atomic::AtomicUsize>);

#[async_trait]
impl GraphOperator for RestoreProbe {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}

struct CheckpointBudgetProbe(Arc<std::sync::atomic::AtomicUsize>);

#[async_trait]
impl GraphOperator for CheckpointBudgetProbe {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    fn set_retractable_extremum_checkpoint_budget(&mut self, bytes: usize) {
        self.0.store(bytes, std::sync::atomic::Ordering::SeqCst);
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

#[test]
fn retractable_extremum_checkpoint_budget_reaches_existing_and_late_nodes() {
    let first = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let second = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.push_test_node("first", Box::new(CheckpointBudgetProbe(Arc::clone(&first))));
    assert_eq!(
        first.load(std::sync::atomic::Ordering::SeqCst),
        crate::config::DEFAULT_MAX_RETRACTABLE_EXTREMUM_CHECKPOINT_BYTES
    );

    graph.set_max_retractable_extremum_checkpoint_bytes(123_456);
    assert_eq!(first.load(std::sync::atomic::Ordering::SeqCst), 123_456);
    graph.push_test_node(
        "second",
        Box::new(CheckpointBudgetProbe(Arc::clone(&second))),
    );
    assert_eq!(second.load(std::sync::atomic::Ordering::SeqCst), 123_456);
}

struct ManagedStateAccountingProbe {
    accounting: ManagedStateAccountingSnapshot,
    samples: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait]
impl GraphOperator for ManagedStateAccountingProbe {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    fn managed_state_accounting(&self) -> Option<ManagedStateAccountingSnapshot> {
        self.samples
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Some(self.accounting)
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Ok(())
    }
}

struct RestoreFailureProbe {
    restores: Arc<std::sync::atomic::AtomicUsize>,
    drops: Arc<std::sync::atomic::AtomicUsize>,
    fail: bool,
}

impl Drop for RestoreFailureProbe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

#[async_trait]
impl GraphOperator for RestoreFailureProbe {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        self.restores
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.fail {
            Err(DbError::Pipeline("injected late restore failure".into()))
        } else {
            Ok(())
        }
    }
}

#[cfg(feature = "cluster")]
struct RestoredReplayFrontierProbe {
    replay_frontier: Option<InputFrontier>,
    processed: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(feature = "cluster")]
#[async_trait]
impl GraphOperator for RestoredReplayFrontierProbe {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if self.replay_frontier.is_none() {
            return Ok(Vec::new());
        }
        assert!(inputs.is_empty(), "replay-only cycle accepted new input");
        self.replay_frontier = None;
        self.processed
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(vec![test_batch()])
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(self.replay_frontier.map(|frontier| {
            let mut data = frontier
                .watermark
                .unwrap_or(i64::MIN)
                .to_le_bytes()
                .to_vec();
            data.push(u8::from(frontier.idle));
            OperatorCheckpoint { data }
        }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        if checkpoint.data.len() != 9 {
            return Err(DbError::Checkpoint(
                "invalid replay-frontier probe checkpoint".into(),
            ));
        }
        let watermark = i64::from_le_bytes(checkpoint.data[..8].try_into().unwrap());
        self.replay_frontier = Some(InputFrontier {
            watermark: (watermark != i64::MIN).then_some(watermark),
            idle: checkpoint.data[8] != 0,
        });
        Ok(())
    }

    fn output_frontier(&self, input: InputFrontier) -> InputFrontier {
        input.held_at(self.replay_frontier.and_then(|frontier| frontier.watermark))
    }

    fn restored_output_frontier(&self) -> Option<InputFrontier> {
        self.replay_frontier
            .map(|frontier| frontier.held_at(frontier.watermark))
    }

    fn wants_input(&self) -> bool {
        self.replay_frontier.is_none()
    }
}

#[test]
fn state_frame_restore_validates_all_operators_before_mutation() {
    let restores = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.allocate_node(GraphNode::new(
        Arc::from("present"),
        Box::new(RestoreProbe(Arc::clone(&restores))),
        1,
    ));
    let frames = [
        ("present".to_string(), bytes::Bytes::from_static(b"present")),
        ("missing".to_string(), bytes::Bytes::from_static(b"missing")),
    ];

    let error = graph
        .restore_state_frames(
            &frames,
            &[],
            u32::from(laminar_core::state::DEFAULT_KEY_GROUP_COUNT.get()),
        )
        .err()
        .expect("a missing operator must reject the frame inventory");

    assert!(
        error.to_string().contains("missing operator 'missing'"),
        "{error}"
    );
    assert_eq!(restores.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn state_frame_restore_enforces_managed_state_budget() {
    let samples = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut graph = test_graph();
    graph.set_max_managed_state_bytes(65);
    graph.allocate_node(GraphNode::new(
        Arc::from("accounted"),
        Box::new(ManagedStateAccountingProbe {
            accounting: ManagedStateAccountingSnapshot {
                live: 11,
                prepared: 22,
                retired: 33,
            },
            samples,
        }),
        0,
    ));

    let error = graph
        .restore_state_frames(
            &[("accounted".to_string(), bytes::Bytes::from_static(b"state"))],
            &[],
            u32::from(laminar_core::state::DEFAULT_KEY_GROUP_COUNT.get()),
        )
        .err()
        .expect("restored managed state above budget must fail");

    assert!(matches!(
        &error,
        DbError::ManagedStateBudgetExceeded {
            accounted_bytes: 66,
            limit_bytes: 65,
            ..
        }
    ));
    assert!(error.requires_pipeline_halt());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn restored_frame_seeds_output_frontier_until_replay_finishes() {
    let processed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.push_test_node(
        "replay",
        Box::new(RestoredReplayFrontierProbe {
            replay_frontier: None,
            processed: Arc::clone(&processed),
        }),
    );
    let mut checkpoint = 42_i64.to_le_bytes().to_vec();
    checkpoint.push(1);
    let (mut graph, count) = graph
        .restore_state_frames(
            &[("replay".to_string(), bytes::Bytes::from(checkpoint))],
            &[],
            u32::from(laminar_core::state::DEFAULT_KEY_GROUP_COUNT.get()),
        )
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(graph.output_watermarks[0], 42);
    assert!(!graph.output_idle[0]);

    let mut results = FxHashMap::default();
    graph
        .execute_single_operator(0, 100, &mut results)
        .await
        .unwrap();
    assert!(processed.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(graph.output_watermarks[0], 42);
    assert!(!graph.output_idle[0]);

    graph
        .execute_single_operator(0, 100, &mut results)
        .await
        .unwrap();
    assert_eq!(graph.output_watermarks[0], 100);
    assert!(!graph.output_idle[0]);
}

#[tokio::test]
async fn state_frame_restore_is_only_open_before_execution() {
    let restores = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.allocate_node(GraphNode::new(
        Arc::from("present"),
        Box::new(RestoreProbe(Arc::clone(&restores))),
        1,
    ));
    graph
        .execute_cycle(&FxHashMap::default(), i64::MIN, None)
        .await
        .unwrap();

    let error = graph
        .restore_state_frames(
            &[("present".to_string(), bytes::Bytes::from_static(b"state"))],
            &[],
            u32::from(laminar_core::state::DEFAULT_KEY_GROUP_COUNT.get()),
        )
        .err()
        .expect("restore after execution must fail");

    assert!(error
        .to_string()
        .contains("before the first execution cycle"));
    assert_eq!(restores.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn late_state_frame_restore_failure_drops_the_partial_graph() {
    let restores = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    for (name, fail) in [("first", false), ("second", true)] {
        graph.allocate_node(GraphNode::new(
            Arc::from(name),
            Box::new(RestoreFailureProbe {
                restores: Arc::clone(&restores),
                drops: Arc::clone(&drops),
                fail,
            }),
            1,
        ));
    }

    let error = graph
        .restore_state_frames(
            &[
                ("first".to_string(), bytes::Bytes::from_static(b"first")),
                ("second".to_string(), bytes::Bytes::from_static(b"second")),
            ],
            &[],
            u32::from(laminar_core::state::DEFAULT_KEY_GROUP_COUNT.get()),
        )
        .err()
        .expect("late operator restore failure must fail the graph");

    assert!(error.to_string().contains("second"), "{error}");
    assert_eq!(restores.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[test]
fn unmanaged_operator_rejects_a_whole_state_frame() {
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.allocate_node(GraphNode::new(
        Arc::from("source"),
        Box::new(SourcePassthrough),
        1,
    ));

    let error = graph
        .restore_state_frames(
            &[("source".to_string(), bytes::Bytes::from_static(b"state"))],
            &[],
            u32::from(laminar_core::state::DEFAULT_KEY_GROUP_COUNT.get()),
        )
        .err()
        .expect("an unmanaged operator must reject a state frame");

    assert!(error
        .to_string()
        .contains("does not accept checkpoint state"));
}

/// Records the batches handed to `stage_checkpointed_shuffle`.
#[cfg(feature = "cluster")]
struct RecordingOperator(Arc<parking_lot::Mutex<Vec<RetainedBatch>>>);

#[cfg(feature = "cluster")]
#[async_trait]
impl GraphOperator for RecordingOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }
    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Ok(())
    }
    fn stage_checkpointed_shuffle(
        &mut self,
        _stage: &str,
        batch: RetainedBatch,
        _watermark: i64,
    ) -> Result<(), DbError> {
        self.0.lock().push(batch);
        Ok(())
    }
}

#[cfg(feature = "cluster")]
#[derive(Debug, PartialEq, Eq)]
enum OrderedShuffleEvent {
    Batch(i64),
    Frontier(String, u64, InputFrontier, u64, u64),
}

#[cfg(feature = "cluster")]
struct OrderedShuffleProbe(Arc<parking_lot::Mutex<Vec<OrderedShuffleEvent>>>);

#[cfg(feature = "cluster")]
#[async_trait]
impl GraphOperator for OrderedShuffleProbe {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn stage_checkpointed_shuffle(
        &mut self,
        _stage: &str,
        batch: RetainedBatch,
        _watermark: i64,
    ) -> Result<(), DbError> {
        assert_eq!(batch.routed_vnodes(), &[0]);
        assert_eq!(batch.peer(), Some(2));
        assert!(batch.assignment_version().is_some());
        assert_eq!(batch.recovery_gen(), Some(0));
        let event_time = batch
            .batch()
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        self.0.lock().push(OrderedShuffleEvent::Batch(event_time));
        Ok(())
    }

    fn stage_checkpointed_shuffle_frontier(
        &mut self,
        stage: &str,
        peer: u64,
        frontier: InputFrontier,
        assignment_version: u64,
        recovery_gen: u64,
    ) -> Result<(), DbError> {
        self.0.lock().push(OrderedShuffleEvent::Frontier(
            stage.to_owned(),
            peer,
            frontier,
            assignment_version,
            recovery_gen,
        ));
        Ok(())
    }
}

#[cfg(feature = "cluster")]
struct AlignmentHarness {
    graph: OperatorGraph,
    local_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    remote_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    remote_sender: laminar_core::shuffle::ShuffleSender,
    fence: laminar_core::checkpoint::CheckpointAssignmentFence,
    recorded: Arc<parking_lot::Mutex<Vec<RetainedBatch>>>,
}

#[cfg(feature = "cluster")]
async fn alignment_harness() -> AlignmentHarness {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{NodeId, VnodeRegistry};

    let registry = Arc::new(VnodeRegistry::new(2));
    registry.set_assignment(vec![NodeId(1), NodeId(2)].into());
    let assignment_version = registry.assignment_version();
    let fence = CheckpointAssignmentFence::from_owner_map(
        assignment_version,
        &[1, 2],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            },
        ],
    )
    .unwrap();
    let local_receiver = Arc::new(
        ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(1))
            .await
            .unwrap(),
    );
    let remote_receiver = Arc::new(
        ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(2))
            .await
            .unwrap(),
    );
    let local_sender = ShuffleSender::new(1, uuid::Uuid::from_u128(1));
    local_sender.register_peer(2, remote_receiver.local_addr());
    let remote_sender = ShuffleSender::new(2, uuid::Uuid::from_u128(2));
    remote_sender.register_peer(1, local_receiver.local_addr());
    let local_process_deadline = Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
        std::time::Duration::from_secs(60),
    ));
    local_receiver
        .install_process_lease_deadline(Arc::clone(&local_process_deadline))
        .unwrap();
    local_sender
        .install_process_lease_deadline(local_process_deadline)
        .unwrap();
    let remote_process_deadline = Arc::new(
        laminar_core::cluster::control::LeaseDeadline::live_for(std::time::Duration::from_secs(60)),
    );
    remote_receiver
        .install_process_lease_deadline(Arc::clone(&remote_process_deadline))
        .unwrap();
    remote_sender
        .install_process_lease_deadline(remote_process_deadline)
        .unwrap();
    local_receiver
        .install_assignment_fence(&fence, &[1, 2])
        .unwrap();
    remote_receiver
        .install_assignment_fence(&fence, &[1, 2])
        .unwrap();
    local_sender
        .install_assignment_fence(&fence, &[1, 2])
        .unwrap();
    remote_sender
        .install_assignment_fence(&fence, &[1, 2])
        .unwrap();

    let recorded = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.set_key_group_count(
        laminar_core::state::KeyGroupCount::try_from(registry.vnode_count()).unwrap(),
    );
    graph.push_test_node("out", Box::new(RecordingOperator(Arc::clone(&recorded))));
    graph.set_cluster_shuffle(crate::operator::sql_query::ClusterShuffleConfig {
        registry,
        sender: Arc::new(local_sender),
        receiver: Arc::clone(&local_receiver),
        self_id: NodeId(1),
    });
    AlignmentHarness {
        graph,
        local_receiver,
        remote_receiver,
        remote_sender,
        fence,
        recorded,
    }
}

#[cfg(feature = "cluster")]
fn ordered_batch(event_time: i64) -> RecordBatch {
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(StringArray::from(vec!["AAPL"])),
            Arc::new(Float64Array::from(vec![150.0])),
            Arc::new(Int64Array::from(vec![event_time])),
        ],
    )
    .unwrap()
}

#[cfg(feature = "cluster")]
fn install_ordered_probe(
    harness: &mut AlignmentHarness,
) -> Arc<parking_lot::Mutex<Vec<OrderedShuffleEvent>>> {
    let events = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let budget = harness.graph.max_retractable_extremum_checkpoint_bytes;
    harness.graph.nodes[0]
        .replace_operator(Box::new(OrderedShuffleProbe(Arc::clone(&events))), budget);
    events
}

#[cfg(feature = "cluster")]
async fn send_remote(harness: &AlignmentHarness, message: laminar_core::shuffle::ShuffleMessage) {
    harness.remote_sender.send_to(1, &message).await.unwrap();
}

#[cfg(feature = "cluster")]
async fn execute_until_events(
    harness: &mut AlignmentHarness,
    events: &parking_lot::Mutex<Vec<OrderedShuffleEvent>>,
    expected: usize,
) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    while events.lock().len() < expected {
        harness
            .graph
            .execute_cycle(&FxHashMap::default(), 7, None)
            .await
            .unwrap();
        assert!(tokio::time::Instant::now() < deadline);
        tokio::task::yield_now().await;
    }
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_dispatches_an_ordered_frontier_cut_before_later_data() {
    use laminar_core::shuffle::ShuffleMessage;

    let mut harness = alignment_harness().await;
    let events = install_ordered_probe(&mut harness);
    let stage = "out::right";
    send_remote(
        &harness,
        ShuffleMessage::checkpointed(stage.into(), 0, ordered_batch(10)),
    )
    .await;
    send_remote(
        &harness,
        ShuffleMessage::Frontier {
            stage: stage.into(),
            watermark: Some(20),
            idle: true,
        },
    )
    .await;
    send_remote(
        &harness,
        ShuffleMessage::checkpointed(stage.into(), 0, ordered_batch(30)),
    )
    .await;

    execute_until_events(&mut harness, &events, 2).await;
    assert_eq!(
        *events.lock(),
        [
            OrderedShuffleEvent::Batch(10),
            OrderedShuffleEvent::Frontier(
                stage.into(),
                2,
                InputFrontier {
                    watermark: Some(20),
                    idle: true,
                },
                harness.fence.assignment_version,
                0,
            ),
        ]
    );

    execute_until_events(&mut harness, &events, 3).await;
    assert_eq!(events.lock()[2], OrderedShuffleEvent::Batch(30));
}

#[cfg(feature = "cluster")]
#[test]
fn graph_rejects_an_unknown_ordered_frontier_stage() {
    let graph = test_graph();
    let error = graph.shuffle_stage_node("missing::left").unwrap_err();
    assert!(
        error.to_string().contains("unknown or removed stage"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alignment_consumes_an_exposed_frontier_cut_before_holdover() {
    use laminar_core::checkpoint::{CheckpointAttempt, CheckpointBarrier};
    use laminar_core::shuffle::ShuffleMessage;

    let mut harness = alignment_harness().await;
    let events = install_ordered_probe(&mut harness);
    harness.graph.output_idle[0] = true;
    let attempt = CheckpointAttempt::new(70, 70);
    let stage = "out::left";
    send_remote(
        &harness,
        ShuffleMessage::checkpointed(stage.into(), 0, ordered_batch(10)),
    )
    .await;
    send_remote(
        &harness,
        ShuffleMessage::Frontier {
            stage: stage.into(),
            watermark: Some(20),
            idle: false,
        },
    )
    .await;
    harness
        .remote_sender
        .fan_out_barrier(
            &[1],
            CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &harness.fence,
        )
        .await
        .unwrap();

    let stage_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let _ = harness
            .local_receiver
            .drain_checkpointed_data_for("__frontier_probe");
        match harness.local_receiver.drain_checkpointed_holdover() {
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
            Ok(staged) => {
                for (received_stage, batch) in staged {
                    harness
                        .graph
                        .stage_checkpointed_shuffle(
                            &received_stage,
                            RetainedBatch::from_received(batch),
                            7,
                        )
                        .unwrap();
                }
            }
            Err(error) => panic!("frontier staging failed: {error}"),
        }
        assert!(tokio::time::Instant::now() < stage_deadline);
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
    harness
        .graph
        .align_shuffle_barriers(
            attempt,
            7,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            None,
        )
        .await
        .unwrap();
    assert!(!harness.graph.output_idle[0]);
    let events = events.lock();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0], OrderedShuffleEvent::Batch(10));
    assert!(matches!(
        &events[1],
        OrderedShuffleEvent::Frontier(received_stage, 2, frontier, _, 0)
            if received_stage == stage
                && *frontier == InputFrontier { watermark: Some(20), idle: false }
    ));
}

#[cfg(feature = "cluster")]
struct ThreeNodeAlignmentHarness {
    graph: OperatorGraph,
    local_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    _peer_two_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    waiting_peer_receiver: Arc<laminar_core::shuffle::ShuffleReceiver>,
    peer_two_sender: laminar_core::shuffle::ShuffleSender,
    peer_three_sender: laminar_core::shuffle::ShuffleSender,
    fence: laminar_core::checkpoint::CheckpointAssignmentFence,
    recorded: Arc<parking_lot::Mutex<Vec<RetainedBatch>>>,
}

#[cfg(feature = "cluster")]
async fn three_node_alignment_harness() -> ThreeNodeAlignmentHarness {
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
    use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
    use laminar_core::state::{NodeId, VnodeRegistry};

    let registry = Arc::new(VnodeRegistry::new(3));
    registry.set_assignment(vec![NodeId(1), NodeId(2), NodeId(3)].into());
    let fence = CheckpointAssignmentFence::from_owner_map(
        registry.assignment_version(),
        &[1, 2, 3],
        vec![
            CheckpointParticipant {
                node_id: 1,
                boot_incarnation: uuid::Uuid::from_u128(1),
            },
            CheckpointParticipant {
                node_id: 2,
                boot_incarnation: uuid::Uuid::from_u128(2),
            },
            CheckpointParticipant {
                node_id: 3,
                boot_incarnation: uuid::Uuid::from_u128(3),
            },
        ],
    )
    .unwrap();
    let local_receiver = Arc::new(
        ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(1))
            .await
            .unwrap(),
    );
    let peer_two_receiver = Arc::new(
        ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(2))
            .await
            .unwrap(),
    );
    let waiting_peer_receiver = Arc::new(
        ShuffleReceiver::bind(3, "127.0.0.1:0".parse().unwrap(), uuid::Uuid::from_u128(3))
            .await
            .unwrap(),
    );
    let local_process_deadline = Arc::new(laminar_core::cluster::control::LeaseDeadline::live_for(
        std::time::Duration::from_secs(60),
    ));
    local_receiver
        .install_process_lease_deadline(Arc::clone(&local_process_deadline))
        .unwrap();
    let peer_two_process_deadline = Arc::new(
        laminar_core::cluster::control::LeaseDeadline::live_for(std::time::Duration::from_secs(60)),
    );
    peer_two_receiver
        .install_process_lease_deadline(Arc::clone(&peer_two_process_deadline))
        .unwrap();
    let peer_three_process_deadline = Arc::new(
        laminar_core::cluster::control::LeaseDeadline::live_for(std::time::Duration::from_secs(60)),
    );
    waiting_peer_receiver
        .install_process_lease_deadline(Arc::clone(&peer_three_process_deadline))
        .unwrap();
    for receiver in [&local_receiver, &peer_two_receiver, &waiting_peer_receiver] {
        receiver
            .install_assignment_fence(&fence, &[1, 2, 3])
            .unwrap();
    }

    let local_sender = ShuffleSender::new(1, uuid::Uuid::from_u128(1));
    local_sender.register_peer(2, peer_two_receiver.local_addr());
    local_sender.register_peer(3, waiting_peer_receiver.local_addr());
    local_sender
        .install_process_lease_deadline(local_process_deadline)
        .unwrap();
    local_sender
        .install_assignment_fence(&fence, &[1, 2, 3])
        .unwrap();
    let peer_two_sender = ShuffleSender::new(2, uuid::Uuid::from_u128(2));
    peer_two_sender.register_peer(1, local_receiver.local_addr());
    peer_two_sender.register_peer(3, waiting_peer_receiver.local_addr());
    peer_two_sender
        .install_process_lease_deadline(peer_two_process_deadline)
        .unwrap();
    peer_two_sender
        .install_assignment_fence(&fence, &[1, 2, 3])
        .unwrap();
    let peer_three_sender = ShuffleSender::new(3, uuid::Uuid::from_u128(3));
    peer_three_sender.register_peer(1, local_receiver.local_addr());
    peer_three_sender.register_peer(2, peer_two_receiver.local_addr());
    peer_three_sender
        .install_process_lease_deadline(peer_three_process_deadline)
        .unwrap();
    peer_three_sender
        .install_assignment_fence(&fence, &[1, 2, 3])
        .unwrap();

    let recorded = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.set_key_group_count(
        laminar_core::state::KeyGroupCount::try_from(registry.vnode_count()).unwrap(),
    );
    graph.push_test_node("out", Box::new(RecordingOperator(Arc::clone(&recorded))));
    graph.set_cluster_shuffle(crate::operator::sql_query::ClusterShuffleConfig {
        registry,
        sender: Arc::new(local_sender),
        receiver: Arc::clone(&local_receiver),
        self_id: NodeId(1),
    });

    ThreeNodeAlignmentHarness {
        graph,
        local_receiver,
        _peer_two_receiver: peer_two_receiver,
        waiting_peer_receiver,
        peer_two_sender,
        peer_three_sender,
        fence,
        recorded,
    }
}

#[cfg(feature = "cluster")]
async fn stage_peer_two_data_and_barrier(
    harness: &ThreeNodeAlignmentHarness,
    attempt: laminar_core::checkpoint::CheckpointAttempt,
) -> RecordBatch {
    use laminar_core::checkpoint::CheckpointBarrier;
    use laminar_core::shuffle::ShuffleMessage;

    let batch = test_batch();
    harness
        .peer_two_sender
        .send_to(
            1,
            &ShuffleMessage::checkpointed("out".into(), 0, batch.clone()),
        )
        .await
        .unwrap();
    harness
        .peer_two_sender
        .fan_out_barrier(
            &[1, 3],
            CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &harness.fence,
        )
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let _ = harness
            .local_receiver
            .drain_checkpointed_data_for("__alignment_probe");
        let barriers = harness.local_receiver.drain_staged_barriers();
        if !barriers.is_empty() {
            for barrier in barriers {
                harness.local_receiver.stash_barrier(barrier);
            }
            return batch;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "peer data and barrier did not reach the holdover"
        );
        tokio::task::yield_now().await;
    }
}

#[cfg(feature = "cluster")]
async fn stage_peer_two_data_barrier_data(
    harness: &ThreeNodeAlignmentHarness,
    attempt: laminar_core::checkpoint::CheckpointAttempt,
) -> (RecordBatch, RecordBatch) {
    use laminar_core::checkpoint::CheckpointBarrier;
    use laminar_core::shuffle::ShuffleMessage;

    let before_barrier = test_batch();
    let after_barrier = RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(StringArray::from(vec!["MSFT", "NVDA"])),
            Arc::new(Float64Array::from(vec![420.0, 125.0])),
            Arc::new(Int64Array::from(vec![3000, 4000])),
        ],
    )
    .unwrap();
    harness
        .peer_two_sender
        .send_to(
            1,
            &ShuffleMessage::checkpointed("out".into(), 0, before_barrier.clone()),
        )
        .await
        .unwrap();
    harness
        .peer_two_sender
        .fan_out_barrier(
            &[1, 3],
            CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &harness.fence,
        )
        .await
        .unwrap();
    harness
        .peer_two_sender
        .send_to(
            1,
            &ShuffleMessage::checkpointed("out".into(), 0, after_barrier.clone()),
        )
        .await
        .unwrap();

    // Reproduce the normal drainer splitting a queued data/barrier/data sequence: the first
    // batch and barrier enter holdovers, while the post-barrier batch remains on the live queue.
    let stage_deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let _ = harness
            .local_receiver
            .drain_checkpointed_data_for("__alignment_probe");
        let barriers = harness.local_receiver.drain_staged_barriers();
        if !barriers.is_empty() {
            for barrier in barriers {
                harness.local_receiver.stash_barrier(barrier);
            }
            break;
        }
        assert!(
            tokio::time::Instant::now() < stage_deadline,
            "remote barrier did not reach the staged holdover"
        );
        tokio::task::yield_now().await;
    }
    (before_barrier, after_barrier)
}

#[cfg(feature = "cluster")]
async fn alignment_abort_controller(
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    attempt: laminar_core::checkpoint::CheckpointAttempt,
    durable: bool,
) -> Arc<laminar_core::cluster::control::ClusterController> {
    alignment_abort_controller_with_announcement(fence, attempt, durable, true).await
}

#[cfg(feature = "cluster")]
async fn alignment_abort_controller_with_announcement(
    fence: &laminar_core::checkpoint::CheckpointAssignmentFence,
    attempt: laminar_core::checkpoint::CheckpointAttempt,
    durable: bool,
    announce: bool,
) -> Arc<laminar_core::cluster::control::ClusterController> {
    use laminar_core::checkpoint_decision::CheckpointVerdict;
    use laminar_core::cluster::control::{
        BarrierAnnouncement, ClusterController, ClusterKv, InMemoryKv, LeaderLeaseOwner,
        LeaderLeaseStore, LeaseDeadline, LeaseOutcome, Phase, ANNOUNCEMENT_KEY,
    };
    use laminar_core::cluster::discovery::{NodeId, NodeInfo, NodeMetadata, NodeState};

    let node_id = NodeId(1);
    let kv = Arc::new(InMemoryKv::new(node_id));
    let kv_trait: Arc<dyn ClusterKv> = kv.clone();
    let info = |id| NodeInfo {
        id: NodeId(id),
        name: format!("node-{id}"),
        rpc_address: String::new(),
        raft_address: String::new(),
        state: NodeState::Active,
        metadata: NodeMetadata::default(),
        last_heartbeat_ms: 0,
    };
    let (_members_tx, members_rx) = tokio::sync::watch::channel(vec![info(1), info(2), info(3)]);
    let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
        node_id,
        Arc::clone(&kv_trait),
        kv_trait,
        None,
        members_rx,
        uuid::Uuid::from_u128(1),
    ));
    controller
        .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(60),
        )))
        .unwrap();
    controller.set_active(true);
    controller.publish_checkpoint_assignment_fence(Some(fence.clone()));

    let backing: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::memory::InMemory::new());
    let authority = Arc::new(LeaderLeaseStore::new(backing, 1_000));
    let owner = LeaderLeaseOwner {
        node: node_id,
        boot: uuid::Uuid::from_u128(1),
        process_term: 1,
    };
    let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap() else {
        panic!("empty alignment authority must grant leadership");
    };
    let proof = lease.proof();
    if durable {
        authority
            .record_cluster_outcome(
                &proof,
                attempt.epoch,
                attempt.checkpoint_id,
                fence.clone(),
                CheckpointVerdict::Abort,
                None,
            )
            .await
            .unwrap();
    }
    controller.set_leader_lease_store(authority);
    if announce {
        kv.seed(
            node_id,
            ANNOUNCEMENT_KEY,
            serde_json::to_string(&BarrierAnnouncement {
                epoch: attempt.epoch,
                checkpoint_id: attempt.checkpoint_id,
                assignment_fence: Some(fence.clone()),
                leader_proof: Some(proof),
                phase: Phase::Abort,
                flags: 0,
            })
            .unwrap(),
        );
    }
    controller
}

/// A peer ships a row + its exact-attempt barrier; alignment retains the row as channel state
/// before completing the certified distributed cut.
#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn align_shuffle_barriers_retains_peer_rows_then_aligns_exact_attempt() {
    use laminar_core::checkpoint::CheckpointAttempt;
    use laminar_core::checkpoint::CheckpointBarrier;
    use laminar_core::shuffle::ShuffleMessage;

    let mut harness = alignment_harness().await;
    let attempt = CheckpointAttempt::new(70, 70);

    let batch = test_batch();
    harness
        .remote_sender
        .send_to(
            1,
            &ShuffleMessage::checkpointed("out".into(), 0, batch.clone()),
        )
        .await
        .unwrap();
    harness
        .remote_sender
        .fan_out_barrier(
            &[1],
            CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &harness.fence,
        )
        .await
        .unwrap();

    harness
        .graph
        .align_shuffle_barriers(
            attempt,
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            None,
        )
        .await
        .unwrap();

    let received = harness.remote_receiver.recv().await.unwrap();
    assert_eq!(received.peer(), 1);
    assert_eq!(received.assignment_digest(), Some(harness.fence.digest()));
    assert!(matches!(
        received.message(),
        ShuffleMessage::Barrier(barrier)
            if barrier.epoch == attempt.epoch
                && barrier.checkpoint_id == attempt.checkpoint_id
    ));

    let got = harness.recorded.lock();
    assert_eq!(
        got.len(),
        1,
        "peer's pre-barrier row retained by the operator"
    );
    assert_eq!(got[0].num_rows(), batch.num_rows());
    assert_eq!(got[0].routed_vnodes(), &[0]);
    assert_eq!(got[0].uniform_vnode(), Some(0));
    assert_eq!(got[0].peer(), Some(2));
    assert_eq!(
        got[0].assignment_version(),
        Some(harness.fence.assignment_version)
    );
    assert_eq!(got[0].recovery_gen(), Some(0));
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_scope_cancellation_preserves_holdover_for_the_next_attempt() {
    use laminar_core::checkpoint::CheckpointAttempt;
    use laminar_core::checkpoint::CheckpointBarrier;

    let mut harness = three_node_alignment_harness().await;
    let cancelled = CheckpointAttempt::new(70, 70);
    let retained = stage_peer_two_data_and_barrier(&harness, cancelled).await;
    let sender = Arc::clone(
        &harness
            .graph
            .cluster_shuffle_config()
            .expect("cluster shuffle")
            .sender,
    );
    let live_peer_three = harness.waiting_peer_receiver.local_addr();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    sender.register_peer(3, listener.local_addr().unwrap());
    let accepted = Arc::new(tokio::sync::Notify::new());
    let stalled_peer = {
        let accepted = Arc::clone(&accepted);
        tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            accepted.notify_one();
            std::future::pending::<()>().await;
        })
    };
    let outcome = {
        let alignment = harness.graph.align_shuffle_barriers(
            cancelled,
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            None,
        );
        tokio::pin!(alignment);
        tokio::select! {
            () = accepted.notified() => {}
            result = &mut alignment => panic!("alignment completed before scope cancellation: {result:?}"),
        }

        sender.suspend_assignment_fence();
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut alignment)
            .await
            .expect("scope cancellation did not release barrier fan-out")
            .unwrap()
    };
    assert_eq!(
        outcome,
        ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging
    );
    assert!(
        harness.recorded.lock().is_empty(),
        "cancelled alignment staged checkpoint holdover"
    );

    harness
        .local_receiver
        .retire_checkpoint_barriers(cancelled, harness.fence.digest())
        .unwrap();
    sender.register_peer(3, live_peer_three);
    assert!(sender
        .install_assignment_fence(&harness.fence, &[1, 2, 3])
        .unwrap());
    let successor = CheckpointAttempt::new(71, 71);
    harness
        .peer_two_sender
        .fan_out_barrier(
            &[1, 3],
            CheckpointBarrier::new(successor.checkpoint_id, successor.epoch),
            &harness.fence,
        )
        .await
        .unwrap();
    harness
        .peer_three_sender
        .fan_out_barrier(
            &[1, 2],
            CheckpointBarrier::new(successor.checkpoint_id, successor.epoch),
            &harness.fence,
        )
        .await
        .unwrap();

    assert_eq!(
        harness
            .graph
            .align_shuffle_barriers(
                successor,
                0,
                &harness.fence,
                tokio::time::Instant::now() + std::time::Duration::from_secs(2),
                None,
            )
            .await
            .unwrap(),
        ShuffleAlignmentOutcome::Aligned
    );
    let recorded = harness.recorded.lock();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].num_rows(), retained.num_rows());
    stalled_peer.abort();
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn receiver_scope_suspension_preserves_holdover_before_graph_staging() {
    use laminar_core::checkpoint::CheckpointAttempt;

    let mut harness = three_node_alignment_harness().await;
    let cancelled = CheckpointAttempt::new(70, 70);
    let retained = stage_peer_two_data_and_barrier(&harness, cancelled).await;
    harness.local_receiver.suspend_assignment_fence();

    let outcome = harness
        .graph
        .align_shuffle_barriers(
            cancelled,
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        outcome,
        ShuffleAlignmentOutcome::ScopeCancelledBeforeStaging
    );
    assert!(harness.recorded.lock().is_empty());

    assert!(harness
        .local_receiver
        .install_assignment_fence(&harness.fence, &[1, 2, 3])
        .unwrap());
    harness
        .local_receiver
        .retire_checkpoint_barriers(cancelled, harness.fence.digest())
        .unwrap();
    let preserved = harness
        .local_receiver
        .drain_checkpointed_holdover()
        .unwrap();
    assert_eq!(preserved.len(), 1);
    assert_eq!(preserved[0].0, "out");
    assert_eq!(preserved[0].1.batch().num_rows(), retained.num_rows());
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_alignment_rejects_staged_data_barrier_data_sequence() {
    use laminar_core::checkpoint::CheckpointAttempt;

    let mut harness = three_node_alignment_harness().await;
    let attempt = CheckpointAttempt::new(70, 70);
    let (before_barrier, _after_barrier) =
        stage_peer_two_data_barrier_data(&harness, attempt).await;

    let error = harness
        .graph
        .align_shuffle_barriers(
            attempt,
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            None,
        )
        .await
        .expect_err("data behind an observed peer barrier must fail the checkpoint");
    assert!(error.to_string().contains("checkpoint barrier"), "{error}");
    assert!(
        error.requires_pipeline_recovery(),
        "destructive alignment failure must rewind the pipeline"
    );
    let retained = harness.recorded.lock();
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].num_rows(), before_barrier.num_rows());
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_alignment_retains_resumed_peer_data_on_durable_abort() {
    use laminar_core::checkpoint::CheckpointAttempt;

    let mut harness = three_node_alignment_harness().await;
    let attempt = CheckpointAttempt::new(80, 80);
    let (before_barrier, after_barrier) = stage_peer_two_data_barrier_data(&harness, attempt).await;
    let controller = alignment_abort_controller(&harness.fence, attempt, true).await;

    let outcome = harness
        .graph
        .align_shuffle_barriers(
            attempt,
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            Some(controller.as_ref()),
        )
        .await
        .expect("an exact durable Abort must end pre-capture alignment cleanly");

    assert_eq!(outcome, ShuffleAlignmentOutcome::Aborted);
    let mut retained: Vec<_> = harness
        .recorded
        .lock()
        .iter()
        .map(|batch| batch.batch().clone())
        .collect();
    assert!(
        matches!(retained.len(), 1 | 2),
        "the pre-barrier batch must be staged before Abort"
    );
    if retained.len() == 1 {
        let receiver_owned = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let batches = harness.local_receiver.drain_checkpointed_data_for("out");
                if !batches.is_empty() {
                    break batches;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("post-barrier batch remained in flight after Abort");
        retained.extend(
            receiver_owned
                .into_iter()
                .map(|batch| batch.batch().clone()),
        );
    }
    assert_eq!(
        retained.len(),
        2,
        "the graph and receiver must jointly own each batch exactly once after Abort"
    );
    assert_eq!(retained[0], before_barrier);
    assert_eq!(retained[1], after_barrier);
    assert!(
        harness
            .local_receiver
            .drain_checkpointed_data_for("out")
            .is_empty(),
        "post-barrier batch was duplicated in receiver ownership"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_alignment_audits_durable_abort_when_announcement_is_lost() {
    use laminar_core::checkpoint::CheckpointAttempt;

    let mut harness = three_node_alignment_harness().await;
    let attempt = CheckpointAttempt::new(90, 90);
    let retained = stage_peer_two_data_and_barrier(&harness, attempt).await;
    let controller =
        alignment_abort_controller_with_announcement(&harness.fence, attempt, true, false).await;

    let outcome = harness
        .graph
        .align_shuffle_barriers(
            attempt,
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            Some(controller.as_ref()),
        )
        .await
        .expect("the periodic authority audit must observe an Abort without gossip");

    assert_eq!(outcome, ShuffleAlignmentOutcome::Aborted);
    let recorded = harness.recorded.lock();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].num_rows(), retained.num_rows());
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn shuffle_alignment_does_not_trust_abort_hint_without_durable_outcome() {
    use laminar_core::checkpoint::CheckpointAttempt;

    let harness = three_node_alignment_harness().await;
    let attempt = CheckpointAttempt::new(90, 90);
    let controller = alignment_abort_controller(&harness.fence, attempt, false).await;

    let hint = OperatorGraph::wait_for_shuffle_alignment_terminal_hint(
        Some(controller.as_ref()),
        attempt,
        None,
        tokio::time::Instant::now() + std::time::Duration::from_secs(1),
    )
    .await
    .unwrap()
    .expect("Abort announcement must wake alignment");
    assert_eq!(hint.epoch, attempt.epoch);
    assert_eq!(hint.checkpoint_id, attempt.checkpoint_id);
    assert_eq!(hint.phase, laminar_core::cluster::control::Phase::Abort);
    assert_eq!(
        OperatorGraph::audit_shuffle_alignment_settlement(
            Some(controller.as_ref()),
            attempt,
            &harness.fence,
        )
        .await
        .unwrap(),
        None
    );
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn shuffle_alignment_rejects_abort_with_a_different_assignment_certificate() {
    use laminar_core::checkpoint::CheckpointAssignmentFence;
    use laminar_core::checkpoint::CheckpointAttempt;

    let harness = three_node_alignment_harness().await;
    let attempt = CheckpointAttempt::new(90, 90);
    let other_fence = CheckpointAssignmentFence::from_owner_map(
        harness.fence.assignment_version,
        &[1, 3, 2],
        harness.fence.participants.clone(),
    )
    .unwrap();
    let controller = alignment_abort_controller(&other_fence, attempt, true).await;

    let error = OperatorGraph::audit_shuffle_alignment_settlement(
        Some(controller.as_ref()),
        attempt,
        &harness.fence,
    )
    .await
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("different assignment certificate"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_sender_rejects_wrong_epoch_for_same_checkpoint_id() {
    use laminar_core::checkpoint::CheckpointAttempt;
    use laminar_core::checkpoint::CheckpointBarrier;

    let harness = alignment_harness().await;
    let expected = CheckpointAttempt::new(70, 70);
    let error = harness
        .remote_sender
        .fan_out_barrier(
            &[1],
            CheckpointBarrier::new(expected.checkpoint_id, 8),
            &harness.fence,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(
        error.to_string().contains("canonical checkpoint ID"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[test]
fn shuffle_attempt_comparison_rejects_all_conflicting_orders() {
    use laminar_core::checkpoint::CheckpointAttempt;

    let expected = CheckpointAttempt::new(70, 70);
    for observed in [
        CheckpointAttempt::new(69, 71),
        CheckpointAttempt::new(71, 69),
        CheckpointAttempt::new(70, 69),
        CheckpointAttempt::new(70, 71),
        CheckpointAttempt::new(69, 70),
        CheckpointAttempt::new(71, 70),
    ] {
        assert!(
            OperatorGraph::compare_shuffle_attempts(expected, observed).is_err(),
            "mixed attempt order must fail: {observed:?}"
        );
    }
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn shuffle_alignment_rejects_newer_durable_terminal_without_announcement() {
    use laminar_core::checkpoint::CheckpointAttempt;

    let attempt = CheckpointAttempt::new(70, 70);
    let newer = CheckpointAttempt::new(71, 71);
    let harness = three_node_alignment_harness().await;
    let controller =
        alignment_abort_controller_with_announcement(&harness.fence, newer, true, false).await;
    let error = OperatorGraph::audit_shuffle_alignment_settlement(
        Some(controller.as_ref()),
        attempt,
        &harness.fence,
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("superseded by durable terminal"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_alignment_rejects_wrong_assignment_digest() {
    use laminar_core::checkpoint::CheckpointAttempt;
    use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointBarrier};

    let harness = alignment_harness().await;
    let wrong_fence = CheckpointAssignmentFence::from_owner_map(
        harness.fence.assignment_version,
        &[2, 1],
        harness.fence.participants.clone(),
    )
    .unwrap();
    let attempt = CheckpointAttempt::new(70, 70);
    let error = harness
        .remote_sender
        .fan_out_barrier(
            &[1],
            CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &wrong_fence,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("assignment roster"), "{error}");
    assert!(harness.recorded.lock().is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_alignment_rejects_changed_local_assignment_scope() {
    use laminar_core::checkpoint::CheckpointAssignmentFence;
    use laminar_core::checkpoint::CheckpointAttempt;

    let mut harness = alignment_harness().await;
    let next = CheckpointAssignmentFence::from_owner_map(
        harness.fence.assignment_version + 1,
        &[1, 2],
        harness.fence.participants.clone(),
    )
    .unwrap();
    let cfg = harness.graph.cluster_shuffle_config().unwrap();
    cfg.sender.install_assignment_fence(&next, &[1, 2]).unwrap();
    cfg.receiver
        .install_assignment_fence(&next, &[1, 2])
        .unwrap();
    let error = harness
        .graph
        .align_shuffle_barriers(
            CheckpointAttempt::new(70, 70),
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            None,
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("assignment differs"), "{error}");
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_transition_discards_staged_pre_recovery_barrier() {
    use laminar_core::checkpoint::CheckpointAttempt;
    use laminar_core::checkpoint::CheckpointBarrier;

    let harness = alignment_harness().await;
    let attempt = CheckpointAttempt::new(70, 70);
    harness
        .remote_sender
        .fan_out_barrier(
            &[1],
            CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &harness.fence,
        )
        .await
        .unwrap();
    let old = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        harness.local_receiver.recv(),
    )
    .await
    .unwrap()
    .unwrap();
    harness.local_receiver.stash_barrier(old);
    harness.local_receiver.set_recovery_gen(1);
    harness.remote_receiver.set_recovery_gen(1);
    harness
        .graph
        .cluster_shuffle_config()
        .unwrap()
        .sender
        .set_recovery_gen(1);

    assert!(harness.local_receiver.drain_staged_barriers().is_empty());
    assert!(harness.remote_receiver.drain_staged_barriers().is_empty());
    assert!(harness.recorded.lock().is_empty());
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_alignment_fails_closed_on_unknown_stage() {
    use laminar_core::checkpoint::CheckpointAttempt;
    use laminar_core::checkpoint::CheckpointBarrier;
    use laminar_core::shuffle::ShuffleMessage;

    let mut harness = alignment_harness().await;
    let attempt = CheckpointAttempt::new(70, 70);
    harness
        .remote_sender
        .send_to(
            1,
            &ShuffleMessage::checkpointed("missing".into(), 0, test_batch()),
        )
        .await
        .unwrap();
    harness
        .remote_sender
        .fan_out_barrier(
            &[1],
            CheckpointBarrier::new(attempt.checkpoint_id, attempt.epoch),
            &harness.fence,
        )
        .await
        .unwrap();
    let error = harness
        .graph
        .align_shuffle_barriers(
            attempt,
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
            None,
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("unknown or removed stage"),
        "{error}"
    );
}

#[cfg(feature = "cluster")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shuffle_alignment_uses_supplied_absolute_deadline() {
    use laminar_core::checkpoint::CheckpointAttempt;

    let mut harness = alignment_harness().await;
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        harness.graph.align_shuffle_barriers(
            CheckpointAttempt::new(70, 70),
            0,
            &harness.fence,
            tokio::time::Instant::now() + std::time::Duration::from_millis(30),
            None,
        ),
    )
    .await
    .expect("alignment ignored its supplied deadline")
    .unwrap_err();
    assert!(error.to_string().contains("absolute deadline"), "{error}");
}

#[test]
fn test_source_passthrough() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async {
        let mut op = SourcePassthrough;
        let batch = test_batch();
        let result = op.process(&[vec![batch.clone()]], &[0]).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    });
}

#[tokio::test]
async fn source_views_share_payloads_but_hide_positions_from_ordinary_queries() {
    use laminar_connectors::connector::{
        schema_with_source_mutations_and_row_positions, schema_with_source_row_positions,
        SourceBatch, SourceRowPositionCapability, SourceRowPositions, SOURCE_PARTITION_COLUMN,
    };

    let visible_batch = test_batch();
    let visible_schema = visible_batch.schema();
    let visible_values = Arc::clone(visible_batch.column(0));
    let positioned_schema = schema_with_source_row_positions(&visible_schema).unwrap();
    let mutation_schema = schema_with_source_mutations_and_row_positions(&visible_schema).unwrap();
    let positions = SourceRowPositions::try_new(
        BinaryArray::from(vec![&b"p0"[..], &b"p0"[..]]),
        BinaryArray::from(vec![&b"1"[..], &b"2"[..]]),
        UInt32Array::from(vec![0, 0]),
    )
    .unwrap();
    let positioned_batch = SourceBatch::positioned(visible_batch, positions)
        .unwrap()
        .into_records_with_metadata(
            SourceRowPositionCapability::Deterministic,
            &positioned_schema,
            &mutation_schema,
        )
        .unwrap();

    let ordinary_seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let positioned_seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let mut graph = test_graph();
    graph.register_source_schema("trades".into(), Arc::clone(&visible_schema));
    let ordinary_source = graph.ensure_source_node("trades");
    let positioned_source = graph.ensure_positioned_source_node("trades");
    let ordinary_node = graph
        .place_operator_node(
            "ordinary_probe",
            Box::new(BatchProbe(Arc::clone(&ordinary_seen))),
            1,
        )
        .unwrap();
    let positioned_node = graph
        .place_operator_node(
            "positioned_probe",
            Box::new(BatchProbe(Arc::clone(&positioned_seen))),
            1,
        )
        .unwrap();
    graph.add_edge(ordinary_source, ordinary_node, 0);
    graph.add_edge(positioned_source, positioned_node, 0);

    let sources = FxHashMap::from_iter([(Arc::from("trades"), vec![positioned_batch])]);
    let frontiers = FxHashMap::from_iter([(
        Arc::from("trades"),
        InputFrontier {
            watermark: Some(42),
            idle: true,
        },
    )]);
    graph
        .execute_cycle_with_frontiers(&sources, i64::MIN, Some(&frontiers))
        .await
        .unwrap();

    let ordinary_batches = ordinary_seen.lock();
    let positioned_batches = positioned_seen.lock();
    assert_eq!(ordinary_batches[0].schema(), visible_schema);
    assert!(ordinary_batches[0]
        .column_by_name(SOURCE_PARTITION_COLUMN)
        .is_none());
    assert_eq!(positioned_batches[0].schema(), positioned_schema);
    assert!(positioned_batches[0]
        .column_by_name(SOURCE_PARTITION_COLUMN)
        .is_some());
    assert!(Arc::ptr_eq(&visible_values, ordinary_batches[0].column(0)));
    assert!(Arc::ptr_eq(
        &visible_values,
        positioned_batches[0].column(0)
    ));
    assert_eq!(
        graph.node_domain[ordinary_node],
        graph.node_domain[positioned_node]
    );
    assert_eq!(graph.output_watermarks[ordinary_source], 42);
    assert_eq!(graph.output_watermarks[positioned_source], 42);
    assert!(graph.output_idle[ordinary_source]);
    assert!(graph.output_idle[positioned_source]);

    drop(ordinary_batches);
    drop(positioned_batches);
    graph.set_shared_source_isolation(true, usize::MAX);
    graph.compute_topo_order();
    assert_ne!(
        graph.node_domain[ordinary_node],
        graph.node_domain[positioned_node]
    );
    let failed = FxHashSet::from_iter([graph.node_domain[positioned_node]]);
    assert!(!graph.source_feeds_failed_domain(ordinary_source, &failed));
    assert!(graph.source_feeds_failed_domain(positioned_source, &failed));
}

#[test]
fn test_graph_construction() {
    let ctx = laminar_sql::create_session_context();
    let mut graph = OperatorGraph::new(ctx);

    graph.add_query(
        "q1".to_string(),
        "SELECT symbol, price FROM trades WHERE price > 100".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    assert_eq!(graph.nodes.len(), 2); // source "trades" + query "q1"
    assert_eq!(graph.edges.len(), 1); // trades → q1
    assert!(graph.source_map.contains_key("trades"));
    assert!(graph.output_map.contains_key("q1"));
}

#[test]
fn test_cascading_queries() {
    let ctx = laminar_sql::create_session_context();
    let mut graph = OperatorGraph::new(ctx);

    graph.add_query(
        "q1".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "q2".to_string(),
        "SELECT symbol FROM q1 WHERE price > 100".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    // source "trades" + query "q1" + query "q2" = 3 nodes
    assert_eq!(graph.nodes.len(), 3);
    // trades → q1, q1 → q2 = 2 edges
    assert_eq!(graph.edges.len(), 2);
    assert!(graph.depends_on_stream.contains(&2)); // q2 depends on q1
}

#[test]
fn test_topo_order() {
    let ctx = laminar_sql::create_session_context();
    let mut graph = OperatorGraph::new(ctx);

    // Add in reverse dependency order
    graph.add_query(
        "q2".to_string(),
        "SELECT * FROM q1".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    graph.compute_topo_order();

    // Find positions in topo order
    let q1_pos = graph
        .topo_order
        .iter()
        .position(|&id| &*graph.nodes[id].name == "q1");
    let q2_pos = graph
        .topo_order
        .iter()
        .position(|&id| &*graph.nodes[id].name == "q2");

    // q1 should appear before q2 (but note: q2 was added first and created
    // a source node "q1" which gets the first edge; the real q1 query node
    // doesn't have that edge. This test mainly verifies no panics.)
    assert!(q1_pos.is_some());
    assert!(q2_pos.is_some());
}

#[test]
fn test_remove_query() {
    let ctx = laminar_sql::create_session_context();
    let mut graph = OperatorGraph::new(ctx);

    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        true,
    );
    assert!(graph.output_map.contains_key("q1"));
    let original_node = graph.output_map["q1"];
    graph.ensure_live_provider("q1", &test_schema());
    let temporal_config = TemporalJoinTranslatorConfig {
        left_table: "trades".to_string(),
        right_table: "versions".to_string(),
        left_key_columns: vec!["symbol".to_string()],
        right_key_columns: vec!["symbol".to_string()],
        left_time_column: "ts".to_string(),
        right_time_column: "valid_from".to_string(),
        join_kind: laminar_sql::translator::TemporalJoinKind::Inner,
        probe_schedule: laminar_sql::translator::TemporalProbeSchedule::as_of(),
        probe_alias: None,
    };
    graph
        .temporal_configs
        .push(("q1".to_string(), temporal_config.clone()));

    graph.remove_query("q1");
    assert!(!graph.output_map.contains_key("q1"));
    assert!(graph.nodes[1].removed); // node 0 = source, node 1 = q1
    assert!(!graph.incremental_tables.contains("q1"));
    assert!(graph.temporal_configs.is_empty());
    assert!(!graph.live_handles.contains_key("q1"));

    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    let replacement_node = graph.output_map["q1"];
    assert_eq!(replacement_node, original_node);
    assert_eq!(
        graph
            .nodes
            .iter()
            .filter(|node| !node.removed && &*node.name == "q1")
            .count(),
        1
    );
    assert!(!graph.incremental_tables.contains("q1"));
    graph.ensure_live_provider("q1", &test_schema());
    assert!(graph.live_handles.contains_key("q1"));

    graph.incremental_tables.insert("metadata_only".to_string());
    graph
        .temporal_configs
        .push(("metadata_only".to_string(), temporal_config));
    graph.ensure_live_provider("metadata_only", &test_schema());
    assert!(!graph.output_map.contains_key("metadata_only"));
    graph.remove_query("metadata_only");
    assert!(!graph.incremental_tables.contains("metadata_only"));
    assert!(graph
        .temporal_configs
        .iter()
        .all(|(query_name, _)| query_name != "metadata_only"));
    assert!(!graph.live_handles.contains_key("metadata_only"));
}

#[test]
fn changelog_join_fails_closed_before_graph_mutation() {
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    let mut incremental = FxHashSet::default();
    incremental.insert("agg_a".to_string());
    incremental.insert("agg_b".to_string());
    graph.set_incremental_tables(incremental);

    graph.add_query(
        "joined".to_string(),
        "SELECT a.k FROM agg_a a JOIN agg_b b ON a.k = b.k".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    assert!(!graph.output_map.contains_key("joined"));
    assert!(graph
        .build_errors
        .iter()
        .any(|error| error.to_string().contains("reads an incremental changelog")));
}

#[test]
fn rejected_control_add_removes_all_query_artifacts() {
    let ctx = laminar_sql::create_session_context();
    let mut graph = OperatorGraph::new(ctx);
    graph
        .build_errors
        .push(DbError::Pipeline("forced admission rejection".into()));

    let mutation = Arc::new(crate::pipeline::ControlMutation::new());
    let (reply, mut result) = tokio::sync::oneshot::channel();
    let message = crate::pipeline::ControlMsg::add_stream(
        "rejected".to_string(),
        "SELECT * FROM events".to_string(),
        None,
        None,
        None,
        None,
        true,
        reply,
        Arc::clone(&mutation),
    );
    crate::pipeline_callback::apply_control_to_graph(&mut graph, message);
    let error = result
        .try_recv()
        .expect("control result must be sent synchronously")
        .unwrap_err();

    assert_eq!(
        mutation.state(),
        crate::pipeline::ControlMutationState::Cancelled
    );
    assert!(matches!(error, DbError::Pipeline(_)));
    assert!(!graph.output_map.contains_key("rejected"));
    assert!(!graph.incremental_tables.contains("rejected"));
    assert!(!graph.live_handles.contains_key("rejected"));
    assert!(graph
        .temporal_configs
        .iter()
        .all(|(query_name, _)| query_name != "rejected"));
    let rejected_nodes: FxHashSet<_> = graph
        .nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| &*node.name == "rejected")
        .map(|(id, node)| {
            assert!(node.removed);
            id
        })
        .collect();
    assert!(!rejected_nodes.is_empty());
    assert!(graph.edges.iter().all(
        |edge| !rejected_nodes.contains(&edge.source) && !rejected_nodes.contains(&edge.target)
    ));
    assert!(graph.nodes.iter().all(|node| {
        node.output_routes
            .iter()
            .all(|(target, _)| !rejected_nodes.contains(target))
    }));
}

#[test]
fn repeated_live_control_create_drop_reuses_graph_slots() {
    let ctx = laminar_sql::create_session_context();
    let mut graph = OperatorGraph::new(ctx);

    for _ in 0..128 {
        let create_mutation = Arc::new(crate::pipeline::ControlMutation::new());
        let (create_reply, mut create_result) = tokio::sync::oneshot::channel();
        crate::pipeline_callback::apply_control_to_graph(
            &mut graph,
            crate::pipeline::ControlMsg::add_stream(
                "churn".to_string(),
                "SELECT * FROM events".to_string(),
                None,
                None,
                None,
                None,
                false,
                create_reply,
                Arc::clone(&create_mutation),
            ),
        );
        create_result
            .try_recv()
            .expect("CREATE acknowledgement must be synchronous")
            .unwrap();
        assert_eq!(
            create_mutation.state(),
            crate::pipeline::ControlMutationState::Applied
        );

        let drop_mutation = Arc::new(crate::pipeline::ControlMutation::new());
        let (drop_reply, mut drop_result) = tokio::sync::oneshot::channel();
        crate::pipeline_callback::apply_control_to_graph(
            &mut graph,
            crate::pipeline::ControlMsg::drop_streams(
                vec!["churn".to_string()],
                drop_reply,
                Arc::clone(&drop_mutation),
            ),
        );
        drop_result
            .try_recv()
            .expect("DROP acknowledgement must be synchronous")
            .unwrap();
        assert_eq!(
            drop_mutation.state(),
            crate::pipeline::ControlMutationState::Applied
        );
    }

    assert_eq!(
        graph.nodes.len(),
        2,
        "one source slot plus one reusable query slot"
    );
    assert_eq!(graph.free_node_ids.len(), 1);
    assert!(graph.edges.is_empty());
}

#[tokio::test]
async fn test_execute_cycle_basic() {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);

    graph.add_query(
        "filtered".to_string(),
        "SELECT symbol, price FROM trades WHERE price > 200".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let batch = test_batch();
    let mut source_batches = FxHashMap::default();
    source_batches.insert(Arc::from("trades"), vec![batch]);

    let results = graph
        .execute_cycle(&source_batches, i64::MAX, None)
        .await
        .unwrap();
    assert!(results.contains_key("filtered"));
    let filtered = &results[&Arc::from("filtered") as &Arc<str>];
    // Only GOOG (price=2800) passes the filter
    let total_rows: usize = filtered.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 1);
}

// --- AI routing ---

struct PosProvider;

#[async_trait]
impl crate::ai::InferenceProvider for PosProvider {
    async fn infer_batch(
        &self,
        request: crate::ai::InferenceRequest,
    ) -> Result<crate::ai::InferenceResponse, crate::ai::ProviderError> {
        Ok(crate::ai::InferenceResponse {
            outputs: crate::ai::InferenceOutputs::Text(vec![
                "pos".to_string();
                request.inputs.len()
            ]),
            usage: crate::ai::Usage::ZERO,
        })
    }
    fn name(&self) -> &'static str {
        "pos"
    }
}

fn stub_ai_runtime() -> Arc<crate::ai::AiRuntime> {
    use crate::ai::{ModelBackend, ModelEntry, ModelRegistry, Task};
    let mut registry = ModelRegistry::new();
    registry
        .register(ModelEntry {
            id: "m".into(),
            tasks: vec![Task::Classify],
            backend: ModelBackend::Remote {
                provider: "p".into(),
                model: "stub-model".into(),
            },
        })
        .unwrap();
    let providers = [(
        "p".to_string(),
        Arc::new(PosProvider) as Arc<dyn crate::ai::InferenceProvider>,
    )];
    Arc::new(crate::ai::AiRuntime::new(
        registry,
        providers,
        None,
        Arc::new(crate::ai::AiResultCache::with_defaults()),
        Arc::new(crate::ai::AiCallLog::with_defaults()),
    ))
}

fn docs_batch() -> RecordBatch {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("text", DataType::Utf8, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["great quarter"])),
        ],
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ai_routing_enriches_rows() {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);
    graph.set_ai_runtime(stub_ai_runtime(), tokio::runtime::Handle::current());
    graph.register_source_schema("docs".to_string(), docs_batch().schema());

    graph.add_query(
        "labeled".to_string(),
        "SELECT id, ai_classify(text, model => 'm', labels => ARRAY['pos','neg']) AS label \
         FROM docs"
            .to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph
        .take_build_errors()
        .expect("AI query should route cleanly");

    // Cycle 1: the row misses the cache and is handed to the worker.
    let mut sources = FxHashMap::default();
    sources.insert(Arc::from("docs"), vec![docs_batch()]);
    let _ = graph.execute_cycle(&sources, i64::MAX, None).await.unwrap();

    // Let the off-thread worker finish, then drain on a later cycle.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let empty = FxHashMap::default();
    let results = graph.execute_cycle(&empty, i64::MAX, None).await.unwrap();

    let out = &results[&(Arc::from("labeled") as Arc<str>)];
    let rows: usize = out.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(rows, 1, "the enriched row should be emitted");
    // Output schema is the residual projection: (id, label).
    let batch = out.iter().find(|b| b.num_rows() > 0).unwrap();
    let label = batch
        .column(batch.schema().index_of("label").unwrap())
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(label.value(0), "pos");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ai_routing_unknown_model_fails_at_build() {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);
    graph.set_ai_runtime(stub_ai_runtime(), tokio::runtime::Handle::current());

    graph.add_query(
        "bad".to_string(),
        "SELECT ai_classify(text, model => 'ghost', labels => ARRAY['a']) AS label FROM docs"
            .to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    assert!(
        graph.take_build_errors().is_err(),
        "unknown model must fail"
    );
}

/// End-to-end through the real graph: `ai_sentiment` lifts to the AI
/// operator, the worker scores on Ring 1, and the emitted column is a
/// numeric `Float64`, not a label.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ai_sentiment_emits_a_double_score() {
    use crate::ai::{
        AiCallLog, AiResultCache, AiRuntime, InferenceOutputs, InferenceProvider, InferenceRequest,
        InferenceResponse, ModelBackend, ModelEntry, ModelRegistry, ProviderError, Task, Usage,
    };

    struct ScoreProvider;
    #[async_trait::async_trait]
    impl InferenceProvider for ScoreProvider {
        async fn infer_batch(
            &self,
            req: InferenceRequest,
        ) -> Result<InferenceResponse, ProviderError> {
            // A compliant sentiment model replies with a bare number.
            Ok(InferenceResponse {
                outputs: InferenceOutputs::Text(vec!["0.8".to_string(); req.inputs.len()]),
                usage: Usage::ZERO,
            })
        }
        fn name(&self) -> &'static str {
            "score"
        }
    }

    let mut registry = ModelRegistry::new();
    registry
        .register(ModelEntry {
            id: "m".into(),
            tasks: vec![Task::Sentiment],
            backend: ModelBackend::Remote {
                provider: "p".into(),
                model: "stub".into(),
            },
        })
        .unwrap();
    let call_log = Arc::new(AiCallLog::with_defaults());
    let runtime = Arc::new(AiRuntime::new(
        registry,
        [(
            "p".to_string(),
            Arc::new(ScoreProvider) as Arc<dyn InferenceProvider>,
        )],
        None,
        Arc::new(AiResultCache::with_defaults()),
        Arc::clone(&call_log),
    ));

    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);
    graph.set_ai_runtime(runtime, tokio::runtime::Handle::current());
    graph.register_source_schema("docs".to_string(), docs_batch().schema());

    graph.add_query(
        "scored".to_string(),
        "SELECT id, ai_sentiment(text, model => 'm') AS sentiment FROM docs".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph
        .take_build_errors()
        .expect("ai_sentiment should route cleanly");

    let mut sources = FxHashMap::default();
    sources.insert(Arc::from("docs"), vec![docs_batch()]);
    let _ = graph.execute_cycle(&sources, i64::MAX, None).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    let results = graph
        .execute_cycle(&FxHashMap::default(), i64::MAX, None)
        .await
        .unwrap();

    let out = &results[&(Arc::from("scored") as Arc<str>)];
    let batch = out.iter().find(|b| b.num_rows() > 0).expect("a scored row");
    let col = batch.column(batch.schema().index_of("sentiment").unwrap());
    let scores = col
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("sentiment is a Float64 score, not a label");
    assert!((scores.value(0) - 0.8).abs() < 1e-9);
    assert_eq!(
        call_log.total_recorded(),
        1,
        "the call is in laminar.ai_calls"
    );
}

#[tokio::test]
async fn test_execute_cycle_empty_source() {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);

    // Register schema so the graph can create empty placeholder tables
    graph.register_source_schema("trades".to_string(), test_schema());

    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let source_batches = FxHashMap::default();
    let results = graph
        .execute_cycle(&source_batches, i64::MAX, None)
        .await
        .unwrap();
    // No source data → empty results (or no entry)
    let total: usize = results
        .get("q1")
        .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum());
    assert_eq!(total, 0);
}

#[tokio::test]
async fn test_fan_out() {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);

    graph.add_query(
        "q1".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "q2".to_string(),
        "SELECT symbol FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let batch = test_batch();
    let mut source_batches = FxHashMap::default();
    source_batches.insert(Arc::from("trades"), vec![batch]);

    let results = graph
        .execute_cycle(&source_batches, i64::MAX, None)
        .await
        .unwrap();
    assert!(results.contains_key("q1"));
    assert!(results.contains_key("q2"));
}

#[test]
fn test_checkpoint_empty() {
    let ctx = laminar_sql::create_session_context();
    let mut graph = OperatorGraph::new(ctx);
    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    let checkpoint = graph.capture_state().unwrap();
    assert!(checkpoint.whole.is_empty());
    assert!(checkpoint.vnodes.is_empty());
}

#[tokio::test]
async fn test_temporal_filter_checkpoint_restore_through_graph() {
    use laminar_sql::parser::EmitClause;
    // test_batch(): ts is Int64 epoch-ms — AAPL@1000, GOOG@2000.
    let sql = "SELECT * FROM trades WHERE ts > now() - INTERVAL '10' SECOND";
    let mut g1 = test_graph();
    g1.add_query(
        "recent".into(),
        sql.into(),
        Some(EmitClause::Changes),
        None,
        None,
        None,
        false,
    );
    let mut src = FxHashMap::default();
    src.insert(Arc::from("trades"), vec![test_batch()]);
    // Frontier 5000ms: both rows are members (exit 11000/12000) ⇒ +1,+1.
    let r = g1.execute_cycle(&src, 5_000, None).await.unwrap();
    assert_eq!(total_rows(&r, "recent"), 2);

    let checkpoint = g1.capture_state().unwrap();
    let (whole, vnodes) = full_state_frames(checkpoint);
    let mut g2 = test_graph();
    g2.add_query(
        "recent".into(),
        sql.into(),
        Some(EmitClause::Changes),
        None,
        None,
        None,
        false,
    );
    let (restored_graph, restored) = g2
        .restore_state_frames(
            &whole,
            &vnodes,
            u32::from(laminar_core::state::DEFAULT_KEY_GROUP_COUNT.get()),
        )
        .unwrap();
    let mut g2 = restored_graph;
    assert_eq!(restored, 1);

    // Advancing to 11000ms ages out AAPL@1000 (exit 11000, strict `>`)
    // but not GOOG@2000 (exit 12000): exactly one -1, nothing lost.
    let empty = FxHashMap::default();
    let r = g2.execute_cycle(&empty, 11_000, None).await.unwrap();
    let batches = r.get("recent").expect("recent output");
    let mut wts = Vec::new();
    for b in batches {
        let w = b
            .column(b.schema().index_of("__weight").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let ts = b
            .column(b.schema().index_of("ts").unwrap())
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            wts.push((w.value(i), ts.value(i)));
        }
    }
    assert_eq!(
        wts,
        vec![(-1, 1000)],
        "only AAPL@1000 ages out post-restore"
    );

    // Re-advancing to the same frontier must not double-retract.
    let r = g2.execute_cycle(&empty, 11_000, None).await.unwrap();
    assert_eq!(total_rows(&r, "recent"), 0);
}

struct DelayOperator;

#[async_trait]
impl GraphOperator for DelayOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Ok(())
    }
}

struct SignalThenPendingOperator {
    entered: Option<tokio::sync::oneshot::Sender<(usize, Option<f64>)>>,
}

fn stateful_probe_observation(inputs: &[Vec<RecordBatch>]) -> (usize, Option<f64>) {
    let batches = inputs.iter().flat_map(|port| port.iter());
    let rows = batches.clone().map(RecordBatch::num_rows).sum();
    let bid = batches
        .filter_map(|batch| batch.column_by_name("bid"))
        .filter_map(|column| column.as_any().downcast_ref::<Float64Array>())
        .find_map(|column| (!column.is_empty()).then(|| column.value(0)));
    (rows, bid)
}

#[async_trait]
impl GraphOperator for SignalThenPendingOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        if let Some(entered) = self.entered.take() {
            let _ = entered.send(stateful_probe_observation(inputs));
        }
        std::future::pending().await
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

struct PanicAfterInputOperator(Arc<parking_lot::Mutex<Option<(usize, Option<f64>)>>>);

#[async_trait]
impl GraphOperator for PanicAfterInputOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        *self.0.lock() = Some(stateful_probe_observation(inputs));
        panic!("injected panic after stateful upstream output");
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }
}

/// Helper: total row count from result batches.
fn total_rows(results: &FxHashMap<Arc<str>, Vec<RecordBatch>>, key: &str) -> usize {
    results
        .get(key)
        .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum())
}

fn full_state_frames(
    capture: GraphStateCapture,
) -> (
    Vec<(String, bytes::Bytes)>,
    Vec<(String, u32, bytes::Bytes)>,
) {
    let whole = capture
        .whole
        .into_iter()
        .map(|state| (state.operator_id, state.state))
        .collect();
    let vnodes = capture
        .vnodes
        .into_iter()
        .map(|(operator, state)| {
            (
                operator,
                state.vnode,
                state
                    .state
                    .expect("the first capture must contain a full vnode frame"),
            )
        })
        .collect();
    (whole, vnodes)
}

/// Creates a graph with streaming functions registered and generous budget.
fn test_graph() -> OperatorGraph {
    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);
    // Debug builds are slow — use a generous budget for tests.
    graph.set_query_budget_ns(5_000_000_000); // 5 seconds
    graph
}
struct CheckpointedBidOperator {
    latest_bid: Option<f64>,
}

#[async_trait]
impl GraphOperator for CheckpointedBidOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let output: Vec<_> = inputs.iter().flatten().cloned().collect();
        if let Some(bid) = output
            .iter()
            .filter_map(|batch| batch.column_by_name("bid"))
            .filter_map(|column| column.as_any().downcast_ref::<Float64Array>())
            .find_map(|column| (!column.is_empty()).then(|| column.value(column.len() - 1)))
        {
            self.latest_bid = Some(bid);
        }
        Ok(output)
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(self.latest_bid.map(|bid| OperatorCheckpoint {
            data: bid.to_le_bytes().to_vec(),
        }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let bytes: [u8; 8] = checkpoint
            .data
            .try_into()
            .map_err(|_| DbError::Pipeline("invalid checkpointed bid state".into()))?;
        self.latest_bid = Some(f64::from_le_bytes(bytes));
        Ok(())
    }
}

fn checkpointed_bid_test_graph() -> OperatorGraph {
    let mut graph = test_graph();
    let stateful = graph
        .place_operator_node(
            "checkpointed_bid",
            Box::new(CheckpointedBidOperator { latest_bid: None }),
            1,
        )
        .unwrap();
    let source = graph.ensure_source_node("quotes");
    graph.add_edge(source, stateful, 0);
    graph
        .output_map
        .insert(Arc::from("checkpointed_bid"), stateful);
    graph.topo_dirty = true;
    graph
}

fn append_stateful_downstream_probe(graph: &mut OperatorGraph, operator: Box<dyn GraphOperator>) {
    let stateful = *graph
        .output_map
        .get("checkpointed_bid")
        .expect("stateful output node");
    let probe = graph
        .place_operator_node("stateful_probe", operator, 1)
        .unwrap();
    graph.add_edge(stateful, probe, 0);
    graph.topo_dirty = true;
}

fn bid_batch(bid: f64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "bid",
        DataType::Float64,
        false,
    )]));
    RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(vec![bid]))]).unwrap()
}

fn bid_sources(bid: f64) -> FxHashMap<Arc<str>, Vec<RecordBatch>> {
    let mut sources = FxHashMap::default();
    sources.insert(Arc::from("quotes"), vec![bid_batch(bid)]);
    sources
}

fn assert_graph_execution_poison(error: &DbError) {
    let DbError::StatefulOperatorPartialApply(reason) = error else {
        panic!("expected graph execution poison, got {error}");
    };
    assert!(reason.contains("cancelled or panicked"), "{reason}");
    assert!(reason.contains("last committed checkpoint"), "{reason}");
}

struct AlwaysFailOperator;

#[async_trait]
impl GraphOperator for AlwaysFailOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Err(DbError::Pipeline("injected operator failure".into()))
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Ok(())
    }
}

struct TerminalShuffleOperator;

#[async_trait]
impl GraphOperator for TerminalShuffleOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::test_probe()
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Err(DbError::ShuffleTerminal(
            "injected permanent routing failure".into(),
        ))
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        Ok(())
    }
}

fn terminal_shuffle_graph(query_budget_ns: u64) -> OperatorGraph {
    let mut graph = test_graph();
    graph.set_query_budget_ns(query_budget_ns);
    graph.set_shared_source_isolation(true, usize::MAX);
    let source = graph.ensure_source_node("trades");
    let terminal = graph
        .place_operator_node("terminal", Box::new(TerminalShuffleOperator), 1)
        .unwrap();
    let healthy = graph
        .place_operator_node("healthy", Box::new(SourcePassthrough), 1)
        .unwrap();
    graph.add_edge(source, terminal, 0);
    graph.add_edge(source, healthy, 0);
    graph.output_map.insert(Arc::from("terminal"), terminal);
    graph.output_map.insert(Arc::from("healthy"), healthy);
    graph.topo_dirty = true;
    graph
}

#[cfg(feature = "cluster")]
#[tokio::test]
async fn checkpoint_capture_guard_excludes_assignment_publication() {
    let mut graph = test_graph();
    let fence = Arc::new(tokio::sync::RwLock::new(()));
    graph.set_rotation_execution_fence(Arc::clone(&fence));
    let writer = Arc::clone(&fence).write_owned().await;

    let mut capture = Box::pin(graph.checkpoint_rotation_guard_until(
        tokio::time::Instant::now() + std::time::Duration::from_secs(1),
    ));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut capture)
            .await
            .is_err(),
        "capture must wait while assignment publication owns the write fence"
    );
    drop(writer);

    let reader = capture
        .await
        .expect("capture should acquire the released rotation fence")
        .expect("configured graph should return a rotation token");
    assert!(
        Arc::clone(&fence).try_write_owned().is_err(),
        "assignment publication must remain excluded through mutable capture"
    );
    drop(reader);
    assert!(Arc::clone(&fence).try_write_owned().is_ok());
}

#[cfg(feature = "cluster")]
#[test]
fn managed_vnode_capture_requires_the_exact_owned_roster() {
    struct InexactCapture(Vec<u32>);

    #[async_trait]
    impl GraphOperator for InexactCapture {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_vnode_state()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn checkpoint_vnodes(
            &mut self,
            _required_vnodes: &[u32],
            _vnode_count: u32,
        ) -> Result<Option<Vec<CapturedVnodeState>>, DbError> {
            Ok(Some(
                self.0
                    .iter()
                    .map(|vnode| CapturedVnodeState {
                        vnode: *vnode,
                        state: Some(bytes::Bytes::from_static(b"state")),
                    })
                    .collect(),
            ))
        }
    }

    for (captured, expected_message) in [
        (vec![0], "captured vnode roster [0]; expected [0, 1]"),
        (
            vec![0, 1, 2],
            "captured vnode roster [0, 1, 2]; expected [0, 1]",
        ),
    ] {
        let mut graph = test_graph();
        graph.set_test_vnode_count(2);
        graph.push_test_node("managed", Box::new(InexactCapture(captured)));

        let error = graph
            .capture_state()
            .expect_err("an inexact managed capture roster must fail closed");

        assert!(error.to_string().contains(expected_message), "{error}");
    }
}

#[cfg(feature = "cluster")]
#[test]
fn managed_state_placement_scopes_capture_and_restore_rosters() {
    struct CaptureProbe {
        capability: OperatorCapability,
        observed: Arc<parking_lot::Mutex<Vec<Vec<u32>>>>,
    }

    #[async_trait]
    impl GraphOperator for CaptureProbe {
        fn cluster_capability(&self) -> OperatorCapability {
            self.capability
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn checkpoint_vnodes(
            &mut self,
            required_vnodes: &[u32],
            _vnode_count: u32,
        ) -> Result<Option<Vec<CapturedVnodeState>>, DbError> {
            self.observed.lock().push(required_vnodes.to_vec());
            Ok(Some(
                required_vnodes
                    .iter()
                    .map(|vnode| CapturedVnodeState {
                        vnode: *vnode,
                        state: Some(bytes::Bytes::from_static(b"state")),
                    })
                    .collect(),
            ))
        }
    }

    for owned in [vec![1, 2], vec![0, 2]] {
        let global_observed = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let keyed_observed = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let mut graph = test_graph();
        graph.set_test_owned_vnodes(3, owned.clone());
        graph.push_test_node(
            "global",
            Box::new(CaptureProbe {
                capability: OperatorCapability::test_global_state(),
                observed: Arc::clone(&global_observed),
            }),
        );
        graph.push_test_node(
            "keyed",
            Box::new(CaptureProbe {
                capability: OperatorCapability::test_vnode_state(),
                observed: Arc::clone(&keyed_observed),
            }),
        );

        let captured = graph.capture_state().unwrap();
        let expected_global = owned
            .contains(&0)
            .then_some(vec![vec![0]])
            .unwrap_or_default();
        assert_eq!(global_observed.lock().as_slice(), expected_global);
        assert_eq!(
            keyed_observed.lock().as_slice(),
            std::slice::from_ref(&owned)
        );

        let mut captured_vnodes: Vec<u32> = captured
            .vnodes
            .iter()
            .map(|(_, state)| state.vnode)
            .collect();
        captured_vnodes.sort_unstable();
        captured_vnodes.dedup();
        assert_eq!(captured_vnodes, owned);
        for vnode in &owned {
            let mut names: Vec<&str> = captured
                .vnodes
                .iter()
                .filter_map(|(name, state)| (state.vnode == *vnode).then_some(name.as_str()))
                .collect();
            names.sort_unstable();
            assert_eq!(
                names,
                if *vnode == 0 {
                    vec!["global", "keyed"]
                } else {
                    vec!["keyed"]
                }
            );
        }
    }
}

#[tokio::test]
async fn declared_managed_state_requires_an_initializer() {
    struct MissingInitializer;

    #[async_trait]
    impl GraphOperator for MissingInitializer {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_vnode_state()
        }

        async fn process(
            &mut self,
            _inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }
    }

    let mut graph = test_graph();
    graph.push_test_node("missing-initializer", Box::new(MissingInitializer));

    let error = graph
        .initialize_managed_state()
        .await
        .err()
        .expect("declared managed state without an initializer must fail startup");

    assert!(
        error
            .to_string()
            .contains("managed-state initialization for operator 'missing-initializer' failed"),
        "{error}"
    );
}

#[test]
fn test_node_domains_disjoint_queries_separate() {
    let mut graph = test_graph();
    graph.register_source_schema("trades_a".to_string(), test_schema());
    graph.register_source_schema("trades_b".to_string(), test_schema());
    graph.add_query(
        "qa".to_string(),
        "SELECT symbol FROM trades_a".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "qb".to_string(),
        "SELECT symbol FROM trades_b".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.compute_topo_order();

    assert_eq!(
        graph.domain_count, 2,
        "disjoint-source queries are separate domains"
    );
    let a = graph.source_map.get("trades_a").copied().unwrap();
    let b = graph.source_map.get("trades_b").copied().unwrap();
    assert_ne!(graph.node_domain[a], graph.node_domain[b]);
}

#[test]
fn test_node_domains_shared_source_joined() {
    let mut graph = test_graph();
    graph.register_source_schema("trades".to_string(), test_schema());
    graph.add_query(
        "qa".to_string(),
        "SELECT symbol FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "qb".to_string(),
        "SELECT price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.compute_topo_order();

    assert_eq!(
        graph.domain_count, 1,
        "queries sharing a source recover together"
    );
}

#[test]
fn test_node_domains_shared_source_isolated() {
    let mut graph = test_graph();
    graph.set_shared_source_isolation(true, usize::MAX);
    graph.register_source_schema("trades".to_string(), test_schema());
    graph.add_query(
        "qa".to_string(),
        "SELECT symbol FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "qb".to_string(),
        "SELECT price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.compute_topo_order();

    assert_eq!(
        graph.domain_count, 2,
        "isolation splits shared-source queries into separate domains"
    );
    let qa = graph.find_node("qa").unwrap();
    let qb = graph.find_node("qb").unwrap();
    assert_ne!(graph.node_domain[qa], graph.node_domain[qb]);
    let src = graph.source_map.get("trades").copied().unwrap();
    assert_eq!(
        graph.node_domain[src],
        usize::MAX,
        "an isolated source is not a failure domain of its own"
    );
}

// A fault in one query sharing a source must not sink a sibling reading the same source: the
// healthy query still emits, and the shared source is held back because it feeds the faulted domain.

#[tokio::test]
async fn terminal_shuffle_bypasses_main_failure_domain_isolation() {
    let mut graph = terminal_shuffle_graph(u64::MAX);

    let error = graph
        .execute_cycle(&trades_source(), i64::MAX, None)
        .await
        .expect_err("terminal routing must abort before isolating one domain");

    assert!(matches!(error, DbError::ShuffleTerminal(_)));
    assert!(!graph.take_cycle_failures().0);
}

#[tokio::test]
async fn terminal_shuffle_bypasses_deferred_failure_domain_isolation() {
    let mut graph = terminal_shuffle_graph(0);

    let error = graph
        .execute_cycle(&trades_source(), i64::MAX, None)
        .await
        .expect_err("a deferred terminal routing failure must abort the cycle");

    assert!(matches!(error, DbError::ShuffleTerminal(_)));
    assert!(!graph.take_cycle_failures().0);
}

#[tokio::test]
async fn test_execute_cycle_isolates_shared_source_sibling() {
    let mut graph = test_graph();
    graph.set_shared_source_isolation(true, usize::MAX);
    let source_node = graph.ensure_source_node("trades");
    let failing = graph
        .place_operator_node("failing", Box::new(AlwaysFailOperator), 1)
        .unwrap();
    let healthy = graph
        .place_operator_node("healthy", Box::new(SourcePassthrough), 1)
        .unwrap();
    graph.add_edge(source_node, failing, 0);
    graph.add_edge(source_node, healthy, 0);
    graph.output_map.insert(Arc::from("failing"), failing);
    graph.output_map.insert(Arc::from("healthy"), healthy);
    graph.topo_dirty = true;

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    let results = graph
        .execute_cycle(&source, i64::MAX, None)
        .await
        .expect("the healthy sibling keeps the cycle Ok though they share a source");

    assert_eq!(
        total_rows(&results, "healthy"),
        2,
        "healthy sibling emitted despite sharing the faulted source"
    );
    assert_eq!(
        total_rows(&results, "failing"),
        0,
        "faulted domain emitted nothing"
    );

    let (any_failed, failed_sources) = graph.take_cycle_failures();
    assert!(any_failed);
    assert!(
        failed_sources.contains(&Arc::from("trades")),
        "the shared source is held back: it feeds the faulted domain"
    );
}

// A transient fault in one shared-source query replays from the preserved input on the next
// cycle (cycle-1 rows + cycle-2 rows), while the healthy sibling only sees new rows.
#[tokio::test]
async fn test_shared_source_isolation_replays_faulted_domain() {
    struct ReplayTestOp {
        fail_once: bool,
        has_failed: bool,
    }
    #[async_trait]
    impl GraphOperator for ReplayTestOp {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            if self.fail_once && !self.has_failed {
                self.has_failed = true;
                return Err(DbError::Pipeline("transient fault".into()));
            }
            Ok(inputs.first().cloned().unwrap_or_default())
        }
        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }
        fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
            Ok(())
        }
    }

    let mut graph = test_graph();
    graph.set_shared_source_isolation(true, usize::MAX);
    let src = graph.ensure_source_node("trades");
    let a = graph
        .place_operator_node(
            "a",
            Box::new(ReplayTestOp {
                fail_once: true,
                has_failed: false,
            }),
            1,
        )
        .unwrap();
    graph.add_edge(src, a, 0);
    graph.output_map.insert(Arc::from("a"), a);
    let b = graph
        .place_operator_node(
            "b",
            Box::new(ReplayTestOp {
                fail_once: false,
                has_failed: false,
            }),
            1,
        )
        .unwrap();
    graph.add_edge(src, b, 0);
    graph.output_map.insert(Arc::from("b"), b);
    graph.topo_dirty = true;

    let mut cycle1 = FxHashMap::default();
    cycle1.insert(Arc::from("trades"), vec![test_batch()]);
    let r1 = graph
        .execute_cycle(&cycle1, i64::MAX, None)
        .await
        .expect("healthy sibling keeps cycle 1 Ok");
    assert_eq!(total_rows(&r1, "b"), 2, "healthy sibling emitted cycle 1");
    assert_eq!(
        total_rows(&r1, "a"),
        0,
        "faulted op emitted nothing cycle 1"
    );
    let (_, failed) = graph.take_cycle_failures();
    assert!(failed.contains(&Arc::from("trades")));

    let mut cycle2 = FxHashMap::default();
    cycle2.insert(Arc::from("trades"), vec![test_batch()]);
    let r2 = graph
        .execute_cycle(&cycle2, i64::MAX, None)
        .await
        .expect("cycle 2 Ok");
    assert_eq!(
        total_rows(&r2, "a"),
        4,
        "faulted op replays preserved cycle-1 rows plus new cycle-2 rows"
    );
    assert_eq!(
        total_rows(&r2, "b"),
        2,
        "healthy sibling sees only new rows (no replay)"
    );
    let (any_failed2, _) = graph.take_cycle_failures();
    assert!(!any_failed2, "no fault on the replay cycle");
}

// A fatal error in one disjoint query must not sink the sibling query: the healthy domain
// still produces output, and only the faulted domain's source is held back from committing.
#[tokio::test]
async fn test_execute_cycle_isolates_failed_domain() {
    let mut graph = test_graph();
    let source_a = graph.ensure_source_node("trades_a");
    let source_b = graph.ensure_source_node("trades_b");
    let failing = graph
        .place_operator_node("failing", Box::new(AlwaysFailOperator), 1)
        .unwrap();
    let healthy = graph
        .place_operator_node("filtered", Box::new(SourcePassthrough), 1)
        .unwrap();
    graph.add_edge(source_a, failing, 0);
    graph.add_edge(source_b, healthy, 0);
    graph.output_map.insert(Arc::from("failing"), failing);
    graph.output_map.insert(Arc::from("filtered"), healthy);
    graph.topo_dirty = true;

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades_a"), vec![test_batch()]);
    source.insert(Arc::from("trades_b"), vec![test_batch()]);

    let results = graph
        .execute_cycle(&source, i64::MAX, None)
        .await
        .expect("a healthy sibling domain keeps the cycle Ok");

    assert_eq!(
        total_rows(&results, "filtered"),
        2,
        "healthy domain emitted"
    );
    assert_eq!(
        total_rows(&results, "failing"),
        0,
        "faulted domain emitted nothing"
    );

    let (any_failed, failed_sources) = graph.take_cycle_failures();
    assert!(any_failed);
    assert!(failed_sources.contains(&Arc::from("trades_a")));
    assert!(!failed_sources.contains(&Arc::from("trades_b")));
}

#[tokio::test]
async fn test_og_compiled_projection() {
    // Non-aggregate projection-only query should compile to PhysicalExpr
    let mut graph = test_graph();
    graph.add_query(
        "projected".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    // First cycle triggers lazy init
    let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&r, "projected"), 2); // Both rows projected

    // Second cycle reuses compiled path (no SQL overhead)
    let r2 = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&r2, "projected"), 2);
}

#[tokio::test]
async fn test_og_compiled_fallback_on_type_mismatch() {
    // WHERE price > 200 has Float64 > Int64 type mismatch that
    // DataFusion's create_physical_expr doesn't coerce. Compiled
    // path should fall back to CachedPlan transparently.
    let mut graph = test_graph();
    graph.add_query(
        "filtered".to_string(),
        "SELECT symbol, price FROM trades WHERE price > 200".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&r, "filtered"), 1); // Only GOOG passes
}

#[tokio::test]
async fn test_og_aggregate_incremental() {
    // GROUP BY should route through IncrementalAggState
    let mut graph = test_graph();
    graph.add_query(
        "agg".to_string(),
        "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    // Cycle 1
    let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&r, "agg"), 2); // AAPL + GOOG groups

    // Cycle 2: running totals accumulate
    let r2 = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    let agg_batches = &r2[&Arc::from("agg") as &Arc<str>];
    assert_eq!(total_rows(&r2, "agg"), 2); // Still 2 groups

    // Verify accumulation: AAPL should be 150+150=300
    let price_col = agg_batches[0]
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let symbol_col = agg_batches[0]
        .column_by_name("symbol")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    for i in 0..agg_batches[0].num_rows() {
        match symbol_col.value(i) {
            "AAPL" => assert!((price_col.value(i) - 300.0).abs() < f64::EPSILON),
            "GOOG" => assert!((price_col.value(i) - 5600.0).abs() < f64::EPSILON),
            other => panic!("unexpected symbol: {other}"),
        }
    }
}

#[tokio::test]
async fn named_bounded_join_feeds_keyed_aggregate_at_safe_frontier() {
    use arrow::array::TimestampMillisecondArray;
    use arrow::datatypes::TimeUnit;
    use laminar_sql::parser::join_parser::JoinType;
    use laminar_sql::translator::{JoinOperatorConfig, StreamJoinConfig};
    use std::time::Duration;

    let mut graph = test_graph();
    let orders_schema = Arc::new(Schema::new(vec![
        Field::new("account", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let receipts_schema = Arc::new(Schema::new(vec![
        Field::new("account", DataType::Utf8, false),
        Field::new("receipt_id", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    graph.register_source_schema("orders".to_string(), Arc::clone(&orders_schema));
    graph.register_source_schema("receipts".to_string(), Arc::clone(&receipts_schema));

    let mut join_config = StreamJoinConfig::new(
        JoinType::Inner,
        vec!["account".to_string()],
        vec!["account".to_string()],
        Duration::from_secs(1),
    );
    join_config.left_table = "orders".to_string();
    join_config.right_table = "receipts".to_string();
    join_config.left_time_column = "ts".to_string();
    join_config.right_time_column = "ts".to_string();
    graph.add_query(
        "matched".to_string(),
        "SELECT o.account AS account, o.amount AS amount FROM orders o JOIN receipts r \
         ON o.account = r.account \
         AND r.ts BETWEEN o.ts AND o.ts + INTERVAL '1' SECOND"
            .to_string(),
        None,
        None,
        None,
        Some(vec![JoinOperatorConfig::StreamStream(join_config)]),
        false,
    );
    graph.add_query(
        "totals".to_string(),
        "SELECT account, SUM(amount) AS total, COUNT(*) AS match_count \
         FROM matched GROUP BY account"
            .to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.register_intermediate_schema(
        "matched",
        &Arc::new(Schema::new(vec![
            Field::new("account", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
        ])),
    );
    graph.take_build_errors().unwrap();

    let orders = RecordBatch::try_new(
        orders_schema,
        vec![
            Arc::new(StringArray::from(vec!["acct-a", "acct-b"])),
            Arc::new(Int64Array::from(vec![10, 999])),
            Arc::new(TimestampMillisecondArray::from(vec![5_000, 6_000])),
        ],
    )
    .unwrap();
    let receipts = RecordBatch::try_new(
        receipts_schema,
        vec![
            Arc::new(StringArray::from(vec!["acct-a"])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(TimestampMillisecondArray::from(vec![5_500])),
        ],
    )
    .unwrap();
    let mut sources = FxHashMap::default();
    sources.insert(Arc::from("orders"), vec![orders]);
    sources.insert(Arc::from("receipts"), vec![receipts]);
    let mut source_watermarks = FxHashMap::default();
    source_watermarks.insert(Arc::from("orders"), 4_000);
    source_watermarks.insert(Arc::from("receipts"), 4_500);

    let results = graph
        .execute_cycle(&sources, 4_500, Some(&source_watermarks))
        .await
        .unwrap();
    assert_eq!(total_rows(&results, "matched"), 1);
    assert_eq!(total_rows(&results, "totals"), 1);

    let totals = &results.get("totals").unwrap()[0];
    let accounts = totals
        .column_by_name("account")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let sums = totals
        .column_by_name("total")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let counts = totals
        .column_by_name("match_count")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(accounts.value(0), "acct-a");
    assert_eq!(sums.value(0), 10);
    assert_eq!(counts.value(0), 1);

    let joined = *graph.output_map.get("matched").unwrap();
    let aggregate = *graph.output_map.get("totals").unwrap();
    let safe_frontier = 4_000_i64.min(4_500 - 1_000);
    assert_eq!(graph.output_watermarks[joined], safe_frontier);
    assert_eq!(graph.output_watermarks[aggregate], safe_frontier);
}

#[tokio::test]
async fn test_og_cascading() {
    // Query A feeds Query B through intermediate LiveSourceProvider
    let mut graph = test_graph();
    graph.add_query(
        "step1".to_string(),
        "SELECT symbol, price * 2 AS doubled FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "step2".to_string(),
        "SELECT symbol, doubled FROM step1 WHERE doubled > 400".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    // step1: AAPL=300, GOOG=5600 (2 rows)
    assert_eq!(total_rows(&r, "step1"), 2);
    // step2: only GOOG=5600 passes WHERE doubled > 400
    assert_eq!(total_rows(&r, "step2"), 1);
}

#[test]
fn test_og_rejects_unbounded_diamond_fanin() {
    let mut graph = test_graph();
    graph.add_query(
        "high".to_string(),
        "SELECT symbol, price FROM trades WHERE price > 200".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "low".to_string(),
        "SELECT symbol, price FROM trades WHERE price <= 200".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "combined".to_string(),
        "SELECT h.symbol, h.price FROM high h INNER JOIN low l ON h.symbol = l.symbol".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    let error = graph.take_build_errors().unwrap_err();
    assert!(error.to_string().contains("unbounded join"));
    assert!(!graph.has_query("combined"));
}

#[test]
fn test_og_rejects_generic_cross_join_fallback() {
    let mut graph = test_graph();
    graph.add_query(
        "crossed".to_string(),
        "SELECT l.symbol FROM trades l CROSS JOIN trades r".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let error = graph.take_build_errors().unwrap_err();
    assert!(error.to_string().contains("could not be planned"));
    assert!(!graph.has_query("crossed"));
}

#[tokio::test]
async fn test_og_budget_exhaustion() {
    // With a tiny budget (1 ns), only the first operator runs
    let mut graph = test_graph();
    graph.set_query_budget_ns(1); // 1 ns budget — effectively skip after first

    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "q2".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();

    // With 1ns budget, not all queries should produce output
    let produced = r.len();
    assert!(
        produced < 2,
        "with 1ns budget, at most one query should run"
    );
}

#[tokio::test]
async fn test_og_budget_deferred_forward_progress() {
    // With a 1ns budget, only the first operator runs in the main loop.
    // The deferred execution pass must guarantee every operator eventually
    // processes its input within N cycles (N = number of deferred operators).
    let mut graph = test_graph();
    graph.set_query_budget_ns(1); // forces break after first operator

    // Add 5 independent queries — all read from "trades"
    for i in 0..5 {
        graph.add_query(
            format!("q{i}"),
            "SELECT * FROM trades".to_string(),
            None,
            None,
            None,
            None,
            false,
        );
    }

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    // Run enough cycles for all 5 operators to get their turn via
    // deferred execution (1 main + 1 deferred per cycle = 5 cycles).
    let mut produced = FxHashSet::default();
    for _ in 0..5 {
        let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
        for key in r.keys() {
            produced.insert(key.to_string());
        }
    }

    assert_eq!(
        produced.len(),
        5,
        "all 5 operators should produce output within 5 cycles, got: {produced:?}"
    );
}

#[tokio::test]
async fn checkpoint_drain_bypasses_query_budget_and_emits_each_row_once() {
    let mut graph = test_graph();
    // This root runs before the source and makes the near-zero budget deterministic.
    graph
        .place_operator_node("delay", Box::new(DelayOperator), 1)
        .unwrap();
    let source = graph.ensure_source_node("trades");
    let middle = graph
        .place_operator_node("middle", Box::new(SourcePassthrough), 1)
        .unwrap();
    let output = graph
        .place_operator_node("output", Box::new(SourcePassthrough), 1)
        .unwrap();
    graph.add_edge(source, middle, 0);
    graph.add_edge(middle, output, 0);
    graph.output_map.insert(Arc::from("output"), output);
    graph.topo_dirty = true;
    graph.set_query_budget_ns(1);

    let batch = test_batch();
    let expected_edge_bytes = batch.get_array_memory_size();
    let mut sources = FxHashMap::default();
    sources.insert(Arc::from("trades"), vec![batch]);

    let normal = graph.execute_cycle(&sources, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&normal, "output"), 0);
    let (any_deferred, deferred_sources) = graph.take_cycle_deferrals();
    assert!(any_deferred);
    assert!(deferred_sources.contains(&Arc::from("trades")));
    assert_eq!(
        graph.checkpoint_pending_input_bytes(),
        expected_edge_bytes,
        "normal budget deferral leaves the source row batch on the middle edge"
    );

    let mut emitted_symbols = Vec::new();
    for _ in 0..3 {
        let mut drained = graph
            .execute_checkpoint_drain_cycle(i64::MAX, None)
            .await
            .unwrap();
        for output_batch in drained.remove("output").unwrap_or_default() {
            let symbols = output_batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            emitted_symbols
                .extend((0..output_batch.num_rows()).map(|row| symbols.value(row).to_string()));
        }
        if graph.checkpoint_is_quiescent() {
            break;
        }
    }

    assert_eq!(graph.checkpoint_pending_input_bytes(), 0);
    assert!(graph.checkpoint_is_quiescent());
    assert_eq!(emitted_symbols, ["AAPL", "GOOG"]);

    let after_quiescence = graph
        .execute_checkpoint_drain_cycle(i64::MAX, None)
        .await
        .unwrap();
    assert_eq!(
        total_rows(&after_quiescence, "output"),
        0,
        "a drained edge is not replayed"
    );
}

#[tokio::test]
async fn checkpoint_drain_accounting_includes_deferred_source_ports() {
    let mut graph = test_graph();
    graph
        .place_operator_node("delay", Box::new(DelayOperator), 1)
        .unwrap();
    let source_a = graph.ensure_source_node("source_a");
    let source_b = graph.ensure_source_node("source_b");
    let output_a = graph
        .place_operator_node("output_a", Box::new(SourcePassthrough), 1)
        .unwrap();
    let output_b = graph
        .place_operator_node("output_b", Box::new(SourcePassthrough), 1)
        .unwrap();
    graph.add_edge(source_a, output_a, 0);
    graph.add_edge(source_b, output_b, 0);
    graph.output_map.insert(Arc::from("output_a"), output_a);
    graph.output_map.insert(Arc::from("output_b"), output_b);
    graph.topo_dirty = true;
    graph.set_query_budget_ns(1);

    let batch = test_batch();
    let batch_bytes = batch.get_array_memory_size();
    let mut sources = FxHashMap::default();
    sources.insert(Arc::from("source_a"), vec![batch.clone()]);
    sources.insert(Arc::from("source_b"), vec![batch]);

    let normal = graph.execute_cycle(&sources, 10, None).await.unwrap();
    assert!(normal.is_empty());
    assert_eq!(graph.input_bufs[source_b][0].len(), 1);
    assert_eq!(
        graph.checkpoint_pending_input_bytes(),
        batch_bytes.saturating_mul(2),
        "one routed edge and one budget-deferred source port are both accounted"
    );
    assert!(!graph.checkpoint_is_quiescent());

    let drained = graph
        .execute_checkpoint_drain_cycle(10, None)
        .await
        .unwrap();
    assert_eq!(total_rows(&drained, "output_a"), 2);
    assert_eq!(total_rows(&drained, "output_b"), 2);
    assert!(graph.checkpoint_is_quiescent());
}

#[tokio::test]
async fn checkpoint_drain_quiescence_detects_zero_byte_row_batch() {
    let mut graph = test_graph();
    let source = graph.ensure_source_node("empty_schema_source");
    let output = graph
        .place_operator_node("output", Box::new(SourcePassthrough), 1)
        .unwrap();
    graph.add_edge(source, output, 0);
    graph.output_map.insert(Arc::from("output"), output);
    graph.topo_dirty = true;

    let options = arrow::array::RecordBatchOptions::new().with_row_count(Some(3));
    let zero_byte_rows =
        RecordBatch::try_new_with_options(Arc::new(Schema::empty()), Vec::new(), &options).unwrap();
    assert_eq!(zero_byte_rows.num_rows(), 3);
    assert_eq!(zero_byte_rows.get_array_memory_size(), 0);
    prefill_port(&mut graph, output, 0, vec![zero_byte_rows]);

    assert_eq!(graph.checkpoint_pending_input_bytes(), 0);
    assert!(!graph.checkpoint_is_quiescent());

    let drained = graph
        .execute_checkpoint_drain_cycle(10, None)
        .await
        .unwrap();
    assert_eq!(total_rows(&drained, "output"), 3);
    assert!(graph.checkpoint_is_quiescent());
}

#[tokio::test]
async fn checkpoint_drain_does_not_poll_unrelated_aggregate_branch() {
    let mut graph = test_graph();
    graph.register_source_schema("trades".to_string(), test_schema());
    graph.add_query(
        "agg".to_string(),
        "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let mut trades = FxHashMap::default();
    trades.insert(Arc::from("trades"), vec![test_batch()]);
    let initial = graph.execute_cycle(&trades, 10, None).await.unwrap();
    assert_eq!(total_rows(&initial, "agg"), 2);
    assert!(graph.checkpoint_is_quiescent());

    let other_source = graph.ensure_source_node("other");
    let other_output = graph
        .place_operator_node("other_output", Box::new(SourcePassthrough), 1)
        .unwrap();
    graph.add_edge(other_source, other_output, 0);
    graph
        .output_map
        .insert(Arc::from("other_output"), other_output);
    graph.topo_dirty = true;
    prefill_port(&mut graph, other_source, 0, vec![test_batch()]);

    let drained = graph
        .execute_checkpoint_drain_cycle(10, None)
        .await
        .unwrap();
    assert_eq!(total_rows(&drained, "other_output"), 2);
    assert_eq!(
        total_rows(&drained, "agg"),
        0,
        "the unchanged aggregate branch must not re-emit during another branch's drain"
    );
    assert!(graph.checkpoint_is_quiescent());
}

#[tokio::test]
async fn checkpoint_drain_failure_or_no_progress_preserves_pending_edges() {
    struct PausedOperator;

    #[async_trait]
    impl GraphOperator for PausedOperator {
        fn cluster_capability(&self) -> OperatorCapability {
            OperatorCapability::test_probe()
        }

        async fn process(
            &mut self,
            inputs: &[Vec<RecordBatch>],
            _watermarks: &[i64],
        ) -> Result<Vec<RecordBatch>, DbError> {
            assert!(inputs.is_empty(), "paused operator must not accept input");
            Ok(Vec::new())
        }

        fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
            Ok(None)
        }

        fn restore(&mut self, _checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
            Ok(())
        }

        fn wants_input(&self) -> bool {
            false
        }
    }

    let mut graph = test_graph();
    let source = graph.ensure_source_node("trades");
    let middle = graph
        .place_operator_node("middle", Box::new(SourcePassthrough), 1)
        .unwrap();
    let paused = graph
        .place_operator_node("paused", Box::new(PausedOperator), 1)
        .unwrap();
    graph.add_edge(source, middle, 0);
    graph.add_edge(middle, paused, 0);
    graph.topo_dirty = true;
    prefill_port(&mut graph, middle, 0, vec![test_batch()]);
    prefill_port(&mut graph, paused, 0, vec![test_batch()]);
    graph.set_max_input_buf_batches(1);

    let pending_before = graph.checkpoint_pending_input_bytes();
    assert_eq!(pending_before, 2 * test_batch().get_array_memory_size());
    assert!(!graph.checkpoint_is_quiescent());

    graph.set_backpressure_policy(BackpressurePolicy::Fail);
    let error = graph
        .execute_checkpoint_drain_cycle(i64::MAX, None)
        .await
        .expect_err("the checkpoint drain must preserve Fail backpressure semantics");
    assert!(matches!(error, DbError::BackpressureFail(_)));
    assert!(
        graph.execution_poison_reason().is_none(),
        "an explicit returned error must retain its disposition without looking like cancellation"
    );
    assert_eq!(graph.checkpoint_pending_input_bytes(), pending_before);
    assert!(!graph.checkpoint_is_quiescent());

    graph.set_backpressure_policy(BackpressurePolicy::Backpressure);
    graph
        .execute_checkpoint_drain_cycle(i64::MAX, None)
        .await
        .unwrap();
    assert_eq!(
        graph.checkpoint_pending_input_bytes(),
        pending_before,
        "a gated/paused drain cycle must not clear pending edge buffers"
    );
    assert_eq!(
        graph.output_watermarks[paused],
        i64::MIN,
        "an operator that declined buffered input must not advance its output watermark"
    );
    assert!(!graph.checkpoint_is_quiescent());
}

#[tokio::test]
async fn cancelled_stateful_cycle_poison_requires_fresh_graph_restore() {
    let mut graph = checkpointed_bid_test_graph();
    graph
        .execute_cycle(&bid_sources(10.0), i64::MIN, None)
        .await
        .unwrap();
    let checkpoint = graph.capture_state().unwrap();
    let (whole, vnodes) = full_state_frames(checkpoint);

    let (entered_tx, mut entered_rx) = tokio::sync::oneshot::channel();
    append_stateful_downstream_probe(
        &mut graph,
        Box::new(SignalThenPendingOperator {
            entered: Some(entered_tx),
        }),
    );
    let replay = bid_sources(20.0);
    let mut cycle = Box::pin(graph.execute_cycle(&replay, i64::MIN, None));
    let observation = tokio::select! {
        entered = &mut entered_rx => entered.expect("pending probe dropped its signal"),
        result = &mut cycle => panic!("cycle completed before cancellation: {result:?}"),
        () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
            panic!("stateful output did not reach the pending probe")
        }
    };
    assert_eq!(
        observation,
        (1, Some(20.0)),
        "the cancelled pass must route the newly admitted stateful output"
    );
    drop(cycle);

    let snapshot_error = match graph.capture_state() {
        Err(error) => error,
        Ok(_) => panic!("cancelled graph generation accepted a checkpoint"),
    };
    assert_graph_execution_poison(&snapshot_error);

    let execution_error = graph
        .execute_cycle(&FxHashMap::default(), i64::MIN, None)
        .await
        .expect_err("cancelled graph generation executed again");
    assert_graph_execution_poison(&execution_error);
    let drain_error = graph
        .execute_checkpoint_drain_cycle(i64::MIN, None)
        .await
        .expect_err("cancelled graph generation entered checkpoint drain");
    assert_graph_execution_poison(&drain_error);

    let (mut restored, restored_operators) = checkpointed_bid_test_graph()
        .restore_state_frames(
            &whole,
            &vnodes,
            u32::from(laminar_core::state::DEFAULT_KEY_GROUP_COUNT.get()),
        )
        .unwrap();
    assert_eq!(restored_operators, 1);
    let output = restored
        .execute_cycle(&replay, i64::MIN, None)
        .await
        .unwrap();
    let batches = output
        .get("checkpointed_bid")
        .expect("replayed stateful output");
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let bid = batches[0]
        .column_by_name("bid")
        .expect("bid column")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64 bid");
    assert_eq!(
        bid.value(0),
        20.0,
        "fresh prior-cut restore must observe the newer replayed quote"
    );
}

#[tokio::test]
async fn caught_stateful_cycle_panic_poison_prevents_graph_reuse() {
    use futures::FutureExt as _;

    let mut graph = checkpointed_bid_test_graph();
    graph
        .execute_cycle(&bid_sources(10.0), i64::MIN, None)
        .await
        .unwrap();
    let observation = Arc::new(parking_lot::Mutex::new(None));
    append_stateful_downstream_probe(
        &mut graph,
        Box::new(PanicAfterInputOperator(Arc::clone(&observation))),
    );
    let replay = bid_sources(20.0);

    let panic = std::panic::AssertUnwindSafe(graph.execute_cycle(&replay, i64::MIN, None))
        .catch_unwind()
        .await;
    assert!(panic.is_err(), "the downstream probe must panic");
    assert_eq!(*observation.lock(), Some((1, Some(20.0))));
    let snapshot_error = match graph.capture_state() {
        Err(error) => error,
        Ok(_) => panic!("panicked graph generation accepted a checkpoint"),
    };
    assert_graph_execution_poison(&snapshot_error);
    let execution_error = graph
        .execute_cycle(&FxHashMap::default(), i64::MIN, None)
        .await
        .expect_err("panicked graph generation executed again");
    assert_graph_execution_poison(&execution_error);
}

#[tokio::test]
async fn test_og_checkpoint_roundtrip_aggregate() {
    // Aggregate state should survive checkpoint + restore
    let mut graph = test_graph();
    graph.register_source_schema("trades".to_string(), test_schema());
    graph.add_query(
        "agg".to_string(),
        "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    let mut graph = graph.initialize_managed_state().await.unwrap();

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    // Cycle 1: build up state
    let _ = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();

    let checkpoint = graph.capture_state().unwrap();
    let (whole, vnodes) = full_state_frames(checkpoint);

    // Create a new graph with same query and restore
    let mut graph2 = test_graph();
    graph2.register_source_schema("trades".to_string(), test_schema());
    graph2.add_query(
        "agg".to_string(),
        "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    let graph2 = graph2.initialize_managed_state().await.unwrap();

    let (restored_graph, restored) = graph2
        .restore_state_frames(
            &whole,
            &vnodes,
            u32::from(laminar_core::state::DEFAULT_KEY_GROUP_COUNT.get()),
        )
        .unwrap();
    let mut graph2 = restored_graph;
    assert!(restored > 0, "should restore at least one operator");

    // New input is applied on top of the authoritative restored image.
    let r = graph2.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&r, "agg"), 2);
}

#[tokio::test]
async fn test_og_aggregate_empty_source_emits_state() {
    // Aggregate queries should emit running state even with no new input
    let mut graph = test_graph();
    graph.register_source_schema("trades".to_string(), test_schema());
    graph.add_query(
        "agg".to_string(),
        "SELECT symbol, SUM(price) AS total FROM trades GROUP BY symbol".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    // First cycle with data
    let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&r, "agg"), 2);

    // Second cycle with no data — should still emit accumulated state
    let empty_source = FxHashMap::default();
    let r2 = graph
        .execute_cycle(&empty_source, i64::MAX, None)
        .await
        .unwrap();
    assert_eq!(total_rows(&r2, "agg"), 2);
}

#[tokio::test]
async fn test_og_reverse_order_cascading() {
    // Queries added in reverse dependency order (q2 before q1).
    // q2 creates a SourcePassthrough placeholder for "q1". When q1 is
    // added, it replaces the placeholder in place so q2's existing edge
    // automatically receives q1's real output.
    let mut graph = test_graph();
    graph.add_query(
        "q2".to_string(),
        "SELECT symbol FROM q1 WHERE price > 200".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "q1".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    // "q1" should NOT be in source_map (it was replaced with a real query)
    assert!(
        !graph.source_map.contains_key("q1"),
        "q1 placeholder should be replaced, not in source_map"
    );
    assert!(graph.output_map.contains_key("q1"));
    assert!(graph.output_map.contains_key("q2"));

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    let r = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(total_rows(&r, "q1"), 2); // AAPL + GOOG
    assert_eq!(total_rows(&r, "q2"), 1); // Only GOOG (price=2800 > 200)
}

#[test]
fn test_pressure_zero_when_cap_disabled() {
    let mut graph = test_graph();
    graph.set_max_input_buf_batches(0); // unlimited
    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    // Push some data into the source buffer
    if let Some(&node_id) = graph.source_map.get("trades") {
        prefill_port(&mut graph, node_id, 0, vec![test_batch(); 10]);
    }
    assert!((graph.input_buf_pressure() - 0.0).abs() < f64::EPSILON);
}

#[test]
fn test_pressure_reflects_fill_ratio() {
    let mut graph = test_graph();
    graph.set_max_input_buf_batches(100);
    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    // Fill source buffer to 50% of cap
    if let Some(&node_id) = graph.source_map.get("trades") {
        prefill_port(&mut graph, node_id, 0, vec![test_batch(); 50]);
    }
    assert!((graph.input_buf_pressure() - 0.5).abs() < f64::EPSILON);
}

#[test]
fn test_pressure_clamped_at_one() {
    let mut graph = test_graph();
    graph.set_max_input_buf_batches(10);
    graph.add_query(
        "q1".to_string(),
        "SELECT * FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    // Overfill the buffer beyond cap — pressure clamps at 1.0.
    if let Some(&node_id) = graph.source_map.get("trades") {
        prefill_port(&mut graph, node_id, 0, vec![test_batch(); 20]);
    }
    assert!((graph.input_buf_pressure() - 1.0).abs() < f64::EPSILON);
}

#[test]
fn test_pressure_empty_graph() {
    let graph = test_graph();
    assert!((graph.input_buf_pressure() - 0.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn test_credit_gate_defers_producer_when_downstream_full() {
    let mut graph = test_graph();
    graph.set_max_input_buf_batches(4);

    // Two queries chained via an intermediate stream: the first projects
    // `trades`, the second reads from the first. The gate should skip the
    // first when the second's input port is full.
    graph.add_query(
        "proj".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "downstream".to_string(),
        "SELECT symbol FROM proj".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    // Find the downstream node id and pre-fill its input buffer at cap,
    // simulating a slow consumer.
    let downstream_id = *graph.output_map.get("downstream").unwrap();
    prefill_port(&mut graph, downstream_id, 0, vec![test_batch(); 4]);

    let proj_id = *graph.output_map.get("proj").unwrap();
    assert!(
        graph.is_downstream_at_capacity(proj_id),
        "proj's downstream should register as at capacity"
    );

    // Run a cycle with trade input. proj must be deferred because its
    // downstream is full — so proj's output_bufs should still hold its
    // source input, and downstream's input should not grow.
    let before_len = graph.input_bufs[downstream_id][0].len();
    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);
    let _ = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(
        graph.input_bufs[downstream_id][0].len(),
        before_len,
        "deferred producer must not have extended a full downstream buffer"
    );
}

// Replacing a SourcePassthrough placeholder must also clear source_node_ids,
// otherwise the node keeps its source-class flag and output_watermarks is
// never advanced — downstream TUMBLE windows never close.
#[tokio::test]
async fn test_placeholder_replacement_clears_source_classification() {
    let mut graph = test_graph();

    // Register the downstream query FIRST — its SQL references
    // `derived`, which triggers an `ensure_source_node("derived")` and
    // seeds `source_node_ids` with the placeholder.
    graph.add_query(
        "aggregate".to_string(),
        "SELECT symbol, SUM(price) AS total FROM derived GROUP BY symbol".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    // Now register `derived` — this replaces the placeholder.
    graph.add_query(
        "derived".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let derived_id = *graph.output_map.get("derived").unwrap();
    assert!(
        !graph.source_node_ids.contains(&derived_id),
        "real operator node must not be classified as a source after \
         placeholder replacement (blocks output_watermarks updates)"
    );
}

#[tokio::test]
async fn test_source_inputs_accumulate_when_deferred() {
    let mut graph = test_graph();
    graph.set_max_input_buf_batches(2);
    graph.add_query(
        "sink".to_string(),
        "SELECT symbol FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    // Pre-fill sink's input at cap. Because sink has no downstream, sink
    // will still run this cycle — so to keep trades deferred across a
    // second cycle we keep the cap threshold tight and re-fill sink each
    // cycle, simulating a continuous slow-consumer scenario.
    let sink_id = *graph.output_map.get("sink").unwrap();
    let source_id = *graph.source_map.get("trades").unwrap();
    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);

    // Cycle 1: sink's input pre-filled to cap, trades deferred, trades
    // input extended by 1.
    prefill_port(&mut graph, sink_id, 0, vec![test_batch(); 2]);
    let _ = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(
        graph.input_bufs[source_id][0].len(),
        1,
        "deferred source must accumulate its input buffer"
    );

    // Cycle 2: re-fill sink to cap so trades stays deferred; trades input
    // must grow from 1 to 2 (extend, not clone_from).
    prefill_port(&mut graph, sink_id, 0, vec![test_batch(); 2]);
    let _ = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    assert_eq!(
        graph.input_bufs[source_id][0].len(),
        2,
        "source input must accumulate across deferred cycles"
    );
}

/// Regression test: LEFT JOIN between a streaming source and a
/// `ReferenceTableProvider` (lookup table) must work across multiple
/// cycles without panicking. Before the fix, `RepartitionExec` in the
/// cached physical plan had consumed internal channels on the first
/// cycle, causing `"partition not used yet"` on the second.
#[tokio::test]
async fn test_lookup_left_join_multi_cycle() {
    use crate::table_store::TableStore;

    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);

    // Register a lookup table via ReferenceTableProvider
    let lookup_schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("company_name", DataType::Utf8, true),
    ]));
    let ts = Arc::new(parking_lot::RwLock::new(TableStore::new()));
    {
        let mut store = ts.write();
        store
            .create_table("instruments", lookup_schema.clone(), "symbol")
            .unwrap();
        let batch = RecordBatch::try_new(
            lookup_schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(StringArray::from(vec!["Apple Inc.", "Alphabet"])),
            ],
        )
        .unwrap();
        store.upsert("instruments", &batch).unwrap();
    }
    let provider = crate::table_provider::ReferenceTableProvider::new(
        "instruments".to_string(),
        lookup_schema,
        ts,
    );
    ctx.register_table("instruments", Arc::new(provider))
        .unwrap();

    let mut graph = OperatorGraph::new(ctx);
    graph.register_source_schema("trades".to_string(), test_schema());
    graph.set_reference_tables(["instruments".to_string()].into_iter().collect());

    graph.add_query(
        "enriched".to_string(),
        "SELECT t.symbol, t.price, i.company_name \
         FROM trades t LEFT JOIN instruments i ON t.symbol = i.symbol"
            .to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    let batch = test_batch(); // AAPL + GOOG
    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![batch.clone()]);

    // Cycle 1
    let r1 = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    let rows1: usize = r1
        .get("enriched")
        .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum());
    assert_eq!(rows1, 2, "cycle 1 should produce 2 joined rows");

    // Cycle 2 — this panicked before the fix
    source.insert(Arc::from("trades"), vec![batch]);
    let r2 = graph.execute_cycle(&source, i64::MAX, None).await.unwrap();
    let rows2: usize = r2
        .get("enriched")
        .map_or(0, |bs| bs.iter().map(|b| b.num_rows()).sum());
    assert_eq!(rows2, 2, "cycle 2 should also produce 2 joined rows");
}

#[tokio::test]
async fn test_self_join_prefilter_end_to_end() {
    use arrow::array::TimestampMillisecondArray;
    use arrow::datatypes::TimeUnit;

    let ctx = laminar_sql::create_session_context();
    laminar_sql::register_streaming_functions(&ctx);
    let mut graph = OperatorGraph::new(ctx);

    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("type", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    graph.register_source_schema("events".to_string(), Arc::clone(&schema));

    graph.add_query(
        "joined".to_string(),
        "SELECT p.key, p.type, a.type \
         FROM events p \
         JOIN events a ON p.key = a.key \
         AND a.ts BETWEEN p.ts AND p.ts + INTERVAL '10' SECOND \
         WHERE p.type = 'A' AND a.type = 'B'"
            .to_string(),
        None,
        None,
        None,
        None,
        false,
    );

    // source + 2 filter nodes + join operator = 4
    assert!(
        graph.nodes.len() >= 4,
        "expected 4+ nodes, got {}",
        graph.nodes.len()
    );

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(vec!["k1", "k1", "k1", "k1"])),
            Arc::new(StringArray::from(vec!["A", "B", "A", "B"])),
            Arc::new(TimestampMillisecondArray::from(vec![
                1000, 2000, 3000, 4000,
            ])),
        ],
    )
    .unwrap();

    let mut source = FxHashMap::default();
    source.insert(Arc::from("events"), vec![batch.clone()]);

    // First cycle seeds the join buffers; second cycle produces matches
    // when buffered left (type=A) rows see right (type=B) rows. Keep the
    // watermark below the rows so the first cycle does not close their interval.
    let _ = graph.execute_cycle(&source, 0, None).await.unwrap();

    source.clear();
    source.insert(Arc::from("events"), vec![batch]);
    let results = graph.execute_cycle(&source, 0, None).await.unwrap();

    let total_rows: usize = results
        .get("joined")
        .map_or(0, |batches| batches.iter().map(|b| b.num_rows()).sum());

    assert!(
        total_rows > 0,
        "should produce matches from prefiltered self-join"
    );
}

fn prefill_port(graph: &mut OperatorGraph, node: usize, port: usize, batches: Vec<RecordBatch>) {
    let bytes: usize = batches.iter().map(RecordBatch::get_array_memory_size).sum();
    graph.input_bufs[node][port] = batches;
    graph.input_buf_bytes[node][port] = bytes;
}

fn producer_consumer_graph(policy: BackpressurePolicy, cap: usize) -> (OperatorGraph, usize) {
    let mut graph = test_graph();
    graph.set_max_input_buf_batches(cap);
    graph.set_backpressure_policy(policy);
    graph.add_query(
        "producer".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "consumer".to_string(),
        "SELECT symbol FROM producer".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    let consumer_id = *graph.output_map.get("consumer").unwrap();
    prefill_port(&mut graph, consumer_id, 0, vec![test_batch(); cap]);
    (graph, consumer_id)
}

fn trades_source() -> FxHashMap<Arc<str>, Vec<RecordBatch>> {
    let mut s = FxHashMap::default();
    s.insert(Arc::from("trades"), vec![test_batch()]);
    s
}

#[tokio::test]
async fn test_backpressure_policy_defers_without_shedding() {
    let (mut graph, consumer_id) = producer_consumer_graph(BackpressurePolicy::Backpressure, 2);
    let _ = graph
        .execute_cycle(&trades_source(), i64::MAX, None)
        .await
        .unwrap();
    assert_eq!(
        graph.input_bufs[consumer_id][0].len(),
        2,
        "consumer input stays at cap — producer must have been deferred"
    );
}

#[tokio::test]
async fn test_shed_oldest_policy_drops_rows_and_increments_counter() {
    let registry = prometheus::Registry::new();
    let prom = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
    let (mut graph, consumer_id) = producer_consumer_graph(BackpressurePolicy::ShedOldest, 2);
    graph.set_metrics(Arc::clone(&prom));

    let _ = graph
        .execute_cycle(&trades_source(), i64::MAX, None)
        .await
        .unwrap();

    assert!(graph.input_bufs[consumer_id][0].len() <= 2);
    assert!(
        prom.shed_records_total
            .with_label_values(&["consumer"])
            .get()
            > 0,
        "shed_records_total should have incremented"
    );
}

#[tokio::test]
async fn test_fail_policy_returns_error_at_cap() {
    let (mut graph, _) = producer_consumer_graph(BackpressurePolicy::Fail, 2);
    let err = graph
        .execute_cycle(&trades_source(), i64::MAX, None)
        .await
        .expect_err("Fail policy must return an error at capacity");
    assert!(
        matches!(err, DbError::BackpressureFail(_)),
        "expected DbError::BackpressureFail, got {err:?}"
    );
}

#[tokio::test]
async fn test_byte_budget_gates_capacity() {
    let mut graph = test_graph();
    graph.set_max_input_buf_bytes(Some(1));
    graph.add_query(
        "producer".to_string(),
        "SELECT symbol, price FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.add_query(
        "consumer".to_string(),
        "SELECT symbol FROM producer".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    let consumer_id = *graph.output_map.get("consumer").unwrap();
    prefill_port(&mut graph, consumer_id, 0, vec![test_batch()]);

    let producer_id = *graph.output_map.get("producer").unwrap();
    assert!(graph.is_downstream_at_capacity(producer_id));
}

#[test]
fn managed_state_accounting_is_sampled_at_cold_cadence_and_skips_removed_nodes() {
    let registry = prometheus::Registry::new();
    let prom = Arc::new(crate::engine_metrics::EngineMetrics::new(&registry));
    let mut graph = test_graph();
    graph.set_metrics(Arc::clone(&prom));

    let active_samples = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let active = graph.allocate_node(GraphNode::new(
        Arc::from("accounted"),
        Box::new(ManagedStateAccountingProbe {
            accounting: ManagedStateAccountingSnapshot {
                live: 11,
                prepared: 22,
                retired: 33,
            },
            samples: Arc::clone(&active_samples),
        }),
        0,
    ));

    let removed_samples = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let removed = graph.allocate_node(GraphNode::new(
        Arc::from("removed"),
        Box::new(ManagedStateAccountingProbe {
            accounting: ManagedStateAccountingSnapshot {
                live: 44,
                prepared: 55,
                retired: 66,
            },
            samples: Arc::clone(&removed_samples),
        }),
        0,
    ));
    graph.managed_state_accounting_peaks[active].observe_transient(
        ManagedStateAccountingSnapshot {
            live: 0,
            prepared: 99,
            retired: 88,
        },
    );

    for _ in 1..STATS_SAMPLE_INTERVAL {
        graph.sample_buffer_stats();
    }
    assert_eq!(active_samples.load(std::sync::atomic::Ordering::SeqCst), 0);

    graph.sample_buffer_stats();

    assert_eq!(active_samples.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(removed_samples.load(std::sync::atomic::Ordering::SeqCst), 1);
    for (phase, expected) in [("live", 11), ("prepared", 99), ("retired", 88)] {
        assert_eq!(
            prom.managed_state_accounted_bytes
                .with_label_values(&["accounted", phase])
                .get(),
            expected
        );
    }
    assert_eq!(
        prom.managed_state_accounted_bytes
            .with_label_values(&["removed", "live"])
            .get(),
        44
    );

    graph.nodes[removed].removed = true;
    for _ in 0..STATS_SAMPLE_INTERVAL {
        graph.sample_buffer_stats();
    }
    assert_eq!(active_samples.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(removed_samples.load(std::sync::atomic::Ordering::SeqCst), 1);
    for phase in ["live", "prepared", "retired"] {
        assert!(
            prom.managed_state_accounted_bytes
                .remove_label_values(&["removed", phase])
                .is_err(),
            "removed operator retained its {phase} metric series"
        );
    }
    for (phase, expected) in [("live", 11), ("prepared", 22), ("retired", 33)] {
        assert_eq!(
            prom.managed_state_accounted_bytes
                .with_label_values(&["accounted", phase])
                .get(),
            expected,
            "transient peak must reset after one sample interval"
        );
    }
}

#[tokio::test]
async fn managed_state_initialization_rejects_a_budget_below_the_empty_topology() {
    let mut graph = test_graph();
    graph.register_source_schema("trades".to_string(), test_schema());
    graph.add_query(
        "agg".to_string(),
        "SELECT SUM(price) AS total FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    graph.set_max_managed_state_bytes(1);

    let error = graph
        .initialize_managed_state()
        .await
        .err()
        .expect("the empty managed topology must still fit its execution budget");

    assert!(matches!(
        &error,
        DbError::ManagedStateBudgetExceeded {
            context,
            accounted_bytes,
            limit_bytes: 1,
        } if context == "managed-state initialization" && *accounted_bytes > 1
    ));
}

#[tokio::test]
async fn aggregate_record_growth_is_rejected_before_output_routing() {
    let mut graph = test_graph();
    graph.register_source_schema("trades".to_string(), test_schema());
    graph.add_query(
        "agg".to_string(),
        "SELECT SUM(price) AS total FROM trades".to_string(),
        None,
        None,
        None,
        None,
        false,
    );
    let aggregate = graph.output_map["agg"];
    let downstream = graph
        .place_operator_node("after_budget", Box::new(SourcePassthrough), 1)
        .unwrap();
    graph.add_edge(aggregate, downstream, 0);
    graph.topo_dirty = true;
    let mut graph = graph
        .initialize_managed_state()
        .await
        .expect("aggregate must initialize within the unconstrained test budget");
    let baseline = graph.managed_state_accounted_bytes();
    assert!(baseline > 0);
    graph.set_max_managed_state_bytes(baseline);

    let mut source = FxHashMap::default();
    source.insert(Arc::from("trades"), vec![test_batch()]);
    let error = graph
        .execute_cycle(&source, i64::MAX, None)
        .await
        .expect_err("record growth above the baseline must halt the pipeline");

    assert!(matches!(
        &error,
        DbError::ManagedStateBudgetExceeded {
            context,
            accounted_bytes,
            limit_bytes,
        } if context == "operator 'agg' record processing"
            && *accounted_bytes > baseline
            && *limit_bytes == baseline
    ));
    assert!(error.requires_pipeline_halt());
    assert!(
        graph.input_bufs[downstream][0].is_empty(),
        "the rejected aggregate output crossed the downstream routing boundary"
    );
}

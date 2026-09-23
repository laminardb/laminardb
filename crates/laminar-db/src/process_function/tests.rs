use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use rustc_hash::FxHashMap;

use super::{
    NativeProcessFunction, ProcessActivation, ProcessActivationResult, ProcessCallback,
    ProcessFunctionDescriptor, ProcessFunctionLimits, ProcessFunctionRegistration, TimerOperation,
    ValueMutation, ValueState,
};
use crate::error::DbError;
use crate::operator_graph::{GraphOperator, GraphStateCapture, InputFrontier, OperatorGraph};
use crate::subscription::{PortalFrame, SubscribeStart};
use crate::LaminarDB;

fn input_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("account", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("account", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),
        Field::new("total", DataType::Int64, false),
        Field::new("crossed", DataType::Boolean, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn descriptor() -> ProcessFunctionDescriptor {
    ProcessFunctionDescriptor {
        version: 1,
        function_id: "account_activity".into(),
        pipeline_state_id: "test_pipeline_v1".into(),
        implementation_digest: "a".repeat(64),
        input_schema: input_schema(),
        output_schema: output_schema(),
        key_columns: vec!["account".into()],
        event_time_column: "ts".into(),
        output_event_time_column: "ts".into(),
        value_state_name: "running_total".into(),
        timer_names: vec!["inactive".into()],
        limits: ProcessFunctionLimits::default(),
    }
}

fn input_batch(rows: &[(&str, i64, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn output_row(account: &str, kind: &str, total: i64, crossed: bool, ts: i64) -> RecordBatch {
    RecordBatch::try_new(
        output_schema(),
        vec![
            Arc::new(StringArray::from(vec![account])),
            Arc::new(StringArray::from(vec![kind])),
            Arc::new(Int64Array::from(vec![total])),
            Arc::new(BooleanArray::from(vec![crossed])),
            Arc::new(TimestampMicrosecondArray::from(vec![ts])),
        ],
    )
    .unwrap()
}

struct AccountActivity;

impl NativeProcessFunction for AccountActivity {
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        assert!(activations.iter().enumerate().all(|(i, activation)| {
            activations
                .iter()
                .skip(i + 1)
                .all(|other| activation.key != other.key)
        }));
        activations
            .iter()
            .map(|activation| {
                let prior = match activation.state {
                    ValueState::Absent | ValueState::Null => 0,
                    ValueState::Value(value) => value,
                };
                let (account, kind, total, crossed, value, timers) = match &activation.callback {
                    ProcessCallback::Input(batch) => {
                        let account = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(0);
                        let amount = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0);
                        let total = prior.checked_add(amount).ok_or_else(|| {
                            DbError::InvalidOperation("account total overflow".into())
                        })?;
                        (
                            account.to_string(),
                            "running",
                            total,
                            prior < 100 && total >= 100,
                            ValueMutation::Set(total),
                            vec![TimerOperation::Set {
                                name: "inactive".into(),
                                at_us: activation.event_time_us + 10_000,
                            }],
                        )
                    }
                    ProcessCallback::Timer { name } => {
                        assert_eq!(name, "inactive");
                        (
                            activation.key_text.clone(),
                            "inactive",
                            prior,
                            false,
                            ValueMutation::Unchanged,
                            Vec::new(),
                        )
                    }
                };
                Ok(ProcessActivationResult {
                    activation_id: activation.id,
                    output: vec![output_row(
                        &account,
                        kind,
                        total,
                        crossed,
                        activation.event_time_us,
                    )],
                    value,
                    timers,
                })
            })
            .collect()
    }
}

fn build_graph(descriptor: ProcessFunctionDescriptor) -> OperatorGraph {
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.set_query_budget_ns(5_000_000_000);
    graph.register_source_schema("events".into(), input_schema());
    graph
        .add_process_function(&ProcessFunctionRegistration {
            output_name: "activity".into(),
            source_name: "events".into(),
            descriptor,
            handler: Arc::new(AccountActivity),
        })
        .unwrap();
    graph
}

fn source(rows: &[(&str, i64, i64)]) -> FxHashMap<Arc<str>, Vec<RecordBatch>> {
    let mut source = FxHashMap::default();
    source.insert(Arc::from("events"), vec![input_batch(rows)]);
    source
}

fn materialize(
    capture: GraphStateCapture,
) -> (
    Vec<(String, bytes::Bytes)>,
    Vec<(String, u32, bytes::Bytes)>,
) {
    let whole = capture
        .whole
        .into_iter()
        .map(|frame| {
            (
                frame.operator_id,
                frame.state.materialize(&mut 0, u64::MAX).unwrap(),
            )
        })
        .collect();
    let vnodes = capture
        .vnodes
        .into_iter()
        .map(|(name, frame)| {
            (
                name,
                frame.vnode,
                frame.state.unwrap().materialize(&mut 0, u64::MAX).unwrap(),
            )
        })
        .collect();
    (whole, vnodes)
}

fn totals(output: &[RecordBatch]) -> Vec<i64> {
    output
        .iter()
        .flat_map(|batch| {
            let values = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..values.len())
                .map(|row| values.value(row))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn keyed_state_and_timer_survive_graph_checkpoint_restore() {
    let mut graph = build_graph(descriptor())
        .initialize_managed_state()
        .await
        .unwrap();
    let first = graph
        .execute_cycle(
            &source(&[("a", 60, 100_000), ("b", 5, 101_000), ("a", 50, 102_000)]),
            105,
            None,
        )
        .await
        .unwrap();
    assert_eq!(totals(&first["activity"]), vec![60, 5, 110]);
    let capture = graph.capture_state(u64::MAX).unwrap();
    let (whole, vnodes) = materialize(capture);
    let restored = build_graph(descriptor())
        .initialize_managed_state()
        .await
        .unwrap()
        .restore_state_frames(&whole, &vnodes, 256)
        .unwrap()
        .0;
    let mut restored = restored;
    let timers = restored
        .execute_cycle(&FxHashMap::default(), 112, None)
        .await
        .unwrap();
    assert_eq!(totals(&timers["activity"]), vec![5, 110]);
    let continued = restored
        .execute_cycle(&source(&[("a", 1, 120_000)]), 120, None)
        .await
        .unwrap();
    assert_eq!(totals(&continued["activity"]), vec![111]);
}

#[tokio::test]
async fn due_timers_remain_scheduled_after_callback_limit() {
    let mut binding = descriptor();
    binding.limits.max_timer_callbacks_per_step = 1;
    let mut graph = build_graph(binding)
        .initialize_managed_state()
        .await
        .unwrap();
    graph
        .execute_cycle(&source(&[("a", 1, 100_000), ("b", 2, 101_000)]), 105, None)
        .await
        .unwrap();
    let first = graph
        .execute_cycle(&FxHashMap::default(), 112, None)
        .await
        .unwrap();
    assert_eq!(totals(&first["activity"]), vec![1]);
    assert!(graph.has_deferred_work());
    assert!(graph.has_runnable_deferred_work());
    let second = graph
        .execute_cycle(&FxHashMap::default(), 112, None)
        .await
        .unwrap();
    assert_eq!(totals(&second["activity"]), vec![2]);
    assert!(!graph.has_deferred_work());
}

#[tokio::test]
async fn restore_rejects_changed_implementation_binding() {
    let mut original = build_graph(descriptor())
        .initialize_managed_state()
        .await
        .unwrap();
    original
        .execute_cycle(&source(&[("a", 1, 100_000)]), 100, None)
        .await
        .unwrap();
    let (whole, vnodes) = materialize(original.capture_state(u64::MAX).unwrap());
    let mut changed = descriptor();
    changed.implementation_digest = "b".repeat(64);
    let result = build_graph(changed)
        .initialize_managed_state()
        .await
        .unwrap()
        .restore_state_frames(&whole, &vnodes, 256);
    assert!(result.is_err());
}

#[tokio::test]
async fn restore_rejects_state_over_declared_key_budget() {
    let mut original = build_graph(descriptor())
        .initialize_managed_state()
        .await
        .unwrap();
    original
        .execute_cycle(&source(&[("a", 1, 100_000), ("b", 1, 100_000)]), 100, None)
        .await
        .unwrap();
    let (whole, vnodes) = materialize(original.capture_state(u64::MAX).unwrap());
    let mut smaller = descriptor();
    smaller.limits.max_keys = 1;
    let result = build_graph(smaller)
        .initialize_managed_state()
        .await
        .unwrap()
        .restore_state_frames(&whole, &vnodes, 256);
    assert!(result.is_err());
}

struct InvalidSecondResponse;

impl NativeProcessFunction for InvalidSecondResponse {
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        Ok(activations
            .iter()
            .enumerate()
            .map(|(index, activation)| ProcessActivationResult {
                activation_id: activation.id,
                output: Vec::new(),
                value: ValueMutation::Set(99),
                timers: (index == 1)
                    .then(|| TimerOperation::Set {
                        name: "undeclared".into(),
                        at_us: activation.event_time_us + 10_000,
                    })
                    .into_iter()
                    .collect(),
            })
            .collect())
    }
}

#[tokio::test]
async fn invalid_batch_response_applies_no_state_or_timer() {
    let mut operator =
        super::ProcessFunctionOperator::new(descriptor(), Arc::new(InvalidSecondResponse), 256)
            .unwrap();
    operator.initialize_managed_state().await.unwrap();
    let result = operator
        .process_with_frontiers(
            &[vec![input_batch(&[("a", 1, 100_000), ("b", 1, 100_000)])]],
            &[InputFrontier {
                watermark: Some(100),
                idle: false,
            }],
        )
        .await;
    assert!(result.is_err());
    assert_eq!(operator.managed_state_accounting().unwrap().live, 0);
    let frame = operator.checkpoint().unwrap().unwrap();
    let checkpoint: serde_json::Value = serde_json::from_slice(&frame.data).unwrap();
    assert_eq!(checkpoint["next_activation_id"], 0);
}

#[tokio::test]
async fn native_function_emits_through_running_database() {
    let db = LaminarDB::open().unwrap();
    db.execute(
        "CREATE SOURCE events (account VARCHAR NOT NULL, amount BIGINT NOT NULL, \
         ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
    )
    .await
    .unwrap();
    db.register_native_process_function(
        "activity",
        "events",
        descriptor(),
        Arc::new(AccountActivity),
    )
    .await
    .unwrap();
    db.execute(
        "CREATE STREAM high_activity AS SELECT account, total FROM activity WHERE total >= 50",
    )
    .await
    .unwrap();
    assert_eq!(db.native_process_functions().len(), 1);
    db.start().await.unwrap();
    let mut portal = db
        .open_subscription("activity", None, SubscribeStart::Tail)
        .await
        .unwrap();
    let mut downstream = db
        .open_subscription("high_activity", None, SubscribeStart::Tail)
        .await
        .unwrap();
    db.source_untyped("events")
        .unwrap()
        .push_arrow(input_batch(&[("a", 60, 100_000), ("b", 5, 101_000)]))
        .unwrap();
    let rows = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let mut values = Vec::new();
        while values.len() < 2 {
            match portal.next_frame().await {
                Some(PortalFrame::Batch { batch, .. }) => values.extend(totals(&[batch])),
                Some(PortalFrame::Barrier { .. }) => {}
                Some(PortalFrame::Error { .. }) | Some(PortalFrame::Lagged(_)) | None => break,
            }
        }
        values
    })
    .await
    .unwrap();
    assert_eq!(rows, vec![60, 5]);
    let forwarded = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            match downstream.next_frame().await {
                Some(PortalFrame::Batch { batch, .. }) => break batch,
                Some(PortalFrame::Barrier { .. }) => {}
                other => panic!("downstream process output unavailable: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    let values = forwarded
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(forwarded.num_rows(), 1);
    assert_eq!(values.value(0), 60);
    db.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_function_restores_from_database_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let build = || async {
        let db = LaminarDB::builder()
            .storage_dir(directory.path())
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig::default())
            .build()
            .await
            .unwrap();
        db.execute(
            "CREATE SOURCE events (account VARCHAR NOT NULL, amount BIGINT NOT NULL, \
             ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        db.register_native_process_function(
            "activity",
            "events",
            descriptor(),
            Arc::new(AccountActivity),
        )
        .await
        .unwrap();
        db.start().await.unwrap();
        db
    };
    let first = build().await;
    let mut first_portal = first
        .open_subscription("activity", None, SubscribeStart::Tail)
        .await
        .unwrap();
    first
        .source_untyped("events")
        .unwrap()
        .push_arrow(input_batch(&[("a", 60, 100_000)]))
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            match first_portal.next_frame().await {
                Some(PortalFrame::Batch { .. }) => break,
                Some(PortalFrame::Barrier { .. }) => {}
                other => panic!("first process output unavailable: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    first.checkpoint().await.unwrap();
    first.shutdown().await.unwrap();
    drop(first_portal);
    drop(first);

    let restored = build().await;
    let mut portal = restored
        .open_subscription("activity", None, SubscribeStart::Tail)
        .await
        .unwrap();
    restored
        .source_untyped("events")
        .unwrap()
        .push_arrow(input_batch(&[("a", 50, 102_000)]))
        .unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            match portal.next_frame().await {
                Some(PortalFrame::Batch { batch, .. }) => break batch,
                Some(PortalFrame::Barrier { .. }) => {}
                other => panic!("restored process output unavailable: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(totals(&[result]), vec![110]);
    restored
        .source_untyped("events")
        .unwrap()
        .push_arrow(input_batch(&[("b", 1, 115_000)]))
        .unwrap();
    let mut seen = Vec::new();
    let timer = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            match portal.next_frame().await {
                Some(PortalFrame::Batch { batch, .. }) => {
                    let kinds = batch
                        .column(1)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    seen.extend((0..kinds.len()).map(|row| kinds.value(row).to_string()));
                    if (0..kinds.len()).any(|row| kinds.value(row) == "inactive") {
                        break batch;
                    }
                }
                Some(PortalFrame::Barrier { .. }) => {}
                other => panic!("restored timer output unavailable: {other:?}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "timer output missing; saw {seen:?}; fault: {:?}",
            restored.last_fault()
        )
    });
    assert_eq!(totals(&[timer]), vec![110]);
    restored.shutdown().await.unwrap();
}

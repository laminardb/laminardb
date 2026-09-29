use std::sync::Arc;

use arrow::array::{
    Array, BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};
use laminar_connectors::connector::DeliveryGuarantee;
use rustc_hash::FxHashMap;

use super::{
    NativeProcessFunction, ProcessActivation, ProcessActivationResult, ProcessCallback,
    ProcessFunctionDescriptor, ProcessFunctionLimits, ProcessFunctionOperator,
    ProcessFunctionRegistration, ProcessHandler, ProcessRuntime, TimerOperation, ValueMutation,
    ValueState,
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
        runtime: ProcessRuntime::NativeRust,
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

#[test]
fn manifest_round_trips_supported_arrow_types_and_binds_semantics() {
    let mut descriptor = descriptor();
    descriptor.output_schema = Arc::new(Schema::new(vec![
        Field::new("boolean", DataType::Boolean, true),
        Field::new("int8", DataType::Int8, true),
        Field::new("int16", DataType::Int16, true),
        Field::new("int32", DataType::Int32, true),
        Field::new("int64", DataType::Int64, true),
        Field::new("float32", DataType::Float32, true),
        Field::new("float64", DataType::Float64, true),
        Field::new("utf8", DataType::Utf8, true),
        Field::new("binary", DataType::Binary, true),
        Field::new("decimal", DataType::Decimal128(20, 4), true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    let bytes = descriptor.to_manifest_json().unwrap();
    let restored = ProcessFunctionDescriptor::from_manifest_json(&bytes).unwrap();
    assert_eq!(restored.input_schema, descriptor.input_schema);
    assert_eq!(restored.output_schema, descriptor.output_schema);
    assert_eq!(restored.to_manifest_json().unwrap(), bytes);
    assert_eq!(
        restored.binding_sha256().unwrap(),
        descriptor.binding_sha256().unwrap()
    );

    let mut changed = restored.clone();
    changed.timer_names.push("another_timer".into());
    assert_ne!(
        changed.binding_sha256().unwrap(),
        descriptor.binding_sha256().unwrap()
    );
    changed = restored;
    changed.limits.max_output_rows -= 1;
    assert_ne!(
        changed.binding_sha256().unwrap(),
        descriptor.binding_sha256().unwrap()
    );
    let mut remote = descriptor.clone();
    remote.runtime = ProcessRuntime::RemoteRust;
    let remote_bytes = remote.to_manifest_json().unwrap();
    assert_eq!(
        ProcessFunctionDescriptor::from_manifest_json(&remote_bytes)
            .unwrap()
            .runtime,
        ProcessRuntime::RemoteRust
    );
    assert_ne!(
        remote.binding_sha256().unwrap(),
        descriptor.binding_sha256().unwrap()
    );
}

#[test]
fn manifest_rejects_unknown_or_incompatible_contracts() {
    let bytes = descriptor().to_manifest_json().unwrap();
    let original: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let mut cases = Vec::new();
    for (field, replacement) in [
        ("protocol_version", serde_json::json!(2)),
        ("runtime", serde_json::json!("unsupported_runtime")),
        ("partitioning_abi", serde_json::json!(999)),
        ("state_codec_version", serde_json::json!(999)),
        ("late_event_policy", serde_json::json!("accept")),
    ] {
        let mut value = original.clone();
        value[field] = replacement;
        cases.push(value);
    }
    for value in cases {
        assert!(ProcessFunctionDescriptor::from_manifest_json(
            &serde_json::to_vec(&value).unwrap()
        )
        .is_err());
    }
    let mut unknown = original.clone();
    unknown["unrecognised"] = serde_json::json!(true);
    assert!(
        ProcessFunctionDescriptor::from_manifest_json(&serde_json::to_vec(&unknown).unwrap())
            .is_err()
    );
    let mut unsupported_type = original;
    unsupported_type["output_schema"][0]["data_type"] = serde_json::json!({"type":"list"});
    assert!(ProcessFunctionDescriptor::from_manifest_json(
        &serde_json::to_vec(&unsupported_type).unwrap()
    )
    .is_err());
    let mut duplicate_field = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap();
    duplicate_field["output_schema"][0]["name"] = serde_json::json!("kind");
    assert!(ProcessFunctionDescriptor::from_manifest_json(
        &serde_json::to_vec(&duplicate_field).unwrap()
    )
    .is_err());
    let mut invalid_decimal = serde_json::from_slice::<serde_json::Value>(&bytes).unwrap();
    invalid_decimal["output_schema"][0]["data_type"] =
        serde_json::json!({"type":"decimal128", "precision":40, "scale":2});
    assert!(ProcessFunctionDescriptor::from_manifest_json(
        &serde_json::to_vec(&invalid_decimal).unwrap()
    )
    .is_err());
    assert!(ProcessFunctionDescriptor::from_manifest_json(&vec![b' '; 64 * 1024 + 1]).is_err());
    let mut oversized = descriptor();
    oversized.output_schema = Arc::new(Schema::new(vec![
        Field::new("x".repeat(64 * 1024), DataType::Int64, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    assert!(oversized.to_manifest_json().is_err());
}

#[test]
fn checkpoint_rejects_changed_descriptor_contract() {
    let mut first =
        ProcessFunctionOperator::new(descriptor(), Arc::new(AccountActivity), 4).unwrap();
    let checkpoint = first.checkpoint().unwrap().unwrap();
    let mut changed = descriptor();
    changed.timer_names.push("second_timer".into());
    let mut replacement =
        ProcessFunctionOperator::new(changed, Arc::new(AccountActivity), 4).unwrap();
    assert!(replacement.restore(checkpoint).is_err());
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

#[tokio::test]
async fn stronger_delivery_rejects_direct_process_source_and_exactly_once() {
    for (delivery, expected) in [
        (
            DeliveryGuarantee::AtLeastOnce,
            "replayable connector source",
        ),
        (
            DeliveryGuarantee::ExactlyOnce,
            "do not support exactly-once",
        ),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let db = LaminarDB::builder()
            .storage_dir(directory.path())
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig::default())
            .delivery_guarantee(delivery)
            .build()
            .await
            .unwrap();
        db.execute(
            "CREATE SOURCE events (account VARCHAR NOT NULL, amount BIGINT NOT NULL, \
             ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        let error = db
            .register_native_process_function(
                "activity",
                "events",
                descriptor(),
                Arc::new(AccountActivity),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        assert!(db.process_functions().is_empty());
        db.shutdown().await.unwrap();
    }
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

fn build_graph(
    descriptor: ProcessFunctionDescriptor,
    handler: Arc<dyn NativeProcessFunction>,
) -> OperatorGraph {
    let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
    graph.set_query_budget_ns(5_000_000_000);
    graph.register_source_schema("events".into(), input_schema());
    graph
        .add_process_function(&ProcessFunctionRegistration {
            output_name: "activity".into(),
            source_name: "events".into(),
            descriptor,
            handler: ProcessHandler::Native(handler),
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

fn activity_rows(output: &[RecordBatch]) -> Vec<(String, String, i64, bool, i64)> {
    let mut rows = Vec::new();
    for batch in output {
        let account = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let kind = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let total = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let crossed = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let time = batch
            .column(4)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push((
                account.value(row).to_string(),
                kind.value(row).to_string(),
                total.value(row),
                crossed.value(row),
                time.value(row),
            ));
        }
    }
    rows
}

#[tokio::test]
async fn batch_splits_and_independent_key_order_preserve_results() {
    let ordered = [
        ("a", 60, 100_000),
        ("b", 5, 100_000),
        ("c", 9, 100_000),
        ("a", 50, 101_000),
        ("b", 10, 101_000),
        ("c", 1, 101_000),
        ("a", 1, 102_000),
        ("b", 20, 102_000),
        ("c", 3, 102_000),
    ];
    let permuted = [
        ordered[2], ordered[0], ordered[1], ordered[5], ordered[3], ordered[4], ordered[8],
        ordered[6], ordered[7],
    ];
    let cases = [
        (vec![input_batch(&ordered)], 256),
        (
            vec![input_batch(&ordered[..4]), input_batch(&ordered[4..])],
            2,
        ),
        (permuted.iter().map(|row| input_batch(&[*row])).collect(), 1),
    ];
    let mut expected = vec![
        ("a".into(), "running".into(), 60, false, 100_000),
        ("a".into(), "running".into(), 110, true, 101_000),
        ("a".into(), "running".into(), 111, false, 102_000),
        ("a".into(), "inactive".into(), 111, false, 112_000),
        ("b".into(), "running".into(), 5, false, 100_000),
        ("b".into(), "running".into(), 15, false, 101_000),
        ("b".into(), "running".into(), 35, false, 102_000),
        ("b".into(), "inactive".into(), 35, false, 112_000),
        ("c".into(), "running".into(), 9, false, 100_000),
        ("c".into(), "running".into(), 10, false, 101_000),
        ("c".into(), "running".into(), 13, false, 102_000),
        ("c".into(), "inactive".into(), 13, false, 112_000),
    ];
    expected.sort_unstable();
    for (batches, max_batch_rows) in cases {
        let mut binding = descriptor();
        binding.limits.max_batch_rows = max_batch_rows;
        let mut graph = build_graph(binding, Arc::new(AccountActivity))
            .initialize_managed_state()
            .await
            .unwrap();
        let mut inputs = FxHashMap::default();
        inputs.insert(Arc::from("events"), batches);
        let first = graph.execute_cycle(&inputs, 95, None).await.unwrap();
        let timers = graph
            .execute_cycle(&FxHashMap::default(), 120, None)
            .await
            .unwrap();
        let mut actual = activity_rows(&first["activity"]);
        actual.extend(activity_rows(&timers["activity"]));
        actual.sort_unstable();
        assert_eq!(actual, expected, "max_batch_rows={max_batch_rows}");
    }
}

struct StateEcho;

impl NativeProcessFunction for StateEcho {
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        activations
            .iter()
            .map(|activation| {
                let ProcessCallback::Input(batch) = &activation.callback else {
                    return Err(DbError::InvalidOperation("unexpected timer".into()));
                };
                let command = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0);
                let mutation = match command {
                    1 => ValueMutation::SetNull,
                    2 => ValueMutation::Unchanged,
                    3 => ValueMutation::Set(42),
                    4 => ValueMutation::Clear,
                    _ => return Err(DbError::InvalidOperation("unknown test command".into())),
                };
                let (kind, value) = match activation.state {
                    ValueState::Absent => ("absent", 0),
                    ValueState::Null => ("null", 0),
                    ValueState::Value(value) => ("value", value),
                };
                Ok(ProcessActivationResult {
                    activation_id: activation.id,
                    output: vec![output_row(
                        &activation.key_text,
                        kind,
                        value,
                        false,
                        activation.event_time_us,
                    )],
                    value: mutation,
                    timers: Vec::new(),
                })
            })
            .collect()
    }
}

#[tokio::test]
async fn absent_null_unchanged_and_clear_survive_restore_without_key_leakage() {
    let mut binding = descriptor();
    binding.timer_names.clear();
    let mut graph = build_graph(binding.clone(), Arc::new(StateEcho))
        .initialize_managed_state()
        .await
        .unwrap();
    let first = graph
        .execute_cycle(&source(&[("a", 1, 100_000), ("b", 3, 100_000)]), 100, None)
        .await
        .unwrap();
    assert_eq!(
        activity_rows(&first["activity"])
            .into_iter()
            .map(|row| (row.0, row.1))
            .collect::<Vec<_>>(),
        vec![("a".into(), "absent".into()), ("b".into(), "absent".into())]
    );
    let (whole, vnodes) = materialize(graph.capture_state(u64::MAX).unwrap());
    let mut restored = build_graph(binding, Arc::new(StateEcho))
        .initialize_managed_state()
        .await
        .unwrap()
        .restore_state_frames(&whole, &vnodes, 256)
        .unwrap()
        .0;
    let after = restored
        .execute_cycle(
            &source(&[
                ("a", 2, 101_000),
                ("b", 2, 101_000),
                ("a", 4, 102_000),
                ("b", 4, 102_000),
                ("a", 2, 103_000),
                ("b", 2, 103_000),
            ]),
            103,
            None,
        )
        .await
        .unwrap();
    let actual = activity_rows(&after["activity"])
        .into_iter()
        .map(|(key, state, value, _, _)| (key, state, value))
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![
            ("a".into(), "null".into(), 0),
            ("b".into(), "value".into(), 42),
            ("a".into(), "null".into(), 0),
            ("b".into(), "value".into(), 42),
            ("a".into(), "absent".into(), 0),
            ("b".into(), "absent".into(), 0),
        ]
    );
}

#[tokio::test]
async fn same_function_identity_in_two_pipelines_has_independent_state() {
    let mut graph = build_graph(descriptor(), Arc::new(AccountActivity));
    let mut second = descriptor();
    second.pipeline_state_id = "other_pipeline_v1".into();
    graph
        .add_process_function(&ProcessFunctionRegistration {
            output_name: "other_activity".into(),
            source_name: "events".into(),
            descriptor: second,
            handler: ProcessHandler::Native(Arc::new(AccountActivity)),
        })
        .unwrap();
    let mut graph = graph.initialize_managed_state().await.unwrap();
    let first = graph
        .execute_cycle(&source(&[("a", 7, 100_000)]), 95, None)
        .await
        .unwrap();
    assert_eq!(totals(&first["activity"]), vec![7]);
    assert_eq!(totals(&first["other_activity"]), vec![7]);
    let second = graph
        .execute_cycle(&source(&[("a", 5, 101_000)]), 96, None)
        .await
        .unwrap();
    assert_eq!(totals(&second["activity"]), vec![12]);
    assert_eq!(totals(&second["other_activity"]), vec![12]);
}

#[tokio::test]
async fn keyed_state_and_timer_survive_graph_checkpoint_restore() {
    let mut graph = build_graph(descriptor(), Arc::new(AccountActivity))
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
    let restored = build_graph(descriptor(), Arc::new(AccountActivity))
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
    let mut graph = build_graph(binding, Arc::new(AccountActivity))
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
    let mut original = build_graph(descriptor(), Arc::new(AccountActivity))
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
    let result = build_graph(changed, Arc::new(AccountActivity))
        .initialize_managed_state()
        .await
        .unwrap()
        .restore_state_frames(&whole, &vnodes, 256);
    assert!(result.is_err());
}

#[tokio::test]
async fn restore_rejects_state_over_declared_key_budget() {
    let mut original = build_graph(descriptor(), Arc::new(AccountActivity))
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
    let result = build_graph(smaller, Arc::new(AccountActivity))
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
    assert_eq!(db.process_functions().len(), 1);
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

#[cfg(feature = "process-remote")]
mod remote_pipeline {
    use std::path::Path;
    use std::time::Duration;

    use async_trait::async_trait;
    use laminar_connectors::checkpoint::SourceCheckpoint;
    use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
    use laminar_connectors::connector::{
        SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceInputMode,
        SourcePosition, SourceStart, SourceTopology,
    };
    use laminar_connectors::error::ConnectorError;
    use laminar_connectors::registry::ConnectorRegistry;
    use sha2::{Digest, Sha256};
    use tokio::net::TcpListener;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::process_function::remote::{
        LocalPythonWorker, LocalPythonWorkerConfig, RemoteProcessClient, RustReferenceWorker,
    };
    use crate::subscription::SubscriptionPortal;

    async fn worker_client(
        descriptor: ProcessFunctionDescriptor,
        handler: Arc<dyn NativeProcessFunction>,
        timeout: Duration,
    ) -> (
        Arc<RemoteProcessClient>,
        CancellationToken,
        tokio::task::JoinHandle<Result<(), DbError>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let worker = RustReferenceWorker::new(descriptor.clone(), handler, 4).unwrap();
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(worker.serve_loopback(listener, shutdown.clone()));
        let client = RemoteProcessClient::connect_loopback(
            &format!("http://{address}"),
            descriptor,
            4,
            timeout,
        )
        .await
        .unwrap();
        (Arc::new(client), shutdown, task)
    }

    fn remote_graph(
        binding: ProcessFunctionDescriptor,
        client: Arc<RemoteProcessClient>,
        runtime: tokio::runtime::Handle,
    ) -> OperatorGraph {
        let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
        graph.set_runtime_handle(runtime);
        graph.set_query_budget_ns(5_000_000_000);
        graph.register_source_schema("events".into(), input_schema());
        graph
            .add_process_function(&ProcessFunctionRegistration {
                output_name: "activity".into(),
                source_name: "events".into(),
                descriptor: binding,
                handler: ProcessHandler::Remote(client),
            })
            .unwrap();
        graph
    }

    async fn drain(graph: &mut OperatorGraph, watermark_ms: i64) -> Vec<RecordBatch> {
        let wake = graph.process_work_wake().unwrap();
        let mut output = Vec::new();
        for _ in 0..32 {
            if !graph.has_runnable_deferred_work() {
                tokio::time::timeout(Duration::from_secs(3), wake.notified())
                    .await
                    .unwrap();
            }
            let mut result = graph
                .execute_cycle(&FxHashMap::default(), watermark_ms, None)
                .await
                .unwrap();
            output.extend(result.remove("activity").unwrap_or_default());
            if graph.checkpoint_is_quiescent() && !graph.has_runnable_deferred_work() {
                return output;
            }
        }
        panic!("remote graph did not drain within 32 completion steps");
    }

    fn python_config(python: String) -> LocalPythonWorkerConfig {
        let repository = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let example = repository.join("examples/process_python");
        let mut python_paths = vec![repository.join("python/laminardb_process")];
        if let Some(dependencies) = std::env::var_os("LAMINAR_PROCESS_PYTHON_DEPS") {
            let path = std::path::PathBuf::from(dependencies);
            python_paths.push(if path.is_absolute() {
                path
            } else {
                repository.join(path)
            });
        }
        LocalPythonWorkerConfig {
            python: python.into(),
            manifest: example.join("manifest.json"),
            handler_file: example.join("handler.py"),
            function: "handle".into(),
            python_paths,
            max_in_flight: 2,
            timeout: Duration::from_secs(5),
        }
    }

    fn python_input(key: &str, amount: i64, at_us: i64) -> RecordBatch {
        RecordBatch::try_new(
            input_schema_for_python(),
            vec![
                Arc::new(StringArray::from(vec![key])),
                Arc::new(Int64Array::from(vec![amount])),
                Arc::new(TimestampMicrosecondArray::from(vec![at_us])),
            ],
        )
        .unwrap()
    }

    fn crash_on_second_python_config(
        python: &str,
        directory: &Path,
    ) -> (LocalPythonWorkerConfig, std::path::PathBuf) {
        let crash_marker = directory.join("crash-on-second-input");
        std::fs::write(&crash_marker, []).unwrap();
        let marker_literal = serde_json::to_string(&crash_marker.to_string_lossy()).unwrap();
        let handler = format!(
            r#"import os
import pyarrow as pa
from laminardb_process import ActivationResult, Mutation

OUTPUT_SCHEMA = pa.schema([
    pa.field("key", pa.utf8(), nullable=False),
    pa.field("total", pa.int64(), nullable=False),
    pa.field("ts", pa.timestamp("us"), nullable=False),
])

def handle(activations):
    results = []
    for activation in activations:
        amount = activation.input.column(1)[0].as_py()
        if amount == 50 and os.path.exists({marker_literal}):
            os.remove({marker_literal})
            os._exit(47)
        total = (activation.state.value or 0) + amount
        output = pa.record_batch([
            pa.array([activation.key_text], type=pa.utf8()),
            pa.array([total], type=pa.int64()),
            pa.array([activation.event_time_us], type=pa.timestamp("us")),
        ], schema=OUTPUT_SCHEMA)
        results.append(ActivationResult(
            activation.id, output=(output,), mutation=Mutation.set(total)
        ))
    return tuple(results)
"#
        );
        let handler_path = directory.join("replay_handler.py");
        std::fs::write(&handler_path, &handler).unwrap();
        let repository = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut descriptor = ProcessFunctionDescriptor::from_manifest_json(
            &std::fs::read(repository.join("examples/process_python/manifest.json")).unwrap(),
        )
        .unwrap();
        descriptor.implementation_digest = format!("{:x}", Sha256::digest(handler.as_bytes()));
        let manifest_path = directory.join("manifest.json");
        std::fs::write(&manifest_path, descriptor.to_manifest_json().unwrap()).unwrap();
        let mut config = python_config(python.to_string());
        config.manifest = manifest_path;
        config.handler_file = handler_path;
        config.timeout = Duration::from_secs(15);
        (config, crash_marker)
    }

    const REPLAY_SOURCE: &str = "process-replay-test";

    #[derive(Clone)]
    struct ReplaySourceControl {
        ready: tokio::sync::watch::Sender<usize>,
        starts: Arc<parking_lot::Mutex<Vec<usize>>>,
    }

    impl ReplaySourceControl {
        fn new() -> Self {
            let (ready, _) = tokio::sync::watch::channel(0);
            Self {
                ready,
                starts: Arc::new(parking_lot::Mutex::new(Vec::new())),
            }
        }

        fn release(&self) {
            self.ready.send_modify(|cut| *cut += 1);
        }

        fn register(&self, registry: &ConnectorRegistry) -> Result<(), ConnectorError> {
            let ready = self.ready.subscribe();
            let starts = Arc::clone(&self.starts);
            registry.register_source(
                REPLAY_SOURCE,
                ConnectorInfo {
                    name: REPLAY_SOURCE.into(),
                    display_name: "Process replay test source".into(),
                    version: "1".into(),
                    is_source: true,
                    is_sink: false,
                    config_keys: Vec::new(),
                },
                Arc::new(move |_| {
                    Ok(Box::new(ReplaySource {
                        cursor: 0,
                        ready: ready.clone(),
                        starts: Arc::clone(&starts),
                    }))
                }),
            )
        }
    }

    struct ReplaySource {
        cursor: usize,
        ready: tokio::sync::watch::Receiver<usize>,
        starts: Arc<parking_lot::Mutex<Vec<usize>>>,
    }

    impl ReplaySource {
        fn checkpoint_at(&self) -> SourceCheckpoint {
            let mut checkpoint = SourceCheckpoint::new();
            checkpoint.set_offset("cursor", self.cursor.to_string());
            checkpoint
                .set_input_channels(vec![b"events".to_vec()])
                .unwrap();
            checkpoint
        }
    }

    #[async_trait]
    impl SourceConnector for ReplaySource {
        fn contract(&self, _: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
            Ok(SourceContract::new(
                SourceConsistency::Replayable,
                SourceTopology::Singleton,
                SourceInputMode::AppendOnly,
            ))
        }

        async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
            let (_, position, _) = request.into_parts();
            self.cursor = match position {
                SourcePosition::Initial => 0,
                SourcePosition::Resume { checkpoint, .. } => checkpoint
                    .get_offset("cursor")
                    .and_then(|cursor| cursor.parse::<usize>().ok())
                    .filter(|cursor| *cursor <= 2)
                    .ok_or_else(|| {
                        ConnectorError::ConfigurationError(
                            "process replay checkpoint has no valid cursor".into(),
                        )
                    })?,
            };
            self.starts.lock().push(self.cursor);
            Ok(())
        }

        async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
            if *self.ready.borrow() <= self.cursor {
                return Ok(None);
            }
            let batch = match self.cursor {
                0 => python_input("a", 60, 100_000),
                1 => python_input("a", 50, 100_050),
                _ => return Ok(None),
            };
            self.cursor += 1;
            Ok(Some(
                SourceBatch::new(batch).with_checkpoint(self.checkpoint_at()),
            ))
        }

        fn schema(&self) -> SchemaRef {
            input_schema_for_python()
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            self.checkpoint_at()
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }
    }

    async fn replayable_python_database(
        path: &Path,
        source: &ReplaySourceControl,
        worker: &LocalPythonWorker,
    ) -> Arc<LaminarDB> {
        let source = source.clone();
        let db = LaminarDB::builder()
            .storage_dir(path)
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: None,
                ..Default::default()
            })
            .register_connector(move |registry| source.register(registry))
            .build()
            .await
            .unwrap();
        db.execute(&format!(
            "CREATE SOURCE events (key VARCHAR NOT NULL, amount BIGINT NOT NULL, \
             ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND) \
             FROM \"{REPLAY_SOURCE}\""
        ))
        .await
        .unwrap();
        db.register_remote_process_function(
            "activity",
            "events",
            worker.client().descriptor().clone(),
            worker.client(),
        )
        .await
        .unwrap();
        db.start().await.unwrap();
        db
    }

    #[cfg(feature = "files")]
    async fn file_process_database(
        checkpoint_dir: &Path,
        input_dir: &Path,
        output_dir: &Path,
        key_column: &str,
        delivery: DeliveryGuarantee,
        client: Arc<RemoteProcessClient>,
    ) -> Arc<LaminarDB> {
        let db = LaminarDB::builder()
            .storage_dir(checkpoint_dir)
            .delivery_guarantee(delivery)
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: None,
                ..Default::default()
            })
            .build()
            .await
            .unwrap();
        let input_path = input_dir.display().to_string().replace('\\', "/");
        db.execute(&format!(
            "CREATE SOURCE events ({key_column} VARCHAR NOT NULL, amount BIGINT NOT NULL, \
             ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND) \
             FROM FILES ('path' = '{input_path}', 'glob_pattern' = '*.json', \
             'stabilisation_delay' = '100ms') FORMAT JSON"
        ))
        .await
        .unwrap();
        db.register_remote_process_function(
            "activity",
            "events",
            client.descriptor().clone(),
            client,
        )
        .await
        .unwrap();
        let output_path = output_dir.display().to_string().replace('\\', "/");
        db.execute(&format!(
            "CREATE SINK activity_files FROM activity INTO FILES ('path' = '{output_path}') FORMAT JSON"
        ))
        .await
        .unwrap();
        db
    }

    #[cfg(feature = "files")]
    fn publish_file_input(staging: &Path, input_dir: &Path, name: &str, row: serde_json::Value) {
        let staged = staging.join(name);
        let mut json = serde_json::to_vec(&row).unwrap();
        json.push(b'\n');
        std::fs::write(&staged, json).unwrap();
        std::fs::rename(staged, input_dir.join(name)).unwrap();
    }

    #[cfg(feature = "files")]
    fn published_file_totals(output_dir: &Path) -> Vec<i64> {
        let mut totals = Vec::new();
        for entry in std::fs::read_dir(output_dir).unwrap() {
            let path = entry.unwrap().path();
            if path
                .extension()
                .is_none_or(|extension| extension != "jsonl")
            {
                continue;
            }
            for line in std::fs::read_to_string(path).unwrap().lines() {
                let value: serde_json::Value = serde_json::from_str(line).unwrap();
                totals.push(value["total"].as_i64().unwrap());
            }
        }
        totals.sort_unstable();
        totals
    }

    #[cfg(feature = "files")]
    #[derive(Clone, Copy)]
    enum HostFailureCut {
        PendingInvocation,
        PublishedOutput,
    }

    #[cfg(feature = "files")]
    struct MarkSecondFile {
        entered: std::path::PathBuf,
        cut: HostFailureCut,
    }

    #[cfg(feature = "files")]
    impl NativeProcessFunction for MarkSecondFile {
        fn invoke(
            &self,
            activations: &[ProcessActivation],
        ) -> Result<Vec<ProcessActivationResult>, DbError> {
            for activation in activations {
                let ProcessCallback::Input(batch) = &activation.callback else {
                    continue;
                };
                let amount = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0);
                if amount == 50 {
                    std::fs::write(&self.entered, activation.id.to_string()).map_err(|error| {
                        DbError::Pipeline(format!("mark pending process invocation: {error}"))
                    })?;
                    if matches!(self.cut, HostFailureCut::PendingInvocation) {
                        std::thread::sleep(Duration::from_secs(25));
                    }
                }
            }
            AccountActivity.invoke(activations)
        }
    }

    #[cfg(feature = "files")]
    struct CaptureInputIds {
        ids: Arc<parking_lot::Mutex<Vec<u64>>>,
    }

    #[cfg(feature = "files")]
    impl NativeProcessFunction for CaptureInputIds {
        fn invoke(
            &self,
            activations: &[ProcessActivation],
        ) -> Result<Vec<ProcessActivationResult>, DbError> {
            let mut ids = self.ids.lock();
            for activation in activations {
                if matches!(&activation.callback, ProcessCallback::Input(_)) {
                    ids.push(activation.id);
                }
            }
            drop(ids);
            AccountActivity.invoke(activations)
        }
    }

    #[cfg(feature = "files")]
    async fn run_host_failure_child(root: &Path, cut: HostFailureCut) {
        let input_dir = root.join("input");
        let output_dir = root.join("output");
        let checkpoint_dir = root.join("checkpoint");
        let mut binding = descriptor();
        binding.runtime = ProcessRuntime::RemoteRust;
        let (client, _shutdown, _worker) = worker_client(
            binding,
            Arc::new(MarkSecondFile {
                entered: root.join("invocation-entered"),
                cut,
            }),
            Duration::from_secs(30),
        )
        .await;
        let db = file_process_database(
            &checkpoint_dir,
            &input_dir,
            &output_dir,
            "account",
            DeliveryGuarantee::AtLeastOnce,
            client,
        )
        .await;
        let mut portal = db
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        db.start().await.unwrap();
        publish_file_input(
            root,
            &input_dir,
            "first.json",
            serde_json::json!({"account": "a", "amount": 60, "ts": 100_000}),
        );
        assert_eq!(next_process_total(&mut portal).await, 60);
        assert!(db.checkpoint().await.unwrap().success);
        assert_eq!(published_file_totals(&output_dir), vec![60]);
        publish_file_input(
            root,
            &input_dir,
            "second.json",
            serde_json::json!({"account": "a", "amount": 50, "ts": 100_005}),
        );
        if matches!(cut, HostFailureCut::PublishedOutput) {
            assert_eq!(next_process_total(&mut portal).await, 110);
            tokio::time::timeout(Duration::from_secs(10), async {
                while published_file_totals(&output_dir) != [60, 110] {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("second file output was not durably published");
            std::fs::write(root.join("output-published"), b"ready").unwrap();
        }
        tokio::time::sleep(Duration::from_secs(20)).await;
        panic!("host failure test child was not terminated at the selected cut");
    }

    async fn next_process_total(portal: &mut SubscriptionPortal) -> i64 {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match portal.next_frame().await {
                    Some(PortalFrame::Batch { batch, .. }) => {
                        assert_eq!(batch.num_rows(), 1);
                        let total_column = batch.schema().index_of("total").unwrap();
                        return batch
                            .column(total_column)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(0);
                    }
                    Some(PortalFrame::Barrier { .. }) => {}
                    other => panic!("Python process output unavailable: {other:?}"),
                }
            }
        })
        .await
        .unwrap()
    }

    async fn checkpointed_python_database(
        path: &Path,
        worker: &LocalPythonWorker,
    ) -> Arc<LaminarDB> {
        let db = LaminarDB::builder()
            .storage_dir(path)
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig::default())
            .build()
            .await
            .unwrap();
        db.execute(
            "CREATE SOURCE events (key VARCHAR NOT NULL, amount BIGINT NOT NULL, \
             ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        db.register_remote_process_function(
            "activity",
            "events",
            worker.client().descriptor().clone(),
            worker.client(),
        )
        .await
        .unwrap();
        db.start().await.unwrap();
        db
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn remote_graph_serializes_keys_and_drains_before_checkpoint() {
        let mut binding = descriptor();
        binding.runtime = ProcessRuntime::RemoteRust;
        binding.limits.max_batch_rows = 2;
        let (client, shutdown, worker) = worker_client(
            binding.clone(),
            Arc::new(AccountActivity),
            Duration::from_secs(3),
        )
        .await;
        let mut graph = remote_graph(binding, client, tokio::runtime::Handle::current())
            .initialize_managed_state()
            .await
            .unwrap();
        let first = graph
            .execute_cycle(
                &source(&[("a", 60, 100_000), ("b", 5, 100_000), ("a", 50, 101_000)]),
                95,
                None,
            )
            .await
            .unwrap();
        assert!(first.get("activity").is_none_or(Vec::is_empty));
        assert!(!graph.checkpoint_is_quiescent());
        let (deferred, sources) = graph.take_cycle_deferrals();
        assert!(deferred);
        assert!(sources.contains("events"));
        let mut actual = activity_rows(&drain(&mut graph, 95).await);
        actual.sort_unstable();
        assert_eq!(
            actual,
            vec![
                ("a".into(), "running".into(), 60, false, 100_000),
                ("a".into(), "running".into(), 110, true, 101_000),
                ("b".into(), "running".into(), 5, false, 100_000),
            ]
        );
        assert!(graph.checkpoint_is_quiescent());

        let first_timer = graph
            .execute_cycle(&FxHashMap::default(), 112, None)
            .await
            .unwrap();
        assert!(first_timer.get("activity").is_none_or(Vec::is_empty));
        let mut timers = activity_rows(&drain(&mut graph, 112).await);
        timers.sort_unstable();
        assert_eq!(
            timers,
            vec![
                ("a".into(), "inactive".into(), 110, false, 111_000),
                ("b".into(), "inactive".into(), 5, false, 110_000),
            ]
        );
        shutdown.cancel();
        worker.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn remote_graph_restore_replays_saved_timer_and_state() {
        let mut binding = descriptor();
        binding.runtime = ProcessRuntime::RemoteRust;
        let (client, shutdown, worker) = worker_client(
            binding.clone(),
            Arc::new(AccountActivity),
            Duration::from_secs(3),
        )
        .await;
        let mut original = remote_graph(
            binding.clone(),
            Arc::clone(&client),
            tokio::runtime::Handle::current(),
        )
        .initialize_managed_state()
        .await
        .unwrap();
        original
            .execute_cycle(&source(&[("a", 60, 100_000)]), 100, None)
            .await
            .unwrap();
        assert_eq!(totals(&drain(&mut original, 100).await), vec![60]);
        let (whole, vnodes) = materialize(original.capture_state(u64::MAX).unwrap());
        drop(original);

        let mut restored = remote_graph(binding, client, tokio::runtime::Handle::current())
            .initialize_managed_state()
            .await
            .unwrap()
            .restore_state_frames(&whole, &vnodes, 256)
            .unwrap()
            .0;
        restored
            .execute_cycle(&FxHashMap::default(), 112, None)
            .await
            .unwrap();
        assert_eq!(
            activity_rows(&drain(&mut restored, 112).await),
            vec![("a".into(), "inactive".into(), 60, false, 110_000)]
        );
        restored
            .execute_cycle(&source(&[("a", 50, 120_000)]), 120, None)
            .await
            .unwrap();
        assert_eq!(totals(&drain(&mut restored, 120).await), vec![110]);
        shutdown.cancel();
        worker.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lost_worker_fences_unaccepted_remote_result() {
        let mut binding = descriptor();
        binding.runtime = ProcessRuntime::RemoteRust;
        let (client, shutdown, worker) = worker_client(
            binding.clone(),
            Arc::new(AccountActivity),
            Duration::from_secs(3),
        )
        .await;
        let mut graph = remote_graph(binding, client, tokio::runtime::Handle::current())
            .initialize_managed_state()
            .await
            .unwrap();
        worker.abort();
        assert!(worker.await.is_err());
        graph
            .execute_cycle(&source(&[("a", 7, 100_000)]), 95, None)
            .await
            .unwrap();
        assert!(!graph.checkpoint_is_quiescent());
        let wake = graph.process_work_wake().unwrap();
        tokio::time::timeout(Duration::from_secs(5), wake.notified())
            .await
            .unwrap();
        let error = graph
            .execute_cycle(&FxHashMap::default(), 95, None)
            .await
            .unwrap_err();
        assert!(error.requires_pipeline_recovery(), "{error}");
        shutdown.cancel();
    }

    #[tokio::test]
    async fn invalid_remote_input_halts_before_dispatch() {
        let mut binding = descriptor();
        binding.runtime = ProcessRuntime::RemoteRust;
        let (client, shutdown, worker) = worker_client(
            binding.clone(),
            Arc::new(AccountActivity),
            Duration::from_secs(3),
        )
        .await;
        let mut operator = ProcessFunctionOperator::new_remote(
            binding,
            &client,
            tokio::runtime::Handle::current(),
            Arc::new(tokio::sync::Notify::new()),
            "activity".into(),
            4,
        )
        .unwrap();
        let wrong_schema = RecordBatch::new_empty(Arc::new(Schema::empty()));
        let error = operator
            .process_with_frontiers(
                &[vec![wrong_schema]],
                &[InputFrontier {
                    watermark: None,
                    idle: false,
                }],
            )
            .await
            .unwrap_err();
        assert!(error.requires_pipeline_halt(), "{error}");
        shutdown.cancel();
        worker.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stopped_worker_runtime_wakes_graph_with_recovery_fault() {
        let mut binding = descriptor();
        binding.runtime = ProcessRuntime::RemoteRust;
        let (client, shutdown, worker) = worker_client(
            binding.clone(),
            Arc::new(AccountActivity),
            Duration::from_secs(3),
        )
        .await;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let handle = runtime.handle().clone();
        runtime.shutdown_background();
        let mut graph = remote_graph(binding, client, handle)
            .initialize_managed_state()
            .await
            .unwrap();
        graph
            .execute_cycle(&source(&[("a", 7, 100_000)]), 95, None)
            .await
            .unwrap();
        let wake = graph.process_work_wake().unwrap();
        tokio::time::timeout(Duration::from_secs(3), wake.notified())
            .await
            .unwrap();
        let error = graph
            .execute_cycle(&FxHashMap::default(), 95, None)
            .await
            .unwrap_err();
        assert!(error.requires_pipeline_recovery(), "{error}");
        shutdown.cancel();
        worker.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn remote_function_emits_through_running_database() {
        let mut binding = descriptor();
        binding.runtime = ProcessRuntime::RemoteRust;
        let (client, shutdown, worker) = worker_client(
            binding.clone(),
            Arc::new(AccountActivity),
            Duration::from_secs(3),
        )
        .await;
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE SOURCE events (account VARCHAR NOT NULL, amount BIGINT NOT NULL, \
             ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        db.register_remote_process_function("activity", "events", binding, client)
            .await
            .unwrap();
        assert_eq!(db.process_functions().len(), 1);
        db.start().await.unwrap();
        let mut portal = db
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        db.source_untyped("events")
            .unwrap()
            .push_arrow(input_batch(&[("a", 60, 100_000), ("a", 50, 101_000)]))
            .unwrap();
        let mut values = Vec::new();
        tokio::time::timeout(Duration::from_secs(3), async {
            while values.len() < 2 {
                match portal.next_frame().await {
                    Some(PortalFrame::Batch { batch, .. }) => values.extend(totals(&[batch])),
                    Some(PortalFrame::Barrier { .. }) => {}
                    other => panic!("remote process output unavailable: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(values, vec![60, 110]);
        db.shutdown().await.unwrap();
        shutdown.cancel();
        worker.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn python_at_least_once_requires_bound_dependencies() {
        let Ok(python) = std::env::var("LAMINAR_PROCESS_PYTHON") else {
            return;
        };
        let worker = LocalPythonWorker::start(python_config(python))
            .await
            .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let db = LaminarDB::builder()
            .storage_dir(directory.path())
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig::default())
            .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
            .build()
            .await
            .unwrap();
        db.execute(
            "CREATE SOURCE events (key VARCHAR NOT NULL, amount BIGINT NOT NULL, \
             ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        let error = db
            .register_remote_process_function(
                "activity",
                "events",
                worker.client().descriptor().clone(),
                worker.client(),
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("immutable dependency binding"),
            "{error}"
        );
        assert!(db.process_functions().is_empty());
        db.shutdown().await.unwrap();
        worker.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn supervised_python_worker_emits_through_running_database() {
        let Ok(python) = std::env::var("LAMINAR_PROCESS_PYTHON") else {
            return;
        };
        let worker = LocalPythonWorker::start(python_config(python))
            .await
            .unwrap();
        assert!(worker.is_alive());
        let descriptor = worker.client().descriptor().clone();
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE SOURCE events (key VARCHAR NOT NULL, amount BIGINT NOT NULL, \
             ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        db.register_remote_process_function("activity", "events", descriptor, worker.client())
            .await
            .unwrap();
        db.start().await.unwrap();
        let mut portal = db
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        let batch = RecordBatch::try_new(
            input_schema_for_python(),
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])),
                Arc::new(Int64Array::from(vec![60, 50])),
                Arc::new(TimestampMicrosecondArray::from(vec![100_000, 101_000])),
            ],
        )
        .unwrap();
        db.source_untyped("events")
            .unwrap()
            .push_arrow(batch)
            .unwrap();
        let mut values = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            while values.len() < 2 {
                match portal.next_frame().await {
                    Some(PortalFrame::Batch { batch, .. }) => {
                        let total = batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        values.extend((0..total.len()).map(|row| total.value(row)));
                    }
                    Some(PortalFrame::Barrier { .. }) => {}
                    other => panic!("Python process output unavailable: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(values, vec![60, 110]);
        db.shutdown().await.unwrap();
        worker.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn python_worker_restart_restores_state_and_timer_from_database_checkpoint() {
        let Ok(python) = std::env::var("LAMINAR_PROCESS_PYTHON") else {
            return;
        };
        let directory = tempfile::tempdir().unwrap();
        let first_worker = LocalPythonWorker::start(python_config(python.clone()))
            .await
            .unwrap();
        let first = checkpointed_python_database(directory.path(), &first_worker).await;
        let mut first_portal = first
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        first
            .source_untyped("events")
            .unwrap()
            .push_arrow(python_input("a", 60, 100_000))
            .unwrap();
        assert_eq!(next_process_total(&mut first_portal).await, 60);
        first
            .source_untyped("events")
            .unwrap()
            .push_arrow(python_input("c", 7, 100_000))
            .unwrap();
        assert_eq!(next_process_total(&mut first_portal).await, 7);
        first.checkpoint().await.unwrap();
        first.shutdown().await.unwrap();
        drop(first_portal);
        drop(first);
        first_worker.shutdown().await.unwrap();

        let second_worker = LocalPythonWorker::start(python_config(python))
            .await
            .unwrap();
        let restored = checkpointed_python_database(directory.path(), &second_worker).await;
        let mut portal = restored
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        restored
            .source_untyped("events")
            .unwrap()
            .push_arrow(python_input("a", 50, 100_050))
            .unwrap();
        assert_eq!(next_process_total(&mut portal).await, 110);
        restored
            .source_untyped("events")
            .unwrap()
            .push_arrow(python_input("b", 1, 101_000))
            .unwrap();
        assert_eq!(next_process_total(&mut portal).await, 1);
        restored.checkpoint().await.unwrap();
        restored
            .source_untyped("events")
            .unwrap()
            .push_arrow(python_input("c", 1, 102_000))
            .unwrap();
        assert_eq!(next_process_total(&mut portal).await, 1);
        restored.shutdown().await.unwrap();
        second_worker.shutdown().await.unwrap();
    }

    #[cfg(any(unix, windows))]
    fn kill_python_worker(process_id: u32) {
        #[cfg(unix)]
        let output = std::process::Command::new("kill")
            .args(["-KILL", &process_id.to_string()])
            .output()
            .unwrap();
        #[cfg(windows)]
        let output = std::process::Command::new("taskkill")
            .args(["/F", "/PID", &process_id.to_string()])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "kill Python worker: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[cfg(any(unix, windows))]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn crashed_python_worker_restores_database_checkpoint() {
        let Ok(python) = std::env::var("LAMINAR_PROCESS_PYTHON") else {
            return;
        };
        let directory = tempfile::tempdir().unwrap();
        let worker = LocalPythonWorker::start(python_config(python.clone()))
            .await
            .unwrap();
        let first = checkpointed_python_database(directory.path(), &worker).await;
        let mut portal = first
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        first
            .source_untyped("events")
            .unwrap()
            .push_arrow(python_input("a", 60, 100_000))
            .unwrap();
        assert_eq!(next_process_total(&mut portal).await, 60);
        first.checkpoint().await.unwrap();

        kill_python_worker(worker.process_id());
        tokio::time::timeout(Duration::from_secs(5), worker.wait_for_exit())
            .await
            .unwrap();
        assert!(!worker.is_alive());
        let error = worker.shutdown().await.err().unwrap();
        assert!(error.to_string().contains("exited unexpectedly"), "{error}");
        first.shutdown().await.unwrap();
        drop(portal);
        drop(first);

        let replacement = LocalPythonWorker::start(python_config(python))
            .await
            .unwrap();
        let restored = checkpointed_python_database(directory.path(), &replacement).await;
        let mut restored_portal = restored
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        restored
            .source_untyped("events")
            .unwrap()
            .push_arrow(python_input("a", 50, 100_050))
            .unwrap();
        assert_eq!(next_process_total(&mut restored_portal).await, 110);
        restored.shutdown().await.unwrap();
        replacement.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn replayable_source_replays_pending_input_after_python_worker_exit() {
        let Ok(python) = std::env::var("LAMINAR_PROCESS_PYTHON") else {
            return;
        };
        let directory = tempfile::tempdir().unwrap();
        let (worker_config, crash_marker) =
            crash_on_second_python_config(&python, directory.path());

        let source = ReplaySourceControl::new();
        let worker = LocalPythonWorker::start(worker_config.clone())
            .await
            .unwrap();
        let first = replayable_python_database(directory.path(), &source, &worker).await;
        let mut portal = first
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        source.release();
        assert_eq!(next_process_total(&mut portal).await, 60);
        let checkpoint = first.checkpoint().await.unwrap();
        assert!(checkpoint.success, "{checkpoint:?}");

        source.release();
        tokio::time::timeout(Duration::from_secs(10), worker.wait_for_exit())
            .await
            .expect("Python worker did not exit during the uncheckpointed invocation");
        assert!(!crash_marker.exists());
        assert!(worker.shutdown().await.is_err());
        let fault = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(fault) = first.last_fault() {
                    break fault;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("database did not observe the failed worker invocation");
        assert!(
            fault.contains("process worker invocation failed"),
            "{fault}"
        );
        let shutdown_error = first.shutdown().await.unwrap_err();
        assert!(
            shutdown_error
                .to_string()
                .contains("process worker invocation failed"),
            "{shutdown_error}"
        );
        drop(portal);
        drop(first);

        let replacement = LocalPythonWorker::start(worker_config).await.unwrap();
        let restored = replayable_python_database(directory.path(), &source, &replacement).await;
        assert_eq!(source.starts.lock().as_slice(), &[0, 1]);
        let mut restored_portal = restored
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        assert_eq!(next_process_total(&mut restored_portal).await, 110);
        let checkpoint = restored.checkpoint().await.unwrap();
        assert!(checkpoint.success, "{checkpoint:?}");
        restored.shutdown().await.unwrap();
        replacement.shutdown().await.unwrap();
    }

    #[cfg(feature = "files")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn file_source_and_sink_replay_pending_python_input_after_worker_exit() {
        let Ok(python) = std::env::var("LAMINAR_PROCESS_PYTHON") else {
            return;
        };
        let directory = tempfile::tempdir().unwrap();
        let input_dir = directory.path().join("input");
        let output_dir = directory.path().join("output");
        let checkpoint_dir = directory.path().join("checkpoint");
        std::fs::create_dir(&input_dir).unwrap();
        std::fs::create_dir(&output_dir).unwrap();
        let (worker_config, crash_marker) =
            crash_on_second_python_config(&python, directory.path());

        let worker = LocalPythonWorker::start(worker_config.clone())
            .await
            .unwrap();
        let first = file_process_database(
            &checkpoint_dir,
            &input_dir,
            &output_dir,
            "key",
            DeliveryGuarantee::BestEffort,
            worker.client(),
        )
        .await;
        let mut portal = first
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        first.start().await.unwrap();
        publish_file_input(
            directory.path(),
            &input_dir,
            "first.json",
            serde_json::json!({"key": "a", "amount": 60, "ts": 100_000}),
        );
        assert_eq!(next_process_total(&mut portal).await, 60);
        let checkpoint = first.checkpoint().await.unwrap();
        assert!(checkpoint.success, "{checkpoint:?}");
        assert_eq!(published_file_totals(&output_dir), vec![60]);

        publish_file_input(
            directory.path(),
            &input_dir,
            "second.json",
            serde_json::json!({"key": "a", "amount": 50, "ts": 100_050}),
        );
        tokio::time::timeout(Duration::from_secs(10), worker.wait_for_exit())
            .await
            .expect("Python worker did not exit during the pending file input");
        assert!(!crash_marker.exists());
        assert!(worker.shutdown().await.is_err());
        tokio::time::timeout(Duration::from_secs(10), async {
            while first.last_fault().is_none() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("database did not observe the failed worker invocation");
        assert!(first.shutdown().await.is_err());
        drop(portal);
        drop(first);

        let replacement = LocalPythonWorker::start(worker_config).await.unwrap();
        let restored = file_process_database(
            &checkpoint_dir,
            &input_dir,
            &output_dir,
            "key",
            DeliveryGuarantee::BestEffort,
            replacement.client(),
        )
        .await;
        let mut restored_portal = restored
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        restored.start().await.unwrap();
        assert_eq!(next_process_total(&mut restored_portal).await, 110);
        let checkpoint = restored.checkpoint().await.unwrap();
        assert!(checkpoint.success, "{checkpoint:?}");
        restored.shutdown().await.unwrap();
        replacement.shutdown().await.unwrap();
        assert_eq!(published_file_totals(&output_dir), vec![60, 110]);
    }

    #[cfg(feature = "files")]
    async fn assert_file_replay_after_host_termination(cut: HostFailureCut, test_name: &str) {
        const CHILD_ENV: &str = "LAMINAR_PROCESS_HOST_FAILURE_TEST_CHILD";
        if let Some(root) = std::env::var_os(CHILD_ENV) {
            run_host_failure_child(Path::new(&root), cut).await;
            return;
        }

        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let input_dir = root.join("input");
        let output_dir = root.join("output");
        let checkpoint_dir = root.join("checkpoint");
        std::fs::create_dir(&input_dir).unwrap();
        std::fs::create_dir(&output_dir).unwrap();
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", test_name, "--nocapture"])
            .env(CHILD_ENV, root)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true);
        let mut child = child.spawn().unwrap();
        let marker = match cut {
            HostFailureCut::PendingInvocation => "invocation-entered",
            HostFailureCut::PublishedOutput => "output-published",
        };
        let entered = tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                if root.join(marker).exists() {
                    return;
                }
                if let Some(status) = child.try_wait().unwrap() {
                    panic!("host failure child exited before {marker}: {status}");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if entered.is_err() {
            child.start_kill().unwrap();
            let _ = child.wait().await;
            panic!("host failure child did not reach {marker}");
        }
        child.start_kill().unwrap();
        let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(!status.success());
        let before_replay = match cut {
            HostFailureCut::PendingInvocation => vec![60],
            HostFailureCut::PublishedOutput => vec![60, 110],
        };
        assert_eq!(published_file_totals(&output_dir), before_replay);
        let pending_id = std::fs::read_to_string(root.join("invocation-entered"))
            .unwrap()
            .parse::<u64>()
            .unwrap();
        assert_eq!(pending_id, 1);

        let mut binding = descriptor();
        binding.runtime = ProcessRuntime::RemoteRust;
        let replayed_ids = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let handler = Arc::new(CaptureInputIds {
            ids: Arc::clone(&replayed_ids),
        });
        let (client, shutdown, worker) =
            worker_client(binding, handler, Duration::from_secs(5)).await;
        let restored = file_process_database(
            &checkpoint_dir,
            &input_dir,
            &output_dir,
            "account",
            DeliveryGuarantee::AtLeastOnce,
            client,
        )
        .await;
        let mut portal = restored
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        restored.start().await.unwrap();
        assert_eq!(next_process_total(&mut portal).await, 110);
        assert!(restored.checkpoint().await.unwrap().success);
        restored.shutdown().await.unwrap();
        shutdown.cancel();
        worker.await.unwrap().unwrap();
        let after_replay = match cut {
            HostFailureCut::PendingInvocation => vec![60, 110],
            HostFailureCut::PublishedOutput => vec![60, 110, 110],
        };
        assert_eq!(published_file_totals(&output_dir), after_replay);
        assert_eq!(replayed_ids.lock().as_slice(), &[pending_id]);
    }

    #[cfg(feature = "files")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn at_least_once_file_source_and_sink_replay_same_activation_after_host_termination() {
        assert_file_replay_after_host_termination(
            HostFailureCut::PendingInvocation,
            "process_function::tests::remote_pipeline::at_least_once_file_source_and_sink_replay_same_activation_after_host_termination",
        )
        .await;
    }

    #[cfg(feature = "files")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn at_least_once_file_sink_republishes_after_uncheckpointed_host_termination() {
        assert_file_replay_after_host_termination(
            HostFailureCut::PublishedOutput,
            "process_function::tests::remote_pipeline::at_least_once_file_sink_republishes_after_uncheckpointed_host_termination",
        )
        .await;
    }

    #[cfg(feature = "files")]
    async fn run_python_host_loss_child(root: &Path, endpoint: &str) {
        let repository = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let manifest =
            std::fs::read(repository.join("examples/process_python/manifest.json")).unwrap();
        let descriptor = ProcessFunctionDescriptor::from_manifest_json(&manifest).unwrap();
        let client =
            RemoteProcessClient::connect_loopback(endpoint, descriptor, 2, Duration::from_secs(15))
                .await
                .unwrap();
        let input_dir = root.join("input");
        let output_dir = root.join("output");
        let checkpoint_dir = root.join("checkpoint");
        let db = file_process_database(
            &checkpoint_dir,
            &input_dir,
            &output_dir,
            "key",
            DeliveryGuarantee::BestEffort,
            Arc::new(client),
        )
        .await;
        let mut portal = db
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        db.start().await.unwrap();
        publish_file_input(
            root,
            &input_dir,
            "first.json",
            serde_json::json!({"key": "a", "amount": 60, "ts": 100_000}),
        );
        assert_eq!(next_process_total(&mut portal).await, 60);
        assert!(db.checkpoint().await.unwrap().success);
        assert_eq!(published_file_totals(&output_dir), vec![60]);
        publish_file_input(
            root,
            &input_dir,
            "second.json",
            serde_json::json!({"key": "a", "amount": 50, "ts": 100_050}),
        );
        assert_eq!(next_process_total(&mut portal).await, 110);
        tokio::time::timeout(Duration::from_secs(10), async {
            while published_file_totals(&output_dir) != [60, 110] {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("second Python output was not durably published");
        std::fs::write(root.join("output-published"), b"ready").unwrap();
        tokio::time::sleep(Duration::from_secs(30)).await;
        panic!("Python host loss test child was not terminated");
    }

    #[cfg(feature = "files")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn python_file_sink_replays_after_separately_supervised_host_loss() {
        const CHILD_ENV: &str = "LAMINAR_PROCESS_PYTHON_HOST_LOSS_CHILD";
        const ENDPOINT_ENV: &str = "LAMINAR_PROCESS_PYTHON_HOST_LOSS_ENDPOINT";
        if let Some(root) = std::env::var_os(CHILD_ENV) {
            let endpoint = std::env::var(ENDPOINT_ENV).unwrap();
            run_python_host_loss_child(Path::new(&root), &endpoint).await;
            return;
        }
        let Ok(python) = std::env::var("LAMINAR_PROCESS_PYTHON") else {
            return;
        };
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path();
        let input_dir = root.join("input");
        let output_dir = root.join("output");
        let checkpoint_dir = root.join("checkpoint");
        std::fs::create_dir(&input_dir).unwrap();
        std::fs::create_dir(&output_dir).unwrap();

        let mut config = python_config(python.clone());
        config.timeout = Duration::from_secs(15);
        let worker = LocalPythonWorker::start(config.clone()).await.unwrap();
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap());
        child
            .args([
                "--exact",
                "process_function::tests::remote_pipeline::python_file_sink_replays_after_separately_supervised_host_loss",
                "--nocapture",
            ])
            .env(CHILD_ENV, root)
            .env(ENDPOINT_ENV, worker.loopback_endpoint())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true);
        let mut child = child.spawn().unwrap();
        let published = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if root.join("output-published").exists() {
                    return;
                }
                if let Some(status) = child.try_wait().unwrap() {
                    panic!("Python host loss child exited before output publication: {status}");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if published.is_err() {
            child.start_kill().unwrap();
            let _ = child.wait().await;
            panic!("Python host loss child did not publish the second output");
        }
        child.start_kill().unwrap();
        let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(!status.success());
        assert!(worker.is_alive());
        assert_eq!(published_file_totals(&output_dir), vec![60, 110]);
        worker.shutdown().await.unwrap();

        let replacement = LocalPythonWorker::start(config).await.unwrap();
        let restored = file_process_database(
            &checkpoint_dir,
            &input_dir,
            &output_dir,
            "key",
            DeliveryGuarantee::BestEffort,
            replacement.client(),
        )
        .await;
        let mut portal = restored
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        restored.start().await.unwrap();
        assert_eq!(next_process_total(&mut portal).await, 110);
        assert!(restored.checkpoint().await.unwrap().success);
        restored.shutdown().await.unwrap();
        replacement.shutdown().await.unwrap();
        assert_eq!(published_file_totals(&output_dir), vec![60, 110, 110]);
    }

    #[tokio::test]
    async fn local_python_worker_rejects_changed_handler_before_spawn() {
        let repository = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let example = repository.join("examples/process_python");
        let directory = tempfile::tempdir().unwrap();
        let changed = directory.path().join("handler.py");
        let mut bytes = std::fs::read(example.join("handler.py")).unwrap();
        bytes.extend_from_slice(b"\n# changed after packaging\n");
        std::fs::write(&changed, bytes).unwrap();
        let error = LocalPythonWorker::start(LocalPythonWorkerConfig {
            python: "python".into(),
            manifest: example.join("manifest.json"),
            handler_file: changed,
            function: "handle".into(),
            python_paths: Vec::new(),
            max_in_flight: 1,
            timeout: Duration::from_secs(1),
        })
        .await
        .err()
        .unwrap();
        assert!(error.to_string().contains("digest differs"), "{error}");
    }

    fn input_schema_for_python() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]))
    }
}

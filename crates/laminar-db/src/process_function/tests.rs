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
        python_environment: None,
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

#[test]
fn manifest_binds_python_environment_and_rejects_invalid_inventory_identity() {
    let mut binding = descriptor();
    let legacy = binding.to_manifest_json().unwrap();
    assert!(!serde_json::from_slice::<serde_json::Value>(&legacy)
        .unwrap()
        .as_object()
        .unwrap()
        .contains_key("python_environment"));
    binding.runtime = ProcessRuntime::RemotePython;
    binding.python_environment = Some(super::PythonEnvironmentBinding {
        version: 1,
        executable: "bin/python3.13".into(),
        handler: "handler:handle".into(),
        runtime_sha256: "b".repeat(64),
        import_roots_sha256: vec!["c".repeat(64), "d".repeat(64)],
    });
    let raw = binding.to_manifest_json().unwrap();
    let restored = ProcessFunctionDescriptor::from_manifest_json(&raw).unwrap();
    assert_eq!(restored.python_environment, binding.python_environment);
    let original: serde_json::Value = serde_json::from_slice(&raw).unwrap();
    for (field, value) in [
        ("version", serde_json::json!(2)),
        ("executable", serde_json::json!("../outside")),
        ("executable", serde_json::json!("/absolute")),
        ("executable", serde_json::json!("C:\\python.exe")),
        ("executable", serde_json::json!("a//b")),
        ("executable", serde_json::json!("a/./b")),
        ("handler", serde_json::json!("handler")),
        ("handler", serde_json::json!("handler:1invalid")),
        ("handler", serde_json::json!("nested.module:handle")),
        ("runtime_sha256", serde_json::json!("B".repeat(64))),
        ("runtime_sha256", serde_json::json!("x".repeat(64))),
        ("import_roots_sha256", serde_json::json!([])),
        (
            "import_roots_sha256",
            serde_json::json!(vec!["d".repeat(64); 17]),
        ),
        ("unexpected", serde_json::json!(true)),
    ] {
        let mut invalid = original.clone();
        invalid["python_environment"][field] = value;
        assert!(
            ProcessFunctionDescriptor::from_manifest_json(&serde_json::to_vec(&invalid).unwrap())
                .is_err(),
            "{field}"
        );
    }
    let mut invalid = original;
    invalid["python_environment"] = serde_json::Value::Null;
    assert!(
        ProcessFunctionDescriptor::from_manifest_json(&serde_json::to_vec(&invalid).unwrap())
            .is_err()
    );
    let mut changed = binding.clone();
    changed
        .python_environment
        .as_mut()
        .unwrap()
        .import_roots_sha256
        .swap(0, 1);
    assert_ne!(
        changed.binding_sha256().unwrap(),
        binding.binding_sha256().unwrap()
    );
    changed.python_environment.as_mut().unwrap().executable = "bin/other-python".into();
    assert_ne!(
        changed.binding_sha256().unwrap(),
        binding.binding_sha256().unwrap()
    );
    changed.runtime = ProcessRuntime::RemoteRust;
    assert!(changed.to_manifest_json().is_err());
}

#[test]
fn checkpoint_restore_rejects_oversized_metadata_before_decode() {
    let mut original =
        ProcessFunctionOperator::new(descriptor(), Arc::new(AccountActivity), 4).unwrap();
    let checkpoint = original.checkpoint().unwrap().unwrap();
    let mut frame: serde_json::Value = serde_json::from_slice(&checkpoint.data).unwrap();
    frame["next_activation_id"] = serde_json::json!(55);
    frame["next_timer_generation"] = serde_json::json!(77);
    frame["watermark_us"] = serde_json::json!(1_000);
    let mut oversized = serde_json::to_vec(&frame).unwrap();
    oversized.resize(4_096, b' ');

    let mut replacement =
        ProcessFunctionOperator::new(descriptor(), Arc::new(AccountActivity), 4).unwrap();
    let before = replacement.checkpoint().unwrap().unwrap().data;
    let error = replacement
        .restore(crate::operator_graph::OperatorCheckpoint { data: oversized })
        .unwrap_err();
    assert!(error.to_string().contains("metadata frame exceeds"));
    assert_eq!(replacement.checkpoint().unwrap().unwrap().data, before);
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

async fn assert_native_state_and_timer_budget_across_churn() {
    let mut binding = descriptor();
    binding.limits.max_keys = 64;
    binding.limits.max_timers = 64;
    binding.limits.max_state_bytes = 16 * 1024;
    binding.limits.max_input_rows = 64;
    let keys = (0..64)
        .map(|index| format!("account_{index}"))
        .collect::<Vec<_>>();
    let mut operator =
        ProcessFunctionOperator::new(binding.clone(), Arc::new(AccountActivity), 4).unwrap();

    for round in 0..64 {
        let time_us = 100_000 + round * 1_000;
        let rows = keys
            .iter()
            .map(|key| (key.as_str(), 1, time_us))
            .collect::<Vec<_>>();
        let output = operator
            .process_with_frontiers(
                &[vec![input_batch(&rows)]],
                &[InputFrontier {
                    watermark: Some(time_us / 1_000),
                    idle: false,
                }],
            )
            .await
            .unwrap();
        assert_eq!(totals(&output), vec![round + 1; keys.len()]);
        assert!(operator.managed_state_accounting().unwrap().live <= 16 * 1024);

        if (round + 1) % 16 == 0 {
            let whole = operator.checkpoint().unwrap().unwrap();
            let frames = operator
                .checkpoint_vnodes(&[0, 1, 2, 3], 4, 64 * 1024)
                .unwrap()
                .unwrap();
            let mut replacement =
                ProcessFunctionOperator::new(binding.clone(), Arc::new(AccountActivity), 4)
                    .unwrap();
            replacement.restore(whole).unwrap();
            for frame in frames {
                let bytes = frame.state.unwrap().materialize(&mut 0, u64::MAX).unwrap();
                replacement.restore_vnode(frame.vnode, 4, &bytes).unwrap();
            }
            assert_eq!(
                replacement.managed_state_accounting(),
                operator.managed_state_accounting()
            );
            operator = replacement;
        }
    }

    let callbacks = operator
        .process_with_frontiers(
            &[Vec::new()],
            &[InputFrontier {
                watermark: Some(173),
                idle: false,
            }],
        )
        .await
        .unwrap();
    assert_eq!(totals(&callbacks), vec![64; keys.len()]);
    assert!(operator.managed_state_accounting().unwrap().live <= 16 * 1024);
    assert!(!operator.deferred_work_is_runnable());

    let overload_rows = keys
        .iter()
        .map(|key| (key.as_str(), 1, 174_000))
        .chain(std::iter::once(("extra", 1, 174_000)))
        .collect::<Vec<_>>();
    let before = operator.managed_state_accounting();
    let error = operator
        .process_with_frontiers(
            &[vec![input_batch(&overload_rows)]],
            &[InputFrontier {
                watermark: Some(174),
                idle: false,
            }],
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("process input budget exceeded"));
    assert_eq!(operator.managed_state_accounting(), before);
}

#[tokio::test]
async fn native_state_and_timer_budget_stays_bounded_across_churn_and_restore() {
    assert_native_state_and_timer_budget_across_churn().await;
}

#[tokio::test]
#[ignore = "manual sustained resource qualification"]
async fn native_state_and_timer_budget_resource_stress() {
    for _ in 0..2_700 {
        assert_native_state_and_timer_budget_across_churn().await;
    }
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

#[tokio::test]
async fn vnode_restore_accepts_escaped_key_with_tight_state_budget() {
    let mut binding = descriptor();
    binding.timer_names.clear();
    binding.limits.max_state_bytes = 256;
    let key = "\u{0001}".repeat(64);
    let mut original =
        ProcessFunctionOperator::new(binding.clone(), Arc::new(StateEcho), 4).unwrap();
    original
        .process_with_frontiers(
            &[vec![input_batch(&[(key.as_str(), 3, 100_000)])]],
            &[InputFrontier {
                watermark: Some(100),
                idle: false,
            }],
        )
        .await
        .unwrap();
    let whole = original.checkpoint().unwrap().unwrap();
    let frames = original
        .checkpoint_vnodes(&[0, 1, 2, 3], 4, u64::MAX)
        .unwrap()
        .unwrap();
    let mut restored = ProcessFunctionOperator::new(binding, Arc::new(StateEcho), 4).unwrap();
    restored.restore(whole).unwrap();
    for frame in frames {
        let mut staged_bytes = 0;
        let bytes = frame
            .state
            .unwrap()
            .materialize(&mut staged_bytes, u64::MAX)
            .unwrap();
        restored.restore_vnode(frame.vnode, 4, &bytes).unwrap();
    }
    assert_eq!(
        restored.managed_state_accounting(),
        original.managed_state_accounting()
    );
}

#[tokio::test]
async fn vnode_capture_budget_failure_preserves_live_state() {
    let mut binding = descriptor();
    binding.timer_names.clear();
    let key = "x".repeat(8 * 1024);
    let mut operator = ProcessFunctionOperator::new(binding, Arc::new(StateEcho), 4).unwrap();
    operator
        .process_with_frontiers(
            &[vec![input_batch(&[(key.as_str(), 3, 100_000)])]],
            &[InputFrontier {
                watermark: Some(100),
                idle: false,
            }],
        )
        .await
        .unwrap();
    let before = operator.managed_state_accounting();
    let baseline = operator
        .checkpoint_vnodes(&[0, 1, 2, 3], 4, u64::MAX)
        .unwrap()
        .unwrap();
    let populated_vnode = baseline
        .iter()
        .max_by_key(|frame| frame.state.as_ref().unwrap().retained_bytes())
        .unwrap()
        .vnode;
    let original_bytes = baseline
        .into_iter()
        .find(|frame| frame.vnode == populated_vnode)
        .unwrap()
        .state
        .unwrap()
        .materialize(&mut 0, u64::MAX)
        .unwrap();
    let error = operator
        .checkpoint_vnodes(&[populated_vnode], 4, 128)
        .unwrap_err();
    assert!(error.to_string().contains("capture budget exceeded"));
    assert_eq!(operator.managed_state_accounting(), before);
    let retry = operator
        .checkpoint_vnodes(&[populated_vnode], 4, u64::MAX)
        .unwrap()
        .unwrap()
        .into_iter()
        .next()
        .unwrap()
        .state
        .unwrap()
        .materialize(&mut 0, u64::MAX)
        .unwrap();
    assert_eq!(original_bytes, retry);
}

#[test]
fn vnode_restore_rejects_oversized_frame_before_decoding() {
    let mut binding = descriptor();
    binding.limits.max_state_bytes = 1;
    let mut operator = ProcessFunctionOperator::new(binding, Arc::new(AccountActivity), 4).unwrap();
    let error = operator.restore_vnode(0, 4, &[b' '; 135]).unwrap_err();
    assert!(error.to_string().contains("frame exceeds state budget"));
    assert_eq!(operator.managed_state_accounting().unwrap().live, 0);
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
            runtime_root: None,
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
        let config = python_test_handler_config(python, directory, &handler);
        (config, crash_marker)
    }

    fn python_test_handler_config(
        python: &str,
        directory: &Path,
        handler: &str,
    ) -> LocalPythonWorkerConfig {
        let handler_path = directory.join("replay_handler.py");
        std::fs::write(&handler_path, handler).unwrap();
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
        config
    }

    #[cfg(feature = "files")]
    fn pending_invocation_python_config(python: &str, directory: &Path) -> LocalPythonWorkerConfig {
        let entered =
            serde_json::to_string(&directory.join("invocation-entered").to_string_lossy()).unwrap();
        let release =
            serde_json::to_string(&directory.join("release-invocation").to_string_lossy()).unwrap();
        let handler = format!(
            r#"from pathlib import Path
import time
from handler import handle as account_activity

ENTERED = Path({entered})
RELEASE = Path({release})

def handle(activations):
    for activation in activations:
        if activation.input is None or activation.input.column(1)[0].as_py() != 50:
            continue
        with ENTERED.open("a", encoding="utf-8") as ids:
            ids.write(str(activation.id) + "\n")
        deadline = time.monotonic() + 25
        while not RELEASE.exists():
            if time.monotonic() >= deadline:
                raise TimeoutError("pending invocation was not released")
            time.sleep(0.01)
    return account_activity(activations)
"#
        );
        let mut config = python_test_handler_config(python, directory, &handler);
        let repository = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        config
            .python_paths
            .push(repository.join("examples/process_python"));
        config
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
    async fn file_source_database(
        checkpoint_dir: &Path,
        input_dir: &Path,
        key_column: &str,
        delivery: DeliveryGuarantee,
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
        db
    }

    #[cfg(feature = "files")]
    async fn add_file_sink(db: &LaminarDB, output_dir: &Path) {
        let output_path = output_dir.display().to_string().replace('\\', "/");
        db.execute(&format!(
            "CREATE SINK activity_files FROM activity INTO FILES ('path' = '{output_path}') FORMAT JSON"
        ))
        .await
        .unwrap();
    }

    #[cfg(feature = "files")]
    async fn file_remote_process_database(
        checkpoint_dir: &Path,
        input_dir: &Path,
        output_dir: &Path,
        key_column: &str,
        delivery: DeliveryGuarantee,
        client: Arc<RemoteProcessClient>,
    ) -> Arc<LaminarDB> {
        let db = file_source_database(checkpoint_dir, input_dir, key_column, delivery).await;
        db.register_remote_process_function(
            "activity",
            "events",
            client.descriptor().clone(),
            client,
        )
        .await
        .unwrap();
        add_file_sink(&db, output_dir).await;
        db
    }

    #[cfg(feature = "files")]
    async fn file_native_process_database(
        checkpoint_dir: &Path,
        input_dir: &Path,
        output_dir: &Path,
        handler: Arc<dyn NativeProcessFunction>,
    ) -> Arc<LaminarDB> {
        let db = file_source_database(
            checkpoint_dir,
            input_dir,
            "account",
            DeliveryGuarantee::AtLeastOnce,
        )
        .await;
        db.register_native_process_function("activity", "events", descriptor(), handler)
            .await
            .unwrap();
        add_file_sink(&db, output_dir).await;
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
    #[derive(Clone, Copy)]
    enum FileHostRuntime {
        NativeRust,
        RemoteRust,
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
    async fn run_host_failure_child(root: &Path, cut: HostFailureCut, runtime: FileHostRuntime) {
        let input_dir = root.join("input");
        let output_dir = root.join("output");
        let checkpoint_dir = root.join("checkpoint");
        let handler: Arc<dyn NativeProcessFunction> = Arc::new(MarkSecondFile {
            entered: root.join("invocation-entered"),
            cut,
        });
        let worker = match runtime {
            FileHostRuntime::RemoteRust => {
                let mut binding = descriptor();
                binding.runtime = ProcessRuntime::RemoteRust;
                Some(worker_client(binding, Arc::clone(&handler), Duration::from_secs(30)).await)
            }
            FileHostRuntime::NativeRust => None,
        };
        let db = if let Some((client, _, _)) = &worker {
            file_remote_process_database(
                &checkpoint_dir,
                &input_dir,
                &output_dir,
                "account",
                DeliveryGuarantee::AtLeastOnce,
                Arc::clone(client),
            )
            .await
        } else {
            file_native_process_database(&checkpoint_dir, &input_dir, &output_dir, handler).await
        };
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
                    other => panic!("process output unavailable: {other:?}"),
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

    fn environment_bound_python_config(
        python: &str,
        runtime: &Path,
        directory: &Path,
    ) -> LocalPythonWorkerConfig {
        let handler_directory = directory.join("handlers");
        std::fs::create_dir(&handler_directory).unwrap();
        let repository = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut source =
            std::fs::read_to_string(repository.join("examples/process_python/handler.py")).unwrap();
        source.push_str("\nfrom environment_helper import check_runtime\ncheck_runtime()\n");
        std::fs::write(handler_directory.join("environment_helper.py"),
            "import sys\ndef check_runtime():\n    assert sys.flags.isolated and sys.flags.no_site and sys.dont_write_bytecode\n").unwrap();
        std::fs::write(
            handler_directory.join("sitecustomize.py"),
            "raise RuntimeError('site imports are forbidden')\n",
        )
        .unwrap();
        let mut config = python_test_handler_config(python, &handler_directory, &source);
        let manifest = directory.join("manifest.json");
        std::fs::rename(&config.manifest, &manifest).unwrap();
        config.manifest = manifest;
        config.runtime_root = Some(runtime.to_path_buf());
        repackage_python_environment(&config);
        config
    }

    fn repackage_python_environment(config: &LocalPythonWorkerConfig) {
        let mut descriptor = ProcessFunctionDescriptor::from_manifest_json(
            &std::fs::read(&config.manifest).unwrap(),
        )
        .unwrap();
        descriptor.implementation_digest = format!(
            "{:x}",
            Sha256::digest(std::fs::read(&config.handler_file).unwrap())
        );
        let mut roots = vec![config.handler_file.parent().unwrap().to_path_buf()];
        roots.extend(config.python_paths.iter().cloned());
        descriptor.python_environment = Some(
            super::super::PythonEnvironmentBinding::capture(
                config.runtime_root.as_ref().unwrap(),
                &config.python,
                &format!(
                    "{}:{}",
                    config.handler_file.file_stem().unwrap().to_str().unwrap(),
                    config.function
                ),
                &roots,
            )
            .unwrap(),
        );
        std::fs::write(&config.manifest, descriptor.to_manifest_json().unwrap()).unwrap();
    }

    #[cfg(windows)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn environment_bound_python_blocks_file_edits_through_shutdown() {
        let (Ok(python), Some(runtime)) = (
            std::env::var("LAMINAR_PROCESS_PYTHON"),
            std::env::var_os("LAMINAR_PROCESS_PYTHON_RUNTIME_ROOT"),
        ) else {
            return;
        };
        let package = tempfile::tempdir().unwrap();
        let config = environment_bound_python_config(&python, Path::new(&runtime), package.path());
        let lazy_module = config.handler_file.parent().unwrap().join("lazy_module.py");
        std::fs::write(&lazy_module, b"VALUE = 1\n").unwrap();
        let mut source = std::fs::read_to_string(&config.handler_file).unwrap();
        source.push_str("\nbase_handle = handle\ndef handle(activations):\n    import lazy_module\n    assert lazy_module.VALUE == 1\n    return base_handle(activations)\n");
        std::fs::write(&config.handler_file, &source).unwrap();
        repackage_python_environment(&config);

        let worker = LocalPythonWorker::start(config.clone()).await.unwrap();
        for file in [&config.handler_file, &config.manifest, &lazy_module] {
            assert_eq!(
                std::fs::write(file, b"changed").unwrap_err().raw_os_error(),
                Some(32)
            );
            assert!(std::fs::remove_file(file).is_err());
        }
        assert!(std::fs::rename(
            config.handler_file.parent().unwrap(),
            package.path().join("moved_handlers")
        )
        .is_err());
        let storage = tempfile::tempdir().unwrap();
        let db = checkpointed_python_database(storage.path(), &worker).await;
        let mut portal = db
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        db.source_untyped("events")
            .unwrap()
            .push_arrow(python_input("a", 60, 100_000))
            .unwrap();
        assert_eq!(next_process_total(&mut portal).await, 60);
        db.shutdown().await.unwrap();
        drop(portal);
        drop(db);
        worker.shutdown().await.unwrap();
        std::fs::write(&lazy_module, b"VALUE = 2\n").unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&config.manifest)
            .unwrap();
    }

    #[cfg(windows)]
    #[derive(Clone, Copy)]
    enum PythonStartupFailure {
        Cancelled,
        ReadinessTimeout,
        HandlerError,
    }

    #[cfg(windows)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn environment_bound_python_reaps_failed_startup_before_releasing_guards() {
        let (Ok(python), Some(runtime)) = (
            std::env::var("LAMINAR_PROCESS_PYTHON"),
            std::env::var_os("LAMINAR_PROCESS_PYTHON_RUNTIME_ROOT"),
        ) else {
            return;
        };
        for failure in [
            PythonStartupFailure::Cancelled,
            PythonStartupFailure::ReadinessTimeout,
            PythonStartupFailure::HandlerError,
        ] {
            let package = tempfile::tempdir().unwrap();
            let mut config =
                environment_bound_python_config(&python, Path::new(&runtime), package.path());
            let (timeout, action, expected) = match failure {
                PythonStartupFailure::ReadinessTimeout => (
                    Duration::from_secs(5),
                    "time.sleep(25)",
                    "readiness timed out",
                ),
                PythonStartupFailure::Cancelled => {
                    (Duration::from_secs(30), "time.sleep(25)", "cancelled")
                }
                PythonStartupFailure::HandlerError => (
                    Duration::from_secs(30),
                    "raise RuntimeError('startup fixture failed')",
                    "closed before readiness",
                ),
            };
            config.timeout = timeout;
            let marker = package.path().join("startup_pid");
            let marker_literal = serde_json::to_string(&marker.to_string_lossy()).unwrap();
            std::fs::write(&config.handler_file, format!(
                "import os, time\nfrom pathlib import Path\nPath({marker_literal}).write_text(str(os.getpid()))\n{action}\ndef handle(_activations):\n    return ()\n"
            )).unwrap();
            repackage_python_environment(&config);
            let startup = tokio::spawn(LocalPythonWorker::start(config.clone()));
            tokio::time::timeout(Duration::from_secs(15), async {
                while !marker.exists() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            match failure {
                PythonStartupFailure::Cancelled => {
                    assert_eq!(
                        std::fs::write(&config.handler_file, b"changed")
                            .unwrap_err()
                            .raw_os_error(),
                        Some(32)
                    );
                    startup.abort();
                    assert!(startup.await.err().unwrap().is_cancelled());
                }
                PythonStartupFailure::ReadinessTimeout | PythonStartupFailure::HandlerError => {
                    let error = startup.await.unwrap().err().unwrap();
                    assert!(error.to_string().contains(expected), "{error}");
                }
            }
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if std::fs::OpenOptions::new()
                        .write(true)
                        .open(&config.handler_file)
                        .is_ok()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let process_id: u32 = std::fs::read_to_string(&marker).unwrap().parse().unwrap();
            let status = std::process::Command::new("powershell")
                .args(["-NoProfile", "-NonInteractive", "-Command"])
                .arg(format!(
                    "if (Get-Process -Id {process_id} -ErrorAction SilentlyContinue) {{ exit 1 }}"
                ))
                .status()
                .unwrap();
            assert!(
                status.success(),
                "startup worker {process_id} was not reaped"
            );
            std::fs::write(&config.handler_file, b"released").unwrap();
            std::fs::OpenOptions::new()
                .write(true)
                .open(&config.manifest)
                .unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn environment_bound_python_restores_and_rejects_dependency_drift() {
        let (Ok(python), Some(runtime)) = (
            std::env::var("LAMINAR_PROCESS_PYTHON"),
            std::env::var_os("LAMINAR_PROCESS_PYTHON_RUNTIME_ROOT"),
        ) else {
            return;
        };
        let package = tempfile::tempdir().unwrap();
        let config = environment_bound_python_config(&python, Path::new(&runtime), package.path());
        let storage = tempfile::tempdir().unwrap();
        for (amount, expected) in [(60, 60), (50, 110)] {
            let worker = LocalPythonWorker::start(config.clone()).await.unwrap();
            let db = checkpointed_python_database(storage.path(), &worker).await;
            let mut portal = db
                .open_subscription("activity", None, SubscribeStart::Tail)
                .await
                .unwrap();
            db.source_untyped("events")
                .unwrap()
                .push_arrow(python_input("a", amount, 100_000))
                .unwrap();
            assert_eq!(next_process_total(&mut portal).await, expected);
            db.checkpoint().await.unwrap();
            db.shutdown().await.unwrap();
            drop(portal);
            drop(db);
            worker.shutdown().await.unwrap();
        }
        assert!(!config
            .handler_file
            .parent()
            .unwrap()
            .join("__pycache__")
            .exists());
        let original = ProcessFunctionDescriptor::from_manifest_json(
            &std::fs::read(&config.manifest).unwrap(),
        )
        .unwrap();
        let helper = config
            .handler_file
            .parent()
            .unwrap()
            .join("environment_helper.py");
        let mut source = std::fs::read(&helper).unwrap();
        source.extend_from_slice(b"\n# Dependency rebuild\n");
        std::fs::write(helper, source).unwrap();
        let error = LocalPythonWorker::start(config.clone())
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains("environment differs"), "{error}");
        repackage_python_environment(&config);
        let worker = LocalPythonWorker::start(config).await.unwrap();
        assert_eq!(
            original.implementation_digest,
            worker.client().descriptor().implementation_digest
        );
        assert_ne!(
            original.python_environment,
            worker.client().descriptor().python_environment
        );
        let db = LaminarDB::builder()
            .storage_dir(storage.path())
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig::default())
            .build()
            .await
            .unwrap();
        db.execute("CREATE SOURCE events (key VARCHAR NOT NULL, amount BIGINT NOT NULL, ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)").await.unwrap();
        db.register_remote_process_function(
            "activity",
            "events",
            worker.client().descriptor().clone(),
            worker.client(),
        )
        .await
        .unwrap();
        let error = db.start().await.unwrap_err();
        assert!(
            error.to_string().contains("checkpoint pipeline identity"),
            "{error}"
        );
        db.shutdown().await.unwrap();
        drop(db);
        let stronger = tempfile::tempdir().unwrap();
        let db = LaminarDB::builder()
            .storage_dir(stronger.path())
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig::default())
            .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
            .build()
            .await
            .unwrap();
        db.execute("CREATE SOURCE events (key VARCHAR NOT NULL, amount BIGINT NOT NULL, ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)").await.unwrap();
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
        db.shutdown().await.unwrap();
        worker.shutdown().await.unwrap();
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
        let first = file_remote_process_database(
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
        let restored = file_remote_process_database(
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
    async fn assert_file_replay_after_host_termination(
        cut: HostFailureCut,
        runtime: FileHostRuntime,
        test_name: &str,
    ) {
        const CHILD_ENV: &str = "LAMINAR_PROCESS_HOST_FAILURE_TEST_CHILD";
        if let Some(root) = std::env::var_os(CHILD_ENV) {
            run_host_failure_child(Path::new(&root), cut, runtime).await;
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

        let replayed_ids = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let handler: Arc<dyn NativeProcessFunction> = Arc::new(CaptureInputIds {
            ids: Arc::clone(&replayed_ids),
        });
        let worker = match runtime {
            FileHostRuntime::RemoteRust => {
                let mut binding = descriptor();
                binding.runtime = ProcessRuntime::RemoteRust;
                Some(worker_client(binding, Arc::clone(&handler), Duration::from_secs(5)).await)
            }
            FileHostRuntime::NativeRust => None,
        };
        let restored = if let Some((client, _, _)) = &worker {
            file_remote_process_database(
                &checkpoint_dir,
                &input_dir,
                &output_dir,
                "account",
                DeliveryGuarantee::AtLeastOnce,
                Arc::clone(client),
            )
            .await
        } else {
            file_native_process_database(&checkpoint_dir, &input_dir, &output_dir, handler).await
        };
        let mut portal = restored
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        restored.start().await.unwrap();
        assert_eq!(next_process_total(&mut portal).await, 110);
        assert!(restored.checkpoint().await.unwrap().success);
        restored.shutdown().await.unwrap();
        if let Some((_, shutdown, worker)) = worker {
            shutdown.cancel();
            worker.await.unwrap().unwrap();
        }
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
            FileHostRuntime::RemoteRust,
            "process_function::tests::remote_pipeline::at_least_once_file_source_and_sink_replay_same_activation_after_host_termination",
        )
        .await;
    }

    #[cfg(feature = "files")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn at_least_once_file_sink_republishes_after_uncheckpointed_host_termination() {
        assert_file_replay_after_host_termination(
            HostFailureCut::PublishedOutput,
            FileHostRuntime::RemoteRust,
            "process_function::tests::remote_pipeline::at_least_once_file_sink_republishes_after_uncheckpointed_host_termination",
        )
        .await;
    }

    #[cfg(feature = "files")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn at_least_once_native_replays_pending_file_after_host_termination() {
        assert_file_replay_after_host_termination(
            HostFailureCut::PendingInvocation,
            FileHostRuntime::NativeRust,
            "process_function::tests::remote_pipeline::at_least_once_native_replays_pending_file_after_host_termination",
        )
        .await;
    }

    #[cfg(feature = "files")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn at_least_once_native_republishes_file_after_host_termination() {
        assert_file_replay_after_host_termination(
            HostFailureCut::PublishedOutput,
            FileHostRuntime::NativeRust,
            "process_function::tests::remote_pipeline::at_least_once_native_republishes_file_after_host_termination",
        )
        .await;
    }

    #[cfg(feature = "files")]
    async fn run_python_host_loss_child(
        root: &Path,
        endpoint: &str,
        manifest: &Path,
        cut: HostFailureCut,
    ) {
        let descriptor =
            ProcessFunctionDescriptor::from_manifest_json(&std::fs::read(manifest).unwrap())
                .unwrap();
        let client =
            RemoteProcessClient::connect_loopback(endpoint, descriptor, 2, Duration::from_secs(15))
                .await
                .unwrap();
        let input_dir = root.join("input");
        let output_dir = root.join("output");
        let checkpoint_dir = root.join("checkpoint");
        let db = file_remote_process_database(
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
        if matches!(cut, HostFailureCut::PublishedOutput) {
            assert_eq!(next_process_total(&mut portal).await, 110);
            tokio::time::timeout(Duration::from_secs(10), async {
                while published_file_totals(&output_dir) != [60, 110] {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("second Python output was not durably published");
            std::fs::write(root.join("output-published"), b"ready").unwrap();
        }
        tokio::time::sleep(Duration::from_secs(30)).await;
        panic!("Python host loss test child was not terminated");
    }

    #[cfg(feature = "files")]
    async fn assert_python_replay_after_host_termination(cut: HostFailureCut, test_name: &str) {
        const CHILD_ENV: &str = "LAMINAR_PROCESS_PYTHON_HOST_LOSS_CHILD";
        const ENDPOINT_ENV: &str = "LAMINAR_PROCESS_PYTHON_HOST_LOSS_ENDPOINT";
        const MANIFEST_ENV: &str = "LAMINAR_PROCESS_PYTHON_HOST_LOSS_MANIFEST";
        if let Some(root) = std::env::var_os(CHILD_ENV) {
            let endpoint = std::env::var(ENDPOINT_ENV).unwrap();
            let manifest = std::env::var_os(MANIFEST_ENV).unwrap();
            run_python_host_loss_child(Path::new(&root), &endpoint, Path::new(&manifest), cut)
                .await;
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

        let config = match cut {
            HostFailureCut::PendingInvocation => pending_invocation_python_config(&python, root),
            HostFailureCut::PublishedOutput => {
                let mut config = python_config(python);
                config.timeout = Duration::from_secs(15);
                config
            }
        };
        let worker = LocalPythonWorker::start(config.clone()).await.unwrap();
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap());
        child
            .args(["--exact", test_name, "--nocapture"])
            .env(CHILD_ENV, root)
            .env(ENDPOINT_ENV, worker.loopback_endpoint())
            .env(MANIFEST_ENV, &config.manifest)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true);
        let mut child = child.spawn().unwrap();
        let marker = match cut {
            HostFailureCut::PendingInvocation => "invocation-entered",
            HostFailureCut::PublishedOutput => "output-published",
        };
        let reached = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if root.join(marker).exists() {
                    return;
                }
                if let Some(status) = child.try_wait().unwrap() {
                    panic!("Python host loss child exited before {marker}: {status}");
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await;
        if reached.is_err() {
            child.start_kill().unwrap();
            let _ = child.wait().await;
            panic!("Python host loss child did not reach {marker}");
        }
        child.start_kill().unwrap();
        let status = tokio::time::timeout(Duration::from_secs(5), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(!status.success());
        assert!(worker.is_alive());
        let expected_before_replay = match cut {
            HostFailureCut::PendingInvocation => vec![60],
            HostFailureCut::PublishedOutput => vec![60, 110],
        };
        assert_eq!(published_file_totals(&output_dir), expected_before_replay);
        if matches!(cut, HostFailureCut::PendingInvocation) {
            assert_eq!(
                std::fs::read_to_string(root.join("invocation-entered"))
                    .unwrap()
                    .lines()
                    .collect::<Vec<_>>(),
                ["1"]
            );
        }
        worker.shutdown().await.unwrap();
        if matches!(cut, HostFailureCut::PendingInvocation) {
            std::fs::write(root.join("release-invocation"), b"ready").unwrap();
        }

        let replacement = LocalPythonWorker::start(config).await.unwrap();
        let restored = file_remote_process_database(
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
        let expected_after_replay = match cut {
            HostFailureCut::PendingInvocation => vec![60, 110],
            HostFailureCut::PublishedOutput => vec![60, 110, 110],
        };
        assert_eq!(published_file_totals(&output_dir), expected_after_replay);
        if matches!(cut, HostFailureCut::PendingInvocation) {
            assert_eq!(
                std::fs::read_to_string(root.join("invocation-entered"))
                    .unwrap()
                    .lines()
                    .collect::<Vec<_>>(),
                ["1", "1"]
            );
        }
    }

    #[cfg(feature = "files")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn python_file_sink_replays_after_separately_supervised_host_loss() {
        assert_python_replay_after_host_termination(
            HostFailureCut::PublishedOutput,
            "process_function::tests::remote_pipeline::python_file_sink_replays_after_separately_supervised_host_loss",
        )
        .await;
    }

    #[cfg(feature = "files")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn python_pending_invocation_replays_after_separately_supervised_host_loss() {
        assert_python_replay_after_host_termination(
            HostFailureCut::PendingInvocation,
            "process_function::tests::remote_pipeline::python_pending_invocation_replays_after_separately_supervised_host_loss",
        )
        .await;
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
            runtime_root: None,
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

    #[tokio::test]
    async fn local_python_worker_caps_inherited_compute_threads() {
        const CHILD_ROOT: &str = "LAMINAR_PROCESS_COMPUTE_THREADS_CHILD";
        let Ok(python) = std::env::var("LAMINAR_PROCESS_PYTHON") else {
            return;
        };
        if let Some(root) = std::env::var_os(CHILD_ROOT) {
            let root = Path::new(&root);
            let bootstrap =
                serde_json::to_string(&root.join("bootstrap.json").to_string_lossy()).unwrap();
            let config = python_test_handler_config(
                &python,
                root,
                &format!(
                    "import json\nfrom pathlib import Path\n\
                     observed = json.loads(Path({bootstrap}).read_text())\n\
                     assert observed == ['1', '1', False], observed\n\
                     def handle(_activations):\n    return ()\n"
                ),
            );
            // sitecustomize runs before the SDK imports NumPy through PyArrow.
            std::fs::write(
                root.join("sitecustomize.py"),
                format!(
                    "import json, os, sys\nfrom pathlib import Path\n\
                     Path({bootstrap}).write_text(json.dumps([\
                     os.environ.get('OMP_NUM_THREADS'), \
                     os.environ.get('OPENBLAS_NUM_THREADS'), 'numpy' in sys.modules]))\n"
                ),
            )
            .unwrap();
            LocalPythonWorker::start(config)
                .await
                .unwrap()
                .shutdown()
                .await
                .unwrap();
            return;
        }

        let directory = tempfile::tempdir().unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "process_function::tests::remote_pipeline::local_python_worker_caps_inherited_compute_threads",
                "--nocapture",
            ])
            .env(CHILD_ROOT, directory.path())
            .env("OMP_NUM_THREADS", "24")
            .env("OPENBLAS_NUM_THREADS", "24")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child test failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[tokio::test]
    async fn local_python_worker_ignores_ambient_pythonpath() {
        const CHILD_ROOT: &str = "LAMINAR_PROCESS_AMBIENT_PATH_CHILD";
        let Ok(python) = std::env::var("LAMINAR_PROCESS_PYTHON") else {
            return;
        };
        if let Some(root) = std::env::var_os(CHILD_ROOT) {
            let handler_dir = Path::new(&root).join("handler");
            let baseline = python_test_handler_config(
                &python,
                &handler_dir,
                "def handle(_activations):\n    return ()\n",
            );
            LocalPythonWorker::start(baseline)
                .await
                .unwrap()
                .shutdown()
                .await
                .unwrap();
            let imported = python_test_handler_config(
                &python,
                &handler_dir,
                "import laminar_process_ambient_path_fixture\n\ndef handle(_activations):\n    return ()\n",
            );
            assert!(LocalPythonWorker::start(imported).await.is_err());
            return;
        }

        let directory = tempfile::tempdir().unwrap();
        let handler_dir = directory.path().join("handler");
        let ambient_dir = directory.path().join("ambient");
        std::fs::create_dir(&handler_dir).unwrap();
        std::fs::create_dir(&ambient_dir).unwrap();
        std::fs::write(
            ambient_dir.join("laminar_process_ambient_path_fixture.py"),
            "VALUE = 1\n",
        )
        .unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "process_function::tests::remote_pipeline::local_python_worker_ignores_ambient_pythonpath",
                "--nocapture",
            ])
            .env(CHILD_ROOT, directory.path())
            .env("PYTHONPATH", ambient_dir)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "child test failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stderr)
                .contains("No module named 'laminar_process_ambient_path_fixture'"),
            "unexpected child stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
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

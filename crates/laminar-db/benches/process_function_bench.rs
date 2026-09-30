use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use criterion::{criterion_group, criterion_main, Criterion};
use laminar_db::process_function::{
    NativeProcessFunction, ProcessActivation, ProcessActivationResult, ProcessCallback,
    ProcessFunctionDescriptor, ProcessFunctionLimits, ValueMutation, ValueState,
};
use laminar_db::subscription::{PortalFrame, SubscribeStart};
use laminar_db::{DbError, LaminarDB};

struct RunningTotal {
    output_schema: Arc<Schema>,
}

impl NativeProcessFunction for RunningTotal {
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, DbError> {
        activations
            .iter()
            .map(|activation| {
                let ProcessCallback::Input(batch) = &activation.callback else {
                    return Err(DbError::InvalidOperation(
                        "unexpected timer callback".into(),
                    ));
                };
                let amount = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| DbError::InvalidOperation("invalid amount array".into()))?
                    .value(0);
                let previous = match activation.state {
                    ValueState::Absent | ValueState::Null => 0,
                    ValueState::Value(value) => value,
                };
                let total = previous
                    .checked_add(amount)
                    .ok_or_else(|| DbError::InvalidOperation("running total overflow".into()))?;
                let output = RecordBatch::try_new(
                    Arc::clone(&self.output_schema),
                    vec![
                        Arc::new(StringArray::from(vec![activation.key_text.as_str()])),
                        Arc::new(Int64Array::from(vec![total])),
                        Arc::new(TimestampMicrosecondArray::from(vec![
                            activation.event_time_us,
                        ])),
                    ],
                )
                .map_err(|error| DbError::InvalidOperation(error.to_string()))?;
                Ok(ProcessActivationResult {
                    activation_id: activation.id,
                    output: vec![output],
                    value: ValueMutation::Set(total),
                    timers: Vec::new(),
                })
            })
            .collect()
    }
}

fn output_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("account", DataType::Utf8, false),
        Field::new("total", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

fn native_end_to_end(
    criterion: &mut Criterion,
    name: &str,
    row_count: usize,
    distinct_keys: usize,
) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (db, source, mut portal, batch) = runtime.block_on(async {
        let db = LaminarDB::open().unwrap();
        db.execute(
            "CREATE SOURCE events (account VARCHAR NOT NULL, amount BIGINT NOT NULL, \
             ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND)",
        )
        .await
        .unwrap();
        let source = db.source_untyped("events").unwrap();
        let output_schema = output_schema();
        db.register_native_process_function(
            "activity",
            "events",
            ProcessFunctionDescriptor {
                runtime: laminar_db::process_function::ProcessRuntime::NativeRust,
                version: 1,
                function_id: "running_total".into(),
                pipeline_state_id: "native_latency_v1".into(),
                implementation_digest: "a".repeat(64),
                python_environment: None,
                input_schema: source.schema().clone(),
                output_schema: Arc::clone(&output_schema),
                key_columns: vec!["account".into()],
                event_time_column: "ts".into(),
                output_event_time_column: "ts".into(),
                value_state_name: "total".into(),
                timer_names: Vec::new(),
                limits: ProcessFunctionLimits::default(),
            },
            Arc::new(RunningTotal { output_schema }),
        )
        .await
        .unwrap();
        db.start().await.unwrap();
        let portal = db
            .open_subscription("activity", None, SubscribeStart::Tail)
            .await
            .unwrap();
        let keys = (0..row_count)
            .map(|row| format!("account-{}", row % distinct_keys))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            source.schema().clone(),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int64Array::from(vec![1; row_count])),
                Arc::new(TimestampMicrosecondArray::from(vec![1_000_000; row_count])),
            ],
        )
        .unwrap();
        (db, source, portal, batch)
    });

    criterion.bench_function(name, |bench| {
        bench.iter(|| {
            runtime.block_on(async {
                source.push_arrow(batch.clone()).unwrap();
                let mut received = 0;
                while received < row_count {
                    match portal.next_frame().await {
                        Some(PortalFrame::Batch { batch, .. }) => {
                            received += batch.num_rows();
                        }
                        Some(PortalFrame::Barrier { .. }) => {}
                        other => panic!("process benchmark output unavailable: {other:?}"),
                    }
                }
                assert_eq!(received, row_count);
                black_box(received)
            })
        });
    });
    runtime.block_on(db.shutdown()).unwrap();
}

fn native_one_row_end_to_end(criterion: &mut Criterion) {
    native_end_to_end(criterion, "native_process_one_row_end_to_end", 1, 1);
}

fn native_batch_end_to_end(criterion: &mut Criterion) {
    native_end_to_end(criterion, "native_process_64_distinct_keys", 64, 64);
    native_end_to_end(criterion, "native_process_64_same_key", 64, 1);
}

fn native_handler_only(criterion: &mut Criterion) {
    let schema = output_schema();
    let handler = RunningTotal {
        output_schema: Arc::clone(&schema),
    };
    for (name, row_count) in [
        ("native_handler_one_row", 1),
        ("native_handler_64_rows", 64),
    ] {
        let keys = (0..row_count)
            .map(|row| format!("account-{row}"))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(keys.clone())),
                Arc::new(Int64Array::from(vec![1; row_count])),
                Arc::new(TimestampMicrosecondArray::from(vec![1_000_000; row_count])),
            ],
        )
        .unwrap();
        let activations = keys
            .into_iter()
            .enumerate()
            .map(|(row, key_text)| ProcessActivation {
                id: row as u64,
                key: Arc::from(vec![row as u8]),
                key_text,
                event_time_us: 1_000_000,
                callback: ProcessCallback::Input(batch.slice(row, 1)),
                state: ValueState::Value(0),
            })
            .collect::<Vec<_>>();
        criterion.bench_function(name, |bench| {
            bench.iter(|| black_box(handler.invoke(black_box(&activations)).unwrap()))
        });
    }
}

criterion_group!(
    benches,
    native_one_row_end_to_end,
    native_batch_end_to_end,
    native_handler_only
);
criterion_main!(benches);

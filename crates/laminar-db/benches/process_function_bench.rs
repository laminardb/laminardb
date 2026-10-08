use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use criterion::{criterion_group, criterion_main, Criterion};
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{
    DeliveryGuarantee, SourceBatch, SourceConnector, SourceConsistency, SourceContract,
    SourceInputMode, SourcePosition, SourceReplayOrder, SourceRowPositionCapability,
    SourceRowPositions, SourceStart, SourceTopology,
};
use laminar_connectors::error::ConnectorError;
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
                determinism: laminar_db::process_function::ProcessDeterminism::Undeclared,
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
    #[cfg(feature = "benchmark-internals")]
    native_operator_routing(criterion);
}

#[cfg(feature = "benchmark-internals")]
fn native_operator_routing(criterion: &mut Criterion) {
    use laminar_db::process_function::benchmark::{
        NativeProcessBenchmark, NativeProcessBenchmarkMode,
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let output_schema = output_schema();
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("account", DataType::Utf8, false),
        Field::new("amount", DataType::Int64, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]));
    for (path, mode) in [
        ("local", NativeProcessBenchmarkMode::Local),
        ("single_owner", NativeProcessBenchmarkMode::SingleOwner),
        ("two_owners", NativeProcessBenchmarkMode::TwoOwners),
    ] {
        for (shape, rows, keys) in [
            ("one_row", 1, 1),
            ("64_distinct", 64, 64),
            ("64_same_key", 64, 1),
        ] {
            let descriptor = ProcessFunctionDescriptor {
                runtime: laminar_db::process_function::ProcessRuntime::NativeRust,
                version: 1,
                function_id: "running_total".into(),
                pipeline_state_id: "routing_latency_v1".into(),
                implementation_digest: "a".repeat(64),
                python_environment: None,
                determinism: laminar_db::process_function::ProcessDeterminism::Undeclared,
                input_schema: Arc::clone(&input_schema),
                output_schema: Arc::clone(&output_schema),
                key_columns: vec!["account".into()],
                event_time_column: "ts".into(),
                output_event_time_column: "ts".into(),
                value_state_name: "total".into(),
                timer_names: Vec::new(),
                limits: ProcessFunctionLimits::default(),
            };
            let mut fixture = runtime
                .block_on(NativeProcessBenchmark::new(
                    descriptor,
                    Arc::new(RunningTotal {
                        output_schema: Arc::clone(&output_schema),
                    }),
                    mode,
                ))
                .unwrap();
            let batch = RecordBatch::try_new(
                Arc::clone(&input_schema),
                vec![
                    Arc::new(StringArray::from(
                        (0..rows)
                            .map(|row| format!("account-{}", row % keys))
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(vec![1; rows])),
                    Arc::new(TimestampMicrosecondArray::from(vec![1_000_000; rows])),
                ],
            )
            .unwrap();
            criterion.bench_function(&format!("native_operator_{path}_{shape}"), |bench| {
                bench
                    .iter(|| black_box(runtime.block_on(fixture.step(black_box(&batch))).unwrap()));
            });
        }
    }
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

struct ReplayBatchSource {
    records: RecordBatch,
    available: Arc<AtomicU64>,
    wake: Arc<tokio::sync::Notify>,
    cursor: u64,
}

#[async_trait::async_trait]
impl SourceConnector for ReplayBatchSource {
    fn contract(&self, _: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Splittable,
            SourceInputMode::AppendOnly,
        )
        .with_row_positions(SourceRowPositionCapability::OrderedDeterministic)
        .with_replay_order(SourceReplayOrder::SingleChannelFixedBatches))
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        self.cursor = match request.into_parts().1 {
            SourcePosition::Initial => 0,
            SourcePosition::Resume { checkpoint, .. } => checkpoint
                .get_offset("cursor")
                .and_then(|cursor| cursor.parse().ok())
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError("invalid benchmark replay cursor".into())
                })?,
            SourcePosition::Initialized { .. } => {
                return Err(ConnectorError::ConfigurationError(
                    "benchmark source has no topology initialization".into(),
                ))
            }
        };
        Ok(())
    }

    async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        use arrow::array::{BinaryArray, UInt32Array};

        if self.cursor >= self.available.load(Ordering::Acquire) {
            return Ok(None);
        }
        let order = self.cursor.to_be_bytes();
        let positions = SourceRowPositions::try_new(
            BinaryArray::from_vec(vec![b"ordered"; 64]),
            BinaryArray::from_vec(vec![order.as_slice(); 64]),
            UInt32Array::from((0..64).collect::<Vec<_>>()),
        )?;
        self.cursor = self.cursor.checked_add(1).ok_or_else(|| {
            ConnectorError::ConfigurationError("benchmark cursor exhausted".into())
        })?;
        Ok(Some(
            SourceBatch::positioned(self.records.clone(), positions)?
                .with_checkpoint(self.checkpoint()),
        ))
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.records.schema()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset("cursor", self.cursor.to_string());
        checkpoint
            .set_input_channels(vec![b"ordered".to_vec()])
            .unwrap();
        checkpoint
    }

    fn data_ready_notify(&self) -> Option<Arc<tokio::sync::Notify>> {
        Some(Arc::clone(&self.wake))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

fn native_fixed_replay_cuts(criterion: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let available = Arc::new(AtomicU64::new(0));
    let wake = Arc::new(tokio::sync::Notify::new());
    let (db, mut portal) = runtime.block_on(async {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("account", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]));
        let keys = (0..64)
            .map(|row| format!("account-{row}"))
            .collect::<Vec<_>>();
        let records = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![
                Arc::new(StringArray::from(keys)),
                Arc::new(Int64Array::from(vec![1; 64])),
                Arc::new(TimestampMicrosecondArray::from(vec![1_000_000; 64])),
            ],
        )
        .unwrap();
        let produced = Arc::clone(&available);
        let notified = Arc::clone(&wake);
        let db = LaminarDB::builder()
            .storage_dir(directory.path())
            .buffer_size(1)
            .checkpoint(laminar_core::streaming::StreamCheckpointConfig {
                interval_ms: None,
                ..Default::default()
            })
            .delivery_guarantee(DeliveryGuarantee::AtLeastOnce)
            .register_connector(move |registry| {
                registry.register_source(
                    "replay-cut-bench",
                    ConnectorInfo {
                        schema_capabilities:
                            laminar_connectors::schema::resolution::SchemaCapabilities::declared(
                                false,
                            ),
                        name: "replay-cut-bench".into(),
                        display_name: "Fixed replay batch benchmark".into(),
                        version: "1".into(),
                        is_source: true,
                        is_sink: false,
                        config_keys: Vec::new(),
                    },
                    Arc::new(move |_| {
                        Ok(Box::new(ReplayBatchSource {
                            records: records.clone(),
                            available: Arc::clone(&produced),
                            wake: Arc::clone(&notified),
                            cursor: 0,
                        }))
                    }),
                )
            })
            .build()
            .await
            .unwrap();
        db.execute(
            "CREATE SOURCE events (account VARCHAR NOT NULL, amount BIGINT NOT NULL, \
             ts TIMESTAMP NOT NULL, WATERMARK FOR ts AS ts - INTERVAL '0' SECOND) \
             FROM \"replay-cut-bench\"",
        )
        .await
        .unwrap();
        let output_schema = output_schema();
        db.register_native_process_function(
            "activity",
            "events",
            ProcessFunctionDescriptor {
                runtime: laminar_db::process_function::ProcessRuntime::NativeRust,
                version: 1,
                function_id: "running_total".into(),
                pipeline_state_id: "source_cut_latency_v1".into(),
                implementation_digest: "a".repeat(64),
                python_environment: None,
                determinism: laminar_db::process_function::ProcessDeterminism::Undeclared,
                input_schema,
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
        (db, portal)
    });
    criterion.bench_function("native_process_fixed_cuts_64_distinct_keys", |bench| {
        bench.iter(|| {
            runtime.block_on(async {
                available.fetch_add(1, Ordering::Release);
                wake.notify_one();
                let mut received = 0;
                while received < 64 {
                    match portal.next_frame().await {
                        Some(PortalFrame::Batch { batch, .. }) => received += batch.num_rows(),
                        Some(PortalFrame::Barrier { .. }) => {}
                        other => panic!("replay cut benchmark output unavailable: {other:?}"),
                    }
                }
                assert_eq!(received, 64);
                black_box(received)
            })
        })
    });
    runtime.block_on(db.shutdown()).unwrap();
}

criterion_group!(
    benches,
    native_one_row_end_to_end,
    native_batch_end_to_end,
    native_handler_only,
    native_fixed_replay_cuts
);
criterion_main!(benches);

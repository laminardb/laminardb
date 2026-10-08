use super::*;
use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};

pub(in super::super) type ActivityRow = (String, String, i64, bool, i64);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(in super::super) struct Callback {
    pub id: u64,
    pub key: String,
    pub timestamp: i64,
    pub timer: bool,
    pub state: ValueState,
}

pub(in super::super) fn descriptor() -> ProcessFunctionDescriptor {
    let timestamp = DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None);
    ProcessFunctionDescriptor {
        version: 1,
        runtime: ProcessRuntime::NativeRust,
        function_id: "cluster_activity_v1".into(),
        pipeline_state_id: "cluster_activity_state_v1".into(),
        implementation_digest: "c".repeat(64),
        python_environment: None,
        determinism: laminar_db::process_function::ProcessDeterminism::Undeclared,
        input_schema: Arc::new(Schema::new(vec![
            Field::new("account", DataType::Utf8, false),
            Field::new("amount", DataType::Int64, false),
            Field::new("ts", timestamp.clone(), false),
        ])),
        output_schema: Arc::new(Schema::new(vec![
            Field::new("account", DataType::Utf8, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("total", DataType::Int64, false),
            Field::new("crossed", DataType::Boolean, false),
            Field::new("ts", timestamp, false),
        ])),
        key_columns: vec!["account".into()],
        event_time_column: "ts".into(),
        output_event_time_column: "ts".into(),
        value_state_name: "total".into(),
        timer_names: vec!["inactive".into()],
        limits: ProcessFunctionLimits {
            max_keys: 8,
            max_timers: 8,
            max_state_bytes: 1024 * 1024,
            ..ProcessFunctionLimits::default()
        },
    }
}

pub(in super::super) struct Activity(pub Arc<ReplayProbe>);

impl NativeProcessFunction for Activity {
    fn invoke(
        &self,
        activations: &[ProcessActivation],
    ) -> Result<Vec<ProcessActivationResult>, laminar_db::DbError> {
        let mut callbacks = self.0.callbacks.lock();
        if callbacks.len() + activations.len() > 64 {
            return Err(laminar_db::DbError::PipelineTerminal(
                "fixture callback bound exceeded".into(),
            ));
        }
        let mut results = Vec::with_capacity(activations.len());
        for activation in activations {
            let previous = match activation.state {
                ValueState::Value(value) => value,
                ValueState::Absent | ValueState::Null => 0,
            };
            let timer = matches!(activation.callback, ProcessCallback::Timer { .. });
            callbacks.push(Callback {
                id: activation.id,
                key: activation.key_text.clone(),
                timestamp: activation.event_time_us,
                timer,
                state: activation.state,
            });
            let amount = match &activation.callback {
                ProcessCallback::Input(batch) => batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        laminar_db::DbError::PipelineTerminal("fixture amount type".into())
                    })?
                    .value(0),
                ProcessCallback::Timer { .. } => 0,
            };
            let total = previous.checked_add(amount).ok_or_else(|| {
                laminar_db::DbError::PipelineTerminal("fixture total overflow".into())
            })?;
            let output = RecordBatch::try_new(
                descriptor().output_schema,
                vec![
                    Arc::new(StringArray::from(vec![activation.key_text.as_str()])),
                    Arc::new(StringArray::from(vec![if timer {
                        "inactive"
                    } else {
                        "running"
                    }])),
                    Arc::new(Int64Array::from(vec![total])),
                    Arc::new(BooleanArray::from(vec![
                        !timer && previous < 100 && total >= 100,
                    ])),
                    Arc::new(TimestampMicrosecondArray::from(vec![
                        activation.event_time_us,
                    ])),
                ],
            )
            .map_err(|error| laminar_db::DbError::PipelineTerminal(error.to_string()))?;
            results.push(ProcessActivationResult {
                activation_id: activation.id,
                output: vec![output],
                value: if timer {
                    ValueMutation::Unchanged
                } else {
                    ValueMutation::Set(total)
                },
                timers: if timer {
                    Vec::new()
                } else {
                    vec![TimerOperation::Set {
                        name: "inactive".into(),
                        at_us: activation.event_time_us + 10_000,
                    }]
                },
            });
        }
        Ok(results)
    }
}

pub(super) fn rows(batch: &RecordBatch) -> Result<Vec<ActivityRow>, ConnectorError> {
    let string = |index| {
        batch
            .column(index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| ConnectorError::SchemaMismatch("fixture string type".into()))
    };
    let accounts = string(0)?;
    let kinds = string(1)?;
    let totals = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| ConnectorError::SchemaMismatch("fixture total type".into()))?;
    let crossed = batch
        .column(3)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| ConnectorError::SchemaMismatch("fixture threshold type".into()))?;
    let times = batch
        .column(4)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| ConnectorError::SchemaMismatch("fixture timestamp type".into()))?;
    Ok((0..batch.num_rows())
        .map(|row| {
            (
                accounts.value(row).into(),
                kinds.value(row).into(),
                totals.value(row),
                crossed.value(row),
                times.value(row),
            )
        })
        .collect())
}

use super::*;

use arrow_array::{
    Array as _, BinaryArray, Int64Array, RecordBatch, TimestampMicrosecondArray, UInt32Array,
};
use laminar_connectors::connector::{
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, SourcePosition,
    SourceRowPositionCapability, SourceRowPositions, WriteResult,
};
use laminar_core::state::PartitionKeyCodecV1;

#[derive(Default)]
pub(super) struct ReplayProbe {
    pub prefix: AtomicU64,
    pub hold: AtomicBool,
    pub polls: AtomicU64,
    pub starts: parking_lot::Mutex<Vec<Resume>>,
    pub output: parking_lot::Mutex<BTreeMap<i64, i64>>,
    pub callbacks: parking_lot::Mutex<Vec<process::Callback>>,
    pub activity: parking_lot::Mutex<Vec<process::ActivityRow>>,
}

pub(super) fn keys() -> Result<[i64; 2]> {
    let codec = PartitionKeyCodecV1::try_new([DataType::Int64])?;
    let values = Arc::new(Int64Array::from_iter_values(0..64));
    let rows = codec.encode_columns(&[values])?;
    let mut keys = [None; 2];
    for (value, row) in (0..64).zip(rows.iter()) {
        let vnode = PartitionKeyCodecV1::vnode_for_encoded(
            row.as_ref(),
            std::num::NonZeroU32::new(2).unwrap(),
        );
        keys[usize::try_from(vnode)?].get_or_insert(value);
    }
    match keys {
        [Some(left), Some(right)] => Ok([left, right]),
        _ => Err(anyhow!("fixture did not cover both vnodes")),
    }
}

fn source_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("account", DataType::Int64, true),
        Field::new("amount", DataType::Int64, true),
        Field::new(
            "ts",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            true,
        ),
    ]))
}

struct ReplaySource {
    probe: Arc<ReplayProbe>,
    keys: [i64; 2],
    owned: Vec<u8>,
    assignment: std::num::NonZeroU64,
    checkpoint: SourceCheckpoint,
}

#[async_trait::async_trait]
impl SourceConnector for ReplaySource {
    fn contract(&self, _: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Splittable,
            SourceInputMode::AppendOnly,
        )
        .with_row_positions(SourceRowPositionCapability::OrderedDeterministic))
    }

    fn set_vnode_assignment(
        &mut self,
        source: &str,
        registry: Arc<VnodeRegistry>,
        node: StateNodeId,
    ) -> Result<(), ConnectorError> {
        if source != "recovery_input" {
            return Err(ConnectorError::ConfigurationError(
                "unknown fixture source".into(),
            ));
        }
        let assignment = registry.versioned_snapshot();
        self.assignment = std::num::NonZeroU64::new(assignment.version()).ok_or_else(|| {
            ConnectorError::ConfigurationError("fixture assignment is absent".into())
        })?;
        self.owned = (0..2_u8)
            .filter(|partition| assignment.owners()[usize::from(*partition)] == node)
            .collect();
        Ok(())
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (_, position, _) = request.into_parts();
        let (id, recovered) = match position {
            SourcePosition::Initial => (0, SourceCheckpoint::new()),
            SourcePosition::Resume {
                attempt,
                checkpoint,
            } => (attempt.checkpoint_id, checkpoint),
            SourcePosition::Initialized { .. } => {
                return Err(ConnectorError::ConfigurationError(
                    "fixture does not certify topology initialization".into(),
                ))
            }
        };
        let mut checkpoint = SourceCheckpoint::new();
        for partition in &self.owned {
            let key = format!("partition-{partition}");
            let offset = match (id, recovered.get_offset(&key)) {
                (0, None) => "0",
                (_, Some(value)) if value.parse::<u64>().is_ok_and(|cursor| cursor <= 4) => value,
                _ => {
                    return Err(ConnectorError::ConfigurationError(
                        "fixture resume cursor is missing or invalid".into(),
                    ))
                }
            };
            checkpoint.set_offset(key, offset);
        }
        checkpoint.set_metadata("fixture", "bounded-replay-v1");
        checkpoint.set_input_channels(
            self.owned
                .iter()
                .map(|partition| vec![1, *partition])
                .collect::<Vec<_>>(),
        )?;
        checkpoint.bind_assignment_version(self.assignment);
        self.checkpoint = checkpoint;
        {
            let mut starts = self.probe.starts.lock();
            if starts.len() >= 8 {
                return Err(ConnectorError::ConfigurationError(
                    "fixture exceeded its restart bound".into(),
                ));
            }
            starts.push(Resume {
                checkpoint_id: id,
                assignment: self.assignment.get(),
                offsets: self
                    .checkpoint
                    .offsets()
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                channels: self.checkpoint.input_channels().unwrap_or(&[]).to_vec(),
            });
        }
        tokio::time::timeout(DEADLINE, async {
            while self.probe.hold.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| ConnectorError::ConfigurationError("fixture recovery hold expired".into()))
    }

    async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        self.probe.polls.fetch_add(1, Ordering::Relaxed);
        let prefix = self.probe.prefix.load(Ordering::Acquire);
        for &partition in &self.owned {
            let key = format!("partition-{partition}");
            let offset = self
                .checkpoint
                .get_offset(&key)
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError("invalid fixture cursor".into())
                })?;
            if offset >= prefix {
                continue;
            }
            let next = offset + 1;
            let batch = RecordBatch::try_new(
                source_schema(),
                vec![
                    Arc::new(Int64Array::from(vec![
                        self.keys[usize::from(1 - partition)],
                    ])),
                    Arc::new(Int64Array::from(vec![
                        i64::try_from(next * 10).unwrap() + i64::from(partition),
                    ])),
                    Arc::new(TimestampMicrosecondArray::from(vec![
                        1_000_000 + i64::try_from(next).unwrap() * 1_000,
                    ])),
                ],
            )
            .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?;
            let order = offset.to_be_bytes();
            let position = SourceRowPositions::try_new(
                BinaryArray::from(vec![&[1, partition][..]]),
                BinaryArray::from(vec![&order[..]]),
                UInt32Array::from(vec![0]),
            )?;
            self.checkpoint.set_offset(key, next.to_string());
            return Ok(Some(
                SourceBatch::positioned(batch, position)?.with_checkpoint(self.checkpoint.clone()),
            ));
        }
        Ok(None)
    }

    fn schema(&self) -> SchemaRef {
        source_schema()
    }
    fn checkpoint(&self) -> SourceCheckpoint {
        self.checkpoint.clone()
    }
    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

struct ObservedSink {
    probe: Arc<ReplayProbe>,
    path: PathBuf,
    file: Option<std::fs::File>,
}

#[async_trait::async_trait]
impl SinkConnector for ObservedSink {
    fn suggested_write_timeout(&self) -> Duration {
        DEADLINE
    }
    fn contract(&self, _: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        Ok(SinkContract::new(
            SinkConsistency::DurableAtLeastOnce,
            SinkTopology::MultiWriter,
            SinkInputMode::FullChangelog,
        ))
    }
    async fn open(&mut self, _: &ConnectorConfig) -> Result<(), ConnectorError> {
        self.file = Some(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)?,
        );
        Ok(())
    }
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("account", DataType::Int64, true),
            Field::new("total", DataType::Int64, true),
        ]))
    }
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        let column = |name| {
            batch
                .column_by_name(name)
                .and_then(|array| array.as_any().downcast_ref::<Int64Array>())
                .ok_or_else(|| {
                    ConnectorError::SchemaMismatch(format!("missing fixture column {name}"))
                })
        };
        let accounts = column("account")?;
        let totals = column("total")?;
        let weights = batch
            .column_by_name("__weight")
            .and_then(|array| array.as_any().downcast_ref::<Int64Array>());
        let mut rows = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            if accounts.is_null(row) || totals.is_null(row) {
                return Err(ConnectorError::SchemaMismatch(
                    "null fixture aggregate".into(),
                ));
            }
            rows.push((
                accounts.value(row),
                totals.value(row),
                weights.map_or(1, |weights| weights.value(row)),
            ));
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| ConnectorError::ConfigurationError("fixture sink is closed".into()))?;
        if file.metadata()?.len() > MAX_MESSAGE_BYTES {
            return Err(ConnectorError::ConfigurationError(
                "fixture sink exceeded its byte bound".into(),
            ));
        }
        use std::io::Write as _;
        serde_json::to_writer(&mut *file, &rows)
            .map_err(|error| ConnectorError::ConfigurationError(error.to_string()))?;
        file.write_all(b"\n")?;
        // The at-least-once contract requires durable acknowledgement, even for this probe.
        file.sync_all()?;
        let mut output = self.probe.output.lock();
        for (key, total, weight) in rows {
            if weight > 0 {
                output.insert(key, total);
            } else if output.get(&key) == Some(&total) {
                output.remove(&key);
            }
        }
        if output.len() > 2 {
            return Err(ConnectorError::ConfigurationError(
                "fixture output exceeded its key bound".into(),
            ));
        }
        Ok(WriteResult::new(batch.num_rows(), 0))
    }
    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.file.take();
        Ok(())
    }
}

pub(super) fn register(
    probe: Arc<ReplayProbe>,
    path: PathBuf,
    runtime: Runtime,
) -> impl FnOnce(&laminar_connectors::registry::ConnectorRegistry) -> Result<(), ConnectorError> {
    move |registry| {
        if runtime != Runtime::Aggregate {
            return process::register(probe, path)(registry);
        }
        let keys = keys().map_err(|error| ConnectorError::ConfigurationError(error.to_string()))?;
        let source_probe = Arc::clone(&probe);
        registry.register_source(
            "recovery-cut-probe",
            ConnectorInfo {
                schema_capabilities:
                    laminar_connectors::schema::resolution::SchemaCapabilities::declared(false),
                name: "recovery-cut-probe".into(),
                display_name: "Recovery cut probe".into(),
                version: "1".into(),
                is_source: true,
                is_sink: false,
                config_keys: Vec::new(),
            },
            Arc::new(move |_| {
                Ok(Box::new(ReplaySource {
                    probe: Arc::clone(&source_probe),
                    keys,
                    owned: Vec::new(),
                    assignment: std::num::NonZeroU64::new(1).unwrap(),
                    checkpoint: SourceCheckpoint::new(),
                }))
            }),
        )?;
        registry.register_sink(
            "recovery-output-probe",
            ConnectorInfo {
                schema_capabilities:
                    laminar_connectors::schema::resolution::SchemaCapabilities::declared(true),
                name: "recovery-output-probe".into(),
                display_name: "Recovery output probe".into(),
                version: "1".into(),
                is_source: false,
                is_sink: true,
                config_keys: Vec::new(),
            },
            Arc::new(move |_, _| {
                Ok(Box::new(ObservedSink {
                    probe: Arc::clone(&probe),
                    path: path.clone(),
                    file: None,
                }))
            }),
        )
    }
}

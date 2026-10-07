use super::*;
use arrow_array::{
    BinaryArray, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray, UInt32Array,
};
use laminar_connectors::connector::{
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, SourcePosition,
    SourceReplayOrder, SourceRowPositionCapability, SourceRowPositions, WriteResult,
};

pub(super) fn key(vnode: u32, label: &str) -> String {
    (0..1000)
        .map(|index| format!("{label}-{index}"))
        .find(|key| {
            let batch = input(key, 0, 100_000);
            laminar_core::shuffle::row_vnodes(&batch, &[0], 2).unwrap()[0] == vnode
        })
        .expect("bounded fixture must cover both vnodes")
}

fn input(key: &str, amount: i64, timestamp: i64) -> RecordBatch {
    RecordBatch::try_new(
        descriptor().input_schema,
        vec![
            Arc::new(StringArray::from(vec![key])),
            Arc::new(Int64Array::from(vec![amount])),
            Arc::new(TimestampMicrosecondArray::from(vec![timestamp])),
        ],
    )
    .unwrap()
}

fn script() -> Vec<RecordBatch> {
    let threshold = key(0, "a");
    let peer = key(1, "b");
    vec![
        input(&threshold, 60, 100_000),
        input(&peer, 7, 105_000),
        input(&threshold, 50, 108_000),
        input(&peer, 13, 107_000),
        input(&key(0, "c"), 1, 125_000),
        input(&key(1, "d"), 2, 145_000),
        input(&key(0, "e"), 0, 165_000),
    ]
}

struct ReplaySource {
    probe: Arc<ReplayProbe>,
    batches: Vec<RecordBatch>,
    owned: bool,
    assignment: std::num::NonZeroU64,
    checkpoint: SourceCheckpoint,
    cursor: u64,
}

#[async_trait::async_trait]
impl SourceConnector for ReplaySource {
    fn contract(&self, _: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(SourceContract::new(
            SourceConsistency::Replayable,
            SourceTopology::Splittable,
            SourceInputMode::AppendOnly,
        )
        .with_row_positions(SourceRowPositionCapability::OrderedDeterministic)
        .with_replay_order(SourceReplayOrder::SingleChannelFixedBatches))
    }

    fn set_vnode_assignment(
        &mut self,
        _: &str,
        registry: Arc<VnodeRegistry>,
        node: StateNodeId,
    ) -> Result<(), ConnectorError> {
        let snapshot = registry.versioned_snapshot();
        self.assignment = std::num::NonZeroU64::new(snapshot.version()).ok_or_else(|| {
            ConnectorError::ConfigurationError("missing fixture assignment".into())
        })?;
        self.owned = snapshot.owners()[0] == node;
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
                    "fixture topology initialization is unsupported".into(),
                ))
            }
        };
        if id != 0
            && self.owned
            && recovered.input_channels() != Some([CHANNEL.to_vec()].as_slice())
        {
            return Err(ConnectorError::ConfigurationError(
                "fixture replay changed the global physical channel".into(),
            ));
        }
        self.cursor = if id == 0 || !self.owned {
            0
        } else {
            recovered
                .get_offset("cursor")
                .and_then(|cursor| cursor.parse().ok())
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError("missing fixture replay cursor".into())
                })?
        };
        if self.cursor > 7 {
            return Err(ConnectorError::ConfigurationError(
                "invalid fixture cursor".into(),
            ));
        }
        self.checkpoint = SourceCheckpoint::new();
        self.checkpoint.bind_assignment_version(self.assignment);
        self.checkpoint.set_input_channels(if self.owned {
            vec![CHANNEL.to_vec()]
        } else {
            Vec::new()
        })?;
        if self.owned {
            self.checkpoint
                .set_offset("cursor", self.cursor.to_string());
        }
        {
            let mut starts = self.probe.starts.lock();
            if starts.len() >= 8 {
                return Err(ConnectorError::ConfigurationError(
                    "fixture restart bound exceeded".into(),
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
        .map_err(|_| ConnectorError::ConfigurationError("fixture Start hold expired".into()))
    }

    async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        self.probe.polls.fetch_add(1, Ordering::Relaxed);
        if !self.owned || self.cursor >= self.probe.prefix.load(Ordering::Acquire) {
            return Ok(None);
        }
        let index = usize::try_from(self.cursor)
            .map_err(|error| ConnectorError::ConfigurationError(error.to_string()))?;
        let batch =
            self.batches.get(index).cloned().ok_or_else(|| {
                ConnectorError::ConfigurationError("fixture source exhausted".into())
            })?;
        let order = self.cursor.to_be_bytes();
        let positions = SourceRowPositions::try_new(
            BinaryArray::from(vec![CHANNEL]),
            BinaryArray::from(vec![order.as_slice()]),
            UInt32Array::from(vec![0]),
        )?;
        self.cursor += 1;
        self.checkpoint
            .set_offset("cursor", self.cursor.to_string());
        Ok(Some(
            SourceBatch::positioned(batch, positions)?.with_checkpoint(self.checkpoint.clone()),
        ))
    }
    fn schema(&self) -> SchemaRef {
        descriptor().input_schema
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
            SinkInputMode::AppendOnly,
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
        descriptor().output_schema
    }
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        let rows = activity::rows(batch)?;
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| ConnectorError::ConfigurationError("fixture sink is closed".into()))?;
        if file.metadata()?.len() > MAX_MESSAGE_BYTES {
            return Err(ConnectorError::ConfigurationError(
                "fixture output byte bound exceeded".into(),
            ));
        }
        use std::io::Write as _;
        serde_json::to_writer(&mut *file, &rows)
            .map_err(|error| ConnectorError::ConfigurationError(error.to_string()))?;
        file.write_all(b"\n")?;
        file.sync_all()?;
        let mut activity = self.probe.activity.lock();
        if activity.len() + rows.len() > 64 {
            return Err(ConnectorError::ConfigurationError(
                "fixture output row bound exceeded".into(),
            ));
        }
        activity.extend(rows);
        Ok(WriteResult::new(batch.num_rows(), 0))
    }
    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.file.take();
        Ok(())
    }
}

pub(in super::super) fn register(
    probe: Arc<ReplayProbe>,
    path: PathBuf,
) -> impl FnOnce(&laminar_connectors::registry::ConnectorRegistry) -> Result<(), ConnectorError> {
    move |registry| {
        let source_probe = Arc::clone(&probe);
        let info = |name: &str, source| ConnectorInfo {
            name: name.into(),
            display_name: name.into(),
            version: "1".into(),
            is_source: source,
            is_sink: !source,
            config_keys: Vec::new(),
        };
        registry.register_source(
            "recovery-cut-probe",
            info("recovery-cut-probe", true),
            Arc::new(move |_| {
                Ok(Box::new(ReplaySource {
                    probe: Arc::clone(&source_probe),
                    batches: script(),
                    owned: false,
                    assignment: std::num::NonZeroU64::new(1).unwrap(),
                    checkpoint: SourceCheckpoint::new(),
                    cursor: 0,
                }))
            }),
        )?;
        registry.register_sink(
            "recovery-output-probe",
            info("recovery-output-probe", false),
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

use super::*;
use arrow::array::{BinaryArray, UInt32Array};
use async_trait::async_trait;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceInputMode,
    SourcePosition, SourceReplayOrder, SourceRowPositionCapability, SourceRowPositions,
    SourceStart, SourceTopology,
};
use laminar_connectors::error::ConnectorError;

pub(super) const CHANNEL: &[u8] = b"global-ordered";
pub(super) const SOURCE: &str = "process-cluster-replay";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Resume {
    pub cursor: u64,
    pub assignment: u64,
    pub channels: Vec<Vec<u8>>,
}

#[derive(Default)]
pub(super) struct SourceProbe {
    pub available: AtomicU64,
    pub hold: AtomicBool,
    pub polls: AtomicU64,
    pub starts: parking_lot::Mutex<Vec<Resume>>,
}

pub(super) struct ReplaySource {
    probe: Arc<SourceProbe>,
    batches: Arc<[RecordBatch]>,
    owned: bool,
    assignment: u64,
    cursor: u64,
}

#[async_trait]
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
        source: &str,
        registry: Arc<VnodeRegistry>,
        node: StateNodeId,
    ) -> Result<(), ConnectorError> {
        if source != "events" {
            return Err(ConnectorError::ConfigurationError(
                "unknown replay source".into(),
            ));
        }
        let assignment = registry.versioned_snapshot();
        self.assignment = assignment.version();
        // One global physical channel follows vnode zero; other owners have an empty inventory.
        self.owned = assignment.owners()[0] == node;
        Ok(())
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (_, position, _) = request.into_parts();
        let channels = match position {
            SourcePosition::Initial => Vec::new(),
            SourcePosition::Resume { checkpoint, .. } => {
                self.cursor = checkpoint
                    .get_offset("cursor")
                    .and_then(|value| value.parse().ok())
                    .ok_or_else(|| {
                        ConnectorError::ConfigurationError("missing replay cursor".into())
                    })?;
                checkpoint.input_channels().unwrap_or(&[]).to_vec()
            }
            SourcePosition::Initialized { .. } => {
                return Err(ConnectorError::ConfigurationError(
                    "unexpected topology initialization".into(),
                ));
            }
        };
        if !usize::try_from(self.cursor).is_ok_and(|cursor| cursor <= self.batches.len()) {
            return Err(ConnectorError::ConfigurationError(
                "invalid replay cursor".into(),
            ));
        }
        {
            let mut starts = self.probe.starts.lock();
            assert!(starts.len() < 8);
            starts.push(Resume {
                cursor: self.cursor,
                assignment: self.assignment,
                channels,
            });
        }
        let deadline = tokio::time::Instant::now() + DEADLINE;
        while self.probe.hold.load(Ordering::Acquire) {
            if tokio::time::Instant::now() >= deadline {
                return Err(ConnectorError::ConfigurationError(
                    "recovery Start hold expired".into(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok(())
    }

    async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        self.probe.polls.fetch_add(1, Ordering::Relaxed);
        if !self.owned || self.cursor >= self.probe.available.load(Ordering::Acquire) {
            return Ok(None);
        }
        let index = usize::try_from(self.cursor)
            .map_err(|error| ConnectorError::ConfigurationError(error.to_string()))?;
        let Some(records) = self.batches.get(index).cloned() else {
            return Ok(None);
        };
        let key = self.cursor.to_be_bytes();
        let rows = records.num_rows();
        let positions = SourceRowPositions::try_new(
            BinaryArray::from_vec(vec![CHANNEL; rows]),
            BinaryArray::from_vec(vec![key.as_slice(); rows]),
            UInt32Array::from((0..u32::try_from(rows).unwrap()).collect::<Vec<_>>()),
        )?;
        self.cursor += 1;
        Ok(Some(
            SourceBatch::positioned(records, positions)?.with_checkpoint(self.checkpoint()),
        ))
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        descriptor().input_schema
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        if let Some(assignment) = std::num::NonZeroU64::new(self.assignment) {
            checkpoint.bind_assignment_version(assignment);
        }
        if self.owned {
            checkpoint.set_offset("cursor", self.cursor.to_string());
        }
        checkpoint
            .set_input_channels(if self.owned {
                vec![CHANNEL.to_vec()]
            } else {
                Vec::new()
            })
            .unwrap();
        checkpoint
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

pub(super) fn register(
    probe: Arc<SourceProbe>,
    batches: Arc<[RecordBatch]>,
) -> impl FnOnce(&laminar_connectors::registry::ConnectorRegistry) -> Result<(), ConnectorError> {
    move |registry| {
        registry.register_source(
            SOURCE,
            ConnectorInfo {
                schema_capabilities:
                    laminar_connectors::schema::resolution::SchemaCapabilities::declared(false),
                name: SOURCE.into(),
                display_name: SOURCE.into(),
                version: "1".into(),
                is_source: true,
                is_sink: false,
                config_keys: Vec::new(),
            },
            Arc::new(move |_| {
                Ok(Box::new(ReplaySource {
                    probe: Arc::clone(&probe),
                    batches: Arc::clone(&batches),
                    owned: false,
                    assignment: 0,
                    cursor: 0,
                }))
            }),
        )
    }
}

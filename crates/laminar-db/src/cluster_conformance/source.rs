//! Replayable, splittable fixture source whose partitions follow vnode ownership.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{BinaryArray, RecordBatch, UInt32Array};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceInputMode,
    SourcePosition, SourceRowPositionCapability, SourceRowPositions, SourceStart, SourceTopology,
};
use laminar_connectors::error::ConnectorError;
use laminar_connectors::registry::ConnectorRegistry;
use laminar_core::state::{NodeId, VnodeRegistry};

/// One fixture source: partition `p` is read by the owner of vnode `p % vnode_count`.
#[derive(Clone)]
pub(super) struct FixtureSource {
    pub name: &'static str,
    /// Column list and watermark clause of `CREATE SOURCE`, including parentheses.
    pub columns: &'static str,
    pub partitions: Arc<[Vec<RecordBatch>]>,
}

impl FixtureSource {
    pub fn connector_type(&self) -> String {
        format!("conformance-{}", self.name)
    }

    pub fn ddl(&self) -> String {
        format!(
            "CREATE SOURCE {} {} FROM \"{}\" ('fixture' = 'conformance')",
            self.name,
            self.columns,
            self.connector_type()
        )
    }

    pub fn schema(&self) -> SchemaRef {
        self.partitions
            .iter()
            .flatten()
            .next()
            .expect("fixture source has at least one batch")
            .schema()
    }

    pub fn batch_count(&self) -> u64 {
        self.partitions.iter().map(|p| p.len() as u64).sum()
    }

    pub fn all_batches(&self) -> Vec<RecordBatch> {
        self.partitions.iter().flatten().cloned().collect()
    }
}

/// Register every fixture source for one node; `polled` counts batches that node delivered, and
/// the node delivers no more than `allowance` of them.
pub(super) fn register(
    sources: Vec<FixtureSource>,
    polled: Arc<AtomicU64>,
    allowance: Arc<AtomicU64>,
) -> impl FnOnce(&ConnectorRegistry) -> Result<(), ConnectorError> + Send + 'static {
    move |registry| {
        for source in sources {
            let name = source.connector_type();
            let polled = Arc::clone(&polled);
            let allowance = Arc::clone(&allowance);
            registry.register_source(
                &name,
                ConnectorInfo {
                    schema_capabilities:
                        laminar_connectors::schema::resolution::SchemaCapabilities::declared(false),
                    name: name.clone(),
                    display_name: name.clone(),
                    version: "1".into(),
                    is_source: true,
                    is_sink: false,
                    config_keys: Vec::new(),
                },
                Arc::new(move |_| {
                    Ok(Box::new(PartitionedSource {
                        schema: source.schema(),
                        owned: (0..source.partitions.len()).collect(),
                        cursors: vec![0; source.partitions.len()],
                        partitions: Arc::clone(&source.partitions),
                        next: 0,
                        assignment: 0,
                        polled: Arc::clone(&polled),
                        allowance: Arc::clone(&allowance),
                    }))
                }),
            )?;
        }
        Ok(())
    }
}

struct PartitionedSource {
    partitions: Arc<[Vec<RecordBatch>]>,
    schema: SchemaRef,
    /// Partitions this node reads: all of them unless a cluster assignment narrows the set.
    owned: Vec<usize>,
    cursors: Vec<usize>,
    next: usize,
    assignment: u64,
    polled: Arc<AtomicU64>,
    allowance: Arc<AtomicU64>,
}

fn channel(partition: usize) -> Vec<u8> {
    format!("p{partition}").into_bytes()
}

#[async_trait]
impl SourceConnector for PartitionedSource {
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
        _: &str,
        registry: Arc<VnodeRegistry>,
        node: NodeId,
    ) -> Result<(), ConnectorError> {
        let assignment = registry.versioned_snapshot();
        let owners = assignment.owners();
        self.assignment = assignment.version();
        self.owned = (0..self.partitions.len())
            .filter(|partition| owners[partition % owners.len()] == node)
            .collect();
        Ok(())
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (_, position, _) = request.into_parts();
        match position {
            SourcePosition::Initial => Ok(()),
            SourcePosition::Resume { .. } | SourcePosition::Initialized { .. } => Err(
                ConnectorError::ConfigurationError("conformance fixture starts only fresh".into()),
            ),
        }
    }

    async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.polled.load(Ordering::Acquire) >= self.allowance.load(Ordering::Acquire) {
            return Ok(None);
        }
        for offset in 0..self.owned.len() {
            let slot = (self.next + offset) % self.owned.len();
            let partition = self.owned[slot];
            let cursor = self.cursors[partition];
            let Some(records) = self.partitions[partition].get(cursor).cloned() else {
                continue;
            };
            let rows = u32::try_from(records.num_rows())
                .map_err(|error| ConnectorError::ConfigurationError(error.to_string()))?;
            let channel = channel(partition);
            let key = (cursor as u64).to_be_bytes();
            let positions = SourceRowPositions::try_new(
                BinaryArray::from_vec(vec![channel.as_slice(); rows as usize]),
                BinaryArray::from_vec(vec![key.as_slice(); rows as usize]),
                UInt32Array::from((0..rows).collect::<Vec<_>>()),
            )?;
            self.cursors[partition] = cursor + 1;
            self.next = (slot + 1) % self.owned.len();
            self.polled.fetch_add(1, Ordering::AcqRel);
            return Ok(Some(
                SourceBatch::positioned(records, positions)?.with_checkpoint(self.checkpoint()),
            ));
        }
        Ok(None)
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        if let Some(assignment) = std::num::NonZeroU64::new(self.assignment) {
            checkpoint.bind_assignment_version(assignment);
        }
        for &partition in &self.owned {
            checkpoint.set_offset(format!("p{partition}"), self.cursors[partition].to_string());
        }
        checkpoint
            .set_input_channels(self.owned.iter().map(|&p| channel(p)).collect::<Vec<_>>())
            .expect("fixture channels are distinct");
        checkpoint
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        Ok(())
    }
}

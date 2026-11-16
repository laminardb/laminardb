//! Fixture sources: a replayable, splittable source whose partitions follow vnode ownership, and
//! a queue the engine runs on the owner of its home vnode.

use std::collections::{BTreeMap, VecDeque};
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
use parking_lot::Mutex;

/// Generator rate in generated fixtures; the rows' `ts_ms` is `seq * 1000 / rate`.
pub(super) const GENERATED_ROWS_PER_SECOND: i64 = 1_000_000;

/// How a fixture source's rows reach the database.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FixtureKind {
    /// The fixture connector: partition `p` is read by the owner of vnode `p % vnode_count`.
    Partitioned,
    /// The built-in generator with one split per partition; partition `c` holds the rows split
    /// `c` emits.
    Generated,
    /// One queue of batches, read by the owner of the source's home vnode. A delivery leaves the
    /// queue when a checkpoint covering it commits and returns to it when its reader closes.
    Queue,
}

/// One fixture source.
#[derive(Clone)]
pub(super) struct FixtureSource {
    pub name: &'static str,
    /// Column list and watermark clause of `CREATE SOURCE`, including parentheses.
    pub columns: &'static str,
    pub partitions: Arc<[Vec<RecordBatch>]>,
    pub kind: FixtureKind,
}

impl FixtureSource {
    pub fn connector_type(&self) -> String {
        format!("conformance-{}", self.name)
    }

    pub fn ddl(&self) -> String {
        if self.kind == FixtureKind::Generated {
            return format!(
                "CREATE SOURCE {} FROM GENERATOR ('splits' = '{}', 'max.rows' = '{}', \
                 'rows.per.second' = '{GENERATED_ROWS_PER_SECOND}', 'batch.max.size' = '64')",
                self.name,
                self.partitions.len(),
                self.all_batches()
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>()
            );
        }
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

    /// Batches the fixture connector delivers; the generator's are not counted.
    pub fn batch_count(&self) -> u64 {
        if self.kind == FixtureKind::Generated {
            return 0;
        }
        self.partitions.iter().map(|p| p.len() as u64).sum()
    }

    pub fn all_batches(&self) -> Vec<RecordBatch> {
        self.partitions.iter().flatten().cloned().collect()
    }
}

/// The queues of one database or cluster: every node of a cluster shares them.
pub(super) struct Queues(BTreeMap<&'static str, Arc<Mutex<Queue>>>);

impl Queues {
    pub fn new(sources: &[FixtureSource]) -> Arc<Self> {
        Arc::new(Self(
            sources
                .iter()
                .filter(|source| source.kind == FixtureKind::Queue)
                .map(|source| {
                    let batches = source.all_batches();
                    let queue = Queue {
                        ready: (0..batches.len()).collect(),
                        batches,
                        consumers: 0,
                    };
                    (source.name, Arc::new(Mutex::new(queue)))
                })
                .collect(),
        ))
    }
}

pub(super) struct Queue {
    batches: Vec<RecordBatch>,
    /// Batches no open reader holds, in delivery order.
    ready: VecDeque<usize>,
    consumers: u64,
}

/// Register every fixture source for one node; `polled` counts batches that node delivered, and
/// the node delivers no more than `allowance` of them.
pub(super) fn register(
    sources: Vec<FixtureSource>,
    polled: Arc<AtomicU64>,
    allowance: Arc<AtomicU64>,
    queues: Arc<Queues>,
) -> impl FnOnce(&ConnectorRegistry) -> Result<(), ConnectorError> + Send + 'static {
    move |registry| {
        for source in sources
            .into_iter()
            .filter(|source| source.kind != FixtureKind::Generated)
        {
            let name = source.connector_type();
            let polled = Arc::clone(&polled);
            let allowance = Arc::clone(&allowance);
            let queue = queues.0.get(source.name).cloned();
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
                    let (polled, allowance) = (Arc::clone(&polled), Arc::clone(&allowance));
                    if let Some(queue) = &queue {
                        return Ok(Box::new(QueueSource {
                            queue: Arc::clone(queue),
                            schema: source.schema(),
                            consumer: None,
                            delivered: 0,
                            unacked: VecDeque::new(),
                            polled,
                            allowance,
                        }));
                    }
                    Ok(Box::new(PartitionedSource {
                        schema: source.schema(),
                        owned: (0..source.partitions.len()).collect(),
                        cursors: vec![0; source.partitions.len()],
                        partitions: Arc::clone(&source.partitions),
                        next: 0,
                        assignment: 0,
                        polled,
                        allowance,
                    }))
                }),
            )?;
        }
        Ok(())
    }
}

/// A queue reader that acknowledges cumulatively, like a broker channel: its cursor names the
/// reader and the last delivery it handed over.
struct QueueSource {
    queue: Arc<Mutex<Queue>>,
    schema: SchemaRef,
    consumer: Option<u64>,
    /// The last tag this reader handed over.
    delivered: u64,
    /// Deliveries not yet acknowledged, as `(tag, batch)`, oldest first.
    unacked: VecDeque<(u64, usize)>,
    polled: Arc<AtomicU64>,
    allowance: Arc<AtomicU64>,
}

#[async_trait]
impl SourceConnector for QueueSource {
    fn contract(&self, _: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        Ok(
            laminar_connectors::testing::with_cluster_singleton_placement(SourceContract::new(
                SourceConsistency::CommitCoupled,
                SourceTopology::Singleton,
                SourceInputMode::AppendOnly,
            )),
        )
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        if let (_, SourcePosition::Initialized { .. }, _) = request.into_parts() {
            return Err(ConnectorError::ConfigurationError(
                "conformance queue has no sealed start".into(),
            ));
        }
        let mut queue = self.queue.lock();
        queue.consumers += 1;
        self.consumer = Some(queue.consumers);
        self.delivered = 0;
        Ok(())
    }

    async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.polled.load(Ordering::Acquire) >= self.allowance.load(Ordering::Acquire) {
            return Ok(None);
        }
        let mut queue = self.queue.lock();
        let Some(batch) = queue.ready.pop_front() else {
            return Ok(None);
        };
        self.delivered += 1;
        self.unacked.push_back((self.delivered, batch));
        self.polled.fetch_add(1, Ordering::AcqRel);
        Ok(Some(SourceBatch::new(queue.batches[batch].clone())))
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        if let Some(consumer) = self.consumer {
            checkpoint.set_offset("consumer", consumer.to_string());
            checkpoint.set_offset("tag", self.delivered.to_string());
        }
        checkpoint
    }

    async fn notify_epoch_committed(
        &mut self,
        _: u64,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        let mine = self.consumer.map(|consumer| consumer.to_string());
        if checkpoint.get_offset("consumer") != mine.as_deref() {
            return Ok(());
        }
        let tag: u64 = checkpoint
            .get_offset("tag")
            .and_then(|tag| tag.parse().ok())
            .unwrap_or(0);
        while self.unacked.front().is_some_and(|(acked, _)| *acked <= tag) {
            self.unacked.pop_front();
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        let mut queue = self.queue.lock();
        for (_, batch) in self.unacked.drain(..).rev() {
            queue.ready.push_front(batch);
        }
        self.consumer = None;
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

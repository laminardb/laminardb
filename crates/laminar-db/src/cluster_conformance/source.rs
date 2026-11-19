//! Fixture sources: a replayable, splittable source whose partitions follow vnode ownership, and
//! a queue the engine runs on the owner of its home vnode.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use arrow::array::{BinaryArray, Int64Array, RecordBatch, UInt32Array};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::{ConnectorConfig, ConnectorInfo};
use laminar_connectors::connector::{
    SourceBatch, SourceConnector, SourceConsistency, SourceContract, SourceInputMode,
    SourceMutation, SourcePosition, SourceRowPositionCapability, SourceRowPositions, SourceStart,
    SourceTopology,
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
    Queue {
        commit: QueueCommit,
        rows: QueueRows,
    },
}

/// What a queue's rows mean.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum QueueRows {
    /// Appended rows, without row positions.
    Append,
    /// Keyed upserts by the declared primary key, positioned by the batch's place in the queue;
    /// a row whose `v` is negative is a tombstone. Like a MongoDB collection incarnation, the
    /// queue's own channel identity differs from its catalog name.
    KeyedUpsert,
}

/// How a queue reader releases deliveries at a commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum QueueCommit {
    /// Every delivery up to the committed cut's tag, like a RabbitMQ channel.
    Cumulative,
    /// Every delivery since the last commit is one unit of work, like IBM MQ under syncpoint:
    /// capturing a checkpoint seals the unit, and the reader gets nothing more until a commit
    /// naming all of it.
    UnitOfWork,
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
                .filter(|source| matches!(source.kind, FixtureKind::Queue { .. }))
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
                    if let (Some(queue), FixtureKind::Queue { commit, rows }) =
                        (&queue, source.kind)
                    {
                        return Ok(Box::new(QueueSource {
                            queue: Arc::clone(queue),
                            channel: format!("{}@incarnation-1", source.name).into_bytes(),
                            schema: source.schema(),
                            commit,
                            rows,
                            consumer: None,
                            delivered: 0,
                            sealed: AtomicU64::new(0),
                            committed: 0,
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

/// A queue reader: its cursor names the reader and the last delivery it handed over.
struct QueueSource {
    queue: Arc<Mutex<Queue>>,
    /// The queue's own channel identity, which differs from its catalog name.
    channel: Vec<u8>,
    schema: SchemaRef,
    commit: QueueCommit,
    rows: QueueRows,
    consumer: Option<u64>,
    /// The last tag this reader handed over.
    delivered: u64,
    /// `delivered` at the last capture; a unit of work above `committed` is sealed.
    sealed: AtomicU64,
    /// The last tag a commit released.
    committed: u64,
    /// Deliveries not yet acknowledged, as `(tag, batch)`, oldest first.
    unacked: VecDeque<(u64, usize)>,
    polled: Arc<AtomicU64>,
    allowance: Arc<AtomicU64>,
}

#[async_trait]
impl SourceConnector for QueueSource {
    fn contract(&self, _: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        let contract = match self.rows {
            QueueRows::Append => SourceContract::new(
                SourceConsistency::CommitCoupled,
                SourceTopology::Singleton,
                SourceInputMode::AppendOnly,
            ),
            QueueRows::KeyedUpsert => SourceContract::new(
                SourceConsistency::CommitCoupled,
                SourceTopology::Singleton,
                SourceInputMode::KeyedUpsert,
            )
            .with_row_positions(SourceRowPositionCapability::OrderedDeterministic),
        };
        Ok(laminar_connectors::testing::with_cluster_singleton_placement(contract))
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
        self.sealed.store(0, Ordering::Relaxed);
        self.committed = 0;
        Ok(())
    }

    async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        if self.polled.load(Ordering::Acquire) >= self.allowance.load(Ordering::Acquire) {
            return Ok(None);
        }
        if self.commit == QueueCommit::UnitOfWork
            && self.sealed.load(Ordering::Relaxed) > self.committed
        {
            return Ok(None);
        }
        let mut queue = self.queue.lock();
        let Some(batch) = queue.ready.pop_front() else {
            return Ok(None);
        };
        self.delivered += 1;
        self.unacked.push_back((self.delivered, batch));
        self.polled.fetch_add(1, Ordering::AcqRel);
        let records = queue.batches[batch].clone();
        let batch = match self.rows {
            QueueRows::Append => SourceBatch::new(records),
            QueueRows::KeyedUpsert => keyed_upserts(&self.channel, batch, records)?,
        };
        // A unit of work binds its own cursor, so that only a barrier capture seals it.
        Ok(Some(match self.commit {
            QueueCommit::Cumulative => batch,
            QueueCommit::UnitOfWork => batch.with_checkpoint(self.cursor()),
        }))
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        self.sealed.store(self.delivered, Ordering::Relaxed);
        self.cursor()
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
        if self.commit == QueueCommit::UnitOfWork && tag > self.committed && tag != self.delivered {
            return Err(ConnectorError::InvalidState {
                expected: format!(
                    "a committed cut covering the whole unit of work (tag {})",
                    self.delivered
                ),
                actual: format!("tag {tag}"),
            });
        }
        while self.unacked.front().is_some_and(|(acked, _)| *acked <= tag) {
            self.unacked.pop_front();
        }
        self.committed = self.committed.max(tag);
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

impl QueueSource {
    fn cursor(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        if let Some(consumer) = self.consumer {
            checkpoint.set_offset("consumer", consumer.to_string());
            checkpoint.set_offset("tag", self.delivered.to_string());
        }
        if self.rows == QueueRows::KeyedUpsert {
            checkpoint
                .set_input_channels(vec![self.channel.clone()])
                .expect("one channel is a valid inventory");
        }
        checkpoint
    }
}

/// Positions by the batch's place in the queue, which every reader agrees on, and a tombstone
/// for each row whose `v` is negative.
fn keyed_upserts(
    channel: &[u8],
    index: usize,
    records: RecordBatch,
) -> Result<SourceBatch, ConnectorError> {
    let positions = batch_positions(channel, index, records.num_rows())?;
    let values = records
        .column_by_name("v")
        .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| ConnectorError::SchemaMismatch("keyed queue rows need `v`".into()))?;
    let mutations: Vec<SourceMutation> = values
        .iter()
        .map(|value| match value {
            Some(value) if value < 0 => SourceMutation::Tombstone,
            _ => SourceMutation::Put,
        })
        .collect();
    SourceBatch::positioned(records, positions)?.with_mutations(mutations)
}

/// Every row of batch `index` on `channel`, ordered by the batch index and then the row.
fn batch_positions(
    channel: &[u8],
    index: usize,
    rows: usize,
) -> Result<SourceRowPositions, ConnectorError> {
    let row_count = u32::try_from(rows)
        .map_err(|error| ConnectorError::ConfigurationError(error.to_string()))?;
    let order = (index as u64).to_be_bytes();
    SourceRowPositions::try_new(
        BinaryArray::from_vec(vec![channel; rows]),
        BinaryArray::from_vec(vec![order.as_slice(); rows]),
        UInt32Array::from((0..row_count).collect::<Vec<_>>()),
    )
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
            let positions = batch_positions(&channel(partition), cursor, records.num_rows())?;
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

//! Cluster placement for a singleton source. The engine routes the source's one channel to a
//! home vnode, and only the node that owns that vnode runs the connector; every other node keeps
//! it closed and reports an empty channel inventory. The home vnode moves like a split channel:
//! at a committed drain the donor acknowledges the handoff cut and closes the connector, and the
//! acquirer starts it from that cut. A drain stops intake at the connector's next replay boundary,
//! where the handoff cut can stand. Recovery restarts every node from the committed cursor, so
//! outside a drain only a node that never held the channel since it started may open it.

use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use tokio::sync::Notify;

use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    ConnectorCancellationPolicy, ConnectorTaskTracker, SourceBatch, SourceBatchCursor,
    SourceCheckpointUnavailablePolicy, SourceConnector, SourceContract, SourceDrainOutcome,
    ReplayRequirement, SourceDrainRequest, SourceDrainSettlement, SourcePosition, SourceRecency,
    SourceStart, SourceTopology,
};
use laminar_connectors::error::ConnectorError;
use laminar_core::checkpoint::{AssignmentDrainId, CheckpointAttempt};
use laminar_core::state::{NodeId, SourceChannelRoute, VnodeAssignmentSnapshot, VnodeRegistry};

const ROUTE: SourceChannelRoute = SourceChannelRoute::V1;

/// A singleton connector that runs only on the owner of its home vnode.
pub(crate) struct SingletonPlacement {
    inner: Box<dyn SourceConnector>,
    /// The source's one channel: its catalog name.
    channel: Vec<u8>,
    /// The schema the engine admitted. A connector may learn its schema only when it opens, so
    /// a node that has not opened it reports this one.
    admitted: Option<SchemaRef>,
    assignment: Option<Assignment>,
    request: Option<SourceStart>,
    open: bool,
    /// A move the control plane decided; the next read or drain settlement performs it.
    pending: Option<Pending>,
    /// Set once this node gives the channel up: its start position is older than its own reads.
    released: bool,
    /// Assignment version the open/closed decision follows; 0 until the first publication.
    reconciled: u64,
    drain: Option<Drain>,
    fault: Option<String>,
}

struct Assignment {
    identity: String,
    registry: Arc<VnodeRegistry>,
    node: NodeId,
}

struct Drain {
    round: AssignmentDrainId,
    committed: Option<(CheckpointAttempt, Arc<SourceCheckpoint>)>,
}

enum Pending {
    /// Start the connector at this position.
    Open(SourcePosition),
    /// Start the connector at a committed handoff cut. The previous owner's intake was fenced
    /// from the cut's capture and it flushed its output before the cut committed, so the start
    /// owes no repeated output.
    Acquire(SourcePosition),
    /// Release what the handoff cut holds, then close: the next owner reads after the cut.
    Close {
        handoff: CheckpointAttempt,
        committed: Arc<SourceCheckpoint>,
    },
}

impl SingletonPlacement {
    pub(crate) fn new(
        name: &str,
        inner: Box<dyn SourceConnector>,
        admitted: Option<SchemaRef>,
    ) -> Self {
        Self {
            inner,
            channel: name.as_bytes().to_vec(),
            admitted,
            assignment: None,
            request: None,
            open: false,
            pending: None,
            released: false,
            reconciled: 0,
            drain: None,
            fault: None,
        }
    }

    fn faulted(&self) -> Result<(), ConnectorError> {
        self.fault.as_ref().map_or(Ok(()), |fault| {
            Err(ConnectorError::InvalidState {
                expected: "singleton source placement".into(),
                actual: fault.clone(),
            })
        })
    }

    /// Follow a new assignment publication: decide whether this node starts or closes the
    /// connector. The move runs in [`Self::settle`], which needs I/O.
    fn plan(&mut self) {
        if self.fault.is_some() || self.pending.is_some() {
            return;
        }
        let Some(assignment) = &self.assignment else {
            return;
        };
        let snapshot = assignment.registry.versioned_snapshot();
        let version = snapshot.version();
        if version == 0
            || version == self.reconciled
            || snapshot.owners().iter().any(NodeId::is_unassigned)
        {
            return;
        }
        let owns = match owns_home(assignment, &snapshot) {
            Ok(owns) => owns,
            Err(error) => {
                self.fault = Some(error.to_string());
                return;
            }
        };
        let handoff = match &self.drain {
            None => None,
            Some(drain) if version == drain.round.predecessor_version => None,
            Some(drain) if version == drain.round.target_version => match &drain.committed {
                // The target is published before its round resolves; wait for the settlement.
                None => return,
                Some(committed) => Some(committed.clone()),
            },
            Some(drain) => {
                self.fault = Some(format!(
                    "assignment {version} was published during drain {:?}",
                    drain.round
                ));
                return;
            }
        };
        self.pending = match (self.open, owns, handoff.clone()) {
            (true, false, Some((handoff, committed))) => {
                self.released = true;
                Some(Pending::Close { handoff, committed })
            }
            (true, false, None) => {
                self.fault = Some("this node lost the singleton channel outside a drain".into());
                return;
            }
            (false, true, Some((attempt, committed))) => {
                Some(Pending::Acquire(SourcePosition::Resume {
                    attempt,
                    checkpoint: (*committed).clone(),
                }))
            }
            (false, true, None) if self.released => {
                self.fault = Some(
                    "the singleton channel moved back to this node outside a drain; its start \
                     position is older than this node's own reads"
                        .into(),
                );
                return;
            }
            (false, true, None) => {
                let Some(request) = &self.request else {
                    self.fault = Some("singleton source assigned before it started".into());
                    return;
                };
                Some(Pending::Open(request.clone().into_parts().1))
            }
            (true, true, _) | (false, false, _) => None,
        };
        self.reconciled = version;
        if handoff.is_some() {
            self.drain = None;
        }
    }

    /// Perform the move [`Self::plan`] decided. A failed move stays pending, so the next read
    /// retries it: the assignment it follows is already reconciled and will not be planned again.
    async fn settle(&mut self) -> Result<(), ConnectorError> {
        self.faulted()?;
        let Some(pending) = self.pending.take() else {
            return Ok(());
        };
        let result = self.perform(&pending).await;
        if result.is_err() {
            self.pending = Some(pending);
        }
        result
    }

    async fn perform(&mut self, pending: &Pending) -> Result<(), ConnectorError> {
        match pending {
            Pending::Open(position) | Pending::Acquire(position) => {
                let mut start = self
                    .request
                    .clone()
                    .ok_or_else(|| invalid("singleton source assigned before it started"))?
                    .with_position(start_position(position))?;
                if matches!(pending, Pending::Acquire(_)) {
                    start = start.with_replay_requirement(ReplayRequirement::FromCheckpoint);
                }
                self.inner.start(start).await?;
                self.open = true;
                tracing::info!(
                    source = %String::from_utf8_lossy(&self.channel),
                    assignment = self.reconciled,
                    "placed singleton source opened on this node"
                );
                if let Some(admitted) = &self.admitted {
                    if self.inner.schema() != *admitted {
                        let error = ConnectorError::SchemaMismatch(
                            "the opened singleton's schema differs from its admitted schema".into(),
                        );
                        self.fault = Some(error.to_string());
                        return Err(error);
                    }
                }
            }
            Pending::Close { handoff, committed } => {
                self.inner
                    .notify_epoch_committed(handoff.epoch, committed)
                    .await?;
                self.inner.close().await?;
                self.open = false;
                tracing::info!(
                    source = %String::from_utf8_lossy(&self.channel),
                    assignment = self.reconciled,
                    "placed singleton source handed off from this node"
                );
            }
        }
        Ok(())
    }

    /// The cursor of this node's share of the channel: before a planned start it is the start
    /// position, since nothing has been read from it yet.
    fn cursor(&self) -> SourceCheckpoint {
        match &self.pending {
            Some(
                Pending::Open(SourcePosition::Initial)
                | Pending::Acquire(SourcePosition::Initial)
                | Pending::Close { .. },
            ) => SourceCheckpoint::new(),
            Some(
                Pending::Open(
                    SourcePosition::Initialized { checkpoint }
                    | SourcePosition::Resume { checkpoint, .. },
                )
                | Pending::Acquire(
                    SourcePosition::Initialized { checkpoint }
                    | SourcePosition::Resume { checkpoint, .. },
                ),
            ) => checkpoint.clone(),
            None if self.open => self.inner.checkpoint(),
            None => SourceCheckpoint::new(),
        }
    }

    /// Whether the open connector stands where a cut can: a connector that polls to a replay
    /// boundary offers no cursor inside a unit, so a drain reads it to the unit's end.
    fn at_replay_boundary(&self) -> Result<bool, ConnectorError> {
        if !self.open
            || self.inner.checkpoint_unavailable_policy()
                != SourceCheckpointUnavailablePolicy::PollToReplayBoundary
        {
            return Ok(true);
        }
        Ok(self.inner.try_checkpoint()?.is_some())
    }

    /// Bind this node's share of the channel and the assignment it follows to a cursor.
    fn bind(&self, mut checkpoint: SourceCheckpoint) -> SourceCheckpoint {
        let owns = match &self.pending {
            Some(Pending::Open(_) | Pending::Acquire(_)) => true,
            Some(Pending::Close { .. }) => false,
            None => self.open,
        };
        let channels = if owns {
            vec![self.channel.clone()]
        } else {
            Vec::new()
        };
        checkpoint
            .set_input_channels(channels)
            .expect("the channel identity was validated at placement");
        if let Some(version) = NonZeroU64::new(self.reconciled) {
            checkpoint.bind_assignment_version(version);
        }
        checkpoint
    }

    fn follows_current_assignment(&self) -> bool {
        self.assignment.as_ref().is_some_and(|assignment| {
            let version = assignment.registry.assignment_version();
            version != 0 && version == self.reconciled
        })
    }
}

/// Run a cluster singleton that supports placement only on the owner of its home vnode.
pub(crate) fn place(
    name: &str,
    connector: Box<dyn SourceConnector>,
    config: &ConnectorConfig,
) -> Result<Box<dyn SourceConnector>, ConnectorError> {
    let contract = connector.contract(config)?;
    if contract.topology == SourceTopology::Singleton
        && super::source_admission::cluster_assignment_scoped(contract)
    {
        // The catalog name is the channel identity; refuse one the cursor format reserves.
        SourceCheckpoint::new().set_input_channels(vec![name.as_bytes().to_vec()])?;
        return Ok(Box::new(SingletonPlacement::new(
            name,
            connector,
            config.arrow_schema(),
        )));
    }
    Ok(connector)
}

/// A cut taken while the owner's first open was still pending names the channel but holds no
/// cursor: nothing was read from it, so the connector starts where that open would have.
fn start_position(position: &SourcePosition) -> SourcePosition {
    match position {
        SourcePosition::Resume { checkpoint, .. } if checkpoint.is_empty() => {
            SourcePosition::Initial
        }
        position => position.clone(),
    }
}

fn owns_home(
    assignment: &Assignment,
    snapshot: &VnodeAssignmentSnapshot,
) -> Result<bool, ConnectorError> {
    let owners = snapshot.owners();
    let vnodes = u32::try_from(owners.len())
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or_else(|| invalid("singleton assignment has no vnodes"))?;
    let home = ROUTE.vnode(&assignment.identity, 0, vnodes);
    Ok(owners[home as usize] == assignment.node)
}

fn invalid(message: &str) -> ConnectorError {
    ConnectorError::ConfigurationError(message.into())
}

#[async_trait]
impl SourceConnector for SingletonPlacement {
    fn cancellation_policy(&self) -> ConnectorCancellationPolicy {
        self.inner.cancellation_policy()
    }

    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        self.inner.terminal_task_tracker()
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        self.request = Some(request);
        Ok(())
    }

    fn supports_initialized_start(&self) -> bool {
        self.inner.supports_initialized_start()
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        self.plan();
        self.settle().await?;
        if !self.open
            || !self.follows_current_assignment()
            || (self.drain.is_some() && self.at_replay_boundary()?)
        {
            return Ok(None);
        }
        let Some(mut batch) = self.inner.poll_batch(max_records).await? else {
            return Ok(None);
        };
        // As the runtime does, ask for a checkpoint only after a batch without its own cursor: a
        // connector that binds one may treat `checkpoint` as a capture.
        let cursor = match batch.take_cursor() {
            Some(SourceBatchCursor::Complete(cursor)) => cursor,
            Some(SourceBatchCursor::Incremental(_)) => {
                return Err(ConnectorError::Internal(
                    "a singleton source batch carries a checkpoint delta".into(),
                ))
            }
            None => self.inner.checkpoint(),
        };
        // The one channel every owner reports is the catalog name, so positioned rows carry it
        // as their partition rather than the connector's own identity, such as a collection's.
        Ok(Some(
            batch
                .with_row_partition(&self.channel)
                .with_checkpoint(self.bind(cursor)),
        ))
    }

    async fn resolve_schema(
        &mut self,
        config: &ConnectorConfig,
        explicit: Option<SchemaRef>,
    ) -> Result<laminar_connectors::schema::resolution::SchemaBinding, ConnectorError> {
        self.inner.resolve_schema(config, explicit).await
    }

    async fn discover_schema(
        &mut self,
        properties: &std::collections::HashMap<String, String>,
    ) -> Result<(), ConnectorError> {
        self.inner.discover_schema(properties).await
    }

    async fn resolve_initial_position(
        &mut self,
        config: &ConnectorConfig,
    ) -> Result<SourceCheckpoint, ConnectorError> {
        let mut checkpoint = self.inner.resolve_initial_position(config).await?;
        checkpoint
            .set_input_channels(vec![self.channel.clone()])
            .expect("the channel identity was validated at placement");
        Ok(checkpoint)
    }

    async fn validate_initial_position(
        &mut self,
        config: &ConnectorConfig,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        self.inner
            .validate_initial_position(config, checkpoint)
            .await
    }

    fn schema(&self) -> SchemaRef {
        match &self.admitted {
            Some(admitted) if !self.open => Arc::clone(admitted),
            _ => self.inner.schema(),
        }
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        self.bind(self.cursor())
    }

    fn try_checkpoint(&self) -> Result<Option<SourceCheckpoint>, ConnectorError> {
        if !self.open || self.pending.is_some() {
            return Ok(Some(self.checkpoint()));
        }
        Ok(self
            .inner
            .try_checkpoint()?
            .map(|checkpoint| self.bind(checkpoint)))
    }

    fn checkpoint_unavailable_policy(&self) -> SourceCheckpointUnavailablePolicy {
        self.inner.checkpoint_unavailable_policy()
    }

    fn checkpoint_ready(&self) -> Result<bool, ConnectorError> {
        self.faulted()?;
        if !self.follows_current_assignment() {
            return Ok(false);
        }
        if self.open && self.pending.is_none() {
            return self.inner.checkpoint_ready();
        }
        Ok(true)
    }

    fn drive_control_plane(&mut self) {
        self.plan();
    }

    fn begin_drain(
        &mut self,
        request: &SourceDrainRequest,
        _deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        self.faulted()?;
        match &self.drain {
            Some(active) if active.round != request.round => Err(ConnectorError::InvalidState {
                expected: format!("singleton drain {:?}", active.round),
                actual: format!("drain {:?}", request.round),
            }),
            Some(_) => Ok(()),
            None => {
                self.drain = Some(Drain {
                    round: request.round,
                    committed: None,
                });
                Ok(())
            }
        }
    }

    fn poll_drain_ready(&mut self, round: AssignmentDrainId) -> Result<bool, ConnectorError> {
        match &self.drain {
            Some(active) if active.round == round => self.at_replay_boundary(),
            active => Err(ConnectorError::InvalidState {
                expected: format!("singleton drain {round:?}"),
                actual: format!("{:?}", active.as_ref().map(|active| active.round)),
            }),
        }
    }

    async fn finish_drain(
        &mut self,
        settlement: SourceDrainSettlement,
        _deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        let resolution = settlement.resolution();
        let drain = self
            .drain
            .as_mut()
            .filter(|drain| drain.round == resolution.round)
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: format!("singleton drain {:?}", resolution.round),
                actual: "no matching drain".into(),
            })?;
        match (
            resolution.outcome,
            settlement.handoff(),
            settlement.committed(),
        ) {
            (SourceDrainOutcome::Abort, None, None) => {
                self.drain = None;
                Ok(())
            }
            (SourceDrainOutcome::Commit, Some(handoff), Some(committed)) => {
                drain.committed = Some((handoff, Arc::clone(committed)));
                self.plan();
                self.settle().await
            }
            (outcome, _, committed) => Err(ConnectorError::InvalidState {
                expected: format!("{outcome:?} settlement"),
                actual: format!("settlement with cursor: {}", committed.is_some()),
            }),
        }
    }

    fn set_vnode_assignment(
        &mut self,
        source_identity: &str,
        registry: Arc<VnodeRegistry>,
        self_id: NodeId,
    ) -> Result<(), ConnectorError> {
        self.assignment = Some(Assignment {
            identity: source_identity.to_owned(),
            registry,
            node: self_id,
        });
        Ok(())
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.pending = None;
        if !self.open {
            return Ok(());
        }
        self.open = false;
        self.inner.close().await
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        self.inner.data_ready_notify()
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        self.inner.contract(config)
    }

    fn recovery_identity_options(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Option<std::collections::BTreeMap<String, String>>, ConnectorError> {
        self.inner.recovery_identity_options(config)
    }

    fn recency(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Option<Arc<dyn SourceRecency>>, ConnectorError> {
        self.inner.recency(config)
    }

    async fn notify_epoch_committed(
        &mut self,
        epoch: u64,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        if !self.open {
            return Ok(());
        }
        self.inner.notify_epoch_committed(epoch, checkpoint).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use arrow::array::{BinaryArray, Int64Array, UInt32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use laminar_connectors::connector::{
        DeliveryGuarantee, ProcessLeaseCheck, SourceConsistency, SourceInputMode, SourceMutation,
        SourceRowPositions, SourceTopology,
    };
    use parking_lot::Mutex;

    use super::*;

    const VNODES: u32 = 4;
    const NAME: &str = "queue_input";

    #[derive(Debug, Clone, PartialEq)]
    enum Event {
        Start(String),
        Ack(String),
        Close,
    }

    fn recorded_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]))
    }

    /// A singleton whose only progress is how many rows it delivered; it records its lifecycle.
    struct Recorder {
        events: Arc<Mutex<Vec<Event>>>,
        open: bool,
        delivered: u64,
        /// Starts still to refuse as if the broker were unreachable.
        refusals: usize,
        /// Acknowledgements still to refuse the same way.
        ack_refusals: usize,
    }

    fn position(position: &SourcePosition) -> String {
        match position {
            SourcePosition::Initial => "initial".into(),
            SourcePosition::Initialized { .. } => "initialized".into(),
            SourcePosition::Resume {
                attempt,
                checkpoint,
            } => format!(
                "resume {} at {}",
                attempt.epoch,
                checkpoint.get_offset("delivered").unwrap_or("-")
            ),
        }
    }

    /// The start a test source records: its position, and whether it owes no repeated output.
    fn started(request: SourceStart) -> Event {
        let replay = request.replay_requirement();
        let (_, start, _) = request.into_parts();
        let mut event = position(&start);
        if replay == ReplayRequirement::FromCheckpoint {
            event.push_str(", owing no repeat");
        }
        Event::Start(event)
    }

    #[async_trait]
    impl SourceConnector for Recorder {
        async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
            assert!(
                request.process_lease().is_some(),
                "the wrapper hands the connector its process lease"
            );
            let event = started(request);
            if self.refusals > 0 {
                self.refusals -= 1;
                return Err(ConnectorError::ConnectionFailed(
                    "broker unreachable".into(),
                ));
            }
            self.events.lock().push(event);
            self.open = true;
            Ok(())
        }

        async fn poll_batch(&mut self, _max: usize) -> Result<Option<SourceBatch>, ConnectorError> {
            assert!(self.open, "a closed singleton was polled");
            self.delivered += 1;
            Ok(Some(SourceBatch::new(row(self.delivered))))
        }

        fn schema(&self) -> SchemaRef {
            if self.open {
                recorded_schema()
            } else {
                Arc::new(Schema::empty())
            }
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            delivered(self.delivered)
        }

        async fn notify_epoch_committed(
            &mut self,
            _epoch: u64,
            checkpoint: &SourceCheckpoint,
        ) -> Result<(), ConnectorError> {
            if self.ack_refusals > 0 {
                self.ack_refusals -= 1;
                return Err(ConnectorError::ConnectionFailed(
                    "broker unreachable".into(),
                ));
            }
            let delivered = checkpoint.get_offset("delivered").unwrap_or("-");
            self.events.lock().push(Event::Ack(delivered.into()));
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.events.lock().push(Event::Close);
            self.open = false;
            Ok(())
        }

        async fn resolve_initial_position(
            &mut self,
            _config: &ConnectorConfig,
        ) -> Result<SourceCheckpoint, ConnectorError> {
            Ok(delivered(0))
        }

        fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
            Ok(placed_singleton())
        }
    }

    /// Like an IBM MQ queue under syncpoint: everything read since the last commit is one unit of
    /// work, and `checkpoint` seals it until a commit names all of it. Each batch binds its own
    /// cursor, so reading does not seal.
    struct UnitOfWork {
        events: Arc<Mutex<Vec<Event>>>,
        gotten: u64,
        committed: u64,
        /// `gotten` at the last capture.
        sealed: AtomicU64,
    }

    fn delivered(count: u64) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset("delivered", count.to_string());
        checkpoint
    }

    /// The `count`th row a test singleton delivers.
    fn row(count: u64) -> RecordBatch {
        let rows = Int64Array::from(vec![i64::try_from(count).unwrap()]);
        RecordBatch::try_new(recorded_schema(), vec![Arc::new(rows)]).unwrap()
    }

    fn placed_singleton() -> SourceContract {
        laminar_connectors::testing::with_cluster_singleton_placement(SourceContract::new(
            SourceConsistency::CommitCoupled,
            SourceTopology::Singleton,
            SourceInputMode::AppendOnly,
        ))
    }

    #[async_trait]
    impl SourceConnector for UnitOfWork {
        async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
            self.events.lock().push(started(request));
            Ok(())
        }

        async fn poll_batch(&mut self, _max: usize) -> Result<Option<SourceBatch>, ConnectorError> {
            if self.sealed.load(Ordering::Relaxed) > self.committed {
                return Ok(None);
            }
            self.gotten += 1;
            Ok(Some(
                SourceBatch::new(row(self.gotten)).with_checkpoint(delivered(self.gotten)),
            ))
        }

        fn schema(&self) -> SchemaRef {
            recorded_schema()
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            self.sealed.store(self.gotten, Ordering::Relaxed);
            delivered(self.gotten)
        }

        async fn notify_epoch_committed(
            &mut self,
            _epoch: u64,
            checkpoint: &SourceCheckpoint,
        ) -> Result<(), ConnectorError> {
            let count: u64 = checkpoint.get_offset("delivered").unwrap().parse().unwrap();
            if count <= self.committed {
                return Ok(());
            }
            if count != self.gotten {
                return Err(ConnectorError::InvalidState {
                    expected: format!("a cut covering the whole unit of work ({})", self.gotten),
                    actual: count.to_string(),
                });
            }
            self.events.lock().push(Event::Ack(count.to_string()));
            self.committed = count;
            Ok(())
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            self.events.lock().push(Event::Close);
            Ok(())
        }

        fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
            Ok(placed_singleton())
        }
    }

    /// Emits positioned upserts under its own channel identity, the second row a tombstone.
    struct Positioned;

    #[async_trait]
    impl SourceConnector for Positioned {
        async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn poll_batch(&mut self, _max: usize) -> Result<Option<SourceBatch>, ConnectorError> {
            let rows = Int64Array::from(vec![1, 2]);
            let records = RecordBatch::try_new(recorded_schema(), vec![Arc::new(rows)]).unwrap();
            let positions = SourceRowPositions::try_new(
                BinaryArray::from_iter_values([b"collection-uuid"; 2]),
                BinaryArray::from_iter_values([[0_u8, 1], [0, 2]]),
                UInt32Array::from(vec![0, 0]),
            )?;
            SourceBatch::positioned(records, positions)?
                .with_mutations(vec![SourceMutation::Put, SourceMutation::Tombstone])
                .map(Some)
        }

        fn schema(&self) -> SchemaRef {
            recorded_schema()
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            SourceCheckpoint::new()
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
            Ok(placed_singleton())
        }
    }

    /// Reads units of three rows, one row a poll, and offers a cursor only between units, as a
    /// Delta commit or a PostgreSQL copy chunk does.
    struct Units {
        delivered: u64,
    }

    #[async_trait]
    impl SourceConnector for Units {
        async fn start(&mut self, _request: SourceStart) -> Result<(), ConnectorError> {
            Ok(())
        }

        async fn poll_batch(&mut self, _max: usize) -> Result<Option<SourceBatch>, ConnectorError> {
            self.delivered += 1;
            Ok(Some(
                SourceBatch::new(row(self.delivered)).with_checkpoint(self.checkpoint()),
            ))
        }

        fn schema(&self) -> SchemaRef {
            recorded_schema()
        }

        fn checkpoint(&self) -> SourceCheckpoint {
            delivered(self.delivered - self.delivered % 3)
        }

        fn try_checkpoint(&self) -> Result<Option<SourceCheckpoint>, ConnectorError> {
            Ok(self.delivered.is_multiple_of(3).then(|| self.checkpoint()))
        }

        fn checkpoint_unavailable_policy(&self) -> SourceCheckpointUnavailablePolicy {
            SourceCheckpointUnavailablePolicy::PollToReplayBoundary
        }

        async fn close(&mut self) -> Result<(), ConnectorError> {
            Ok(())
        }

        fn contract(&self, _config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
            Ok(placed_singleton())
        }
    }

    struct Node {
        source: SingletonPlacement,
        registry: Arc<VnodeRegistry>,
        events: Arc<Mutex<Vec<Event>>>,
    }

    impl Node {
        async fn new(id: u64) -> Self {
            Self::with(id, 0, 0, recorded_schema()).await
        }

        /// A node whose connector refuses its first `refusals` starts and `ack_refusals`
        /// acknowledgements, admitted with `admitted`.
        async fn with(id: u64, refusals: usize, ack_refusals: usize, admitted: SchemaRef) -> Self {
            let events = Arc::new(Mutex::new(Vec::new()));
            let recorder = Recorder {
                events: Arc::clone(&events),
                open: false,
                delivered: 0,
                refusals,
                ack_refusals,
            };
            Self::placed(id, Box::new(recorder), events, admitted).await
        }

        async fn unit_of_work(id: u64) -> Self {
            let events = Arc::new(Mutex::new(Vec::new()));
            let inner = UnitOfWork {
                events: Arc::clone(&events),
                gotten: 0,
                committed: 0,
                sealed: AtomicU64::new(0),
            };
            Self::placed(id, Box::new(inner), events, recorded_schema()).await
        }

        async fn placed(
            id: u64,
            inner: Box<dyn SourceConnector>,
            events: Arc<Mutex<Vec<Event>>>,
            admitted: SchemaRef,
        ) -> Self {
            let registry = Arc::new(VnodeRegistry::new_unassigned(VNODES));
            let mut source = SingletonPlacement::new(NAME, inner, Some(admitted));
            source
                .set_vnode_assignment(NAME, Arc::clone(&registry), NodeId(id))
                .unwrap();
            let request = SourceStart::new(
                ConnectorConfig::new("recorder"),
                SourcePosition::Initial,
                DeliveryGuarantee::AtLeastOnce,
            )
            .unwrap()
            .with_process_lease(ProcessLeaseCheck::new(|| true));
            source.start(request).await.unwrap();
            Self {
                source,
                registry,
                events,
            }
        }

        /// Publish an assignment that gives the home vnode to `owner` and every other to 9.
        fn publish(&self, owner: u64, version: u64) {
            let home = ROUTE.vnode(NAME, 0, NonZeroU32::new(VNODES).unwrap()) as usize;
            let mut owners = vec![NodeId(9); VNODES as usize];
            owners[home] = NodeId(owner);
            self.registry
                .set_assignment_and_version(owners.into(), version);
        }

        async fn poll(&mut self) -> Option<SourceCheckpoint> {
            let mut batch = self.source.poll_batch(8).await.unwrap()?;
            match batch.take_cursor() {
                Some(laminar_connectors::connector::SourceBatchCursor::Complete(cursor)) => {
                    Some(cursor)
                }
                other => panic!("a placed singleton batch carries a complete cursor: {other:?}"),
            }
        }

        fn events(&self) -> Vec<Event> {
            self.events.lock().clone()
        }
    }

    fn round(predecessor: u64) -> AssignmentDrainId {
        AssignmentDrainId {
            predecessor_version: predecessor,
            target_version: predecessor + 1,
            digest: [7; 32],
        }
    }

    fn deadline() -> tokio::time::Instant {
        tokio::time::Instant::now() + std::time::Duration::from_secs(5)
    }

    async fn begin(nodes: &mut [&mut Node], round: AssignmentDrainId) {
        let request = SourceDrainRequest::new(round).unwrap();
        for node in nodes {
            node.source.begin_drain(&request, deadline()).unwrap();
            assert!(node.source.poll_drain_ready(round).unwrap());
            assert!(
                node.poll().await.is_none(),
                "a draining singleton reads nothing"
            );
        }
    }

    #[tokio::test]
    async fn a_sealed_start_cursor_names_the_one_channel() {
        let mut node = Node::new(1).await;
        let cursor = node
            .source
            .resolve_initial_position(&ConnectorConfig::new("recorder"))
            .await
            .unwrap();
        assert_eq!(
            cursor.input_channels(),
            Some(&[NAME.as_bytes().to_vec()][..])
        );
        assert_eq!(
            cursor.assignment_version(),
            None,
            "a sealed cursor is unowned"
        );
    }

    #[tokio::test]
    async fn a_cut_taken_before_the_owner_opened_resumes_at_the_initial_position() {
        let mut owner = Node::new(1).await;
        owner.publish(1, 1);
        owner.source.drive_control_plane();
        let cut = owner.source.checkpoint();
        assert_eq!(cut.input_channels(), Some(&[NAME.as_bytes().to_vec()][..]));
        assert!(cut.is_empty(), "nothing was read before the first open");

        let mut recovered = Node::new(1).await;
        let resume = SourcePosition::Resume {
            attempt: CheckpointAttempt::canonical(1),
            checkpoint: cut,
        };
        let request = SourceStart::new(
            ConnectorConfig::new("recorder"),
            resume,
            DeliveryGuarantee::AtLeastOnce,
        )
        .unwrap()
        .with_process_lease(ProcessLeaseCheck::new(|| true));
        recovered.source.start(request).await.unwrap();
        recovered.publish(1, 1);
        recovered.poll().await.expect("the owner reads");
        assert_eq!(recovered.events(), [Event::Start("initial".into())]);
    }

    #[tokio::test]
    async fn only_the_home_vnode_owner_runs_the_singleton() {
        let (mut a, mut b) = (Node::new(1).await, Node::new(2).await);
        for node in [&a, &b] {
            node.publish(1, 1);
        }
        let cursor = a.poll().await.expect("the owner reads");
        assert_eq!(
            cursor.input_channels(),
            Some(&[NAME.as_bytes().to_vec()][..])
        );
        assert_eq!(cursor.assignment_version().map(NonZeroU64::get), Some(1));
        assert_eq!(cursor.get_offset("delivered"), Some("1"));
        assert!(b.poll().await.is_none());
        assert!(
            b.events().is_empty(),
            "a non-owner never starts the connector"
        );
        let idle = b.source.checkpoint();
        assert_eq!(idle.input_channels(), Some(&[][..]));
        assert_eq!(idle.assignment_version().map(NonZeroU64::get), Some(1));
        assert!(idle.offsets().is_empty());
        assert!(a.source.checkpoint_ready().unwrap() && b.source.checkpoint_ready().unwrap());
        assert_eq!(a.events(), [Event::Start("initial".into())]);
        assert_eq!(
            b.source.schema(),
            recorded_schema(),
            "a node that never opens the connector reports the admitted schema"
        );
    }

    #[tokio::test]
    async fn a_committed_drain_moves_the_singleton_at_the_handoff_cut() {
        let (mut a, mut b) = (Node::new(1).await, Node::new(2).await);
        for node in [&a, &b] {
            node.publish(1, 1);
        }
        a.poll().await.unwrap();
        a.poll().await.unwrap();
        b.poll().await;
        begin(&mut [&mut a, &mut b], round(1)).await;
        let handoff = Arc::new(a.source.checkpoint());
        let attempt = CheckpointAttempt::canonical(5);
        // The settlement arrives before the target publication: nothing moves until both.
        for node in [&mut a, &mut b] {
            node.source
                .finish_drain(
                    SourceDrainSettlement::commit(round(1), attempt, Arc::clone(&handoff)),
                    deadline(),
                )
                .await
                .unwrap();
        }
        assert_eq!(a.events(), [Event::Start("initial".into())]);
        for node in [&a, &b] {
            node.publish(2, 2);
        }
        assert!(a.poll().await.is_none());
        let cursor = b.poll().await.expect("the acquirer reads");
        assert_eq!(
            a.events(),
            [
                Event::Start("initial".into()),
                Event::Ack("2".into()),
                Event::Close
            ],
            "the donor releases the handoff cut before it closes"
        );
        assert_eq!(
            b.events(),
            [Event::Start("resume 5 at 2, owing no repeat".into())],
            "the previous owner wrote nothing after the handoff cut"
        );
        assert_eq!(cursor.assignment_version().map(NonZeroU64::get), Some(2));
        assert_eq!(a.source.checkpoint().input_channels(), Some(&[][..]));
    }

    /// A drain that begins inside a unit reads the unit to its end, so the handoff cut can be
    /// captured, and reads nothing past it.
    #[tokio::test]
    async fn a_drain_inside_a_unit_reads_to_its_end_before_the_handoff_cut() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut a = Node::placed(
            1,
            Box::new(Units { delivered: 0 }),
            events,
            recorded_schema(),
        )
        .await;
        a.publish(1, 1);
        a.poll()
            .await
            .expect("the owner reads the first row of a unit");
        let request = SourceDrainRequest::new(round(1)).unwrap();
        a.source.begin_drain(&request, deadline()).unwrap();
        for _ in 0..2 {
            assert!(
                !a.source.poll_drain_ready(round(1)).unwrap(),
                "inside the unit"
            );
            assert!(
                a.poll().await.is_some(),
                "the drain reads to the unit's end"
            );
        }
        assert!(a.source.poll_drain_ready(round(1)).unwrap());
        assert!(a.poll().await.is_none(), "nothing past the boundary");
        let cut = a
            .source
            .try_checkpoint()
            .unwrap()
            .expect("a cut at the boundary");
        assert_eq!(cut.get_offset("delivered"), Some("3"));
    }

    #[tokio::test]
    async fn an_aborted_drain_keeps_the_owner_reading() {
        let mut a = Node::new(1).await;
        a.publish(1, 1);
        a.poll().await.unwrap();
        begin(&mut [&mut a], round(1)).await;
        a.source
            .finish_drain(SourceDrainSettlement::abort(round(1)), deadline())
            .await
            .unwrap();
        assert_eq!(a.poll().await.unwrap().get_offset("delivered"), Some("2"));
        assert_eq!(a.events(), [Event::Start("initial".into())]);
    }

    #[tokio::test]
    async fn outside_a_drain_only_a_node_that_never_held_the_channel_takes_it() {
        // A node new to the assignment starts the connector from its start position.
        let mut b = Node::new(2).await;
        b.publish(1, 1);
        assert!(b.poll().await.is_none());
        b.publish(2, 2);
        assert!(b.poll().await.is_some());

        // The owner losing the channel without a drain faults rather than reading twice.
        let mut a = Node::new(1).await;
        a.publish(1, 1);
        a.poll().await.unwrap();
        a.publish(2, 2);
        let error = a.source.poll_batch(8).await.unwrap_err();
        assert!(error.to_string().contains("outside a drain"), "{error}");
        assert!(a.source.checkpoint_ready().is_err());

        // A channel given up at a drain may come back only through another drain.
        let mut c = Node::new(3).await;
        c.publish(3, 1);
        c.poll().await.unwrap();
        begin(&mut [&mut c], round(1)).await;
        c.source
            .finish_drain(
                SourceDrainSettlement::commit(
                    round(1),
                    CheckpointAttempt::canonical(4),
                    Arc::new(c.source.checkpoint()),
                ),
                deadline(),
            )
            .await
            .unwrap();
        c.publish(2, 2);
        assert!(c.poll().await.is_none());
        c.publish(3, 3);
        let error = c.source.poll_batch(8).await.unwrap_err();
        assert!(error.to_string().contains("moved back"), "{error}");
    }

    #[tokio::test]
    async fn a_failed_open_is_retried_by_the_next_read() {
        let mut a = Node::with(1, 1, 0, recorded_schema()).await;
        a.publish(1, 1);
        let error = a.source.poll_batch(8).await.unwrap_err();
        assert!(error.is_transient(), "{error}");
        assert_eq!(
            a.source.checkpoint().input_channels(),
            Some(&[NAME.as_bytes().to_vec()][..]),
            "the owner keeps claiming the channel it has not opened yet"
        );
        assert_eq!(a.poll().await.unwrap().get_offset("delivered"), Some("1"));
        assert_eq!(a.events(), [Event::Start("initial".into())]);
    }

    #[tokio::test]
    async fn an_opened_singleton_must_produce_its_admitted_schema() {
        let other = Arc::new(Schema::new(vec![Field::new("m", DataType::Utf8, true)]));
        let mut a = Node::with(1, 0, 0, Arc::clone(&other)).await;
        assert_eq!(a.source.schema(), other);
        a.publish(1, 1);
        let error = a.source.poll_batch(8).await.unwrap_err();
        assert!(
            matches!(error, ConnectorError::SchemaMismatch(_)),
            "{error}"
        );
        assert!(a.source.checkpoint_ready().is_err());
        a.source.close().await.unwrap();
        assert_eq!(
            a.events(),
            [Event::Start("initial".into()), Event::Close],
            "the mismatched connector is still closed"
        );
    }

    #[tokio::test]
    async fn a_failed_handoff_acknowledgement_is_retried_before_the_close() {
        let mut a = Node::with(1, 0, 1, recorded_schema()).await;
        a.publish(1, 1);
        a.poll().await.unwrap();
        begin(&mut [&mut a], round(1)).await;
        let handoff = Arc::new(a.source.checkpoint());
        a.source
            .finish_drain(
                SourceDrainSettlement::commit(round(1), CheckpointAttempt::canonical(3), handoff),
                deadline(),
            )
            .await
            .unwrap();
        a.publish(2, 2);
        let error = a.source.poll_batch(8).await.unwrap_err();
        assert!(error.is_transient(), "{error}");
        assert_eq!(
            a.source.checkpoint().input_channels(),
            Some(&[][..]),
            "the donor no longer claims the channel it is releasing"
        );
        assert!(a.poll().await.is_none());
        assert_eq!(
            a.events(),
            [
                Event::Start("initial".into()),
                Event::Ack("1".into()),
                Event::Close
            ]
        );
    }

    /// A connector that binds its own cursors may seal what it has read when asked for a
    /// checkpoint, so only barrier and drain captures ask, and it reads on between them.
    #[tokio::test]
    async fn a_unit_of_work_reads_several_batches_per_checkpoint() {
        let (mut a, mut b) = (Node::unit_of_work(1).await, Node::unit_of_work(2).await);
        for node in [&a, &b] {
            node.publish(1, 1);
        }
        for count in ["1", "2", "3"] {
            let cursor = a.poll().await.expect("the unit of work stays open");
            assert_eq!(cursor.get_offset("delivered"), Some(count));
        }
        let cut = a.source.try_checkpoint().unwrap().unwrap();
        assert!(
            a.poll().await.is_none(),
            "a sealed unit waits for its commit"
        );
        a.source.notify_epoch_committed(1, &cut).await.unwrap();
        assert_eq!(a.poll().await.unwrap().get_offset("delivered"), Some("4"));

        begin(&mut [&mut a, &mut b], round(1)).await;
        let handoff = Arc::new(a.source.checkpoint());
        for node in [&mut a, &mut b] {
            node.source
                .finish_drain(
                    SourceDrainSettlement::commit(
                        round(1),
                        CheckpointAttempt::canonical(2),
                        Arc::clone(&handoff),
                    ),
                    deadline(),
                )
                .await
                .unwrap();
        }
        for node in [&a, &b] {
            node.publish(2, 2);
        }
        assert!(a.poll().await.is_none());
        b.poll().await.expect("the acquirer reads");
        assert_eq!(
            a.events(),
            [
                Event::Start("initial".into()),
                Event::Ack("3".into()),
                Event::Ack("4".into()),
                Event::Close
            ],
            "the donor commits the sealed handoff unit before it closes"
        );
        assert_eq!(
            b.events(),
            [Event::Start("resume 2 at 4, owing no repeat".into())]
        );
    }

    /// Every owner reports the catalog name as the one channel, so positioned rows carry it as
    /// their partition; order keys and mutations pass through.
    #[tokio::test]
    async fn positioned_rows_take_the_channel_as_their_partition() {
        let mut node =
            Node::placed(1, Box::new(Positioned), Arc::default(), recorded_schema()).await;
        node.publish(1, 1);
        let batch = node
            .source
            .poll_batch(8)
            .await
            .unwrap()
            .expect("the owner reads");
        let positions = batch.row_positions().expect("positions pass through");
        assert!(positions
            .partition()
            .iter()
            .all(|partition| partition == Some(NAME.as_bytes())));
        assert_eq!(positions.order_key().value(1), [0, 2]);
        assert_eq!(
            batch.mutations(),
            Some(&[SourceMutation::Put, SourceMutation::Tombstone][..])
        );
    }

    #[test]
    fn a_source_named_like_the_reserved_channel_is_refused() {
        let recorder = Recorder {
            events: Arc::default(),
            open: false,
            delivered: 0,
            refusals: 0,
            ack_refusals: 0,
        };
        assert!(place("\0", Box::new(recorder), &ConnectorConfig::new("recorder")).is_err());
    }
}

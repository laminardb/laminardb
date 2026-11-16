//! Cluster placement for a singleton source. The engine routes the source's one channel to a
//! home vnode, and only the node that owns that vnode runs the connector; every other node keeps
//! it closed and reports an empty channel inventory. The home vnode moves like a split channel:
//! at a committed drain the donor acknowledges the handoff cut and closes the connector, and the
//! acquirer starts it from that cut. Recovery restarts every node from the committed cursor, so
//! outside a drain only a node that never held the channel since it started may open it.

use std::num::{NonZeroU32, NonZeroU64};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use tokio::sync::Notify;

use laminar_connectors::checkpoint::SourceCheckpoint;
use laminar_connectors::config::ConnectorConfig;
use laminar_connectors::connector::{
    ConnectorCancellationPolicy, ConnectorTaskTracker, SourceBatch,
    SourceCheckpointUnavailablePolicy, SourceConnector, SourceContract, SourceDrainOutcome,
    SourceDrainRequest, SourceDrainSettlement, SourcePosition, SourceRecency, SourceStart,
    SourceTopology,
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
    assignment: Option<Assignment>,
    request: Option<SourceStart>,
    open: bool,
    /// Set once this node gives the channel up: its start position is older than its own reads.
    released: bool,
    /// Assignment version the open/closed state follows; 0 until the first publication.
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

impl SingletonPlacement {
    pub(crate) fn new(name: &str, inner: Box<dyn SourceConnector>) -> Self {
        Self {
            inner,
            channel: name.as_bytes().to_vec(),
            assignment: None,
            request: None,
            open: false,
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

    fn fail(&mut self, fault: String) -> Result<(), ConnectorError> {
        self.fault = Some(fault);
        self.faulted()
    }

    /// Follow a new assignment publication: start the connector when this node takes the home
    /// vnode and close it when this node gives it up.
    async fn follow(&mut self) -> Result<(), ConnectorError> {
        self.faulted()?;
        let Some(assignment) = &self.assignment else {
            return Ok(());
        };
        let snapshot = assignment.registry.versioned_snapshot();
        let version = snapshot.version();
        if version == 0
            || version == self.reconciled
            || snapshot.owners().iter().any(NodeId::is_unassigned)
        {
            return Ok(());
        }
        let owns = owns_home(assignment, &snapshot)?;
        let handoff = match &self.drain {
            None => None,
            Some(drain) if version == drain.round.predecessor_version => None,
            Some(drain) if version == drain.round.target_version => match &drain.committed {
                // The target is published before its round resolves; wait for the settlement.
                None => return Ok(()),
                Some(committed) => Some(committed.clone()),
            },
            Some(drain) => {
                let round = drain.round;
                return self.fail(format!(
                    "assignment {version} was published during drain {round:?}"
                ));
            }
        };
        match (self.open, owns, handoff.clone()) {
            (true, false, Some((handoff, committed))) => {
                // The next owner reads after the handoff cut: release what the cut holds first.
                self.inner
                    .notify_epoch_committed(handoff.epoch, &committed)
                    .await?;
                self.inner.close().await?;
                self.open = false;
                self.released = true;
            }
            (true, false, None) => {
                return self.fail("this node lost the singleton channel outside a drain".into());
            }
            (false, true, Some((attempt, committed))) => {
                self.start_inner(SourcePosition::Resume {
                    attempt,
                    checkpoint: (*committed).clone(),
                })
                .await?;
            }
            (false, true, None) if self.released => {
                return self.fail(
                    "the singleton channel moved back to this node outside a drain; its start \
                     position is older than this node's own reads"
                        .into(),
                );
            }
            (false, true, None) => {
                let position = self
                    .request
                    .clone()
                    .ok_or_else(|| invalid("singleton source assigned before it started"))?
                    .into_parts()
                    .1;
                self.start_inner(position).await?;
            }
            (true, true, _) | (false, false, _) => {}
        }
        self.reconciled = version;
        if handoff.is_some() {
            self.drain = None;
        }
        Ok(())
    }

    async fn start_inner(&mut self, position: SourcePosition) -> Result<(), ConnectorError> {
        let (config, _, delivery) = self
            .request
            .clone()
            .ok_or_else(|| invalid("singleton source assigned before it started"))?
            .into_parts();
        self.inner
            .start(SourceStart::new(config, position, delivery)?)
            .await?;
        self.open = true;
        Ok(())
    }

    /// Bind this node's share of the channel and the assignment it follows to a cursor.
    fn bind(&self, mut checkpoint: SourceCheckpoint) -> SourceCheckpoint {
        let channels = if self.open {
            vec![self.channel.clone()]
        } else {
            Vec::new()
        };
        checkpoint
            .set_input_channels(channels)
            .expect("at most one channel is always ordered");
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
        return Ok(Box::new(SingletonPlacement::new(name, connector)));
    }
    Ok(connector)
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
        self.follow().await?;
        if !self.open || self.drain.is_some() || !self.follows_current_assignment() {
            return Ok(None);
        }
        let Some(batch) = self.inner.poll_batch(max_records).await? else {
            return Ok(None);
        };
        Ok(Some(
            batch.with_checkpoint(self.bind(self.inner.checkpoint())),
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
            .expect("one channel is always ordered");
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
        self.inner.schema()
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let checkpoint = if self.open {
            self.inner.checkpoint()
        } else {
            SourceCheckpoint::new()
        };
        self.bind(checkpoint)
    }

    fn try_checkpoint(&self) -> Result<Option<SourceCheckpoint>, ConnectorError> {
        if !self.open {
            return Ok(Some(self.bind(SourceCheckpoint::new())));
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
        if self.open {
            return self.inner.checkpoint_ready();
        }
        Ok(true)
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
            Some(active) if active.round == round => Ok(true),
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
                self.follow().await
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

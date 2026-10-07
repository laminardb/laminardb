//! Benchmark fixtures for native processing and private cluster execution hooks.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::RecordBatch;
use laminar_core::checkpoint::{CheckpointAssignmentFence, CheckpointParticipant};
use laminar_core::cluster::control::LeaseDeadline;
use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
use laminar_core::state::{NodeId, VnodeRegistry};
use uuid::Uuid;

use super::{NativeProcessFunction, ProcessFunctionDescriptor, ProcessFunctionOperator};
use crate::error::DbError;
use crate::operator::sql_query::ClusterShuffleConfig;
use crate::operator_graph::{GraphOperator, InputFrontier};

const VNODE_COUNT: u32 = 1_024;

/// Execution path measured with the same prepared input and native handler.
#[derive(Clone, Copy)]
pub enum NativeProcessBenchmarkMode {
    /// Existing in-process operator execution without cluster authority.
    Local,
    /// Single-owner routing and result fencing under a cluster assignment.
    SingleOwner,
    /// Private input routing across two loopback shuffle endpoints. Admission remains closed.
    TwoOwners,
}

/// A production native operator with deterministic fixture authority and no connector I/O.
pub struct NativeProcessBenchmark {
    operator: ProcessFunctionOperator,
    peer: Option<BenchmarkPeer>,
}

struct BenchmarkPeer {
    operator: ProcessFunctionOperator,
    receiver: Arc<ShuffleReceiver>,
}

impl NativeProcessBenchmark {
    /// Prepare the same operator in the selected execution mode before timing begins.
    ///
    /// # Errors
    /// Rejects invalid descriptors or inability to prepare the loopback shuffle fixture.
    pub async fn new(
        descriptor: ProcessFunctionDescriptor,
        handler: Arc<dyn NativeProcessFunction>,
        mode: NativeProcessBenchmarkMode,
    ) -> Result<Self, DbError> {
        if matches!(mode, NativeProcessBenchmarkMode::Local) {
            return Ok(Self {
                operator: ProcessFunctionOperator::new(descriptor, handler, VNODE_COUNT)?,
                peer: None,
            });
        }
        let owners = match mode {
            NativeProcessBenchmarkMode::SingleOwner => vec![7; VNODE_COUNT as usize],
            NativeProcessBenchmarkMode::TwoOwners => (0..VNODE_COUNT)
                .map(|vnode| 7 + u64::from(vnode % 2))
                .collect(),
            NativeProcessBenchmarkMode::Local => unreachable!("local fixture returned above"),
        };
        let assignment = CheckpointAssignmentFence::from_owner_map(
            7,
            &owners,
            owners
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .map(|node_id| CheckpointParticipant {
                    node_id,
                    boot_incarnation: Uuid::from_u128(u128::from(node_id)),
                })
                .collect(),
        )
        .map_err(DbError::Config)?;
        let (operator, scope) = cluster_operator(
            descriptor.clone(),
            Arc::clone(&handler),
            7,
            &owners,
            &assignment,
        )
        .await?;
        let peer = if matches!(mode, NativeProcessBenchmarkMode::TwoOwners) {
            let (operator, remote) =
                cluster_operator(descriptor, handler, 8, &owners, &assignment).await?;
            scope.sender.register_peer(8, remote.receiver.local_addr());
            remote.sender.register_peer(7, scope.receiver.local_addr());
            Some(BenchmarkPeer {
                operator,
                receiver: remote.receiver,
            })
        } else {
            None
        };
        Ok(Self { operator, peer })
    }

    /// Process a prepared Arrow batch through production routing, state validation and application.
    /// Two-owner measurements drain both operators and transport rows assigned to the peer.
    ///
    /// # Errors
    /// Returns the ordinary operator errors, including lost fixture authority and resource limits.
    pub async fn step(&mut self, batch: &RecordBatch) -> Result<Vec<RecordBatch>, DbError> {
        if self.peer.is_some() {
            return self.step_two_owners(batch).await;
        }
        self.operator
            .process_with_frontiers(
                &[vec![batch.clone()]],
                &[InputFrontier {
                    watermark: None,
                    idle: false,
                }],
            )
            .await
    }

    async fn step_two_owners(&mut self, batch: &RecordBatch) -> Result<Vec<RecordBatch>, DbError> {
        let peer = self
            .peer
            .as_mut()
            .ok_or_else(|| DbError::Config("benchmark peer is missing".into()))?;
        let before = self
            .operator
            .accepted_activations()
            .saturating_add(peer.operator.accepted_activations());
        let frontier = [InputFrontier::default()];
        let mut output = self
            .operator
            .process_with_frontiers(&[vec![batch.clone()]], &frontier)
            .await?;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                for (stage, batch) in peer.receiver.drain_checkpointed_staged() {
                    peer.operator.stage_checkpointed_shuffle(
                        &stage,
                        crate::operator::RetainedBatch::from_received(batch),
                        i64::MIN,
                    )?;
                }
                output.extend(self.operator.process_with_frontiers(&[], &frontier).await?);
                output.extend(peer.operator.process_with_frontiers(&[], &frontier).await?);
                let processed = self
                    .operator
                    .accepted_activations()
                    .saturating_add(peer.operator.accepted_activations())
                    .saturating_sub(before);
                if processed == batch.num_rows() as u64
                    && !self.operator.checkpoint_drain_pending()
                    && !peer.operator.checkpoint_drain_pending()
                {
                    break Ok(output);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| DbError::Config("two-owner benchmark drain timed out".into()))?
    }
}

async fn cluster_operator(
    descriptor: ProcessFunctionDescriptor,
    handler: Arc<dyn NativeProcessFunction>,
    node: u64,
    owners: &[u64],
    assignment: &CheckpointAssignmentFence,
) -> Result<(ProcessFunctionOperator, ClusterShuffleConfig), DbError> {
    let mut operator = ProcessFunctionOperator::new(descriptor, handler, VNODE_COUNT)?;
    let boot = Uuid::from_u128(u128::from(node));
    let registry = Arc::new(VnodeRegistry::single_owner(VNODE_COUNT, NodeId(node)));
    registry.set_assignment_and_version(
        Arc::from(owners.iter().copied().map(NodeId).collect::<Vec<_>>()),
        7,
    );
    let sender = Arc::new(ShuffleSender::new(node, boot));
    let receiver = Arc::new(
        ShuffleReceiver::bind(node, std::net::SocketAddr::from(([127, 0, 0, 1], 0)), boot)
            .await
            .map_err(|error| DbError::Config(error.to_string()))?,
    );
    let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(300)));
    sender
        .bind_process_lease_deadline_pair(&receiver, Arc::clone(&deadline))
        .map_err(|error| DbError::Config(error.to_string()))?;
    sender
        .install_assignment_fence(assignment, owners)
        .map_err(|error| DbError::Config(error.to_string()))?;
    receiver
        .install_assignment_fence(assignment, owners)
        .map_err(|error| DbError::Config(error.to_string()))?;
    sender.set_recovery_gen(3);
    receiver.set_recovery_gen(3);
    operator.require_cluster_execution(
        "activity",
        tokio::runtime::Handle::current(),
        NodeId(node),
    )?;
    operator.bind_startup_assignment(
        assignment,
        &owners
            .iter()
            .enumerate()
            .filter_map(|(vnode, owner)| {
                (*owner == node)
                    .then_some(u32::try_from(vnode).expect("fixed benchmark vnode domain fits u32"))
            })
            .collect::<Vec<_>>(),
    )?;
    let scope = ClusterShuffleConfig {
        registry,
        sender,
        receiver,
        self_id: NodeId(node),
        topology: None,
    };
    operator.bind_process_execution_authority(&scope, deadline)?;
    Ok((operator, scope))
}

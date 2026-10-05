//! Benchmark fixtures for native processing and the private single-owner execution hooks.

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
    /// Private single-owner routing and result fencing; cluster admission remains rejected.
    SingleOwner,
}

/// A production native operator with deterministic fixture authority and no connector I/O.
pub struct NativeProcessBenchmark {
    operator: ProcessFunctionOperator,
}

impl NativeProcessBenchmark {
    /// Prepare the same operator in either execution mode before timing begins.
    ///
    /// # Errors
    /// Rejects invalid descriptors or inability to prepare the loopback shuffle fixture.
    pub async fn new(
        descriptor: ProcessFunctionDescriptor,
        handler: Arc<dyn NativeProcessFunction>,
        mode: NativeProcessBenchmarkMode,
    ) -> Result<Self, DbError> {
        let mut operator = ProcessFunctionOperator::new(descriptor, handler, VNODE_COUNT)?;
        if matches!(mode, NativeProcessBenchmarkMode::SingleOwner) {
            let boot = Uuid::from_u128(7);
            let owners = vec![7; VNODE_COUNT as usize];
            let assignment = CheckpointAssignmentFence::from_owner_map(
                7,
                &owners,
                vec![CheckpointParticipant {
                    node_id: 7,
                    boot_incarnation: boot,
                }],
            )
            .map_err(DbError::Config)?;
            let registry = Arc::new(VnodeRegistry::single_owner(VNODE_COUNT, NodeId(7)));
            registry
                .set_assignment_and_version(Arc::from(vec![NodeId(7); VNODE_COUNT as usize]), 7);
            let sender = Arc::new(ShuffleSender::new(7, boot));
            let receiver = Arc::new(
                ShuffleReceiver::bind(7, std::net::SocketAddr::from(([127, 0, 0, 1], 0)), boot)
                    .await
                    .map_err(|error| DbError::Config(error.to_string()))?,
            );
            let deadline = Arc::new(LeaseDeadline::live_for(Duration::from_secs(300)));
            sender
                .bind_process_lease_deadline_pair(&receiver, Arc::clone(&deadline))
                .map_err(|error| DbError::Config(error.to_string()))?;
            sender
                .install_assignment_fence(&assignment, &owners)
                .map_err(|error| DbError::Config(error.to_string()))?;
            receiver
                .install_assignment_fence(&assignment, &owners)
                .map_err(|error| DbError::Config(error.to_string()))?;
            sender.set_recovery_gen(3);
            receiver.set_recovery_gen(3);
            operator.require_cluster_execution()?;
            operator.bind_startup_assignment(&assignment, &(0..VNODE_COUNT).collect::<Vec<_>>())?;
            operator.bind_process_execution_authority(
                &ClusterShuffleConfig {
                    registry,
                    sender,
                    receiver,
                    self_id: NodeId(7),
                    topology: None,
                },
                deadline,
            )?;
        }
        Ok(Self { operator })
    }

    /// Process a prepared Arrow batch through production routing, state validation and application.
    ///
    /// # Errors
    /// Returns the ordinary operator errors, including lost fixture authority and resource limits.
    pub async fn step(&mut self, batch: &RecordBatch) -> Result<Vec<RecordBatch>, DbError> {
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
}

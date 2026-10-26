//! In a cluster a view placed by its full row keeps each row on the owner of the row's vnode,
//! which is in general not the node that produced the row. The result node routes the view
//! stream's rows there: it applies its own vnodes' rows once the others are sent, and applies the
//! rows peers route to it as they arrive. Application on different nodes commutes, and each row's
//! insert and retraction come in order from the one node that owns its upstream group.

use arrow::array::RecordBatch;
use laminar_core::shuffle::{ShuffleMessage, ShuffleTopologyFence};

use crate::error::DbError;
use crate::mv_store::MvStore;
use crate::operator::sql_query::ClusterShuffleConfig;
use crate::operator::RetainedBatch;

type SendOutcome = (Result<(), DbError>, Option<Vec<(u64, ShuffleMessage)>>);

pub(super) struct ViewRouting {
    config: ClusterShuffleConfig,
    pending: Option<PendingSend>,
}

/// Rows routed to their owners and not yet admitted by the transport, with this node's own
/// rows, which apply only once the others are sent.
struct PendingSend {
    local: Vec<RecordBatch>,
    outbound: Option<Vec<(u64, ShuffleMessage)>>,
    assignment_version: u64,
    send: Option<(
        tokio::task::JoinHandle<()>,
        tokio::sync::oneshot::Receiver<SendOutcome>,
    )>,
}

impl Drop for PendingSend {
    fn drop(&mut self) {
        if let Some((send, _)) = &self.send {
            send.abort();
        }
    }
}

impl ViewRouting {
    pub(super) const fn new(config: ClusterShuffleConfig) -> Self {
        Self {
            config,
            pending: None,
        }
    }

    pub(super) fn bind_topology(&mut self, topology: ShuffleTopologyFence) {
        self.config.topology = Some(topology);
    }

    pub(super) const fn is_sending(&self) -> bool {
        self.pending.is_some()
    }

    /// Route one cycle's rows, or advance the send in flight.
    pub(super) fn process(
        &mut self,
        view: &str,
        store: &parking_lot::RwLock<MvStore>,
        batches: &[RecordBatch],
    ) -> Result<(), DbError> {
        if self.pending.is_some() {
            if batches.iter().any(|batch| batch.num_rows() != 0) {
                return Err(DbError::ShufflePartialSend(format!(
                    "view '{view}' received rows while routing earlier rows"
                )));
            }
            return self.advance_send(view, store);
        }
        if batches.iter().all(|batch| batch.num_rows() == 0) {
            return Ok(());
        }
        let assignment = self.config.topology_snapshot()?;
        let stage = laminar_core::mv::view_result_operator(view);
        let mut local = Vec::new();
        let mut outbound = Vec::new();
        {
            let store = store.read();
            for batch in batches.iter().filter(|batch| batch.num_rows() != 0) {
                let vnodes = store.row_vnodes(view, batch)?;
                let plan = laminar_core::shuffle::route_checkpointed_batch(
                    batch,
                    &vnodes,
                    &assignment,
                    self.config.self_id,
                )
                .map_err(|error| {
                    crate::operator::shuffle_routing_error(
                        &format!("view [{view}] routing"),
                        &error,
                    )
                })?;
                local.extend(plan.local.into_iter().map(|route| route.batch));
                outbound.extend(plan.remote.into_iter().map(|route| {
                    (
                        route.owner.0,
                        ShuffleMessage::checkpointed_routed(
                            stage.clone(),
                            route.routed_vnodes,
                            route.batch,
                        ),
                    )
                }));
            }
        }
        if outbound.is_empty() {
            return apply(view, store, &local);
        }
        self.pending = Some(PendingSend {
            local,
            outbound: Some(outbound),
            assignment_version: assignment.version(),
            send: None,
        });
        self.start_send(view);
        Ok(())
    }

    fn start_send(&mut self, view: &str) {
        let Some(pending) = self.pending.as_mut() else {
            return;
        };
        let Some(outbound) = pending.outbound.take() else {
            return;
        };
        let sender = std::sync::Arc::clone(&self.config.sender);
        let topology = self.config.topology;
        let assignment_version = pending.assignment_version;
        let wake = self.config.receiver.work_ready_notify();
        let context = format!("view [{view}] shuffle");
        let (completion_tx, completion) = tokio::sync::oneshot::channel();
        let send = tokio::spawn(async move {
            let outcome = crate::operator::send_shuffle_plan_retaining(
                &sender,
                topology,
                assignment_version,
                outbound,
                &context,
            )
            .await;
            let should_wake = !matches!(&outcome.0, Err(error) if error.is_shuffle_not_ready());
            if completion_tx.send(outcome).is_ok() && should_wake {
                wake.notify_one();
            }
        });
        pending.send = Some((send, completion));
    }

    /// Apply this node's rows once the send completes; keep the plan to retry a send the
    /// transport was not ready for.
    fn advance_send(
        &mut self,
        view: &str,
        store: &parking_lot::RwLock<MvStore>,
    ) -> Result<(), DbError> {
        let Some((_, completion)) = self
            .pending
            .as_mut()
            .and_then(|pending| pending.send.as_mut())
        else {
            self.start_send(view);
            return Ok(());
        };
        let outcome = match completion.try_recv() {
            Ok(outcome) => outcome,
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => return Ok(()),
            Err(tokio::sync::oneshot::error::TryRecvError::Closed) => {
                self.pending = None;
                return Err(DbError::ShufflePartialSend(format!(
                    "view '{view}' send task ended without a delivery outcome"
                )));
            }
        };
        let Some(mut pending) = self.pending.take() else {
            return Ok(());
        };
        pending.send = None;
        match outcome {
            (Ok(()), _) => apply(view, store, &pending.local).map_err(|error| {
                DbError::ShufflePartialSend(format!(
                    "view '{view}' failed after routing rows to their owners: {error}"
                ))
            }),
            (Err(error), Some(outbound)) if error.is_shuffle_not_ready() => {
                pending.outbound = Some(outbound);
                self.pending = Some(pending);
                Ok(())
            }
            (Err(error), _) => Err(error),
        }
    }

    /// Apply rows a peer routed here. Every row must fall on a vnode this node owns, among the
    /// vnodes the peer declared.
    pub(super) fn apply_received(
        &self,
        view: &str,
        store: &parking_lot::RwLock<MvStore>,
        retained: &RetainedBatch,
    ) -> Result<(), DbError> {
        let assignment = self.config.topology_snapshot()?;
        let routed = retained.routed_vnodes();
        let owned = |vnode: &u32| {
            assignment.owners().get(*vnode as usize) == Some(&self.config.self_id)
                && routed.binary_search(vnode).is_ok()
        };
        let batch = retained.batch();
        let vnodes = store.read().row_vnodes(view, batch)?;
        if batch.num_rows() == 0
            || !routed.windows(2).all(|pair| pair[0] < pair[1])
            || !routed.iter().all(owned)
            || !vnodes.iter().all(owned)
        {
            return Err(DbError::ShuffleTerminal(format!(
                "view '{view}' received rows outside the vnodes it owns"
            )));
        }
        apply(view, store, std::slice::from_ref(batch))
    }
}

fn apply(
    view: &str,
    store: &parking_lot::RwLock<MvStore>,
    batches: &[RecordBatch],
) -> Result<(), DbError> {
    store
        .write()
        .update_views(std::iter::once((view, batches)), false)
        .map_err(|error| {
            DbError::StatefulOperatorPartialApply(format!(
                "view '{view}' could not apply routed rows; recovery from the committed \
                 checkpoint is required: {error}"
            ))
        })
}

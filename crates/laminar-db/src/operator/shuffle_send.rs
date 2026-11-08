//! A managed operator's outbound shuffle frames for one cycle.
//!
//! The operator applies the cycle's local rows only once the send's outcome is known. Polling the
//! send in the cycle lets a send the transport admits without waiting finish there, so those rows
//! apply in the same cycle instead of after a task wakes the coordinator for another one.

use std::future::Future;
use std::sync::Arc;
use std::task::Poll;

use laminar_core::shuffle::ShuffleMessage;
use tokio::sync::oneshot;

use super::sql_query::ClusterShuffleConfig;
use crate::error::DbError;

/// A send's result, and the frames to send again when it failed before any frame was admitted.
pub(crate) type ShuffleSendOutcome = (Result<(), DbError>, Option<Vec<(u64, ShuffleMessage)>>);

/// The send of one cycle's outbound frames.
pub(crate) enum ShuffleSend {
    /// Every frame was admitted, or the send failed, while the cycle polled it.
    Completed(ShuffleSendOutcome),
    /// The send waited and finishes in a task.
    Running(RunningShuffleSend),
}

/// A send that finishes in a task, which wakes the coordinator unless the transport was not ready.
/// Dropping it aborts the task.
pub(crate) struct RunningShuffleSend {
    task: tokio::task::JoinHandle<()>,
    completion: oneshot::Receiver<ShuffleSendOutcome>,
}

impl Drop for RunningShuffleSend {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ShuffleSend {
    /// Sends `outbound` and polls the send once. A send that waits (for a connection, outbound
    /// budget or an encode on the blocking pool) keeps the progress it made, so a frame one peer
    /// already admitted is neither repeated nor lost, and finishes in a task.
    pub(crate) async fn start(
        config: &ClusterShuffleConfig,
        assignment_version: u64,
        outbound: Vec<(u64, ShuffleMessage)>,
        context: String,
    ) -> Self {
        let sender = Arc::clone(&config.sender);
        let topology = config.topology;
        let mut send = Box::pin(async move {
            super::send_shuffle_plan_retaining(
                &sender,
                topology,
                assignment_version,
                outbound,
                &context,
            )
            .await
        });
        if let Poll::Ready(outcome) =
            std::future::poll_fn(|cx| Poll::Ready(send.as_mut().poll(cx))).await
        {
            return Self::Completed(outcome);
        }
        let wake = config.receiver.work_ready_notify();
        let (completion_tx, completion) = oneshot::channel();
        let task = tokio::spawn(async move {
            let outcome = send.await;
            let should_wake = !matches!(&outcome.0, Err(error) if error.is_shuffle_not_ready());
            if completion_tx.send(outcome).is_ok() && should_wake {
                wake.notify_one();
            }
        });
        Self::Running(RunningShuffleSend { task, completion })
    }

    /// Takes the outcome of the send in `slot` once it has finished. A send still running stays
    /// in `slot` and gives `None`.
    pub(crate) fn take_finished(
        slot: &mut Option<Self>,
    ) -> Option<Result<ShuffleSendOutcome, &'static str>> {
        match slot.take()? {
            Self::Completed(outcome) => Some(Ok(outcome)),
            Self::Running(mut running) => match running.completion.try_recv() {
                Ok(outcome) => Some(Ok(outcome)),
                Err(oneshot::error::TryRecvError::Empty) => {
                    *slot = Some(Self::Running(running));
                    None
                }
                Err(oneshot::error::TryRecvError::Closed) => {
                    Some(Err("send task ended without a delivery outcome"))
                }
            },
        }
    }

    /// A send running `send` as its task.
    #[cfg(test)]
    pub(crate) fn spawned(send: impl Future<Output = ShuffleSendOutcome> + Send + 'static) -> Self {
        let (completion_tx, completion) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = completion_tx.send(send.await);
        });
        Self::Running(RunningShuffleSend { task, completion })
    }

    #[cfg(test)]
    pub(crate) fn is_finished(&self) -> bool {
        match self {
            Self::Completed(_) => true,
            Self::Running(running) => running.task.is_finished(),
        }
    }
}

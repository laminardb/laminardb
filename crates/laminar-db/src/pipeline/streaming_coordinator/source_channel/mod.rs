//! Byte admission for the shared connector FIFO, including parked messages and shutdown tails.

use std::sync::Arc;

use arrow_array::RecordBatch;
use crossfire::{mpsc, AsyncRx, MAsyncTx};
use laminar_core::streaming::StreamingError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use super::SourceMsg;

pub(super) struct QueuedSourceMsg {
    pub(super) message: SourceMsg,
    // Retained through dequeue/parking; staging transfers Arrow ownership to the cycle/graph.
    _permit: Option<OwnedSemaphorePermit>,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum SourceQueueError {
    #[error("source batch retains {bytes} Arrow bytes, exceeding source_queue_max_bytes={limit}")]
    Oversized { bytes: usize, limit: usize },
    #[error("source queue is full")]
    Full,
    #[error("source queue is closed")]
    Closed,
}

#[derive(Clone)]
pub(super) struct SourceMsgTx {
    tx: MAsyncTx<mpsc::Array<QueuedSourceMsg>>,
    budget: Arc<Semaphore>,
    max_bytes: usize,
}

pub(super) struct SourceMsgRx {
    rx: AsyncRx<mpsc::Array<QueuedSourceMsg>>,
    budget: Arc<Semaphore>,
}

pub(super) fn channel(capacity: usize, max_bytes: usize) -> (SourceMsgTx, SourceMsgRx) {
    let (tx, rx) = mpsc::bounded_async(capacity);
    let budget = Arc::new(Semaphore::new(max_bytes));
    (
        SourceMsgTx {
            tx,
            budget: Arc::clone(&budget),
            max_bytes,
        },
        SourceMsgRx { rx, budget },
    )
}

impl SourceMsgTx {
    fn batch_bytes(&self, batch: &RecordBatch) -> Result<u32, SourceQueueError> {
        // Arrow reports retained capacities, including backing storage for slices, nested arrays
        // and views. Charge aliases independently; deduplicating would add hot-path bookkeeping.
        let bytes = laminar_core::streaming::retained_arrow_bytes(batch);
        if bytes > self.max_bytes {
            return Err(SourceQueueError::Oversized {
                bytes,
                limit: self.max_bytes,
            });
        }
        u32::try_from(bytes).map_err(|_| SourceQueueError::Oversized {
            bytes,
            limit: self.max_bytes,
        })
    }

    // Bound the producer's one pending-cursor/waiting batch before retaining it. Connector decode
    // scratch exists before this boundary; it is not part of the shared queue budget.
    pub(super) fn validate_batch(&self, batch: RecordBatch) -> Result<RecordBatch, StreamingError> {
        self.batch_bytes(&batch)
            .map_err(|error| StreamingError::InvalidConfig(error.to_string()))?;
        Ok(batch)
    }

    pub(super) async fn send(&self, message: SourceMsg) -> Result<(), SourceQueueError> {
        let permit = match &message {
            SourceMsg::Batch { batch, .. } => {
                let bytes = self.batch_bytes(batch)?;
                Some(
                    Arc::clone(&self.budget)
                        .acquire_many_owned(bytes)
                        .await
                        .map_err(|_| SourceQueueError::Closed)?,
                )
            }
            // Control keeps the same FIFO; a full data-byte budget must not block a barrier
            // behind another source's byte waiter. Count saturation retains existing backpressure.
            SourceMsg::Barrier { .. } => None,
        };
        self.tx
            .send(QueuedSourceMsg {
                message,
                _permit: permit,
            })
            .await
            .map_err(|_| SourceQueueError::Closed)
    }

    pub(super) fn try_send(&self, message: SourceMsg) -> Result<(), SourceQueueError> {
        let permit = match &message {
            SourceMsg::Batch { batch, .. } => {
                let bytes = self.batch_bytes(batch)?;
                Some(
                    Arc::clone(&self.budget)
                        .try_acquire_many_owned(bytes)
                        .map_err(|error| match error {
                            TryAcquireError::NoPermits => SourceQueueError::Full,
                            TryAcquireError::Closed => SourceQueueError::Closed,
                        })?,
                )
            }
            SourceMsg::Barrier { .. } => None,
        };
        self.tx
            .try_send(QueuedSourceMsg {
                message,
                _permit: permit,
            })
            .map_err(|error| match error {
                crossfire::TrySendError::Full(_) => SourceQueueError::Full,
                crossfire::TrySendError::Disconnected(_) => SourceQueueError::Closed,
            })
    }
}

impl SourceMsgRx {
    pub(super) fn recv(
        &self,
    ) -> impl std::future::Future<Output = Result<QueuedSourceMsg, crossfire::RecvError>> + Send + '_
    {
        self.rx.recv()
    }

    pub(super) fn try_recv(&self) -> Result<QueuedSourceMsg, crossfire::TryRecvError> {
        self.rx.try_recv()
    }
}

impl Drop for SourceMsgRx {
    fn drop(&mut self) {
        // A dequeued/parked message may still hold capacity when the receiver disappears.
        // Wake byte waiters as well as the channel's own count waiters.
        self.budget.close();
    }
}

#[cfg(test)]
impl From<SourceMsg> for QueuedSourceMsg {
    fn from(message: SourceMsg) -> Self {
        Self {
            message,
            _permit: None,
        }
    }
}

#[cfg(test)]
mod tests;

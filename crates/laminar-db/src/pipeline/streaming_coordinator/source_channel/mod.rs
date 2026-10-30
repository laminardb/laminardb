//! Byte admission for the shared connector FIFO, including parked messages and shutdown tails.

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::RecordBatch;
use crossfire::{mpsc, AsyncRx, MAsyncTx};
use laminar_core::streaming::StreamingError;
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

use super::SourceMsg;

pub(super) struct QueuedSourceMsg {
    pub(super) message: SourceMsg,
    // Retained through dequeue and parking; the coordinator keeps it until a graph port admits
    // the staged batch.
    permit: Option<OwnedSemaphorePermit>,
}

impl QueuedSourceMsg {
    pub(super) fn into_parts(self) -> (SourceMsg, Option<OwnedSemaphorePermit>) {
        (self.message, self.permit)
    }
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

/// Nanoseconds each source spent waiting for queue bytes or a queue slot, by source name.
#[derive(Default)]
pub(super) struct SourceQueueWaits {
    sources: Mutex<Vec<(Arc<str>, Arc<AtomicU64>)>>,
}

impl SourceQueueWaits {
    fn counter(&self, source: &str) -> Arc<AtomicU64> {
        let mut sources = self.sources.lock();
        if let Some((_, waited)) = sources.iter().find(|(name, _)| &**name == source) {
            return Arc::clone(waited);
        }
        let waited = Arc::new(AtomicU64::new(0));
        sources.push((Arc::from(source), Arc::clone(&waited)));
        waited
    }

    pub(super) fn totals(&self) -> Vec<(Arc<str>, u64)> {
        self.sources
            .lock()
            .iter()
            .map(|(name, waited)| (Arc::clone(name), waited.load(Ordering::Relaxed)))
            .collect()
    }
}

/// Adds the time since the first wait began, also when the send is cancelled mid-wait.
struct QueueWait<'a> {
    waited_ns: Option<&'a AtomicU64>,
    started: Option<Instant>,
}

impl QueueWait<'_> {
    /// Await `operation`, reading the clock only if its first poll is pending; admission that
    /// is immediate stays untimed.
    async fn time<F: Future>(&mut self, operation: F) -> F::Output {
        let mut operation = std::pin::pin!(operation);
        std::future::poll_fn(|cx| {
            let poll = operation.as_mut().poll(cx);
            if poll.is_pending() && self.waited_ns.is_some() && self.started.is_none() {
                self.started = Some(Instant::now());
            }
            poll
        })
        .await
    }
}

impl Drop for QueueWait<'_> {
    fn drop(&mut self) {
        if let (Some(waited_ns), Some(started)) = (self.waited_ns, self.started) {
            let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            waited_ns.fetch_add(elapsed, Ordering::Relaxed);
        }
    }
}

#[derive(Clone)]
pub(super) struct SourceMsgTx {
    tx: MAsyncTx<mpsc::Array<QueuedSourceMsg>>,
    budget: Arc<Semaphore>,
    max_bytes: usize,
    waits: Arc<SourceQueueWaits>,
    waited_ns: Option<Arc<AtomicU64>>,
}

pub(super) struct SourceMsgRx {
    rx: AsyncRx<mpsc::Array<QueuedSourceMsg>>,
    budget: Arc<Semaphore>,
    waits: Arc<SourceQueueWaits>,
}

/// Scrape-time view of one queue. It never keeps a stopped queue alive; the wait counters
/// outlive it so totals survive a pipeline restart.
pub(super) struct QueueObservation {
    max_bytes: usize,
    budget: std::sync::Weak<Semaphore>,
    pub(super) waits: Arc<SourceQueueWaits>,
}

impl QueueObservation {
    pub(super) fn reserved_bytes(&self) -> usize {
        self.budget.upgrade().map_or(0, |budget| {
            self.max_bytes.saturating_sub(budget.available_permits())
        })
    }
}

pub(super) fn channel(capacity: usize, max_bytes: usize) -> (SourceMsgTx, SourceMsgRx) {
    let (tx, rx) = mpsc::bounded_async(capacity);
    let budget = Arc::new(Semaphore::new(max_bytes));
    let waits = Arc::new(SourceQueueWaits::default());
    (
        SourceMsgTx {
            tx,
            budget: Arc::clone(&budget),
            max_bytes,
            waits: Arc::clone(&waits),
            waited_ns: None,
        },
        SourceMsgRx { rx, budget, waits },
    )
}

impl SourceMsgTx {
    /// A producer handle whose waits are recorded under `source`.
    pub(super) fn for_source(&self, source: &str) -> Self {
        Self {
            waited_ns: Some(self.waits.counter(source)),
            ..self.clone()
        }
    }

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
        let mut wait = QueueWait {
            waited_ns: self.waited_ns.as_deref(),
            started: None,
        };
        let permit = match &message {
            SourceMsg::Batch { batch, .. } => {
                let bytes = self.batch_bytes(batch)?;
                let acquire = Arc::clone(&self.budget).acquire_many_owned(bytes);
                Some(
                    wait.time(acquire)
                        .await
                        .map_err(|_| SourceQueueError::Closed)?,
                )
            }
            // Control keeps the same FIFO; a full data-byte budget must not block a barrier
            // behind another source's byte waiter. Count saturation retains existing backpressure.
            SourceMsg::Barrier { .. } => None,
        };
        let send = self.tx.send(QueuedSourceMsg { message, permit });
        wait.time(send).await.map_err(|_| SourceQueueError::Closed)
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
            .try_send(QueuedSourceMsg { message, permit })
            .map_err(|error| match error {
                crossfire::TrySendError::Full(_) => SourceQueueError::Full,
                crossfire::TrySendError::Disconnected(_) => SourceQueueError::Closed,
            })
    }
}

impl SourceMsgRx {
    pub(super) fn observation(&self, max_bytes: usize) -> QueueObservation {
        QueueObservation {
            max_bytes,
            budget: Arc::downgrade(&self.budget),
            waits: Arc::clone(&self.waits),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.rx.len()
    }

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
            permit: None,
        }
    }
}

#[cfg(test)]
mod tests;

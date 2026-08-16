//! Detached connector-task admission and terminal lifetime tracking.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use tokio::sync::Notify;

/// What the runtime must do when a started connector operation is cancelled.
///
/// This is an internal connector/driver capability, not a deployment option.
/// Cancellation always respects the runtime-owned deadline. A connector may be
/// reused only when dropping the exact future is known to preserve its state;
/// otherwise the runtime retires the complete connector generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectorCancellationPolicy {
    /// Dropping an in-flight future leaves the connector valid for recovery or reuse.
    CancelSafe,
    /// Dropping an in-flight future may leave its external outcome unknown, so
    /// the connector instance must not process another operation.
    RetireConnector,
}

const CONNECTOR_TASK_OWNER_DROPPED: usize = 1usize << (usize::BITS - 1);

pub(super) struct ConnectorTaskState {
    state: AtomicUsize,
    terminated: Notify,
}

/// Sole admission authority for detached tasks owned by one connector generation.
///
/// The owner must live inside the connector. Dropping it seals the generation so
/// terminal completion can be observed after every admitted task guard is gone.
pub struct ConnectorTaskOwner {
    inner: Arc<ConnectorTaskState>,
}

/// Cloneable, non-owning admission handle for dynamically spawned connector tasks.
///
/// The handle does not keep the task generation open. Admission fails after
/// the sole [`ConnectorTaskOwner`] is dropped, including when existing task
/// guards still keep the generation observable.
#[derive(Clone)]
pub struct ConnectorTaskAdmission {
    pub(super) inner: Weak<ConnectorTaskState>,
}

/// Cloneable observer for terminal completion of one connector generation.
#[derive(Clone)]
pub struct ConnectorTaskTracker {
    inner: Arc<ConnectorTaskState>,
}

/// RAII proof that one connector-owned task is still active.
///
/// Move the guard into the task before spawning it and retain it for the task's
/// full lifetime.
#[must_use = "dropping the guard marks its connector task complete"]
pub struct ConnectorTaskGuard {
    inner: Arc<ConnectorTaskState>,
}

impl ConnectorTaskOwner {
    /// Create the sole task owner and its cloneable terminal observer.
    #[must_use]
    pub fn new() -> (Self, ConnectorTaskTracker) {
        let inner = Arc::new(ConnectorTaskState {
            state: AtomicUsize::new(0),
            terminated: Notify::new(),
        });
        (
            Self {
                inner: Arc::clone(&inner),
            },
            ConnectorTaskTracker { inner },
        )
    }

    /// Create a non-owning admission handle for tasks discovered by owned work.
    ///
    /// This is intended for accept loops and similar dynamic task producers.
    /// Cloning the handle never extends the connector generation's admission
    /// lifetime.
    #[must_use]
    pub fn admission(&self) -> ConnectorTaskAdmission {
        ConnectorTaskAdmission {
            inner: Arc::downgrade(&self.inner),
        }
    }

    /// Admit one task into this live connector generation.
    ///
    /// The returned guard must be created before the task is spawned and moved
    /// into that task. `None` means the generation can no longer admit work.
    #[must_use]
    pub fn track(&self) -> Option<ConnectorTaskGuard> {
        track_connector_task(&self.inner)
    }
}

impl ConnectorTaskAdmission {
    /// Admit one dynamic task while the connector generation remains open.
    ///
    /// Returns `None` once the sole owner has been dropped. A successful
    /// admission remains tracked until its returned guard is dropped.
    #[must_use]
    pub fn track(&self) -> Option<ConnectorTaskGuard> {
        let inner = self.inner.upgrade()?;
        track_connector_task(&inner)
    }
}

fn track_connector_task(inner: &Arc<ConnectorTaskState>) -> Option<ConnectorTaskGuard> {
    let mut observed = inner.state.load(Ordering::Acquire);
    loop {
        if observed & CONNECTOR_TASK_OWNER_DROPPED != 0 {
            return None;
        }
        let next = observed.checked_add(1)?;
        if next & CONNECTOR_TASK_OWNER_DROPPED != 0 {
            return None;
        }
        match inner
            .state
            .compare_exchange_weak(observed, next, Ordering::AcqRel, Ordering::Acquire)
        {
            Ok(_) => {
                return Some(ConnectorTaskGuard {
                    inner: Arc::clone(inner),
                });
            }
            Err(actual) => observed = actual,
        }
    }
}

impl Drop for ConnectorTaskOwner {
    fn drop(&mut self) {
        let previous = self
            .inner
            .state
            .fetch_or(CONNECTOR_TASK_OWNER_DROPPED, Ordering::AcqRel);
        debug_assert_eq!(previous & CONNECTOR_TASK_OWNER_DROPPED, 0);
        if previous == 0 {
            self.inner.terminated.notify_waiters();
        }
    }
}

impl ConnectorTaskTracker {
    /// Whether the owner and all task guards have been dropped.
    #[must_use]
    pub fn is_terminated(&self) -> bool {
        self.inner.state.load(Ordering::Acquire) == CONNECTOR_TASK_OWNER_DROPPED
    }

    /// Wait until the owner and all task guards have been dropped.
    pub async fn wait_terminated(&self) {
        loop {
            let notified = self.inner.terminated.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_terminated() {
                return;
            }
            notified.await;
        }
    }
}

impl Drop for ConnectorTaskGuard {
    fn drop(&mut self) {
        let previous = self.inner.state.fetch_sub(1, Ordering::AcqRel);
        debug_assert_ne!(previous & !CONNECTOR_TASK_OWNER_DROPPED, 0);
        if previous == CONNECTOR_TASK_OWNER_DROPPED | 1 {
            self.inner.terminated.notify_waiters();
        }
    }
}

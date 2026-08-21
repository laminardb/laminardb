//! Sink actor ownership: admission/close/terminal state, the detached actor
//! future, spawn supervision, and the re-spawn policy after an actor exits.

//! INVARIANT: the actor future detaches from the supervisor so a panic or
//! drop cannot cancel in-flight connector I/O; only close/terminal states
//! retire it.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use laminar_connectors::connector::ConnectorTaskTracker;
use laminar_connectors::error::ConnectorError;
use tokio::task::JoinHandle;
use tokio::time::Instant;

#[derive(Clone)]
pub(super) enum SinkCloseOutcome {
    Success,
    Failure(Arc<str>),
}

impl SinkCloseOutcome {
    pub(super) fn into_result(self) -> Result<(), ConnectorError> {
        match self {
            Self::Success => Ok(()),
            Self::Failure(error) => Err(ConnectorError::Internal(error.to_string())),
        }
    }
}

pub(super) struct SinkCloseState {
    pub(super) phase: parking_lot::Mutex<&'static str>,
    pub(super) outcome: parking_lot::Mutex<Option<SinkCloseOutcome>>,
    pub(super) notify: tokio::sync::Notify,
}

pub(super) struct SinkTerminalState {
    pub(super) actor: Arc<SinkActorState>,
    pub(super) connector_tasks: Option<ConnectorTaskTracker>,
}

pub(super) struct SinkActorState {
    pub(super) accepting: AtomicBool,
    pub(super) finished: AtomicBool,
    pub(super) finished_notify: tokio::sync::Notify,
}

impl SinkActorState {
    pub(super) fn new() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            finished: AtomicBool::new(false),
            finished_notify: tokio::sync::Notify::new(),
        }
    }

    pub(super) fn stop_admission(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    pub(super) fn finish(&self) {
        self.stop_admission();
        if !self.finished.swap(true, Ordering::AcqRel) {
            self.finished_notify.notify_waiters();
        }
    }

    async fn wait_finished_until(&self, deadline: Instant) -> bool {
        loop {
            let notified = self.finished_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.finished.load(Ordering::Acquire) {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified.as_mut())
                .await
                .is_err()
            {
                return self.finished.load(Ordering::Acquire);
            }
        }
    }
}

#[cfg(test)]
pub(super) struct SinkActorLifetime(pub(super) Arc<SinkActorState>);

#[cfg(test)]
impl Drop for SinkActorLifetime {
    fn drop(&mut self) {
        self.0.finish();
    }
}

pub(super) struct SinkActorFuture<F> {
    actor: Option<std::pin::Pin<Box<F>>>,
    terminal: Arc<SinkActorState>,
}

// Moving this wrapper never moves the separately pinned actor allocation.
impl<F> Unpin for SinkActorFuture<F> {}

impl<F> std::future::Future for SinkActorFuture<F>
where
    F: std::future::Future<Output = ()>,
{
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let actor = self
            .actor
            .as_mut()
            .expect("sink actor polled after terminal completion");
        if actor.as_mut().poll(context).is_pending() {
            return std::task::Poll::Pending;
        }
        // Drop the complete actor future, including its connector, before publishing exit.
        self.actor.take();
        self.terminal.finish();
        std::task::Poll::Ready(())
    }
}

impl<F> Drop for SinkActorFuture<F> {
    fn drop(&mut self) {
        // Cancellation must drop the actor and its connector before another generation can observe
        // terminal completion.
        self.actor.take();
        self.terminal.finish();
    }
}

pub(super) fn spawn_sink_actor<F>(
    runtime: &tokio::runtime::Handle,
    actor: F,
    terminal: Arc<SinkActorState>,
) -> tokio::task::JoinHandle<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    runtime.spawn(SinkActorFuture {
        actor: Some(Box::pin(actor)),
        terminal,
    })
}

impl SinkCloseState {
    pub(super) fn new() -> Self {
        Self {
            phase: parking_lot::Mutex::new("admission"),
            outcome: parking_lot::Mutex::new(None),
            notify: tokio::sync::Notify::new(),
        }
    }

    pub(super) fn set_phase(&self, phase: &'static str) {
        *self.phase.lock() = phase;
    }

    pub(super) fn phase(&self) -> &'static str {
        *self.phase.lock()
    }

    pub(super) fn publish_outcome(&self, outcome: SinkCloseOutcome) {
        let mut current = self.outcome.lock();
        if current.is_none() {
            *current = Some(outcome);
        }
        drop(current);
        self.notify.notify_waiters();
    }

    pub(super) fn finish(&self, outcome: SinkCloseOutcome) {
        self.publish_outcome(outcome);
    }

    pub(super) fn outcome(&self) -> Option<SinkCloseOutcome> {
        self.outcome.lock().clone()
    }
}

impl SinkTerminalState {
    pub(super) fn new(
        actor: Arc<SinkActorState>,
        connector_tasks: Option<ConnectorTaskTracker>,
    ) -> Self {
        Self {
            actor,
            connector_tasks,
        }
    }

    pub(super) fn is_finished(&self) -> bool {
        self.actor.finished.load(Ordering::Acquire)
            && self
                .connector_tasks
                .as_ref()
                .is_none_or(ConnectorTaskTracker::is_terminated)
    }

    pub(super) async fn wait_until(&self, deadline: Instant) -> bool {
        if !self.actor.wait_finished_until(deadline).await {
            return false;
        }
        let Some(tasks) = self.connector_tasks.as_ref() else {
            return true;
        };
        if tasks.is_terminated() {
            return true;
        }
        if tokio::time::timeout_at(deadline, tasks.wait_terminated())
            .await
            .is_err()
        {
            return tasks.is_terminated();
        }
        true
    }
}

pub(super) struct OwnedSinkTask {
    pub(super) actor_abort: tokio::task::AbortHandle,
    pub(super) terminal_join: JoinHandle<Result<(), Arc<str>>>,
    pub(super) terminal_state: Arc<SinkTerminalState>,
}

impl OwnedSinkTask {
    pub(super) fn abort_actor(&self) {
        self.actor_abort.abort();
    }
}

pub(super) fn supervise_sink_task(
    actor: JoinHandle<()>,
    terminal_tasks: Option<ConnectorTaskTracker>,
    actor_state: Arc<SinkActorState>,
    runtime: &tokio::runtime::Handle,
) -> OwnedSinkTask {
    let actor_abort = actor.abort_handle();
    let terminal_state = Arc::new(SinkTerminalState::new(actor_state, terminal_tasks.clone()));
    let terminal_join = runtime.spawn(async move {
        let actor_result = actor.await.map_err(|error| Arc::from(error.to_string()));
        if let Some(tasks) = terminal_tasks {
            tasks.wait_terminated().await;
        }
        actor_result
    });
    OwnedSinkTask {
        actor_abort,
        terminal_join,
        terminal_state,
    }
}

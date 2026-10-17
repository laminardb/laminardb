//! One OS thread per connector generation for blocking MQI calls.
//!
//! The thread creates its session, runs one request at a time, and drops the session when the
//! request channel closes; the session's `Drop` backs out before disconnecting. An async deadline
//! only stops waiting: an MQI call already running finishes on the thread, and the generation
//! that gave up on it is terminal.

use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TrySendError};
use std::time::Duration;

use tokio::sync::oneshot;

use crate::connector::ConnectorTaskGuard;
use crate::error::ConnectorError;

type Request<S> = Box<dyn FnOnce(&mut S) + Send>;

/// Owner of a worker thread and its session `S`.
pub(super) struct Worker<S> {
    requests: Option<SyncSender<Request<S>>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl<S: 'static> Worker<S> {
    /// Start the thread and wait (bounded) for `open` to create the session on it.
    pub(super) async fn start(
        name: &str,
        guard: ConnectorTaskGuard,
        timeout: Duration,
        open: impl FnOnce() -> Result<S, ConnectorError> + Send + 'static,
    ) -> Result<Self, ConnectorError> {
        let (requests, inbox) = sync_channel::<Request<S>>(1);
        let (ready, opened) = oneshot::channel();
        let thread = std::thread::Builder::new()
            .name(name.to_string())
            .spawn(move || {
                let _guard = guard;
                run(open, &inbox, ready);
            })
            .map_err(|error| ConnectorError::Internal(format!("IBM MQ worker thread: {error}")))?;
        let worker = Self {
            requests: Some(requests),
            thread: Some(thread),
        };
        match tokio::time::timeout(timeout, opened).await {
            Ok(Ok(Ok(()))) => Ok(worker),
            Ok(Ok(Err(error))) => Err(error),
            Ok(Err(_)) => Err(ConnectorError::Internal(
                "IBM MQ worker stopped while connecting".into(),
            )),
            Err(_) => Err(ConnectorError::Timeout(millis(timeout))),
        }
    }

    /// Run `operation` on the worker thread, waiting at most `timeout` for its result.
    pub(super) async fn call<T: Send + 'static>(
        &self,
        operation: &'static str,
        timeout: Duration,
        work: impl FnOnce(&mut S) -> Result<T, ConnectorError> + Send + 'static,
    ) -> Result<T, ConnectorError> {
        self.call_or(operation, timeout, work, |_, _| {}).await
    }

    /// Like [`Self::call`], running `lost` on the worker thread when nobody is waiting for the
    /// result any more (the caller timed out or its future was dropped).
    pub(super) async fn call_or<T: Send + 'static>(
        &self,
        operation: &'static str,
        timeout: Duration,
        work: impl FnOnce(&mut S) -> Result<T, ConnectorError> + Send + 'static,
        lost: impl FnOnce(&mut S, Result<T, ConnectorError>) + Send + 'static,
    ) -> Result<T, ConnectorError> {
        let requests = self
            .requests
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "a running IBM MQ worker".into(),
                actual: "stopped".into(),
            })?;
        let (reply, result) = oneshot::channel();
        let request: Request<S> = Box::new(move |session| {
            if let Err(result) = reply.send(work(session)) {
                lost(session, result);
            }
        });
        requests.try_send(request).map_err(|error| match error {
            TrySendError::Full(_) => ConnectorError::InvalidState {
                expected: "an idle IBM MQ worker".into(),
                actual: "an earlier MQI call outlived its deadline".into(),
            },
            TrySendError::Disconnected(_) => ConnectorError::InvalidState {
                expected: "a running IBM MQ worker".into(),
                actual: "the worker thread exited".into(),
            },
        })?;
        match tokio::time::timeout(timeout, result).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ConnectorError::Internal(format!(
                "IBM MQ worker ended during {operation}"
            ))),
            Err(_) => Err(ConnectorError::outcome_unknown(
                format!("{operation} did not return within {}ms", millis(timeout)),
                true,
            )),
        }
    }

    /// Close the request channel and wait (bounded) for the session to back out and disconnect.
    pub(super) async fn stop(mut self, timeout: Duration) -> Result<(), ConnectorError> {
        self.requests.take();
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        let joined = tokio::task::spawn_blocking(move || thread.join());
        match tokio::time::timeout(timeout, joined).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(_))) => Err(ConnectorError::Internal("IBM MQ worker panicked".into())),
            Ok(Err(error)) => Err(ConnectorError::Internal(format!(
                "IBM MQ worker join: {error}"
            ))),
            Err(_) => Err(ConnectorError::Timeout(millis(timeout))),
        }
    }
}

fn run<S>(
    open: impl FnOnce() -> Result<S, ConnectorError>,
    inbox: &Receiver<Request<S>>,
    ready: oneshot::Sender<Result<(), ConnectorError>>,
) {
    let mut session = match open() {
        Ok(session) => session,
        Err(error) => {
            let _ = ready.send(Err(error));
            return;
        }
    };
    if ready.send(Ok(())).is_err() {
        return;
    }
    // Ends when the owner drops its sender; `session` then drops on this thread.
    while let Ok(request) = inbox.recv() {
        request(&mut session);
    }
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

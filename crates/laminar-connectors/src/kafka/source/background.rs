//! Blocking Kafka work and bounded background-task retirement.

use super::{
    debug, warn, Arc, AsyncMutex, AtomicBool, ConnectorTaskGuard, ConnectorTaskOwner, Ordering,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KafkaBlockingTaskError {
    Retired,
    WorkerDropped,
}

impl std::fmt::Display for KafkaBlockingTaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retired => f.write_str("Kafka connector generation retired"),
            Self::WorkerDropped => f.write_str("Kafka blocking worker ended without a result"),
        }
    }
}

/// Retains every synchronous librdkafka call started by one source generation.
///
/// Tokio cannot cancel a `spawn_blocking` closure after it starts. Keeping its
/// handle here lets close abort queued work and reap completed joins. The
/// connector's generic terminal tracker, retained by `terminal_guard`, is the
/// replacement fence when a native call outlives its calling future.
#[derive(Clone)]
pub(super) struct KafkaBlockingTasks {
    pub(super) retired: Arc<AtomicBool>,
    handles: Arc<AsyncMutex<Vec<tokio::task::JoinHandle<()>>>>,
    reaper_started: Arc<AtomicBool>,
    terminal_guard: Arc<ConnectorTaskGuard>,
}

impl KafkaBlockingTasks {
    pub(super) fn new(terminal_guard: ConnectorTaskGuard) -> Self {
        Self {
            retired: Arc::new(AtomicBool::new(false)),
            handles: Arc::new(AsyncMutex::new(Vec::new())),
            reaper_started: Arc::new(AtomicBool::new(false)),
            terminal_guard: Arc::new(terminal_guard),
        }
    }

    pub(super) async fn run<T, F>(&self, operation: F) -> Result<T, KafkaBlockingTaskError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        if self.retired.load(Ordering::Acquire) {
            return Err(KafkaBlockingTaskError::Retired);
        }

        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let mut handles = self.handles.lock().await;
        if self.retired.load(Ordering::Acquire) {
            return Err(KafkaBlockingTaskError::Retired);
        }
        // This under-lock check is the admission seal: after retirement no
        // code path may clone the grouped terminal guard for provider work.
        let retired = Arc::clone(&self.retired);
        let terminal_guard = Arc::clone(&self.terminal_guard);
        handles.push(tokio::task::spawn_blocking(move || {
            let _terminal_guard = terminal_guard;
            if retired.load(Ordering::Acquire) {
                let _ = result_tx.send(Err(KafkaBlockingTaskError::Retired));
                return;
            }
            let result = operation();
            if retired.load(Ordering::Acquire) {
                let _ = result_tx.send(Err(KafkaBlockingTaskError::Retired));
            } else {
                let _ = result_tx.send(Ok(result));
            }
        }));
        drop(handles);

        let result = result_rx
            .await
            .map_err(|_| KafkaBlockingTaskError::WorkerDropped)?;
        self.reap_finished().await;
        result
    }

    pub(super) async fn reap_finished(&self) {
        let completed = {
            let mut handles = self.handles.lock().await;
            let mut completed = Vec::new();
            let mut index = 0;
            while index < handles.len() {
                if handles[index].is_finished() {
                    completed.push(handles.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            completed
        };
        for handle in completed {
            if let Err(error) = handle.await {
                warn!(%error, "Kafka blocking worker failed");
            }
        }
    }

    pub(super) fn retire(&self) {
        self.retired.store(true, Ordering::Release);
    }

    pub(super) async fn join_until(&self, deadline: tokio::time::Instant) -> bool {
        self.retire();
        let mut handles = self.handles.lock().await;
        for handle in handles.iter() {
            handle.abort();
        }
        while let Some(handle) = handles.first_mut() {
            match tokio::time::timeout_at(deadline, handle).await {
                Ok(result) => {
                    if let Err(error) = result {
                        debug!(%error, "Kafka blocking worker cancelled during retirement");
                    }
                    handles.swap_remove(0);
                }
                Err(_) => return false,
            }
        }
        self.reaper_started.store(true, Ordering::Release);
        true
    }

    pub(super) fn ensure_reaper(&self) {
        self.retire();
        if self.reaper_started.swap(true, Ordering::AcqRel) {
            return;
        }
        // Retirement is published before this lock. If the list is empty while
        // holding it, no racing `run` can pass its under-lock retirement check
        // and install a later handle.
        if self
            .handles
            .try_lock()
            .is_ok_and(|handles| handles.is_empty())
        {
            return;
        }
        let generation = self.clone();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            self.reaper_started.store(false, Ordering::Release);
            warn!("Kafka blocking generation retired outside a Tokio runtime");
            return;
        };
        drop(runtime.spawn(async move {
            let mut handles = generation.handles.lock().await;
            for handle in handles.iter() {
                handle.abort();
            }
            while let Some(handle) = handles.first_mut() {
                if let Err(error) = handle.await {
                    debug!(%error, "Kafka blocking worker cancelled during reaping");
                }
                handles.swap_remove(0);
            }
        }));
    }

    /// Admit only terminal destruction after retirement. This does not run a
    /// provider operation or expose a result to the retired connector.
    pub(super) fn spawn_final_drop<T: Send + Sync + 'static>(
        &self,
        owner: Arc<T>,
        resource: &'static str,
    ) {
        let terminal_guard = Arc::clone(&self.terminal_guard);
        let reap = move || {
            let _terminal_guard = terminal_guard;
            while Arc::strong_count(&owner) > 1 {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            drop(owner);
        };
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            drop(runtime.spawn_blocking(reap));
        } else {
            warn!(resource, "Kafka resource retired outside a Tokio runtime");
            if let Err(error) = std::thread::Builder::new()
                .name("laminardb-kafka-resource-drop".into())
                .spawn(reap)
            {
                tracing::error!(resource, %error, "failed to start Kafka resource teardown thread");
            }
        }
    }
}

pub(super) async fn join_background_task(
    handle: &mut Option<tokio::task::JoinHandle<()>>,
    deadline: tokio::time::Instant,
    task: &'static str,
) {
    let Some(owned) = handle.as_mut() else {
        return;
    };
    let completed = match tokio::time::timeout_at(deadline, &mut *owned).await {
        Ok(Ok(())) => true,
        Ok(Err(error)) => {
            warn!(task, %error, "Kafka background task failed during shutdown");
            true
        }
        Err(_) => {
            warn!(
                task,
                "Kafka background task shutdown timed out; aborting it"
            );
            owned.abort();
            false
        }
    };
    if completed {
        *handle = None;
    }
}

#[cfg(test)]
mod tests;

pub(super) fn ensure_background_task_reaper(
    handle: tokio::task::JoinHandle<()>,
    task_owner: &ConnectorTaskOwner,
    task: &'static str,
) {
    handle.abort();
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        warn!(
            task,
            "Kafka background task retired outside a Tokio runtime"
        );
        return;
    };
    let Some(terminal_guard) = task_owner.track() else {
        warn!(
            task,
            "Kafka task generation was sealed before reader reaping"
        );
        return;
    };
    drop(runtime.spawn(async move {
        let _terminal_guard = terminal_guard;
        if let Err(error) = handle.await {
            debug!(task, %error, "Kafka retired background task reaped");
        }
    }));
}

/// Keep one owner on Tokio's blocking pool until all async task owners drain, then perform the
/// potentially blocking final drop there. The generation tracker retains the blocking handle if
/// the caller's close deadline expires.
pub(super) async fn reap_last_arc_off_runtime<T: Send + Sync + 'static>(
    blocking_tasks: &KafkaBlockingTasks,
    owner: Arc<T>,
    deadline: tokio::time::Instant,
    resource: &'static str,
) {
    let reaper = blocking_tasks.run(move || {
        while Arc::strong_count(&owner) > 1 {
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        drop(owner);
    });
    match tokio::time::timeout_at(deadline, reaper).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => warn!(resource, %error, "Kafka blocking reaper failed"),
        Err(_) => warn!(
            resource,
            "Kafka resource cleanup exceeded close deadline; generation reaper retained it"
        ),
    }
}

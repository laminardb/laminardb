//! Single-node server shutdown and process-worker failure handling.

use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "process-remote")]
use futures::stream::FuturesUnordered;
#[cfg(feature = "process-remote")]
use futures::StreamExt as _;
use tracing::{info, warn};

use super::{wait_for_termination_signal, ServerError, SingleServerRuntime};

const SERVER_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) async fn abort_and_join_server_task<T>(
    task: &mut tokio::task::JoinHandle<T>,
    task_name: &'static str,
) -> bool {
    task.abort();
    match tokio::time::timeout(SERVER_TASK_SHUTDOWN_TIMEOUT, task).await {
        Ok(Ok(_)) => true,
        Ok(Err(error)) if error.is_cancelled() => true,
        Ok(Err(error)) => {
            warn!(task = task_name, %error, "Server task failed during shutdown");
            false
        }
        Err(_) => {
            warn!(
                task = task_name,
                timeout = ?SERVER_TASK_SHUTDOWN_TIMEOUT,
                "Server task did not stop within the shutdown bound"
            );
            false
        }
    }
}

impl SingleServerRuntime {
    pub(super) async fn wait_for_shutdown(&mut self) -> Result<(), ServerError> {
        #[cfg(feature = "process-remote")]
        let termination = {
            let worker_exit = async {
                let mut exits = self
                    .process_workers
                    .iter()
                    .enumerate()
                    .map(|(index, worker)| async move {
                        worker.wait_for_exit().await;
                        index
                    })
                    .collect::<FuturesUnordered<_>>();
                exits.next().await
            };
            tokio::select! {
                result = wait_for_termination_signal() => result.map(|()| None),
                index = worker_exit, if !self.process_workers.is_empty() => Ok(index),
            }
        };
        #[cfg(not(feature = "process-remote"))]
        let termination: Result<Option<usize>, ServerError> =
            wait_for_termination_signal().await.map(|()| None);

        let primary = match termination {
            Ok(None) => {
                info!("Received shutdown signal, shutting down...");
                None
            }
            Ok(Some(index)) => Some(format!(
                "process worker {index} exited; restart the server to restore committed state"
            )),
            Err(ServerError::Shutdown(message)) => Some(message),
            Err(error) => Some(error.to_string()),
        };
        self.serving_gate.fence();

        let watcher_handle = &mut self.watcher_handle;
        let pgwire_handle = &mut self.pgwire_handle;
        let api_handle = &mut self.api_handle;
        let (watcher_stopped, pgwire_stopped, api_stopped) = tokio::join!(
            async {
                if let Some(handle) = watcher_handle.as_mut() {
                    abort_and_join_server_task(handle, "configuration watcher").await
                } else {
                    true
                }
            },
            async {
                if let Some(handle) = pgwire_handle.as_mut() {
                    abort_and_join_server_task(handle, "PostgreSQL wire server").await
                } else {
                    true
                }
            },
            abort_and_join_server_task(api_handle, "HTTP API server"),
        );

        let shutdown_result = self.db.shutdown().await;
        self.db_shutdown_complete = shutdown_result.is_ok();
        #[cfg(feature = "process-remote")]
        let worker_shutdown =
            crate::process_functions::shutdown(std::mem::take(&mut self.process_workers)).await;
        let mut shutdown_errors = Vec::new();
        if let Err(error) = shutdown_result {
            shutdown_errors.push(format!("database: {error}"));
        }
        #[cfg(feature = "process-remote")]
        if let Err(error) = worker_shutdown {
            shutdown_errors.push(format!("process workers: {error}"));
        }
        if !(watcher_stopped && pgwire_stopped && api_stopped) {
            shutdown_errors.push("one or more server tasks did not terminate cleanly".into());
        }
        if let Some(primary) = primary {
            shutdown_errors.insert(0, primary);
        }
        if !shutdown_errors.is_empty() {
            return Err(ServerError::Shutdown(shutdown_errors.join("; ")));
        }

        info!("Shutdown complete");
        Ok(())
    }
}

impl Drop for SingleServerRuntime {
    fn drop(&mut self) {
        self.serving_gate.fence();
        if !self.db_shutdown_complete {
            self.db.close();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let db = Arc::clone(&self.db);
                drop(runtime.spawn(async move {
                    if let Err(error) = db.shutdown().await {
                        warn!(%error, "Database cleanup after server handle drop failed");
                    }
                }));
            }
        }
        if let Some(handle) = &self.watcher_handle {
            handle.abort();
        }
        if let Some(handle) = &self.pgwire_handle {
            handle.abort();
        }
        self.api_handle.abort();
    }
}

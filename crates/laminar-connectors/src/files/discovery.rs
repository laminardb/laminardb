//! File discovery engine (Ring 2).
//!
//! Watches a directory for new files and emits [`DiscoveredFile`] events
//! to the source connector via an `mpsc` channel. Supports two modes:
//!
//! - **Event mode**: `notify::recommended_watcher()` for local filesystems
//! - **Poll mode**: `notify::PollWatcher` for NFS/CIFS/FUSE mounts

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossfire::{mpsc, AsyncRx, MAsyncTx, MTx};
use tracing::{debug, error, info};

use super::manifest::FileInventorySnapshot;
use crate::connector::ConnectorTaskGuard;
use crate::error::ConnectorError;

struct InitialScanInput {
    watch_dir: String,
    glob_matcher: Option<globset::GlobMatcher>,
    known: Arc<FileInventorySnapshot>,
}

/// A file discovered by the discovery engine.
#[derive(Debug, Clone)]
pub struct DiscoveredFile {
    /// Full local path to the file.
    pub path: String,
    /// File size in bytes.
    pub size: u64,
    /// Last modification time (millis since epoch).
    pub modified_ms: u64,
}

/// Configuration for the discovery engine.
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// Local directory path to watch.
    pub path: String,
    /// Polling interval for poll mode.
    pub poll_interval: Duration,
    /// Stabilisation delay after last modify event (event mode).
    pub stabilisation_delay: Duration,
    /// Glob pattern for filtering file names.
    pub glob_pattern: Option<String>,
}

/// Handle to a running discovery engine.
///
/// Drop aborts async discovery. An already-running blocking inventory scan
/// finishes under its task guard; `abort_and_join_until` is the bounded
/// shutdown path.
pub struct FileDiscoveryEngine {
    /// Channel receiver for discovered files.
    rx: AsyncRx<mpsc::Array<DiscoveredFile>>,
    /// Background task. Aborted on Drop so the docstring promise holds —
    /// plain `JoinHandle::drop` would detach the task, leaking it.
    handle: Option<tokio::task::JoinHandle<Result<(), ConnectorError>>>,
    /// Blocking initial inventory task, retained so close cannot detach it.
    initial_scan_handle: Option<tokio::task::JoinHandle<()>>,
    /// Persistent diagnostic once the task has terminated unexpectedly.
    terminal_error: Option<String>,
}

impl Drop for FileDiscoveryEngine {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
        if let Some(handle) = &self.initial_scan_handle {
            handle.abort();
        }
    }
}

impl FileDiscoveryEngine {
    /// Starts a discovery engine for the given config.
    ///
    /// Files already present in `known_files` are skipped.
    pub(super) fn start(
        config: DiscoveryConfig,
        known_files: Arc<FileInventorySnapshot>,
        task_guard: ConnectorTaskGuard,
        initial_scan_guard: ConnectorTaskGuard,
    ) -> Self {
        let (tx, rx) = mpsc::bounded_async::<DiscoveredFile>(256);
        let (initial_scan_tx, initial_scan_rx) = std::sync::mpsc::channel::<InitialScanInput>();
        let (initial_result_tx, initial_result_rx) = tokio::sync::oneshot::channel();
        let initial_scan_handle = tokio::task::spawn_blocking(move || {
            let _scan_guard = initial_scan_guard;
            let Ok(input) = initial_scan_rx.recv() else {
                return;
            };
            let result =
                scan_initial_inventory(&input.watch_dir, input.glob_matcher.as_ref(), &input.known);
            let _ = initial_result_tx.send(result);
        });
        let handle = tokio::spawn(async move {
            let _task_guard = task_guard;
            let result =
                local_discovery_loop(config, tx, known_files, initial_scan_tx, initial_result_rx)
                    .await;
            if let Err(error) = &result {
                error!(%error, "file discovery loop failed");
            }
            result
        });

        Self {
            rx,
            handle: Some(handle),
            initial_scan_handle: Some(initial_scan_handle),
            terminal_error: None,
        }
    }

    /// Drains available discovered files (non-blocking).
    ///
    /// Returns up to `max` files.
    ///
    /// # Errors
    ///
    /// Returns a non-transient error after the discovery task terminates.
    pub async fn drain(&mut self, max: usize) -> Result<Vec<DiscoveredFile>, ConnectorError> {
        self.check_terminal_failure().await?;
        let mut files = Vec::with_capacity(max.min(64));
        for _ in 0..max {
            match self.rx.try_recv() {
                Ok(file) => files.push(file),
                Err(_) => break,
            }
        }
        self.check_terminal_failure().await?;
        Ok(files)
    }

    /// Aborts discovery and joins the task without detaching it on timeout.
    ///
    /// # Errors
    ///
    /// Returns an error when either owned task fails or misses `deadline`.
    pub(super) async fn abort_and_join_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        let discovery_result = self.abort_discovery_until(deadline).await;
        let initial_scan_result = self.abort_initial_scan_until(deadline).await;

        initial_scan_result?;
        discovery_result?;
        if let Some(error) = &self.terminal_error {
            return Err(Self::terminal_connector_error(error.clone()));
        }
        Ok(())
    }

    async fn abort_discovery_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        let Some(handle) = self.handle.as_mut() else {
            return Ok(());
        };

        handle.abort();
        let wait = deadline.saturating_duration_since(tokio::time::Instant::now());
        let result = match tokio::time::timeout_at(deadline, handle).await {
            Ok(result) => result,
            Err(_) => {
                return Err(ConnectorError::Timeout(wait.as_millis() as u64));
            }
        };
        self.handle.take();
        if tokio::time::Instant::now() >= deadline {
            return Err(ConnectorError::Timeout(wait.as_millis() as u64));
        }

        match result {
            Err(error) if error.is_cancelled() => Ok(()),
            Ok(Err(error)) => Err(self.remember_terminal_error(&error)),
            Ok(Ok(())) => Err(self.remember_terminal_error(&ConnectorError::ReadError(
                "file discovery task terminated unexpectedly".into(),
            ))),
            Err(error) => Err(
                self.remember_terminal_error(&ConnectorError::Internal(format!(
                    "file discovery task failed: {error}"
                ))),
            ),
        }
    }

    async fn abort_initial_scan_until(
        &mut self,
        deadline: tokio::time::Instant,
    ) -> Result<(), ConnectorError> {
        let Some(handle) = self.initial_scan_handle.as_mut() else {
            return Ok(());
        };

        handle.abort();
        let wait = deadline.saturating_duration_since(tokio::time::Instant::now());
        let result = match tokio::time::timeout_at(deadline, handle).await {
            Ok(result) => result,
            Err(_) => {
                return Err(ConnectorError::Timeout(wait.as_millis() as u64));
            }
        };
        self.initial_scan_handle.take();
        if tokio::time::Instant::now() >= deadline {
            return Err(ConnectorError::Timeout(wait.as_millis() as u64));
        }

        match result {
            Ok(()) => Ok(()),
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(ConnectorError::Internal(format!(
                "file discovery initial scan task failed: {error}"
            ))),
        }
    }

    async fn check_terminal_failure(&mut self) -> Result<(), ConnectorError> {
        if let Some(error) = &self.terminal_error {
            return Err(Self::terminal_connector_error(error.clone()));
        }
        if !self
            .handle
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            return Ok(());
        }

        let result = self
            .handle
            .take()
            .expect("finished discovery task handle was present")
            .await;
        let error = match result {
            Ok(Err(error)) => error,
            Ok(Ok(())) => {
                ConnectorError::ReadError("file discovery task terminated unexpectedly".into())
            }
            Err(error) => ConnectorError::Internal(format!(
                "file discovery task failed unexpectedly: {error}"
            )),
        };
        Err(self.remember_terminal_error(&error))
    }

    fn remember_terminal_error(&mut self, error: &ConnectorError) -> ConnectorError {
        let error = error.to_string();
        self.terminal_error = Some(error.clone());
        Self::terminal_connector_error(error)
    }

    fn terminal_connector_error(actual: String) -> ConnectorError {
        ConnectorError::InvalidState {
            expected: "running file discovery task".into(),
            actual,
        }
    }

    #[cfg(test)]
    /// Replaces a fully joined discovery task with a controlled test task.
    ///
    /// # Panics
    ///
    /// Panics if the previous discovery or initial scan task remains installed.
    pub(super) fn install_task_for_test(
        &mut self,
        handle: tokio::task::JoinHandle<Result<(), ConnectorError>>,
    ) {
        assert!(self.handle.is_none());
        assert!(self.initial_scan_handle.is_none());
        self.terminal_error = None;
        self.handle = Some(handle);
    }
}

// ── Local discovery (event + poll modes) ─────────────────────────────

async fn local_discovery_loop(
    config: DiscoveryConfig,
    tx: MAsyncTx<mpsc::Array<DiscoveredFile>>,
    known: Arc<FileInventorySnapshot>,
    initial_scan_tx: std::sync::mpsc::Sender<InitialScanInput>,
    initial_result_rx: tokio::sync::oneshot::Receiver<Result<Vec<(String, u64)>, ConnectorError>>,
) -> Result<(), ConnectorError> {
    use notify::{RecursiveMode, Watcher};

    // Determine the directory to watch (strip glob from path).
    let (watch_dir, glob_from_path) = split_dir_and_glob(&config.path);

    let effective_glob = config.glob_pattern.as_deref().or(glob_from_path.as_deref());

    let glob_matcher = effective_glob
        .and_then(|p| globset::Glob::new(p).ok())
        .map(|g| g.compile_matcher());

    if !Path::new(&watch_dir).is_dir() {
        return Err(ConnectorError::ConfigurationError(format!(
            "file discovery: path '{watch_dir}' is not a directory",
        )));
    }

    let use_poll = should_use_poll_watcher(&watch_dir);
    if use_poll {
        info!("file discovery: using poll watcher for '{watch_dir}' (network filesystem detected)");
    }

    // Channel from notify watcher → our async loop.
    let (notify_tx, notify_rx) = mpsc::bounded_async::<String>(512);

    // Start the appropriate watcher.
    // Both RecommendedWatcher and PollWatcher are Send, but `dyn Watcher` is not.
    // We hold them as concrete types inside an enum to preserve Send.
    enum WatcherHolder {
        Recommended {
            _watcher: notify::RecommendedWatcher,
        },
        Poll {
            _watcher: notify::PollWatcher,
        },
    }

    let _watcher: WatcherHolder = if use_poll {
        let notify_tx_clone: MTx<_> = notify_tx.clone().into_blocking();
        let poll_config = notify::Config::default().with_poll_interval(config.poll_interval);
        let mut watcher = notify::PollWatcher::new(
            move |result: Result<notify::Event, notify::Error>| {
                if let Ok(event) = result {
                    for path in event.paths {
                        if let Some(s) = path.to_str() {
                            let _ = notify_tx_clone.send(s.to_string());
                        }
                    }
                }
            },
            poll_config,
        )
        .map_err(|e| {
            ConnectorError::ConfigurationError(format!("failed to create PollWatcher: {e}"))
        })?;
        watcher
            .watch(Path::new(&watch_dir), RecursiveMode::NonRecursive)
            .map_err(|e| {
                ConnectorError::ConfigurationError(format!("failed to watch directory: {e}"))
            })?;
        WatcherHolder::Poll { _watcher: watcher }
    } else {
        let notify_tx_clone: MTx<_> = notify_tx.clone().into_blocking();
        let mut watcher =
            notify::recommended_watcher(move |result: Result<notify::Event, notify::Error>| {
                if let Ok(event) = result {
                    // Only emit for create/modify/rename-to events.
                    use notify::EventKind;
                    match event.kind {
                        EventKind::Create(_) | EventKind::Modify(_) => {
                            for path in event.paths {
                                if let Some(s) = path.to_str() {
                                    let _ = notify_tx_clone.send(s.to_string());
                                }
                            }
                        }
                        _ => {}
                    }
                }
            })
            .map_err(|e| {
                ConnectorError::ConfigurationError(format!("failed to create watcher: {e}"))
            })?;
        watcher
            .watch(Path::new(&watch_dir), RecursiveMode::NonRecursive)
            .map_err(|e| {
                ConnectorError::ConfigurationError(format!("failed to watch directory: {e}"))
            })?;
        WatcherHolder::Recommended { _watcher: watcher }
    };

    // Stabilisation tracking: path → (size, last_seen_ms).
    let mut pending: std::collections::HashMap<String, (u64, u64)> =
        std::collections::HashMap::new();

    initial_scan_tx
        .send(InitialScanInput {
            watch_dir: watch_dir.clone(),
            glob_matcher: glob_matcher.clone(),
            known: Arc::clone(&known),
        })
        .map_err(|_| ConnectorError::Internal("file discovery initial scan task stopped".into()))?;
    let initial_inventory = initial_result_rx.await.map_err(|_| {
        ConnectorError::Internal("file discovery initial scan result was lost".into())
    })??;
    let observed_at = now_millis();
    for (path, size) in initial_inventory {
        pending.insert(path, (size, observed_at));
    }

    let stabilise_ms = config.stabilisation_delay.as_millis() as u64;

    loop {
        // Drain new events.
        while let Ok(path) = notify_rx.try_recv() {
            stage_candidate(path, glob_matcher.as_ref(), &known, &mut pending);
        }

        // Check stabilised files.
        let now = now_millis();
        let mut ready: Vec<String> = Vec::new();
        for (path, (size, last_seen)) in &pending {
            if now.saturating_sub(*last_seen) >= stabilise_ms {
                // Verify size hasn't changed.
                let current_size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
                if current_size == *size && current_size > 0 {
                    ready.push(path.clone());
                }
            }
        }

        for path in ready {
            if let Some((size, _)) = pending.remove(&path) {
                debug!("file discovery: file ready: {path} ({size} bytes)");
                let _ = tx
                    .send(DiscoveredFile {
                        path,
                        size,
                        modified_ms: now,
                    })
                    .await;
            }
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn scan_initial_inventory(
    watch_dir: &str,
    glob_matcher: Option<&globset::GlobMatcher>,
    known: &FileInventorySnapshot,
) -> Result<Vec<(String, u64)>, ConnectorError> {
    let entries = std::fs::read_dir(watch_dir).map_err(|error| {
        ConnectorError::ReadError(format!(
            "file discovery: failed to scan directory '{watch_dir}': {error}"
        ))
    })?;
    let mut inventory = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            ConnectorError::ReadError(format!(
                "file discovery: failed to read an entry in '{watch_dir}': {error}"
            ))
        })?;
        if let Some(path) = entry.path().to_str() {
            let path = path.to_owned();
            if let Some(size) = candidate_size(&path, glob_matcher, known) {
                inventory.push((path, size));
            }
        }
    }
    Ok(inventory)
}

fn stage_candidate(
    path: String,
    glob_matcher: Option<&globset::GlobMatcher>,
    known: &FileInventorySnapshot,
    pending: &mut std::collections::HashMap<String, (u64, u64)>,
) {
    if let Some(size) = candidate_size(&path, glob_matcher, known) {
        pending.insert(path, (size, now_millis()));
    }
}

fn candidate_size(
    path: &str,
    glob_matcher: Option<&globset::GlobMatcher>,
    known: &FileInventorySnapshot,
) -> Option<u64> {
    if let Some(matcher) = glob_matcher {
        let filename = Path::new(path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if !matcher.is_match(filename) {
            return None;
        }
    }
    if known.contains(path) {
        return None;
    }
    let Ok(metadata) = std::fs::metadata(path) else {
        return None;
    };
    metadata.is_file().then_some(metadata.len())
}

/// Splits a path like `/data/logs/*.csv` into `("/data/logs", Some("*.csv"))`.
fn split_dir_and_glob(path: &str) -> (String, Option<String>) {
    // If path contains glob characters, split at the last directory separator before them.
    if path.contains('*') || path.contains('?') || path.contains('[') {
        if let Some(sep) = path.rfind(['/', '\\']) {
            let dir = &path[..sep];
            let pattern = &path[sep + 1..];
            return (dir.to_string(), Some(pattern.to_string()));
        }
    }
    (path.to_string(), None)
}

/// Determines if a poll watcher should be used (network filesystem detection).
#[allow(clippy::unnecessary_wraps)]
fn should_use_poll_watcher(path: &str) -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::ffi::CString;
        if let Ok(c_path) = CString::new(path) {
            unsafe {
                let mut buf: libc::statfs = std::mem::zeroed();
                if libc::statfs(c_path.as_ptr(), &raw mut buf) == 0 {
                    #[allow(clippy::cast_sign_loss)]
                    let fs_type = buf.f_type as u64;
                    return matches!(
                        fs_type,
                        0x6969          // NFS
                        | 0x5346_544e   // NTFS (FUSE)
                        | 0xFF53_4D42   // CIFS/SMB
                        | 0x0027_e0eb   // ECRYPTFS
                        | 0x6573_5546   // FUSE (general)
                        | 0x6e66_7364 // nfsd
                    );
                }
            }
        }
        false
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        false
    }
}

#[allow(clippy::cast_possible_truncation)]
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests;

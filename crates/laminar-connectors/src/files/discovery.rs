//! File discovery engine (Ring 2).
//!
//! Watches a directory for new files and emits [`DiscoveredFile`] events
//! to the source connector via an `mpsc` channel. Supports three modes:
//!
//! - **Event mode**: `notify::recommended_watcher()` for local filesystems
//! - **Poll mode**: `notify::PollWatcher` for NFS/CIFS/FUSE mounts
//! - **Cloud poll mode**: `object_store::list()` for cloud storage paths

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crossfire::{mpsc, AsyncRx, MAsyncTx, MTx};
use tracing::{debug, error, info, warn};

use super::manifest::FileIngestionManifest;
use crate::error::ConnectorError;

/// A file discovered by the discovery engine.
#[derive(Debug, Clone)]
pub struct DiscoveredFile {
    /// Full path or URL to the file.
    pub path: String,
    /// File size in bytes.
    pub size: u64,
    /// Last modification time (millis since epoch).
    pub modified_ms: u64,
}

/// Configuration for the discovery engine.
#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    /// Base path or cloud URL to watch.
    pub path: String,
    /// Polling interval for poll/cloud modes.
    pub poll_interval: Duration,
    /// Stabilisation delay after last modify event (event mode).
    pub stabilisation_delay: Duration,
    /// Glob pattern for filtering file names.
    pub glob_pattern: Option<String>,
}

/// Handle to a running discovery engine. Dropping this stops the engine.
pub struct FileDiscoveryEngine {
    /// Channel receiver for discovered files.
    rx: AsyncRx<mpsc::Array<DiscoveredFile>>,
    /// Abort handle for the background task.
    _abort: tokio::task::JoinHandle<()>,
}

impl FileDiscoveryEngine {
    /// Starts a discovery engine for the given config.
    ///
    /// Files already present in `known_files` are skipped.
    pub fn start(config: DiscoveryConfig, known_files: Arc<FileIngestionManifest>) -> Self {
        let (tx, rx) = mpsc::bounded_async::<DiscoveredFile>(256);

        let abort = if is_cloud_path(&config.path) {
            tokio::spawn(cloud_poll_loop(config, tx, known_files))
        } else {
            tokio::spawn(async move {
                if let Err(e) = local_discovery_loop(config, tx, known_files).await {
                    error!(error = %e, "file discovery loop failed");
                }
            })
        };

        Self { rx, _abort: abort }
    }

    /// Drains available discovered files (non-blocking).
    ///
    /// Returns up to `max` files.
    pub fn drain(&mut self, max: usize) -> Vec<DiscoveredFile> {
        let mut files = Vec::with_capacity(max.min(64));
        for _ in 0..max {
            match self.rx.try_recv() {
                Ok(file) => files.push(file),
                Err(_) => break,
            }
        }
        files
    }
}

/// Returns `true` if the path is a cloud storage URL.
fn is_cloud_path(path: &str) -> bool {
    path.starts_with("s3://")
        || path.starts_with("gs://")
        || path.starts_with("az://")
        || path.starts_with("abfs://")
        || path.starts_with("abfss://")
}

// ── Cloud poll mode ──────────────────────────────────────────────────

async fn cloud_poll_loop(
    config: DiscoveryConfig,
    tx: MAsyncTx<mpsc::Array<DiscoveredFile>>,
    known: Arc<FileIngestionManifest>,
) {
    let (store, prefix) = match build_cloud_store(&config.path) {
        Ok(v) => v,
        Err(e) => {
            warn!(
                "file discovery: cannot create object store for '{}': {e}",
                config.path
            );
            return;
        }
    };

    let glob = config
        .glob_pattern
        .as_deref()
        .and_then(|p| globset::Glob::new(p).ok())
        .map(|g| g.compile_matcher());

    let mut prev_sizes: std::collections::HashMap<String, u64> = std::collections::HashMap::new();

    loop {
        match list_cloud_files(&store, &prefix).await {
            Ok(entries) => {
                for (path, size) in entries {
                    // Glob filter on filename.
                    if let Some(ref matcher) = glob {
                        let filename = path.rsplit('/').next().unwrap_or(&path);
                        if !matcher.is_match(filename) {
                            continue;
                        }
                    }

                    // Dedup against known files.
                    if known.contains(&path) {
                        continue;
                    }

                    // Size-stable check: need same size across two consecutive polls.
                    match prev_sizes.get(&path) {
                        Some(&prev) if prev == size => {
                            // Stable — emit.
                            let _ = tx
                                .send(DiscoveredFile {
                                    path: path.clone(),
                                    size,
                                    modified_ms: now_millis(),
                                })
                                .await;
                            prev_sizes.remove(&path);
                        }
                        _ => {
                            // First seen or size changed — record and wait.
                            prev_sizes.insert(path, size);
                        }
                    }
                }
            }
            Err(e) => {
                warn!("file discovery: cloud list error: {e}");
            }
        }
        tokio::time::sleep(config.poll_interval).await;
    }
}

async fn list_cloud_files(
    store: &Arc<dyn object_store::ObjectStore>,
    prefix: &object_store::path::Path,
) -> Result<Vec<(String, u64)>, object_store::Error> {
    let mut entries = Vec::new();
    let mut stream = store.list(Some(prefix));
    use tokio_stream::StreamExt;
    while let Some(result) = stream.next().await {
        let meta = result?;
        entries.push((meta.location.to_string(), meta.size));
    }
    Ok(entries)
}

fn build_cloud_store(
    url: &str,
) -> Result<
    (Arc<dyn object_store::ObjectStore>, object_store::path::Path),
    Box<dyn std::error::Error + Send + Sync>,
> {
    // For cloud paths, we need a proper object store. Use the InMemory store
    // as a placeholder — production wiring uses laminar-storage's factory.
    // Extract prefix from URL for listing.
    let scheme_end = url.find("://").map(|i| i + 3).unwrap_or(0);
    let rest = &url[scheme_end..];
    // Split into bucket and prefix at first '/'.
    let prefix = if let Some(slash) = rest.find('/') {
        &rest[slash + 1..]
    } else {
        ""
    };
    let path = object_store::path::Path::from(prefix);
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    warn!(
        "file discovery: using InMemory object store for '{url}' — \
           configure a real store via laminar-storage for production"
    );
    Ok((store, path))
}

// ── Local discovery (event + poll modes) ─────────────────────────────

async fn local_discovery_loop(
    config: DiscoveryConfig,
    tx: MAsyncTx<mpsc::Array<DiscoveredFile>>,
    known: Arc<FileIngestionManifest>,
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
    #[allow(dead_code)] // Fields held for drop behavior (stops the watcher).
    enum WatcherHolder {
        Recommended(notify::RecommendedWatcher),
        Poll(notify::PollWatcher),
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
        WatcherHolder::Poll(watcher)
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
        WatcherHolder::Recommended(watcher)
    };

    // Also do an initial directory scan for files that already exist.
    if let Ok(entries) = std::fs::read_dir(&watch_dir) {
        for entry in entries.flatten() {
            if let Some(s) = entry.path().to_str() {
                let _ = notify_tx.send(s.to_string()).await;
            }
        }
    }

    // Stabilisation tracking: path → (size, last_seen_ms).
    let mut pending: std::collections::HashMap<String, (u64, u64)> =
        std::collections::HashMap::new();

    let stabilise_ms = config.stabilisation_delay.as_millis() as u64;

    loop {
        // Drain new events.
        while let Ok(path) = notify_rx.try_recv() {
            // Apply glob filter.
            if let Some(ref matcher) = glob_matcher {
                let filename = Path::new(&path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("");
                if !matcher.is_match(filename) {
                    continue;
                }
            }

            // Skip directories.
            if Path::new(&path).is_dir() {
                continue;
            }

            // Skip known files.
            if known.contains(&path) {
                continue;
            }

            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            pending.insert(path, (size, now_millis()));
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
mod tests {
    use super::*;

    #[test]
    fn test_split_dir_and_glob() {
        let (dir, glob) = split_dir_and_glob("/data/logs/*.csv");
        assert_eq!(dir, "/data/logs");
        assert_eq!(glob.as_deref(), Some("*.csv"));

        let (dir, glob) = split_dir_and_glob("/data/logs");
        assert_eq!(dir, "/data/logs");
        assert!(glob.is_none());

        let (dir, glob) = split_dir_and_glob("/data/logs/events_*.json");
        assert_eq!(dir, "/data/logs");
        assert_eq!(glob.as_deref(), Some("events_*.json"));
    }

    #[test]
    fn test_is_cloud_path() {
        assert!(is_cloud_path("s3://bucket/prefix"));
        assert!(is_cloud_path("gs://bucket/prefix"));
        assert!(is_cloud_path("az://container/path"));
        assert!(is_cloud_path("abfs://container@account/path"));
        assert!(!is_cloud_path("/local/path"));
        assert!(!is_cloud_path("C:\\Users\\data"));
    }

    #[test]
    fn test_should_use_poll_on_local() {
        // On non-Linux or local FS, should return false.
        assert!(!should_use_poll_watcher("/tmp"));
    }

    #[tokio::test]
    async fn test_drain_empty() {
        let config = DiscoveryConfig {
            path: "/nonexistent_test_dir_12345".into(),
            poll_interval: Duration::from_secs(60),
            stabilisation_delay: Duration::from_secs(1),
            glob_pattern: None,
        };
        let known = Arc::new(FileIngestionManifest::new());
        let mut engine = FileDiscoveryEngine::start(config, known);
        // Give the background task a moment to start (and fail on the bad path).
        tokio::time::sleep(Duration::from_millis(50)).await;
        let files = engine.drain(10);
        assert!(files.is_empty());
    }
}

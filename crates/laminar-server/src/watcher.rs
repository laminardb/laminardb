//! File system watcher for automatic config hot-reload.
//!
//! Watches the config file's parent directory using the `notify` crate and
//! triggers a reload when the target file is modified. Handles atomic editor
//! saves (write-temp + rename) by watching the directory rather than the file.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

fn file_content_hash(path: &std::path::Path) -> Option<u64> {
    use std::hash::{Hash, Hasher};
    let bytes = std::fs::read(path).ok()?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    bytes.hash(&mut hasher);
    Some(hasher.finish())
}

use crate::config;
use crate::http::AppState;
use crate::reload;

/// Watch the config file and trigger reload on changes.
///
/// Runs forever until the task is aborted. Errors are logged but never
/// cause the watcher to exit (resilience against transient failures).
pub async fn watch_config(config_path: PathBuf, state: Arc<AppState>, debounce: Duration) {
    let (tx, mut rx) = mpsc::channel::<()>(16);

    // Canonicalize the config path for reliable comparison
    let canonical = match config_path.canonicalize() {
        Ok(p) => p,
        Err(e) => {
            warn!(
                "Could not canonicalize config path '{}': {e} — watcher disabled",
                config_path.display()
            );
            return;
        }
    };

    // Watch the parent directory (handles atomic saves: write-tmp + rename)
    let watch_dir = match canonical.parent() {
        Some(p) => p.to_path_buf(),
        None => {
            warn!("Config file has no parent directory — watcher disabled");
            return;
        }
    };

    let target = canonical.clone();
    let mut watcher: RecommendedWatcher =
        match notify::recommended_watcher(move |result: Result<Event, notify::Error>| {
            match result {
                Ok(event) => {
                    let dominated = event.paths.iter().any(|p| {
                        // Compare canonical paths to handle symlinks/relative paths
                        p.canonicalize().ok().as_ref() == Some(&target)
                    });
                    if dominated {
                        let _ = tx.blocking_send(());
                    }
                }
                Err(e) => {
                    warn!("File watcher error: {e}");
                }
            }
        }) {
            Ok(w) => w,
            Err(e) => {
                error!("Failed to create file watcher: {e} — hot reload disabled");
                return;
            }
        };

    if let Err(e) = watcher.watch(&watch_dir, RecursiveMode::NonRecursive) {
        error!(
            "Failed to watch directory '{}': {e} — hot reload disabled",
            watch_dir.display()
        );
        return;
    }

    info!("Watching config file '{}' for changes", canonical.display());

    // Track content hash to skip spurious inotify events (Docker overlay mounts)
    let mut last_hash = file_content_hash(&canonical);

    // Keep the watcher alive and process debounced events
    loop {
        // Wait for first notification
        if rx.recv().await.is_none() {
            debug!("Watcher channel closed, exiting");
            return;
        }

        // Debounce: sleep then drain any queued notifications
        tokio::time::sleep(debounce).await;
        while rx.try_recv().is_ok() {}

        let current_hash = file_content_hash(&canonical);
        if current_hash == last_hash {
            debug!("File event but content unchanged, skipping");
            continue;
        }

        info!("Config file change detected, reloading...");

        last_hash = current_hash;

        // Load new config
        let new_config = match config::load_config(&canonical) {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to load config on file change: {e}");
                continue;
            }
        };

        // Acquire reload guard
        let _guard = match state.reload_guard.try_acquire() {
            Some(g) => g,
            None => {
                debug!("Another reload in progress, skipping file-triggered reload");
                continue;
            }
        };

        // Diff against current config
        let current = state.current_config.read().await;
        let diff = reload::diff_configs(&current, &new_config);
        drop(current);

        if diff.is_empty() {
            for w in &diff.warnings {
                warn!("Config reload warning: {w}");
            }
            if diff.warnings.is_empty() {
                debug!("No reloadable changes detected");
            }
            continue;
        }

        // Apply the diff
        let result = reload::apply_reload(&state.db, &diff).await;

        // Update metrics
        state.reload_total.fetch_add(1, Ordering::Relaxed);
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let now = chrono::Utc::now().timestamp() as u64;
        state.reload_last_ts.store(now, Ordering::Relaxed);

        if result.success {
            let mut current = state.current_config.write().await;
            *current = new_config;
            info!(
                "File-triggered reload complete: {} ops applied",
                result.applied.len()
            );
        } else {
            warn!(
                "File-triggered reload partial failure: {} applied, {} failed",
                result.applied.len(),
                result.failed.len()
            );
        }

        for w in &result.warnings {
            warn!("Reload warning: {w}");
        }
    }
}

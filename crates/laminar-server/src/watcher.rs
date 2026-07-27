//! File system watcher for automatic config hot-reload.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crossfire::{mpsc, MTx};
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
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

/// Process one observed content change. Keeping the publication boundary here makes the file
/// watcher path directly testable without depending on platform-specific notification timing.
async fn reload_changed_config(
    state: &Arc<AppState>,
    canonical: &Path,
) -> Option<reload::ReloadResult> {
    let new_config = match config::load_config(canonical) {
        Ok(config) => config,
        Err(error) => {
            warn!("Failed to load config on file change: {error}");
            return None;
        }
    };

    let _guard = match state.reload_guard.try_acquire() {
        Some(guard) => guard,
        None => {
            debug!("Another reload in progress, skipping file-triggered reload");
            return None;
        }
    };

    let diff = {
        let current = state.current_config.read();
        reload::diff_configs(&current, &new_config)
    };

    if diff.is_empty() {
        for warning in &diff.warnings {
            warn!("Config reload warning: {warning}");
        }
        if diff.warnings.is_empty() {
            debug!("No reloadable changes detected");
        }
        return None;
    }

    let result = reload::apply_reload(&state.db, &diff).await;
    state.server_metrics.reload_total.inc();

    if result.success {
        let mut current = state.current_config.write();
        reload::commit_reloadable_config(&mut current, new_config);
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

    for warning in &result.warnings {
        warn!("Reload warning: {warning}");
    }
    Some(result)
}

/// Watch the config file and trigger reload on changes. Runs until aborted.
pub async fn watch_config(config_path: PathBuf, state: Arc<AppState>, debounce: Duration) {
    let (tx, rx) = mpsc::bounded_async::<()>(16);
    let blocking_tx: MTx<_> = tx.clone().into_blocking();

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
                        let _ = blocking_tx.send(());
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
        if rx.recv().await.is_err() {
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

        reload_changed_config(&state, &canonical).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[cfg(feature = "cluster")]
    use crate::http::DiagnosticReadGate;
    use crate::http::{ws_connection_slots, HttpAuthPolicy, ServingGate};
    use crate::reload::ReloadGuard;

    fn test_state(path: PathBuf, config: config::ServerConfig) -> Arc<AppState> {
        let registry = Arc::new(crate::metrics::build_registry([
            ("instance".into(), "watcher-test".into()),
            ("pipeline".into(), "watcher-test".into()),
        ]));
        let db = laminar_db::LaminarDB::open().unwrap();
        db.set_engine_metrics(Arc::new(laminar_db::EngineMetrics::new(&registry)));
        let server_metrics = crate::metrics::ServerMetrics::new(&registry);
        let auth_policy = HttpAuthPolicy::from_server(&config.server);
        let serving_gate = Arc::new(ServingGate::starting());
        assert!(serving_gate.open());
        Arc::new(AppState {
            db,
            config_path: path,
            current_config: parking_lot::RwLock::new(config),
            reload_guard: ReloadGuard::new(),
            registry,
            server_metrics,
            auth_policy,
            #[cfg(feature = "cluster")]
            diagnostic_reads: DiagnosticReadGate::new(),
            ws_slots: ws_connection_slots(),
            serving_gate,
            #[cfg(feature = "cluster")]
            cluster: None,
        })
    }

    fn original_config() -> config::ServerConfig {
        let mut config: config::ServerConfig = toml::from_str("[server]\n").unwrap();
        config.server.console_token = Some(config::Secret::new("original-console-token"));
        config
    }

    #[tokio::test]
    async fn watcher_retains_pure_restart_only_configuration() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            file,
            "[server]\nbind = \"127.0.0.1:9494\"\nconsole_token = \"replacement-console-token\""
        )
        .unwrap();
        let original = original_config();
        let state = test_state(file.path().to_path_buf(), original.clone());

        let result = reload_changed_config(&state, file.path()).await;

        assert!(result.is_none());
        assert_eq!(*state.current_config.read(), original);
    }

    #[tokio::test]
    async fn watcher_commits_live_sections_but_retains_mixed_restart_only_changes() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            file,
            "[server]\nbind = \"127.0.0.1:9595\"\nconsole_token = \"replacement-console-token\""
        )
        .unwrap();
        let mut original = original_config();
        original.sources.push(config::SourceConfig {
            name: "removed_source".to_string(),
            connector: "kafka".to_string(),
            format: "json".to_string(),
            properties: toml::Table::new(),
            schema: vec![],
            watermark: None,
        });
        let original_server = original.server.clone();
        let state = test_state(file.path().to_path_buf(), original);

        let result = reload_changed_config(&state, file.path())
            .await
            .expect("a live removal must be attempted");

        assert!(result.success, "failures: {:?}", result.failed);
        let current = state.current_config.read();
        assert!(current.sources.is_empty());
        assert_eq!(current.server, original_server);
    }

    #[tokio::test]
    async fn watcher_failure_commits_neither_live_nor_restart_only_configuration() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            file,
            "[server]\nbind = \"127.0.0.1:9696\"\nconsole_token = \"replacement-console-token\"\n\n[[pipeline]]\nname = \"bad_reload\"\nsql = \"NOT VALID SQL AT ALL\""
        )
        .unwrap();
        let original = original_config();
        let state = test_state(file.path().to_path_buf(), original.clone());

        let result = reload_changed_config(&state, file.path())
            .await
            .expect("invalid DDL must be attempted");

        assert!(!result.success);
        assert_eq!(*state.current_config.read(), original);
    }

    #[tokio::test]
    async fn watcher_parse_error_uses_the_redacted_config_error() {
        const SENTINEL: &str = "LDB_WATCHER_SECRET_SENTINEL_226f5367";
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            file,
            "[server]\nconsole_token = ${{LDB_WATCHER_REDACTION_TOKEN:-{SENTINEL}}}"
        )
        .unwrap();
        let original = original_config();
        let state = test_state(file.path().to_path_buf(), original.clone());
        let error = config::load_config(file.path()).unwrap_err();
        assert!(!error.to_string().contains(SENTINEL));
        assert!(!format!("{error:?}").contains(SENTINEL));

        assert!(reload_changed_config(&state, file.path()).await.is_none());
        assert_eq!(*state.current_config.read(), original);
    }
}

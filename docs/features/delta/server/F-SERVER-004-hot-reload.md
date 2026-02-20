# F-SERVER-004: Hot Reload (Config Diff, SIGHUP)

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SERVER-004 |
| **Status** | Draft |
| **Priority** | P1 |
| **Phase** | 6c |
| **Effort** | L (5-8 days) |
| **Dependencies** | F-SERVER-002 (Engine Construction), F-SERVER-003 (HTTP API) |
| **Blocks** | None |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-server` |
| **Module** | `crates/laminar-server/src/reload.rs` |

## Summary

Hot reload enables operators to modify `laminardb.toml` and apply changes to a running server without full restart. Changes are applied by sending `POST /api/v1/reload` or delivering a `SIGHUP` signal (Unix) / named pipe message (Windows). The server diffs the current running configuration against the newly parsed file, categorizing each component as added, removed, modified, or unchanged. New pipelines are compiled and started. Removed pipelines are drained and stopped. Modified pipelines are restarted from checkpoint. Unchanged pipelines continue uninterrupted. The reload is atomic: if any new/modified component fails validation, the entire reload is aborted and the running configuration remains unchanged.

## Goals

- Diff old config against new config to produce `ConfigDiff` (added/removed/modified/unchanged)
- Atomic reload: all-or-nothing on validation failure (no partial state)
- Pipeline drain procedure: stop accepting events -> flush in-flight -> checkpoint -> stop
- Support SIGHUP signal handler on Unix for reload triggering
- Support named pipe or HTTP-only reload on Windows
- Preserve exactly-once semantics during pipeline restart (resume from checkpoint)
- Report reload results (what changed) via HTTP response and structured logging

## Non-Goals

- Schema evolution of running sources (requires source connector restart)
- Live migration of state between different state backends
- Cross-node coordinated reload in delta mode (each node reloads independently)
- Rollback to previous config version (manual re-edit and re-reload)
- Config file watching (inotify/fswatch) -- only explicit trigger via signal or HTTP

## Technical Design

### Architecture

The reload subsystem sits between the HTTP API and the engine builder. It re-uses the same config parsing (F-SERVER-001) and engine construction (F-SERVER-002) logic but applies changes incrementally rather than from scratch.

```
Trigger (SIGHUP or POST /reload)
    |
    v
+-------------------+     +-------------------+
| Re-parse TOML     | --> | Diff old vs new   |
| (F-SERVER-001)    |     | config             |
+-------------------+     +--------+----------+
                                   |
                          +--------v----------+
                          | Validate new/     |
                          | modified items    |
                          | (compile SQL,     |
                          |  create connectors)|
                          +--------+----------+
                                   |
                       +-----------v-----------+
                       | Apply changes:         |
                       | - Start added          |
                       | - Drain + stop removed |
                       | - Restart modified     |
                       | - Preserve unchanged   |
                       +------------------------+
```

### API/Interface

```rust
use crate::config::{ServerConfig, SourceConfig, LookupConfig, PipelineConfig, SinkConfig};
use std::collections::HashMap;

/// Result of a hot reload operation.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ReloadResult {
    /// Pipeline names that were added (new in config).
    pub added: Vec<String>,
    /// Pipeline names that were removed (absent from new config).
    pub removed: Vec<String>,
    /// Pipeline names that were modified (SQL or properties changed).
    pub modified: Vec<String>,
    /// Pipeline names that were unchanged.
    pub unchanged: Vec<String>,
    /// Sources that were added.
    pub sources_added: Vec<String>,
    /// Sources that were removed.
    pub sources_removed: Vec<String>,
    /// Lookups that were added.
    pub lookups_added: Vec<String>,
    /// Lookups that were removed.
    pub lookups_removed: Vec<String>,
}

/// Compute the difference between two configurations.
///
/// Compares all sections: sources, lookups, pipelines, sinks.
/// Items are matched by name. A modified item is one where the
/// name matches but any property differs.
pub fn diff_configs(old: &ServerConfig, new: &ServerConfig) -> ConfigDiff {
    ConfigDiff {
        sources: diff_named_items(&old.sources, &new.sources, |s| &s.name),
        lookups: diff_named_items(&old.lookups, &new.lookups, |l| &l.name),
        pipelines: diff_named_items(&old.pipelines, &new.pipelines, |p| &p.name),
        sinks: diff_named_items(&old.sinks, &new.sinks, |s| &s.name),
        server_changed: old.server != new.server,
        state_changed: old.state != new.state,
        checkpoint_changed: old.checkpoint != new.checkpoint,
    }
}

/// Execute a hot reload operation.
///
/// This is the main entry point called by the HTTP handler and
/// the SIGHUP handler.
///
/// # Atomicity
///
/// The reload is all-or-nothing. Steps:
/// 1. Parse new config (fail -> abort, no changes)
/// 2. Compute diff
/// 3. Validate all new/modified items (compile SQL, create connectors)
///    (fail -> abort, no changes)
/// 4. Apply changes (drain removed, start added, restart modified)
///
/// If step 3 fails, nothing from step 4 is executed.
pub async fn execute_reload(
    engine: &LaminarEngine,
    config_path: &std::path::Path,
    current_config: &ServerConfig,
) -> Result<(ReloadResult, ServerConfig), ReloadError> {
    // Step 1: Parse new config
    let new_config = crate::config::load_config(config_path)
        .map_err(|e| ReloadError::ConfigParse(e.to_string()))?;

    // Step 2: Compute diff
    let diff = diff_configs(current_config, &new_config);

    tracing::info!(
        added_pipelines = diff.pipelines.added.len(),
        removed_pipelines = diff.pipelines.removed.len(),
        modified_pipelines = diff.pipelines.modified.len(),
        unchanged_pipelines = diff.pipelines.unchanged.len(),
        "config diff computed"
    );

    // Step 3: Validate all new/modified items (dry-run)
    validate_reload(&diff, &new_config, engine).await?;

    // Step 4: Apply changes (point of no return)
    let result = apply_reload(&diff, &new_config, engine).await?;

    Ok((result, new_config))
}

/// Validate that all new/modified components can be constructed.
///
/// This is a dry-run that creates connectors and compiles SQL
/// without actually starting anything. If any validation fails,
/// the entire reload is aborted.
async fn validate_reload(
    diff: &ConfigDiff,
    new_config: &ServerConfig,
    engine: &LaminarEngine,
) -> Result<(), ReloadError> {
    let mut errors = Vec::new();

    // Validate new/modified sources
    for name in diff.sources.added.iter().chain(diff.sources.modified.iter()) {
        let source = new_config.sources.iter().find(|s| &s.name == name).unwrap();
        if let Err(e) = engine.validate_source(source).await {
            errors.push(format!("source '{}': {}", name, e));
        }
    }

    // Validate new/modified pipelines (compile SQL)
    for name in diff.pipelines.added.iter().chain(diff.pipelines.modified.iter()) {
        let pipeline = new_config.pipelines.iter().find(|p| &p.name == name).unwrap();
        if let Err(e) = engine.validate_pipeline_sql(&pipeline.sql).await {
            errors.push(format!("pipeline '{}': {}", name, e));
        }
    }

    // Validate new/modified sinks
    for name in diff.sinks.added.iter().chain(diff.sinks.modified.iter()) {
        let sink = new_config.sinks.iter().find(|s| &s.name == name).unwrap();
        if let Err(e) = engine.validate_sink(sink).await {
            errors.push(format!("sink '{}': {}", name, e));
        }
    }

    if !errors.is_empty() {
        return Err(ReloadError::ValidationFailed { errors });
    }

    Ok(())
}

/// Apply the diff to the running engine.
///
/// Order of operations:
/// 1. Drain and stop removed pipelines
/// 2. Start added sources
/// 3. Compile and start added pipelines
/// 4. Attach added sinks
/// 5. Restart modified pipelines (drain -> checkpoint -> recompile -> start)
async fn apply_reload(
    diff: &ConfigDiff,
    new_config: &ServerConfig,
    engine: &LaminarEngine,
) -> Result<ReloadResult, ReloadError> {
    // 1. Remove: Drain and stop removed pipelines
    for name in &diff.pipelines.removed {
        tracing::info!(pipeline = %name, "draining removed pipeline");
        drain_pipeline(engine, name).await?;
        engine.remove_pipeline(name).await
            .map_err(|e| ReloadError::ApplyFailed(e.to_string()))?;
    }

    // Remove sources that are no longer referenced
    for name in &diff.sources.removed {
        tracing::info!(source = %name, "removing source");
        engine.remove_source(name).await
            .map_err(|e| ReloadError::ApplyFailed(e.to_string()))?;
    }

    // Remove lookups that are no longer referenced
    for name in &diff.lookups.removed {
        tracing::info!(lookup = %name, "removing lookup");
        engine.remove_lookup(name).await
            .map_err(|e| ReloadError::ApplyFailed(e.to_string()))?;
    }

    // 2. Add new sources
    for name in &diff.sources.added {
        let source = new_config.sources.iter().find(|s| &s.name == name).unwrap();
        tracing::info!(source = %name, "adding new source");
        engine.add_source(source).await
            .map_err(|e| ReloadError::ApplyFailed(e.to_string()))?;
    }

    // 3. Add new lookups
    for name in &diff.lookups.added {
        let lookup = new_config.lookups.iter().find(|l| &l.name == name).unwrap();
        tracing::info!(lookup = %name, "adding new lookup");
        engine.add_lookup(lookup).await
            .map_err(|e| ReloadError::ApplyFailed(e.to_string()))?;
    }

    // 4. Add new pipelines
    for name in &diff.pipelines.added {
        let pipeline = new_config.pipelines.iter().find(|p| &p.name == name).unwrap();
        tracing::info!(pipeline = %name, "adding new pipeline");
        engine.add_pipeline(pipeline).await
            .map_err(|e| ReloadError::ApplyFailed(e.to_string()))?;
    }

    // 5. Add new sinks
    for name in &diff.sinks.added {
        let sink = new_config.sinks.iter().find(|s| &s.name == name).unwrap();
        tracing::info!(sink = %name, "adding new sink");
        engine.add_sink(sink).await
            .map_err(|e| ReloadError::ApplyFailed(e.to_string()))?;
    }

    // 6. Restart modified pipelines
    for name in &diff.pipelines.modified {
        let pipeline = new_config.pipelines.iter().find(|p| &p.name == name).unwrap();
        tracing::info!(pipeline = %name, "restarting modified pipeline");
        restart_modified_pipeline(engine, pipeline).await?;
    }

    Ok(ReloadResult {
        added: diff.pipelines.added.clone(),
        removed: diff.pipelines.removed.clone(),
        modified: diff.pipelines.modified.clone(),
        unchanged: diff.pipelines.unchanged.clone(),
        sources_added: diff.sources.added.clone(),
        sources_removed: diff.sources.removed.clone(),
        lookups_added: diff.lookups.added.clone(),
        lookups_removed: diff.lookups.removed.clone(),
    })
}
```

### Data Structures

```rust
/// Diff between two configurations.
///
/// Each component type has its own `ItemDiff` tracking what was
/// added, removed, modified, or left unchanged.
#[derive(Debug, Clone)]
pub struct ConfigDiff {
    pub sources: ItemDiff,
    pub lookups: ItemDiff,
    pub pipelines: ItemDiff,
    pub sinks: ItemDiff,
    pub server_changed: bool,
    pub state_changed: bool,
    pub checkpoint_changed: bool,
}

/// Diff for a single component type (sources, pipelines, etc.).
#[derive(Debug, Clone, Default)]
pub struct ItemDiff {
    /// Names of items that exist only in the new config.
    pub added: Vec<String>,
    /// Names of items that exist only in the old config.
    pub removed: Vec<String>,
    /// Names of items that exist in both but have changed.
    pub modified: Vec<String>,
    /// Names of items that are identical in both configs.
    pub unchanged: Vec<String>,
}

/// Generic diff function for named config items.
///
/// Compares items by name. Items present in new but not old are "added".
/// Items present in old but not new are "removed". Items present in both
/// are "modified" if they differ or "unchanged" if identical.
fn diff_named_items<T: PartialEq>(
    old: &[T],
    new: &[T],
    get_name: impl Fn(&T) -> &String,
) -> ItemDiff {
    let old_map: HashMap<&String, &T> = old.iter().map(|i| (get_name(i), i)).collect();
    let new_map: HashMap<&String, &T> = new.iter().map(|i| (get_name(i), i)).collect();

    let mut diff = ItemDiff::default();

    // Check for added and modified
    for (name, new_item) in &new_map {
        match old_map.get(name) {
            None => diff.added.push((*name).clone()),
            Some(old_item) => {
                if old_item != new_item {
                    diff.modified.push((*name).clone());
                } else {
                    diff.unchanged.push((*name).clone());
                }
            }
        }
    }

    // Check for removed
    for name in old_map.keys() {
        if !new_map.contains_key(name) {
            diff.removed.push((*name).clone());
        }
    }

    diff
}

/// Errors during hot reload.
#[derive(Debug, thiserror::Error)]
pub enum ReloadError {
    /// Config file could not be parsed.
    #[error("config parse error: {0}")]
    ConfigParse(String),

    /// One or more new/modified components failed validation.
    /// The entire reload is aborted; no changes were applied.
    #[error("reload validation failed (no changes applied):\n{}", errors.join("\n  - "))]
    ValidationFailed { errors: Vec<String> },

    /// Error during the apply phase (after validation passed).
    #[error("reload apply failed: {0}")]
    ApplyFailed(String),

    /// Pipeline drain timed out.
    #[error("pipeline drain timed out for '{pipeline}' after {timeout_secs}s")]
    DrainTimeout {
        pipeline: String,
        timeout_secs: u64,
    },
}
```

### Algorithm/Flow

#### Pipeline Drain Procedure

```rust
/// Drain a pipeline: stop accepting new events, flush in-flight,
/// checkpoint state, then stop.
///
/// Drain timeout is configurable (default 30s). If the pipeline
/// does not drain within the timeout, it is forcefully stopped.
async fn drain_pipeline(
    engine: &LaminarEngine,
    pipeline_name: &str,
) -> Result<(), ReloadError> {
    let timeout = std::time::Duration::from_secs(30);

    // 1. Signal source connectors to stop producing for this pipeline
    engine.pause_pipeline(pipeline_name).await
        .map_err(|e| ReloadError::ApplyFailed(e.to_string()))?;

    // 2. Wait for in-flight events to flush through operators
    let drained = tokio::time::timeout(
        timeout,
        engine.wait_for_drain(pipeline_name),
    ).await;

    match drained {
        Ok(Ok(())) => {
            tracing::info!(pipeline = %pipeline_name, "pipeline drained");
        }
        Ok(Err(e)) => {
            tracing::warn!(
                pipeline = %pipeline_name,
                error = %e,
                "pipeline drain error, forcing stop"
            );
        }
        Err(_) => {
            tracing::warn!(
                pipeline = %pipeline_name,
                timeout_secs = timeout.as_secs(),
                "pipeline drain timed out, forcing stop"
            );
        }
    }

    // 3. Trigger a checkpoint to persist current state
    engine.checkpoint_pipeline(pipeline_name).await
        .map_err(|e| ReloadError::ApplyFailed(e.to_string()))?;

    // 4. Stop the pipeline
    engine.stop_pipeline(pipeline_name).await
        .map_err(|e| ReloadError::ApplyFailed(e.to_string()))?;

    Ok(())
}

/// Restart a modified pipeline from its last checkpoint.
///
/// 1. Drain the old pipeline instance
/// 2. Recompile the SQL with the new definition
/// 3. Start the new pipeline from the last checkpoint
async fn restart_modified_pipeline(
    engine: &LaminarEngine,
    pipeline: &PipelineConfig,
) -> Result<(), ReloadError> {
    // Drain old instance
    drain_pipeline(engine, &pipeline.name).await?;

    // Remove old instance
    engine.remove_pipeline(&pipeline.name).await
        .map_err(|e| ReloadError::ApplyFailed(e.to_string()))?;

    // Add new instance (will restore from checkpoint automatically)
    engine.add_pipeline(pipeline).await
        .map_err(|e| ReloadError::ApplyFailed(e.to_string()))?;

    tracing::info!(pipeline = %pipeline.name, "pipeline restarted from checkpoint");
    Ok(())
}
```

#### SIGHUP Signal Handler (Unix)

```rust
/// Install the SIGHUP signal handler for hot reload.
///
/// On Unix, SIGHUP triggers a config reload. On Windows, this
/// function is a no-op; use the HTTP endpoint instead.
#[cfg(unix)]
pub fn install_sighup_handler(
    engine: Arc<LaminarEngine>,
    config_path: std::path::PathBuf,
    current_config: Arc<tokio::sync::RwLock<ServerConfig>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut signal = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::hangup(),
        ).expect("failed to install SIGHUP handler");

        loop {
            signal.recv().await;
            tracing::info!("received SIGHUP, initiating config reload");

            let current = current_config.read().await.clone();
            match execute_reload(&engine, &config_path, &current).await {
                Ok((result, new_config)) => {
                    *current_config.write().await = new_config;
                    tracing::info!(
                        added = result.added.len(),
                        removed = result.removed.len(),
                        modified = result.modified.len(),
                        "SIGHUP reload complete"
                    );
                }
                Err(e) => {
                    tracing::error!(error = %e, "SIGHUP reload failed");
                }
            }
        }
    })
}

/// On Windows, SIGHUP is not available. Log a message explaining
/// the HTTP endpoint should be used instead.
#[cfg(windows)]
pub fn install_sighup_handler(
    _engine: Arc<LaminarEngine>,
    _config_path: std::path::PathBuf,
    _current_config: Arc<tokio::sync::RwLock<ServerConfig>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async {
        tracing::info!(
            "SIGHUP not available on Windows; use POST /api/v1/reload for config reload"
        );
    })
}
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `ReloadError::ConfigParse` | New TOML file is malformed or has missing env vars | No changes applied; fix TOML and retry |
| `ReloadError::ValidationFailed` | New/modified SQL is invalid or connector cannot be created | No changes applied; fix config and retry |
| `ReloadError::ApplyFailed` | Error during drain, stop, or start of a pipeline | Partial changes may be applied; check pipeline status |
| `ReloadError::DrainTimeout` | Pipeline did not drain within timeout | Pipeline forcefully stopped; events may be re-processed from checkpoint |

## Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Config diff computation | < 1ms | Unit test timer |
| Validation phase (5 pipelines) | < 500ms | Integration test |
| Pipeline drain (empty state) | < 1s | Integration test |
| Pipeline drain (1GB state) | < 30s | Integration test |
| Total reload (no changes) | < 10ms | Integration test |
| Total reload (1 modified pipeline) | < 5s | Integration test |

## Test Plan

### Unit Tests

- [ ] `test_diff_configs_no_changes` -- Identical configs produce empty diff
- [ ] `test_diff_configs_added_pipeline` -- New pipeline detected
- [ ] `test_diff_configs_removed_pipeline` -- Removed pipeline detected
- [ ] `test_diff_configs_modified_pipeline` -- Changed SQL detected
- [ ] `test_diff_configs_mixed_changes` -- Mix of add/remove/modify/unchanged
- [ ] `test_diff_configs_added_source` -- New source detected
- [ ] `test_diff_configs_removed_sink` -- Removed sink detected
- [ ] `test_diff_named_items_all_added` -- All items new
- [ ] `test_diff_named_items_all_removed` -- All items removed
- [ ] `test_diff_named_items_all_unchanged` -- All items identical
- [ ] `test_reload_error_display_messages` -- All error variants readable

### Integration Tests

- [ ] `test_reload_add_new_pipeline` -- Add pipeline via reload, verify running
- [ ] `test_reload_remove_pipeline` -- Remove pipeline via reload, verify stopped
- [ ] `test_reload_modify_pipeline_sql` -- Change SQL, verify pipeline restarts from checkpoint
- [ ] `test_reload_validation_failure_no_changes` -- Invalid SQL aborts entire reload
- [ ] `test_reload_preserves_unchanged_pipelines` -- Unchanged pipelines continue processing
- [ ] `test_reload_via_http_endpoint` -- POST /api/v1/reload triggers reload
- [ ] `test_sighup_triggers_reload` -- (Unix only) SIGHUP triggers reload
- [ ] `test_drain_timeout_forces_stop` -- Pipeline drain times out, force stopped

### Benchmarks

- [ ] `bench_config_diff_small` -- 5 pipelines, target: < 1ms
- [ ] `bench_config_diff_large` -- 100 pipelines, target: < 10ms
- [ ] `bench_reload_no_changes` -- Same config, target: < 10ms

## Rollout Plan

1. **Phase 1**: Implement `diff_configs()` and `ConfigDiff` structs
2. **Phase 2**: Implement `validate_reload()` dry-run validation
3. **Phase 3**: Implement `drain_pipeline()` procedure
4. **Phase 4**: Implement `apply_reload()` with ordered operations
5. **Phase 5**: Implement `execute_reload()` orchestrator
6. **Phase 6**: Wire into HTTP endpoint (`POST /api/v1/reload`)
7. **Phase 7**: Implement SIGHUP handler (Unix) with platform cfg
8. **Phase 8**: Integration tests and edge case testing
9. **Phase 9**: Code review and merge

## Open Questions

- [ ] Should config sections implement `PartialEq` for diffing, or use a hash-based comparison?
- [ ] Should the drain timeout be configurable per-pipeline in TOML?
- [ ] Should we support "preview mode" that computes the diff and validates without applying?
- [ ] If `[state]` or `[checkpoint]` sections change, should the reload be rejected (requires full restart)?
- [ ] Should removed sinks be drained (flush buffered output) before stopping?
- [ ] On Windows, should we support a named pipe trigger as an alternative to HTTP-only reload?

## Completion Checklist

- [ ] `ConfigDiff` and `ItemDiff` structs implemented
- [ ] `diff_configs()` function implemented
- [ ] `validate_reload()` dry-run validation implemented
- [ ] `drain_pipeline()` procedure implemented
- [ ] `apply_reload()` with ordered operations
- [ ] `execute_reload()` orchestrator with atomicity
- [ ] HTTP endpoint wired (`POST /api/v1/reload`)
- [ ] SIGHUP handler installed (Unix)
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Documentation updated (`#![deny(missing_docs)]`)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [F-SERVER-001: TOML Config](F-SERVER-001-toml-config.md) -- Config parsing
- [F-SERVER-002: Engine Construction](F-SERVER-002-engine-construction.md) -- Pipeline construction
- [F-SERVER-003: HTTP API](F-SERVER-003-http-api.md) -- Reload endpoint
- [Nginx Hot Reload](https://nginx.org/en/docs/control.html) -- Inspiration for SIGHUP reload
- [Apache Flink Savepoints](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/savepoints/) -- Pipeline restart from checkpoint

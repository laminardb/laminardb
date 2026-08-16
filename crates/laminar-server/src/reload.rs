//! Hot-reload: config diff engine and incremental DDL application.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde::Serialize;
use tracing::{info, warn};

use laminar_db::LaminarDB;

use crate::config::{LookupConfig, PipelineConfig, ServerConfig, SinkConfig, SourceConfig};
use crate::server;

#[derive(Debug, Default)]
pub struct ConfigDiff {
    pub sources_added: Vec<SourceConfig>,
    pub sources_removed: Vec<SourceConfig>,
    pub sources_changed: Vec<SourceConfig>,

    pub lookups_added: Vec<LookupConfig>,
    pub lookups_removed: Vec<LookupConfig>,
    pub lookups_changed: Vec<LookupConfig>,

    pub pipelines_added: Vec<PipelineConfig>,
    pub pipelines_removed: Vec<PipelineConfig>,
    pub pipelines_changed: Vec<PipelineConfig>,

    pub sinks_added: Vec<SinkConfig>,
    pub sinks_removed: Vec<SinkConfig>,
    pub sinks_changed: Vec<SinkConfig>,

    pub warnings: Vec<String>,
}

impl ConfigDiff {
    pub fn is_empty(&self) -> bool {
        self.sources_added.is_empty()
            && self.sources_removed.is_empty()
            && self.sources_changed.is_empty()
            && self.lookups_added.is_empty()
            && self.lookups_removed.is_empty()
            && self.lookups_changed.is_empty()
            && self.pipelines_added.is_empty()
            && self.pipelines_removed.is_empty()
            && self.pipelines_changed.is_empty()
            && self.sinks_added.is_empty()
            && self.sinks_removed.is_empty()
            && self.sinks_changed.is_empty()
    }
}

#[derive(Debug, Serialize)]
pub struct ReloadResult {
    pub success: bool,
    pub applied: Vec<ReloadOp>,
    pub failed: Vec<ReloadFailure>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct ReloadOp {
    pub action: String,
    pub object_type: String,
    pub name: String,
}

#[derive(Debug, Serialize)]
pub struct ReloadFailure {
    pub action: String,
    pub object_type: String,
    pub name: String,
    pub error: String,
}

/// Compute the diff between an old and new configuration.
pub fn diff_configs(old: &ServerConfig, new: &ServerConfig) -> ConfigDiff {
    let mut diff = ConfigDiff::default();

    // Diff named sections
    diff_named_section(
        &old.sources,
        &new.sources,
        |s| &s.name,
        &mut diff.sources_added,
        &mut diff.sources_removed,
        &mut diff.sources_changed,
    );

    diff_named_section(
        &old.lookups,
        &new.lookups,
        |l| &l.name,
        &mut diff.lookups_added,
        &mut diff.lookups_removed,
        &mut diff.lookups_changed,
    );

    diff_named_section(
        &old.pipelines,
        &new.pipelines,
        |p| &p.name,
        &mut diff.pipelines_added,
        &mut diff.pipelines_removed,
        &mut diff.pipelines_changed,
    );

    diff_named_section(
        &old.sinks,
        &new.sinks,
        |s| &s.name,
        &mut diff.sinks_added,
        &mut diff.sinks_removed,
        &mut diff.sinks_changed,
    );

    // Non-reloadable section warnings
    if old.server != new.server {
        diff.warnings
            .push("[server] section changed — requires restart".to_string());
    }
    if old.checkpoint != new.checkpoint {
        diff.warnings
            .push("[checkpoint] section changed — requires restart".to_string());
    }
    if old.discovery != new.discovery {
        diff.warnings
            .push("[discovery] section changed — requires restart".to_string());
    }
    if old.sql != new.sql {
        diff.warnings
            .push("sql field changed — requires restart".to_string());
    }
    // Restart policy / coordinated recovery is captured once at startup (to_policy /
    // enable_supervision), so a live edit needs a restart rather than being silently dropped.
    if old.supervision != new.supervision {
        diff.warnings
            .push("[supervision] section changed — requires restart".to_string());
    }
    if old.node_id != new.node_id {
        diff.warnings
            .push("node_id changed — requires restart".to_string());
    }
    // The AI runtime (providers, models, defaults) is built once at startup and
    // isn't hot-swappable, so changes need a restart rather than being ignored.
    if old.ai != new.ai {
        diff.warnings
            .push("[ai] section changed — requires restart".to_string());
    }
    if old.models != new.models {
        diff.warnings
            .push("[models] section changed — requires restart".to_string());
    }

    diff
}

/// Commit only the configuration sections supported by live reload.
pub(crate) fn commit_reloadable_config(current: &mut ServerConfig, new: ServerConfig) {
    current.sources = new.sources;
    current.lookups = new.lookups;
    current.pipelines = new.pipelines;
    current.sinks = new.sinks;
}

fn diff_named_section<T: Clone + PartialEq>(
    old: &[T],
    new: &[T],
    name_fn: fn(&T) -> &str,
    added: &mut Vec<T>,
    removed: &mut Vec<T>,
    changed: &mut Vec<T>,
) {
    // Build name → item maps
    let old_map: std::collections::HashMap<&str, &T> =
        old.iter().map(|item| (name_fn(item), item)).collect();
    let new_map: std::collections::HashMap<&str, &T> =
        new.iter().map(|item| (name_fn(item), item)).collect();

    // Removed: in old but not in new
    for (name, item) in &old_map {
        if !new_map.contains_key(name) {
            removed.push((*item).clone());
        }
    }

    // Added or changed: in new
    for (name, new_item) in &new_map {
        match old_map.get(name) {
            None => added.push((*new_item).clone()),
            Some(old_item) => {
                if *old_item != *new_item {
                    changed.push((*new_item).clone());
                }
            }
        }
    }
}

/// Apply a config diff to a live `LaminarDB` instance via incremental DDL.
///
/// Remove phase (reverse dependency order): sinks → streams → lookups → sources.
/// Create phase (dependency order): sources → lookups → pipelines → sinks.
pub async fn apply_reload(db: &LaminarDB, diff: &ConfigDiff) -> ReloadResult {
    let mut applied = Vec::new();
    let mut failed = Vec::new();

    // Remove phase (reverse dependency order)
    for sink in diff.sinks_removed.iter().chain(diff.sinks_changed.iter()) {
        let ddl = format!("DROP SINK IF EXISTS {} CASCADE", sink.name);
        exec_ddl(
            db,
            &ddl,
            "drop",
            "sink",
            &sink.name,
            &mut applied,
            &mut failed,
        )
        .await;
    }
    for p in diff
        .pipelines_removed
        .iter()
        .chain(diff.pipelines_changed.iter())
    {
        let ddl = format!("DROP STREAM IF EXISTS {} CASCADE", p.name);
        exec_ddl(
            db,
            &ddl,
            "drop",
            "stream",
            &p.name,
            &mut applied,
            &mut failed,
        )
        .await;
    }
    for l in diff
        .lookups_removed
        .iter()
        .chain(diff.lookups_changed.iter())
    {
        let ddl = format!("DROP LOOKUP TABLE IF EXISTS {} CASCADE", l.name);
        exec_ddl(
            db,
            &ddl,
            "drop",
            "lookup",
            &l.name,
            &mut applied,
            &mut failed,
        )
        .await;
    }
    for s in diff
        .sources_removed
        .iter()
        .chain(diff.sources_changed.iter())
    {
        let ddl = format!("DROP SOURCE IF EXISTS {} CASCADE", s.name);
        exec_ddl(
            db,
            &ddl,
            "drop",
            "source",
            &s.name,
            &mut applied,
            &mut failed,
        )
        .await;
    }

    // Create phase (dependency order)
    for s in diff.sources_added.iter().chain(diff.sources_changed.iter()) {
        let ddl = server::source_to_ddl(s);
        exec_ddl(
            db,
            &ddl,
            "create",
            "source",
            &s.name,
            &mut applied,
            &mut failed,
        )
        .await;
    }
    for l in diff.lookups_added.iter().chain(diff.lookups_changed.iter()) {
        match server::lookup_to_ddl(l) {
            Ok(ddl) => {
                exec_ddl(
                    db,
                    &ddl,
                    "create",
                    "lookup",
                    &l.name,
                    &mut applied,
                    &mut failed,
                )
                .await;
            }
            Err(e) => {
                warn!("Invalid lookup config '{}': {e}", l.name);
                failed.push(ReloadFailure {
                    action: "create".to_string(),
                    object_type: "lookup".to_string(),
                    name: l.name.clone(),
                    error: e.to_string(),
                });
            }
        }
    }
    for p in diff
        .pipelines_added
        .iter()
        .chain(diff.pipelines_changed.iter())
    {
        let ddl = server::pipeline_to_ddl(p);
        exec_ddl(
            db,
            &ddl,
            "create",
            "stream",
            &p.name,
            &mut applied,
            &mut failed,
        )
        .await;
    }
    for sink in diff.sinks_added.iter().chain(diff.sinks_changed.iter()) {
        let ddl = server::sink_to_ddl(sink);
        exec_ddl(
            db,
            &ddl,
            "create",
            "sink",
            &sink.name,
            &mut applied,
            &mut failed,
        )
        .await;
    }

    ReloadResult {
        success: failed.is_empty(),
        applied,
        failed,
        warnings: diff.warnings.clone(),
    }
}

async fn exec_ddl(
    db: &LaminarDB,
    ddl: &str,
    action: &str,
    object_type: &str,
    name: &str,
    applied: &mut Vec<ReloadOp>,
    failed: &mut Vec<ReloadFailure>,
) {
    match db.execute(ddl).await {
        Ok(_) => {
            info!("{action} {object_type}: {name}");
            applied.push(ReloadOp {
                action: action.to_string(),
                object_type: object_type.to_string(),
                name: name.to_string(),
            });
        }
        Err(e) => {
            warn!("Failed to {action} {object_type} '{name}': {e}");
            failed.push(ReloadFailure {
                action: action.to_string(),
                object_type: object_type.to_string(),
                name: name.to_string(),
                error: e.to_string(),
            });
        }
    }
}

/// Prevents concurrent reloads via CAS on an `AtomicBool`.
#[derive(Clone)]
pub struct ReloadGuard {
    in_progress: Arc<AtomicBool>,
}

impl ReloadGuard {
    pub fn new() -> Self {
        Self {
            in_progress: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn try_acquire(&self) -> Option<ReloadGuardHandle> {
        let was_free =
            self.in_progress
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire);
        if was_free.is_ok() {
            Some(ReloadGuardHandle {
                flag: Arc::clone(&self.in_progress),
            })
        } else {
            None
        }
    }
}

pub struct ReloadGuardHandle {
    flag: Arc<AtomicBool>,
}

impl Drop for ReloadGuardHandle {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests;

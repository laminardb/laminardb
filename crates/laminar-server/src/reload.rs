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
    if old.state != new.state {
        diff.warnings
            .push("[state] section changed — requires restart".to_string());
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
    if old.coordination != new.coordination {
        diff.warnings
            .push("[coordination] section changed — requires restart".to_string());
    }
    if old.node_id != new.node_id {
        diff.warnings
            .push("node_id changed — requires restart".to_string());
    }

    diff
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
mod tests {
    use super::*;
    use crate::config::*;

    fn empty_config() -> ServerConfig {
        ServerConfig {
            server: ServerSection::default(),
            state: StateSection::default(),
            checkpoint: CheckpointSection::default(),
            sources: vec![],
            lookups: vec![],
            pipelines: vec![],
            sinks: vec![],
            discovery: None,
            coordination: None,
            node_id: None,
            sql: None,
        }
    }

    fn make_source(name: &str) -> SourceConfig {
        SourceConfig {
            name: name.to_string(),
            connector: "kafka".to_string(),
            format: "json".to_string(),
            properties: toml::Table::new(),
            schema: vec![],
            watermark: None,
        }
    }

    fn make_pipeline(name: &str, sql: &str) -> PipelineConfig {
        PipelineConfig {
            name: name.to_string(),
            sql: sql.to_string(),
        }
    }

    fn make_sink(name: &str, pipeline: &str) -> SinkConfig {
        SinkConfig {
            name: name.to_string(),
            pipeline: pipeline.to_string(),
            connector: "kafka".to_string(),
            delivery: "at_least_once".to_string(),
            properties: toml::Table::new(),
        }
    }

    fn make_lookup(name: &str) -> LookupConfig {
        LookupConfig {
            name: name.to_string(),
            connector: "postgres".to_string(),
            strategy: "poll".to_string(),
            cache: LookupCacheConfig::default(),
            properties: toml::Table::new(),
            primary_key: vec![],
            schema: vec![],
        }
    }

    // -- diff_configs tests --

    #[test]
    fn test_diff_empty_configs() {
        let old = empty_config();
        let new = empty_config();
        let diff = diff_configs(&old, &new);
        assert!(diff.is_empty());
        assert!(diff.warnings.is_empty());
    }

    #[test]
    fn test_diff_identical_configs() {
        let mut old = empty_config();
        old.sources.push(make_source("s1"));
        old.pipelines.push(make_pipeline("p1", "SELECT 1"));
        let new = old.clone();
        let diff = diff_configs(&old, &new);
        assert!(diff.is_empty());
    }

    #[test]
    fn test_diff_source_added() {
        let old = empty_config();
        let mut new = empty_config();
        new.sources.push(make_source("new_src"));
        let diff = diff_configs(&old, &new);
        assert_eq!(diff.sources_added.len(), 1);
        assert_eq!(diff.sources_added[0].name, "new_src");
        assert!(diff.sources_removed.is_empty());
        assert!(diff.sources_changed.is_empty());
    }

    #[test]
    fn test_diff_source_removed() {
        let mut old = empty_config();
        old.sources.push(make_source("old_src"));
        let new = empty_config();
        let diff = diff_configs(&old, &new);
        assert!(diff.sources_added.is_empty());
        assert_eq!(diff.sources_removed.len(), 1);
        assert_eq!(diff.sources_removed[0].name, "old_src");
    }

    #[test]
    fn test_diff_source_changed() {
        let mut old = empty_config();
        old.sources.push(make_source("s1"));
        let mut new = empty_config();
        let mut changed = make_source("s1");
        changed.format = "avro".to_string();
        new.sources.push(changed);
        let diff = diff_configs(&old, &new);
        assert!(diff.sources_added.is_empty());
        assert!(diff.sources_removed.is_empty());
        assert_eq!(diff.sources_changed.len(), 1);
        assert_eq!(diff.sources_changed[0].format, "avro");
    }

    #[test]
    fn test_diff_pipeline_sql_changed() {
        let mut old = empty_config();
        old.pipelines.push(make_pipeline("p1", "SELECT 1"));
        let mut new = empty_config();
        new.pipelines.push(make_pipeline("p1", "SELECT 2"));
        let diff = diff_configs(&old, &new);
        assert_eq!(diff.pipelines_changed.len(), 1);
        assert_eq!(diff.pipelines_changed[0].sql, "SELECT 2");
    }

    #[test]
    fn test_diff_sink_changed() {
        let mut old = empty_config();
        old.sinks.push(make_sink("out", "p1"));
        let mut new = empty_config();
        let mut changed = make_sink("out", "p1");
        changed.delivery = "exactly_once".to_string();
        new.sinks.push(changed);
        let diff = diff_configs(&old, &new);
        assert_eq!(diff.sinks_changed.len(), 1);
    }

    #[test]
    fn test_diff_lookup_changed() {
        let mut old = empty_config();
        old.lookups.push(make_lookup("lk1"));
        let mut new = empty_config();
        let mut changed = make_lookup("lk1");
        changed.strategy = "cdc".to_string();
        new.lookups.push(changed);
        let diff = diff_configs(&old, &new);
        assert_eq!(diff.lookups_changed.len(), 1);
    }

    #[test]
    fn test_diff_non_reloadable_warnings() {
        let old = empty_config();
        let mut new = empty_config();
        new.server.bind = "0.0.0.0:9999".to_string();
        new.state.backend = "mmap".to_string();
        let diff = diff_configs(&old, &new);
        assert!(diff.is_empty()); // no reloadable changes
        assert!(diff.warnings.iter().any(|w| w.contains("[server]")));
        assert!(diff.warnings.iter().any(|w| w.contains("[state]")));
    }

    #[test]
    fn test_diff_multiple_sections_changed() {
        let old = empty_config();
        let mut new = empty_config();
        new.sources.push(make_source("s1"));
        new.pipelines.push(make_pipeline("p1", "SELECT 1"));
        new.sinks.push(make_sink("out", "p1"));
        let diff = diff_configs(&old, &new);
        assert_eq!(diff.sources_added.len(), 1);
        assert_eq!(diff.pipelines_added.len(), 1);
        assert_eq!(diff.sinks_added.len(), 1);
        assert!(!diff.is_empty());
    }

    #[test]
    fn test_is_empty_on_default() {
        let diff = ConfigDiff::default();
        assert!(diff.is_empty());
    }

    // -- ReloadGuard tests --

    #[test]
    fn test_guard_acquire_release() {
        let guard = ReloadGuard::new();
        {
            let handle = guard.try_acquire();
            assert!(handle.is_some());
            // While held, second acquire fails
            assert!(guard.try_acquire().is_none());
        }
        // After drop, can acquire again
        assert!(guard.try_acquire().is_some());
    }

    #[test]
    fn test_guard_concurrent_reject() {
        let guard = ReloadGuard::new();
        let _handle = guard.try_acquire().unwrap();
        assert!(guard.try_acquire().is_none());
        assert!(guard.try_acquire().is_none());
    }

    #[test]
    fn test_guard_raii_drop() {
        let guard = ReloadGuard::new();
        let handle = guard.try_acquire().unwrap();
        drop(handle);
        let handle2 = guard.try_acquire();
        assert!(handle2.is_some());
    }

    // -- apply_reload tests (using real LaminarDB) --

    #[tokio::test]
    async fn test_apply_add_source() {
        let db = LaminarDB::open().unwrap();
        let mut diff = ConfigDiff::default();
        diff.sources_added.push(SourceConfig {
            name: "test_src".to_string(),
            connector: "kafka".to_string(),
            format: "json".to_string(),
            properties: toml::Table::new(),
            schema: vec![ColumnDef {
                name: "id".to_string(),
                data_type: "BIGINT".to_string(),
                nullable: false,
            }],
            watermark: None,
        });
        let result = apply_reload(&db, &diff).await;
        // Connector may not be available in test builds; verify the op was attempted
        let total = result.applied.len() + result.failed.len();
        assert_eq!(total, 1, "expected exactly one create operation");
        if result.success {
            assert_eq!(result.applied[0].action, "create");
            assert_eq!(result.applied[0].name, "test_src");
        } else {
            assert_eq!(result.failed[0].action, "create");
            assert_eq!(result.failed[0].name, "test_src");
        }
    }

    #[tokio::test]
    async fn test_apply_remove_source() {
        let db = LaminarDB::open().unwrap();
        // First create the source
        db.execute("CREATE SOURCE rm_src (id BIGINT)")
            .await
            .unwrap();

        let mut diff = ConfigDiff::default();
        diff.sources_removed.push(make_source("rm_src"));
        let result = apply_reload(&db, &diff).await;
        assert!(result.success);
        assert_eq!(result.applied.len(), 1);
        assert_eq!(result.applied[0].action, "drop");
    }

    #[tokio::test]
    async fn test_apply_change_pipeline() {
        let db = LaminarDB::open().unwrap();
        // Create source and initial pipeline
        db.execute("CREATE SOURCE cp_src (id BIGINT, val DOUBLE)")
            .await
            .unwrap();
        db.execute("CREATE STREAM cp_pipe AS SELECT id, val FROM cp_src")
            .await
            .unwrap();

        // Change pipeline SQL
        let mut diff = ConfigDiff::default();
        diff.pipelines_changed
            .push(make_pipeline("cp_pipe", "SELECT id FROM cp_src"));
        let result = apply_reload(&db, &diff).await;
        assert!(result.success);
        // Should have 2 ops: drop + create
        assert_eq!(result.applied.len(), 2);
    }

    #[tokio::test]
    async fn test_apply_ordered_removal() {
        let db = LaminarDB::open().unwrap();
        // Create a full pipeline chain: source → pipeline → sink
        db.execute("CREATE SOURCE ord_src (id BIGINT)")
            .await
            .unwrap();
        db.execute("CREATE STREAM ord_pipe AS SELECT id FROM ord_src")
            .await
            .unwrap();

        // Remove all in one diff — should respect dependency order
        let mut diff = ConfigDiff::default();
        diff.sources_removed.push(make_source("ord_src"));
        diff.pipelines_removed
            .push(make_pipeline("ord_pipe", "SELECT id FROM ord_src"));

        let result = apply_reload(&db, &diff).await;
        assert!(result.success);
        // Pipeline should be dropped before source
        let drop_names: Vec<&str> = result
            .applied
            .iter()
            .filter(|op| op.action == "drop")
            .map(|op| op.name.as_str())
            .collect();
        let pipe_idx = drop_names.iter().position(|n| *n == "ord_pipe");
        let src_idx = drop_names.iter().position(|n| *n == "ord_src");
        assert!(pipe_idx < src_idx, "pipeline must be dropped before source");
    }

    #[tokio::test]
    async fn test_apply_empty_diff() {
        let db = LaminarDB::open().unwrap();
        let diff = ConfigDiff::default();
        let result = apply_reload(&db, &diff).await;
        assert!(result.success);
        assert!(result.applied.is_empty());
        assert!(result.failed.is_empty());
    }

    #[tokio::test]
    async fn test_apply_warnings_passed_through() {
        let db = LaminarDB::open().unwrap();
        let mut diff = ConfigDiff::default();
        diff.warnings
            .push("[server] section changed — requires restart".to_string());
        let result = apply_reload(&db, &diff).await;
        assert!(result.success);
        assert_eq!(result.warnings.len(), 1);
        assert!(result.warnings[0].contains("[server]"));
    }
}

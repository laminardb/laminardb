use super::*;
use crate::config::*;

fn empty_config() -> ServerConfig {
    ServerConfig {
        server: ServerSection::default(),
        checkpoint: CheckpointSection::default(),
        supervision: Default::default(),
        sources: vec![],
        lookups: vec![],
        pipelines: vec![],
        sinks: vec![],
        discovery: None,
        node_id: None,
        sql: None,
        ai: Default::default(),
        models: Default::default(),
    }
}

fn make_source(name: &str) -> SourceConfig {
    SourceConfig {
        name: name.to_string(),
        connector: "kafka".to_string(),
        format: "json".to_string(),
        properties: toml::Table::new(),
        schema: vec![],
        primary_key: vec![],
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
        format: None,
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

#[test]
fn commit_reloadable_config_replaces_only_live_sections() {
    let mut current = empty_config();
    current.sources.push(make_source("old_source"));
    current.lookups.push(make_lookup("old_lookup"));
    current
        .pipelines
        .push(make_pipeline("old_pipeline", "SELECT 1"));
    current.sinks.push(make_sink("old_sink", "old_pipeline"));
    current.server.bind = "127.0.0.1:8100".to_owned();
    current.server.console_token = Some(Secret::new("old-console-token"));
    current.checkpoint.url = "file:///old-checkpoints".to_owned();
    current.supervision.max_restarts = Some(3);
    current.sql = Some("CREATE SOURCE retained (id BIGINT)".to_owned());
    current.discovery = Some(DiscoverySection {
        strategy: "static".to_owned(),
        seeds: vec!["127.0.0.1:9000".to_owned()],
        gossip_port: 9_001,
        advertise_host: Some("127.0.0.1".to_owned()),
        failure_domain: Some("rack=old".to_owned()),
        placement_isolation_tier: 0,
        cluster_tls_cert: None,
        cluster_tls_key: None,
        cluster_tls_client_ca: None,
        cluster_tls_server_name: None,
    });
    current.node_id = Some("old-node".to_owned());
    current
        .ai
        .defaults
        .insert("classify".to_owned(), "old-model".to_owned());
    current.models.insert(
        "old-model".to_owned(),
        ModelConfig {
            kind: "local".to_owned(),
            task: TaskSpec::One("classify".to_owned()),
            provider: None,
            model: None,
            source: Some("file:///old-model".to_owned()),
        },
    );

    let retained_server = current.server.clone();
    let retained_checkpoint = current.checkpoint.clone();
    let retained_supervision = current.supervision.clone();
    let retained_sql = current.sql.clone();
    let retained_discovery = current.discovery.clone();
    let retained_node_id = current.node_id.clone();
    let retained_ai = current.ai.clone();
    let retained_models = current.models.clone();

    let mut new = current.clone();
    new.sources = vec![make_source("new_source")];
    new.lookups = vec![make_lookup("new_lookup")];
    new.pipelines = vec![make_pipeline("new_pipeline", "SELECT 2")];
    new.sinks = vec![make_sink("new_sink", "new_pipeline")];
    new.server.bind = "127.0.0.1:8200".to_owned();
    new.server.console_token = Some(Secret::new("new-console-token"));
    new.checkpoint.url = "file:///new-checkpoints".to_owned();
    new.supervision.max_restarts = Some(9);
    new.sql = Some("CREATE SOURCE replaced (id BIGINT)".to_owned());
    new.discovery.as_mut().unwrap().strategy = "gossip".to_owned();
    new.node_id = Some("new-node".to_owned());
    new.ai.defaults.clear();
    new.models.clear();

    commit_reloadable_config(&mut current, new);

    assert_eq!(current.sources, vec![make_source("new_source")]);
    assert_eq!(current.lookups, vec![make_lookup("new_lookup")]);
    assert_eq!(
        current.pipelines,
        vec![make_pipeline("new_pipeline", "SELECT 2")]
    );
    assert_eq!(current.sinks, vec![make_sink("new_sink", "new_pipeline")]);
    assert_eq!(current.server, retained_server);
    assert_eq!(
        current.server.console_token.as_ref().unwrap().expose(),
        "old-console-token"
    );
    assert_eq!(current.checkpoint, retained_checkpoint);
    assert_eq!(current.supervision, retained_supervision);
    assert_eq!(current.sql, retained_sql);
    assert_eq!(current.discovery, retained_discovery);
    assert_eq!(current.node_id, retained_node_id);
    assert_eq!(current.ai, retained_ai);
    assert_eq!(current.models, retained_models);
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
    changed.properties.insert(
        "topic".to_string(),
        toml::Value::String("new-topic".to_string()),
    );
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
    new.checkpoint.url = "file:///data/checkpoints".to_owned();
    new.ai
        .defaults
        .insert("classify".to_string(), "m".to_string());
    let diff = diff_configs(&old, &new);
    assert!(diff.is_empty()); // no reloadable changes
    assert!(diff.warnings.iter().any(|w| w.contains("[server]")));
    assert!(diff.warnings.iter().any(|w| w.contains("[checkpoint]")));
    assert!(diff.warnings.iter().any(|w| w.contains("[ai]")));
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
        primary_key: vec![],
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

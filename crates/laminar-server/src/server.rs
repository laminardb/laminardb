//! Engine construction and lifecycle for LaminarDB server.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::signal;
use tracing::info;

use laminar_core::streaming::checkpoint::StreamCheckpointConfig;
use laminar_db::{DbError, EngineMetrics, LaminarDB, Profile};

#[cfg(feature = "cluster")]
use crate::cluster_config::{ClusterConfig, ClusterConfigError};
use crate::config::{
    ConfigError, LookupConfig, PipelineConfig, ServerConfig, ServerMode, SinkConfig, SourceConfig,
};
use crate::http;
#[cfg(feature = "cluster")]
use crate::http::ClusterComponents;
use crate::metrics::ServerMetrics;
use crate::reload::ReloadGuard;
#[cfg(all(test, any(feature = "otel", feature = "kafka", feature = "cluster")))]
use laminar_core::state::StateBackendConfig;
use laminar_core::state::StateBackendDurability;

/// Handle to a running LaminarDB server. Call `wait_for_shutdown` to block until Ctrl-C.
pub enum ServerHandle {
    Single {
        db: Arc<LaminarDB>,
        api_handle: tokio::task::JoinHandle<()>,
        pgwire_handle: Option<tokio::task::JoinHandle<()>>,
        watcher_handle: Option<tokio::task::JoinHandle<()>>,
    },
    #[cfg(feature = "cluster")]
    Cluster(Box<crate::cluster::ClusterHandle>),
}

impl ServerHandle {
    /// Block until SIGINT/SIGTERM, then gracefully shut down.
    pub async fn wait_for_shutdown(self) -> Result<(), ServerError> {
        match self {
            Self::Single {
                db,
                api_handle,
                pgwire_handle,
                watcher_handle,
            } => {
                wait_for_termination_signal().await?;

                info!("Received shutdown signal, shutting down...");

                if let Some(wh) = &watcher_handle {
                    wh.abort();
                }
                if let Some(pg) = &pgwire_handle {
                    pg.abort();
                }
                db.shutdown()
                    .await
                    .map_err(|e| ServerError::Shutdown(e.to_string()))?;
                api_handle.abort();

                info!("Shutdown complete");
                Ok(())
            }
            #[cfg(feature = "cluster")]
            Self::Cluster(handle) => (*handle)
                .wait_for_shutdown()
                .await
                .map_err(|e| ServerError::Cluster(e.to_string())),
        }
    }
}

async fn wait_for_termination_signal() -> Result<(), ServerError> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())
            .map_err(|e| ServerError::Shutdown(format!("SIGTERM handler failed: {e}")))?;
        tokio::select! {
            result = signal::ctrl_c() => {
                result.map_err(|e| ServerError::Shutdown(format!("SIGINT handler failed: {e}")))?;
            }
            _ = sigterm.recv() => {}
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        signal::ctrl_c()
            .await
            .map_err(|e| ServerError::Shutdown(format!("signal handler failed: {e}")))?;
        Ok(())
    }
}

/// Build and start a LaminarDB server from the given configuration.
pub async fn run_server(
    config: ServerConfig,
    config_path: PathBuf,
) -> Result<ServerHandle, ServerError> {
    // Cluster mode: gated behind the `cluster` feature flag.
    #[cfg(feature = "cluster")]
    {
        let cluster_cfg = ClusterConfig::from_server_config(&config)?;

        if let Some(cluster_cfg) = cluster_cfg {
            let handle = crate::cluster::start_cluster(config, cluster_cfg, config_path)
                .await
                .map_err(|e| ServerError::Cluster(e.to_string()))?;
            return Ok(ServerHandle::Cluster(Box::new(handle)));
        }
    }
    #[cfg(not(feature = "cluster"))]
    if config.server.mode == ServerMode::Cluster {
        return Err(ServerError::Cluster(
            "Cluster mode requires the 'cluster' feature flag. \
             This mode is not yet production-ready."
                .to_string(),
        ));
    }

    // Build LaminarDB via builder API
    let mut builder = LaminarDB::builder();
    builder = builder.delivery_guarantee(config.server.delivery);
    if let Some(ref token) = config.server.console_token {
        builder = builder.http_auth_token(token.expose());
    }
    let storage_dir = config.state.local_storage_dir();
    let has_storage = config
        .state
        .durability_scope()
        .satisfies(StateBackendDurability::NodeDurable);
    if let Some(path) = storage_dir {
        builder = builder.storage_dir(path);
    }

    let profile = match config.server.mode {
        ServerMode::Single if has_storage => Profile::Embedded,
        ServerMode::Single => Profile::BareMetal,
        ServerMode::Cluster => Profile::Cluster,
    };
    builder = builder.profile(profile);
    builder = builder.restart_policy(config.supervision.to_policy());
    builder = builder.incremental_emit(config.server.incremental_emit);
    builder = apply_checkpoint_config(builder, &config.checkpoint.url, &config.checkpoint, false);

    // Build the state backend + single-owner vnode registry from config so
    // the checkpoint coordinator's durability gate runs with real markers.
    let key_groups = config.server.resolved_key_groups();
    let state_backend = config
        .state
        .build(key_groups)
        .map_err(|e| ServerError::Build(format!("state backend: {e}")))?;
    let vnode_registry = Arc::new(laminar_core::state::VnodeRegistry::single_owner(
        u32::from(key_groups),
        laminar_core::state::NodeId(0),
    ));
    builder = builder
        .state_backend(state_backend)
        .vnode_registry(vnode_registry);

    // Build the AI subsystem from `[ai]`/`[models]` and install it. Without
    // configured models this is a no-op and `ai_*` functions fail at plan time.
    if let Some(ai_runtime) = crate::ai::build_ai_runtime(&config)? {
        builder = builder.ai(ai_runtime);
    }

    let db = builder
        .build()
        .await
        .map_err(|e| ServerError::Build(e.to_string()))?;
    // Auto-recover from a fatal cycle fault by restarting from the last checkpoint.
    db.enable_supervision();

    // Prometheus registry — must be set before start().
    let hostname = gethostname::gethostname().to_string_lossy().into_owned();
    let pipeline_name = config
        .pipelines
        .first()
        .map_or("default", |p| p.name.as_str())
        .to_string();
    let registry = Arc::new(crate::metrics::build_registry([
        ("instance".into(), hostname),
        ("pipeline".into(), pipeline_name),
    ]));
    let engine_metrics = Arc::new(EngineMetrics::new(&registry));
    db.set_engine_metrics(Arc::clone(&engine_metrics));
    db.set_prometheus_registry(Arc::clone(&registry))
        .map_err(|error| ServerError::Start(error.to_string()))?;

    execute_config_ddl(&db, &config, false).await?;

    db.start()
        .await
        .map_err(|e| ServerError::Start(e.to_string()))?;
    info!("Pipeline started");

    let pgwire_bind = config.server.pgwire_bind.clone();
    let pgwire_users = config.server.pgwire_users.clone();
    let pgwire_allow_remote = config.server.pgwire_allow_remote;
    let pgwire_tls_cert = config.server.pgwire_tls_cert.clone();
    let pgwire_tls_key = config.server.pgwire_tls_key.clone();
    let pgwire_tls_client_ca = config.server.pgwire_tls_client_ca.clone();
    let pgwire_tls_min_version =
        crate::pgwire::TlsMinVersion::from_config_str(&config.server.pgwire_tls_min_version)
            .expect("pgwire_tls_min_version validated at config load");
    let pgwire_max_connections = config.server.pgwire_max_connections;
    let pgwire_max_auth_failures = config.server.pgwire_max_auth_failures_per_min;
    let (app_state, api_handle) = start_http_api(
        Arc::clone(&db),
        registry,
        config_path.clone(),
        config,
        // Single-node server mode has no cluster control plane.
        #[cfg(feature = "cluster")]
        None,
    )
    .await?;
    let watcher_handle = spawn_config_watcher(&app_state, config_path);

    let pgwire_handle = if let Some(bind) = pgwire_bind {
        let tls = match (&pgwire_tls_cert, &pgwire_tls_key) {
            (Some(c), Some(k)) => Some(crate::pgwire::TlsPaths {
                cert: c,
                key: k,
                min_version: pgwire_tls_min_version,
                client_ca: pgwire_tls_client_ca.as_deref(),
            }),
            _ => None,
        };
        match crate::pgwire::serve(
            Arc::clone(&db),
            &bind,
            pgwire_users,
            pgwire_allow_remote,
            tls,
            pgwire_max_connections,
            pgwire_max_auth_failures,
        )
        .await
        {
            Ok((_, h)) => Some(h),
            Err(e) => {
                // Roll back: stop the HTTP server, the file watcher, and the
                // pipeline before propagating the bind failure.
                if let Some(wh) = &watcher_handle {
                    wh.abort();
                }
                api_handle.abort();
                let _ = db.shutdown().await;
                return Err(e);
            }
        }
    } else {
        None
    };

    Ok(ServerHandle::Single {
        db,
        api_handle,
        pgwire_handle,
        watcher_handle,
    })
}

// ---------------------------------------------------------------------------
// Shared helpers (used by both single-node and cluster startup)
// ---------------------------------------------------------------------------

/// Apply checkpoint settings to a `LaminarDB` builder.
///
/// Cluster configuration validation admits only shared cloud URLs. Single-node
/// mode keeps `file://` on the local `FileSystemCheckpointStore` (stable on-disk
/// layout); remote schemes (`s3://`, …) use the object-store implementation.
pub(crate) fn apply_checkpoint_config(
    mut builder: laminar_db::LaminarDbBuilder,
    checkpoint_url: &str,
    checkpoint: &crate::config::CheckpointSection,
    cluster: bool,
) -> laminar_db::LaminarDbBuilder {
    let cfg = StreamCheckpointConfig {
        interval_ms: Some(u64::try_from(checkpoint.interval.as_millis()).unwrap_or(u64::MAX)),
        timeout_ms: Some(u64::try_from(checkpoint.timeout.as_millis()).unwrap_or(u64::MAX)),
        data_dir: file_url_to_path(checkpoint_url),
        max_retained: Some(checkpoint.max_retained),
        max_staged_bytes: checkpoint.max_staged_bytes,
    };
    builder = builder.checkpoint(cfg);

    let is_file = checkpoint_url.starts_with("file://");
    if !checkpoint_url.is_empty() && (cluster || !is_file) {
        builder = builder.object_store_url(checkpoint_url.to_string());
        if !checkpoint.storage.is_empty() {
            builder = builder.object_store_options(checkpoint.storage.clone());
        }
    }

    builder
}

/// Execute DDL for all config sections (sources, lookups, pipelines, sinks, raw SQL).
pub(crate) async fn execute_config_ddl(
    db: &LaminarDB,
    config: &ServerConfig,
    cluster_bootstrap: bool,
) -> Result<(), ServerError> {
    let mut definitions = Vec::new();
    for source in &config.sources {
        definitions.push(("source", source.name.clone(), source_to_ddl(source)));
    }
    for lookup in &config.lookups {
        definitions.push(("lookup", lookup.name.clone(), lookup_to_ddl(lookup)?));
    }
    for pipeline in &config.pipelines {
        definitions.push(("pipeline", pipeline.name.clone(), pipeline_to_ddl(pipeline)));
    }
    for sink in &config.sinks {
        definitions.push(("sink", sink.name.clone(), sink_to_ddl(sink)));
    }
    if let Some(ref sql) = config.sql {
        let trimmed = sql.trim();
        if !trimmed.is_empty() {
            definitions.push((
                "sql",
                trimmed.chars().take(80).collect(),
                trimmed.to_owned(),
            ));
        }
    }

    #[cfg(feature = "cluster")]
    if cluster_bootstrap {
        let sql = definitions
            .iter()
            .map(|(_, _, sql)| sql.clone())
            .collect::<Vec<_>>();
        db.execute_cluster_bootstrap_batch(&sql)
            .await
            .map_err(|source| ServerError::Ddl {
                section: "cluster catalog".to_string(),
                name: "startup inventory".to_string(),
                source,
            })?;
        info!(
            entries = definitions.len(),
            "Sealed cluster catalog inventory"
        );
        return Ok(());
    }
    #[cfg(not(feature = "cluster"))]
    let _ = cluster_bootstrap;

    for (section, name, sql) in definitions {
        db.execute(&sql).await.map_err(|source| ServerError::Ddl {
            section: section.to_string(),
            name: name.clone(),
            source,
        })?;
        info!(section, %name, "Applied configuration DDL");
    }
    Ok(())
}

/// Start HTTP API server and return (shared state, join handle).
///
/// In cluster mode the caller passes `Some(ClusterComponents)` so the
/// `/api/v1/cluster/*` endpoints can surface membership, vnode assignments,
/// and leadership; single-node startup passes `None`.
pub(crate) async fn start_http_api(
    db: Arc<LaminarDB>,
    registry: Arc<prometheus::Registry>,
    config_path: PathBuf,
    config: ServerConfig,
    #[cfg(feature = "cluster")] cluster: Option<ClusterComponents>,
) -> Result<(Arc<http::AppState>, tokio::task::JoinHandle<()>), ServerError> {
    Ok(prepare_http_api(
        db,
        registry,
        config_path,
        config,
        #[cfg(feature = "cluster")]
        cluster,
    )
    .await?
    .start())
}

pub(crate) struct PreparedHttpApi {
    app_state: Arc<http::AppState>,
    router: axum::Router,
    listener: tokio::net::TcpListener,
    bind: String,
}

impl PreparedHttpApi {
    pub(crate) fn start(self) -> (Arc<http::AppState>, tokio::task::JoinHandle<()>) {
        let handle = http::serve_listener(self.router, self.listener);
        info!("HTTP API listening on {}", self.bind);
        (self.app_state, handle)
    }
}

pub(crate) async fn prepare_http_api(
    db: Arc<LaminarDB>,
    registry: Arc<prometheus::Registry>,
    config_path: PathBuf,
    config: ServerConfig,
    #[cfg(feature = "cluster")] cluster: Option<ClusterComponents>,
) -> Result<PreparedHttpApi, ServerError> {
    let bind = config.server.bind.clone();

    let server_metrics = ServerMetrics::new(&registry);

    let app_state = Arc::new(http::AppState {
        db,
        config_path,
        current_config: parking_lot::RwLock::new(config),
        reload_guard: ReloadGuard::new(),
        registry,
        server_metrics,
        ws_slots: http::ws_connection_slots(),
        #[cfg(feature = "cluster")]
        cluster,
    });
    let router = http::build_router(Arc::clone(&app_state));
    let listener = http::bind_listener(&bind).await?;
    Ok(PreparedHttpApi {
        app_state,
        router,
        listener,
        bind,
    })
}

/// Spawn config file watcher unless disabled via `LAMINAR_DISABLE_FILE_WATCH=1`.
pub(crate) fn spawn_config_watcher(
    app_state: &Arc<http::AppState>,
    config_path: PathBuf,
) -> Option<tokio::task::JoinHandle<()>> {
    let disabled =
        std::env::var("LAMINAR_DISABLE_FILE_WATCH").is_ok_and(|v| v == "1" || v == "true");
    if disabled {
        info!("Config file watcher disabled via LAMINAR_DISABLE_FILE_WATCH");
        return None;
    }
    let watcher_state = Arc::clone(app_state);
    let handle = tokio::spawn(async move {
        crate::watcher::watch_config(
            config_path,
            watcher_state,
            std::time::Duration::from_millis(500),
        )
        .await;
    });
    info!("Config file watcher started");
    Some(handle)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract a local filesystem path from a `file://` URL, or `None` for cloud URLs.
fn file_url_to_path(url: &str) -> Option<PathBuf> {
    let raw = url.strip_prefix("file://")?;
    // Accept both absolute file URLs and the explicitly relative form used by
    // portable examples (`file://./state`). A host-style URL remains rejected;
    // local checkpoint storage must not silently reinterpret a hostname.
    if !raw.starts_with('/') && !raw.starts_with("./") && !raw.starts_with("../") {
        return None;
    }
    #[cfg(windows)]
    let raw = {
        let b = raw.as_bytes();
        if b.len() >= 3 && b[0] == b'/' && b[1].is_ascii_alphabetic() && b[2] == b':' {
            &raw[1..]
        } else {
            raw
        }
    };
    Some(PathBuf::from(raw))
}

// ---------------------------------------------------------------------------
// DDL generation
// ---------------------------------------------------------------------------

fn connector_sql_identifier(connector: &str) -> String {
    let mut chars = connector.chars();
    let unquoted = chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && chars.all(|character| character.is_ascii_alphanumeric() || character == '_');
    if unquoted {
        connector.to_ascii_uppercase()
    } else {
        format!("\"{}\"", connector.replace('"', "\"\""))
    }
}

pub fn source_to_ddl(source: &SourceConfig) -> String {
    let mut parts = Vec::new();
    parts.push(format!("CREATE SOURCE {}", source.name));

    // Column definitions
    let mut col_defs: Vec<String> = source
        .schema
        .iter()
        .map(|c| {
            let nullability = if c.nullable { "" } else { " NOT NULL" };
            format!("{} {}{}", c.name, c.data_type, nullability)
        })
        .collect();

    // Watermark clause
    if let Some(wm) = &source.watermark {
        let secs = wm.max_out_of_orderness.as_secs();
        col_defs.push(format!(
            "WATERMARK FOR {} AS {} - INTERVAL '{}' SECOND",
            wm.column, wm.column, secs
        ));
    }

    if !col_defs.is_empty() {
        parts.push(format!("({})", col_defs.join(", ")));
    }

    // FROM CONNECTOR (...) clause
    let connector_keyword = connector_sql_identifier(&source.connector);
    let mut opts = Vec::new();
    opts.push(format!("format = '{}'", source.format));
    for (key, value) in &source.properties {
        // Quote keys that contain dots (e.g. kafka.session.timeout.ms)
        // to prevent SQL parser errors with dotted identifiers.
        if key.contains('.') {
            opts.push(format!("\"{}\" = '{}'", key, toml_value_to_sql(value)));
        } else {
            opts.push(format!("{} = '{}'", key, toml_value_to_sql(value)));
        }
    }
    parts.push(format!("FROM {} ({})", connector_keyword, opts.join(", ")));

    parts.join(" ")
}

pub fn pipeline_to_ddl(pipeline: &PipelineConfig) -> String {
    format!("CREATE STREAM {} AS {}", pipeline.name, pipeline.sql.trim())
}

pub fn sink_to_ddl(sink: &SinkConfig) -> String {
    let connector_keyword = connector_sql_identifier(&sink.connector);
    let opts: Vec<String> = sink
        .properties
        .iter()
        .map(|(key, value)| {
            if key.contains('.') {
                format!("\"{}\" = '{}'", key, toml_value_to_sql(value))
            } else {
                format!("{} = '{}'", key, toml_value_to_sql(value))
            }
        })
        .collect();

    if opts.is_empty() {
        format!(
            "CREATE SINK {} FROM {} INTO {}",
            sink.name, sink.pipeline, connector_keyword
        )
    } else {
        format!(
            "CREATE SINK {} FROM {} INTO {} ({})",
            sink.name,
            sink.pipeline,
            connector_keyword,
            opts.join(", ")
        )
    }
}

#[allow(clippy::result_large_err)]
pub fn lookup_to_ddl(lookup: &LookupConfig) -> Result<String, ServerError> {
    if lookup.schema.is_empty() {
        return Err(ServerError::Ddl {
            section: "lookup".to_string(),
            name: lookup.name.clone(),
            source: DbError::Config(format!(
                "[[lookup]] '{}' requires a [[lookup.schema]] section with at least \
                 one column definition",
                lookup.name,
            )),
        });
    }

    let mut parts = Vec::new();
    parts.push(format!("CREATE LOOKUP TABLE {}", lookup.name));

    // Column definitions + PRIMARY KEY
    let mut col_defs: Vec<String> = lookup
        .schema
        .iter()
        .map(|c| {
            let nullability = if c.nullable { "" } else { " NOT NULL" };
            format!("{} {}{}", c.name, c.data_type, nullability)
        })
        .collect();
    if !lookup.primary_key.is_empty() {
        col_defs.push(format!("PRIMARY KEY ({})", lookup.primary_key.join(", ")));
    }
    parts.push(format!("({})", col_defs.join(", ")));

    // WITH clause
    let mut opts = Vec::new();
    opts.push(format!("'connector' = '{}'", lookup.connector));
    opts.push(format!("'strategy' = '{}'", lookup.strategy));
    // Note: the lookup-table parser keys cache options on the dotted form
    // (`cache.memory` / `cache.ttl`); emitting `cache_memory` / `cache_ttl`
    // here would be silently ignored by validate_properties.
    if lookup.cache.size_bytes != 100 * 1024 * 1024 {
        opts.push(format!("'cache.memory' = '{}'", lookup.cache.size_bytes));
    }
    if lookup.cache.ttl.as_secs() != 300 {
        opts.push(format!("'cache.ttl' = '{}'", lookup.cache.ttl.as_secs()));
    }
    for (key, value) in &lookup.properties {
        opts.push(format!("'{}' = '{}'", key, toml_value_to_sql(value)));
    }
    parts.push(format!("WITH ({})", opts.join(", ")));

    Ok(parts.join(" "))
}

/// Convert a TOML value to a SQL string literal value.
/// Escapes single quotes (SQL standard: ' → '').
fn toml_value_to_sql(value: &toml::Value) -> String {
    match value {
        toml::Value::String(s) => s.replace('\'', "''"),
        toml::Value::Integer(i) => i.to_string(),
        toml::Value::Float(f) => f.to_string(),
        toml::Value::Boolean(b) => b.to_string(),
        toml::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(toml_value_to_sql).collect();
            items.join(",")
        }
        other => format!("{other}"),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("failed to build LaminarDB: {0}")]
    Build(String),
    #[error("failed to execute DDL for {section} '{name}': {source}")]
    Ddl {
        section: String,
        name: String,
        source: DbError,
    },
    #[error("failed to start pipeline: {0}")]
    Start(String),
    #[error("HTTP server error: {0}")]
    Http(String),
    #[error("shutdown error: {0}")]
    Shutdown(String),
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("cluster mode error: {0}")]
    Cluster(String),
    #[cfg(feature = "cluster")]
    #[error(transparent)]
    ClusterConfig(#[from] ClusterConfigError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_checkpoint_url_preserves_explicit_relative_path() {
        assert_eq!(
            file_url_to_path("file://./.laminardb/checkpoints"),
            Some(PathBuf::from("./.laminardb/checkpoints"))
        );
        assert!(file_url_to_path("file://checkpoint-host/path").is_none());
    }
    use crate::config::*;

    fn make_source(name: &str, connector: &str) -> SourceConfig {
        SourceConfig {
            name: name.to_string(),
            connector: connector.to_string(),
            format: "json".to_string(),
            properties: toml::Table::new(),
            schema: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: "BIGINT".to_string(),
                    nullable: false,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: "VARCHAR".to_string(),
                    nullable: true,
                },
            ],
            watermark: None,
        }
    }

    #[cfg(feature = "cluster")]
    async fn catalog_test_db(
        object_store: Arc<dyn object_store::ObjectStore>,
    ) -> (
        Arc<LaminarDB>,
        Arc<laminar_core::cluster::control::CatalogManifestStore>,
    ) {
        use laminar_core::cluster::control::{
            CatalogManifestStore, ClusterController, ClusterKv, InMemoryKv, LeaderLeaseOwner,
            LeaderLeaseStore, LeaseDeadline, LeaseOutcome,
        };
        use laminar_core::cluster::discovery::NodeId;

        let node = NodeId(1);
        let boot = uuid::Uuid::from_u128(101);
        let owner = LeaderLeaseOwner {
            node,
            boot,
            process_term: 1,
        };
        let authority = Arc::new(LeaderLeaseStore::new(Arc::clone(&object_store), 1_000));
        let LeaseOutcome::Acquired(lease) = authority.try_acquire(&owner, 0).await.unwrap() else {
            unreachable!()
        };
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            node,
            Arc::clone(&kv),
            kv,
            None,
            members_rx,
            boot,
        ));
        controller.set_active(false);
        controller.set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
            std::time::Duration::from_secs(30),
        )));
        let (_lease_tx, lease_rx) = tokio::sync::watch::channel(Some(lease));
        controller
            .set_leader_lease_watch(
                lease_rx,
                owner,
                Arc::new(LeaseDeadline::live_for(std::time::Duration::from_secs(30))),
            )
            .unwrap();
        controller.set_leader_lease_store(Arc::clone(&authority));
        let manifest_store = Arc::new(CatalogManifestStore::new(authority));
        let db = LaminarDB::builder()
            .cluster_controller(controller)
            .cluster_checkpoint_object_store(object_store)
            .catalog_manifest_store(Arc::clone(&manifest_store))
            .build()
            .await
            .unwrap();
        (db, manifest_store)
    }

    #[test]
    fn test_source_to_ddl_basic() {
        let source = make_source("events", "kafka");
        let ddl = source_to_ddl(&source);
        assert!(ddl.starts_with("CREATE SOURCE events"));
        assert!(ddl.contains("id BIGINT NOT NULL"));
        assert!(ddl.contains("name VARCHAR"));
        assert!(ddl.contains("FROM KAFKA"));
        assert!(ddl.contains("format = 'json'"));
    }

    /// Columnless OTel source + WATERMARK FOR must compose: the OTel
    /// connector implements `discover_schema` so the DDL layer can
    /// resolve columns before validating the watermark.
    #[cfg(feature = "otel")]
    #[tokio::test]
    async fn execute_config_ddl_columnless_otel_with_watermark_succeeds() {
        let mut source = SourceConfig {
            name: "otel_events".to_string(),
            connector: "otel".to_string(),
            format: "json".to_string(),
            properties: toml::Table::new(),
            schema: vec![],
            watermark: Some(WatermarkConfig {
                column: "_laminar_received_at".to_string(),
                max_out_of_orderness: std::time::Duration::from_secs(10),
            }),
        };
        // Bind to an ephemeral port so the test doesn't clash with 4317.
        source
            .properties
            .insert("port".to_string(), toml::Value::String("0".to_string()));
        source.properties.insert(
            "signals".to_string(),
            toml::Value::String("logs".to_string()),
        );

        let db = laminar_db::LaminarDB::open().unwrap();
        let config = ServerConfig {
            server: ServerSection::default(),
            state: StateBackendConfig::default(),
            checkpoint: CheckpointSection::default(),
            supervision: Default::default(),
            sources: vec![source],
            lookups: vec![],
            pipelines: vec![],
            sinks: vec![],
            sql: None,
            discovery: None,
            node_id: None,
            ai: Default::default(),
            models: Default::default(),
        };
        execute_config_ddl(&db, &config, false)
            .await
            .expect("columnless OTel + WATERMARK FOR should compose");
    }

    /// Columnless Kafka source + WATERMARK FOR: the Kafka connector can't
    /// discover a schema without `bootstrap.servers` configured, so the DDL
    /// layer surfaces a "schema auto-discovery failed: …" error (or, when
    /// the connector returns no schema, "could not auto-discover a schema").
    /// The server no longer pre-empts this — we just check the error bubbles
    /// up clearly. Requires the kafka connector to be registered.
    #[cfg(feature = "kafka")]
    #[tokio::test]
    async fn execute_config_ddl_columnless_kafka_surfaces_discovery_error() {
        let mut source = make_source("events", "kafka");
        source.schema.clear();
        source.watermark = Some(WatermarkConfig {
            column: "ts".to_string(),
            max_out_of_orderness: std::time::Duration::from_secs(5),
        });

        let db = laminar_db::LaminarDB::open().unwrap();
        let config = ServerConfig {
            server: ServerSection::default(),
            state: StateBackendConfig::default(),
            checkpoint: CheckpointSection::default(),
            supervision: Default::default(),
            sources: vec![source],
            lookups: vec![],
            pipelines: vec![],
            sinks: vec![],
            sql: None,
            discovery: None,
            node_id: None,
            ai: Default::default(),
            models: Default::default(),
        };
        let err = execute_config_ddl(&db, &config, false).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("schema auto-discovery failed")
                || msg.contains("could not auto-discover a schema")
                || msg.contains("no columns declared"),
            "expected schema-discovery error from the DDL layer, got: {msg}"
        );
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn cluster_config_rejects_expanded_connector_secret_before_manifest_write() {
        use object_store::ObjectStore;

        let mut source = make_source("secured", "generator");
        source.properties.insert(
            "password".to_string(),
            toml::Value::String("expanded-password-must-not-persist".to_string()),
        );
        let config = ServerConfig {
            server: ServerSection::default(),
            state: StateBackendConfig::default(),
            checkpoint: CheckpointSection::default(),
            supervision: Default::default(),
            sources: vec![source],
            lookups: vec![],
            pipelines: vec![],
            sinks: vec![],
            sql: None,
            discovery: None,
            node_id: None,
            ai: Default::default(),
            models: Default::default(),
        };
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let (db, manifest_store) = catalog_test_db(object_store).await;

        let error = execute_config_ddl(&db, &config, true).await.unwrap_err();
        assert!(error.to_string().contains("cannot persist secret property"));
        assert!(manifest_store.load().await.unwrap().is_none());
    }

    #[cfg(feature = "cluster")]
    #[tokio::test]
    async fn empty_cluster_config_still_seals_an_empty_inventory() {
        use object_store::ObjectStore;

        let config = ServerConfig {
            server: ServerSection::default(),
            state: StateBackendConfig::default(),
            checkpoint: CheckpointSection::default(),
            supervision: Default::default(),
            sources: vec![],
            lookups: vec![],
            pipelines: vec![],
            sinks: vec![],
            sql: None,
            discovery: None,
            node_id: None,
            ai: Default::default(),
            models: Default::default(),
        };
        let object_store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let (db, manifest_store) = catalog_test_db(object_store).await;

        execute_config_ddl(&db, &config, true).await.unwrap();
        assert_eq!(
            manifest_store.load().await.unwrap().unwrap().entries,
            Vec::new()
        );
    }

    #[test]
    fn test_source_to_ddl_with_watermark() {
        let mut source = make_source("events", "kafka");
        source.watermark = Some(WatermarkConfig {
            column: "ts".to_string(),
            max_out_of_orderness: std::time::Duration::from_secs(5),
        });
        let ddl = source_to_ddl(&source);
        assert!(ddl.contains("WATERMARK FOR ts AS ts - INTERVAL '5' SECOND"));
    }

    #[test]
    fn connector_identifiers_preserve_provider_punctuation() {
        let hyphenated = source_to_ddl(&make_source("events", "postgres-cdc"));
        assert!(hyphenated.contains("FROM \"postgres-cdc\""));

        let underscored = source_to_ddl(&make_source("events", "vendor_v2"));
        assert!(underscored.contains("FROM VENDOR_V2"));
    }

    #[test]
    fn test_source_to_ddl_with_properties() {
        let mut source = make_source("events", "kafka");
        source.properties.insert(
            "bootstrap.servers".to_string(),
            toml::Value::String("localhost:9092".to_string()),
        );
        source.properties.insert(
            "topic".to_string(),
            toml::Value::String("events".to_string()),
        );
        let ddl = source_to_ddl(&source);
        assert!(ddl.contains("\"bootstrap.servers\" = 'localhost:9092'"));
        assert!(ddl.contains("topic = 'events'"));
    }

    #[test]
    fn test_pipeline_to_ddl() {
        let pipeline = PipelineConfig {
            name: "vwap".to_string(),
            sql: "SELECT symbol, SUM(price) FROM trades GROUP BY symbol".to_string(),
        };
        let ddl = pipeline_to_ddl(&pipeline);
        assert_eq!(
            ddl,
            "CREATE STREAM vwap AS SELECT symbol, SUM(price) FROM trades GROUP BY symbol"
        );
    }

    #[test]
    fn test_sink_to_ddl() {
        let mut props = toml::Table::new();
        props.insert(
            "topic".to_string(),
            toml::Value::String("output".to_string()),
        );
        props.insert(
            "bootstrap.servers".to_string(),
            toml::Value::String("localhost:9092".to_string()),
        );
        let sink = SinkConfig {
            name: "output_sink".to_string(),
            pipeline: "vwap".to_string(),
            connector: "kafka".to_string(),
            properties: props,
        };
        let ddl = sink_to_ddl(&sink);
        assert!(ddl.starts_with("CREATE SINK output_sink FROM vwap INTO KAFKA"));
        assert!(ddl.contains("topic = 'output'"));
        assert!(ddl.contains("\"bootstrap.servers\" = 'localhost:9092'"));
        // Delivery is injected from the pipeline-wide engine contract at connector build time.
        assert!(!ddl.contains("delivery"));
    }

    #[test]
    fn test_sink_to_ddl_has_no_per_sink_delivery_dimension() {
        let sink = SinkConfig {
            name: "out".to_string(),
            pipeline: "p".to_string(),
            connector: "kafka".to_string(),
            properties: toml::Table::new(),
        };
        let ddl = sink_to_ddl(&sink);
        assert!(!ddl.contains("delivery"));
    }

    #[test]
    fn test_lookup_to_ddl() {
        let lookup = LookupConfig {
            name: "instruments".to_string(),
            connector: "postgres".to_string(),
            strategy: "poll".to_string(),
            cache: LookupCacheConfig::default(),
            properties: {
                let mut t = toml::Table::new();
                t.insert(
                    "connection".to_string(),
                    toml::Value::String("postgresql://localhost/db".to_string()),
                );
                t
            },
            primary_key: vec!["symbol".to_string()],
            schema: vec![ColumnDef {
                name: "symbol".to_string(),
                data_type: "VARCHAR".to_string(),
                nullable: false,
            }],
        };
        let ddl = lookup_to_ddl(&lookup).unwrap();
        assert!(ddl.starts_with("CREATE LOOKUP TABLE instruments"));
        assert!(ddl.contains("symbol VARCHAR NOT NULL"));
        assert!(ddl.contains("PRIMARY KEY (symbol)"));
        assert!(ddl.contains("'connector' = 'postgres'"));
        assert!(ddl.contains("'strategy' = 'poll'"));
        assert!(ddl.contains("'connection' = 'postgresql://localhost/db'"));
    }

    #[test]
    fn test_lookup_to_ddl_no_primary_key() {
        let lookup = LookupConfig {
            name: "t".to_string(),
            connector: "postgres".to_string(),
            strategy: "poll".to_string(),
            cache: LookupCacheConfig::default(),
            properties: toml::Table::new(),
            primary_key: vec![],
            schema: vec![ColumnDef {
                name: "id".to_string(),
                data_type: "INT".to_string(),
                nullable: false,
            }],
        };
        let ddl = lookup_to_ddl(&lookup).unwrap();
        assert!(!ddl.contains("PRIMARY KEY"));
    }

    #[test]
    fn test_lookup_to_ddl_empty_schema_rejected() {
        let lookup = LookupConfig {
            name: "bad".to_string(),
            connector: "postgres".to_string(),
            strategy: "poll".to_string(),
            cache: LookupCacheConfig::default(),
            properties: toml::Table::new(),
            primary_key: vec![],
            schema: vec![],
        };
        assert!(lookup_to_ddl(&lookup).is_err());
    }

    #[test]
    fn test_toml_value_to_sql() {
        assert_eq!(
            toml_value_to_sql(&toml::Value::String("hello".to_string())),
            "hello"
        );
        assert_eq!(toml_value_to_sql(&toml::Value::Integer(42)), "42");
        assert_eq!(toml_value_to_sql(&toml::Value::Boolean(true)), "true");
        assert_eq!(toml_value_to_sql(&toml::Value::Float(3.25)), "3.25");
    }

    #[test]
    fn test_toml_value_to_sql_escapes_single_quotes() {
        assert_eq!(
            toml_value_to_sql(&toml::Value::String("it's a test".to_string())),
            "it''s a test"
        );
        assert_eq!(
            toml_value_to_sql(&toml::Value::String("a''b".to_string())),
            "a''''b"
        );
    }
}

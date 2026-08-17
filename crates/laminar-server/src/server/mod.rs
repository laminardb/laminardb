//! Engine construction and lifecycle for LaminarDB server.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::signal;
use tracing::{info, warn};

use laminar_core::streaming::checkpoint::StreamCheckpointConfig;
use laminar_db::{DbError, EngineMetrics, LaminarDB};

#[cfg(feature = "cluster")]
use crate::cluster_config::{ClusterConfig, ClusterConfigError};
#[cfg(not(feature = "cluster"))]
use crate::config::ServerMode;
use crate::config::{
    ConfigError, LookupConfig, PipelineConfig, ServerConfig, SinkConfig, SourceConfig,
};
use crate::http;
#[cfg(feature = "cluster")]
use crate::http::ClusterComponents;
use crate::metrics::ServerMetrics;
use crate::reload::ReloadGuard;

/// Handle to a running LaminarDB server. Call `wait_for_shutdown` to block until Ctrl-C.
pub struct ServerHandle {
    runtime: ServerRuntime,
}

enum ServerRuntime {
    Single(SingleServerRuntime),
    #[cfg(feature = "cluster")]
    Cluster(Box<crate::cluster::ClusterHandle>),
}

struct SingleServerRuntime {
    db: Arc<LaminarDB>,
    db_shutdown_complete: bool,
    serving_gate: Arc<http::ServingGate>,
    api_handle: tokio::task::JoinHandle<()>,
    pgwire_handle: Option<tokio::task::JoinHandle<()>>,
    watcher_handle: Option<tokio::task::JoinHandle<()>>,
}

const SERVER_TASK_SHUTDOWN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

async fn abort_and_join_server_task<T>(
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
    async fn wait_for_shutdown(&mut self) -> Result<(), ServerError> {
        wait_for_termination_signal().await?;

        info!("Received shutdown signal, shutting down...");
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
        shutdown_result.map_err(|error| ServerError::Shutdown(error.to_string()))?;
        if !(watcher_stopped && pgwire_stopped && api_stopped) {
            return Err(ServerError::Shutdown(
                "one or more server tasks did not terminate cleanly".into(),
            ));
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

impl ServerHandle {
    /// Block until SIGINT/SIGTERM, then gracefully shut down.
    pub async fn wait_for_shutdown(self) -> Result<(), ServerError> {
        match self.runtime {
            ServerRuntime::Single(mut runtime) => runtime.wait_for_shutdown().await,
            #[cfg(feature = "cluster")]
            ServerRuntime::Cluster(handle) => (*handle)
                .wait_for_shutdown()
                .await
                .map_err(|e| ServerError::Cluster(e.to_string())),
        }
    }
}

pub(crate) async fn wait_for_termination_signal() -> Result<(), ServerError> {
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
    // Validate independently of config-file loading: cluster startup acquires durable leases and
    // starts discovery before constructing LaminarDB, and programmatic callers can bypass the
    // TOML validator entirely.
    crate::config::validate_http_auth(&config)
        .map_err(|error| ServerError::Build(format!("HTTP authentication: {error}")))?;
    let temporal_join_idle_history_retention = config
        .server
        .validated_temporal_join_idle_history_retention()
        .map_err(|error| ServerError::Build(format!("server.{error}")))?;
    let source_idle_timeout = config
        .server
        .validated_source_idle_timeout()
        .map_err(|error| ServerError::Build(format!("server.{error}")))?;
    let event_time_max_future_skew = config
        .server
        .validated_event_time_max_future_skew()
        .map_err(|error| ServerError::Build(format!("server.{error}")))?;
    resolved_checkpoint_node_data_bytes(&config.checkpoint)
        .map_err(|error| ServerError::Build(format!("checkpoint.max_node_data_bytes: {error}")))?;

    // Cluster mode: gated behind the `cluster` feature flag.
    #[cfg(feature = "cluster")]
    {
        let cluster_cfg = ClusterConfig::from_server_config(&config)?;

        if let Some(cluster_cfg) = cluster_cfg {
            let handle = crate::cluster::start_cluster(config, cluster_cfg, config_path)
                .await
                .map_err(|e| ServerError::Cluster(e.to_string()))?;
            return Ok(ServerHandle {
                runtime: ServerRuntime::Cluster(Box::new(handle)),
            });
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
    builder = builder.restart_policy(config.supervision.to_policy());
    builder = builder.incremental_emit(config.server.incremental_emit);
    if let Some(retention) = temporal_join_idle_history_retention {
        builder = builder.temporal_join_idle_history_retention(retention);
    }
    if let Some(timeout) = source_idle_timeout {
        builder = builder.source_idle_timeout(timeout);
    }
    builder = builder.event_time_max_future_skew(event_time_max_future_skew);
    builder = apply_local_checkpoint_config(builder, &config.checkpoint.url, &config.checkpoint)
        .map_err(|error| ServerError::Build(format!("checkpoint storage: {error}")))?;

    let key_groups = config.server.resolved_key_groups();
    let vnode_registry = Arc::new(laminar_core::state::VnodeRegistry::single_owner(
        u32::from(key_groups),
        laminar_core::state::LOCAL_NODE_ID,
    ));
    builder = builder.vnode_registry(vnode_registry);

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
    let http_runtime = start_http_api(
        Arc::clone(&db),
        registry,
        config_path.clone(),
        config,
        // Single-node server mode has no cluster control plane.
        #[cfg(feature = "cluster")]
        None,
    )
    .await;
    let (app_state, mut api_handle) = match http_runtime {
        Ok(runtime) => runtime,
        Err(error) => {
            let _ = db.shutdown().await;
            return Err(error);
        }
    };
    let mut watcher_handle = spawn_config_watcher(&app_state, config_path);

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
                tokio::join!(
                    async {
                        if let Some(handle) = watcher_handle.as_mut() {
                            abort_and_join_server_task(handle, "configuration watcher").await;
                        }
                    },
                    abort_and_join_server_task(&mut api_handle, "HTTP API server"),
                );
                let _ = db.shutdown().await;
                return Err(e);
            }
        }
    } else {
        None
    };

    Ok(ServerHandle {
        runtime: ServerRuntime::Single(SingleServerRuntime {
            db,
            db_shutdown_complete: false,
            serving_gate: Arc::clone(&app_state.serving_gate),
            api_handle,
            pgwire_handle,
            watcher_handle,
        }),
    })
}

// ---------------------------------------------------------------------------
// Shared helpers (used by both single-node and cluster startup)
// ---------------------------------------------------------------------------

/// Invalid checkpoint storage or state-budget configuration.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CheckpointConfigurationError {
    #[error(transparent)]
    Storage(#[from] laminar_core::checkpoint::object_store_builder::ObjectStoreBuilderError),
    #[error("checkpoint.max_node_data_bytes: {0}")]
    NodeDataBudget(laminar_core::checkpoint::CheckpointStoreError),
}

/// Apply local-runtime checkpoint settings to a `LaminarDB` builder.
pub(crate) fn apply_local_checkpoint_config(
    mut builder: laminar_db::LaminarDbBuilder,
    checkpoint_url: &str,
    checkpoint: &crate::config::CheckpointSection,
) -> Result<laminar_db::LaminarDbBuilder, CheckpointConfigurationError> {
    if checkpoint_url.starts_with("file://") {
        laminar_core::checkpoint::object_store_builder::file_url_path(checkpoint_url)?;
    }
    builder = apply_checkpoint_settings(builder, checkpoint)?;
    builder = builder.object_store_url(checkpoint_url.to_string());
    if !checkpoint.storage.is_empty() {
        builder = builder.object_store_options(checkpoint.storage.clone());
    }

    Ok(builder)
}

#[cfg(feature = "cluster")]
pub(crate) fn apply_verified_cluster_checkpoint_config(
    builder: laminar_db::LaminarDbBuilder,
    checkpoint: &crate::config::CheckpointSection,
    namespaces: laminar_core::cluster::control::VerifiedClusterNamespaces,
) -> Result<laminar_db::LaminarDbBuilder, CheckpointConfigurationError> {
    Ok(apply_checkpoint_settings(builder, checkpoint)?.verified_cluster_namespaces(namespaces))
}

fn apply_checkpoint_settings(
    builder: laminar_db::LaminarDbBuilder,
    checkpoint: &crate::config::CheckpointSection,
) -> Result<laminar_db::LaminarDbBuilder, CheckpointConfigurationError> {
    let max_node_data_bytes = resolved_checkpoint_node_data_bytes(checkpoint)
        .map_err(CheckpointConfigurationError::NodeDataBudget)?;
    Ok(builder.checkpoint(StreamCheckpointConfig {
        interval_ms: Some(u64::try_from(checkpoint.interval.as_millis()).unwrap_or(u64::MAX)),
        timeout_ms: Some(u64::try_from(checkpoint.timeout.as_millis()).unwrap_or(u64::MAX)),
        data_dir: None,
        max_node_data_bytes: Some(max_node_data_bytes),
    }))
}

/// Resolve the one capture and recovery admission budget used by every server checkpoint store.
pub(crate) fn resolved_checkpoint_node_data_bytes(
    checkpoint: &crate::config::CheckpointSection,
) -> Result<u64, laminar_core::checkpoint::CheckpointStoreError> {
    let max_node_data_bytes = checkpoint.max_node_data_bytes.unwrap_or(
        laminar_core::checkpoint::checkpoint_store::DEFAULT_MAX_CHECKPOINT_NODE_DATA_BYTES,
    );
    laminar_core::checkpoint::checkpoint_store::validate_max_checkpoint_node_data_bytes(
        max_node_data_bytes,
    )?;
    Ok(max_node_data_bytes)
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
    let serving_gate = Arc::new(http::ServingGate::starting());
    #[cfg(feature = "cluster")]
    if let Some(cluster) = cluster.as_ref() {
        let deadline = cluster.controller.process_lease_deadline().ok_or_else(|| {
            ServerError::Http(
                "cluster HTTP serving requires the shared process lease deadline".into(),
            )
        })?;
        serving_gate
            .install_process_lease_deadline(deadline)
            .map_err(|error| ServerError::Http(error.into()))?;
    }
    let prepared = prepare_http_api(
        db,
        registry,
        config_path,
        config,
        serving_gate,
        #[cfg(feature = "cluster")]
        cluster,
    )
    .await?;
    if !prepared.app_state.open_startup_gate() {
        return Err(ServerError::Http(
            "HTTP serving authority was fenced during startup".into(),
        ));
    }
    prepared.start().await
}

pub(crate) struct PreparedHttpApi {
    app_state: Arc<http::AppState>,
    router: axum::Router,
    listener: tokio::net::TcpListener,
    bind: String,
}

struct StartingHttpServer {
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl StartingHttpServer {
    fn new(handle: tokio::task::JoinHandle<()>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    fn into_handle(mut self) -> tokio::task::JoinHandle<()> {
        self.handle
            .take()
            .expect("starting HTTP server handle is present until disarmed")
    }
}

impl Drop for StartingHttpServer {
    fn drop(&mut self) {
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

impl PreparedHttpApi {
    pub(crate) async fn start(
        self,
    ) -> Result<(Arc<http::AppState>, tokio::task::JoinHandle<()>), ServerError> {
        let (handle, started) = http::serve_listener(self.router, self.listener);
        let starting = StartingHttpServer::new(handle);
        if started.await.is_err() {
            let handle = starting.into_handle();
            handle.abort();
            let _ = handle.await;
            return Err(ServerError::Http(
                "HTTP serve task stopped before entering its accept loop".into(),
            ));
        }
        let handle = starting.into_handle();
        info!("HTTP API listening on {}", self.bind);
        Ok((self.app_state, handle))
    }
}

pub(crate) async fn prepare_http_api(
    db: Arc<LaminarDB>,
    registry: Arc<prometheus::Registry>,
    config_path: PathBuf,
    config: ServerConfig,
    serving_gate: Arc<http::ServingGate>,
    #[cfg(feature = "cluster")] cluster: Option<ClusterComponents>,
) -> Result<PreparedHttpApi, ServerError> {
    let bind = config.server.bind.clone();
    let auth_policy = http::HttpAuthPolicy::from_server(&config.server);

    let server_metrics = ServerMetrics::new(&registry);

    let app_state = Arc::new(http::AppState {
        db,
        config_path,
        current_config: parking_lot::RwLock::new(config),
        reload_guard: ReloadGuard::new(),
        registry,
        server_metrics,
        auth_policy,
        #[cfg(feature = "cluster")]
        diagnostic_reads: http::DiagnosticReadGate::new(),
        ws_slots: http::ws_connection_slots(),
        serving_gate,
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

fn connector_option_key_sql(key: &str) -> String {
    format!("\"{}\"", key.replace('"', "\"\""))
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
    if !source.primary_key.is_empty() {
        col_defs.push(format!("PRIMARY KEY ({})", source.primary_key.join(", ")));
    }

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
    let opts: Vec<String> = source
        .properties
        .iter()
        .map(|(key, value)| {
            format!(
                "{} = '{}'",
                connector_option_key_sql(key),
                toml_value_to_sql(value)
            )
        })
        .collect();
    if opts.is_empty() {
        parts.push(format!("FROM {connector_keyword}"));
    } else {
        parts.push(format!("FROM {} ({})", connector_keyword, opts.join(", ")));
    }
    parts.push(format!(
        "FORMAT {}",
        connector_sql_identifier(&source.format)
    ));

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
            format!(
                "{} = '{}'",
                connector_option_key_sql(key),
                toml_value_to_sql(value)
            )
        })
        .collect();

    let mut ddl = if opts.is_empty() {
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
    };
    if let Some(format) = &sink.format {
        ddl.push_str(" FORMAT ");
        ddl.push_str(&connector_sql_identifier(format));
    }
    ddl
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
mod tests;

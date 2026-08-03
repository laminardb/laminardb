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
    resolved_checkpoint_state_bytes(&config.checkpoint)
        .map_err(|error| ServerError::Build(format!("checkpoint.max_staged_bytes: {error}")))?;

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
    builder = apply_local_checkpoint_config(builder, &config.checkpoint.url, &config.checkpoint)
        .map_err(|error| ServerError::Build(format!("checkpoint storage: {error}")))?;

    let key_groups = config.server.resolved_key_groups();
    let state_backend = checkpoint_state_backend(&config.checkpoint, key_groups)
        .await
        .map_err(|error| ServerError::Build(format!("checkpoint storage: {error}")))?;
    let vnode_registry = Arc::new(laminar_core::state::VnodeRegistry::single_owner(
        u32::from(key_groups),
        laminar_core::state::LOCAL_NODE_ID,
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
    Storage(#[from] laminar_core::storage::object_store_builder::ObjectStoreBuilderError),
    #[error("checkpoint.max_staged_bytes: {0}")]
    StateBudget(laminar_core::storage::checkpoint_store::CheckpointStoreError),
    #[error("checkpoint store contract: {0}")]
    StoreContract(laminar_core::storage::checkpoint_store::CheckpointStoreError),
    #[error("checkpoint URL is not durable object storage ({0:?})")]
    Durability(laminar_core::state::StateBackendDurability),
}

/// Apply local-runtime checkpoint settings to a `LaminarDB` builder.
pub(crate) fn apply_local_checkpoint_config(
    mut builder: laminar_db::LaminarDbBuilder,
    checkpoint_url: &str,
    checkpoint: &crate::config::CheckpointSection,
) -> Result<laminar_db::LaminarDbBuilder, CheckpointConfigurationError> {
    if checkpoint_url.starts_with("file://") {
        laminar_core::storage::object_store_builder::file_url_path(checkpoint_url)?;
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
    let max_state_data_bytes = resolved_checkpoint_state_bytes(checkpoint)
        .map_err(CheckpointConfigurationError::StateBudget)?;
    Ok(builder.checkpoint(StreamCheckpointConfig {
        interval_ms: Some(u64::try_from(checkpoint.interval.as_millis()).unwrap_or(u64::MAX)),
        timeout_ms: Some(u64::try_from(checkpoint.timeout.as_millis()).unwrap_or(u64::MAX)),
        data_dir: None,
        max_retained: Some(checkpoint.max_retained),
        max_staged_bytes: Some(max_state_data_bytes),
    }))
}

async fn checkpoint_state_backend(
    checkpoint: &crate::config::CheckpointSection,
    key_groups: laminar_core::state::KeyGroupCount,
) -> Result<Arc<dyn laminar_core::state::StateBackend>, CheckpointConfigurationError> {
    let store = laminar_core::storage::object_store_builder::build_object_store(
        &checkpoint.url,
        &checkpoint.storage,
    )?;
    let durability = laminar_core::state::StateBackendDurability::for_storage_url(&checkpoint.url);
    let probe_timeout = std::time::Duration::from_secs(5);
    match durability {
        laminar_core::state::StateBackendDurability::NodeDurable => {
            laminar_core::storage::checkpoint_store::probe_object_store_conditional_create(
                store.as_ref(),
                "startup/",
                probe_timeout,
            )
            .await
        }
        laminar_core::state::StateBackendDurability::ClusterShared => {
            laminar_core::storage::checkpoint_store::probe_object_store_conditional_update(
                store.as_ref(),
                "startup/",
                probe_timeout,
            )
            .await
        }
        durability => return Err(CheckpointConfigurationError::Durability(durability)),
    }
    .map_err(CheckpointConfigurationError::StoreContract)?;
    let vnode_capacity = u32::from(key_groups);
    let backend = match durability {
        laminar_core::state::StateBackendDurability::NodeDurable => {
            laminar_core::state::ObjectStoreBackend::node_durable(store, "local", vnode_capacity)
        }
        laminar_core::state::StateBackendDurability::ClusterShared => {
            laminar_core::state::ObjectStoreBackend::cluster_shared(store, "local", vnode_capacity)
        }
        durability => return Err(CheckpointConfigurationError::Durability(durability)),
    };
    Ok(Arc::new(backend))
}

/// Resolve the one capture and recovery admission budget used by every server checkpoint store.
pub(crate) fn resolved_checkpoint_state_bytes(
    checkpoint: &crate::config::CheckpointSection,
) -> Result<u64, laminar_core::storage::checkpoint_store::CheckpointStoreError> {
    let max_state_data_bytes = checkpoint
        .max_staged_bytes
        .unwrap_or(laminar_core::checkpoint::checkpoint_store::DEFAULT_MAX_CHECKPOINT_STATE_BYTES);
    laminar_core::storage::checkpoint_store::validate_max_checkpoint_state_bytes(
        max_state_data_bytes,
    )?;
    Ok(max_state_data_bytes)
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
mod tests {
    use super::*;

    use crate::config::*;

    #[test]
    fn checkpoint_config_rejects_relative_file_urls() {
        let result = apply_local_checkpoint_config(
            LaminarDB::builder(),
            "file://./relative",
            &CheckpointSection::default(),
        );
        let Err(error) = result else {
            panic!("relative checkpoint URL was admitted");
        };
        assert!(error.to_string().contains("remote host"), "{error}");
    }

    #[test]
    fn checkpoint_state_budget_has_one_default_and_honours_an_override() {
        let mut checkpoint = CheckpointSection::default();
        assert_eq!(
            resolved_checkpoint_state_bytes(&checkpoint).unwrap(),
            laminar_core::checkpoint::checkpoint_store::DEFAULT_MAX_CHECKPOINT_STATE_BYTES
        );

        checkpoint.max_staged_bytes = Some(8 * 1024 * 1024);
        assert_eq!(
            resolved_checkpoint_state_bytes(&checkpoint).unwrap(),
            8 * 1024 * 1024
        );
    }

    #[test]
    fn checkpoint_state_budget_rejects_zero_and_unaddressable_limits() {
        let mut checkpoint = CheckpointSection {
            max_staged_bytes: Some(0),
            ..CheckpointSection::default()
        };
        assert!(resolved_checkpoint_state_bytes(&checkpoint).is_err());

        checkpoint.max_staged_bytes = Some((isize::MAX as u64) + 1);
        let error = resolved_checkpoint_state_bytes(&checkpoint).unwrap_err();
        assert!(error
            .to_string()
            .contains("exceeds this process address space"));
    }

    #[tokio::test]
    async fn server_entry_rejects_invalid_budget_before_runtime_mode_routing() {
        for mode in [ServerMode::Single, ServerMode::Cluster] {
            let mut config: ServerConfig = toml::from_str("").unwrap();
            config.server.mode = mode;
            config.checkpoint.max_staged_bytes = Some(0);

            let result = run_server(config, PathBuf::from("unused.toml")).await;
            let Err(error) = result else {
                panic!("invalid checkpoint state budget was admitted in {mode:?} mode");
            };
            assert!(
                error.to_string().contains("checkpoint.max_staged_bytes"),
                "{error}"
            );
        }
    }

    #[tokio::test]
    async fn server_entry_rejects_programmatic_diagnostic_auth_before_other_startup_work() {
        let mut config: ServerConfig = toml::from_str("").unwrap();
        config.server.diagnostic_read_token = Some(Secret::new("invalid"));
        // This second invalid value makes the test terminate safely even if authentication
        // validation is accidentally moved later; authentication must still win.
        config.checkpoint.max_staged_bytes = Some(0);

        let result = run_server(config, PathBuf::from("unused.toml")).await;
        let Err(error) = result else {
            panic!("invalid programmatic diagnostic authentication was admitted");
        };
        let message = error.to_string();
        assert!(message.contains("HTTP authentication"), "{message}");
        assert!(message.contains("diagnostic_read_token"), "{message}");
        assert!(
            !message.contains("checkpoint.max_staged_bytes"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn cancelling_http_start_does_not_detach_the_listener() {
        let server = ServerSection {
            bind: "127.0.0.1:0".into(),
            ..ServerSection::default()
        };
        let config = ServerConfig {
            server,
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
        let registry = Arc::new(crate::metrics::build_registry([
            ("instance".into(), "test".into()),
            ("pipeline".into(), "test".into()),
        ]));
        let prepared = prepare_http_api(
            LaminarDB::open().unwrap(),
            registry,
            PathBuf::from("unused.toml"),
            config,
            Arc::new(http::ServingGate::starting()),
            #[cfg(feature = "cluster")]
            None,
        )
        .await
        .unwrap();
        let address = prepared.listener.local_addr().unwrap();

        {
            let start = prepared.start();
            tokio::pin!(start);
            assert!(futures::poll!(start.as_mut()).is_pending());
        }

        let rebound = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if let Ok(listener) = tokio::net::TcpListener::bind(address).await {
                    return listener;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelling HTTP startup must release its listener");
        drop(rebound);
    }

    #[tokio::test]
    async fn aborted_server_task_is_joined_before_cleanup_returns() {
        let mut task = tokio::spawn(std::future::pending::<()>());
        let observer = task.abort_handle();

        assert!(abort_and_join_server_task(&mut task, "test task").await);

        assert!(observer.is_finished());
    }

    #[tokio::test]
    async fn dropping_single_server_handle_fences_and_aborts_owned_tasks() {
        let serving_gate = Arc::new(http::ServingGate::starting());
        assert!(serving_gate.open());
        let api_handle = tokio::spawn(std::future::pending::<()>());
        let api_abort = api_handle.abort_handle();
        let pgwire_handle = tokio::spawn(std::future::pending::<()>());
        let pgwire_abort = pgwire_handle.abort_handle();
        let watcher_handle = tokio::spawn(std::future::pending::<()>());
        let watcher_abort = watcher_handle.abort_handle();
        let db = LaminarDB::open().unwrap();
        let handle = ServerHandle {
            runtime: ServerRuntime::Single(SingleServerRuntime {
                db: Arc::clone(&db),
                db_shutdown_complete: false,
                serving_gate: Arc::clone(&serving_gate),
                api_handle,
                pgwire_handle: Some(pgwire_handle),
                watcher_handle: Some(watcher_handle),
            }),
        };

        drop(handle);

        assert_eq!(
            serving_gate.rejection_message(),
            Some("server serving authority is fenced")
        );
        assert!(db.is_closed());
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !(api_abort.is_finished()
                && pgwire_abort.is_finished()
                && watcher_abort.is_finished())
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropped server handle left an owned task running");
        db.shutdown().await.unwrap();
    }

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
        let LeaseOutcome::Acquired(lease) = authority.begin_new_term(&owner, 0).await.unwrap()
        else {
            unreachable!()
        };
        let kv: Arc<dyn ClusterKv> = Arc::new(InMemoryKv::new(node));
        let (_members_tx, members_rx) = tokio::sync::watch::channel(Vec::new());
        let controller = Arc::new(ClusterController::new_with_recovery_incarnation(
            node,
            Arc::clone(&kv),
            Arc::clone(&kv),
            None,
            members_rx,
            boot,
        ));
        controller.set_active(false);
        controller
            .set_process_lease_deadline(Arc::new(LeaseDeadline::live_for(
                std::time::Duration::from_secs(30),
            )))
            .unwrap();
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
        let participant = laminar_core::checkpoint::CheckpointParticipant {
            node_id: node.0,
            boot_incarnation: boot,
        };
        let verified_namespaces =
            laminar_core::cluster::control::prove_shared_object_store_namespaces(
                participant,
                &[participant],
                kv,
                Arc::clone(&object_store),
                Arc::clone(&object_store),
                std::time::Duration::from_secs(1),
            )
            .await
            .unwrap();
        let state_backend: Arc<dyn laminar_core::state::StateBackend> =
            Arc::new(laminar_core::state::ObjectStoreBackend::cluster_shared(
                verified_namespaces.state_store(),
                node.to_string(),
                1,
            ));
        let vnode_registry = Arc::new(laminar_core::state::VnodeRegistry::new(1));
        let db = LaminarDB::builder()
            .cluster_controller(controller)
            .verified_cluster_namespaces(verified_namespaces)
            .state_backend(state_backend)
            .vnode_registry(vnode_registry)
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
        assert!(ddl.contains("FROM KAFKA FORMAT JSON"));
        assert!(!ddl.contains("format ="));
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
        source.properties.insert(
            "client-id".to_string(),
            toml::Value::String("source-client".to_string()),
        );
        source.properties.insert(
            "vendor\"option".to_string(),
            toml::Value::String("quoted-key".to_string()),
        );
        let ddl = source_to_ddl(&source);
        assert!(ddl.contains("\"bootstrap.servers\" = 'localhost:9092'"));
        assert!(ddl.contains("\"topic\" = 'events'"));
        assert!(ddl.contains("\"client-id\" = 'source-client'"));
        assert!(ddl.contains("\"vendor\"\"option\" = 'quoted-key'"));
        assert!(ddl.ends_with(") FORMAT JSON"));

        let statements = laminar_sql::parser::parse_streaming_sql(&ddl).unwrap();
        let laminar_sql::parser::StreamingStatement::CreateSource(parsed) = &statements[0] else {
            panic!("expected CREATE SOURCE")
        };
        assert_eq!(
            parsed
                .connector_options
                .get("client-id")
                .map(String::as_str),
            Some("source-client")
        );
        assert_eq!(
            parsed
                .connector_options
                .get("vendor\"option")
                .map(String::as_str),
            Some("quoted-key")
        );
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
        props.insert(
            "oauthbearer-token".to_string(),
            toml::Value::String("token".to_string()),
        );
        let sink = SinkConfig {
            name: "output_sink".to_string(),
            pipeline: "vwap".to_string(),
            connector: "kafka".to_string(),
            format: Some("json".to_string()),
            properties: props,
        };
        let ddl = sink_to_ddl(&sink);
        assert!(ddl.starts_with("CREATE SINK output_sink FROM vwap INTO KAFKA"));
        assert!(ddl.contains("\"topic\" = 'output'"));
        assert!(ddl.contains("\"bootstrap.servers\" = 'localhost:9092'"));
        assert!(ddl.contains("\"oauthbearer-token\" = 'token'"));
        assert!(ddl.ends_with(") FORMAT JSON"));
        assert!(!ddl.contains("format ="));
        // Delivery is injected from the pipeline-wide engine contract at connector build time.
        assert!(!ddl.contains("delivery"));

        let statements = laminar_sql::parser::parse_streaming_sql(&ddl).unwrap();
        let laminar_sql::parser::StreamingStatement::CreateSink(parsed) = &statements[0] else {
            panic!("expected CREATE SINK")
        };
        assert_eq!(
            parsed
                .connector_options
                .get("oauthbearer-token")
                .map(String::as_str),
            Some("token")
        );
    }

    #[test]
    fn test_sink_to_ddl_has_no_per_sink_delivery_dimension() {
        let sink = SinkConfig {
            name: "out".to_string(),
            pipeline: "p".to_string(),
            connector: "kafka".to_string(),
            format: None,
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

//! Engine construction and lifecycle for LaminarDB server.

use std::path::PathBuf;
use std::sync::Arc;

use tokio::signal;
use tracing::info;

use laminar_core::streaming::checkpoint::StreamCheckpointConfig;
use laminar_db::{DbError, EngineMetrics, LaminarDB, Profile};

use crate::config::{
    ConfigError, LookupConfig, PipelineConfig, ServerConfig, SinkConfig, SourceConfig,
};
#[cfg(feature = "cluster-unstable")]
use crate::delta_config::{DeltaConfig, DeltaConfigError};
use crate::http;
use crate::metrics::ServerMetrics;
use crate::reload::ReloadGuard;

/// Handle to a running LaminarDB server. Call `wait_for_shutdown` to block until Ctrl-C.
pub enum ServerHandle {
    Embedded {
        db: Arc<LaminarDB>,
        api_handle: tokio::task::JoinHandle<()>,
        watcher_handle: Option<tokio::task::JoinHandle<()>>,
    },
    #[cfg(feature = "cluster-unstable")]
    Delta(Box<crate::delta::DeltaHandle>),
}

impl ServerHandle {
    /// Block until SIGINT/SIGTERM, then gracefully shut down.
    pub async fn wait_for_shutdown(self) -> Result<(), ServerError> {
        match self {
            Self::Embedded {
                db,
                api_handle,
                watcher_handle,
            } => {
                wait_for_termination_signal().await?;

                info!("Received shutdown signal, shutting down...");

                if let Some(wh) = &watcher_handle {
                    wh.abort();
                }
                db.shutdown()
                    .await
                    .map_err(|e| ServerError::Shutdown(e.to_string()))?;
                api_handle.abort();

                info!("Shutdown complete");
                Ok(())
            }
            #[cfg(feature = "cluster-unstable")]
            Self::Delta(handle) => (*handle)
                .wait_for_shutdown()
                .await
                .map_err(|e| ServerError::Delta(e.to_string())),
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
    // Cluster mode: gated behind the `cluster-unstable` feature flag.
    #[cfg(feature = "cluster-unstable")]
    {
        let delta_cfg = DeltaConfig::from_server_config(&config)?;

        if let Some(delta_cfg) = delta_cfg {
            let handle = crate::delta::start_delta(config, delta_cfg, config_path)
                .await
                .map_err(|e| ServerError::Delta(e.to_string()))?;
            return Ok(ServerHandle::Delta(Box::new(handle)));
        }
    }
    #[cfg(not(feature = "cluster-unstable"))]
    if config.server.mode == "delta" {
        return Err(ServerError::Delta(
            "Delta/cluster mode requires the 'cluster-unstable' feature flag. \
             This mode is not yet production-ready."
                .to_string(),
        ));
    }

    // Build LaminarDB via builder API
    let mut builder = LaminarDB::builder();

    let has_storage = config.state.backend != "memory";
    if has_storage {
        builder = builder.storage_dir(&config.state.path);
    }

    let profile = match config.server.mode.as_str() {
        "embedded" if has_storage => Profile::Embedded,
        "embedded" => Profile::BareMetal,
        "delta" => Profile::Delta,
        _ => Profile::BareMetal,
    };
    builder = builder.profile(profile);
    builder = apply_checkpoint_config(builder, &config.checkpoint.url, &config.checkpoint);

    let db = builder
        .build()
        .await
        .map_err(|e| ServerError::Build(e.to_string()))?;
    let db = Arc::new(db);

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
    db.set_prometheus_registry(Arc::clone(&registry));

    execute_config_ddl(&db, &config).await?;

    db.start()
        .await
        .map_err(|e| ServerError::Start(e.to_string()))?;
    info!("Pipeline started");

    let (app_state, api_handle) =
        start_http_api(Arc::clone(&db), registry, config_path.clone(), config).await?;
    let watcher_handle = spawn_config_watcher(&app_state, config_path);

    Ok(ServerHandle::Embedded {
        db,
        api_handle,
        watcher_handle,
    })
}

// ---------------------------------------------------------------------------
// Shared helpers (used by both embedded and delta startup)
// ---------------------------------------------------------------------------

/// Apply checkpoint settings to a `LaminarDB` builder.
pub(crate) fn apply_checkpoint_config(
    mut builder: laminar_db::LaminarDbBuilder,
    checkpoint_url: &str,
    checkpoint: &crate::config::CheckpointSection,
) -> laminar_db::LaminarDbBuilder {
    let cfg = StreamCheckpointConfig {
        interval_ms: Some(checkpoint.interval.as_millis() as u64),
        data_dir: file_url_to_path(checkpoint_url),
        max_retained: Some(checkpoint.max_retained),
    };
    builder = builder.checkpoint(cfg);

    if !checkpoint_url.starts_with("file:///") && !checkpoint_url.is_empty() {
        builder = builder.object_store_url(checkpoint_url.to_string());
        if !checkpoint.storage.is_empty() {
            builder = builder.object_store_options(checkpoint.storage.clone());
        }
    }

    if let Some(tiering) = &checkpoint.tiering {
        builder = builder.tiering(laminar_db::TieringConfig {
            hot_class: tiering.hot_class.clone(),
            warm_class: tiering.warm_class.clone(),
            cold_class: tiering.cold_class.clone(),
            hot_retention_secs: tiering.hot_retention.as_secs(),
            warm_retention_secs: tiering.warm_retention.as_secs(),
        });
    }

    builder
}

/// Execute DDL for all config sections (sources, lookups, pipelines, sinks, raw SQL).
pub(crate) async fn execute_config_ddl(
    db: &LaminarDB,
    config: &ServerConfig,
) -> Result<(), ServerError> {
    // Empty-schema + WATERMARK FOR is handled inside the laminar-db DDL
    // layer: it calls `discover_schema` on the connector, populates the
    // source columns from the result, then validates the watermark column
    // against them. Connectors that cannot discover a schema (e.g. Kafka
    // without a reachable Schema Registry) return a clear error from that
    // path — we don't need to pre-empt it here.
    for source in &config.sources {
        let ddl = source_to_ddl(source);
        db.execute(&ddl).await.map_err(|e| ServerError::Ddl {
            section: "source".to_string(),
            name: source.name.clone(),
            source: e,
        })?;
        info!("Created source: {}", source.name);
    }

    for lookup in &config.lookups {
        let ddl = lookup_to_ddl(lookup)?;
        db.execute(&ddl).await.map_err(|e| ServerError::Ddl {
            section: "lookup".to_string(),
            name: lookup.name.clone(),
            source: e,
        })?;
        info!("Created lookup table: {}", lookup.name);
    }

    for pipeline in &config.pipelines {
        let ddl = pipeline_to_ddl(pipeline);
        db.execute(&ddl).await.map_err(|e| ServerError::Ddl {
            section: "pipeline".to_string(),
            name: pipeline.name.clone(),
            source: e,
        })?;
        info!("Created pipeline: {}", pipeline.name);
    }

    for sink in &config.sinks {
        let ddl = sink_to_ddl(sink);
        db.execute(&ddl).await.map_err(|e| ServerError::Ddl {
            section: "sink".to_string(),
            name: sink.name.clone(),
            source: e,
        })?;
        info!("Created sink: {}", sink.name);
    }

    if let Some(ref sql) = config.sql {
        let trimmed = sql.trim();
        if !trimmed.is_empty() {
            db.execute(trimmed).await.map_err(|e| {
                let snippet: String = trimmed.chars().take(80).collect();
                ServerError::Ddl {
                    section: "sql".to_string(),
                    name: snippet,
                    source: e,
                }
            })?;
            info!("Executed SQL pipeline definition");
        }
    }

    Ok(())
}

/// Start HTTP API server and return (shared state, join handle).
pub(crate) async fn start_http_api(
    db: Arc<LaminarDB>,
    registry: Arc<prometheus::Registry>,
    config_path: PathBuf,
    config: ServerConfig,
) -> Result<(Arc<http::AppState>, tokio::task::JoinHandle<()>), ServerError> {
    let bind = config.server.bind.clone();

    let server_metrics = ServerMetrics::new(&registry);

    let app_state = Arc::new(http::AppState {
        db,
        config_path,
        current_config: tokio::sync::RwLock::new(config),
        reload_guard: ReloadGuard::new(),
        registry,
        server_metrics,
    });
    let router = http::build_router(Arc::clone(&app_state));
    let api_handle = http::serve(router, &bind).await?;
    info!("HTTP API listening on {bind}");
    Ok((app_state, api_handle))
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
    if !url.starts_with("file:///") {
        return None;
    }
    let raw = url.strip_prefix("file://").unwrap_or(url);
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
    let connector_keyword = source.connector.replace('-', "_").to_uppercase();
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
    let connector_keyword = sink.connector.replace('-', "_").to_uppercase();
    let mut opts: Vec<String> = sink
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
    if sink.delivery != "at_least_once" {
        opts.push(format!("delivery = '{}'", sink.delivery));
    }

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
    if lookup.cache.size_bytes != 100 * 1024 * 1024 {
        opts.push(format!("'cache_memory' = '{}'", lookup.cache.size_bytes));
    }
    if lookup.cache.ttl.as_secs() != 300 {
        opts.push(format!("'cache_ttl' = '{}'", lookup.cache.ttl.as_secs()));
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
    #[error("delta mode error: {0}")]
    Delta(String),
    #[cfg(feature = "cluster-unstable")]
    #[error(transparent)]
    DeltaConfig(#[from] DeltaConfigError),
}

#[cfg(test)]
mod tests {
    use super::*;
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
            state: StateSection::default(),
            checkpoint: CheckpointSection::default(),
            sources: vec![source],
            lookups: vec![],
            pipelines: vec![],
            sinks: vec![],
            sql: None,
            discovery: None,
            coordination: None,
            node_id: None,
        };
        execute_config_ddl(&db, &config)
            .await
            .expect("columnless OTel + WATERMARK FOR should compose");
    }

    /// Columnless Kafka source + WATERMARK FOR: the Kafka connector can't
    /// discover a schema without a reachable Schema Registry, so the DDL
    /// layer surfaces a "no columns declared and connector … could not
    /// auto-discover a schema" error. The server no longer pre-empts this
    /// — we just check the error bubbles up clearly.
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
            state: StateSection::default(),
            checkpoint: CheckpointSection::default(),
            sources: vec![source],
            lookups: vec![],
            pipelines: vec![],
            sinks: vec![],
            sql: None,
            discovery: None,
            coordination: None,
            node_id: None,
        };
        let err = execute_config_ddl(&db, &config).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("could not auto-discover a schema") || msg.contains("no columns declared"),
            "expected schema-discovery error from the DDL layer, got: {msg}"
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
    fn test_source_to_ddl_with_properties() {
        let mut source = make_source("events", "kafka");
        source.properties.insert(
            "brokers".to_string(),
            toml::Value::String("localhost:9092".to_string()),
        );
        source.properties.insert(
            "topic".to_string(),
            toml::Value::String("events".to_string()),
        );
        let ddl = source_to_ddl(&source);
        assert!(ddl.contains("brokers = 'localhost:9092'"));
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
            "brokers".to_string(),
            toml::Value::String("localhost:9092".to_string()),
        );
        let sink = SinkConfig {
            name: "output_sink".to_string(),
            pipeline: "vwap".to_string(),
            connector: "kafka".to_string(),
            delivery: "at_least_once".to_string(),
            properties: props,
        };
        let ddl = sink_to_ddl(&sink);
        assert!(ddl.starts_with("CREATE SINK output_sink FROM vwap INTO KAFKA"));
        assert!(ddl.contains("topic = 'output'"));
        assert!(ddl.contains("brokers = 'localhost:9092'"));
        // at_least_once is default, should not appear
        assert!(!ddl.contains("delivery"));
    }

    #[test]
    fn test_sink_to_ddl_exactly_once() {
        let sink = SinkConfig {
            name: "out".to_string(),
            pipeline: "p".to_string(),
            connector: "kafka".to_string(),
            delivery: "exactly_once".to_string(),
            properties: toml::Table::new(),
        };
        let ddl = sink_to_ddl(&sink);
        assert!(ddl.contains("delivery = 'exactly_once'"));
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

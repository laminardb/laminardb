//! Engine construction and lifecycle management for LaminarDB server.
//!
//! Translates TOML configuration into SQL DDL statements and executes them
//! against `LaminarDB`, then manages the pipeline lifecycle (start, run,
//! shutdown).

use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use tokio::signal;
use tracing::info;

use laminar_core::streaming::checkpoint::StreamCheckpointConfig;
use laminar_db::{DbError, LaminarDB, Profile};

use crate::config::{
    ConfigError, LookupConfig, PipelineConfig, ServerConfig, SinkConfig, SourceConfig,
};
use crate::delta_config::{DeltaConfig, DeltaConfigError};
use crate::http;
use crate::reload::ReloadGuard;

/// Handle to a running LaminarDB server (embedded or delta mode).
///
/// Call [`wait_for_shutdown`](ServerHandle::wait_for_shutdown) to block until
/// the server receives a shutdown signal (Ctrl-C).
pub enum ServerHandle {
    /// Embedded (single-node) mode.
    Embedded {
        /// LaminarDB engine.
        db: Arc<LaminarDB>,
        /// HTTP API task.
        api_handle: tokio::task::JoinHandle<()>,
        /// Config file watcher task.
        watcher_handle: Option<tokio::task::JoinHandle<()>>,
    },
    /// Delta (multi-node) mode.
    Delta(Box<crate::delta::DeltaHandle>),
}

impl ServerHandle {
    /// Block until `ctrl_c` is received, then gracefully shut down.
    pub async fn wait_for_shutdown(self) -> Result<(), ServerError> {
        match self {
            Self::Embedded {
                db,
                api_handle,
                watcher_handle,
            } => {
                signal::ctrl_c()
                    .await
                    .map_err(|e| ServerError::Shutdown(format!("signal handler failed: {e}")))?;

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
            Self::Delta(handle) => (*handle)
                .wait_for_shutdown()
                .await
                .map_err(|e| ServerError::Delta(e.to_string())),
        }
    }
}

/// Build and start a LaminarDB server from the given configuration.
///
/// 1. Constructs a `LaminarDB` instance via the builder API
/// 2. Executes DDL for all TOML-defined sources, lookups, pipelines, and sinks
/// 3. Starts the streaming pipeline
/// 4. Spawns the HTTP API server
///
/// # Errors
///
/// Returns `ServerError` if any phase fails.
pub async fn run_server(
    config: ServerConfig,
    config_path: PathBuf,
) -> Result<ServerHandle, ServerError> {
    let delta_cfg = DeltaConfig::from_server_config(&config)?;

    if let Some(delta_cfg) = delta_cfg {
        let handle = crate::delta::start_delta(config, delta_cfg, config_path)
            .await
            .map_err(|e| ServerError::Delta(e.to_string()))?;
        return Ok(ServerHandle::Delta(Box::new(handle)));
    }

    // 1. Build LaminarDB via builder API
    let mut builder = LaminarDB::builder();

    // Map state backend → storage_dir (must happen before profile selection)
    let has_storage = config.state.backend != "memory";
    if has_storage {
        builder = builder.storage_dir(&config.state.path);
    }

    // Map mode → profile (Embedded requires storage_dir; fall back to BareMetal)
    let profile = match config.server.mode.as_str() {
        "embedded" if has_storage => Profile::Embedded,
        "embedded" => Profile::BareMetal,
        "delta" => Profile::Delta,
        _ => Profile::BareMetal,
    };
    builder = builder.profile(profile);

    // Map checkpoint config
    let checkpoint_url = &config.checkpoint.url;
    let checkpoint_cfg = StreamCheckpointConfig {
        interval_ms: Some(config.checkpoint.interval.as_millis() as u64),
        data_dir: if checkpoint_url.starts_with("file:///") {
            Some(PathBuf::from(
                checkpoint_url
                    .strip_prefix("file://")
                    .unwrap_or(checkpoint_url),
            ))
        } else {
            None
        },
        max_retained: Some(10),
        ..StreamCheckpointConfig::default()
    };
    builder = builder.checkpoint(checkpoint_cfg);

    // Object store URL for non-file checkpoint URLs
    if !checkpoint_url.starts_with("file:///") && !checkpoint_url.is_empty() {
        builder = builder.object_store_url(checkpoint_url.clone());
        if !config.checkpoint.storage.is_empty() {
            builder = builder.object_store_options(config.checkpoint.storage.clone());
        }
    }

    // Wire tiering config if present
    if let Some(tiering) = &config.checkpoint.tiering {
        builder = builder.tiering(laminar_db::TieringConfig {
            hot_class: tiering.hot_class.clone(),
            warm_class: tiering.warm_class.clone(),
            cold_class: tiering.cold_class.clone(),
            hot_retention_secs: tiering.hot_retention.as_secs(),
            warm_retention_secs: tiering.warm_retention.as_secs(),
        });
    }

    let db = builder
        .build()
        .await
        .map_err(|e| ServerError::Build(e.to_string()))?;
    let db = Arc::new(db);

    // 2. Execute DDL for each TOML section (order matters: sources → lookups → pipelines → sinks)
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
        let ddl = lookup_to_ddl(lookup);
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

    // 3. Start pipeline
    db.start()
        .await
        .map_err(|e| ServerError::Start(e.to_string()))?;
    info!("Pipeline started");

    // 4. Start HTTP API
    let bind = config.server.bind.clone();
    let app_state = Arc::new(http::AppState {
        db: Arc::clone(&db),
        config_path: config_path.clone(),
        started_at: chrono::Utc::now(),
        current_config: tokio::sync::RwLock::new(config),
        reload_guard: ReloadGuard::new(),
        reload_total: AtomicU64::new(0),
        reload_last_ts: AtomicU64::new(0),
    });
    let router = http::build_router(Arc::clone(&app_state));
    let api_handle = http::serve(router, &bind).await?;
    info!("HTTP API listening on {bind}");

    // 5. Spawn config file watcher
    let watcher_handle = {
        let watcher_state = Arc::clone(&app_state);
        let watcher_path = config_path;
        Some(tokio::spawn(async move {
            crate::watcher::watch_config(
                watcher_path,
                watcher_state,
                std::time::Duration::from_millis(500),
            )
            .await;
        }))
    };
    info!("Config file watcher started");

    Ok(ServerHandle::Embedded {
        db,
        api_handle,
        watcher_handle,
    })
}

// ---------------------------------------------------------------------------
// DDL generation
// ---------------------------------------------------------------------------

/// Generate `CREATE SOURCE` DDL from a TOML source config.
///
/// Uses the connector-first syntax:
/// ```sql
/// CREATE SOURCE name (col1 TYPE, col2 TYPE, WATERMARK FOR col AS col - INTERVAL 'n' SECOND)
/// FROM CONNECTOR (key = 'value', ...);
/// ```
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

/// Generate `CREATE STREAM ... AS ...` DDL from a TOML pipeline config.
pub fn pipeline_to_ddl(pipeline: &PipelineConfig) -> String {
    format!("CREATE STREAM {} AS {}", pipeline.name, pipeline.sql.trim())
}

/// Generate `CREATE SINK` DDL from a TOML sink config.
///
/// Uses the `INTO CONNECTOR (...)` syntax:
/// ```sql
/// CREATE SINK name FROM pipeline INTO KAFKA (key = 'value', ...)
/// ```
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

/// Generate `CREATE LOOKUP TABLE` DDL from a TOML lookup config.
///
/// ```sql
/// CREATE LOOKUP TABLE name (col1 TYPE, ...) WITH (
///     'connector' = 'type', 'strategy' = '...', ...
/// )
/// ```
pub fn lookup_to_ddl(lookup: &LookupConfig) -> String {
    let mut parts = Vec::new();
    parts.push(format!("CREATE LOOKUP TABLE {}", lookup.name));

    // Column definitions
    if !lookup.schema.is_empty() {
        let col_defs: Vec<String> = lookup
            .schema
            .iter()
            .map(|c| {
                let nullability = if c.nullable { "" } else { " NOT NULL" };
                format!("{} {}{}", c.name, c.data_type, nullability)
            })
            .collect();
        parts.push(format!("({})", col_defs.join(", ")));
    }

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

    parts.join(" ")
}

/// Convert a TOML value to a SQL string literal value.
fn toml_value_to_sql(value: &toml::Value) -> String {
    match value {
        toml::Value::String(s) => s.clone(),
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

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Server runtime errors.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// LaminarDB builder failure.
    #[error("failed to build LaminarDB: {0}")]
    Build(String),

    /// DDL execution failure.
    #[error("failed to execute DDL for {section} '{name}': {source}")]
    Ddl {
        /// Section type (source, pipeline, sink, lookup).
        section: String,
        /// Object name.
        name: String,
        /// Underlying database error.
        source: DbError,
    },

    /// Pipeline start failure.
    #[error("failed to start pipeline: {0}")]
    Start(String),

    /// HTTP bind failure.
    #[error("HTTP server error: {0}")]
    Http(String),

    /// Graceful shutdown failure.
    #[error("shutdown error: {0}")]
    Shutdown(String),

    /// Configuration error.
    #[error(transparent)]
    Config(#[from] ConfigError),

    /// Delta mode error.
    #[error("delta mode error: {0}")]
    Delta(String),

    /// Delta configuration error.
    #[error(transparent)]
    DeltaConfig(#[from] DeltaConfigError),
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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
            parallelism: None,
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
            pushdown: true,
            cache: LookupCacheConfig::default(),
            properties: {
                let mut t = toml::Table::new();
                t.insert(
                    "connection".to_string(),
                    toml::Value::String("postgresql://localhost/db".to_string()),
                );
                t
            },
            schema: vec![ColumnDef {
                name: "symbol".to_string(),
                data_type: "VARCHAR".to_string(),
                nullable: false,
            }],
        };
        let ddl = lookup_to_ddl(&lookup);
        assert!(ddl.starts_with("CREATE LOOKUP TABLE instruments"));
        assert!(ddl.contains("symbol VARCHAR NOT NULL"));
        assert!(ddl.contains("'connector' = 'postgres'"));
        assert!(ddl.contains("'strategy' = 'poll'"));
        assert!(ddl.contains("'connection' = 'postgresql://localhost/db'"));
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
}

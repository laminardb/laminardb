//! TOML configuration parsing for LaminarDB server.
//!
//! Loads, validates, and applies defaults to `laminardb.toml` configuration
//! files. Supports environment variable substitution (`${VAR}` syntax) and
//! both embedded (single-node) and delta (multi-node) operating modes.

use std::collections::HashSet;
use std::path::Path;
use std::sync::LazyLock;
use std::time::Duration;

use regex::Regex;
use serde::Deserialize;

/// Regex for `${VAR}` and `${VAR:-default}` patterns.
static ENV_VAR_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}").expect("valid regex")
});

/// Load, parse, and validate a LaminarDB configuration file.
///
/// Performs the following steps:
/// 1. Read the file from disk
/// 2. Substitute environment variables (`${VAR}` syntax)
/// 3. Parse TOML into `ServerConfig`
/// 4. Apply defaults for missing optional fields
/// 5. Validate referential integrity
///
/// # Errors
///
/// Returns `ConfigError` if the file cannot be read, environment
/// variables are missing, TOML is malformed, or validation fails.
pub fn load_config(path: &Path) -> Result<ServerConfig, ConfigError> {
    let raw = std::fs::read_to_string(path).map_err(|e| ConfigError::FileRead {
        path: path.to_path_buf(),
        source: e,
    })?;

    let substituted = substitute_env_vars(&raw)?;
    let config: ServerConfig =
        toml::from_str(&substituted).map_err(|e| ConfigError::ParseError {
            path: path.to_path_buf(),
            source: e,
        })?;

    validate_config(&config)?;
    Ok(config)
}

/// Substitute `${VAR_NAME}` patterns with environment variable values.
///
/// Supports optional default values: `${VAR_NAME:-default_value}`.
/// Unresolved variables without defaults produce an error.
fn substitute_env_vars(input: &str) -> Result<String, ConfigError> {
    let mut errors = Vec::new();
    let result = ENV_VAR_RE.replace_all(input, |caps: &regex::Captures| {
        let var_name = &caps[1];
        match std::env::var(var_name) {
            Ok(val) => val,
            Err(_) => {
                if let Some(default) = caps.get(2) {
                    default.as_str().to_string()
                } else {
                    errors.push(var_name.to_string());
                    String::new()
                }
            }
        }
    });

    if !errors.is_empty() {
        return Err(ConfigError::MissingEnvVars { vars: errors });
    }

    Ok(result.into_owned())
}

/// Validate referential integrity and semantic constraints.
fn validate_config(config: &ServerConfig) -> Result<(), ConfigError> {
    let mut errors = Vec::new();

    // Collect all pipeline names
    let pipeline_names: HashSet<&str> = config.pipelines.iter().map(|p| p.name.as_str()).collect();

    // Validate: sink must reference an existing pipeline
    for sink in &config.sinks {
        if !pipeline_names.contains(sink.pipeline.as_str()) {
            errors.push(format!(
                "sink '{}' references unknown pipeline '{}'",
                sink.name, sink.pipeline
            ));
        }
    }

    // Validate: no duplicate names within a section
    let mut seen_sources = HashSet::new();
    for source in &config.sources {
        if !seen_sources.insert(&source.name) {
            errors.push(format!("duplicate source name: '{}'", source.name));
        }
    }

    let mut seen_pipelines = HashSet::new();
    for pipeline in &config.pipelines {
        if !seen_pipelines.insert(&pipeline.name) {
            errors.push(format!("duplicate pipeline name: '{}'", pipeline.name));
        }
    }

    let mut seen_sinks = HashSet::new();
    for sink in &config.sinks {
        if !seen_sinks.insert(&sink.name) {
            errors.push(format!("duplicate sink name: '{}'", sink.name));
        }
    }

    let mut seen_lookups = HashSet::new();
    for lookup in &config.lookups {
        if !seen_lookups.insert(&lookup.name) {
            errors.push(format!("duplicate lookup name: '{}'", lookup.name));
        }
    }

    // Validate: bind address is parseable
    if config.server.bind.parse::<std::net::SocketAddr>().is_err() {
        errors.push(format!(
            "invalid server bind address: '{}'",
            config.server.bind
        ));
    }

    // Validate: delta mode requires discovery and coordination
    if config.server.mode == "delta" {
        if config.discovery.is_none() {
            errors.push("mode = \"delta\" requires a [discovery] section".to_string());
        }
        if config.coordination.is_none() {
            errors.push("mode = \"delta\" requires a [coordination] section".to_string());
        }
        if config.node_id.is_none() {
            errors.push("mode = \"delta\" requires node_id to be set".to_string());
        }
    }

    if !errors.is_empty() {
        return Err(ConfigError::ValidationErrors { errors });
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Top-level server configuration, deserialized from TOML.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ServerConfig {
    #[serde(default)]
    pub server: ServerSection,

    #[serde(default)]
    pub state: StateSection,

    #[serde(default)]
    pub checkpoint: CheckpointSection,

    #[serde(default, rename = "source")]
    pub sources: Vec<SourceConfig>,

    #[serde(default, rename = "lookup")]
    pub lookups: Vec<LookupConfig>,

    #[serde(default, rename = "pipeline")]
    pub pipelines: Vec<PipelineConfig>,

    #[serde(default, rename = "sink")]
    pub sinks: Vec<SinkConfig>,

    /// Raw SQL DDL executed before `start()`, as alternative to structured sections.
    #[serde(default)]
    pub sql: Option<String>,

    pub discovery: Option<DiscoverySection>,
    pub coordination: Option<CoordinationSection>,
    pub node_id: Option<String>,
}

/// `[server]` section.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ServerSection {
    /// "embedded" (single-node) or "delta" (multi-node).
    #[serde(default = "default_mode")]
    pub mode: String,

    #[serde(default = "default_bind")]
    pub bind: String,

    /// 0 = auto-detect CPU count.
    #[serde(default)]
    pub workers: usize,

    #[serde(default = "default_log_level")]
    pub log_level: String,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            mode: default_mode(),
            bind: default_bind(),
            workers: 0,
            log_level: default_log_level(),
        }
    }
}

/// `[state]` section.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct StateSection {
    /// "memory", "mmap", or "disaggregated".
    #[serde(default = "default_state_backend")]
    pub backend: String,

    #[serde(default = "default_state_path")]
    pub path: String,
}

impl Default for StateSection {
    fn default() -> Self {
        Self {
            backend: default_state_backend(),
            path: default_state_path(),
        }
    }
}

/// `[checkpoint]` section.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CheckpointSection {
    /// Supports: `file:///path`, `s3://bucket/prefix`, `gs://bucket/prefix`.
    #[serde(default = "default_checkpoint_url")]
    pub url: String,

    #[serde(default = "default_checkpoint_interval", with = "humantime_serde")]
    pub interval: Duration,

    /// Number of recent checkpoints to retain before pruning.
    #[serde(default = "default_max_retained")]
    pub max_retained: usize,

    /// Cloud storage credentials (e.g., `aws_access_key_id`, `aws_region`).
    #[serde(default)]
    pub storage: std::collections::HashMap<String, String>,

    #[serde(default)]
    pub tiering: Option<TieringSection>,
}

impl Default for CheckpointSection {
    fn default() -> Self {
        Self {
            url: default_checkpoint_url(),
            interval: default_checkpoint_interval(),
            max_retained: default_max_retained(),
            storage: std::collections::HashMap::new(),
            tiering: None,
        }
    }
}

/// `[checkpoint.tiering]` section: S3 storage class tiering.
///
/// Controls how checkpoint objects are assigned to S3 storage classes
/// for cost optimization. Active checkpoints use the hot tier,
/// older checkpoints are moved to warm/cold tiers via S3 Lifecycle rules.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TieringSection {
    /// e.g., `"EXPRESS_ONE_ZONE"`, `"STANDARD"`.
    #[serde(default = "default_hot_class")]
    pub hot_class: String,

    #[serde(default = "default_warm_class")]
    pub warm_class: String,

    /// Empty = no cold tier.
    #[serde(default)]
    pub cold_class: String,

    #[serde(default = "default_hot_retention", with = "humantime_serde")]
    pub hot_retention: Duration,

    /// 0 = no cold tier.
    #[serde(default = "default_warm_retention", with = "humantime_serde")]
    pub warm_retention: Duration,
}

/// `[[source]]` section.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SourceConfig {
    pub name: String,

    /// "kafka", "postgres_cdc", "mysql_cdc", "otel", etc.
    pub connector: String,

    /// "json", "avro", "protobuf", "csv".
    #[serde(default = "default_format")]
    pub format: String,

    #[serde(default)]
    pub properties: toml::Table,

    #[serde(default)]
    pub schema: Vec<ColumnDef>,

    pub watermark: Option<WatermarkConfig>,
}

/// Column definition within a source or lookup schema.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ColumnDef {
    pub name: String,

    /// SQL type: "INT", "BIGINT", "VARCHAR", "TIMESTAMP", "DOUBLE", "BOOLEAN".
    #[serde(rename = "type")]
    pub data_type: String,

    #[serde(default = "default_true")]
    pub nullable: bool,
}

/// Watermark configuration for a source.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct WatermarkConfig {
    pub column: String,

    #[serde(default = "default_max_ooo", with = "humantime_serde")]
    pub max_out_of_orderness: Duration,
}

/// `[[lookup]]` section.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LookupConfig {
    pub name: String,

    /// "postgres", "mysql", "redis", "csv".
    pub connector: String,

    /// "poll", "cdc", "manual".
    #[serde(default = "default_lookup_strategy")]
    pub strategy: String,

    #[serde(default)]
    pub cache: LookupCacheConfig,

    #[serde(default)]
    pub properties: toml::Table,

    #[serde(default)]
    pub primary_key: Vec<String>,

    #[serde(default)]
    pub schema: Vec<ColumnDef>,
}

/// Cache configuration for lookup tables.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LookupCacheConfig {
    #[serde(default = "default_cache_size")]
    pub size_bytes: u64,

    #[serde(default = "default_cache_ttl", with = "humantime_serde")]
    pub ttl: Duration,
}

impl Default for LookupCacheConfig {
    fn default() -> Self {
        Self {
            size_bytes: default_cache_size(),
            ttl: default_cache_ttl(),
        }
    }
}

/// `[[pipeline]]` section.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PipelineConfig {
    pub name: String,
    pub sql: String,
}

/// `[[sink]]` section.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SinkConfig {
    pub name: String,
    pub pipeline: String,

    /// "kafka", "postgres", "delta-lake", "iceberg", "files", "websocket", "stdout".
    pub connector: String,

    /// "at_least_once" or "exactly_once".
    #[serde(default = "default_delivery")]
    pub delivery: String,

    #[serde(default)]
    pub properties: toml::Table,
}

/// `[discovery]` section (delta mode).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DiscoverySection {
    /// "static", "dns", "gossip".
    pub strategy: String,

    #[serde(default)]
    pub seeds: Vec<String>,

    #[serde(default = "default_gossip_port")]
    pub gossip_port: u16,
}

/// `[coordination]` section (delta mode).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CoordinationSection {
    #[serde(default = "default_coordination_strategy")]
    pub strategy: String,

    #[serde(default = "default_raft_port")]
    pub raft_port: u16,

    #[serde(default = "default_election_timeout", with = "humantime_serde")]
    pub election_timeout: Duration,

    #[serde(default = "default_heartbeat_interval", with = "humantime_serde")]
    pub heartbeat_interval: Duration,
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Configuration errors with structured context.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Failed to read the configuration file.
    #[error("failed to read config file '{}': {source}", path.display())]
    FileRead {
        /// Path that failed to read.
        path: std::path::PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },

    /// TOML parse error.
    #[error("failed to parse config file '{}': {source}", path.display())]
    ParseError {
        /// Path that failed to parse.
        path: std::path::PathBuf,
        /// Underlying TOML error.
        source: toml::de::Error,
    },

    /// Missing required environment variables.
    #[error("missing environment variables: {}", vars.join(", "))]
    MissingEnvVars {
        /// Names of missing variables.
        vars: Vec<String>,
    },

    /// Referential integrity or semantic validation failures.
    #[error("configuration validation errors:\n  - {}", errors.join("\n  - "))]
    ValidationErrors {
        /// List of validation error messages.
        errors: Vec<String>,
    },
}

// ---------------------------------------------------------------------------
// Default value functions
// ---------------------------------------------------------------------------

fn default_mode() -> String {
    "embedded".to_string()
}
fn default_bind() -> String {
    "127.0.0.1:8080".to_string()
}
fn default_log_level() -> String {
    "info".to_string()
}
fn default_state_backend() -> String {
    "memory".to_string()
}
fn default_state_path() -> String {
    "./data/state".to_string()
}
fn default_checkpoint_url() -> String {
    let base = std::env::temp_dir();
    let path = base.join("laminardb").join("checkpoints");
    // file:// URLs need forward slashes on all platforms.
    let path_str = path.to_string_lossy().replace('\\', "/");
    format!("file:///{path_str}")
}
fn default_max_retained() -> usize {
    10
}
fn default_checkpoint_interval() -> Duration {
    Duration::from_secs(10)
}
fn default_format() -> String {
    "json".to_string()
}
fn default_max_ooo() -> Duration {
    Duration::from_secs(5)
}
fn default_lookup_strategy() -> String {
    "poll".to_string()
}
fn default_true() -> bool {
    true
}
fn default_cache_size() -> u64 {
    100 * 1024 * 1024
}
fn default_cache_ttl() -> Duration {
    Duration::from_secs(300)
}
fn default_delivery() -> String {
    "at_least_once".to_string()
}
fn default_hot_class() -> String {
    "STANDARD".to_string()
}
fn default_warm_class() -> String {
    "STANDARD".to_string()
}
fn default_hot_retention() -> Duration {
    Duration::from_secs(86400) // 24h
}
fn default_warm_retention() -> Duration {
    Duration::from_secs(604_800) // 7d
}
fn default_gossip_port() -> u16 {
    7946
}
fn default_coordination_strategy() -> String {
    "raft".to_string()
}
fn default_raft_port() -> u16 {
    7947
}
fn default_election_timeout() -> Duration {
    Duration::from_millis(1500)
}
fn default_heartbeat_interval() -> Duration {
    Duration::from_millis(300)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let toml = "[server]\n";
        let config: ServerConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.server.mode, "embedded");
        assert_eq!(config.server.bind, "127.0.0.1:8080");
        assert!(config.sources.is_empty());
        assert!(config.pipelines.is_empty());
        assert!(config.sinks.is_empty());
    }

    #[test]
    fn test_parse_full_embedded_config() {
        let toml = r#"
[server]
mode = "embedded"
bind = "127.0.0.1:8080"
workers = 4

[state]
backend = "memory"

[checkpoint]
url = "file:///tmp/checkpoints"
interval = "10s"
mode = "aligned"

[[source]]
name = "trades"
connector = "kafka"
format = "json"
[source.properties]
brokers = "localhost:9092"
topic = "trades"
[[source.schema]]
name = "symbol"
type = "VARCHAR"
nullable = false
[[source.schema]]
name = "price"
type = "DOUBLE"
[source.watermark]
column = "trade_time"
max_out_of_orderness = "5s"

[[pipeline]]
name = "vwap"
sql = "SELECT symbol, SUM(price) FROM trades GROUP BY symbol"

[[sink]]
name = "output"
pipeline = "vwap"
connector = "kafka"
[sink.properties]
topic = "vwap_output"
"#;

        let config: ServerConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.sources.len(), 1);
        assert_eq!(config.sources[0].name, "trades");
        assert_eq!(config.sources[0].schema.len(), 2);
        assert!(!config.sources[0].schema[0].nullable);
        assert!(config.sources[0].schema[1].nullable); // default true
        assert!(config.sources[0].watermark.is_some());
        assert_eq!(config.pipelines.len(), 1);
        assert_eq!(config.sinks.len(), 1);
        assert_eq!(config.sinks[0].pipeline, "vwap");

        validate_config(&config).unwrap();
    }

    #[test]
    fn test_parse_full_delta_config() {
        let toml = r#"
node_id = "star-1"

[server]
mode = "delta"
bind = "0.0.0.0:8080"
workers = 8

[state]
backend = "mmap"
path = "/data/state"
max_size_bytes = 10737418240

[checkpoint]
url = "s3://bucket/checkpoints"
interval = "30s"
snapshot_strategy = "fork_cow"

[discovery]
strategy = "static"
seeds = ["node-1:7946", "node-2:7946"]
gossip_port = 7946

[coordination]
strategy = "raft"
raft_port = 7947
election_timeout = "1500ms"
heartbeat_interval = "300ms"

[[source]]
name = "orders"
connector = "kafka"
format = "avro"

[[pipeline]]
name = "enrichment"
sql = "SELECT * FROM orders"
parallelism = 8

[[sink]]
name = "output"
pipeline = "enrichment"
connector = "kafka"
delivery = "exactly_once"
"#;

        let config: ServerConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.node_id.as_deref(), Some("star-1"));
        assert_eq!(config.server.mode, "delta");
        assert_eq!(config.state.backend, "mmap");
        assert!(config.discovery.is_some());
        assert!(config.coordination.is_some());

        let coord = config.coordination.as_ref().unwrap();
        assert_eq!(coord.election_timeout, Duration::from_millis(1500));
        assert_eq!(coord.heartbeat_interval, Duration::from_millis(300));

        validate_config(&config).unwrap();
    }

    #[test]
    fn test_env_var_substitution_resolves() {
        std::env::set_var("LAMINAR_TEST_VAR_1", "resolved_value");
        let input = "brokers = \"${LAMINAR_TEST_VAR_1}\"";
        let result = substitute_env_vars(input).unwrap();
        assert_eq!(result, "brokers = \"resolved_value\"");
        std::env::remove_var("LAMINAR_TEST_VAR_1");
    }

    #[test]
    fn test_env_var_substitution_with_default() {
        // Ensure the variable is NOT set
        std::env::remove_var("LAMINAR_TEST_UNSET_VAR");
        let input = "brokers = \"${LAMINAR_TEST_UNSET_VAR:-localhost:9092}\"";
        let result = substitute_env_vars(input).unwrap();
        assert_eq!(result, "brokers = \"localhost:9092\"");
    }

    #[test]
    fn test_env_var_substitution_missing_errors() {
        std::env::remove_var("LAMINAR_TEST_MISSING_1");
        std::env::remove_var("LAMINAR_TEST_MISSING_2");
        let input = "a = \"${LAMINAR_TEST_MISSING_1}\"\nb = \"${LAMINAR_TEST_MISSING_2}\"";
        let err = substitute_env_vars(input).unwrap_err();
        match err {
            ConfigError::MissingEnvVars { vars } => {
                assert!(vars.contains(&"LAMINAR_TEST_MISSING_1".to_string()));
                assert!(vars.contains(&"LAMINAR_TEST_MISSING_2".to_string()));
            }
            _ => panic!("expected MissingEnvVars"),
        }
    }

    #[test]
    fn test_validate_sink_references_missing_pipeline() {
        let toml = r#"
[[pipeline]]
name = "exists"
sql = "SELECT 1"

[[sink]]
name = "broken"
pipeline = "nonexistent"
connector = "kafka"
"#;

        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        match err {
            ConfigError::ValidationErrors { errors } => {
                assert!(errors[0].contains("nonexistent"));
            }
            _ => panic!("expected ValidationErrors"),
        }
    }

    #[test]
    fn test_validate_duplicate_source_names() {
        let toml = r#"
[[source]]
name = "dup"
connector = "kafka"

[[source]]
name = "dup"
connector = "kafka"

[[pipeline]]
name = "p"
sql = "SELECT 1"
"#;

        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        match err {
            ConfigError::ValidationErrors { errors } => {
                assert!(errors.iter().any(|e| e.contains("duplicate source")));
            }
            _ => panic!("expected ValidationErrors"),
        }
    }

    #[test]
    fn test_validate_duplicate_pipeline_names() {
        let toml = r#"
[[pipeline]]
name = "dup"
sql = "SELECT 1"

[[pipeline]]
name = "dup"
sql = "SELECT 2"
"#;

        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        match err {
            ConfigError::ValidationErrors { errors } => {
                assert!(errors.iter().any(|e| e.contains("duplicate pipeline")));
            }
            _ => panic!("expected ValidationErrors"),
        }
    }

    #[test]
    fn test_validate_invalid_bind_address() {
        let toml = r#"
[server]
bind = "not-a-socket-addr"
"#;

        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        match err {
            ConfigError::ValidationErrors { errors } => {
                assert!(errors.iter().any(|e| e.contains("invalid server bind")));
            }
            _ => panic!("expected ValidationErrors"),
        }
    }

    #[test]
    fn test_default_values_applied() {
        let config = ServerConfig {
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
        };

        assert_eq!(config.server.mode, "embedded");
        assert_eq!(config.server.bind, "127.0.0.1:8080");
        assert_eq!(config.server.workers, 0);
        assert_eq!(config.server.log_level, "info");
        assert_eq!(config.state.backend, "memory");
        assert_eq!(config.checkpoint.interval, Duration::from_secs(10));
    }

    #[test]
    fn test_checkpoint_duration_parsing() {
        let toml = r#"
[checkpoint]
interval = "30s"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.checkpoint.interval, Duration::from_secs(30));

        let toml2 = r#"
[checkpoint]
interval = "1m"
"#;
        let config2: ServerConfig = toml::from_str(toml2).unwrap();
        assert_eq!(config2.checkpoint.interval, Duration::from_secs(60));

        let toml3 = r#"
[checkpoint]
interval = "500ms"
"#;
        let config3: ServerConfig = toml::from_str(toml3).unwrap();
        assert_eq!(config3.checkpoint.interval, Duration::from_millis(500));
    }

    #[test]
    fn test_watermark_config_parsing() {
        let toml = r#"
[[source]]
name = "s"
connector = "kafka"
[source.watermark]
column = "event_time"
max_out_of_orderness = "10s"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        let wm = config.sources[0].watermark.as_ref().unwrap();
        assert_eq!(wm.column, "event_time");
        assert_eq!(wm.max_out_of_orderness, Duration::from_secs(10));
    }

    #[test]
    fn test_lookup_cache_defaults() {
        let cache = LookupCacheConfig::default();
        assert_eq!(cache.size_bytes, 100 * 1024 * 1024);
        assert_eq!(cache.ttl, Duration::from_secs(300));
    }

    #[test]
    fn test_delta_mode_requires_discovery() {
        let toml = r#"
[server]
mode = "delta"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        match err {
            ConfigError::ValidationErrors { errors } => {
                assert!(errors.iter().any(|e| e.contains("[discovery]")));
                assert!(errors.iter().any(|e| e.contains("[coordination]")));
                assert!(errors.iter().any(|e| e.contains("node_id")));
            }
            _ => panic!("expected ValidationErrors"),
        }
    }

    #[test]
    fn test_source_schema_parsing() {
        let toml = r#"
[[source]]
name = "test"
connector = "kafka"
[[source.schema]]
name = "id"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "name"
type = "VARCHAR"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.sources[0].schema.len(), 2);
        assert_eq!(config.sources[0].schema[0].data_type, "BIGINT");
        assert!(!config.sources[0].schema[0].nullable);
        assert_eq!(config.sources[0].schema[1].data_type, "VARCHAR");
        assert!(config.sources[0].schema[1].nullable); // default
    }

    #[test]
    fn test_checkpoint_tiering_parsing() {
        let toml = r#"
[checkpoint]
url = "s3://bucket/checkpoints"
interval = "30s"

[checkpoint.tiering]
hot_class = "EXPRESS_ONE_ZONE"
warm_class = "STANDARD"
cold_class = "GLACIER_IR"
hot_retention = "12h"
warm_retention = "3d"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        let tiering = config.checkpoint.tiering.as_ref().unwrap();
        assert_eq!(tiering.hot_class, "EXPRESS_ONE_ZONE");
        assert_eq!(tiering.warm_class, "STANDARD");
        assert_eq!(tiering.cold_class, "GLACIER_IR");
        assert_eq!(tiering.hot_retention, Duration::from_secs(43200));
        assert_eq!(tiering.warm_retention, Duration::from_secs(259_200));
    }

    #[test]
    fn test_checkpoint_tiering_defaults() {
        let toml = r#"
[checkpoint]
url = "s3://bucket/checkpoints"

[checkpoint.tiering]
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        let tiering = config.checkpoint.tiering.as_ref().unwrap();
        assert_eq!(tiering.hot_class, "STANDARD");
        assert_eq!(tiering.warm_class, "STANDARD");
        assert!(tiering.cold_class.is_empty());
        assert_eq!(tiering.hot_retention, Duration::from_secs(86400));
        assert_eq!(tiering.warm_retention, Duration::from_secs(604_800));
    }

    #[test]
    fn test_checkpoint_no_tiering() {
        let toml = r#"
[checkpoint]
url = "s3://bucket/checkpoints"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        assert!(config.checkpoint.tiering.is_none());
    }

    #[test]
    fn test_config_error_display_messages() {
        let err = ConfigError::MissingEnvVars {
            vars: vec!["A".to_string(), "B".to_string()],
        };
        assert_eq!(err.to_string(), "missing environment variables: A, B");

        let err = ConfigError::ValidationErrors {
            errors: vec!["error one".to_string(), "error two".to_string()],
        };
        let msg = err.to_string();
        assert!(msg.contains("error one"));
        assert!(msg.contains("error two"));
    }
}

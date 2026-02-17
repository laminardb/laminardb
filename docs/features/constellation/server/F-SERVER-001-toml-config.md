# F-SERVER-001: TOML Configuration Parsing

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F-SERVER-001 |
| **Status** | Draft |
| **Priority** | P0 |
| **Phase** | 6c |
| **Effort** | M (3-5 days) |
| **Dependencies** | None (foundational) |
| **Blocks** | F-SERVER-002, F-SERVER-003, F-SERVER-004, F-SERVER-005 |
| **Owner** | TBD |
| **Created** | 2026-02-16 |
| **Crate** | `laminar-server` |
| **Module** | `crates/laminar-server/src/config.rs` |

## Summary

LaminarDB server reads a `laminardb.toml` configuration file that declaratively defines the entire streaming pipeline topology. This eliminates Rust boilerplate for SQL-literate users: they define sources, lookup tables, SQL pipelines, and sinks in TOML, and the server constructs and runs everything. The configuration supports environment variable substitution (`${VAR_NAME}` syntax), sensible defaults for all optional fields, comprehensive validation at parse time, and two operational modes: embedded (single-node) and constellation (multi-node). External dependencies are `toml` for parsing and `serde` for deserialization.

## Goals

- Define a complete TOML schema covering all LaminarDB pipeline components
- Parse and validate `laminardb.toml` with clear, actionable error messages on misconfiguration
- Support environment variable substitution in string values (e.g., `${KAFKA_BROKERS}`)
- Provide sensible defaults for all optional fields so minimal configs work out of the box
- Support both embedded mode (single-node, in-process) and constellation mode (multi-node, distributed)
- Validate referential integrity (e.g., sink references a pipeline that exists, pipeline references sources that exist)
- Strongly typed Rust structs with `serde::Deserialize` for all config sections
- Config file path configurable via CLI `--config` flag (already in `main.rs`)

## Non-Goals

- Runtime configuration changes (covered by F-SERVER-004 hot reload)
- GUI configuration editor (covered by Phase 5 admin dashboard)
- Configuration distribution across constellation nodes (each node reads its own file)
- Secret management (secrets should be in environment variables, not in TOML)
- JSON or YAML config format support (TOML only for v1)

## Technical Design

### Architecture

The configuration layer sits between the CLI argument parser (clap) and the engine construction layer (F-SERVER-002). It is purely a data transformation: TOML text -> validated Rust structs.

```
CLI (clap)           Config Layer              Engine Construction
+-----------+        +------------------+      +-------------------+
| --config  | -----> | parse TOML file  | ---> | LaminarDb::from_  |
| path      |        | env var subst    |      |   config(&config) |
+-----------+        | validate refs    |      |   .build()        |
                     | apply defaults   |      +-------------------+
                     +------------------+
```

### API/Interface

```rust
use std::path::Path;
use std::time::Duration;
use serde::Deserialize;

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
    let raw = std::fs::read_to_string(path)
        .map_err(|e| ConfigError::FileRead {
            path: path.to_path_buf(),
            source: e,
        })?;

    let substituted = substitute_env_vars(&raw)?;
    let config: ServerConfig = toml::from_str(&substituted)
        .map_err(|e| ConfigError::ParseError {
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
    let re = regex::Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
        .expect("valid regex");

    let mut errors = Vec::new();
    let result = re.replace_all(input, |caps: &regex::Captures| {
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

    // Collect all source names
    let source_names: std::collections::HashSet<&str> =
        config.sources.iter().map(|s| s.name.as_str()).collect();

    // Collect all lookup names
    let lookup_names: std::collections::HashSet<&str> =
        config.lookups.iter().map(|l| l.name.as_str()).collect();

    // Collect all pipeline names
    let pipeline_names: std::collections::HashSet<&str> =
        config.pipelines.iter().map(|p| p.name.as_str()).collect();

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
    let mut seen_sources = std::collections::HashSet::new();
    for source in &config.sources {
        if !seen_sources.insert(&source.name) {
            errors.push(format!("duplicate source name: '{}'", source.name));
        }
    }

    let mut seen_pipelines = std::collections::HashSet::new();
    for pipeline in &config.pipelines {
        if !seen_pipelines.insert(&pipeline.name) {
            errors.push(format!("duplicate pipeline name: '{}'", pipeline.name));
        }
    }

    // Validate: bind address is parseable
    if config.server.bind.parse::<std::net::SocketAddr>().is_err() {
        errors.push(format!(
            "invalid server bind address: '{}'",
            config.server.bind
        ));
    }

    // Validate: constellation mode requires discovery and coordination
    if config.server.mode == "constellation" {
        if config.discovery.is_none() {
            errors.push(
                "mode = \"constellation\" requires a [discovery] section".to_string()
            );
        }
        if config.coordination.is_none() {
            errors.push(
                "mode = \"constellation\" requires a [coordination] section".to_string()
            );
        }
        if config.node_id.is_none() {
            errors.push(
                "mode = \"constellation\" requires node_id to be set".to_string()
            );
        }
    }

    if !errors.is_empty() {
        return Err(ConfigError::ValidationErrors { errors });
    }

    Ok(())
}
```

### Data Structures

```rust
/// Top-level server configuration.
///
/// Deserialized from `laminardb.toml`. All sections except `[server]`
/// are optional (an empty config starts a server with no pipelines).
#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    /// Server-level settings.
    #[serde(default)]
    pub server: ServerSection,

    /// State store configuration.
    #[serde(default)]
    pub state: StateSection,

    /// Checkpoint configuration.
    #[serde(default)]
    pub checkpoint: CheckpointSection,

    /// Streaming sources (Kafka, CDC, etc.).
    #[serde(default, rename = "source")]
    pub sources: Vec<SourceConfig>,

    /// Lookup tables for enrichment joins.
    #[serde(default, rename = "lookup")]
    pub lookups: Vec<LookupConfig>,

    /// SQL pipelines to compile and execute.
    #[serde(default, rename = "pipeline")]
    pub pipelines: Vec<PipelineConfig>,

    /// Output sinks (Kafka, Postgres, Delta Lake, etc.).
    #[serde(default, rename = "sink")]
    pub sinks: Vec<SinkConfig>,

    /// Constellation mode settings (optional; absent = embedded mode).
    pub discovery: Option<DiscoverySection>,

    /// Coordination settings (optional; absent = embedded mode).
    pub coordination: Option<CoordinationSection>,

    /// Node identity for constellation mode.
    pub node_id: Option<String>,
}

/// [server] section: server-level settings.
#[derive(Debug, Clone, Deserialize)]
pub struct ServerSection {
    /// Operating mode: "embedded" (single-node) or "constellation" (multi-node).
    #[serde(default = "default_mode")]
    pub mode: String,

    /// Bind address for HTTP API.
    #[serde(default = "default_bind")]
    pub bind: String,

    /// Bind address for Prometheus metrics endpoint.
    #[serde(default = "default_metrics_bind")]
    pub metrics_bind: String,

    /// Number of worker threads (0 = auto-detect CPU count).
    #[serde(default)]
    pub workers: usize,

    /// Log level: trace, debug, info, warn, error.
    #[serde(default = "default_log_level")]
    pub log_level: String,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            mode: "embedded".to_string(),
            bind: "127.0.0.1:8080".to_string(),
            metrics_bind: "127.0.0.1:9090".to_string(),
            workers: 0,
            log_level: "info".to_string(),
        }
    }
}

/// [state] section: state store backend configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct StateSection {
    /// Backend type: "memory" or "mmap".
    /// Note: "rocksdb" was removed in F-LOOKUP-010. Persistent state
    /// uses mmap backend or foyer hybrid cache (F-LOOKUP-005).
    #[serde(default = "default_state_backend")]
    pub backend: String,

    /// Path for persistent state (mmap, rocksdb).
    #[serde(default = "default_state_path")]
    pub path: String,

    /// Maximum state size in bytes (0 = unlimited for memory backend).
    #[serde(default)]
    pub max_size_bytes: u64,
}

impl Default for StateSection {
    fn default() -> Self {
        Self {
            backend: "memory".to_string(),
            path: "./data/state".to_string(),
            max_size_bytes: 0,
        }
    }
}

/// [checkpoint] section: checkpointing configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct CheckpointSection {
    /// Storage URL for checkpoint data.
    /// Supports: file:///path, s3://bucket/prefix, gs://bucket/prefix.
    #[serde(default = "default_checkpoint_url")]
    pub url: String,

    /// Checkpoint interval (e.g., "10s", "1m", "30s").
    #[serde(default = "default_checkpoint_interval", with = "humantime_serde")]
    pub interval: Duration,

    /// Checkpoint mode: "aligned" or "unaligned".
    #[serde(default = "default_checkpoint_mode")]
    pub mode: String,

    /// Snapshot strategy: "full", "incremental", "fork_cow".
    #[serde(default = "default_snapshot_strategy")]
    pub snapshot_strategy: String,
}

impl Default for CheckpointSection {
    fn default() -> Self {
        Self {
            url: "file:///tmp/laminardb/checkpoints".to_string(),
            interval: Duration::from_secs(10),
            mode: "aligned".to_string(),
            snapshot_strategy: "full".to_string(),
        }
    }
}

/// [[source]] section: streaming source definition.
#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    /// Unique name for this source (referenced by SQL and sinks).
    pub name: String,

    /// Connector type: "kafka", "postgres_cdc", "mysql_cdc", "generator".
    pub connector: String,

    /// Data format: "json", "avro", "protobuf", "csv".
    #[serde(default = "default_format")]
    pub format: String,

    /// Connector-specific properties (e.g., broker, topic, table).
    #[serde(default)]
    pub properties: toml::Table,

    /// Schema definition (column names and types).
    #[serde(default)]
    pub schema: Vec<ColumnDef>,

    /// Watermark configuration.
    pub watermark: Option<WatermarkConfig>,
}

/// Column definition within a source schema.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnDef {
    /// Column name.
    pub name: String,

    /// SQL type: "INT", "BIGINT", "VARCHAR", "TIMESTAMP", "DOUBLE", "BOOLEAN".
    #[serde(rename = "type")]
    pub data_type: String,

    /// Whether this column is nullable.
    #[serde(default = "default_true")]
    pub nullable: bool,
}

/// Watermark configuration for a source.
#[derive(Debug, Clone, Deserialize)]
pub struct WatermarkConfig {
    /// Column containing event timestamps.
    pub column: String,

    /// Maximum allowed out-of-orderness (e.g., "5s", "1m").
    #[serde(default = "default_max_ooo", with = "humantime_serde")]
    pub max_out_of_orderness: Duration,
}

/// [[lookup]] section: lookup table for enrichment joins.
#[derive(Debug, Clone, Deserialize)]
pub struct LookupConfig {
    /// Unique name for this lookup table.
    pub name: String,

    /// Connector type: "postgres", "mysql", "redis", "csv".
    pub connector: String,

    /// Refresh strategy: "poll", "cdc", "manual".
    #[serde(default = "default_lookup_strategy")]
    pub strategy: String,

    /// Whether to push down predicates to the source.
    #[serde(default = "default_true")]
    pub pushdown: bool,

    /// Cache configuration.
    #[serde(default)]
    pub cache: LookupCacheConfig,

    /// Connector-specific properties.
    #[serde(default)]
    pub properties: toml::Table,

    /// Schema definition.
    #[serde(default)]
    pub schema: Vec<ColumnDef>,
}

/// Cache configuration for lookup tables.
#[derive(Debug, Clone, Deserialize)]
pub struct LookupCacheConfig {
    /// Cache size in bytes.
    #[serde(default = "default_cache_size")]
    pub size_bytes: u64,

    /// TTL for cached entries (e.g., "5m", "1h").
    #[serde(default = "default_cache_ttl", with = "humantime_serde")]
    pub ttl: Duration,

    /// Enable hybrid (memory + disk) caching via foyer.
    #[serde(default)]
    pub hybrid: bool,
}

impl Default for LookupCacheConfig {
    fn default() -> Self {
        Self {
            size_bytes: 100 * 1024 * 1024, // 100 MB
            ttl: Duration::from_secs(300),  // 5 minutes
            hybrid: false,
        }
    }
}

/// [[pipeline]] section: SQL pipeline definition.
#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfig {
    /// Unique name for this pipeline.
    pub name: String,

    /// SQL query defining the pipeline logic.
    /// Supports full LaminarDB SQL dialect (windows, joins, EMIT clause).
    pub sql: String,

    /// Optional parallelism override (default: server worker count).
    pub parallelism: Option<usize>,
}

/// [[sink]] section: output sink definition.
#[derive(Debug, Clone, Deserialize)]
pub struct SinkConfig {
    /// Unique name for this sink.
    pub name: String,

    /// Pipeline this sink reads from.
    pub pipeline: String,

    /// Connector type: "kafka", "postgres", "delta_lake", "iceberg", "stdout".
    pub connector: String,

    /// Delivery guarantee: "at_least_once", "exactly_once".
    #[serde(default = "default_delivery")]
    pub delivery: String,

    /// Connector-specific properties.
    #[serde(default)]
    pub properties: toml::Table,
}

/// [discovery] section: constellation node discovery.
#[derive(Debug, Clone, Deserialize)]
pub struct DiscoverySection {
    /// Discovery strategy: "static", "dns", "gossip".
    pub strategy: String,

    /// Seed node addresses for initial cluster bootstrap.
    #[serde(default)]
    pub seeds: Vec<String>,

    /// Port for gossip protocol communication.
    #[serde(default = "default_gossip_port")]
    pub gossip_port: u16,
}

/// [coordination] section: constellation coordination.
#[derive(Debug, Clone, Deserialize)]
pub struct CoordinationSection {
    /// Coordination strategy: "raft".
    #[serde(default = "default_coordination_strategy")]
    pub strategy: String,

    /// Port for Raft RPC communication.
    #[serde(default = "default_raft_port")]
    pub raft_port: u16,

    /// Raft election timeout range.
    #[serde(default = "default_election_timeout", with = "humantime_serde")]
    pub election_timeout: Duration,

    /// Raft heartbeat interval.
    #[serde(default = "default_heartbeat_interval", with = "humantime_serde")]
    pub heartbeat_interval: Duration,
}

/// Configuration errors with structured context.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// Failed to read the configuration file.
    #[error("failed to read config file '{}': {source}", path.display())]
    FileRead {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    /// TOML parse error.
    #[error("failed to parse config file '{}': {source}", path.display())]
    ParseError {
        path: std::path::PathBuf,
        source: toml::de::Error,
    },

    /// Missing required environment variables.
    #[error("missing environment variables: {}", vars.join(", "))]
    MissingEnvVars { vars: Vec<String> },

    /// Referential integrity or semantic validation failures.
    #[error("configuration validation errors:\n{}", errors.join("\n  - "))]
    ValidationErrors { errors: Vec<String> },
}

// Default value functions for serde
fn default_mode() -> String { "embedded".to_string() }
fn default_bind() -> String { "127.0.0.1:8080".to_string() }
fn default_metrics_bind() -> String { "127.0.0.1:9090".to_string() }
fn default_log_level() -> String { "info".to_string() }
fn default_state_backend() -> String { "memory".to_string() }
fn default_state_path() -> String { "./data/state".to_string() }
fn default_checkpoint_url() -> String { "file:///tmp/laminardb/checkpoints".to_string() }
fn default_checkpoint_interval() -> Duration { Duration::from_secs(10) }
fn default_checkpoint_mode() -> String { "aligned".to_string() }
fn default_snapshot_strategy() -> String { "full".to_string() }
fn default_format() -> String { "json".to_string() }
fn default_max_ooo() -> Duration { Duration::from_secs(5) }
fn default_lookup_strategy() -> String { "poll".to_string() }
fn default_true() -> bool { true }
fn default_cache_size() -> u64 { 100 * 1024 * 1024 }
fn default_cache_ttl() -> Duration { Duration::from_secs(300) }
fn default_delivery() -> String { "at_least_once".to_string() }
fn default_gossip_port() -> u16 { 7946 }
fn default_coordination_strategy() -> String { "raft".to_string() }
fn default_raft_port() -> u16 { 7947 }
fn default_election_timeout() -> Duration { Duration::from_millis(1500) }
fn default_heartbeat_interval() -> Duration { Duration::from_millis(300) }
```

### Algorithm/Flow

#### Configuration Loading Flow

```
1. CLI parses --config path (default: "laminardb.toml")
2. load_config(path) is called:
   a. Read file to string
   b. Regex scan for ${VAR_NAME} and ${VAR_NAME:-default}
      - Resolve each against std::env::var()
      - Collect errors for missing vars without defaults
   c. Parse substituted TOML string into ServerConfig
      - serde applies #[serde(default)] for missing fields
   d. Validate referential integrity:
      - Each sink.pipeline exists in pipelines[]
      - No duplicate names within sources, pipelines, sinks, lookups
      - Bind addresses are parseable as SocketAddr
      - Constellation fields present if mode == "constellation"
   e. Return validated ServerConfig
3. On error: print structured error message with file path, line number (if available), and suggestion
```

### Example TOML: Embedded Mode

```toml
# laminardb.toml -- Embedded mode (single node, in-process)

[server]
mode = "embedded"
bind = "127.0.0.1:8080"
metrics_bind = "127.0.0.1:9090"
workers = 0           # auto-detect CPU count
log_level = "info"

[state]
backend = "memory"
path = "./data/state"

[checkpoint]
url = "file:///tmp/laminardb/checkpoints"
interval = "10s"
mode = "aligned"
snapshot_strategy = "full"

[[source]]
name = "market_trades"
connector = "kafka"
format = "json"
[source.properties]
brokers = "${KAFKA_BROKERS:-localhost:9092}"
topic = "trades"
group_id = "laminardb-trades"
start_offset = "latest"
[[source.schema]]
name = "symbol"
type = "VARCHAR"
nullable = false
[[source.schema]]
name = "price"
type = "DOUBLE"
nullable = false
[[source.schema]]
name = "quantity"
type = "BIGINT"
nullable = false
[[source.schema]]
name = "trade_time"
type = "TIMESTAMP"
nullable = false
[source.watermark]
column = "trade_time"
max_out_of_orderness = "5s"

[[lookup]]
name = "instruments"
connector = "postgres"
strategy = "poll"
pushdown = true
[lookup.properties]
connection_string = "${POSTGRES_URL:-postgres://localhost/market_data}"
table = "instruments"
poll_interval = "60s"
[lookup.cache]
size_bytes = 104857600  # 100 MB
ttl = "5m"
hybrid = false

[[pipeline]]
name = "vwap_1m"
sql = """
SELECT
    t.symbol,
    SUM(t.price * t.quantity) / SUM(t.quantity) AS vwap,
    SUM(t.quantity) AS total_volume,
    COUNT(*) AS trade_count,
    i.exchange,
    i.currency
FROM market_trades t
JOIN instruments i ON t.symbol = i.symbol
GROUP BY
    TUMBLE(t.trade_time, INTERVAL '1' MINUTE),
    t.symbol, i.exchange, i.currency
EMIT AFTER WATERMARK
"""

[[sink]]
name = "vwap_to_kafka"
pipeline = "vwap_1m"
connector = "kafka"
delivery = "at_least_once"
[sink.properties]
brokers = "${KAFKA_BROKERS:-localhost:9092}"
topic = "vwap_results"

[[sink]]
name = "vwap_to_delta"
pipeline = "vwap_1m"
connector = "delta_lake"
delivery = "exactly_once"
[sink.properties]
table_uri = "s3://data-lake/vwap"
partition_columns = ["symbol"]
```

### Example TOML: Constellation Mode

```toml
# laminardb-constellation.toml -- Multi-node constellation mode

node_id = "star-1"

[server]
mode = "constellation"
bind = "0.0.0.0:8080"
metrics_bind = "0.0.0.0:9090"
workers = 8

[state]
backend = "mmap"
path = "/data/laminardb/state"
max_size_bytes = 10737418240  # 10 GB

[checkpoint]
url = "s3://laminardb-checkpoints/prod"
interval = "30s"
mode = "aligned"
snapshot_strategy = "fork_cow"

[discovery]
strategy = "static"
seeds = ["star-1.laminardb.svc:7946", "star-2.laminardb.svc:7946", "star-3.laminardb.svc:7946"]
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
[source.properties]
brokers = "${KAFKA_BROKERS}"
topic = "orders"
group_id = "laminardb-orders"
schema_registry = "${SCHEMA_REGISTRY_URL}"

[[pipeline]]
name = "order_enrichment"
sql = """
SELECT o.*, c.name AS customer_name, c.tier
FROM orders o
JOIN customers c ON o.customer_id = c.id
"""
parallelism = 8

[[sink]]
name = "enriched_orders"
pipeline = "order_enrichment"
connector = "kafka"
delivery = "exactly_once"
[sink.properties]
brokers = "${KAFKA_BROKERS}"
topic = "enriched_orders"
```

### Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `ConfigError::FileRead` | Config file not found or unreadable | Print error with path, suggest checking file exists and permissions |
| `ConfigError::ParseError` | Malformed TOML syntax | Print TOML error with line/column from `toml::de::Error` |
| `ConfigError::MissingEnvVars` | `${VAR}` references unset env var with no default | List all missing vars, suggest setting them or using `:-default` syntax |
| `ConfigError::ValidationErrors` | Referential integrity failures | List all validation errors (not just first), suggest fixes |

## Performance Targets

| Metric | Target | Measurement |
|--------|--------|-------------|
| Config parse time (small file) | < 1ms | Unit test with timer |
| Config parse time (large file, 100 pipelines) | < 10ms | Benchmark |
| Env var substitution | < 100us | Regex is compiled once |
| Validation pass | < 1ms | Linear scan of config |
| Memory for parsed config | < 64KB typical | Heap profiling |

## Test Plan

### Unit Tests

- [ ] `test_parse_minimal_config` -- Only `[server]` section, all defaults applied
- [ ] `test_parse_full_embedded_config` -- Complete embedded mode config
- [ ] `test_parse_full_constellation_config` -- Complete constellation mode config
- [ ] `test_env_var_substitution_resolves` -- `${VAR}` replaced with env value
- [ ] `test_env_var_substitution_with_default` -- `${VAR:-default}` uses default when unset
- [ ] `test_env_var_substitution_missing_errors` -- Missing var without default produces error
- [ ] `test_validate_sink_references_missing_pipeline` -- Validation catches dangling reference
- [ ] `test_validate_duplicate_source_names` -- Duplicate source names caught
- [ ] `test_validate_duplicate_pipeline_names` -- Duplicate pipeline names caught
- [ ] `test_validate_invalid_bind_address` -- Unparseable bind address caught
- [ ] `test_default_values_applied` -- All serde defaults produce expected values
- [ ] `test_checkpoint_duration_parsing` -- "10s", "1m", "500ms" all parse correctly
- [ ] `test_watermark_config_parsing` -- Watermark column and max_out_of_orderness parsed
- [ ] `test_lookup_cache_defaults` -- Default cache size (100MB) and TTL (5m) applied
- [ ] `test_constellation_mode_requires_discovery` -- Validation error if mode=constellation without [discovery]
- [ ] `test_source_schema_parsing` -- Column types parsed correctly
- [ ] `test_config_error_display_messages` -- All error variants produce readable messages

### Integration Tests

- [ ] `test_load_config_from_temp_file` -- Write TOML to temp file, load, verify
- [ ] `test_load_config_file_not_found` -- Returns FileRead error with path
- [ ] `test_env_var_substitution_end_to_end` -- Set env var, write config, load, verify substitution
- [ ] `test_config_roundtrip_serialization` -- Parse config, serialize back to TOML, parse again, compare

### Benchmarks

- [ ] `bench_parse_small_config` -- 10-line config parse time
- [ ] `bench_parse_large_config` -- 100-pipeline config parse time
- [ ] `bench_env_var_substitution` -- 50 variables in a config

## Rollout Plan

1. **Phase 1**: Define all config structs with serde derives and default values
2. **Phase 2**: Implement `load_config()` with env var substitution
3. **Phase 3**: Implement `validate_config()` with referential integrity checks
4. **Phase 4**: Unit tests for all config combinations
5. **Phase 5**: Integration tests with temp files
6. **Phase 6**: Wire into `main.rs` replacing the TODO comment
7. **Phase 7**: Documentation and example configs
8. **Phase 8**: Code review and merge

## Open Questions

- [ ] Should we support TOML includes/imports (e.g., `@include "sources.toml"`) for large deployments?
- [ ] Should `humantime_serde` be used for duration parsing, or a custom deserializer?
- [ ] Should connector-specific properties use `toml::Table` (dynamic) or typed sub-structs per connector?
- [ ] Should we validate SQL syntax at config parse time (requires DataFusion) or defer to engine construction?
- [ ] Should constellation mode config be in the same file or a separate overlay file?

## Completion Checklist

- [ ] All config structs defined with serde derives
- [ ] `load_config()` implemented with env var substitution
- [ ] `validate_config()` implemented with referential integrity
- [ ] Default value functions for all optional fields
- [ ] `ConfigError` enum with all variants
- [ ] Unit tests passing (>80% coverage)
- [ ] Integration tests passing
- [ ] Example TOML files for embedded and constellation modes
- [ ] Wired into `main.rs`
- [ ] Documentation updated (`#![deny(missing_docs)]`)
- [ ] CHANGELOG updated
- [ ] Code reviewed
- [ ] Merged to main
- [ ] Feature INDEX.md updated

## References

- [TOML Specification](https://toml.io/en/) -- Configuration file format
- [serde documentation](https://serde.rs/) -- Serialization framework
- [F-SERVER-002: Engine Construction](F-SERVER-002-engine-construction.md) -- Consumes parsed config
- [F-SERVER-005: Constellation Mode](F-SERVER-005-constellation-mode.md) -- Extended config for multi-node
- [crates/laminar-server/src/main.rs](../../../../crates/laminar-server/src/main.rs) -- Existing CLI skeleton

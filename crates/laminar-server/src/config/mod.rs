//! TOML configuration parsing for LaminarDB server.
//!
//! Supports `${VAR}` and `${VAR:-default}` environment variable substitution.

use std::collections::HashSet;
use std::path::Path;
use std::sync::LazyLock;
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use laminar_core::checkpoint::object_store_builder::CheckpointStorageScope;
use laminar_core::state::{KeyGroupCount, DEFAULT_KEY_GROUP_COUNT};
use laminar_db::DeliveryGuarantee;
use regex::Regex;
use serde::Deserialize;

mod validation;

use validation::validate_config;
pub(crate) use validation::validate_http_auth;

/// Regex for `${VAR}` and `${VAR:-default}` patterns.
static ENV_VAR_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}").expect("valid regex")
});

/// NIST baseline; MD5 has no work factor, so length is the only knob.
const MIN_PGWIRE_PASSWORD_LEN: usize = 12;

/// Minimum length for the console bearer token (HTTP control-plane auth).
const MIN_CONSOLE_TOKEN_LEN: usize = 8;

/// Both HTTP bearer credentials use a full 256 bits when observer access is enabled.
const HTTP_AUTH_TOKEN_BYTES: usize = 32;

/// Unpadded base64url length of a 32-byte credential.
const HTTP_AUTH_TOKEN_ENCODED_LEN: usize = 43;

/// Load, parse, and validate a LaminarDB configuration file.
pub fn load_config(path: &Path) -> Result<ServerConfig, ConfigError> {
    let raw = std::fs::read_to_string(path).map_err(|e| ConfigError::FileRead {
        path: path.to_path_buf(),
        source: e,
    })?;

    let substituted = substitute_env_vars(&raw)?;
    let config: ServerConfig = toml::from_str(&substituted).map_err(|mut source| {
        // `toml::de::Error` retains and renders its input. The substituted document can
        // contain credentials, so detach it before the error crosses this boundary.
        source.set_input(None);
        ConfigError::ParseError {
            path: path.to_path_buf(),
            source,
        }
    })?;

    validate_config(&config)?;
    Ok(config)
}

/// Substitute `${VAR}` and `${VAR:-default}` patterns with environment values.
/// `$${VAR}` escapes substitution and becomes the literal `${VAR}` used by cluster DDL.
fn substitute_env_vars(input: &str) -> Result<String, ConfigError> {
    const ESCAPED_OPEN: &str = "\0LAMINAR_ENV_OPEN\0";
    let escaped = input.replace("$${", ESCAPED_OPEN);
    let mut errors = Vec::new();
    let result = ENV_VAR_RE.replace_all(&escaped, |caps: &regex::Captures| {
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

    Ok(result.replace(ESCAPED_OPEN, "${"))
}

/// Top-level server configuration deserialized from `laminardb.toml`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default)]
    pub server: ServerSection,
    #[serde(default)]
    pub checkpoint: CheckpointSection,
    /// `[supervision]` — auto-restart policy on a fatal fault (single-node only).
    #[serde(default)]
    pub supervision: SupervisionSection,
    #[serde(default, rename = "source")]
    pub sources: Vec<SourceConfig>,
    #[serde(default, rename = "lookup")]
    pub lookups: Vec<LookupConfig>,
    #[serde(default, rename = "pipeline")]
    pub pipelines: Vec<PipelineConfig>,
    #[serde(default, rename = "sink")]
    pub sinks: Vec<SinkConfig>,
    /// Raw SQL DDL executed before `start()`, as an alternative to structured sections.
    #[serde(default)]
    pub sql: Option<String>,
    pub discovery: Option<DiscoverySection>,
    pub node_id: Option<String>,
    /// `[ai]` — AI provider wiring and per-task default models.
    #[serde(default)]
    pub ai: AiSection,
    /// `[models.<name>]` — the AI model registry.
    #[serde(default)]
    pub models: std::collections::HashMap<String, ModelConfig>,
}

/// Runtime protocol boundary selected by `[server].mode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServerMode {
    /// Standalone single-node server.
    #[default]
    Single,
    /// Multi-node runtime with discovery, durable leases, and shared state.
    Cluster,
}

/// `[server]` section.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerSection {
    #[serde(default)]
    pub mode: ServerMode,
    /// Stable hash partitions used by every deployment mode.
    #[serde(default)]
    pub key_groups: Option<KeyGroupCount>,
    #[serde(default = "default_bind")]
    pub bind: String,
    /// End-to-end pipeline delivery contract. Connector protocols are derived from this value.
    #[serde(default = "default_delivery")]
    pub delivery: DeliveryGuarantee,
    /// Query execution policy for keyed running aggregates; independent of checkpoint storage.
    #[serde(default = "default_incremental_emit")]
    pub incremental_emit: bool,
    /// Right-side history retained while a temporal join input is idle.
    #[serde(default, with = "humantime_serde")]
    pub temporal_join_idle_history_retention: Option<Duration>,
    /// Mark inactive watermarked sources and input channels idle after this duration.
    #[serde(default, with = "humantime_serde")]
    pub source_idle_timeout: Option<Duration>,
    /// Event timestamps farther ahead of wall clock do not advance source watermarks.
    /// Zero disables the guard.
    #[serde(
        default = "default_event_time_max_future_skew",
        with = "humantime_serde"
    )]
    pub event_time_max_future_skew: Duration,
    /// Postgres wire bind address; `None` disables it.
    #[serde(default)]
    pub pgwire_bind: Option<String>,
    /// MD5 auth users. Empty → trust auth (loopback only).
    #[serde(default)]
    pub pgwire_users: std::collections::HashMap<String, Secret>,
    /// Required true to bind pgwire on a non-loopback address; remote binds also require TLS.
    #[serde(default)]
    pub pgwire_allow_remote: bool,
    /// PEM cert; pair with `pgwire_tls_key` to enable TLS.
    #[serde(default)]
    pub pgwire_tls_cert: Option<std::path::PathBuf>,
    /// PEM private key (PKCS#8 or RSA).
    #[serde(default)]
    pub pgwire_tls_key: Option<std::path::PathBuf>,
    /// PEM CA bundle; requires every client to present a cert chained to these roots (mTLS).
    #[serde(default)]
    pub pgwire_tls_client_ca: Option<std::path::PathBuf>,
    /// Concurrent session cap; excess accepts close immediately.
    #[serde(default = "default_pgwire_max_connections")]
    pub pgwire_max_connections: usize,
    /// Per-IP auth-failure cap in a 60s rolling window. 0 disables.
    #[serde(default = "default_pgwire_max_auth_failures_per_min")]
    pub pgwire_max_auth_failures_per_min: u32,
    /// Minimum TLS protocol version: `"1.2"` (default) or `"1.3"`.
    #[serde(default = "default_pgwire_tls_min_version")]
    pub pgwire_tls_min_version: String,
    /// Bearer token gating the HTTP console API; `None` leaves it unauthenticated (loopback/dev only).
    #[serde(default)]
    pub console_token: Option<Secret>,
    /// Read-only bearer token for cluster diagnostics; enables the split diagnostic boundary.
    #[serde(default)]
    pub diagnostic_read_token: Option<Secret>,
    /// CORS allow-list of console origins; `None` falls back to a permissive policy (dev only).
    #[serde(default)]
    pub console_cors_allowed_origins: Option<Vec<String>>,
}

fn default_pgwire_max_connections() -> usize {
    256
}

fn default_pgwire_max_auth_failures_per_min() -> u32 {
    10
}

fn default_pgwire_tls_min_version() -> String {
    "1.2".to_string()
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            mode: ServerMode::default(),
            key_groups: None,
            bind: default_bind(),
            delivery: default_delivery(),
            incremental_emit: default_incremental_emit(),
            temporal_join_idle_history_retention: None,
            source_idle_timeout: None,
            event_time_max_future_skew: default_event_time_max_future_skew(),
            pgwire_bind: None,
            pgwire_users: std::collections::HashMap::new(),
            pgwire_allow_remote: false,
            pgwire_tls_cert: None,
            pgwire_tls_key: None,
            pgwire_tls_client_ca: None,
            pgwire_max_connections: default_pgwire_max_connections(),
            pgwire_max_auth_failures_per_min: default_pgwire_max_auth_failures_per_min(),
            pgwire_tls_min_version: default_pgwire_tls_min_version(),
            console_token: None,
            diagnostic_read_token: None,
            console_cors_allowed_origins: None,
        }
    }
}

impl ServerSection {
    /// Configured key-group topology, or the common deployment default.
    #[must_use]
    pub(crate) fn resolved_key_groups(&self) -> KeyGroupCount {
        self.key_groups.unwrap_or(DEFAULT_KEY_GROUP_COUNT)
    }

    pub(crate) fn validated_temporal_join_idle_history_retention(
        &self,
    ) -> Result<Option<Duration>, &'static str> {
        let Some(retention) = self.temporal_join_idle_history_retention else {
            return Ok(None);
        };
        let retention_ms = i64::try_from(retention.as_millis()).map_err(|_| {
            "temporal_join_idle_history_retention exceeds the supported millisecond range"
        })?;
        if retention_ms == 0 {
            return Err("temporal_join_idle_history_retention must be at least 1ms");
        }
        Ok(Some(retention))
    }

    pub(crate) fn validated_source_idle_timeout(&self) -> Result<Option<Duration>, &'static str> {
        let Some(timeout) = self.source_idle_timeout else {
            return Ok(None);
        };
        let timeout_ms = u64::try_from(timeout.as_millis())
            .map_err(|_| "source_idle_timeout exceeds the supported millisecond range")?;
        if timeout_ms == 0 {
            return Err("source_idle_timeout must be at least 1ms");
        }
        Ok(Some(Duration::from_millis(timeout_ms)))
    }

    pub(crate) fn validated_event_time_max_future_skew(&self) -> Result<Duration, &'static str> {
        let skew_ms = i64::try_from(self.event_time_max_future_skew.as_millis())
            .map_err(|_| "event_time_max_future_skew exceeds the supported millisecond range")?;
        if !self.event_time_max_future_skew.is_zero() && skew_ms == 0 {
            return Err("event_time_max_future_skew must be zero or at least 1ms");
        }
        Ok(Duration::from_millis(skew_ms.unsigned_abs()))
    }
}

fn default_event_time_max_future_skew() -> Duration {
    Duration::from_millis(laminar_core::time::DEFAULT_MAX_FUTURE_SKEW_MS.unsigned_abs())
}

/// `[supervision]` — auto-restart policy; unset fields fall back to engine defaults.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
pub struct SupervisionSection {
    pub max_restarts: Option<usize>,
    pub window_secs: Option<u64>,
    pub initial_backoff_ms: Option<u64>,
    pub max_backoff_secs: Option<u64>,
}

impl SupervisionSection {
    /// Resolve into a [`laminar_db::RestartPolicy`], applying defaults for unset fields.
    pub fn to_policy(&self) -> laminar_db::RestartPolicy {
        let mut p = laminar_db::RestartPolicy::default();
        if let Some(v) = self.max_restarts {
            p.max_restarts = v;
        }
        if let Some(v) = self.window_secs {
            p.window = std::time::Duration::from_secs(v);
        }
        if let Some(v) = self.initial_backoff_ms {
            p.initial_backoff = std::time::Duration::from_millis(v);
        }
        if let Some(v) = self.max_backoff_secs {
            p.max_backoff = std::time::Duration::from_secs(v);
        }
        p
    }
}

/// String that redacts itself in `Debug` output.
#[derive(Clone, PartialEq, Eq, Deserialize)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    #[cfg(test)]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.chars().count()
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[REDACTED]")
    }
}

/// `[checkpoint]` section.
#[derive(Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointSection {
    /// Storage URL: file:///path, s3://bucket/prefix, gs://bucket/prefix.
    #[serde(default = "default_checkpoint_url")]
    pub url: String,
    #[serde(default = "default_checkpoint_interval", with = "humantime_serde")]
    pub interval: Duration,
    /// One end-to-end checkpoint-attempt deadline.
    #[serde(default = "default_checkpoint_timeout", with = "humantime_serde")]
    pub timeout: Duration,
    /// Cloud storage credentials/config (e.g., `aws_access_key_id`).
    #[serde(default)]
    pub storage: std::collections::HashMap<String, String>,
    /// Cap on captured-state bytes held by in-flight epochs awaiting
    /// upload; admission pauses at the cap. Default 512 MiB.
    #[serde(default)]
    pub max_node_data_bytes: Option<u64>,
}

impl Default for CheckpointSection {
    fn default() -> Self {
        Self {
            url: default_checkpoint_url(),
            interval: default_checkpoint_interval(),
            timeout: default_checkpoint_timeout(),
            storage: std::collections::HashMap::new(),
            max_node_data_bytes: None,
        }
    }
}

// Manual Debug: `storage` values can hold cloud secrets.
impl std::fmt::Debug for CheckpointSection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointSection")
            .field("url", &self.url)
            .field("interval", &self.interval)
            .field("timeout", &self.timeout)
            .field(
                "storage",
                &self
                    .storage
                    .keys()
                    .map(|k| (k, "[REDACTED]"))
                    .collect::<Vec<_>>(),
            )
            .field("max_node_data_bytes", &self.max_node_data_bytes)
            .finish()
    }
}

fn default_ai_max_concurrency() -> usize {
    8
}

/// `[ai]` — provider wiring and per-task defaults; models live in the top-level `[models.*]` tables.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
pub struct AiSection {
    /// `[ai.providers.<name>]` — transport endpoints.
    #[serde(default)]
    pub providers: std::collections::HashMap<String, ProviderConfig>,
    /// `[ai.defaults]` — task name → default model name (e.g. `classify = "finbert"`).
    #[serde(default)]
    pub defaults: std::collections::HashMap<String, String>,
}

/// `[ai.providers.<name>]`.
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
pub struct ProviderConfig {
    /// Transport kind: `anthropic`, `openai`, or `local`. Inferred from the
    /// provider name when omitted (for the canonical names).
    #[serde(default)]
    pub kind: Option<String>,
    /// Name of the environment variable holding the API key (remote providers).
    /// The key itself is never stored in config.
    #[serde(default)]
    pub api_key_env: Option<String>,
    /// Base URL (remote). Defaults per kind when omitted.
    #[serde(default)]
    pub base_url: Option<String>,
    /// Maximum concurrent requests issued per batch (remote).
    #[serde(default = "default_ai_max_concurrency")]
    pub max_concurrency: usize,
    /// Steady requests-per-second cap (remote); paced by a token bucket. Unset = no limit.
    #[serde(default)]
    pub requests_per_second: Option<u32>,
    /// Model cache directory or `object_store` URI (local provider).
    #[serde(default)]
    pub cache_dir: Option<String>,
}

/// `[models.<name>]`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ModelConfig {
    /// `local` or `remote`.
    pub kind: String,
    /// One task (`task = "classify"`) or several (`task = ["classify", "extract"]`).
    pub task: TaskSpec,
    /// Remote: the provider name (a key in `[ai.providers]`).
    #[serde(default)]
    pub provider: Option<String>,
    /// Remote: the provider-specific model id.
    #[serde(default)]
    pub model: Option<String>,
    /// Local: the weight source (`hf:org/repo` or a file/`object_store` URI).
    #[serde(default)]
    pub source: Option<String>,
}

/// A model's task list, written as a single string or an array.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum TaskSpec {
    /// A single task.
    One(String),
    /// Several tasks.
    Many(Vec<String>),
}

impl TaskSpec {
    /// The task names as a list.
    #[must_use]
    pub fn tasks(&self) -> Vec<String> {
        match self {
            TaskSpec::One(t) => vec![t.clone()],
            TaskSpec::Many(ts) => ts.clone(),
        }
    }
}

/// Control-plane mTLS is all-or-nothing: cert, key, client_ca, and server_name
/// must be set together, and each path must exist.
fn validate_cluster_tls(config: &ServerConfig, errors: &mut Vec<String>) {
    if config.server.mode != ServerMode::Cluster {
        return;
    }
    let Some(d) = &config.discovery else {
        return;
    };
    let configured = [
        d.cluster_tls_cert.is_some(),
        d.cluster_tls_key.is_some(),
        d.cluster_tls_client_ca.is_some(),
        d.cluster_tls_server_name.is_some(),
    ];
    if !configured.iter().any(|is_set| *is_set) {
        return;
    }
    let complete = configured.iter().all(|is_set| *is_set)
        && d.cluster_tls_server_name
            .as_ref()
            .is_some_and(|name| cluster_tls_server_name_is_valid(name));
    if !complete {
        errors.push(
            "cluster_tls requires cluster_tls_cert, cluster_tls_key, \
             cluster_tls_client_ca, and cluster_tls_server_name together"
                .to_string(),
        );
        return;
    }
    for (label, path) in [
        ("cluster_tls_cert", &d.cluster_tls_cert),
        ("cluster_tls_key", &d.cluster_tls_key),
        ("cluster_tls_client_ca", &d.cluster_tls_client_ca),
    ] {
        if let Some(p) = path {
            if !p.exists() {
                errors.push(format!("{label} not found: {}", p.display()));
            }
        }
    }
}

pub(crate) fn cluster_tls_server_name_is_valid(name: &str) -> bool {
    !name.is_empty()
        && name.trim() == name
        && tokio_rustls::rustls::pki_types::ServerName::try_from(name.to_string()).is_ok()
}

/// Structural validation of `[ai]`/`[models]`; semantic checks happen when the registry is built.
fn validate_ai(config: &ServerConfig, errors: &mut Vec<String>) {
    for (name, model) in &config.models {
        match model.kind.as_str() {
            "remote" => {
                match &model.provider {
                    Some(p) if config.ai.providers.contains_key(p) => {}
                    Some(p) => errors.push(format!("model '{name}': unknown provider '{p}'")),
                    None => {
                        errors.push(format!(
                            "model '{name}': remote model requires a 'provider'"
                        ));
                    }
                }
                if model.model.is_none() {
                    errors.push(format!(
                        "model '{name}': remote model requires a 'model' id"
                    ));
                }
            }
            "local" => {
                if model.source.is_none() {
                    errors.push(format!("model '{name}': local model requires a 'source'"));
                }
            }
            other => errors.push(format!(
                "model '{name}': kind must be 'local' or 'remote', got '{other}'"
            )),
        }
        if model.task.tasks().is_empty() {
            errors.push(format!("model '{name}': at least one task is required"));
        }
    }

    for (name, provider) in &config.ai.providers {
        // Mirror runtime kind resolution (explicit `kind`, else provider name) so validation
        // matches how the provider is actually built.
        let kind = provider.kind.as_deref().unwrap_or(name.as_str());
        if kind == "local" {
            // Without a cache_dir no LocalProvider can be built.
            if provider.cache_dir.is_none() {
                errors.push(format!(
                    "provider '{name}': local provider requires a 'cache_dir'"
                ));
            }
        } else if provider.api_key_env.is_none() {
            errors.push(format!(
                "provider '{name}': remote provider requires 'api_key_env'"
            ));
        }
    }

    for (task, model_name) in &config.ai.defaults {
        if !config.models.contains_key(model_name) {
            errors.push(format!(
                "ai.defaults.{task} references unknown model '{model_name}'"
            ));
        }
    }
}

/// `[[source]]` section.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub name: String,
    /// Connector type: "kafka", "postgres-cdc", "mongodb-cdc", "generator".
    pub connector: String,
    #[serde(default = "default_format")]
    pub format: String,
    #[serde(default)]
    pub properties: toml::Table,
    #[serde(default)]
    pub schema: Vec<ColumnDef>,
    #[serde(default)]
    pub primary_key: Vec<String>,
    pub watermark: Option<WatermarkConfig>,
}

/// Column definition within a source or lookup schema.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ColumnDef {
    pub name: String,
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

/// `[[lookup]]` section: lookup table for enrichment joins.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LookupConfig {
    pub name: String,
    /// Connector type: "postgres", "redis", "csv".
    pub connector: String,
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
#[serde(deny_unknown_fields)]
pub struct SinkConfig {
    pub name: String,
    pub pipeline: String,
    /// Connector type: "kafka", "postgres", "delta-lake", "iceberg", "stdout".
    pub connector: String,
    /// Optional serialization format, emitted as a `FORMAT` clause.
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub properties: toml::Table,
}

/// `[discovery]` section: cluster node discovery.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoverySection {
    pub strategy: String,
    #[serde(default)]
    pub seeds: Vec<String>,
    #[serde(default = "default_gossip_port")]
    pub gossip_port: u16,
    #[serde(default)]
    pub advertise_host: Option<String>,
    /// Failure-domain locality, gossiped to peers. A flat label (`"rack17"`)
    /// or coarsest-first `;`-separated tiers (`"region=...;zone=...;rack=..."`).
    #[serde(default)]
    pub failure_domain: Option<String>,
    /// Locality tier the placement/blast-radius metrics group by (0 = coarsest).
    #[serde(default)]
    pub placement_isolation_tier: usize,
    /// Optional PEM cert for control-plane (barrier/shuffle) mTLS; when set,
    /// key + client_ca + server_name must also be set.
    #[serde(default)]
    pub cluster_tls_cert: Option<std::path::PathBuf>,
    /// PEM private key paired with `cluster_tls_cert`.
    #[serde(default)]
    pub cluster_tls_key: Option<std::path::PathBuf>,
    /// CA that signed every peer cert; verifies both directions (mTLS).
    #[serde(default)]
    pub cluster_tls_client_ca: Option<std::path::PathBuf>,
    /// DNS SAN present in every node cert, verified on connect (peers dial by IP).
    #[serde(default)]
    pub cluster_tls_server_name: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file '{}': {source}", path.display())]
    FileRead {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse config file '{}': {source}", path.display())]
    ParseError {
        path: std::path::PathBuf,
        source: toml::de::Error,
    },
    #[error("missing environment variables: {}", vars.join(", "))]
    MissingEnvVars { vars: Vec<String> },
    #[error("configuration validation errors:\n  - {}", errors.join("\n  - "))]
    ValidationErrors { errors: Vec<String> },
}

fn default_bind() -> String {
    "127.0.0.1:8080".to_string()
}
fn default_checkpoint_url() -> String {
    let base = std::env::temp_dir();
    let path = base.join("laminardb");
    let path_str = path.to_string_lossy().replace('\\', "/");
    if path_str.starts_with('/') {
        format!("file://{path_str}")
    } else {
        format!("file:///{path_str}")
    }
}
fn default_incremental_emit() -> bool {
    true
}
fn default_checkpoint_interval() -> Duration {
    Duration::from_secs(10)
}
fn default_checkpoint_timeout() -> Duration {
    Duration::from_secs(120)
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
fn default_delivery() -> DeliveryGuarantee {
    DeliveryGuarantee::AtLeastOnce
}
fn default_gossip_port() -> u16 {
    7946
}
// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;

//! TOML configuration parsing for LaminarDB server.
//!
//! Supports `${VAR}` and `${VAR:-default}` environment variable substitution.

use std::collections::HashSet;
use std::path::Path;
use std::sync::LazyLock;
use std::time::Duration;

use laminar_core::state::StateBackendConfig;
use regex::Regex;
use serde::Deserialize;

/// Regex for `${VAR}` and `${VAR:-default}` patterns.
static ENV_VAR_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}").expect("valid regex")
});

/// NIST baseline; MD5 has no work factor, so length is the only knob.
const MIN_PGWIRE_PASSWORD_LEN: usize = 12;

/// Minimum length for the console bearer token (HTTP control-plane auth).
const MIN_CONSOLE_TOKEN_LEN: usize = 8;

/// Load, parse, and validate a LaminarDB configuration file.
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

/// Substitute `${VAR}` and `${VAR:-default}` patterns with environment values.
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
    if let Some(addr) = &config.server.pgwire_bind {
        if addr.parse::<std::net::SocketAddr>().is_err() {
            errors.push(format!("invalid server pgwire_bind address: '{}'", addr));
        }
    }
    for (user, password) in &config.server.pgwire_users {
        if user.is_empty() {
            errors.push("pgwire_users contains an empty username".to_string());
        }
        let pw = password.expose();
        if let Some(rest) = pw.strip_prefix("md5") {
            // pg_authid-style pre-hash: 'md5' + lowercase-hex(md5(password ‖ user)).
            // Strict shape so a typo isn't silently treated as plaintext.
            let valid =
                rest.len() == 32 && rest.chars().all(|c| matches!(c, '0'..='9' | 'a'..='f'));
            if !valid {
                errors.push(format!(
                    "pgwire_users['{user}']: pre-hashed value must be 'md5' \
                     followed by 32 lowercase hex characters"
                ));
            }
        } else if password.len() < MIN_PGWIRE_PASSWORD_LEN {
            errors.push(format!(
                "pgwire_users['{user}']: password must be at least {MIN_PGWIRE_PASSWORD_LEN} characters"
            ));
        }
    }
    if config.server.pgwire_max_connections == 0 {
        errors.push(
            "pgwire_max_connections must be > 0; remove pgwire_bind to disable the listener"
                .to_string(),
        );
    }
    match (
        &config.server.pgwire_tls_cert,
        &config.server.pgwire_tls_key,
    ) {
        (Some(_), None) | (None, Some(_)) => {
            errors.push("pgwire_tls_cert and pgwire_tls_key must be set together".to_string());
        }
        (Some(cert), Some(key)) => {
            if !cert.exists() {
                errors.push(format!("pgwire_tls_cert not found: {}", cert.display()));
            }
            if !key.exists() {
                errors.push(format!("pgwire_tls_key not found: {}", key.display()));
            }
        }
        (None, None) => {}
    }
    match config.server.pgwire_tls_min_version.as_str() {
        "1.2" | "1.3" => {}
        other => errors.push(format!(
            "pgwire_tls_min_version must be \"1.2\" or \"1.3\" (got \"{other}\")"
        )),
    }
    if let Some(ca) = &config.server.pgwire_tls_client_ca {
        if config.server.pgwire_tls_cert.is_none() {
            errors.push(
                "pgwire_tls_client_ca requires pgwire_tls_cert + pgwire_tls_key (mTLS \
                 layers on top of server TLS)"
                    .to_string(),
            );
        }
        if !ca.exists() {
            errors.push(format!("pgwire_tls_client_ca not found: {}", ca.display()));
        }
    }

    if let Some(token) = &config.server.console_token {
        if token.len() < MIN_CONSOLE_TOKEN_LEN {
            errors.push(format!(
                "server.console_token must be at least {MIN_CONSOLE_TOKEN_LEN} characters"
            ));
        }
    }

    // Each CORS origin becomes an `Access-Control-Allow-Origin` header value;
    // reject anything that isn't a valid HTTP header value (e.g. control
    // characters) before it reaches the router.
    if let Some(origins) = &config.server.console_cors_allowed_origins {
        for origin in origins {
            if origin.parse::<axum::http::HeaderValue>().is_err() {
                errors.push(format!(
                    "invalid origin in server.console_cors_allowed_origins: '{}'",
                    origin
                ));
            }
        }
    }

    // Validate: cluster mode requires discovery and coordination
    if config.server.mode == "cluster" {
        if config.discovery.is_none() {
            errors.push("mode = \"cluster\" requires a [discovery] section".to_string());
        }
        if config.coordination.is_none() {
            errors.push("mode = \"cluster\" requires a [coordination] section".to_string());
        }
        if config.node_id.is_none() {
            errors.push("mode = \"cluster\" requires node_id to be set".to_string());
        }
        // Distributed 2PC has a per-barrier cost of ~1-3s (manifest
        // persist + durability gate + sink commit). Cadences tighter
        // than 2s spend more than half their time on coordination.
        if config.checkpoint.interval < Duration::from_secs(2) {
            errors.push(format!(
                "mode = \"cluster\": checkpoint.interval = {:?} is too tight; minimum is 2s",
                config.checkpoint.interval,
            ));
        }
    }

    validate_ai(config, &mut errors);
    validate_cluster_tls(config, &mut errors);

    if !errors.is_empty() {
        return Err(ConfigError::ValidationErrors { errors });
    }

    Ok(())
}

/// Top-level server configuration deserialized from `laminardb.toml`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ServerConfig {
    #[serde(default)]
    pub server: ServerSection,
    #[serde(default)]
    pub state: StateBackendConfig,
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
    /// Raw SQL DDL executed before `start()`, as an alternative to structured sections.
    #[serde(default)]
    pub sql: Option<String>,
    pub discovery: Option<DiscoverySection>,
    pub coordination: Option<CoordinationSection>,
    pub node_id: Option<String>,
    /// `[ai]` — AI provider wiring and per-task default models.
    #[serde(default)]
    pub ai: AiSection,
    /// `[models.<name>]` — the AI model registry (top-level, per the contract).
    #[serde(default)]
    pub models: std::collections::HashMap<String, ModelConfig>,
}

/// `[server]` section.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ServerSection {
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default = "default_bind")]
    pub bind: String,
    /// Postgres wire bind address; `None` disables it.
    #[serde(default)]
    pub pgwire_bind: Option<String>,
    /// MD5 auth users. Empty → trust auth (loopback only).
    #[serde(default)]
    pub pgwire_users: std::collections::HashMap<String, Secret>,
    /// Required true to bind pgwire on a non-loopback address.
    #[serde(default)]
    pub pgwire_allow_remote: bool,
    /// PEM cert; pair with `pgwire_tls_key` to enable TLS.
    #[serde(default)]
    pub pgwire_tls_cert: Option<std::path::PathBuf>,
    /// PEM private key (PKCS#8 or RSA).
    #[serde(default)]
    pub pgwire_tls_key: Option<std::path::PathBuf>,
    /// PEM CA bundle. Setting this requires every connecting client to
    /// present a certificate chained to one of these roots (mTLS).
    #[serde(default)]
    pub pgwire_tls_client_ca: Option<std::path::PathBuf>,
    /// Concurrent session cap; excess accepts close immediately.
    #[serde(default = "default_pgwire_max_connections")]
    pub pgwire_max_connections: usize,
    /// Per-IP auth-failure cap in a 60s rolling window. 0 disables.
    #[serde(default = "default_pgwire_max_auth_failures_per_min")]
    pub pgwire_max_auth_failures_per_min: u32,
    /// Minimum TLS protocol version: `"1.2"` (default) or `"1.3"`. Pinning
    /// to `"1.3"` is the PCI-DSS / FedRAMP-High posture; rustls already
    /// disables TLS 1.0/1.1 unconditionally.
    #[serde(default = "default_pgwire_tls_min_version")]
    pub pgwire_tls_min_version: String,
    /// Bearer token gating the HTTP control-plane (console) API. `None`
    /// leaves the HTTP API unauthenticated — loopback/dev only.
    #[serde(default)]
    pub console_token: Option<Secret>,
    /// CORS allow-list of console origins. `None` falls back to the legacy
    /// permissive policy (dev only); set this before exposing the console.
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
            mode: default_mode(),
            bind: default_bind(),
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
            console_cors_allowed_origins: None,
        }
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
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CheckpointSection {
    /// Storage URL: file:///path, s3://bucket/prefix, gs://bucket/prefix.
    #[serde(default = "default_checkpoint_url")]
    pub url: String,
    #[serde(default = "default_checkpoint_interval", with = "humantime_serde")]
    pub interval: Duration,
    /// Number of recent checkpoints to retain before pruning.
    #[serde(default = "default_max_retained")]
    pub max_retained: usize,
    /// Cloud storage credentials/config (e.g., `aws_access_key_id`).
    #[serde(default)]
    pub storage: std::collections::HashMap<String, String>,
}

impl Default for CheckpointSection {
    fn default() -> Self {
        Self {
            url: default_checkpoint_url(),
            interval: default_checkpoint_interval(),
            max_retained: default_max_retained(),
            storage: std::collections::HashMap::new(),
        }
    }
}

fn default_ai_max_concurrency() -> usize {
    8
}

/// `[ai]` — provider wiring and per-task defaults. Models live in the top-level
/// `[models.*]` tables, per the configuration contract.
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
    /// Steady requests-per-second cap (remote). When set, calls are paced by a
    /// token bucket — bursts are shaped, not sent unbounded. Unset = no limit.
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

/// Structural validation of the `[ai]` / `[models]` config — references resolve
/// and required fields are present. Semantic checks (task names, label seam)
/// happen when the registry is built.
/// Control-plane mTLS is all-or-nothing: cert, key, client_ca, and server_name
/// must be set together, and each path must exist.
fn validate_cluster_tls(config: &ServerConfig, errors: &mut Vec<String>) {
    let Some(d) = &config.discovery else {
        return;
    };
    let set = [
        d.cluster_tls_cert.is_some(),
        d.cluster_tls_key.is_some(),
        d.cluster_tls_client_ca.is_some(),
        d.cluster_tls_server_name.is_some(),
    ];
    if !set.iter().any(|s| *s) {
        return; // TLS disabled
    }
    if !set.iter().all(|s| *s) {
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
        // Mirror runtime kind resolution exactly (explicit `kind`, else the
        // provider name) so validation can't disagree with how the provider is
        // actually built — a `cache_dir` on a remote provider must not excuse a
        // missing key.
        let kind = provider.kind.as_deref().unwrap_or(name.as_str());
        if kind == "local" {
            // A local provider must carry a cache_dir, or no LocalProvider can be
            // built and local models would fail at runtime — reject it now.
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
pub struct SourceConfig {
    pub name: String,
    /// Connector type: "kafka", "postgres_cdc", "mysql_cdc", "generator".
    pub connector: String,
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
    /// Connector type: "postgres", "mysql", "redis", "csv".
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
pub struct SinkConfig {
    pub name: String,
    pub pipeline: String,
    /// Connector type: "kafka", "postgres", "delta-lake", "iceberg", "stdout".
    pub connector: String,
    #[serde(default = "default_delivery")]
    pub delivery: String,
    #[serde(default)]
    pub properties: toml::Table,
}

/// `[discovery]` section: delta node discovery.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DiscoverySection {
    pub strategy: String,
    #[serde(default)]
    pub seeds: Vec<String>,
    #[serde(default = "default_gossip_port")]
    pub gossip_port: u16,
    #[serde(default)]
    pub advertise_host: Option<String>,
    /// PEM cert for control-plane (barrier/query/shuffle) mTLS; enable by
    /// setting cert + key + client_ca + server_name together.
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

/// `[coordination]` section: delta coordination.
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

fn default_mode() -> String {
    "embedded".to_string()
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

    const AI_TOML: &str = r#"
[server]

[ai.providers.anthropic]
api_key_env = "LAMINAR_ANTHROPIC_API_KEY"
base_url = "https://api.anthropic.com"
max_concurrency = 8

[ai.providers.openai]
api_key_env = "LAMINAR_OPENAI_API_KEY"
base_url = "https://api.openai.com/v1"

[ai.providers.local]
cache_dir = "/var/lib/laminar/models"

[models.finbert]
kind = "local"
source = "hf:onnx-community/finbert"
task = "classify"

[models.haiku]
kind = "remote"
provider = "anthropic"
model = "claude-haiku-4-5-20251001"
task = ["classify", "extract", "complete"]

[ai.defaults]
classify = "finbert"
complete = "haiku"
"#;

    #[test]
    fn parses_ai_section_and_models() {
        let config: ServerConfig = toml::from_str(AI_TOML).unwrap();
        assert_eq!(config.ai.providers.len(), 3);
        assert_eq!(
            config.ai.providers["anthropic"].api_key_env.as_deref(),
            Some("LAMINAR_ANTHROPIC_API_KEY")
        );
        assert_eq!(config.ai.providers["openai"].max_concurrency, 8);
        assert_eq!(
            config.ai.providers["local"].cache_dir.as_deref(),
            Some("/var/lib/laminar/models")
        );
        assert_eq!(config.models["finbert"].task.tasks(), vec!["classify"]);
        assert_eq!(
            config.models["haiku"].task.tasks(),
            vec!["classify", "extract", "complete"]
        );
        assert_eq!(config.ai.defaults["classify"], "finbert");
        validate_config(&config).unwrap();
    }

    #[test]
    fn rejects_local_provider_without_cache_dir() {
        let toml = r#"
[server]
[ai.providers.local]
[models.m]
kind = "local"
source = "hf:x/y"
task = "classify"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn rejects_unknown_provider_and_default() {
        let toml = r#"
[server]
[ai.providers.anthropic]
api_key_env = "K"
[models.bad]
kind = "remote"
provider = "ghost"
model = "x"
task = "classify"
[ai.defaults]
classify = "missing"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        let msg = format!("{err:?}");
        assert!(msg.contains("unknown provider 'ghost'"), "{msg}");
        assert!(msg.contains("unknown model 'missing'"), "{msg}");
    }

    #[test]
    fn rejects_remote_provider_without_api_key_env() {
        let toml = r#"
[server]
[ai.providers.openai]
base_url = "http://localhost:8000/v1"
[models.m]
kind = "remote"
provider = "openai"
model = "x"
task = "embed"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        assert!(format!("{err:?}").contains("requires 'api_key_env'"));
    }

    #[test]
    fn local_model_requires_source() {
        let toml = r#"
[server]
[models.m]
kind = "local"
task = "classify"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        assert!(format!("{err:?}").contains("requires a 'source'"));
    }

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

[state]
backend = "in_process"

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
mode = "cluster"
bind = "0.0.0.0:8080"

[state]
backend = "local"
path = "/data/state"
vnode_capacity = 256

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
        assert_eq!(config.server.mode, "cluster");
        assert!(matches!(config.state, StateBackendConfig::Local { .. }));
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
    fn test_cluster_mode_rejects_tight_checkpoint_interval() {
        // Two-phase commit in cluster mode can't keep up with sub-2s
        // cadence.
        let toml = r#"
node_id = "n1"

[server]
mode = "cluster"

[checkpoint]
interval = "500ms"

[discovery]
strategy = "static"
seeds = ["x:1"]

[coordination]
strategy = "raft"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        match err {
            ConfigError::ValidationErrors { errors } => {
                assert!(
                    errors.iter().any(|e| e.contains("too tight")),
                    "expected tight-interval error, got: {errors:?}",
                );
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
    fn test_validate_zero_max_connections() {
        let toml = r#"
[server]
pgwire_max_connections = 0
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        match err {
            ConfigError::ValidationErrors { errors } => {
                assert!(
                    errors.iter().any(|e| e.contains("must be > 0")),
                    "errors: {errors:?}"
                );
            }
            _ => panic!("expected ValidationErrors"),
        }
    }

    #[test]
    fn test_validate_client_ca_requires_server_cert() {
        let toml = r#"
[server]
pgwire_tls_client_ca = "/does/not/matter.pem"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        match err {
            ConfigError::ValidationErrors { errors } => {
                assert!(
                    errors
                        .iter()
                        .any(|e| e.contains("requires pgwire_tls_cert")),
                    "errors: {errors:?}"
                );
            }
            _ => panic!("expected ValidationErrors"),
        }
    }

    #[test]
    fn test_validate_rejects_unknown_tls_min_version() {
        let toml = r#"
[server]
pgwire_tls_min_version = "1.4"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        match err {
            ConfigError::ValidationErrors { errors } => {
                assert!(
                    errors.iter().any(|e| e.contains("pgwire_tls_min_version")),
                    "errors: {errors:?}"
                );
            }
            _ => panic!("expected ValidationErrors"),
        }
    }

    #[test]
    fn test_validate_accepts_well_formed_pre_hashed_pgwire_password() {
        let toml = r#"
[server]
[server.pgwire_users]
alice = "md55d41402abc4b2a76b9719d911017c592"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        // 35-char pre-hashed value bypasses the MIN_PGWIRE_PASSWORD_LEN gate.
        validate_config(&config).expect("well-formed pre-hash must validate");
    }

    #[test]
    fn test_validate_rejects_malformed_pre_hashed_pgwire_password() {
        // 'md5' prefix followed by non-hex — clearly meant to be pre-hashed
        // but malformed; rejected so a typo doesn't slip through as plaintext.
        let toml = r#"
[server]
[server.pgwire_users]
alice = "md5zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        match err {
            ConfigError::ValidationErrors { errors } => {
                assert!(
                    errors.iter().any(|e| e.contains("pre-hashed")),
                    "errors: {errors:?}",
                );
            }
            _ => panic!("expected ValidationErrors"),
        }
    }

    #[test]
    fn test_validate_short_pgwire_password() {
        let toml = r#"
[server]
[server.pgwire_users]
alice = "short"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        match err {
            ConfigError::ValidationErrors { errors } => {
                assert!(
                    errors.iter().any(|e| e.contains("at least 12 characters")),
                    "errors: {errors:?}"
                );
            }
            _ => panic!("expected ValidationErrors"),
        }
    }

    #[test]
    fn test_validate_short_console_token() {
        let toml = r#"
[server]
console_token = "abc"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        match err {
            ConfigError::ValidationErrors { errors } => {
                assert!(
                    errors
                        .iter()
                        .any(|e| e.contains("server.console_token must be at least 8 characters")),
                    "errors: {errors:?}"
                );
            }
            _ => panic!("expected ValidationErrors"),
        }
    }

    #[test]
    fn test_validate_accepts_well_formed_console_token() {
        let toml = r#"
[server]
console_token = "supersecret-token"
console_cors_allowed_origins = ["https://console.example.com", "http://localhost:5173"]
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        validate_config(&config).expect("8+ char console token must validate");
        assert_eq!(config.server.console_token.as_ref().unwrap().len(), 17);
        assert_eq!(
            config.server.console_cors_allowed_origins,
            Some(vec![
                "https://console.example.com".to_string(),
                "http://localhost:5173".to_string(),
            ])
        );
    }

    #[test]
    fn test_console_token_redacted_in_debug() {
        let toml = r#"
[server]
console_token = "supersecret-token"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        validate_config(&config).unwrap();
        let dump = format!("{:?}", config.server);
        assert!(!dump.contains("supersecret"), "secret leaked: {dump}");
        assert!(
            dump.contains("REDACTED"),
            "expected REDACTED marker: {dump}"
        );
    }

    #[test]
    fn test_validate_invalid_cors_origin() {
        // A control character (bell, U+0007) in the origin makes it an invalid
        // HTTP header value, so config validation must reject it. TOML basic
        // strings can't carry a raw control byte, so the field is set in Rust.
        let toml = r#"
[server]
"#;
        let mut config: ServerConfig = toml::from_str(toml).unwrap();
        config.server.console_cors_allowed_origins =
            Some(vec!["http://e\u{0007}vil.example.com".to_string()]);
        let err = validate_config(&config).unwrap_err();
        match err {
            ConfigError::ValidationErrors { errors } => {
                assert!(
                    errors.iter().any(
                        |e| e.contains("invalid origin in server.console_cors_allowed_origins")
                    ),
                    "errors: {errors:?}"
                );
            }
            _ => panic!("expected ValidationErrors"),
        }
    }

    #[test]
    fn test_console_auth_defaults_to_none() {
        let config = ServerSection::default();
        assert!(config.console_token.is_none());
        assert!(config.console_cors_allowed_origins.is_none());
    }

    #[test]
    fn test_validate_pgwire_password_redacted_in_debug() {
        let toml = r#"
[server]
[server.pgwire_users]
alice = "wonderland-key"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        validate_config(&config).unwrap();
        let dump = format!("{:?}", config.server);
        assert!(!dump.contains("wonderland"), "secret leaked: {dump}");
        assert!(
            dump.contains("REDACTED"),
            "expected REDACTED marker: {dump}"
        );
    }

    #[test]
    fn test_default_values_applied() {
        let config = ServerConfig {
            server: ServerSection::default(),
            state: StateBackendConfig::default(),
            checkpoint: CheckpointSection::default(),
            sources: vec![],
            lookups: vec![],
            pipelines: vec![],
            sinks: vec![],
            discovery: None,
            coordination: None,
            node_id: None,
            sql: None,
            ai: Default::default(),
            models: Default::default(),
        };

        assert_eq!(config.server.mode, "embedded");
        assert_eq!(config.server.bind, "127.0.0.1:8080");
        assert!(matches!(config.state, StateBackendConfig::InProcess { .. }));
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
    fn test_cluster_mode_requires_discovery() {
        let toml = r#"
[server]
mode = "cluster"

[checkpoint]
interval = "10s"
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

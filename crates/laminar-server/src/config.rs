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

/// Validate the startup-bound HTTP authentication boundary.
///
/// This is separate from full file validation so programmatic startup paths can enforce the
/// same credential and bind invariants before creating any externally visible resources.
pub(crate) fn validate_http_auth(config: &ServerConfig) -> Result<(), ConfigError> {
    let mut errors = Vec::new();
    collect_http_auth_errors(config, &mut errors);
    if errors.is_empty() {
        Ok(())
    } else {
        Err(ConfigError::ValidationErrors { errors })
    }
}

fn collect_http_auth_errors(config: &ServerConfig, errors: &mut Vec<String>) {
    let bind = match config.server.bind.parse::<std::net::SocketAddr>() {
        Ok(bind) => Some(bind),
        Err(_) => {
            errors.push(format!(
                "invalid server bind address: '{}'",
                config.server.bind
            ));
            None
        }
    };

    let Some(diagnostic_token) = &config.server.diagnostic_read_token else {
        if let Some(console_token) = &config.server.console_token {
            if console_token.len() < MIN_CONSOLE_TOKEN_LEN {
                errors.push(format!(
                    "server.console_token must be at least {MIN_CONSOLE_TOKEN_LEN} characters"
                ));
            }
        }
        return;
    };

    if config.server.mode != ServerMode::Cluster {
        errors.push("server.diagnostic_read_token requires server.mode = \"cluster\"".to_string());
    }

    match bind {
        Some(bind) if !bind.ip().is_loopback() => errors.push(
            "server.diagnostic_read_token requires server.bind to be a loopback socket address"
                .to_string(),
        ),
        Some(_) | None => {}
    }

    if !is_canonical_http_auth_token(diagnostic_token) {
        errors.push(format!(
            "server.diagnostic_read_token must be the canonical unpadded base64url encoding of \
             exactly {HTTP_AUTH_TOKEN_BYTES} bytes ({HTTP_AUTH_TOKEN_ENCODED_LEN} characters)"
        ));
    }

    match &config.server.console_token {
        None => errors.push(
            "server.diagnostic_read_token requires server.console_token to be configured"
                .to_string(),
        ),
        Some(console_token) => {
            if !is_canonical_http_auth_token(console_token) {
                errors.push(format!(
                    "server.console_token must be the canonical unpadded base64url encoding of \
                     exactly {HTTP_AUTH_TOKEN_BYTES} bytes ({HTTP_AUTH_TOKEN_ENCODED_LEN} \
                     characters) when server.diagnostic_read_token is configured"
                ));
            }
            if console_token == diagnostic_token {
                errors.push(
                    "server.diagnostic_read_token must differ from server.console_token"
                        .to_string(),
                );
            }
        }
    }
}

fn is_canonical_http_auth_token(token: &Secret) -> bool {
    let encoded = token.expose();
    if encoded.len() != HTTP_AUTH_TOKEN_ENCODED_LEN {
        return false;
    }

    match URL_SAFE_NO_PAD.decode(encoded) {
        Ok(decoded) if decoded.len() == HTTP_AUTH_TOKEN_BYTES => {
            URL_SAFE_NO_PAD.encode(decoded) == encoded
        }
        Ok(_) | Err(_) => false,
    }
}

fn validate_config(config: &ServerConfig) -> Result<(), ConfigError> {
    let mut errors = Vec::new();

    let pipeline_names: HashSet<&str> = config.pipelines.iter().map(|p| p.name.as_str()).collect();

    for sink in &config.sinks {
        if !pipeline_names.contains(sink.pipeline.as_str()) {
            errors.push(format!(
                "sink '{}' references unknown pipeline '{}'",
                sink.name, sink.pipeline
            ));
        }
        if sink
            .properties
            .keys()
            .any(|key| key.eq_ignore_ascii_case("format"))
        {
            errors.push(format!(
                "sink '{}': format must be configured as a top-level sink field, not under sink.properties",
                sink.name
            ));
        }
        collect_connector_property_errors("sink", &sink.name, &sink.properties, &mut errors);
    }

    let mut seen_sources = HashSet::new();
    for source in &config.sources {
        if !seen_sources.insert(&source.name) {
            errors.push(format!("duplicate source name: '{}'", source.name));
        }
        if source
            .properties
            .keys()
            .any(|key| key.eq_ignore_ascii_case("format"))
        {
            errors.push(format!(
                "source '{}': format must be configured as a top-level source field, not under source.properties",
                source.name
            ));
        }
        collect_connector_property_errors("source", &source.name, &source.properties, &mut errors);
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

    collect_http_auth_errors(config, &mut errors);

    if let Some(addr) = &config.server.pgwire_bind {
        match addr.parse::<std::net::SocketAddr>() {
            Ok(addr) if !addr.ip().is_loopback() && config.server.pgwire_tls_cert.is_none() => {
                errors.push(
                    "non-loopback pgwire_bind requires pgwire_tls_cert + pgwire_tls_key"
                        .to_string(),
                );
            }
            Ok(_) => {}
            Err(_) => errors.push(format!("invalid server pgwire_bind address: '{}'", addr)),
        }
    }
    for (user, password) in &config.server.pgwire_users {
        if user.is_empty() {
            errors.push("pgwire_users contains an empty username".to_string());
        }
        let pw = password.expose();
        if let Some(rest) = pw.strip_prefix("md5") {
            // pg_authid-style pre-hash; strict shape so a typo isn't treated as plaintext.
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

    // CORS origins become `Access-Control-Allow-Origin` values; reject invalid header values.
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

    if config.server.mode == ServerMode::Cluster {
        match config.server.delivery {
            DeliveryGuarantee::BestEffort => errors.push(
                "cluster mode requires at_least_once delivery; best_effort has no defined \
                 rebalance/state-loss contract"
                    .to_string(),
            ),
            DeliveryGuarantee::AtLeastOnce | DeliveryGuarantee::ExactlyOnce => {}
        }
        if config.discovery.is_none() {
            errors.push("mode = \"cluster\" requires a [discovery] section".to_string());
        }
        if config.node_id.is_none() {
            errors.push("mode = \"cluster\" requires node_id to be set".to_string());
        }
        // Below 100ms the capture-quorum round-trip itself dominates the barrier.
        if config.checkpoint.interval < Duration::from_millis(100) {
            errors.push(format!(
                "mode = \"cluster\": checkpoint.interval = {:?} is too tight; minimum is 100ms",
                config.checkpoint.interval,
            ));
        }
        let checkpoint_scope = CheckpointStorageScope::for_url(&config.checkpoint.url);
        if checkpoint_scope != CheckpointStorageScope::ClusterShared {
            errors.push(format!(
                "mode = \"cluster\" requires ClusterShared [checkpoint] storage for manifests \
                 and decisions; configured scope is {checkpoint_scope:?}. Use s3://, gs://, or \
                 az:// storage"
            ));
        }
    } else if config.server.delivery == DeliveryGuarantee::ExactlyOnce {
        if !config.checkpoint.url.starts_with("file://") {
            errors.push(
                "[LDB-0014] embedded/single-node exactly-once currently requires a local \
                 file:// checkpoint namespace protected by an exclusive process lock; shared \
                 object-store checkpoints require a term-fenced deployment lease"
                    .to_string(),
            );
        }
        let checkpoint_scope = CheckpointStorageScope::for_url(&config.checkpoint.url);
        if checkpoint_scope == CheckpointStorageScope::Volatile {
            errors.push(format!(
                "exactly-once delivery requires at least NodeDurable [checkpoint] storage; \
                 configured scope is {checkpoint_scope:?}"
            ));
        }
    } else if config.server.delivery == DeliveryGuarantee::AtLeastOnce {
        let checkpoint_scope = CheckpointStorageScope::for_url(&config.checkpoint.url);
        if checkpoint_scope == CheckpointStorageScope::Volatile {
            errors.push(format!(
                "at-least-once delivery requires at least NodeDurable [checkpoint] storage \
                 before source acknowledgements can advance; configured scope is \
                 {checkpoint_scope:?}"
            ));
        }
    }
    // 0 would pause barrier admission permanently, silently wedging checkpointing.
    if config.checkpoint.interval.is_zero() {
        errors.push("checkpoint.interval must be > 0".to_string());
    }
    if config.checkpoint.timeout.is_zero() {
        errors.push("checkpoint.timeout must be > 0".to_string());
    }
    if config.checkpoint.max_node_data_bytes == Some(0) {
        errors.push("checkpoint.max_node_data_bytes must be > 0".to_string());
    }
    if config.checkpoint.max_retained == 0 {
        errors.push("checkpoint.max_retained must be > 0".to_string());
    }
    // 0 prunes every prior timestamp, so the restart-rate budget never trips (unbounded restart loop).
    if config.supervision.window_secs == Some(0) {
        errors.push("supervision.window_secs must be > 0".to_string());
    }

    validate_ai(config, &mut errors);
    validate_cluster_tls(config, &mut errors);
    if !errors.is_empty() {
        return Err(ConfigError::ValidationErrors { errors });
    }

    Ok(())
}

fn collect_connector_property_errors(
    kind: &str,
    name: &str,
    properties: &toml::Table,
    errors: &mut Vec<String>,
) {
    for (key, value) in properties {
        if !connector_property_is_flat(value) {
            errors.push(format!(
                "{kind} '{name}': property '{key}' is nested; connector properties must be flat (quote dotted keys such as \"bootstrap.servers\")"
            ));
        }
    }
}

fn connector_property_is_flat(value: &toml::Value) -> bool {
    match value {
        toml::Value::Table(_) => false,
        toml::Value::Array(values) => values.iter().all(connector_property_is_flat),
        _ => true,
    }
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
    /// Number of predecessor checkpoints retained alongside the current recovery cut.
    #[serde(default = "default_max_retained")]
    pub max_retained: usize,
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
            max_retained: default_max_retained(),
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
            .field("max_retained", &self.max_retained)
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
fn default_max_retained() -> usize {
    10
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
mod tests {
    use super::*;

    #[test]
    fn shipped_server_configs_deserialize() {
        for (name, input) in [
            ("root", include_str!("../../../laminardb.toml")),
            (
                "minimal",
                include_str!("../../../examples/laminardb-minimal.toml"),
            ),
            (
                "standalone",
                include_str!("../../../examples/laminardb.toml"),
            ),
            (
                "cluster",
                include_str!("../../../examples/laminardb-cluster.toml"),
            ),
            (
                "binance-1",
                include_str!("../../../examples/binance-cluster-node1.toml"),
            ),
            (
                "binance-2",
                include_str!("../../../examples/binance-cluster-node2.toml"),
            ),
            (
                "bluesky-firehose",
                include_str!("../../../examples/bluesky-firehose/laminar.toml"),
            ),
            (
                "bluesky-news",
                include_str!("../../../examples/bluesky-news/laminar.toml"),
            ),
            (
                "aiops",
                include_str!("../../../examples/claude-code-aiops/config.toml"),
            ),
            (
                "iceberg",
                include_str!("../../../examples/kafka-iceberg-timeseries/laminar.toml"),
            ),
            (
                "nats",
                include_str!("../../../examples/nats-payments/config.toml"),
            ),
            (
                "server-demo",
                include_str!("../../../examples/server-demo/laminardb.toml"),
            ),
        ] {
            toml::from_str::<ServerConfig>(input)
                .unwrap_or_else(|error| panic!("{name} config does not deserialize: {error}"));
        }
    }

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

    fn canonical_http_auth_secret(byte: u8) -> Secret {
        Secret::new(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([byte; 32]))
    }

    fn diagnostic_auth_config(
        diagnostic_read_token: Secret,
        console_token: Option<Secret>,
    ) -> ServerConfig {
        let mut config: ServerConfig = toml::from_str("[server]\n").unwrap();
        config.server.mode = ServerMode::Cluster;
        config.server.bind = "127.0.0.1:8080".to_string();
        config.server.console_token = console_token;
        config.server.diagnostic_read_token = Some(diagnostic_read_token);
        config
    }

    fn http_auth_errors(config: &ServerConfig) -> Vec<String> {
        match validate_http_auth(config).unwrap_err() {
            ConfigError::ValidationErrors { errors } => errors,
            error => panic!("expected validation errors, got {error:?}"),
        }
    }

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
        assert_eq!(config.server.mode, ServerMode::Single);
        assert_eq!(config.server.bind, "127.0.0.1:8080");
        assert!(config.server.incremental_emit);
        assert_eq!(config.server.delivery, DeliveryGuarantee::AtLeastOnce);
        assert!(config.sources.is_empty());
        assert!(config.pipelines.is_empty());
        assert!(config.sinks.is_empty());
    }

    #[test]
    fn parse_error_does_not_retain_substituted_input() {
        const SENTINEL: &str = "LDB_PARSE_SECRET_SENTINEL_4f8757d46e";
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("invalid-secret.toml");
        let input =
            format!("[server]\nconsole_token = ${{LDB_PARSE_REDACTION_TEST_TOKEN:-{SENTINEL}}}\n");
        std::fs::write(&path, input).unwrap();

        let error = load_config(&path).expect_err("the unquoted substituted token is invalid TOML");
        assert!(matches!(&error, ConfigError::ParseError { .. }));
        assert!(!error.to_string().contains(SENTINEL));
        assert!(!format!("{error:?}").contains(SENTINEL));

        let mut source = std::error::Error::source(&error);
        assert!(
            source.is_some(),
            "parse errors must retain their typed source"
        );
        while let Some(cause) = source {
            assert!(!cause.to_string().contains(SENTINEL));
            assert!(!format!("{cause:?}").contains(SENTINEL));
            source = cause.source();
        }
    }

    #[test]
    fn test_server_mode_rejects_unknown_values() {
        let error = toml::from_str::<ServerConfig>("[server]\nmode = \"cluser\"\n")
            .expect_err("a mistyped runtime mode must not fall back to single-node mode");
        let message = error.to_string();
        assert!(message.contains("unknown variant"), "{message}");
        assert!(
            message.contains("single") && message.contains("cluster"),
            "{message}"
        );
        assert!(!message.contains("embedded"), "{message}");
    }

    #[test]
    fn test_server_mode_rejects_retired_embedded_value() {
        let error = toml::from_str::<ServerConfig>("[server]\nmode = \"embedded\"\n")
            .expect_err("the standalone server mode is named single");
        let message = error.to_string();
        assert!(message.contains("unknown variant"), "{message}");
        assert!(
            message.contains("single") && message.contains("cluster"),
            "{message}"
        );
    }

    #[test]
    fn test_removed_coordination_section_is_rejected() {
        let error = toml::from_str::<ServerConfig>(
            "[server]\nmode = \"cluster\"\n[coordination]\nstrategy = \"raft\"\n",
        )
        .expect_err("removed coordination settings must not be silently ignored");
        assert!(error.to_string().contains("unknown field"), "{error}");
        assert!(error.to_string().contains("coordination"), "{error}");
    }

    #[test]
    fn test_parse_full_single_config() {
        let toml = r#"
[server]
mode = "single"
bind = "127.0.0.1:8080"

[checkpoint]
url = "file:///tmp/checkpoints"
interval = "10s"

[[source]]
name = "trades"
connector = "kafka"
format = "json"
[source.properties]
"bootstrap.servers" = "localhost:9092"
"group.id" = "laminardb-trades"
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
format = "json"
[sink.properties]
"bootstrap.servers" = "localhost:9092"
topic = "vwap_output"
"#;

        let config: ServerConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.sources.len(), 1);
        assert_eq!(config.sources[0].name, "trades");
        assert_eq!(
            config.sources[0]
                .properties
                .get("bootstrap.servers")
                .and_then(toml::Value::as_str),
            Some("localhost:9092")
        );
        assert_eq!(
            config.sources[0]
                .properties
                .get("group.id")
                .and_then(toml::Value::as_str),
            Some("laminardb-trades")
        );
        assert_eq!(config.sources[0].schema.len(), 2);
        assert!(!config.sources[0].schema[0].nullable);
        assert!(config.sources[0].schema[1].nullable); // default true
        assert!(config.sources[0].watermark.is_some());
        assert_eq!(config.pipelines.len(), 1);
        assert_eq!(config.sinks.len(), 1);
        assert_eq!(config.sinks[0].pipeline, "vwap");
        assert_eq!(config.sinks[0].format.as_deref(), Some("json"));
        assert_eq!(
            config.sinks[0]
                .properties
                .get("bootstrap.servers")
                .and_then(toml::Value::as_str),
            Some("localhost:9092")
        );

        validate_config(&config).unwrap();
    }

    #[test]
    fn test_format_is_not_a_connector_property() {
        let config: ServerConfig = toml::from_str(
            r#"
[[source]]
name = "input"
connector = "kafka"
[source.properties]
FoRmAt = "json"

[[pipeline]]
name = "events"
sql = "SELECT 1"

[[sink]]
name = "output"
pipeline = "events"
connector = "kafka"
[sink.properties]
format = "json"
"#,
        )
        .unwrap();

        let error = validate_config(&config).unwrap_err().to_string();
        assert!(error.contains("top-level source field"), "{error}");
        assert!(error.contains("top-level sink field"), "{error}");

        let error = toml::from_str::<ServerConfig>(
            r#"
[[source]]
name = "input"
connector = "kafka"
formt = "json"
"#,
        )
        .expect_err("misspelled source runtime fields must fail closed");
        assert!(error.to_string().contains("formt"), "{error}");

        let config: ServerConfig = toml::from_str(
            r#"
[[source]]
name = "input"
connector = "kafka"
[source.properties]
bootstrap.servers = "localhost:9092"
"#,
        )
        .unwrap();
        let error = validate_config(&config).unwrap_err().to_string();
        assert!(error.contains("quote dotted keys"), "{error}");
    }

    #[test]
    fn test_parse_full_cluster_config() {
        let toml = r#"
node_id = "star-1"

[server]
mode = "cluster"
bind = "0.0.0.0:8080"
delivery = "at_least_once"
key_groups = 256

[checkpoint]
url = "s3://bucket/checkpoints"
interval = "30s"

[discovery]
strategy = "static"
seeds = ["node-1:7946", "node-2:7946"]
gossip_port = 7946
failure_domain = "region=us-east-1;zone=us-east-1a;rack=r17"
placement_isolation_tier = 1

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
"#;

        let config: ServerConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.node_id.as_deref(), Some("star-1"));
        assert_eq!(config.server.mode, ServerMode::Cluster);
        assert_eq!(config.server.delivery, DeliveryGuarantee::AtLeastOnce);
        assert_eq!(config.server.resolved_key_groups().get(), 256);
        assert_eq!(
            CheckpointStorageScope::for_url(&config.checkpoint.url),
            CheckpointStorageScope::ClusterShared
        );
        assert!(config.discovery.is_some());

        let disc = config.discovery.as_ref().unwrap();
        assert_eq!(
            disc.failure_domain.as_deref(),
            Some("region=us-east-1;zone=us-east-1a;rack=r17")
        );
        assert_eq!(disc.placement_isolation_tier, 1);

        validate_config(&config).unwrap();
    }

    #[test]
    fn checkpoint_storage_scope_is_fail_closed() {
        let local_exact: ServerConfig =
            toml::from_str("[server]\ndelivery = \"exactly_once\"\n").unwrap();
        validate_config(&local_exact)
            .expect("the default durable checkpoint URL is sufficient for local exactly-once");

        let local_cluster: ServerConfig = toml::from_str(
            r#"
node_id = "node-1"

[server]
mode = "cluster"

[discovery]
strategy = "static"
seeds = ["node-1:7946"]

"#,
        )
        .unwrap();
        let ConfigError::ValidationErrors { errors } = validate_config(&local_cluster).unwrap_err()
        else {
            panic!("expected validation errors");
        };
        assert!(errors
            .iter()
            .any(|error| error.contains("ClusterShared [checkpoint]")));

        let cluster_exact: ServerConfig = toml::from_str(
            r#"
node_id = "node-1"

[server]
mode = "cluster"
delivery = "exactly_once"

[discovery]
strategy = "static"
seeds = ["node-1:7946"]

"#,
        )
        .unwrap();
        let ConfigError::ValidationErrors { errors } = validate_config(&cluster_exact).unwrap_err()
        else {
            panic!("expected validation errors");
        };
        assert!(errors
            .iter()
            .any(|error| { error.contains("ClusterShared [checkpoint]") }));

        let cluster_exact_complete: ServerConfig = toml::from_str(
            r#"
node_id = "node-1"

[server]
mode = "cluster"
delivery = "exactly_once"

[checkpoint]
url = "s3://bucket/checkpoints"

[discovery]
strategy = "static"
seeds = ["node-1:7946"]

"#,
        )
        .unwrap();
        validate_config(&cluster_exact_complete)
            .expect("connector contracts, not the server mode, gate cluster exact delivery");

        let cluster_best_effort: ServerConfig = toml::from_str(
            r#"
node_id = "node-1"

[server]
mode = "cluster"
delivery = "best_effort"

[checkpoint]
url = "s3://bucket/checkpoints"

[discovery]
strategy = "static"
seeds = ["node-1:7946"]

"#,
        )
        .unwrap();
        let ConfigError::ValidationErrors { errors } =
            validate_config(&cluster_best_effort).unwrap_err()
        else {
            panic!("expected validation errors");
        };
        assert!(errors.iter().any(|error| {
            error.contains("cluster mode requires at_least_once") && error.contains("best_effort")
        }));

        let volatile_checkpoint: ServerConfig = toml::from_str(
            r#"
[server]
delivery = "at_least_once"

[checkpoint]
url = "memory://checkpoint"
"#,
        )
        .unwrap();
        let ConfigError::ValidationErrors { errors } =
            validate_config(&volatile_checkpoint).unwrap_err()
        else {
            panic!("expected validation errors");
        };
        assert!(errors.iter().any(|error| {
            error.contains("NodeDurable [checkpoint]") && error.contains("source acknowledgements")
        }));

        let shared_local_exact: ServerConfig = toml::from_str(
            r#"
[server]
delivery = "exactly_once"

[checkpoint]
url = "s3://bucket/checkpoints"
"#,
        )
        .unwrap();
        let ConfigError::ValidationErrors { errors } =
            validate_config(&shared_local_exact).unwrap_err()
        else {
            panic!("expected validation errors");
        };
        assert!(errors.iter().any(|error| error.contains("[LDB-0014]")));
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
        std::env::remove_var("LAMINAR_TEST_UNSET_VAR");
        let input = "brokers = \"${LAMINAR_TEST_UNSET_VAR:-localhost:9092}\"";
        let result = substitute_env_vars(input).unwrap();
        assert_eq!(result, "brokers = \"localhost:9092\"");
    }

    #[test]
    fn escaped_env_var_is_preserved_for_per_node_connector_resolution() {
        std::env::remove_var("LAMINAR_TEST_CONNECTOR_PASSWORD");
        let input = "password = \"$${LAMINAR_TEST_CONNECTOR_PASSWORD}\"";
        let result = substitute_env_vars(input).unwrap();
        assert_eq!(result, "password = \"${LAMINAR_TEST_CONNECTOR_PASSWORD}\"");
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
        // Below 100ms the capture-quorum round-trip itself dominates
        // the barrier.
        let toml = r#"
node_id = "n1"

[server]
mode = "cluster"

[checkpoint]
interval = "50ms"

[discovery]
strategy = "static"
seeds = ["x:1"]

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
    fn remote_cluster_plaintext_is_accepted() {
        let config: ServerConfig = toml::from_str(
            r#"
node_id = "node-1"

[server]
mode = "cluster"
bind = "0.0.0.0:8080"
delivery = "at_least_once"

[checkpoint]
url = "s3://bucket/checkpoints"

[discovery]
strategy = "static"
seeds = ["node-1:7946"]
"#,
        )
        .unwrap();
        validate_config(&config).expect("remote cluster may explicitly run without TLS");
    }

    #[test]
    fn cluster_plaintext_and_complete_mtls_are_accepted() {
        let mut config: ServerConfig = toml::from_str(
            r#"
node_id = "node-1"

[server]
mode = "cluster"
bind = "127.0.0.1:8080"
delivery = "at_least_once"

[checkpoint]
url = "s3://bucket/checkpoints"

[discovery]
strategy = "static"
seeds = ["127.0.0.1:7946"]
"#,
        )
        .unwrap();
        validate_config(&config).expect("cluster control may remain plaintext");

        let directory = tempfile::tempdir().unwrap();
        let cert = directory.path().join("node.crt");
        let key = directory.path().join("node.key");
        let ca = directory.path().join("cluster-ca.crt");
        for path in [&cert, &key, &ca] {
            std::fs::write(path, b"test material").unwrap();
        }
        config.server.bind = "0.0.0.0:8080".into();
        let discovery = config.discovery.as_mut().unwrap();
        discovery.seeds = vec!["10.0.0.2:7946".into()];
        discovery.cluster_tls_cert = Some(cert);
        discovery.cluster_tls_key = Some(key);
        discovery.cluster_tls_client_ca = Some(ca);
        discovery.cluster_tls_server_name = Some("laminardb-cluster.internal".into());
        validate_config(&config).expect("complete remote cluster mTLS should be admitted");
    }

    #[test]
    fn invalid_cluster_tls_server_name_is_not_treated_as_absent() {
        let mut config: ServerConfig = toml::from_str(
            r#"
node_id = "node-1"

[server]
mode = "cluster"
bind = "127.0.0.1:8080"
delivery = "at_least_once"

[checkpoint]
url = "s3://bucket/checkpoints"

[discovery]
strategy = "static"
seeds = ["127.0.0.1:7946"]
cluster_tls_server_name = "bad name"
"#,
        )
        .unwrap();
        let ConfigError::ValidationErrors { errors } = validate_config(&config).unwrap_err() else {
            panic!("expected validation errors");
        };
        assert!(
            errors
                .iter()
                .any(|error| error.contains("cluster_tls requires")),
            "errors: {errors:?}"
        );

        config.server.mode = ServerMode::Single;
        validate_config(&config).expect("cluster TLS settings do not affect single-node mode");
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
    fn test_validate_remote_pgwire_requires_tls() {
        let toml = r#"
[server]
pgwire_bind = "0.0.0.0:5432"
pgwire_allow_remote = true
pgwire_users = { alice = "wonderland-key" }
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        let err = validate_config(&config).unwrap_err();
        match err {
            ConfigError::ValidationErrors { errors } => {
                assert!(
                    errors
                        .iter()
                        .any(|e| e.contains("non-loopback pgwire_bind requires")),
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
    fn legacy_console_only_token_remains_compatible() {
        let config: ServerConfig = toml::from_str(
            r#"
[server]
console_token = "supersecret-token"
"#,
        )
        .unwrap();

        validate_http_auth(&config).expect("legacy console-only credentials remain valid");
    }

    #[test]
    fn diagnostic_token_requires_console_token() {
        let config = diagnostic_auth_config(canonical_http_auth_secret(1), None);
        let errors = http_auth_errors(&config);

        assert!(
            errors
                .iter()
                .any(|error| error.contains("requires server.console_token")),
            "errors: {errors:?}"
        );
    }

    #[test]
    fn diagnostic_token_rejects_weak_diagnostic_credential() {
        let config =
            diagnostic_auth_config(Secret::new("weak"), Some(canonical_http_auth_secret(2)));
        let errors = http_auth_errors(&config);

        assert!(
            errors.iter().any(|error| {
                error.contains("server.diagnostic_read_token") && error.contains("exactly 32 bytes")
            }),
            "errors: {errors:?}"
        );
    }

    #[test]
    fn diagnostic_token_rejects_noncanonical_base64url() {
        let mut noncanonical = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([3_u8; 32]);
        noncanonical.replace_range(HTTP_AUTH_TOKEN_ENCODED_LEN - 1.., "B");
        assert_eq!(noncanonical.len(), HTTP_AUTH_TOKEN_ENCODED_LEN);

        let config = diagnostic_auth_config(
            Secret::new(noncanonical),
            Some(canonical_http_auth_secret(4)),
        );
        let errors = http_auth_errors(&config);

        assert!(
            errors.iter().any(|error| {
                error.contains("server.diagnostic_read_token") && error.contains("canonical")
            }),
            "errors: {errors:?}"
        );
    }

    #[test]
    fn diagnostic_token_requires_strong_console_credential() {
        let config =
            diagnostic_auth_config(canonical_http_auth_secret(5), Some(Secret::new("weak")));
        let errors = http_auth_errors(&config);

        assert!(
            errors.iter().any(|error| {
                error.contains("server.console_token") && error.contains("exactly 32 bytes")
            }),
            "errors: {errors:?}"
        );
    }

    #[test]
    fn diagnostic_and_console_tokens_must_be_distinct() {
        let token = canonical_http_auth_secret(6);
        let config = diagnostic_auth_config(token.clone(), Some(token));
        let errors = http_auth_errors(&config);

        assert!(
            errors.iter().any(|error| error.contains("must differ")),
            "errors: {errors:?}"
        );
    }

    #[test]
    fn diagnostic_token_requires_cluster_mode() {
        let mut config = diagnostic_auth_config(
            canonical_http_auth_secret(7),
            Some(canonical_http_auth_secret(8)),
        );
        config.server.mode = ServerMode::Single;
        let errors = http_auth_errors(&config);

        assert!(
            errors
                .iter()
                .any(|error| error.contains("requires server.mode = \"cluster\"")),
            "errors: {errors:?}"
        );
    }

    #[test]
    fn diagnostic_token_requires_loopback_http_bind() {
        let mut config = diagnostic_auth_config(
            canonical_http_auth_secret(9),
            Some(canonical_http_auth_secret(10)),
        );
        config.server.bind = "0.0.0.0:8080".to_string();
        let errors = http_auth_errors(&config);

        assert!(
            errors.iter().any(|error| error.contains("loopback")),
            "errors: {errors:?}"
        );
    }

    #[test]
    fn diagnostic_token_accepts_valid_split_credentials() {
        let diagnostic = canonical_http_auth_secret(11);
        let console = canonical_http_auth_secret(12);
        let diagnostic_value = diagnostic.expose().to_string();
        let console_value = console.expose().to_string();
        let config = diagnostic_auth_config(diagnostic, Some(console));

        validate_http_auth(&config).expect("valid split diagnostic credentials must pass");
        let debug = format!("{:?}", config.server);
        assert!(
            !debug.contains(&diagnostic_value),
            "diagnostic token leaked"
        );
        assert!(!debug.contains(&console_value), "console token leaked");
    }

    #[test]
    fn file_loader_uses_the_shared_diagnostic_auth_validator() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("diagnostic-auth.toml");
        let diagnostic = canonical_http_auth_secret(13);
        let console = canonical_http_auth_secret(14);
        let invalid = format!(
            "[server]\nconsole_token = \"{}\"\ndiagnostic_read_token = \"{}\"\n",
            console.expose(),
            diagnostic.expose()
        );
        std::fs::write(&path, invalid).unwrap();
        let error = load_config(&path).expect_err("single-node diagnostic auth must fail");
        assert!(
            error
                .to_string()
                .contains("diagnostic_read_token requires server.mode"),
            "{error}"
        );

        let valid = format!(
            r#"node_id = "node-1"

[server]
mode = "cluster"
bind = "127.0.0.1:8080"
console_token = "{}"
diagnostic_read_token = "{}"

[checkpoint]
url = "az://laminardb-test/checkpoints"

[discovery]
strategy = "static"
seeds = []
gossip_port = 7946
"#,
            console.expose(),
            diagnostic.expose()
        );
        std::fs::write(&path, valid).unwrap();
        let loaded = load_config(&path).expect("valid file-based split credentials must pass");
        assert_eq!(loaded.server.mode, ServerMode::Cluster);
        assert_eq!(
            loaded
                .server
                .diagnostic_read_token
                .as_ref()
                .unwrap()
                .expose(),
            diagnostic.expose()
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
        assert!(config.diagnostic_read_token.is_none());
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
        };

        assert_eq!(config.server.mode, ServerMode::Single);
        assert_eq!(config.server.bind, "127.0.0.1:8080");
        assert_eq!(config.checkpoint.interval, Duration::from_secs(10));
        assert_eq!(config.checkpoint.timeout, Duration::from_secs(120));
    }

    #[test]
    fn key_groups_are_mode_neutral_and_typed() {
        let single: ServerConfig = toml::from_str("[server]\n").unwrap();
        assert_eq!(single.server.resolved_key_groups(), DEFAULT_KEY_GROUP_COUNT);

        let configured_single: ServerConfig =
            toml::from_str("[server]\nmode = \"single\"\nkey_groups = 64\n").unwrap();
        assert_eq!(configured_single.server.resolved_key_groups().get(), 64);
        validate_config(&configured_single).unwrap();

        let cluster: ServerConfig = toml::from_str("[server]\nmode = \"cluster\"\n").unwrap();
        assert_eq!(
            cluster.server.resolved_key_groups(),
            DEFAULT_KEY_GROUP_COUNT
        );

        let configured: ServerConfig =
            toml::from_str("[server]\nmode = \"cluster\"\nkey_groups = 1024\n").unwrap();
        assert_eq!(configured.server.resolved_key_groups().get(), 1024);

        for mode in ["single", "cluster"] {
            for invalid in [0_u32, u32::from(u16::MAX) + 1] {
                let input = format!("[server]\nmode = \"{mode}\"\nkey_groups = {invalid}\n");
                assert!(toml::from_str::<ServerConfig>(&input).is_err());
            }
        }
    }

    #[test]
    fn test_checkpoint_duration_parsing() {
        let toml = r#"
[checkpoint]
interval = "30s"
timeout = "2m"
"#;
        let config: ServerConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.checkpoint.interval, Duration::from_secs(30));
        assert_eq!(config.checkpoint.timeout, Duration::from_secs(120));

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
    fn incremental_emit_is_server_execution_policy() {
        let config: ServerConfig = toml::from_str(
            r#"
[server]
incremental_emit = false
"#,
        )
        .unwrap();

        assert!(!config.server.incremental_emit);
    }

    #[test]
    fn sink_rejects_per_connector_delivery_dimension() {
        let error = toml::from_str::<ServerConfig>(
            r#"
[[sink]]
name = "out"
pipeline = "p"
connector = "kafka"
delivery = "exactly_once"
"#,
        )
        .expect_err("delivery is a pipeline-wide server contract");
        assert!(error.to_string().contains("unknown field"), "{error}");
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

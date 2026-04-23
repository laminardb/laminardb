//! NATS source and sink configuration.

use std::time::Duration;

use crate::config::ConnectorConfig;
use crate::connector::DeliveryGuarantee;
use crate::error::ConnectorError;
use crate::serde::Format;

/// NATS connector mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    /// Plain NATS pub/sub. No durability, no replay, at-most-once.
    Core,
    /// `JetStream` with durable pull consumers.
    #[default]
    JetStream,
}

str_enum!(fromstr Mode, lowercase, ConnectorError, "invalid nats mode",
    Core => "core";
    JetStream => "jetstream"
);

/// `JetStream` consumer ack policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AckPolicy {
    /// Each message acked individually.
    #[default]
    Explicit,
    /// No ack required.
    None,
}

str_enum!(fromstr AckPolicy, lowercase, ConnectorError, "invalid ack.policy",
    Explicit => "explicit";
    None => "none"
);

/// `JetStream` consumer delivery policy — where to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeliverPolicy {
    /// Every message retained by the stream.
    #[default]
    All,
    /// Only messages published after consumer creation.
    New,
    /// Start from a user-supplied stream sequence.
    ByStartSequence,
    /// Start from a user-supplied timestamp.
    ByStartTime,
}

str_enum!(fromstr DeliverPolicy, lowercase, ConnectorError, "invalid deliver.policy",
    All => "all";
    New => "new";
    ByStartSequence => "by_start_sequence";
    ByStartTime => "by_start_time"
);

/// Where the sink gets the subject to publish to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubjectSpec {
    /// A single literal subject used for every row.
    Literal(String),
    /// Per-row: read the named column and use its string value.
    Column(String),
}

/// NATS user-authentication mode. Custom `Debug` redacts secrets
/// because the parent config structs derive `Debug`.
#[derive(Clone, Default, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum AuthMode {
    #[default]
    None,
    UserPass {
        user: String,
        password: String,
    },
    Token(String),
    CredsFile(String),
}

impl std::fmt::Debug for AuthMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => f.write_str("None"),
            Self::UserPass { user, .. } => f
                .debug_struct("UserPass")
                .field("user", user)
                .field("password", &"<redacted>")
                .finish(),
            Self::Token(_) => f.debug_tuple("Token").field(&"<redacted>").finish(),
            Self::CredsFile(path) => f.debug_tuple("CredsFile").field(path).finish(),
        }
    }
}

/// TLS transport configuration.
#[derive(Debug, Clone, Default)]
#[allow(missing_docs)]
pub struct TlsConfig {
    pub enabled: bool,
    pub ca_location: Option<String>,
    pub cert_location: Option<String>,
    pub key_location: Option<String>,
}

/// NATS source configuration.
#[derive(Debug, Clone)]
#[allow(missing_docs)] // one-line `///` per field noises up the config struct
pub struct NatsSourceConfig {
    pub servers: Vec<String>,
    pub mode: Mode,
    pub auth: AuthMode,
    pub tls: TlsConfig,

    // JetStream
    pub stream: Option<String>,
    pub subject: Option<String>,
    pub subject_filters: Vec<String>,
    pub consumer: Option<String>,
    pub deliver_policy: DeliverPolicy,
    pub start_sequence: Option<u64>,
    pub start_time: Option<String>, // RFC3339; parsed against the NATS client's time type in open()
    pub ack_policy: AckPolicy,
    pub ack_wait: Duration,
    pub max_deliver: i64,
    pub max_ack_pending: i64,
    pub fetch_batch: usize,
    pub fetch_max_wait: Duration,
    /// Consecutive fetch errors before `health_check` reports
    /// `Unhealthy`. Zero disables the flip.
    pub fetch_error_threshold: u32,
    /// Interval between `consumer.info()` polls that feed the
    /// `nats_source_consumer_lag` gauge. Zero disables the poll.
    pub lag_poll_interval: Duration,

    // Core
    pub queue_group: Option<String>,

    pub format: Format,
}

impl NatsSourceConfig {
    /// # Errors
    ///
    /// `ConfigurationError` on any parse or validation failure.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let servers = parse_servers(config)?;
        let mode = parse_or_default::<Mode>(config, "mode")?;
        let auth = parse_auth(config)?;
        let tls = parse_tls(config)?;

        let stream = config.get("stream").map(str::to_string);
        let subject = config.get("subject").map(str::to_string);
        let subject_filters = config
            .get("subject.filters")
            .map(split_csv)
            .unwrap_or_default();
        let consumer = config.get("consumer").map(str::to_string);
        let deliver_policy = parse_or_default::<DeliverPolicy>(config, "deliver.policy")?;
        let start_sequence = parse_opt_u64(config, "start.sequence")?;
        let start_time = config.get("start.time").map(str::to_string);
        let ack_policy = parse_or_default::<AckPolicy>(config, "ack.policy")?;
        let ack_wait = require_positive_duration(config, "ack.wait.ms", Duration::from_secs(60))?;
        let max_deliver = require_positive_or_unlimited_i64(config, "max.deliver", 5)?;
        let max_ack_pending = require_positive_or_unlimited_i64(config, "max.ack.pending", 10_000)?;
        let fetch_batch = require_positive_usize(config, "fetch.batch", 500)?;
        let fetch_max_wait =
            require_positive_duration(config, "fetch.max.wait.ms", Duration::from_millis(500))?;
        let fetch_error_threshold = parse_u32(config, "fetch.error.threshold", 10)?;
        let lag_poll_interval =
            parse_duration_ms(config, "lag.poll.interval.ms", Duration::from_secs(10))?;
        let queue_group = config.get("queue.group").map(str::to_string);
        let format = parse_format(config)?;

        let cfg = Self {
            servers,
            mode,
            auth,
            tls,
            stream,
            subject,
            subject_filters,
            consumer,
            deliver_policy,
            start_sequence,
            start_time,
            ack_policy,
            ack_wait,
            max_deliver,
            max_ack_pending,
            fetch_batch,
            fetch_max_wait,
            fetch_error_threshold,
            lag_poll_interval,
            queue_group,
            format,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        match self.mode {
            Mode::JetStream => {
                if self.stream.is_none() {
                    return Err(cfg_err(
                        "[LDB-5040] jetstream mode requires 'stream' to be set",
                    ));
                }
                if self.consumer.is_none() {
                    return Err(cfg_err(
                        "[LDB-5041] jetstream mode requires 'consumer' (durable name) — \
                         ephemeral consumers are not supported",
                    ));
                }
                if self.subject.is_none() && self.subject_filters.is_empty() {
                    return Err(cfg_err(
                        "[LDB-5042] jetstream mode requires 'subject' or 'subject.filters' \
                         — the consumer must have at least one filter",
                    ));
                }
                if matches!(self.deliver_policy, DeliverPolicy::ByStartSequence)
                    && self.start_sequence.is_none()
                {
                    return Err(cfg_err(
                        "[LDB-5043] deliver.policy=by_start_sequence requires 'start.sequence'",
                    ));
                }
                if matches!(self.deliver_policy, DeliverPolicy::ByStartTime)
                    && self.start_time.is_none()
                {
                    return Err(cfg_err(
                        "[LDB-5044] deliver.policy=by_start_time requires 'start.time'",
                    ));
                }
            }
            Mode::Core => {
                if self.subject.is_none() {
                    return Err(cfg_err(
                        "[LDB-5045] core mode requires 'subject' (JetStream stream config \
                         does not apply)",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// NATS sink configuration.
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub struct NatsSinkConfig {
    pub servers: Vec<String>,
    pub mode: Mode,
    pub auth: AuthMode,
    pub tls: TlsConfig,
    pub stream: Option<String>,
    pub subject: SubjectSpec,
    pub expected_stream: Option<String>,
    pub delivery_guarantee: DeliveryGuarantee,
    pub dedup_id_column: Option<String>,
    /// Stream's `duplicate_window` must be at least this long under
    /// exactly-once — else rollback redelivery can land outside the
    /// dedup horizon.
    pub min_duplicate_window: Duration,
    pub max_pending: usize,
    pub ack_timeout: Duration,
    pub format: Format,
    pub header_columns: Vec<String>,
}

impl NatsSinkConfig {
    /// # Errors
    ///
    /// `ConfigurationError` on any parse or validation failure.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let servers = parse_servers(config)?;
        let mode = parse_or_default::<Mode>(config, "mode")?;
        let auth = parse_auth(config)?;
        let tls = parse_tls(config)?;

        let subject = match (config.get("subject"), config.get("subject.column")) {
            (Some(s), None) => SubjectSpec::Literal(s.to_string()),
            (None, Some(col)) => SubjectSpec::Column(col.to_string()),
            (Some(_), Some(_)) => {
                return Err(cfg_err(
                    "[LDB-5050] set 'subject' OR 'subject.column', not both",
                ));
            }
            (None, None) => {
                return Err(cfg_err(
                    "[LDB-5051] 'subject' or 'subject.column' is required",
                ));
            }
        };

        let delivery_guarantee =
            parse_or_default::<DeliveryGuarantee>(config, "delivery.guarantee")
                .map_err(|_| cfg_err("[LDB-5052] invalid delivery.guarantee"))?;
        let dedup_id_column = config.get("dedup.id.column").map(str::to_string);

        let cfg = Self {
            servers,
            mode,
            auth,
            tls,
            stream: config.get("stream").map(str::to_string),
            subject,
            expected_stream: config.get("expected.stream").map(str::to_string),
            delivery_guarantee,
            dedup_id_column,
            min_duplicate_window: require_positive_duration(
                config,
                "min.duplicate.window.ms",
                Duration::from_secs(120),
            )?,
            max_pending: require_positive_usize(config, "max.pending", 4096)?,
            ack_timeout: require_positive_duration(
                config,
                "ack.timeout.ms",
                Duration::from_secs(30),
            )?,
            format: parse_format(config)?,
            header_columns: config
                .get("header.columns")
                .map(split_csv)
                .unwrap_or_default(),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<(), ConnectorError> {
        if self.mode == Mode::Core && self.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            return Err(cfg_err(
                "[LDB-5053] delivery.guarantee=exactly_once is not supported in mode=core \
                 (no server-side dedup); use mode=jetstream",
            ));
        }
        if self.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
            && self.dedup_id_column.is_none()
        {
            return Err(cfg_err(
                "[LDB-5054] delivery.guarantee=exactly_once requires 'dedup.id.column' — \
                 msg-id dedup with epoch-row hashing is not supported (deterministic replay \
                 is too fragile; name a unique-per-row column)",
            ));
        }
        if self.delivery_guarantee == DeliveryGuarantee::ExactlyOnce && self.stream.is_none() {
            return Err(cfg_err(
                "[LDB-5055] delivery.guarantee=exactly_once requires 'stream' so the sink \
                 can validate its duplicate_window at startup",
            ));
        }
        for h in &self.header_columns {
            if is_reserved_header(h) {
                return Err(cfg_err(&format!(
                    "[LDB-5065] header.columns entry '{h}' collides with a reserved NATS \
                     header (Nats-Msg-Id / Nats-Expected-Stream); rename the column",
                )));
            }
        }
        Ok(())
    }
}

/// Header names the sink manages itself; a user header with the same
/// name would otherwise silently clobber exactly-once semantics.
fn is_reserved_header(name: &str) -> bool {
    const RESERVED: &[&str] = &["Nats-Msg-Id", "Nats-Expected-Stream"];
    RESERVED.iter().any(|r| r.eq_ignore_ascii_case(name))
}

// ── helpers ──

fn cfg_err(msg: &str) -> ConnectorError {
    ConnectorError::ConfigurationError(msg.to_string())
}

fn parse_servers(config: &ConnectorConfig) -> Result<Vec<String>, ConnectorError> {
    let raw = config.require("servers")?;
    let servers: Vec<String> = split_csv(raw);
    if servers.is_empty() {
        return Err(cfg_err("[LDB-5030] 'servers' must not be empty"));
    }
    Ok(servers)
}

fn split_csv(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn parse_or_default<T: std::str::FromStr + Default>(
    config: &ConnectorConfig,
    key: &str,
) -> Result<T, ConnectorError>
where
    T::Err: std::fmt::Display,
{
    match config.get(key) {
        Some(s) => s
            .parse()
            .map_err(|e: T::Err| cfg_err(&format!("invalid {key}: {e}"))),
        None => Ok(T::default()),
    }
}

fn parse_format(config: &ConnectorConfig) -> Result<Format, ConnectorError> {
    match config.get("format") {
        Some(s) => Format::parse(s).map_err(|e| cfg_err(&e.to_string())),
        None => Ok(Format::Json),
    }
}

fn parse_bool(config: &ConnectorConfig, key: &str, default: bool) -> Result<bool, ConnectorError> {
    match config.get(key) {
        Some(s) => s
            .parse::<bool>()
            .map_err(|_| cfg_err(&format!("{key} must be 'true' or 'false', got '{s}'"))),
        None => Ok(default),
    }
}

fn parse_opt_u64(config: &ConnectorConfig, key: &str) -> Result<Option<u64>, ConnectorError> {
    config.get(key).map(|s| parse_int(key, s)).transpose()
}

fn parse_i64(config: &ConnectorConfig, key: &str, default: i64) -> Result<i64, ConnectorError> {
    config.get(key).map_or(Ok(default), |s| parse_int(key, s))
}

fn parse_usize(
    config: &ConnectorConfig,
    key: &str,
    default: usize,
) -> Result<usize, ConnectorError> {
    config.get(key).map_or(Ok(default), |s| parse_int(key, s))
}

fn parse_u32(config: &ConnectorConfig, key: &str, default: u32) -> Result<u32, ConnectorError> {
    config.get(key).map_or(Ok(default), |s| parse_int(key, s))
}

fn require_positive_duration(
    config: &ConnectorConfig,
    key: &str,
    default: Duration,
) -> Result<Duration, ConnectorError> {
    let v = parse_duration_ms(config, key, default)?;
    if v.is_zero() {
        return Err(cfg_err(&format!("{key} must be > 0")));
    }
    Ok(v)
}

fn require_positive_usize(
    config: &ConnectorConfig,
    key: &str,
    default: usize,
) -> Result<usize, ConnectorError> {
    let v = parse_usize(config, key, default)?;
    if v == 0 {
        return Err(cfg_err(&format!("{key} must be > 0")));
    }
    Ok(v)
}

/// NATS allows `-1` for "unlimited" in `max_deliver` / `max_ack_pending`.
/// Reject 0 (undefined) and negative values other than -1.
fn require_positive_or_unlimited_i64(
    config: &ConnectorConfig,
    key: &str,
    default: i64,
) -> Result<i64, ConnectorError> {
    let v = parse_i64(config, key, default)?;
    if v == 0 || v < -1 {
        return Err(cfg_err(&format!("{key} must be > 0, or -1 for unlimited")));
    }
    Ok(v)
}

fn parse_duration_ms(
    config: &ConnectorConfig,
    key: &str,
    default: Duration,
) -> Result<Duration, ConnectorError> {
    config.get(key).map_or(Ok(default), |s| {
        parse_int::<u64>(key, s).map(Duration::from_millis)
    })
}

fn parse_int<T: std::str::FromStr>(key: &str, raw: &str) -> Result<T, ConnectorError> {
    raw.parse::<T>()
        .map_err(|_| cfg_err(&format!("{key} must be an integer, got '{raw}'")))
}

fn parse_auth(config: &ConnectorConfig) -> Result<AuthMode, ConnectorError> {
    let mode = config.get("auth.mode").unwrap_or("none");
    match mode {
        "none" | "" => {
            if config.get("user").is_some()
                || config.get("password").is_some()
                || config.get("token").is_some()
                || config.get("creds.file").is_some()
            {
                return Err(cfg_err(
                    "[LDB-5063] credentials set but auth.mode=none; \
                     set auth.mode explicitly or remove the credentials",
                ));
            }
            Ok(AuthMode::None)
        }
        "user_pass" => {
            let user = config
                .get("user")
                .ok_or_else(|| cfg_err("[LDB-5060] auth.mode=user_pass requires 'user'"))?
                .to_string();
            let password = config
                .get("password")
                .ok_or_else(|| cfg_err("[LDB-5060] auth.mode=user_pass requires 'password'"))?
                .to_string();
            Ok(AuthMode::UserPass { user, password })
        }
        "token" => {
            let token = config
                .get("token")
                .ok_or_else(|| cfg_err("[LDB-5061] auth.mode=token requires 'token'"))?
                .to_string();
            Ok(AuthMode::Token(token))
        }
        "creds_file" => {
            let path = config
                .get("creds.file")
                .ok_or_else(|| cfg_err("[LDB-5064] auth.mode=creds_file requires 'creds.file'"))?
                .to_string();
            Ok(AuthMode::CredsFile(path))
        }
        other => Err(cfg_err(&format!(
            "invalid auth.mode '{other}'; expected none | user_pass | token | creds_file"
        ))),
    }
}

fn parse_tls(config: &ConnectorConfig) -> Result<TlsConfig, ConnectorError> {
    let cert_location = config.get("tls.cert.location").map(str::to_string);
    let key_location = config.get("tls.key.location").map(str::to_string);
    if cert_location.is_some() != key_location.is_some() {
        return Err(cfg_err(
            "[LDB-5062] tls.cert.location and tls.key.location must both be set \
             (mutual-TLS client cert) or both be unset",
        ));
    }
    Ok(TlsConfig {
        enabled: parse_bool(config, "tls.enabled", false)?,
        ca_location: config.get("tls.ca.location").map(str::to_string),
        cert_location,
        key_location,
    })
}

/// Build `ConnectOptions` from parsed auth + TLS. Shared between
/// source and sink.
///
/// # Errors
///
/// Returns `ConnectorError` if the creds file can't be read or parsed.
pub(super) fn build_connect_options(
    auth: &AuthMode,
    tls: &TlsConfig,
) -> Result<async_nats::ConnectOptions, ConnectorError> {
    let mut opts = async_nats::ConnectOptions::new();
    match auth {
        AuthMode::None => {}
        AuthMode::UserPass { user, password } => {
            opts = opts.user_and_password(user.clone(), password.clone());
        }
        AuthMode::Token(token) => {
            opts = opts.token(token.clone());
        }
        AuthMode::CredsFile(path) => {
            let contents = std::fs::read_to_string(path)
                .map_err(|e| cfg_err(&format!("creds.file '{path}': {e}")))?;
            opts = async_nats::ConnectOptions::with_credentials(&contents)
                .map_err(|e| cfg_err(&format!("creds.file '{path}' invalid: {e}")))?;
        }
    }
    let tls_touched = tls.enabled || tls.ca_location.is_some() || tls.cert_location.is_some();
    if tls_touched {
        opts = opts.require_tls(true);
    }
    if let Some(ca) = &tls.ca_location {
        opts = opts.add_root_certificates(ca.into());
    }
    if let (Some(cert), Some(key)) = (&tls.cert_location, &tls.key_location) {
        opts = opts.add_client_certificate(cert.into(), key.into());
    }
    Ok(opts)
}

#[cfg(test)]
#[allow(clippy::disallowed_types)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn cfg(pairs: &[(&str, &str)]) -> ConnectorConfig {
        let mut props = HashMap::new();
        for (k, v) in pairs {
            props.insert((*k).to_string(), (*v).to_string());
        }
        ConnectorConfig::with_properties("nats", props)
    }

    // ── source ──

    #[test]
    fn source_jetstream_requires_stream() {
        let err = NatsSourceConfig::from_config(&cfg(&[
            ("servers", "nats://a:4222"),
            ("consumer", "c"),
            ("subject", "x.>"),
        ]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("LDB-5040"), "got: {err}");
    }

    #[test]
    fn source_jetstream_requires_consumer() {
        let err = NatsSourceConfig::from_config(&cfg(&[
            ("servers", "nats://a:4222"),
            ("stream", "S"),
            ("subject", "x.>"),
        ]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("LDB-5041"), "got: {err}");
    }

    #[test]
    fn source_jetstream_requires_subject_or_filters() {
        let err = NatsSourceConfig::from_config(&cfg(&[
            ("servers", "nats://a:4222"),
            ("stream", "S"),
            ("consumer", "c"),
        ]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("LDB-5042"), "got: {err}");
    }

    #[test]
    fn source_core_requires_subject() {
        let err =
            NatsSourceConfig::from_config(&cfg(&[("servers", "nats://a:4222"), ("mode", "core")]))
                .unwrap_err()
                .to_string();
        assert!(err.contains("LDB-5045"), "got: {err}");
    }

    #[test]
    fn source_by_start_sequence_requires_start_sequence() {
        let err = NatsSourceConfig::from_config(&cfg(&[
            ("servers", "nats://a:4222"),
            ("stream", "S"),
            ("consumer", "c"),
            ("subject", "x.>"),
            ("deliver.policy", "by_start_sequence"),
        ]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("LDB-5043"), "got: {err}");
    }

    #[test]
    fn source_happy_path_jetstream() {
        let parsed = NatsSourceConfig::from_config(&cfg(&[
            ("servers", "nats://a:4222,nats://b:4222"),
            ("stream", "ORDERS"),
            ("consumer", "laminar-orders"),
            ("subject.filters", "orders.us.*,orders.eu.*"),
            ("ack.wait.ms", "45000"),
            ("max.deliver", "3"),
        ]))
        .unwrap();
        assert_eq!(parsed.servers.len(), 2);
        assert_eq!(parsed.mode, Mode::JetStream);
        assert_eq!(parsed.subject_filters, vec!["orders.us.*", "orders.eu.*"]);
        assert_eq!(parsed.ack_wait, Duration::from_secs(45));
        assert_eq!(parsed.max_deliver, 3);
    }

    #[test]
    fn source_happy_path_core() {
        let parsed = NatsSourceConfig::from_config(&cfg(&[
            ("servers", "nats://a:4222"),
            ("mode", "core"),
            ("subject", "events"),
            ("queue.group", "workers"),
        ]))
        .unwrap();
        assert_eq!(parsed.mode, Mode::Core);
        assert_eq!(parsed.subject.as_deref(), Some("events"));
        assert_eq!(parsed.queue_group.as_deref(), Some("workers"));
    }

    // ── sink ──

    #[test]
    fn sink_rejects_subject_and_subject_column() {
        let err = NatsSinkConfig::from_config(&cfg(&[
            ("servers", "nats://a:4222"),
            ("subject", "x"),
            ("subject.column", "c"),
        ]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("LDB-5050"), "got: {err}");
    }

    #[test]
    fn sink_rejects_neither_subject_nor_column() {
        let err = NatsSinkConfig::from_config(&cfg(&[("servers", "nats://a:4222")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("LDB-5051"), "got: {err}");
    }

    #[test]
    fn sink_rejects_header_column_colliding_with_reserved() {
        let err = NatsSinkConfig::from_config(&cfg(&[
            ("servers", "nats://a:4222"),
            ("subject", "x"),
            ("header.columns", "trace_id,nats-msg-id"),
        ]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("LDB-5065"), "got: {err}");
    }

    #[test]
    fn sink_rejects_core_with_exactly_once() {
        let err = NatsSinkConfig::from_config(&cfg(&[
            ("servers", "nats://a:4222"),
            ("mode", "core"),
            ("subject", "x"),
            ("delivery.guarantee", "exactly_once"),
        ]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("LDB-5053"), "got: {err}");
    }

    #[test]
    fn sink_rejects_exactly_once_without_stream() {
        let err = NatsSinkConfig::from_config(&cfg(&[
            ("servers", "nats://a:4222"),
            ("subject", "x"),
            ("delivery.guarantee", "exactly_once"),
            ("dedup.id.column", "event_id"),
        ]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("LDB-5055"), "got: {err}");
    }

    #[test]
    fn sink_rejects_exactly_once_without_dedup_column() {
        let err = NatsSinkConfig::from_config(&cfg(&[
            ("servers", "nats://a:4222"),
            ("stream", "OUT"),
            ("subject", "x.processed"),
            ("delivery.guarantee", "exactly_once"),
        ]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("LDB-5054"), "got: {err}");
    }

    #[test]
    fn sink_happy_path_exactly_once() {
        let parsed = NatsSinkConfig::from_config(&cfg(&[
            ("servers", "nats://a:4222"),
            ("stream", "OUT"),
            ("subject.column", "out_subject"),
            ("delivery.guarantee", "exactly_once"),
            ("dedup.id.column", "event_id"),
            ("header.columns", "trace_id,tenant"),
        ]))
        .unwrap();
        assert_eq!(parsed.subject, SubjectSpec::Column("out_subject".into()));
        assert_eq!(parsed.delivery_guarantee, DeliveryGuarantee::ExactlyOnce);
        assert_eq!(parsed.dedup_id_column.as_deref(), Some("event_id"));
        assert_eq!(parsed.header_columns, vec!["trace_id", "tenant"]);
    }

    // ── servers ──

    #[test]
    fn servers_required() {
        let err = NatsSourceConfig::from_config(&cfg(&[]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("servers"), "got: {err}");
    }

    #[test]
    fn zero_fetch_batch_rejected() {
        let err = NatsSourceConfig::from_config(&jetstream_happy(&[("fetch.batch", "0")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("fetch.batch must be > 0"), "got: {err}");
    }

    #[test]
    fn zero_ack_wait_rejected() {
        let err = NatsSourceConfig::from_config(&jetstream_happy(&[("ack.wait.ms", "0")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("ack.wait.ms must be > 0"), "got: {err}");
    }

    #[test]
    fn max_deliver_unlimited_accepted() {
        let parsed =
            NatsSourceConfig::from_config(&jetstream_happy(&[("max.deliver", "-1")])).unwrap();
        assert_eq!(parsed.max_deliver, -1);
    }

    #[test]
    fn max_deliver_negative_other_than_minus_one_rejected() {
        let err = NatsSourceConfig::from_config(&jetstream_happy(&[("max.deliver", "-5")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("max.deliver"), "got: {err}");
    }

    #[test]
    fn sink_zero_max_pending_rejected() {
        let err = NatsSinkConfig::from_config(&cfg(&[
            ("servers", "nats://a:4222"),
            ("subject", "x"),
            ("max.pending", "0"),
        ]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("max.pending must be > 0"), "got: {err}");
    }

    #[test]
    fn servers_empty_csv_rejected() {
        let err = NatsSourceConfig::from_config(&cfg(&[("servers", ",,")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("LDB-5030"), "got: {err}");
    }

    // ── auth ──

    fn jetstream_happy(pairs: &[(&str, &str)]) -> ConnectorConfig {
        let mut base = vec![
            ("servers", "nats://a:4222"),
            ("stream", "S"),
            ("consumer", "c"),
            ("subject", "x.>"),
        ];
        base.extend_from_slice(pairs);
        cfg(&base)
    }

    #[test]
    fn auth_user_pass_requires_user() {
        let err = NatsSourceConfig::from_config(&jetstream_happy(&[
            ("auth.mode", "user_pass"),
            ("password", "secret"),
        ]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("LDB-5060"), "got: {err}");
    }

    #[test]
    fn auth_user_pass_requires_password() {
        let err = NatsSourceConfig::from_config(&jetstream_happy(&[
            ("auth.mode", "user_pass"),
            ("user", "alice"),
        ]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("LDB-5060"), "got: {err}");
    }

    #[test]
    fn auth_user_pass_happy_path() {
        let parsed = NatsSourceConfig::from_config(&jetstream_happy(&[
            ("auth.mode", "user_pass"),
            ("user", "alice"),
            ("password", "wonderland"),
        ]))
        .unwrap();
        assert_eq!(
            parsed.auth,
            AuthMode::UserPass {
                user: "alice".into(),
                password: "wonderland".into(),
            }
        );
    }

    #[test]
    fn auth_token_requires_token() {
        let err = NatsSourceConfig::from_config(&jetstream_happy(&[("auth.mode", "token")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("LDB-5061"), "got: {err}");
    }

    #[test]
    fn auth_token_happy_path() {
        let parsed = NatsSourceConfig::from_config(&jetstream_happy(&[
            ("auth.mode", "token"),
            ("token", "abc123"),
        ]))
        .unwrap();
        assert_eq!(parsed.auth, AuthMode::Token("abc123".into()));
    }

    #[test]
    fn auth_none_with_stray_credentials_rejected() {
        let err = NatsSourceConfig::from_config(&jetstream_happy(&[("user", "alice")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("LDB-5063"), "got: {err}");
    }

    #[test]
    fn auth_creds_file_requires_path() {
        let err = NatsSourceConfig::from_config(&jetstream_happy(&[("auth.mode", "creds_file")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("LDB-5064"), "got: {err}");
    }

    #[test]
    fn auth_creds_file_happy_path() {
        let parsed = NatsSourceConfig::from_config(&jetstream_happy(&[
            ("auth.mode", "creds_file"),
            ("creds.file", "/secrets/user.creds"),
        ]))
        .unwrap();
        assert_eq!(
            parsed.auth,
            AuthMode::CredsFile("/secrets/user.creds".into())
        );
    }

    #[test]
    fn auth_unknown_mode_rejected() {
        let err = NatsSourceConfig::from_config(&jetstream_happy(&[("auth.mode", "banana")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("invalid auth.mode"), "got: {err}");
    }

    // ── tls ──

    #[test]
    fn tls_cert_without_key_rejected() {
        let err = NatsSourceConfig::from_config(&jetstream_happy(&[(
            "tls.cert.location",
            "/certs/client.pem",
        )]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("LDB-5062"), "got: {err}");
    }

    #[test]
    fn tls_key_without_cert_rejected() {
        let err = NatsSourceConfig::from_config(&jetstream_happy(&[(
            "tls.key.location",
            "/certs/client.key",
        )]))
        .unwrap_err()
        .to_string();
        assert!(err.contains("LDB-5062"), "got: {err}");
    }

    #[test]
    fn tls_happy_path() {
        let parsed = NatsSourceConfig::from_config(&jetstream_happy(&[
            ("tls.enabled", "true"),
            ("tls.ca.location", "/certs/ca.pem"),
            ("tls.cert.location", "/certs/client.pem"),
            ("tls.key.location", "/certs/client.key"),
        ]))
        .unwrap();
        assert!(parsed.tls.enabled);
        assert_eq!(parsed.tls.ca_location.as_deref(), Some("/certs/ca.pem"));
        assert_eq!(
            parsed.tls.cert_location.as_deref(),
            Some("/certs/client.pem")
        );
    }

    #[test]
    fn auth_and_tls_on_sink() {
        let parsed = NatsSinkConfig::from_config(&cfg(&[
            ("servers", "nats://a:4222"),
            ("subject", "x"),
            ("auth.mode", "user_pass"),
            ("user", "alice"),
            ("password", "wonderland"),
            ("tls.enabled", "true"),
        ]))
        .unwrap();
        assert!(matches!(parsed.auth, AuthMode::UserPass { .. }));
        assert!(parsed.tls.enabled);
    }
}

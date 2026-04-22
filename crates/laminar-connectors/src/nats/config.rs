//! NATS source and sink configuration.
//!
//! Parsing + startup validation for the six hard-fail rules from the plan
//! that can be checked without a live NATS connection. The remaining two
//! rules (ack-wait vs checkpoint interval, duplicate-window vs max-deliver)
//! need server-side metadata and run in `open()` — not here.

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
    /// `JetStream` with durable pull consumers. Default.
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
    /// Each message acked individually. Default.
    #[default]
    Explicit,
    /// No ack required (fire-and-forget within JS).
    None,
}

str_enum!(fromstr AckPolicy, lowercase, ConnectorError, "invalid ack.policy",
    Explicit => "explicit";
    None => "none"
);

/// `JetStream` consumer delivery policy — where to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeliverPolicy {
    /// Replay every message retained by the stream. Default.
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

/// NATS user-authentication mode.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum AuthMode {
    /// No authentication.
    #[default]
    None,
    /// Username + password (one of the most common NATS auth shapes).
    UserPass {
        /// Username.
        user: String,
        /// Password.
        password: String,
    },
    /// Plain-text bearer token.
    Token(String),
}

/// TLS transport configuration.
///
/// Independent of [`AuthMode`] — real deployments usually pair TLS with
/// user/password or token auth on top.
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Require the connection to upgrade to TLS.
    pub enabled: bool,
    /// PEM-encoded CA certificate used to verify the server.
    pub ca_location: Option<String>,
    /// Client certificate for mutual TLS (paired with `key_location`).
    pub cert_location: Option<String>,
    /// Client private key for mutual TLS (paired with `cert_location`).
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
    pub fetch_max_bytes: usize,
    /// Consecutive fetch errors before the source is reported
    /// `Unhealthy`. Zero disables the health flip (an escape hatch for
    /// tests that need to decouple health from error counts).
    pub fetch_error_threshold: u32,

    // Core
    pub queue_group: Option<String>,

    // Format / metadata
    pub format: Format,
    pub include_metadata: bool,
    pub include_headers: bool,
    pub event_time_column: Option<String>,

    // Error handling
    pub poison_dlq_subject: Option<String>,
}

impl NatsSourceConfig {
    /// Parse the source config from a `ConnectorConfig`.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` on missing required keys,
    /// unparseable values, or violations of the startup-validation rules
    /// that don't need server metadata.
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
        let ack_wait = parse_duration_ms(config, "ack.wait.ms", Duration::from_secs(60))?;
        let max_deliver = parse_i64(config, "max.deliver", 5)?;
        let max_ack_pending = parse_i64(config, "max.ack.pending", 10_000)?;
        let fetch_batch = parse_usize(config, "fetch.batch", 500)?;
        let fetch_max_wait =
            parse_duration_ms(config, "fetch.max.wait.ms", Duration::from_millis(500))?;
        let fetch_max_bytes = parse_usize(config, "fetch.max.bytes", 1 << 20)?;
        let fetch_error_threshold = parse_u32(config, "fetch.error.threshold", 10)?;
        let queue_group = config.get("queue.group").map(str::to_string);
        let format = parse_format(config)?;
        let include_metadata = parse_bool(config, "include.metadata", false)?;
        let include_headers = parse_bool(config, "include.headers", false)?;
        let event_time_column = config.get("event.time.column").map(str::to_string);
        let poison_dlq_subject = config.get("poison.dlq.subject").map(str::to_string);

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
            fetch_max_bytes,
            fetch_error_threshold,
            queue_group,
            format,
            include_metadata,
            include_headers,
            event_time_column,
            poison_dlq_subject,
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
    /// Minimum `duplicate_window` the target stream must be configured
    /// with when `delivery.guarantee=exactly_once`. Parsed from
    /// `min.duplicate.window.ms`; defaults to 2 minutes. The sink
    /// refuses to start if the stream's actual window is shorter, since
    /// a short window means rollback redelivery can land outside the
    /// dedup horizon and produce duplicates silently.
    pub min_duplicate_window: Duration,
    pub max_pending: usize,
    pub ack_timeout: Duration,
    pub flush_batch_size: usize,
    pub format: Format,
    pub header_columns: Vec<String>,
    pub poison_dlq_subject: Option<String>,
}

impl NatsSinkConfig {
    /// Parse the sink config from a `ConnectorConfig`.
    ///
    /// # Errors
    ///
    /// Returns `ConnectorError::ConfigurationError` on missing required keys,
    /// unparseable values, or violations of startup-validation rules.
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
            min_duplicate_window: parse_duration_ms(
                config,
                "min.duplicate.window.ms",
                Duration::from_secs(120),
            )?,
            max_pending: parse_usize(config, "max.pending", 4096)?,
            ack_timeout: parse_duration_ms(config, "ack.timeout.ms", Duration::from_secs(30))?,
            flush_batch_size: parse_usize(config, "flush.batch.size", 1000)?,
            format: parse_format(config)?,
            header_columns: config
                .get("header.columns")
                .map(split_csv)
                .unwrap_or_default(),
            poison_dlq_subject: config.get("poison.dlq.subject").map(str::to_string),
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
        Ok(())
    }
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
            // Leftover credentials with auth.mode=none is a muddle. Fail
            // loudly so the operator chooses one.
            if config.get("user").is_some()
                || config.get("password").is_some()
                || config.get("token").is_some()
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
        other => Err(cfg_err(&format!(
            "invalid auth.mode '{other}'; expected none | user_pass | token"
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

/// Build `ConnectOptions` from parsed auth + TLS config. Shared by the
/// source and the sink so they don't drift in how they apply these.
pub(super) fn build_connect_options(
    auth: &AuthMode,
    tls: &TlsConfig,
) -> async_nats::ConnectOptions {
    let mut opts = async_nats::ConnectOptions::new();
    match auth {
        AuthMode::None => {}
        AuthMode::UserPass { user, password } => {
            opts = opts.user_and_password(user.clone(), password.clone());
        }
        AuthMode::Token(token) => {
            opts = opts.token(token.clone());
        }
    }
    // Any TLS-related key turns on the requirement. Operators who want
    // plain TCP just leave the TLS keys unset.
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
    opts
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
            ("include.metadata", "true"),
        ]))
        .unwrap();
        assert_eq!(parsed.servers.len(), 2);
        assert_eq!(parsed.mode, Mode::JetStream);
        assert_eq!(parsed.subject_filters, vec!["orders.us.*", "orders.eu.*"]);
        assert_eq!(parsed.ack_wait, Duration::from_secs(45));
        assert_eq!(parsed.max_deliver, 3);
        assert!(parsed.include_metadata);
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

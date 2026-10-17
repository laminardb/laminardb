//! IBM MQ source and sink configuration.
//!
//! Connection settings map to MQI structures: `queue.manager` to the `MQCONNX` queue manager
//! name, `channel` and `connection.name` to an `MQCD` client channel, `ccdt.url` to the
//! `MQCNO` CCDT URL, `username`/`password` to `MQCSP`, TLS options to `MQCD`/`MQSCO`, and
//! `reconnect` to the `MQCNO` reconnect option. With neither `channel` nor `ccdt.url`, the
//! client's own environment (`MQSERVER`, `MQCCDTURL`, `mqclient.ini`) selects the channel.

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use super::metadata::MetadataColumns;
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::serde::Format;

const RUNTIME_SOURCE_KEYS: &[&str] = &[
    "_arrow_schema",
    "_primary_key_columns",
    "format",
    "laminar.source.name",
];
const RUNTIME_SINK_KEYS: &[&str] = &[
    "_arrow_schema",
    "delivery.guarantee",
    "format",
    "sink.write.timeout.ms",
];
const CONNECTION_KEYS: &[&str] = &[
    "queue.manager",
    "channel",
    "connection.name",
    "ccdt.url",
    "username",
    "password",
    "application.name",
    "tls.cipher.spec",
    "tls.key.repository",
    "tls.key.repository.password",
    "tls.certificate.label",
    "tls.peer.name",
    "reconnect",
    "client.library",
    "operation.timeout.ms",
];
const SOURCE_KEYS: &[&str] = &[
    "queue",
    "wait.interval.ms",
    "max.message.bytes",
    "max.batch.bytes",
    "max.uow.messages",
    "max.uow.bytes",
    "on.decode.error",
    "convert",
    "metadata.columns",
];
const SINK_KEYS: &[&str] = &[
    "queue",
    "target.queue.manager",
    "persistence",
    "priority",
    "priority.column",
    "expiry",
    "message.id.column",
    "correlation.id.column",
    "reply.to.queue",
    "reply.to.queue.manager",
    "message.format",
    "ccsid",
    "property.columns",
    "max.uow.messages",
];

/// MQ's default `MAXMSGL` for queues and channels (4 MiB).
const DEFAULT_MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
/// MQ's largest supported message (100 MiB).
pub(super) const MQ_MAX_MESSAGE_BYTES: usize = 100 * 1024 * 1024;
const DEFAULT_MAX_BATCH_BYTES: usize = 16 * 1024 * 1024;
/// MQ's default queue manager `MAXUMSGS`.
const DEFAULT_MAX_UOW_MESSAGES: usize = 10_000;
const DEFAULT_MAX_UOW_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_SINK_UOW_MESSAGES: usize = 1_000;
const DEFAULT_WAIT: Duration = Duration::from_millis(100);
const MAX_WAIT: Duration = Duration::from_secs(5);
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_OPERATION_TIMEOUT: Duration = Duration::from_secs(300);

/// `MQCNO` reconnect option.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Reconnect {
    /// `MQCNO_RECONNECT_DISABLED`: a broken connection ends the connector generation.
    #[default]
    Disabled,
    /// `MQCNO_RECONNECT_Q_MGR`: reconnect only to the same queue manager.
    QueueManager,
    /// `MQCNO_RECONNECT`: reconnect to any queue manager the channel definitions allow.
    Any,
}

str_enum!(fromstr Reconnect, lowercase, ConnectorError, "invalid reconnect",
    Disabled => "disabled";
    QueueManager => "qmgr", "queue_manager";
    Any => "any"
);

/// How the client finds its server-connection channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
    /// An `MQCD` built from `channel` and `connection.name` (`host(port)[,host(port)]`).
    Channel {
        /// Server-connection channel name.
        channel: String,
        /// `CONNAME` list; the client tries entries in order.
        connection_name: String,
    },
    /// A client channel definition table (`file://`, `http(s)://`, or `ftp://` URL).
    Ccdt(String),
    /// The IBM MQ client's environment and `mqclient.ini`.
    ClientEnvironment,
}

/// TLS settings applied to the client channel.
#[derive(Clone, PartialEq, Eq)]
pub struct TlsConfig {
    /// `SSLCIPH` cipher specification, for example `ANY_TLS13_OR_HIGHER`.
    pub cipher_spec: String,
    /// Key repository stem (`.kdb`) or PKCS#12 file the client trusts and presents.
    pub key_repository: Option<String>,
    /// Key repository password.
    pub key_repository_password: Option<String>,
    /// Client certificate label in the key repository.
    pub certificate_label: Option<String>,
    /// `SSLPEER` distinguished-name filter the server certificate must match.
    pub peer_name: Option<String>,
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field("cipher_spec", &self.cipher_spec)
            .field("key_repository", &self.key_repository)
            .field("certificate_label", &self.certificate_label)
            .field("peer_name", &self.peer_name)
            .finish_non_exhaustive()
    }
}

/// Validated queue manager connection settings.
#[derive(Clone)]
pub struct ConnectionConfig {
    /// Queue manager name; `None` lets the channel definition decide.
    pub queue_manager: Option<String>,
    /// Channel selection.
    pub endpoint: Endpoint,
    /// `MQCSP` user.
    pub username: Option<String>,
    /// `MQCSP` password.
    pub password: Option<String>,
    /// `MQCNO` application name (at most 28 characters).
    pub application_name: String,
    /// TLS settings.
    pub tls: Option<TlsConfig>,
    /// Reconnect option.
    pub reconnect: Reconnect,
    /// Explicit IBM MQ client library (`libmqm_r`) path.
    pub client_library: Option<PathBuf>,
    /// Bound the async side waits for one MQI operation; it does not cancel the native call.
    pub operation_timeout: Duration,
}

impl std::fmt::Debug for ConnectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionConfig")
            .field("queue_manager", &self.queue_manager)
            .field("endpoint", &self.endpoint)
            .field("username", &self.username)
            .field("application_name", &self.application_name)
            .field("tls", &self.tls)
            .field("reconnect", &self.reconnect)
            .field("client_library", &self.client_library)
            .finish_non_exhaustive()
    }
}

/// What the source does with a message it cannot decode or that exceeds `max.message.bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DecodeErrorPolicy {
    /// Stop the source; MQBACK returns the message and increments its `BackoutCount`.
    #[default]
    Fail,
    /// Move the message to the queue's `BOQNAME` in the same unit of work.
    BackoutQueue,
}

str_enum!(fromstr DecodeErrorPolicy, lowercase, ConnectorError, "invalid on.decode.error",
    Fail => "fail";
    BackoutQueue => "backout_queue", "backout-queue"
);

/// Validated IBM MQ source settings.
#[derive(Debug, Clone)]
pub struct IbmMqSourceConfig {
    /// Queue manager connection.
    pub connection: ConnectionConfig,
    /// Local, alias, or cluster queue consumed by this source.
    pub queue: String,
    /// `MQGMO` wait interval for the first message of a poll.
    pub wait_interval: Duration,
    /// Receive buffer size; a longer message is never partially delivered.
    pub max_message_bytes: usize,
    /// Payload bytes per poll.
    pub max_batch_bytes: usize,
    /// Messages one unit of work may hold before intake waits for a checkpoint.
    pub max_uow_messages: usize,
    /// Payload bytes one unit of work may hold before intake waits for a checkpoint.
    pub max_uow_bytes: usize,
    /// Undecodable or oversized message handling.
    pub on_decode_error: DecodeErrorPolicy,
    /// `MQGMO_CONVERT` to UTF-8 (CCSID 1208) for messages in a convertible format.
    pub convert: bool,
    /// Payload codec.
    pub format: Format,
    /// Requested `MQMD` and property metadata columns.
    pub metadata: MetadataColumns,
}

/// `MQMD` `Persistence` written by the sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Persistence {
    /// `MQPER_PERSISTENT`: logged by the queue manager before `MQCMIT` returns.
    #[default]
    Persistent,
    /// `MQPER_NOT_PERSISTENT`: lost on queue manager restart.
    NotPersistent,
    /// `MQPER_PERSISTENCE_AS_Q_DEF`: the queue's `DEFPSIST`.
    AsQueueDefinition,
}

str_enum!(fromstr Persistence, lowercase, ConnectorError, "invalid persistence",
    Persistent => "persistent";
    NotPersistent => "not_persistent", "not-persistent";
    AsQueueDefinition => "as_queue_definition", "as-queue-definition"
);

/// Where a per-message value comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Valued<T> {
    /// Same value for every message.
    Literal(T),
    /// Read from a query column.
    Column(String),
}

/// Validated IBM MQ sink settings.
#[derive(Debug, Clone)]
pub struct IbmMqSinkConfig {
    /// Queue manager connection.
    pub connection: ConnectionConfig,
    /// Target queue (local, alias, remote definition, or cluster queue).
    pub queue: String,
    /// `MQOD` `ObjectQMgrName` for a queue on another queue manager.
    pub target_queue_manager: Option<String>,
    /// `MQMD` `Persistence`.
    pub persistence: Persistence,
    /// `MQMD` `Priority` (0-9).
    pub priority: Option<Valued<i32>>,
    /// `MQMD` `Expiry` in tenths of a second.
    pub expiry: Option<i32>,
    /// Column providing `MsgId` (Binary or Utf8, at most 24 bytes); otherwise a new id.
    pub message_id_column: Option<String>,
    /// Column providing `CorrelId` (Binary or Utf8, at most 24 bytes).
    pub correlation_id_column: Option<String>,
    /// `MQMD` `ReplyToQ`.
    pub reply_to_queue: Option<String>,
    /// `MQMD` `ReplyToQMgr`.
    pub reply_to_queue_manager: Option<String>,
    /// `MQMD` `Format` override (8 characters); defaults by codec.
    pub message_format: Option<String>,
    /// `MQMD` `CodedCharSetId` of text payloads.
    pub ccsid: i32,
    /// Columns written as typed message properties.
    pub property_columns: Vec<String>,
    /// Messages per unit of work; a larger batch commits in several units.
    pub max_uow_messages: usize,
    /// Payload codec.
    pub format: Format,
}

impl IbmMqSinkConfig {
    /// Columns consumed as message descriptor fields or properties rather than payload.
    #[must_use]
    pub fn property_columns(&self) -> Vec<&str> {
        let mut columns = Vec::new();
        if let Some(Valued::Column(name)) = &self.priority {
            columns.push(name.as_str());
        }
        columns.extend(self.message_id_column.as_deref());
        columns.extend(self.correlation_id_column.as_deref());
        columns.extend(self.property_columns.iter().map(String::as_str));
        columns
    }
}

impl IbmMqSourceConfig {
    /// Parse and validate a source configuration without I/O.
    ///
    /// # Errors
    /// Returns a configuration error for unknown, missing, conflicting, or invalid options.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let format = parse_source_format(config)?;
        reject_unknown(
            config,
            &[
                RUNTIME_SOURCE_KEYS,
                CONNECTION_KEYS,
                SOURCE_KEYS,
                format.source_option_keys(),
            ],
            "source",
        )?;
        let connection = ConnectionConfig::from_config(config)?;
        if connection.reconnect != Reconnect::Disabled {
            return Err(ConnectorError::ConfigurationError(
                "the IBM MQ source requires 'reconnect' = 'disabled': a client reconnect backs \
                 out the unit of work holding uncheckpointed messages, and the engine restart is \
                 the qualified recovery path"
                    .into(),
            ));
        }
        let max_message_bytes =
            config.get_positive("max.message.bytes", DEFAULT_MAX_MESSAGE_BYTES)?;
        if max_message_bytes > MQ_MAX_MESSAGE_BYTES {
            return Err(ConnectorError::ConfigurationError(format!(
                "'max.message.bytes' exceeds IBM MQ's {MQ_MAX_MESSAGE_BYTES}-byte message limit"
            )));
        }
        let max_batch_bytes = config.get_positive("max.batch.bytes", DEFAULT_MAX_BATCH_BYTES)?;
        let max_uow_bytes = config.get_positive("max.uow.bytes", DEFAULT_MAX_UOW_BYTES)?;
        if max_batch_bytes < max_message_bytes || max_uow_bytes < max_batch_bytes {
            return Err(ConnectorError::ConfigurationError(
                "limits must satisfy max.message.bytes <= max.batch.bytes <= max.uow.bytes".into(),
            ));
        }
        let wait_interval = config.get_millis("wait.interval.ms", DEFAULT_WAIT, MAX_WAIT)?;
        Ok(Self {
            connection,
            queue: config
                .get_non_empty("queue")?
                .ok_or_else(|| ConnectorError::missing_config("queue"))?,
            wait_interval,
            max_message_bytes,
            max_batch_bytes,
            max_uow_messages: config.get_positive("max.uow.messages", DEFAULT_MAX_UOW_MESSAGES)?,
            max_uow_bytes,
            on_decode_error: config.get_parsed("on.decode.error")?.unwrap_or_default(),
            convert: config.get_parsed("convert")?.unwrap_or(false),
            format,
            metadata: MetadataColumns::parse(config.get("metadata.columns"))?,
        })
    }

    /// Semantic identity: the queue consumed, without channel endpoints or credentials.
    #[must_use]
    pub fn identity(&self) -> std::collections::BTreeMap<String, String> {
        std::collections::BTreeMap::from([
            (
                "queue.manager".to_string(),
                self.connection.queue_manager.clone().unwrap_or_default(),
            ),
            ("queue".to_string(), self.queue.clone()),
            ("format".to_string(), self.format.to_string()),
            ("convert".to_string(), self.convert.to_string()),
            ("metadata.columns".to_string(), self.metadata.to_string()),
        ])
    }
}

impl IbmMqSinkConfig {
    /// Parse and validate a sink configuration without I/O.
    ///
    /// # Errors
    /// Returns a configuration error for unknown, missing, conflicting, or invalid options.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        reject_unknown(
            config,
            &[RUNTIME_SINK_KEYS, CONNECTION_KEYS, SINK_KEYS],
            "sink",
        )?;
        let priority = match (
            config.get_parsed::<i32>("priority")?,
            config.get_non_empty("priority.column")?,
        ) {
            (Some(_), Some(_)) => {
                return Err(conflict("set 'priority' or 'priority.column', not both"))
            }
            (Some(priority), None) => Some(Valued::Literal(check_priority(priority)?)),
            (None, Some(column)) => Some(Valued::Column(column)),
            (None, None) => None,
        };
        let expiry = config.get_parsed::<i32>("expiry")?;
        if expiry.is_some_and(|tenths| tenths <= 0) {
            return Err(conflict(
                "'expiry' is in tenths of a second and must be positive; omit it for unlimited",
            ));
        }
        let message_format = config.get_non_empty("message.format")?;
        if message_format
            .as_ref()
            .is_some_and(|format| format.len() > 8)
        {
            return Err(conflict(
                "'message.format' is an MQMD Format of at most 8 characters",
            ));
        }
        let sink = Self {
            connection: ConnectionConfig::from_config(config)?,
            queue: config
                .get_non_empty("queue")?
                .ok_or_else(|| ConnectorError::missing_config("queue"))?,
            target_queue_manager: config.get_non_empty("target.queue.manager")?,
            persistence: config.get_parsed("persistence")?.unwrap_or_default(),
            priority,
            expiry,
            message_id_column: config.get_non_empty("message.id.column")?,
            correlation_id_column: config.get_non_empty("correlation.id.column")?,
            reply_to_queue: config.get_non_empty("reply.to.queue")?,
            reply_to_queue_manager: config.get_non_empty("reply.to.queue.manager")?,
            message_format,
            ccsid: config.get_parsed("ccsid")?.unwrap_or(1208),
            property_columns: config.get_list("property.columns"),
            max_uow_messages: config.get_positive("max.uow.messages", DEFAULT_SINK_UOW_MESSAGES)?,
            format: parse_sink_format(config)?,
        };
        let mut seen = std::collections::BTreeSet::new();
        if let Some(duplicate) = sink
            .property_columns()
            .into_iter()
            .find(|name| !seen.insert(*name))
        {
            return Err(conflict(&format!(
                "column '{duplicate}' is mapped to more than one message field"
            )));
        }
        Ok(sink)
    }
}

impl ConnectionConfig {
    fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        let endpoint = match (
            config.get_non_empty("channel")?,
            config.get_non_empty("connection.name")?,
            config.get_non_empty("ccdt.url")?,
        ) {
            (Some(channel), Some(connection_name), None) => {
                bounded("channel", &channel, 20)?;
                bounded("connection.name", &connection_name, 264)?;
                Endpoint::Channel {
                    channel,
                    connection_name,
                }
            }
            (None, None, Some(url)) => Endpoint::Ccdt(url),
            (None, None, None) => Endpoint::ClientEnvironment,
            (_, _, Some(_)) => {
                return Err(conflict(
                    "set 'channel'/'connection.name' or 'ccdt.url', not both",
                ))
            }
            _ => {
                return Err(conflict(
                    "'channel' and 'connection.name' must be set together",
                ))
            }
        };
        let username = config.get_non_empty("username")?;
        let password = config.get("password").map(str::to_string);
        if password.is_some() && username.is_none() {
            return Err(conflict("'password' requires 'username'"));
        }
        let application_name = config
            .get_non_empty("application.name")?
            .unwrap_or_else(|| "laminardb".to_string());
        bounded("application.name", &application_name, 28)?;
        let queue_manager = config.get_non_empty("queue.manager")?;
        if let Some(name) = &queue_manager {
            bounded("queue.manager", name, 48)?;
        }
        Ok(Self {
            queue_manager,
            endpoint,
            username,
            password,
            application_name,
            tls: tls(config)?,
            reconnect: config.get_parsed("reconnect")?.unwrap_or_default(),
            client_library: config.get_non_empty("client.library")?.map(PathBuf::from),
            operation_timeout: config.get_millis(
                "operation.timeout.ms",
                DEFAULT_OPERATION_TIMEOUT,
                MAX_OPERATION_TIMEOUT,
            )?,
        })
    }
}

fn tls(config: &ConnectorConfig) -> Result<Option<TlsConfig>, ConnectorError> {
    let cipher_spec = config.get_non_empty("tls.cipher.spec")?;
    let others = [
        "tls.key.repository",
        "tls.key.repository.password",
        "tls.certificate.label",
        "tls.peer.name",
    ];
    let Some(cipher_spec) = cipher_spec else {
        if let Some(key) = others.iter().find(|key| config.get(key).is_some()) {
            return Err(conflict(&format!("'{key}' requires 'tls.cipher.spec'")));
        }
        return Ok(None);
    };
    if config.get_non_empty("ccdt.url")?.is_some() {
        return Err(conflict(
            "with 'ccdt.url' the channel definition owns its CipherSpec and peer name",
        ));
    }
    bounded("tls.cipher.spec", &cipher_spec, 32)?;
    Ok(Some(TlsConfig {
        cipher_spec,
        key_repository: config.get_non_empty("tls.key.repository")?,
        key_repository_password: config
            .get("tls.key.repository.password")
            .map(str::to_string),
        certificate_label: config.get_non_empty("tls.certificate.label")?,
        peer_name: config.get_non_empty("tls.peer.name")?,
    }))
}

fn check_priority(priority: i32) -> Result<i32, ConnectorError> {
    if (0..=9).contains(&priority) {
        Ok(priority)
    } else {
        Err(conflict("'priority' must be between 0 and 9"))
    }
}

fn reject_unknown(
    config: &ConnectorConfig,
    groups: &[&[&str]],
    direction: &str,
) -> Result<(), ConnectorError> {
    let allowed: Vec<&str> = groups
        .iter()
        .flat_map(|group| group.iter().copied())
        .collect();
    config.reject_unknown_properties(&allowed, &format!("IBM MQ {direction}"))
}

fn format_option(config: &ConnectorConfig) -> Result<Format, ConnectorError> {
    Ok(config
        .get("format")
        .map(Format::from_str)
        .transpose()
        .map_err(|error| ConnectorError::ConfigurationError(error.to_string()))?
        .unwrap_or(Format::Json))
}

/// Sources also read FORMAT FPML: one complete document per message.
fn parse_source_format(config: &ConnectorConfig) -> Result<Format, ConnectorError> {
    match format_option(config)? {
        format @ (Format::Json | Format::Csv | Format::Raw) => Ok(format),
        #[cfg(feature = "fpml")]
        Format::Fpml => Ok(Format::Fpml),
        #[cfg(not(feature = "fpml"))]
        Format::Fpml => Err(ConnectorError::ConfigurationError(
            "format 'fpml' requires a build with the fpml feature".into(),
        )),
        other => Err(ConnectorError::FeatureUnsupported(format!(
            "IBM MQ sources support FORMAT JSON, CSV, RAW, and FPML; {other} is not qualified \
             for them"
        ))),
    }
}

fn parse_sink_format(config: &ConnectorConfig) -> Result<Format, ConnectorError> {
    match format_option(config)? {
        format @ (Format::Json | Format::Csv | Format::Raw) => Ok(format),
        other => Err(ConnectorError::FeatureUnsupported(format!(
            "IBM MQ sinks support FORMAT JSON, CSV, and RAW; {other} is not qualified for them"
        ))),
    }
}

fn bounded(key: &str, value: &str, max: usize) -> Result<(), ConnectorError> {
    if value.len() > max {
        return Err(conflict(&format!(
            "'{key}' exceeds the {max}-character MQI field"
        )));
    }
    Ok(())
}

fn conflict(message: &str) -> ConnectorError {
    ConnectorError::ConfigurationError(message.to_string())
}

#[cfg(test)]
mod tests;

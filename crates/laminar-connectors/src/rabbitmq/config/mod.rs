//! RabbitMQ source and sink configuration.
//!
//! Connection settings use the AMQP URI natively (`heartbeat`, `frame_max`, `channel_max`,
//! `connection_timeout`, and `auth_mechanism` query parameters are interpreted by the client).
//! Every other option is either a native AMQP property/argument or a local resource bound.

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};
use lapin::uri::{AMQPScheme, AMQPUri};

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
    "url",
    "username",
    "password",
    "connection.name",
    "connect.timeout.ms",
    "tls.ca.location",
    "tls.cert.location",
    "tls.key.location",
];
const SOURCE_KEYS: &[&str] = &[
    "queue",
    "queue.declare",
    "queue.durable",
    "queue.type",
    "queue.arguments",
    "queue.bind.exchange",
    "queue.bind.routing.keys",
    "queue.bind.arguments",
    "prefetch.count",
    "consumer.tag",
    "consumer.exclusive",
    "consumer.arguments",
    "max.message.bytes",
    "max.batch.bytes",
    "on.decode.error",
    "metadata.columns",
];
const SINK_KEYS: &[&str] = &[
    "exchange",
    "exchange.declare",
    "exchange.type",
    "exchange.durable",
    "routing.key",
    "routing.key.column",
    "mandatory",
    "delivery.mode",
    "content.type",
    "content.encoding",
    "message.id.column",
    "correlation.id.column",
    "priority",
    "priority.column",
    "expiration.ms",
    "reply.to",
    "type",
    "app.id",
    "header.columns",
    "max.pending",
    "max.pending.bytes",
    "confirm.timeout.ms",
];

/// RabbitMQ's default `max_message_size` (16 MiB); larger messages are rejected by the broker.
pub(super) const DEFAULT_MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_BATCH_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_PREFETCH: u16 = 1_000;
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_MAX_PENDING: usize = 4_096;
const DEFAULT_MAX_PENDING_BYTES: usize = 64 * 1024 * 1024;
const MAX_IO_TIMEOUT: Duration = Duration::from_secs(300);

/// Validated broker connection settings shared by the source and sink.
#[derive(Clone)]
pub struct ConnectionConfig {
    /// Ordered endpoints; each start tries them once, in order.
    pub endpoints: Vec<AMQPUri>,
    /// Client-provided connection name shown by the broker.
    pub connection_name: String,
    /// Bound for TCP, TLS, and AMQP connection negotiation per endpoint.
    pub connect_timeout: Duration,
    /// PEM CA bundle trusted in addition to the Web PKI roots for `amqps` peer verification.
    pub tls_ca: Option<PathBuf>,
    /// PEM client certificate and PKCS#8 key for mutual TLS.
    pub tls_client: Option<(PathBuf, PathBuf)>,
}

impl std::fmt::Debug for ConnectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let endpoints: Vec<String> = self.endpoints.iter().map(redacted_endpoint).collect();
        f.debug_struct("ConnectionConfig")
            .field("endpoints", &endpoints)
            .field("connection_name", &self.connection_name)
            .field("connect_timeout", &self.connect_timeout)
            .field("tls_ca", &self.tls_ca)
            .field(
                "tls_client",
                &self.tls_client.as_ref().map(|(cert, _)| cert),
            )
            .finish()
    }
}

/// An endpoint rendered without credentials, for logs and errors.
pub(super) fn redacted_endpoint(uri: &AMQPUri) -> String {
    let scheme = match uri.scheme {
        AMQPScheme::AMQP => "amqp",
        AMQPScheme::AMQPS => "amqps",
    };
    format!(
        "{scheme}://{}:{}/{}",
        uri.authority.host, uri.authority.port, uri.vhost
    )
}

/// What the source does with a message its codec cannot decode or that exceeds the size bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DecodeErrorPolicy {
    /// Stop the source; the message stays unacknowledged and is redelivered after recovery.
    #[default]
    Fail,
    /// `basic.reject(requeue=false)`: the broker dead-letters the message if the queue has a
    /// dead-letter exchange (by policy or argument) and otherwise discards it.
    Reject,
}

str_enum!(fromstr DecodeErrorPolicy, lowercase, ConnectorError, "invalid on.decode.error",
    Fail => "fail";
    Reject => "reject"
);

/// Opt-in, idempotent queue declaration. Without it the queue must already exist.
#[derive(Debug, Clone, PartialEq)]
pub struct QueueDeclaration {
    /// `durable` declaration flag.
    pub durable: bool,
    /// Declaration arguments, including `x-queue-type` when `queue.type` is set.
    pub arguments: FieldTable,
}

/// Opt-in, idempotent `queue.bind` calls applied at startup.
#[derive(Debug, Clone, PartialEq)]
pub struct QueueBinding {
    /// Source exchange.
    pub exchange: String,
    /// One `queue.bind` per routing key.
    pub routing_keys: Vec<String>,
    /// Binding arguments (for example a headers-exchange match).
    pub arguments: FieldTable,
}

/// Validated RabbitMQ source settings.
#[derive(Debug, Clone)]
pub struct RabbitMqSourceConfig {
    /// Broker connection.
    pub connection: ConnectionConfig,
    /// Queue consumed by this source.
    pub queue: String,
    /// Declaration applied before consuming, if requested.
    pub declare: Option<QueueDeclaration>,
    /// Bindings applied before consuming, if requested.
    pub binding: Option<QueueBinding>,
    /// `basic.qos` prefetch count: the most unacknowledged deliveries the broker sends.
    pub prefetch: u16,
    /// Consumer tag; the broker generates one when absent.
    pub consumer_tag: Option<String>,
    /// `basic.consume` exclusive flag.
    pub exclusive: bool,
    /// `basic.consume` arguments (for example `x-priority`).
    pub consumer_arguments: FieldTable,
    /// Largest accepted message body.
    pub max_message_bytes: usize,
    /// Payload bytes decoded into one batch.
    pub max_batch_bytes: usize,
    /// Undecodable or oversized message handling.
    pub on_decode_error: DecodeErrorPolicy,
    /// Payload codec.
    pub format: Format,
    /// Requested message metadata columns.
    pub metadata: MetadataColumns,
}

/// AMQP `delivery-mode` written by the sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeliveryMode {
    /// Delivery mode 2: written to disk by durable queues before the publisher confirm.
    #[default]
    Persistent,
    /// Delivery mode 1: may be lost on broker restart.
    Transient,
}

str_enum!(fromstr DeliveryMode, lowercase, ConnectorError, "invalid delivery.mode",
    Persistent => "persistent", "2";
    Transient => "transient", "1"
);

impl DeliveryMode {
    /// The AMQP octet.
    #[must_use]
    pub const fn amqp(self) -> u8 {
        match self {
            Self::Persistent => 2,
            Self::Transient => 1,
        }
    }
}

/// Opt-in, idempotent exchange declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExchangeDeclaration {
    /// Exchange type (`direct`, `fanout`, `topic`, `headers`, or a plugin type).
    pub kind: String,
    /// `durable` declaration flag.
    pub durable: bool,
}

/// Where a per-message value comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Valued<T> {
    /// Same value for every message.
    Literal(T),
    /// Read from a query column.
    Column(String),
}

/// Validated RabbitMQ sink settings.
#[derive(Debug, Clone)]
pub struct RabbitMqSinkConfig {
    /// Broker connection.
    pub connection: ConnectionConfig,
    /// Target exchange; empty is the default exchange.
    pub exchange: String,
    /// Declaration applied at open, if requested.
    pub exchange_declare: Option<ExchangeDeclaration>,
    /// Routing key.
    pub routing_key: Valued<String>,
    /// `mandatory` publish flag: unroutable messages are returned and fail the write.
    pub mandatory: bool,
    /// `delivery-mode` property.
    pub delivery_mode: DeliveryMode,
    /// `content-type` property.
    pub content_type: Option<String>,
    /// `content-encoding` property.
    pub content_encoding: Option<String>,
    /// Column providing `message-id`.
    pub message_id_column: Option<String>,
    /// Column providing `correlation-id`.
    pub correlation_id_column: Option<String>,
    /// `priority` property.
    pub priority: Option<Valued<u8>>,
    /// `expiration` property: per-message TTL in milliseconds, enforced by the broker.
    pub expiration_ms: Option<u32>,
    /// `reply-to` property.
    pub reply_to: Option<String>,
    /// `type` property.
    pub message_type: Option<String>,
    /// `app-id` property.
    pub app_id: Option<String>,
    /// Columns written as typed message headers.
    pub header_columns: Vec<String>,
    /// Most publishes awaiting a broker confirm.
    pub max_pending: usize,
    /// Most payload bytes awaiting a broker confirm.
    pub max_pending_bytes: usize,
    /// Bound for one confirm drain.
    pub confirm_timeout: Duration,
    /// Payload codec.
    pub format: Format,
}

impl RabbitMqSinkConfig {
    /// Columns consumed as routing data or message properties rather than payload.
    #[must_use]
    pub fn property_columns(&self) -> Vec<&str> {
        let mut columns = Vec::new();
        if let Valued::Column(name) = &self.routing_key {
            columns.push(name.as_str());
        }
        if let Some(Valued::Column(name)) = &self.priority {
            columns.push(name.as_str());
        }
        columns.extend(self.message_id_column.as_deref());
        columns.extend(self.correlation_id_column.as_deref());
        columns.extend(self.header_columns.iter().map(String::as_str));
        columns
    }
}

impl RabbitMqSourceConfig {
    /// Parse and validate a source configuration without I/O.
    ///
    /// # Errors
    /// Returns a configuration error for unknown, missing, conflicting, or invalid options.
    pub fn from_config(config: &ConnectorConfig) -> Result<Self, ConnectorError> {
        reject_unknown(
            config,
            &[RUNTIME_SOURCE_KEYS, CONNECTION_KEYS, SOURCE_KEYS],
            "source",
        )?;
        let connection = ConnectionConfig::from_config(config, "laminardb-rabbitmq-source")?;
        let queue = config
            .get_non_empty("queue")?
            .ok_or_else(|| ConnectorError::missing_config("queue"))?;
        let declare = config.get_parsed::<bool>("queue.declare")?.unwrap_or(false);
        let declaration_keys = ["queue.durable", "queue.type", "queue.arguments"];
        if !declare {
            if let Some(key) = declaration_keys
                .iter()
                .find(|key| config.get(key).is_some())
            {
                return Err(conflict(&format!(
                    "'{key}' applies only with 'queue.declare' = 'true'; an existing queue keeps \
                     its declaration and operator policies"
                )));
            }
        }
        let declare = declare
            .then(|| -> Result<QueueDeclaration, ConnectorError> {
                let mut arguments = parse_table(config, "queue.arguments")?;
                if let Some(kind) = config.get_non_empty("queue.type")? {
                    if arguments
                        .inner()
                        .contains_key(&ShortString::from("x-queue-type"))
                    {
                        return Err(conflict(
                            "'queue.type' and 'queue.arguments' x-queue-type are both set",
                        ));
                    }
                    arguments.insert("x-queue-type".into(), AMQPValue::LongString(kind.into()));
                }
                reject_stream_queue(&arguments)?;
                Ok(QueueDeclaration {
                    durable: config.get_parsed::<bool>("queue.durable")?.unwrap_or(true),
                    arguments,
                })
            })
            .transpose()?;
        let binding = parse_binding(config)?;
        let prefetch = config
            .get_parsed::<u16>("prefetch.count")?
            .unwrap_or(DEFAULT_PREFETCH);
        if prefetch == 0 {
            return Err(ConnectorError::ConfigurationError(
                "'prefetch.count' must be positive; 0 means unlimited in AMQP and leaves client \
                 memory unbounded"
                    .into(),
            ));
        }
        let max_message_bytes =
            config.get_positive("max.message.bytes", DEFAULT_MAX_MESSAGE_BYTES)?;
        let max_batch_bytes = config.get_positive("max.batch.bytes", DEFAULT_MAX_BATCH_BYTES)?;
        if max_batch_bytes < max_message_bytes {
            return Err(conflict(
                "'max.batch.bytes' must be at least 'max.message.bytes' so every accepted message \
                 fits one batch",
            ));
        }
        let format = parse_format(config)?;
        let metadata = MetadataColumns::parse(config.get("metadata.columns"))?;
        Ok(Self {
            connection,
            queue,
            declare,
            binding,
            prefetch,
            consumer_tag: config.get_non_empty("consumer.tag")?,
            exclusive: config
                .get_parsed::<bool>("consumer.exclusive")?
                .unwrap_or(false),
            consumer_arguments: parse_table(config, "consumer.arguments")?,
            max_message_bytes,
            max_batch_bytes,
            on_decode_error: config
                .get("on.decode.error")
                .map(DecodeErrorPolicy::from_str)
                .transpose()?
                .unwrap_or_default(),
            format,
            metadata,
        })
    }

    /// Semantic identity: the broker object consumed, without endpoints or credentials.
    #[must_use]
    pub fn identity(&self) -> std::collections::BTreeMap<String, String> {
        let vhost = self
            .connection
            .endpoints
            .first()
            .map_or_else(String::new, |uri| uri.vhost.clone());
        std::collections::BTreeMap::from([
            ("vhost".to_string(), vhost),
            ("queue".to_string(), self.queue.clone()),
            ("format".to_string(), self.format.to_string()),
            ("metadata.columns".to_string(), self.metadata.to_string()),
        ])
    }
}

impl RabbitMqSinkConfig {
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
        let connection = ConnectionConfig::from_config(config, "laminardb-rabbitmq-sink")?;
        let exchange = config.get("exchange").unwrap_or_default().to_string();
        let declare = config
            .get_parsed::<bool>("exchange.declare")?
            .unwrap_or(false);
        if !declare
            && (config.get("exchange.type").is_some() || config.get("exchange.durable").is_some())
        {
            return Err(conflict(
                "'exchange.type' and 'exchange.durable' apply only with 'exchange.declare' = 'true'",
            ));
        }
        if declare && exchange.is_empty() {
            return Err(conflict("the default exchange cannot be declared"));
        }
        let exchange_declare = declare
            .then(|| -> Result<ExchangeDeclaration, ConnectorError> {
                Ok(ExchangeDeclaration {
                    kind: config
                        .get_non_empty("exchange.type")?
                        .ok_or_else(|| ConnectorError::missing_config("exchange.type"))?,
                    durable: config
                        .get_parsed::<bool>("exchange.durable")?
                        .unwrap_or(true),
                })
            })
            .transpose()?;
        let routing_key = match (
            config.get("routing.key"),
            config.get_non_empty("routing.key.column")?,
        ) {
            (Some(_), Some(_)) => {
                return Err(conflict(
                    "set 'routing.key' or 'routing.key.column', not both",
                ))
            }
            (Some(key), None) => Valued::Literal(key.to_string()),
            (None, Some(column)) => Valued::Column(column),
            (None, None) => {
                return Err(ConnectorError::ConfigurationError(
                    "set 'routing.key' (may be empty) or 'routing.key.column'".into(),
                ))
            }
        };
        let priority = match (
            config.get("priority"),
            config.get_non_empty("priority.column")?,
        ) {
            (Some(_), Some(_)) => {
                return Err(conflict("set 'priority' or 'priority.column', not both"))
            }
            (Some(_), None) => Some(Valued::Literal(config.require_parsed::<u8>("priority")?)),
            (None, Some(column)) => Some(Valued::Column(column)),
            (None, None) => None,
        };
        let header_columns = config.get_list("header.columns");
        let confirm_timeout = config.get_millis(
            "confirm.timeout.ms",
            DEFAULT_CONFIRM_TIMEOUT,
            MAX_IO_TIMEOUT,
        )?;
        let max_pending = config.get_positive("max.pending", DEFAULT_MAX_PENDING)?;
        let max_pending_bytes =
            config.get_positive("max.pending.bytes", DEFAULT_MAX_PENDING_BYTES)?;
        let sink = Self {
            connection,
            exchange,
            exchange_declare,
            routing_key,
            mandatory: config.get_parsed::<bool>("mandatory")?.unwrap_or(true),
            delivery_mode: config
                .get("delivery.mode")
                .map(DeliveryMode::from_str)
                .transpose()?
                .unwrap_or_default(),
            content_type: config.get_non_empty("content.type")?,
            content_encoding: config.get_non_empty("content.encoding")?,
            message_id_column: config.get_non_empty("message.id.column")?,
            correlation_id_column: config.get_non_empty("correlation.id.column")?,
            priority,
            expiration_ms: config.get_parsed::<u32>("expiration.ms")?,
            reply_to: config.get_non_empty("reply.to")?,
            message_type: config.get_non_empty("type")?,
            app_id: config.get_non_empty("app.id")?,
            header_columns,
            max_pending,
            max_pending_bytes,
            confirm_timeout,
            format: parse_format(config)?,
        };
        let mut seen = std::collections::BTreeSet::new();
        if let Some(duplicate) = sink
            .property_columns()
            .into_iter()
            .find(|name| !seen.insert(*name))
        {
            return Err(conflict(&format!(
                "column '{duplicate}' is mapped to more than one message property"
            )));
        }
        Ok(sink)
    }
}

impl ConnectionConfig {
    fn from_config(config: &ConnectorConfig, default_name: &str) -> Result<Self, ConnectorError> {
        let urls = config.get_list("url");
        if urls.is_empty() {
            return Err(ConnectorError::missing_config("url"));
        }
        let username = config.get("username");
        let password = config.get("password");
        if password.is_some() && username.is_none() {
            return Err(conflict("'password' requires 'username'"));
        }
        let endpoints = urls
            .iter()
            .map(|url| {
                let mut uri = AMQPUri::from_str(url).map_err(|error| {
                    // The parser error never echoes the URI, so credentials stay out of logs.
                    ConnectorError::ConfigurationError(format!("invalid AMQP 'url': {error}"))
                })?;
                if let Some(username) = username {
                    if url.contains('@') {
                        return Err(conflict(
                            "credentials are set both in 'url' and in 'username'/'password'",
                        ));
                    }
                    uri.authority.userinfo.username = username.to_string();
                    uri.authority.userinfo.password = password.unwrap_or_default().to_string();
                }
                Ok(uri)
            })
            .collect::<Result<Vec<_>, ConnectorError>>()?;
        let tls_ca = config.get_non_empty("tls.ca.location")?.map(PathBuf::from);
        let tls_client = match (
            config.get_non_empty("tls.cert.location")?,
            config.get_non_empty("tls.key.location")?,
        ) {
            (Some(cert), Some(key)) => Some((PathBuf::from(cert), PathBuf::from(key))),
            (None, None) => None,
            _ => {
                return Err(conflict(
                    "'tls.cert.location' and 'tls.key.location' must be set together",
                ))
            }
        };
        if (tls_ca.is_some() || tls_client.is_some())
            && endpoints.iter().any(|uri| uri.scheme != AMQPScheme::AMQPS)
        {
            return Err(conflict(
                "TLS options require every 'url' to use the amqps scheme",
            ));
        }
        let uri_timeouts: Vec<u64> = endpoints
            .iter()
            .filter_map(|uri| uri.query.connection_timeout)
            .collect();
        if !uri_timeouts.is_empty() && config.get("connect.timeout.ms").is_some() {
            return Err(conflict(
                "the connection timeout is set both by 'connect.timeout.ms' and by the 'url' \
                 connection_timeout query parameter",
            ));
        }
        let connect_timeout = match uri_timeouts.iter().max() {
            Some(millis) => Duration::from_millis(*millis),
            None => config.get_millis(
                "connect.timeout.ms",
                DEFAULT_CONNECT_TIMEOUT,
                MAX_IO_TIMEOUT,
            )?,
        };
        let vhosts: std::collections::BTreeSet<&str> =
            endpoints.iter().map(|uri| uri.vhost.as_str()).collect();
        if vhosts.len() > 1 {
            return Err(conflict(
                "every 'url' must name the same virtual host; endpoints are failover addresses \
                 for one broker or cluster",
            ));
        }
        Ok(Self {
            endpoints,
            connection_name: config
                .get_non_empty("connection.name")?
                .unwrap_or_else(|| default_name.to_string()),
            connect_timeout,
            tls_ca,
            tls_client,
        })
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
    config.reject_unknown_properties(&allowed, &format!("RabbitMQ {direction}"))
}

fn parse_binding(config: &ConnectorConfig) -> Result<Option<QueueBinding>, ConnectorError> {
    let Some(exchange) = config.get_non_empty("queue.bind.exchange")? else {
        if config.get("queue.bind.routing.keys").is_some()
            || config.get("queue.bind.arguments").is_some()
        {
            return Err(conflict(
                "'queue.bind.routing.keys' and 'queue.bind.arguments' require 'queue.bind.exchange'",
            ));
        }
        return Ok(None);
    };
    let mut routing_keys = config.get_list("queue.bind.routing.keys");
    if routing_keys.is_empty() {
        routing_keys.push(String::new());
    }
    Ok(Some(QueueBinding {
        exchange,
        routing_keys,
        arguments: parse_table(config, "queue.bind.arguments")?,
    }))
}

/// Stream queues do not redeliver unacknowledged messages after a consumer closes; a consumer
/// restarts from its `x-stream-offset`, so this connector's recovery contract does not hold.
fn reject_stream_queue(arguments: &FieldTable) -> Result<(), ConnectorError> {
    let is_stream = arguments
        .inner()
        .get(&ShortString::from("x-queue-type"))
        .is_some_and(
            |value| matches!(value, AMQPValue::LongString(kind) if kind.as_bytes() == b"stream"),
        );
    if is_stream {
        return Err(ConnectorError::FeatureUnsupported(
            "stream queues do not redeliver unacknowledged messages and are not supported by the \
             AMQP 0-9-1 source; use a classic or quorum queue"
                .into(),
        ));
    }
    Ok(())
}

fn parse_format(config: &ConnectorConfig) -> Result<Format, ConnectorError> {
    let format = config
        .get("format")
        .map(Format::parse)
        .transpose()
        .map_err(|error| ConnectorError::ConfigurationError(error.to_string()))?
        .unwrap_or(Format::Json);
    match format {
        Format::Json | Format::Csv | Format::Raw => Ok(format),
        other => Err(ConnectorError::FeatureUnsupported(format!(
            "RabbitMQ supports FORMAT JSON, CSV, and RAW; {other} is not qualified for it"
        ))),
    }
}

/// A JSON object of scalar AMQP arguments. JSON numbers become signed 64-bit integers (or
/// doubles when fractional), strings become long strings, and booleans stay booleans.
fn parse_table(config: &ConnectorConfig, key: &str) -> Result<FieldTable, ConnectorError> {
    let Some(text) = config.get(key) else {
        return Ok(FieldTable::default());
    };
    let value: serde_json::Value = serde_json::from_str(text).map_err(|error| {
        ConnectorError::ConfigurationError(format!("'{key}' must be a JSON object: {error}"))
    })?;
    let serde_json::Value::Object(entries) = value else {
        return Err(ConnectorError::ConfigurationError(format!(
            "'{key}' must be a JSON object"
        )));
    };
    let mut table = FieldTable::default();
    for (name, value) in entries {
        let amqp = match value {
            serde_json::Value::Bool(flag) => AMQPValue::Boolean(flag),
            serde_json::Value::String(text) => AMQPValue::LongString(LongString::from(text)),
            serde_json::Value::Number(number) => match (number.as_i64(), number.as_f64()) {
                (Some(integer), _) => AMQPValue::LongLongInt(integer),
                (None, Some(float)) => AMQPValue::Double(float),
                (None, None) => {
                    return Err(ConnectorError::ConfigurationError(format!(
                        "'{key}' entry '{name}' is out of the signed 64-bit range"
                    )))
                }
            },
            _ => {
                return Err(ConnectorError::ConfigurationError(format!(
                    "'{key}' entry '{name}' must be a string, number, or boolean"
                )))
            }
        };
        if name.len() > 255 {
            return Err(ConnectorError::ConfigurationError(format!(
                "'{key}' entry name exceeds the 255-byte AMQP short string"
            )));
        }
        table.insert(name.into(), amqp);
    }
    Ok(table)
}

fn conflict(message: &str) -> ConnectorError {
    ConnectorError::ConfigurationError(message.to_string())
}

#[cfg(test)]
mod tests;

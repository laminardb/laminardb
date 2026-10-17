//! Broker connection, TLS material, opt-in topology, and AMQP error classification.
//!
//! Connection recovery is deliberately not enabled: delivery tags and pending publisher
//! confirms belong to one channel, so a lost channel ends the connector generation and the
//! engine restarts from its last durable checkpoint.

use lapin::options::{ExchangeDeclareOptions, QueueBindOptions, QueueDeclareOptions};
use lapin::tcp::{OwnedIdentity, OwnedTLSConfig};
use lapin::types::FieldTable;
use lapin::{Channel, Connection, ConnectionProperties, ErrorKind, ExchangeKind};

use super::config::{
    redacted_endpoint, ConnectionConfig, ExchangeDeclaration, QueueBinding, QueueDeclaration,
};
use crate::error::ConnectorError;

/// Open a connection to the first reachable endpoint, trying each once in order.
pub(super) async fn connect(config: &ConnectionConfig) -> Result<Connection, ConnectorError> {
    let tls = tls_config(config).await?;
    let properties =
        ConnectionProperties::default().with_connection_name(config.connection_name.clone().into());
    let runtime = lapin::runtime::default_runtime()
        .map_err(|error| ConnectorError::Internal(format!("lapin runtime: {error}")))?;
    let timeout_ms = u64::try_from(config.connect_timeout.as_millis()).unwrap_or(u64::MAX);
    let mut failures = Vec::with_capacity(config.endpoints.len());
    for endpoint in &config.endpoints {
        let mut uri = endpoint.clone();
        uri.query.connection_timeout.get_or_insert(timeout_ms);
        let attempt = Connection::connect_uri_with_config(
            uri,
            properties.clone(),
            tls.clone(),
            runtime.clone(),
        );
        match tokio::time::timeout(config.connect_timeout, attempt).await {
            Ok(Ok(connection)) => return Ok(connection),
            Ok(Err(error)) => failures.push(format!("{}: {error}", redacted_endpoint(endpoint))),
            Err(_) => failures.push(format!(
                "{}: no AMQP connection within {timeout_ms}ms",
                redacted_endpoint(endpoint)
            )),
        }
    }
    Err(ConnectorError::ConnectionFailed(format!(
        "RabbitMQ connection failed: {}",
        failures.join("; ")
    )))
}

async fn tls_config(config: &ConnectionConfig) -> Result<OwnedTLSConfig, ConnectorError> {
    let read = |path: std::path::PathBuf| async move {
        tokio::fs::read(&path).await.map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "cannot read TLS file '{}': {error}",
                path.display()
            ))
        })
    };
    let cert_chain = match &config.tls_ca {
        Some(path) => Some(String::from_utf8(read(path.clone()).await?).map_err(|_| {
            ConnectorError::ConfigurationError("'tls.ca.location' must be a PEM file".into())
        })?),
        None => None,
    };
    let identity = match &config.tls_client {
        Some((cert, key)) => Some(OwnedIdentity::PKCS8 {
            pem: read(cert.clone()).await?,
            key: read(key.clone()).await?,
        }),
        None => None,
    };
    Ok(OwnedTLSConfig {
        identity,
        cert_chain,
    })
}

/// Verify (passive) or idempotently declare the queue, then apply requested bindings.
pub(super) async fn prepare_queue(
    channel: &Channel,
    queue: &str,
    declaration: Option<&QueueDeclaration>,
    binding: Option<&QueueBinding>,
) -> Result<(), ConnectorError> {
    let (options, arguments) = match declaration {
        Some(declaration) => (
            QueueDeclareOptions {
                durable: declaration.durable,
                ..QueueDeclareOptions::default()
            },
            declaration.arguments.clone(),
        ),
        None => (
            QueueDeclareOptions {
                passive: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        ),
    };
    channel
        .queue_declare(queue.into(), options, arguments)
        .await
        .map_err(|error| topology_error(&format!("queue '{queue}'"), &error))?;
    let Some(binding) = binding else {
        return Ok(());
    };
    for routing_key in &binding.routing_keys {
        channel
            .queue_bind(
                queue.into(),
                binding.exchange.as_str().into(),
                routing_key.as_str().into(),
                QueueBindOptions::default(),
                binding.arguments.clone(),
            )
            .await
            .map_err(|error| {
                topology_error(
                    &format!("binding of '{queue}' to exchange '{}'", binding.exchange),
                    &error,
                )
            })?;
    }
    Ok(())
}

/// Verify (passive) or idempotently declare the sink exchange. The default exchange always exists.
pub(super) async fn prepare_exchange(
    channel: &Channel,
    exchange: &str,
    declaration: Option<&ExchangeDeclaration>,
) -> Result<(), ConnectorError> {
    if exchange.is_empty() {
        return Ok(());
    }
    let (kind, options) = match declaration {
        Some(declaration) => (
            exchange_kind(&declaration.kind),
            ExchangeDeclareOptions {
                durable: declaration.durable,
                ..ExchangeDeclareOptions::default()
            },
        ),
        None => (
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                passive: true,
                ..ExchangeDeclareOptions::default()
            },
        ),
    };
    channel
        .exchange_declare(exchange.into(), kind, options, FieldTable::default())
        .await
        .map_err(|error| topology_error(&format!("exchange '{exchange}'"), &error))
}

fn exchange_kind(kind: &str) -> ExchangeKind {
    match kind.to_ascii_lowercase().as_str() {
        "direct" => ExchangeKind::Direct,
        "fanout" => ExchangeKind::Fanout,
        "headers" => ExchangeKind::Headers,
        "topic" => ExchangeKind::Topic,
        _ => ExchangeKind::Custom(kind.into()),
    }
}

/// AMQP reply code carried by a broker error, if any.
pub(super) fn reply_code(error: &lapin::Error) -> Option<u16> {
    match error.kind() {
        ErrorKind::ProtocolError(amqp) => Some(amqp.get_id()),
        _ => None,
    }
}

fn topology_error(object: &str, error: &lapin::Error) -> ConnectorError {
    match reply_code(error) {
        Some(404) => ConnectorError::ConfigurationError(format!(
            "{object} does not exist; provision it or enable the connector's opt-in declaration: \
             {error}"
        )),
        Some(406) => ConnectorError::ConfigurationError(format!(
            "{object} exists with a different declaration; the connector never deletes or \
             redeclares broker objects: {error}"
        )),
        Some(403) => ConnectorError::ConfigurationError(format!(
            "{object}: access refused for the configured user: {error}"
        )),
        _ => ConnectorError::ConnectionFailed(format!("{object}: {error}")),
    }
}

/// Close a channel and its connection, bounded; the broker requeues unacknowledged deliveries.
pub(super) async fn close(
    channel: &Channel,
    connection: &Connection,
    timeout: std::time::Duration,
) -> Result<(), ConnectorError> {
    let shutdown = async {
        if channel.status().connected() {
            channel
                .close(200, "laminardb connector closed".into())
                .await?;
        }
        if connection.status().connected() {
            connection
                .close(200, "laminardb connector closed".into())
                .await?;
        }
        Ok::<(), lapin::Error>(())
    };
    match tokio::time::timeout(timeout, shutdown).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(ConnectorError::ConnectionFailed(format!(
            "RabbitMQ close failed: {error}"
        ))),
        Err(_) => Err(ConnectorError::Timeout(
            u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
        )),
    }
}

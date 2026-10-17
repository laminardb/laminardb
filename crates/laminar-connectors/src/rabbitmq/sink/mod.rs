//! RabbitMQ sink: publisher-confirmed, optionally mandatory publishes to one exchange.
//!
//! Publishes are pipelined within `max.pending` messages and `max.pending.bytes`; `flush` (and
//! therefore every checkpoint's pre-commit) waits until the broker has confirmed each one.
//! A confirm means the broker accepted the message for every queue it routed to; durable
//! storage additionally needs persistent delivery mode and durable (for node loss, replicated)
//! queues, which the operator provisions. Any nack, return, or lost outcome ends this sink
//! generation; replay from the last checkpoint may then publish duplicates (at-least-once).

mod properties;

use std::collections::VecDeque;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use lapin::options::{BasicPublishOptions, ConfirmSelectOptions};
use lapin::{BasicProperties, Channel, Confirmation, Connection, PublisherConfirm};

use super::config::{DeliveryMode, RabbitMqSinkConfig};
use super::connection::{close, connect, prepare_exchange};
use super::metrics::{saturating, SinkMetrics};
use crate::config::ConnectorConfig;
use crate::connector::{
    SinkConnector, SinkConsistency, SinkContract, SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;
use crate::serde::RecordSerializer;
use properties::{base_properties, payload_schema, BoundProperties};

/// RabbitMQ AMQP 0-9-1 exchange sink.
pub struct RabbitMqSink {
    schema: SchemaRef,
    registry: Option<prometheus::Registry>,
    config: Option<RabbitMqSinkConfig>,
    writer: Option<Writer>,
    runtime: Option<Runtime>,
    failure: Option<String>,
}

struct Writer {
    serializer: Box<dyn RecordSerializer>,
    payload_indices: Vec<usize>,
    base: BasicProperties,
}

struct Runtime {
    connection: Connection,
    channel: Channel,
    pending: VecDeque<(PublisherConfirm, usize)>,
    pending_bytes: usize,
    metrics: SinkMetrics,
}

/// Outcome tally of one confirm drain.
#[derive(Default)]
struct Drained {
    confirmed: usize,
    nacked: usize,
    returned: Option<String>,
    ambiguous: Option<String>,
}

impl Drained {
    fn into_result(self) -> Result<usize, ConnectorError> {
        if let Some(cause) = self.ambiguous {
            return Err(ConnectorError::outcome_unknown(
                format!(
                    "RabbitMQ publish outcome unknown after {} confirmed: {cause}",
                    self.confirmed
                ),
                true,
            ));
        }
        if let Some(cause) = self.returned {
            return Err(ConnectorError::ConfigurationError(format!(
                "a mandatory publish was returned as unroutable ({cause}); bind a queue to the \
                 exchange for this routing key"
            )));
        }
        if self.nacked > 0 {
            return Err(ConnectorError::WriteError(format!(
                "the broker negatively confirmed {} publish(es)",
                self.nacked
            )));
        }
        Ok(self.confirmed)
    }
}

impl Runtime {
    /// Await confirms oldest-first until at most `keep` messages and `keep_bytes` remain pending.
    async fn drain(
        &mut self,
        keep: usize,
        keep_bytes: usize,
        deadline: tokio::time::Instant,
    ) -> Result<usize, ConnectorError> {
        let mut drained = Drained::default();
        while self.pending.len() > keep || self.pending_bytes > keep_bytes {
            let Some((confirm, bytes)) = self.pending.pop_front() else {
                break;
            };
            self.pending_bytes -= bytes;
            match tokio::time::timeout_at(deadline, confirm).await {
                Ok(Ok(Confirmation::Ack(None))) => drained.confirmed += 1,
                Ok(Ok(Confirmation::Ack(Some(returned)))) => {
                    self.metrics.returned.inc();
                    drained.returned.get_or_insert_with(|| {
                        format!("{} {}", returned.reply_code, returned.reply_text.as_str())
                    });
                }
                Ok(Ok(Confirmation::Nack(_))) => {
                    self.metrics.nacked.inc();
                    drained.nacked += 1;
                }
                Ok(Ok(Confirmation::NotRequested)) => {
                    drained
                        .ambiguous
                        .get_or_insert_with(|| "the channel is not in confirm mode".to_string());
                }
                Ok(Err(error)) => {
                    self.metrics.ambiguous.inc();
                    drained.ambiguous.get_or_insert_with(|| error.to_string());
                }
                Err(_) => {
                    self.metrics.ambiguous.inc();
                    drained
                        .ambiguous
                        .get_or_insert_with(|| "no broker confirm before the deadline".to_string());
                }
            }
        }
        self.metrics.confirmed.inc_by(drained.confirmed as u64);
        self.metrics.pending.set(saturating(self.pending.len()));
        self.metrics
            .pending_bytes
            .set(saturating(self.pending_bytes));
        drained.into_result()
    }
}

impl RabbitMqSink {
    /// Create an unopened sink; metrics register on `registry` at open.
    #[must_use]
    pub fn new(schema: SchemaRef, registry: Option<&prometheus::Registry>) -> Self {
        Self {
            schema,
            registry: registry.cloned(),
            config: None,
            writer: None,
            runtime: None,
            failure: None,
        }
    }

    fn fail(&mut self, error: ConnectorError) -> ConnectorError {
        tracing::error!(%error, "RabbitMQ sink generation ended");
        self.failure.get_or_insert(error.to_string());
        error
    }

    fn live(&mut self) -> Result<(&RabbitMqSinkConfig, &Writer, &mut Runtime), ConnectorError> {
        if let Some(cause) = &self.failure {
            return Err(ConnectorError::InvalidState {
                expected: "a live RabbitMQ sink".into(),
                actual: cause.clone(),
            });
        }
        match (&self.config, &self.writer, self.runtime.as_mut()) {
            (Some(config), Some(writer), Some(runtime)) => Ok((config, writer, runtime)),
            _ => Err(ConnectorError::InvalidState {
                expected: "an open RabbitMQ sink".into(),
                actual: "open() has not completed".into(),
            }),
        }
    }

    fn writer(parsed: &RabbitMqSinkConfig, input: &SchemaRef) -> Result<Writer, ConnectorError> {
        let payload = payload_schema(parsed, input)?;
        crate::serde::schema_contract::validate_writer(parsed.format, &payload)?;
        let payload_indices = payload
            .fields()
            .iter()
            .map(|field| input.index_of(field.name()))
            .collect::<Result<_, _>>()
            .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?;
        Ok(Writer {
            serializer: crate::serde::create_serializer(parsed.format)?,
            base: base_properties(parsed, &payload),
            payload_indices,
        })
    }

    async fn publish(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        let (config, writer, runtime) = self.live()?;
        let rows = BoundProperties::bind(config, batch)?;
        let payload = batch
            .project(&writer.payload_indices)
            .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?;
        let records = writer.serializer.serialize(&payload)?;
        if records.len() != batch.num_rows() {
            return Err(ConnectorError::Internal(format!(
                "RabbitMQ serializer returned {} records for {} rows",
                records.len(),
                batch.num_rows()
            )));
        }
        let deadline = tokio::time::Instant::now() + config.confirm_timeout;
        let options = BasicPublishOptions {
            mandatory: config.mandatory,
            ..BasicPublishOptions::default()
        };
        let mut bytes = 0_u64;
        for (row, record) in records.iter().enumerate() {
            let keep = config.max_pending - 1;
            let keep_bytes = config.max_pending_bytes.saturating_sub(record.len());
            runtime.drain(keep, keep_bytes, deadline).await?;
            let confirm = runtime
                .channel
                .basic_publish(
                    config.exchange.as_str().into(),
                    rows.routing_key(row).into(),
                    options,
                    record,
                    rows.properties(row, &writer.base),
                )
                .await
                .map_err(|error| {
                    let prior = row > 0 || !runtime.pending.is_empty();
                    if prior {
                        ConnectorError::outcome_unknown(format!("RabbitMQ publish: {error}"), true)
                    } else {
                        ConnectorError::WriteError(format!("RabbitMQ publish: {error}"))
                    }
                })?;
            runtime.pending.push_back((confirm, record.len()));
            runtime.pending_bytes += record.len();
            bytes += record.len() as u64;
        }
        runtime.metrics.published.inc_by(records.len() as u64);
        runtime.metrics.bytes.inc_by(bytes);
        runtime
            .metrics
            .pending
            .set(saturating(runtime.pending.len()));
        runtime
            .metrics
            .pending_bytes
            .set(saturating(runtime.pending_bytes));
        Ok(WriteResult::new(records.len(), bytes))
    }
}

#[async_trait]
impl SinkConnector for RabbitMqSink {
    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        let parsed = RabbitMqSinkConfig::from_config(config)?;
        let consistency = match parsed.delivery_mode {
            DeliveryMode::Persistent => SinkConsistency::DurableAtLeastOnce,
            DeliveryMode::Transient => SinkConsistency::Ephemeral,
        };
        Ok(SinkContract::new(
            consistency,
            SinkTopology::MultiWriter,
            SinkInputMode::AppendOnly,
        ))
    }

    async fn resolve_schema(
        &mut self,
        config: &ConnectorConfig,
        input: SchemaRef,
    ) -> Result<crate::schema::resolution::SchemaBinding, ConnectorError> {
        use crate::schema::resolution::{logical_binding, SchemaDirection, SchemaOrigin};
        let parsed = RabbitMqSinkConfig::from_config(config)?;
        Self::writer(&parsed, &input)?;
        logical_binding(config, SchemaDirection::Sink, SchemaOrigin::Query, &input)
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        let parsed = RabbitMqSinkConfig::from_config(config)?;
        let input = config
            .arrow_schema()
            .or_else(|| {
                config
                    .schema_binding()
                    .map(|binding| std::sync::Arc::new(binding.logical.clone()))
            })
            .unwrap_or_else(|| self.schema.clone());
        let writer = Self::writer(&parsed, &input)?;
        let metrics = SinkMetrics::new(&parsed.connection.connection_name, self.registry.as_ref())?;
        let connection = connect(&parsed.connection).await?;
        let channel = connection.create_channel().await.map_err(|error| {
            ConnectorError::ConnectionFailed(format!("RabbitMQ channel: {error}"))
        })?;
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|error| {
                ConnectorError::ConnectionFailed(format!("confirm.select: {error}"))
            })?;
        prepare_exchange(&channel, &parsed.exchange, parsed.exchange_declare.as_ref()).await?;
        tracing::info!(
            exchange = %parsed.exchange,
            mandatory = parsed.mandatory,
            delivery_mode = ?parsed.delivery_mode,
            endpoints = ?parsed.connection,
            "RabbitMQ sink publishing"
        );
        self.schema = input;
        self.writer = Some(writer);
        self.runtime = Some(Runtime {
            connection,
            channel,
            pending: VecDeque::new(),
            pending_bytes: 0,
            metrics,
        });
        self.config = Some(parsed);
        self.failure = None;
        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        match self.publish(batch).await {
            Ok(result) => Ok(result),
            Err(error) if self.failure.is_some() => Err(error),
            Err(error) => Err(self.fail(error)),
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn suggested_write_timeout(&self) -> Duration {
        // A write may wait for confirms to free pending capacity, bounded by the confirm timeout.
        self.config
            .as_ref()
            .map_or(Duration::from_secs(30), |config| config.confirm_timeout)
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        let (config, _, runtime) = self.live()?;
        let deadline = tokio::time::Instant::now() + config.confirm_timeout;
        match runtime.drain(0, 0, deadline).await {
            Ok(_) => Ok(()),
            Err(error) => Err(self.fail(error)),
        }
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        let drained = if self.failure.is_none() && self.runtime.is_some() {
            self.flush().await
        } else {
            Ok(())
        };
        let closed = match self.runtime.take() {
            Some(runtime) => {
                close(
                    &runtime.channel,
                    &runtime.connection,
                    Duration::from_secs(5),
                )
                .await
            }
            None => Ok(()),
        };
        drained.and(closed)
    }
}

#[cfg(test)]
mod tests;

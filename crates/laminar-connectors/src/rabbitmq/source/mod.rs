//! RabbitMQ source: one manual-acknowledgement consumer on an existing (or opt-in declared)
//! classic or quorum queue.
//!
//! Deliveries are handed to the pipeline unacknowledged and acknowledged only after the
//! checkpoint that contains them is durable. Delivery tags increase monotonically per channel and
//! deliveries are handed off in tag order, so a cumulative `basic.ack(multiple)` at the highest
//! handed-off tag covered by a committed checkpoint acknowledges exactly the committed messages.
//! A lost channel ends this generation: the broker requeues every unacknowledged delivery and
//! the engine restarts from its last durable checkpoint (at-least-once; replay may duplicate).

use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use futures_util::StreamExt;
use lapin::message::Delivery;
use lapin::options::{BasicAckOptions, BasicConsumeOptions, BasicQosOptions, BasicRejectOptions};
use lapin::types::ShortString;
use lapin::{Channel, Connection, Consumer};
use tokio::sync::{mpsc, Notify};

use super::config::{DecodeErrorPolicy, RabbitMqSourceConfig};
use super::connection::{close, connect, prepare_queue};
use super::metrics::{saturating, SourceMetrics};
use crate::checkpoint::SourceCheckpoint;
use crate::config::ConnectorConfig;
use crate::connector::{
    ConnectorTaskAdmission, ConnectorTaskOwner, ConnectorTaskTracker, SourceBatch, SourceConnector,
    SourceConsistency, SourceContract, SourceInputMode, SourcePosition, SourceStart,
    SourceTopology,
};
use crate::error::{ConnectorError, SerdeError};
use crate::serde::{self, Format, RecordDeserializer};

const GENERATION: &str = "generation";
const DELIVERY_TAG: &str = "delivery_tag";
const CLOSE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// RabbitMQ AMQP 0-9-1 queue source.
pub struct RabbitMqSource {
    schema: SchemaRef,
    registry: Option<prometheus::Registry>,
    config: Option<RabbitMqSourceConfig>,
    running: Option<Running>,
    failure: Option<String>,
    data_ready: Arc<Notify>,
    task_owner: ConnectorTaskOwner,
    task_tracker: ConnectorTaskTracker,
}

/// Resources of one channel generation; dropping it closes the channel and connection, which
/// makes the broker requeue every unacknowledged delivery.
struct Running {
    connection: Connection,
    channel: Channel,
    deliveries: mpsc::Receiver<Result<Delivery, String>>,
    reader: tokio::task::JoinHandle<()>,
    carry: Option<Delivery>,
    decoder: Arc<Decoder>,
    ledger: AckLedger,
    metrics: SourceMetrics,
    tasks: ConnectorTaskAdmission,
}

impl Drop for Running {
    fn drop(&mut self) {
        // The reader owns the consumer, which keeps the channel open until it is dropped.
        self.reader.abort();
    }
}

/// Positions of one channel generation, in delivery tags.
#[derive(Debug)]
struct AckLedger {
    generation: u64,
    handed_off: u64,
    acked: u64,
    /// `(last tag, deliveries)` per handed-off batch not yet acknowledged.
    batches: VecDeque<(u64, usize)>,
}

impl AckLedger {
    fn new(generation: u64) -> Self {
        Self {
            generation,
            handed_off: 0,
            acked: 0,
            batches: VecDeque::new(),
        }
    }

    fn hand_off(&mut self, last_tag: u64, deliveries: usize) {
        debug_assert!(
            last_tag > self.handed_off,
            "delivery tags increase per channel"
        );
        self.handed_off = last_tag;
        self.batches.push_back((last_tag, deliveries));
    }

    fn unacked(&self) -> usize {
        self.batches.iter().map(|(_, count)| count).sum()
    }

    /// The tag to acknowledge cumulatively for a committed cursor, if any.
    ///
    /// A cursor from another generation names a channel that no longer exists; its deliveries
    /// were requeued when that channel closed, so there is nothing to acknowledge.
    fn commit_target(&self, cursor: &SourceCheckpoint) -> Result<Option<u64>, ConnectorError> {
        let (Some(generation), Some(tag)) = (
            cursor.get_offset(GENERATION),
            cursor.get_offset(DELIVERY_TAG),
        ) else {
            return Ok(None);
        };
        let generation = u64::from_str_radix(generation, 16).map_err(|_| corrupt_cursor())?;
        let tag: u64 = tag.parse().map_err(|_| corrupt_cursor())?;
        if generation != self.generation || tag <= self.acked {
            return Ok(None);
        }
        if tag > self.handed_off {
            return Err(ConnectorError::InvalidState {
                expected: format!("a committed delivery tag at most {}", self.handed_off),
                actual: format!("committed delivery tag {tag}"),
            });
        }
        Ok(Some(tag))
    }

    /// Record a cumulative acknowledgement, returning how many deliveries it settled.
    fn acknowledge(&mut self, tag: u64) -> usize {
        self.acked = tag;
        let mut settled = 0;
        while self.batches.front().is_some_and(|(last, _)| *last <= tag) {
            settled += self.batches.pop_front().map_or(0, |(_, count)| count);
        }
        settled
    }

    fn cursor(&self) -> SourceCheckpoint {
        let mut cursor = SourceCheckpoint::new();
        cursor.set_offset(GENERATION, format!("{:016x}", self.generation));
        cursor.set_offset(DELIVERY_TAG, self.handed_off.to_string());
        cursor
    }
}

fn corrupt_cursor() -> ConnectorError {
    ConnectorError::InvalidState {
        expected: "a RabbitMQ source cursor".into(),
        actual: "an unparseable generation or delivery tag".into(),
    }
}

/// Payload codec plus the metadata columns appended after it.
struct Decoder {
    deserializer: Box<dyn RecordDeserializer>,
    payload: SchemaRef,
    output: SchemaRef,
    config: RabbitMqSourceConfig,
}

/// One decoded poll: the output batch and the tags, ascending, of messages the codec rejected.
struct Decoded {
    records: Option<RecordBatch>,
    rejected: Vec<u64>,
}

impl Decoder {
    fn decode(&self, deliveries: &[Delivery]) -> Result<Decoded, ConnectorError> {
        let payloads: Vec<&[u8]> = deliveries
            .iter()
            .map(|delivery| delivery.data.as_slice())
            .collect();
        match self
            .deserializer
            .deserialize_batch(&payloads, &self.payload)
        {
            Ok(batch) => {
                let all: Vec<&Delivery> = deliveries.iter().collect();
                let counts = if self.config.metadata.is_empty() || batch.num_rows() == all.len() {
                    None
                } else {
                    Some(self.row_counts(&payloads)?)
                };
                let records = self.with_metadata(batch, &all, counts.as_deref())?;
                Ok(Decoded {
                    records: Some(records),
                    rejected: Vec::new(),
                })
            }
            Err(error) if self.config.on_decode_error == DecodeErrorPolicy::Fail => {
                Err(ConnectorError::Serde(error))
            }
            Err(_) => self.isolate(deliveries),
        }
    }

    /// Decode each message alone so undecodable ones can be rejected and the rest handed off.
    fn isolate(&self, deliveries: &[Delivery]) -> Result<Decoded, ConnectorError> {
        let mut good = Vec::with_capacity(deliveries.len());
        let mut batches = Vec::with_capacity(deliveries.len());
        let mut counts = Vec::with_capacity(deliveries.len());
        let mut rejected = Vec::new();
        for delivery in deliveries {
            match self
                .deserializer
                .deserialize_batch(&[delivery.data.as_slice()], &self.payload)
            {
                Ok(batch) => {
                    counts.push(batch.num_rows());
                    batches.push(batch);
                    good.push(delivery);
                }
                Err(_) => rejected.push(delivery.delivery_tag),
            }
        }
        if good.is_empty() {
            return Ok(Decoded {
                records: None,
                rejected,
            });
        }
        let payload =
            arrow_select::concat::concat_batches(&self.payload, &batches).map_err(|error| {
                ConnectorError::Serde(SerdeError::MalformedInput(error.to_string()))
            })?;
        let records = self.with_metadata(payload, &good, Some(&counts))?;
        Ok(Decoded {
            records: Some(records),
            rejected,
        })
    }

    fn row_counts(&self, payloads: &[&[u8]]) -> Result<Vec<usize>, ConnectorError> {
        payloads
            .iter()
            .map(|payload| {
                self.deserializer
                    .deserialize_batch(&[payload], &self.payload)
                    .map(|batch| batch.num_rows())
                    .map_err(ConnectorError::Serde)
            })
            .collect()
    }

    /// Append metadata, repeating a message's values once per row its payload decoded to.
    fn with_metadata(
        &self,
        payload: RecordBatch,
        deliveries: &[&Delivery],
        counts: Option<&[usize]>,
    ) -> Result<RecordBatch, ConnectorError> {
        if self.config.metadata.is_empty() {
            return Ok(payload);
        }
        let per_row: Vec<&Delivery> = match counts {
            None => deliveries.to_vec(),
            Some(counts) => deliveries
                .iter()
                .zip(counts)
                .flat_map(|(delivery, count)| std::iter::repeat_n(*delivery, *count))
                .collect(),
        };
        if per_row.len() != payload.num_rows() {
            return Err(ConnectorError::Serde(SerdeError::RecordCountMismatch {
                expected: per_row.len(),
                got: payload.num_rows(),
            }));
        }
        let mut columns: Vec<ArrayRef> = payload.columns().to_vec();
        columns.extend(self.config.metadata.arrays(&per_row)?);
        RecordBatch::try_new(Arc::clone(&self.output), columns)
            .map_err(|error| ConnectorError::Serde(SerdeError::MalformedInput(error.to_string())))
    }
}

impl RabbitMqSource {
    /// Create an unstarted source; metrics register on `registry` at start.
    #[must_use]
    pub fn new(schema: SchemaRef, registry: Option<&prometheus::Registry>) -> Self {
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        Self {
            schema,
            registry: registry.cloned(),
            config: None,
            running: None,
            failure: None,
            data_ready: Arc::new(Notify::new()),
            task_owner,
            task_tracker,
        }
    }

    /// End this generation; every later poll repeats the cause as a terminal error.
    fn fail(&mut self, cause: String) -> ConnectorError {
        tracing::error!(cause = %cause, "RabbitMQ source generation ended");
        self.running = None;
        self.failure.get_or_insert(cause.clone());
        terminal(cause)
    }

    /// The payload codec and the schemas of the committed contract.
    #[cfg_attr(not(feature = "fpml"), allow(clippy::unused_async))]
    async fn decoder(
        &self,
        config: &ConnectorConfig,
        parsed: &RabbitMqSourceConfig,
    ) -> Result<Decoder, ConnectorError> {
        #[cfg(feature = "fpml")]
        if parsed.format == Format::Fpml {
            use crate::schema::traits::FormatDecoder;
            let guard = self.task_owner.track().ok_or_else(retired)?;
            let decoder = crate::fpml::message::start_decoder(
                config,
                config.schema_binding(),
                "RabbitMQ",
                self.registry.as_ref(),
                guard,
            )
            .await?;
            let payload = decoder.output_schema();
            parsed.metadata.validate_payload(&payload)?;
            return Ok(Decoder {
                deserializer: Box::new(crate::fpml::message::MessageDecoder(Arc::new(decoder))),
                output: parsed.metadata.output_schema(&payload),
                payload,
                config: parsed.clone(),
            });
        }
        let (payload, output) = self.resolve_output(config, parsed)?;
        Ok(Decoder {
            deserializer: serde::create_deserializer(parsed.format)?,
            payload,
            output,
            config: parsed.clone(),
        })
    }

    fn resolve_output(
        &self,
        config: &ConnectorConfig,
        parsed: &RabbitMqSourceConfig,
    ) -> Result<(SchemaRef, SchemaRef), ConnectorError> {
        let declared = config
            .arrow_schema()
            .or_else(|| (!self.schema.fields().is_empty()).then(|| Arc::clone(&self.schema)));
        let payload = declared
            .map(|output| parsed.metadata.payload_schema(&output))
            .transpose()?;
        let binding = serde::schema_contract::reader_binding(config, parsed.format, payload)?;
        let payload = Arc::new(binding.logical);
        parsed.metadata.validate_payload(&payload)?;
        let output = parsed.metadata.output_schema(&payload);
        if config
            .schema_binding()
            .is_some_and(|expected| expected.logical != *output)
        {
            return Err(ConnectorError::SchemaMismatch(
                "RabbitMQ reader differs from its committed contract".into(),
            ));
        }
        Ok((payload, output))
    }

    async fn open(
        &self,
        parsed: &RabbitMqSourceConfig,
        decoder: Decoder,
        metrics: SourceMetrics,
    ) -> Result<Running, ConnectorError> {
        let connection = connect(&parsed.connection).await?;
        let channel = connection.create_channel().await.map_err(|error| {
            ConnectorError::ConnectionFailed(format!("RabbitMQ channel: {error}"))
        })?;
        prepare_queue(
            &channel,
            &parsed.queue,
            parsed.declare.as_ref(),
            parsed.binding.as_ref(),
        )
        .await?;
        channel
            .basic_qos(parsed.prefetch, BasicQosOptions { global: false })
            .await
            .map_err(|error| ConnectorError::ConnectionFailed(format!("basic.qos: {error}")))?;
        let consumer = channel
            .basic_consume(
                parsed.queue.as_str().into(),
                parsed.consumer_tag.clone().unwrap_or_default().into(),
                BasicConsumeOptions {
                    no_ack: false,
                    exclusive: parsed.exclusive,
                    ..BasicConsumeOptions::default()
                },
                parsed.consumer_arguments.clone(),
            )
            .await
            .map_err(|error| {
                ConnectorError::ConnectionFailed(format!(
                    "basic.consume on '{}': {error}",
                    parsed.queue
                ))
            })?;
        let (sender, deliveries) = mpsc::channel(usize::from(parsed.prefetch));
        let guard = self.task_owner.track().ok_or_else(retired)?;
        let data_ready = Arc::clone(&self.data_ready);
        let reader = tokio::spawn(async move {
            let _guard = guard;
            read_deliveries(consumer, sender, data_ready).await;
        });
        Ok(Running {
            connection,
            channel,
            deliveries,
            reader,
            carry: None,
            decoder: Arc::new(decoder),
            ledger: AckLedger::new(rand::random()),
            metrics,
            tasks: self.task_owner.admission(),
        })
    }
}

/// Forward deliveries in tag order. Capacity equals the prefetch window, which the broker never
/// exceeds while every delivery stays unacknowledged, so the client buffer and this channel
/// together hold at most `prefetch.count` messages.
async fn read_deliveries(
    mut consumer: Consumer,
    sender: mpsc::Sender<Result<Delivery, String>>,
    data_ready: Arc<Notify>,
) {
    while let Some(item) = consumer.next().await {
        let terminal = item.is_err();
        if sender
            .send(item.map_err(|error| error.to_string()))
            .await
            .is_err()
        {
            return;
        }
        data_ready.notify_one();
        if terminal {
            return;
        }
    }
    let _ = sender
        .send(Err(
            "the broker cancelled the consumer (queue deleted or consumer timeout)".into(),
        ))
        .await;
    data_ready.notify_one();
}

fn retired() -> ConnectorError {
    ConnectorError::Internal("RabbitMQ source task generation is retired".into())
}

fn terminal(cause: String) -> ConnectorError {
    ConnectorError::InvalidState {
        expected: "a live RabbitMQ channel".into(),
        actual: cause,
    }
}

enum Collected {
    Deliveries(Vec<Delivery>),
    Lost(String),
}

impl Running {
    /// Take ready deliveries within the message-count and payload-byte bounds.
    fn collect(&mut self, max_records: usize) -> Collected {
        let max_batch_bytes = self.decoder.config.max_batch_bytes;
        let mut deliveries = Vec::new();
        let mut bytes = 0_usize;
        while deliveries.len() < max_records.max(1) {
            let delivery = match self.carry.take() {
                Some(delivery) => delivery,
                None => match self.deliveries.try_recv() {
                    Ok(Ok(delivery)) => delivery,
                    Ok(Err(cause)) => return Collected::Lost(cause),
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        return Collected::Lost("the delivery reader stopped".into())
                    }
                },
            };
            if !deliveries.is_empty() && bytes.saturating_add(delivery.data.len()) > max_batch_bytes
            {
                self.carry = Some(delivery);
                break;
            }
            bytes = bytes.saturating_add(delivery.data.len());
            deliveries.push(delivery);
        }
        Collected::Deliveries(deliveries)
    }

    /// `FpML` validation blocks, so it runs on a tracked blocking thread; other codecs decode
    /// inline.
    async fn decode(&self, deliveries: Vec<Delivery>) -> Result<(Vec<Delivery>, Decoded), String> {
        if self.decoder.config.format != Format::Fpml {
            let decoded = self
                .decoder
                .decode(&deliveries)
                .map_err(|error| format!("decode failed: {error}"))?;
            return Ok((deliveries, decoded));
        }
        let guard = self.tasks.track().ok_or_else(|| retired().to_string())?;
        let codec = Arc::clone(&self.decoder);
        let (deliveries, result) = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let result = codec.decode(&deliveries);
            (deliveries, result)
        })
        .await
        .map_err(|error| format!("decode task failed: {error}"))?;
        let decoded = result.map_err(|error| format!("decode failed: {error}"))?;
        Ok((deliveries, decoded))
    }

    /// Decode and hand off ready deliveries. Any error ends the generation.
    async fn poll(&mut self, max_records: usize) -> Result<Option<SourceBatch>, String> {
        let deliveries = match self.collect(max_records) {
            Collected::Deliveries(deliveries) if deliveries.is_empty() => return Ok(None),
            Collected::Deliveries(deliveries) => deliveries,
            Collected::Lost(cause) => return Err(cause),
        };
        if deliveries.iter().any(is_stream_delivery) {
            return Err(
                "the queue is a stream queue; stream queues do not redeliver \
                        unacknowledged messages and are not supported by this source"
                    .into(),
            );
        }
        let config = &self.decoder.config;
        let (accepted, oversized): (Vec<Delivery>, Vec<Delivery>) = deliveries
            .into_iter()
            .partition(|delivery| delivery.data.len() <= config.max_message_bytes);
        if let (Some(first), DecodeErrorPolicy::Fail) = (oversized.first(), config.on_decode_error)
        {
            return Err(format!(
                "a {}-byte message exceeds max.message.bytes = {}",
                first.data.len(),
                config.max_message_bytes
            ));
        }
        let (accepted, decoded) = self.decode(accepted).await?;
        let mut rejected: Vec<u64> = oversized
            .iter()
            .map(|delivery| delivery.delivery_tag)
            .collect();
        rejected.extend(&decoded.rejected);
        for tag in &rejected {
            self.channel
                .basic_reject(*tag, BasicRejectOptions { requeue: false })
                .await
                .map_err(|error| format!("basic.reject failed: {error}"))?;
        }
        self.metrics.rejected.inc_by(rejected.len() as u64);
        let handed: Vec<&Delivery> = accepted
            .iter()
            .filter(|delivery| {
                decoded
                    .rejected
                    .binary_search(&delivery.delivery_tag)
                    .is_err()
            })
            .collect();
        let Some(last) = handed.last() else {
            return Ok(None);
        };
        self.ledger.hand_off(last.delivery_tag, handed.len());
        self.metrics.messages.inc_by(handed.len() as u64);
        self.metrics.bytes.inc_by(
            handed
                .iter()
                .map(|delivery| delivery.data.len() as u64)
                .sum(),
        );
        self.metrics.redelivered.inc_by(
            handed
                .iter()
                .filter(|delivery| delivery.redelivered)
                .count() as u64,
        );
        self.metrics.unacked.set(saturating(self.ledger.unacked()));
        let records = decoded.records.filter(|records| records.num_rows() > 0);
        if let Some(records) = &records {
            self.metrics.rows.inc_by(records.num_rows() as u64);
        }
        Ok(records.map(SourceBatch::new))
    }

    /// Cumulatively acknowledge the deliveries a committed cursor covers.
    async fn acknowledge(&mut self, cursor: &SourceCheckpoint) -> Result<(), String> {
        let Some(tag) = self
            .ledger
            .commit_target(cursor)
            .map_err(|error| error.to_string())?
        else {
            return Ok(());
        };
        self.channel
            .basic_ack(tag, BasicAckOptions { multiple: true })
            .await
            .map_err(|error| format!("basic.ack: {error}"))?;
        let settled = self.ledger.acknowledge(tag);
        self.metrics.acked.inc_by(settled as u64);
        self.metrics.unacked.set(saturating(self.ledger.unacked()));
        Ok(())
    }
}

/// Stream queues attach `x-stream-offset` to every delivery and do not redeliver unacknowledged
/// messages, so this source's recovery contract does not hold for them.
fn is_stream_delivery(delivery: &Delivery) -> bool {
    delivery
        .properties
        .headers()
        .as_ref()
        .is_some_and(|headers| {
            headers
                .inner()
                .contains_key(&ShortString::from("x-stream-offset"))
        })
}

#[async_trait]
impl SourceConnector for RabbitMqSource {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    async fn resolve_schema(
        &mut self,
        config: &ConnectorConfig,
        explicit: Option<SchemaRef>,
    ) -> Result<crate::schema::resolution::SchemaBinding, ConnectorError> {
        use crate::schema::resolution::{logical_binding, SchemaDirection};
        let parsed = RabbitMqSourceConfig::from_config(config)?;
        #[cfg(feature = "fpml")]
        if parsed.format == Format::Fpml {
            return crate::fpml::message::source_binding(config, explicit, "RabbitMQ", |payload| {
                parsed.metadata.validate_payload(payload)?;
                Ok(parsed.metadata.output_schema(payload))
            })
            .await;
        }
        let reader = serde::schema_contract::reader_binding(config, parsed.format, explicit)?;
        let payload = Arc::new(reader.logical);
        parsed.metadata.validate_payload(&payload)?;
        logical_binding(
            config,
            SchemaDirection::Source,
            reader.origin,
            &parsed.metadata.output_schema(&payload),
        )
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        RabbitMqSourceConfig::from_config(config)?;
        // The queue releases a message only when the checkpoint containing it is durable.
        // Exactly-once is not certified: broker acknowledgement and sink commit are separate.
        Ok(SourceContract::new(
            SourceConsistency::CommitCoupled,
            SourceTopology::Singleton,
            SourceInputMode::AppendOnly,
        ))
    }

    fn recovery_identity_options(
        &self,
        config: &ConnectorConfig,
    ) -> Result<Option<std::collections::BTreeMap<String, String>>, ConnectorError> {
        Ok(Some(RabbitMqSourceConfig::from_config(config)?.identity()))
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        let (config, position, _) = request.into_parts();
        if matches!(position, SourcePosition::Initialized { .. }) {
            return Err(ConnectorError::ConfigurationError(
                "RabbitMQ has no sealed topology startup contract".into(),
            ));
        }
        // A resume needs no positioning: every delivery the committed cut did not cover is still
        // unacknowledged, and the broker redelivered it when the previous channel closed.
        let parsed = RabbitMqSourceConfig::from_config(&config)?;
        let name = config.get("laminar.source.name").unwrap_or("rabbitmq");
        let metrics = SourceMetrics::new(name, self.registry.as_ref())?;
        let decoder = self.decoder(&config, &parsed).await?;
        let output = Arc::clone(&decoder.output);
        let running = self.open(&parsed, decoder, metrics).await?;
        tracing::info!(
            queue = %parsed.queue,
            prefetch = parsed.prefetch,
            endpoints = ?parsed.connection,
            "RabbitMQ source consuming"
        );
        self.schema = output;
        self.config = Some(parsed);
        self.failure = None;
        self.running = Some(running);
        Ok(())
    }

    async fn poll_batch(
        &mut self,
        max_records: usize,
    ) -> Result<Option<SourceBatch>, ConnectorError> {
        if let Some(cause) = &self.failure {
            return Err(terminal(cause.clone()));
        }
        let Some(running) = self.running.as_mut() else {
            return Ok(None);
        };
        match running.poll(max_records).await {
            Ok(batch) => Ok(batch),
            Err(cause) => Err(self.fail(cause)),
        }
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        self.running
            .as_ref()
            .map_or_else(SourceCheckpoint::new, |running| running.ledger.cursor())
    }

    async fn notify_epoch_committed(
        &mut self,
        epoch: u64,
        checkpoint: &SourceCheckpoint,
    ) -> Result<(), ConnectorError> {
        let Some(running) = self.running.as_mut() else {
            return Ok(());
        };
        match running.acknowledge(checkpoint).await {
            Ok(()) => Ok(()),
            Err(cause) => Err(self.fail(format!(
                "acknowledging committed epoch {epoch} failed; its deliveries will be \
                 redelivered: {cause}"
            ))),
        }
    }

    fn data_ready_notify(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.data_ready))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        let Some(running) = self.running.take() else {
            return Ok(());
        };
        running.reader.abort();
        close(&running.channel, &running.connection, CLOSE_TIMEOUT).await
    }
}

#[cfg(test)]
mod tests;

//! Broker queue ownership for ordinary arrival and deterministic partition rounds.

use std::collections::VecDeque;

use rdkafka::consumer::stream_consumer::StreamPartitionQueue;
use rdkafka::error::KafkaError;

use super::super::{ConnectorError, KafkaReplayOrder, KafkaSourceConfig};
use super::{
    Arc, Consumer, KafkaPartitionRoutes, KafkaPayload, LaminarConsumerContext, Message,
    StreamConsumer,
};

#[derive(Debug, thiserror::Error)]
pub(super) enum KafkaInputError {
    #[error(transparent)]
    Broker(#[from] KafkaError),
    #[error(transparent)]
    Contract(#[from] ConnectorError),
}

impl KafkaInputError {
    pub(super) fn is_transient(&self) -> bool {
        matches!(self, Self::Broker(error) if super::kafka_reader_error_is_transient(error))
    }
}

pub(super) struct KafkaReaderInput {
    vnode_routing: bool,
    routes: KafkaPartitionRoutes,
    capture_headers: bool,
    cached_topic: Arc<str>,
    cached_topic_routes: Option<Arc<[u32]>>,
    rounds: Option<PartitionRoundInput>,
}

struct PartitionInput {
    topic: String,
    partition: i32,
    queue: StreamPartitionQueue<LaminarConsumerContext>,
    buffered: VecDeque<KafkaPayload>,
}

struct PartitionRoundInput {
    inputs: Vec<PartitionInput>,
    initialized: bool,
    partition_count: usize,
    next: usize,
    round_bytes: usize,
    buffered_count: usize,
    buffered_bytes: usize,
    max_records: usize,
    max_bytes: usize,
}

impl KafkaReaderInput {
    pub(super) fn replay_order(&self) -> KafkaReplayOrder {
        match self.rounds {
            Some(_) => KafkaReplayOrder::PartitionRounds,
            None => KafkaReplayOrder::Unspecified,
        }
    }

    pub(super) fn new(
        config: &KafkaSourceConfig,
        vnode_routing: bool,
        routes: KafkaPartitionRoutes,
        partition_count: usize,
    ) -> Self {
        Self {
            vnode_routing,
            routes,
            capture_headers: config.include_headers,
            cached_topic: Arc::from(""),
            cached_topic_routes: None,
            rounds: (config.replay_order == KafkaReplayOrder::PartitionRounds).then(|| {
                PartitionRoundInput {
                    inputs: Vec::new(),
                    initialized: false,
                    partition_count,
                    next: 0,
                    round_bytes: 0,
                    buffered_count: 0,
                    buffered_bytes: 0,
                    max_records: config.reader_channel_capacity,
                    max_bytes: usize::try_from(config.fetch_max_bytes.unwrap_or(50 * 1024 * 1024))
                        .unwrap_or(0),
                }
            }),
        }
    }

    pub(super) fn reset(&mut self) {
        if let Some(rounds) = &mut self.rounds {
            rounds.inputs.clear();
            rounds.initialized = false;
            rounds.next = 0;
            rounds.round_bytes = 0;
            rounds.buffered_count = 0;
            rounds.buffered_bytes = 0;
        }
    }

    pub(super) async fn recv(
        &mut self,
        consumer: &Arc<StreamConsumer<LaminarConsumerContext>>,
    ) -> Result<Option<KafkaPayload>, KafkaInputError> {
        let Some(rounds) = &mut self.rounds else {
            let message = consumer.recv().await?;
            return Ok(super::build_reader_payload(
                &message,
                self.vnode_routing,
                &self.routes,
                self.capture_headers,
                &mut self.cached_topic,
                &mut self.cached_topic_routes,
            )?);
        };
        rounds.initialize(consumer)?;
        if let Some(payload) = rounds.take_buffered()? {
            return Ok(Some(payload));
        }
        // Poll the main queue first: pre-split messages precede their partition queue's data.
        // It must also remain serviced for librdkafka control/error events while a partition idles.
        let (message, main_queue) = tokio::select! {
            biased;
            message = consumer.recv() => (message?, true),
            message = async {
                match rounds.inputs.get(rounds.next) {
                    Some(input) => input.queue.recv().await,
                    None => std::future::pending().await,
                }
            } => (message?, false),
        };
        if message
            .payload()
            .is_some_and(|bytes| bytes.len() > rounds.max_bytes)
        {
            return Err(ConnectorError::Internal(
                "Kafka partition round exceeds fetch.max.bytes".into(),
            )
            .into());
        }
        let payload = super::build_reader_payload(
            &message,
            self.vnode_routing,
            &self.routes,
            self.capture_headers,
            &mut self.cached_topic,
            &mut self.cached_topic_routes,
        )?;
        drop(message);
        match payload {
            Some(payload) => Ok(rounds.accept(payload, main_queue)?),
            None => Ok(None), // Tombstones do not consume a round slot.
        }
    }
}

impl PartitionRoundInput {
    fn initialize(
        &mut self,
        consumer: &Arc<StreamConsumer<LaminarConsumerContext>>,
    ) -> Result<(), KafkaInputError> {
        if self.initialized {
            return Ok(());
        }
        let assignment = consumer.assignment()?;
        if assignment.count() != 0 && assignment.count() != self.partition_count {
            return Err(ConnectorError::Internal(
                "Kafka partition_rounds requires the complete inventory on one owner".into(),
            )
            .into());
        }
        let mut inputs = assignment
            .elements()
            .iter()
            .map(|entry| (entry.topic().to_string(), entry.partition()))
            .collect::<Vec<_>>();
        inputs.sort_unstable();
        for (topic, partition) in inputs {
            let queue = consumer
                .split_partition_queue(&topic, partition)
                .ok_or_else(|| {
                    ConnectorError::Internal(format!(
                        "Kafka could not split partition queue '{topic}-{partition}'"
                    ))
                })?;
            self.inputs.push(PartitionInput {
                topic,
                partition,
                queue,
                buffered: VecDeque::new(),
            });
        }
        self.initialized = true;
        Ok(())
    }

    fn take_buffered(&mut self) -> Result<Option<KafkaPayload>, ConnectorError> {
        let Some(input) = self.inputs.get_mut(self.next) else {
            return Ok(None);
        };
        let Some(payload) = input.buffered.pop_front() else {
            return Ok(None);
        };
        self.buffered_count -= 1;
        self.buffered_bytes -= payload_bytes(&payload);
        self.advance(&payload)?;
        Ok(Some(payload))
    }

    fn accept(
        &mut self,
        payload: KafkaPayload,
        main_queue: bool,
    ) -> Result<Option<KafkaPayload>, ConnectorError> {
        let index = self
            .inputs
            .binary_search_by(|input| {
                (input.topic.as_str(), input.partition)
                    .cmp(&(payload.topic.as_ref(), payload.partition))
            })
            .map_err(|_| {
                ConnectorError::Internal(
                    "Kafka round payload is outside its assigned inventory".into(),
                )
            })?;
        if index == self.next {
            self.advance(&payload)?;
            return Ok(Some(payload));
        }
        if !main_queue {
            return Err(ConnectorError::Internal(
                "Kafka partition queue returned another partition".into(),
            ));
        }
        let bytes = payload_bytes(&payload);
        if self.buffered_count >= self.max_records
            || bytes > self.max_bytes.saturating_sub(self.buffered_bytes)
        {
            return Err(ConnectorError::Internal(
                "Kafka pre-split round buffer exceeds its record/byte bound".into(),
            ));
        }
        self.inputs[index].buffered.push_back(payload);
        self.buffered_count += 1;
        self.buffered_bytes += bytes;
        Ok(None)
    }

    fn advance(&mut self, payload: &KafkaPayload) -> Result<(), ConnectorError> {
        let bytes = payload_bytes(payload);
        if bytes > self.max_bytes.saturating_sub(self.round_bytes) {
            return Err(ConnectorError::Internal(
                "Kafka partition round exceeds fetch.max.bytes".into(),
            ));
        }
        self.round_bytes += bytes;
        self.next += 1;
        if self.next == self.inputs.len() {
            self.next = 0;
            self.round_bytes = 0;
        }
        Ok(())
    }
}

fn payload_bytes(payload: &KafkaPayload) -> usize {
    payload
        .data
        .len()
        .saturating_add(payload.headers_json.as_ref().map_or(0, String::len))
}

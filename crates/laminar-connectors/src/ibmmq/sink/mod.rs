//! IBM MQ queue sink: `MQPUT` under syncpoint and `MQCMIT` per unit of work.
//!
//! A batch is put in units of at most `max.uow.messages` messages; each unit commits before the
//! write returns, so a successful write is durable on the queue manager for persistent messages.
//! A failed put backs its unit out. A lost commit outcome, or a failure after an earlier unit of
//! the same batch committed, ends this sink generation with an unknown outcome; replay from the
//! last checkpoint may then put duplicates (at-least-once).

mod message;

use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use mqi::prelude::*;
use mqi::types::{MessageFormat, MQCMHO, MQPMO, MQSMPO};
use mqi::{constants, structs, Properties};

use super::config::{IbmMqSinkConfig, Persistence};
use super::library;
use super::metrics::SinkMetrics;
use super::session::{classify, Session};
use super::worker::Worker;
use crate::config::ConnectorConfig;
use crate::connector::{
    ConnectorTaskOwner, ConnectorTaskTracker, SinkConnector, SinkConsistency, SinkContract,
    SinkInputMode, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;
use crate::serde::RecordSerializer;
use message::{base_descriptor, payload_schema, Outgoing};

const CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

/// IBM MQ queue sink.
pub struct IbmMqSink {
    schema: SchemaRef,
    registry: Option<prometheus::Registry>,
    config: Option<IbmMqSinkConfig>,
    writer: Option<Writer>,
    worker: Option<Worker<Session>>,
    metrics: Option<SinkMetrics>,
    failure: Option<String>,
    task_owner: ConnectorTaskOwner,
    task_tracker: ConnectorTaskTracker,
}

struct Writer {
    serializer: Box<dyn RecordSerializer>,
    payload_indices: Vec<usize>,
    base: libmqm_sys::MQMD,
}

/// Why a put batch stopped, and whether any of it was already committed.
struct PutFailure {
    committed: usize,
    error: ConnectorError,
}

/// Put `messages` in units of `unit` messages, committing each unit.
fn put_all(session: &Session, messages: &[Outgoing], unit: usize) -> Result<usize, PutFailure> {
    let mut committed = 0;
    for part in messages.chunks(unit) {
        for outgoing in part {
            if let Err(error) = put_one(session, outgoing) {
                let backed_out = session.backout();
                let error = match backed_out {
                    Ok(()) => error,
                    Err(backout) => ConnectorError::outcome_unknown(
                        format!("{error}; MQBACK also failed: {backout}"),
                        true,
                    ),
                };
                return Err(PutFailure { committed, error });
            }
        }
        session
            .commit()
            .map_err(|error| PutFailure { committed, error })?;
        committed += part.len();
    }
    Ok(committed)
}

fn put_one(session: &Session, outgoing: &Outgoing) -> Result<(), ConnectorError> {
    let handle = Properties::new(session.connection(), MQCMHO::default())
        .map_err(|error| classify("MQCRTMH", &error))?;
    for (name, value) in &outgoing.properties {
        handle
            .set_property(name.as_str(), value, MQSMPO::default())
            .warn_as_error()
            .map_err(|error| classify(&format!("MQSETMP '{name}'"), &error))?;
    }
    let mut options: MQPMO = constants::MQPMO_SYNCPOINT | constants::MQPMO_FAIL_IF_QUIESCING;
    if outgoing.new_message_id {
        options |= constants::MQPMO_NEW_MSG_ID;
    }
    let md = structs::MQMD::new(outgoing.md);
    let format = MessageFormat::from_mqmd(&md);
    let mut handle = handle;
    session
        .queue
        .put_message(
            &(options, md, &mut handle),
            &(outgoing.data.as_slice(), format),
        )
        .warn_as_error()
        .map_err(|error| match error.2 {
            constants::MQRC_MSG_TOO_BIG_FOR_Q | constants::MQRC_MSG_TOO_BIG_FOR_Q_MGR => {
                ConnectorError::ConfigurationError(format!(
                    "MQPUT: a {}-byte message exceeds the queue's MAXMSGL: {error}",
                    outgoing.data.len()
                ))
            }
            _ => classify("MQPUT", &error),
        })
}

impl IbmMqSink {
    /// Create an unopened sink; metrics register on `registry` at open.
    #[must_use]
    pub fn new(schema: SchemaRef, registry: Option<&prometheus::Registry>) -> Self {
        let (task_owner, task_tracker) = ConnectorTaskOwner::new();
        Self {
            schema,
            registry: registry.cloned(),
            config: None,
            writer: None,
            worker: None,
            metrics: None,
            failure: None,
            task_owner,
            task_tracker,
        }
    }

    fn writer(parsed: &IbmMqSinkConfig, input: &SchemaRef) -> Result<Writer, ConnectorError> {
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
            base: base_descriptor(parsed, &payload)?,
            payload_indices,
        })
    }

    fn fail(&mut self, error: ConnectorError) -> ConnectorError {
        tracing::error!(%error, "IBM MQ sink generation ended");
        self.worker = None;
        self.failure.get_or_insert(error.to_string());
        error
    }

    async fn put(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        if let Some(cause) = &self.failure {
            return Err(ConnectorError::InvalidState {
                expected: "a live IBM MQ sink".into(),
                actual: cause.clone(),
            });
        }
        let (Some(config), Some(writer), Some(worker), Some(metrics)) =
            (&self.config, &self.writer, &self.worker, &self.metrics)
        else {
            return Err(ConnectorError::InvalidState {
                expected: "an open IBM MQ sink".into(),
                actual: "open() has not completed".into(),
            });
        };
        let payload = batch
            .project(&writer.payload_indices)
            .map_err(|error| ConnectorError::SchemaMismatch(error.to_string()))?;
        let records = writer.serializer.serialize(&payload)?;
        if records.len() != batch.num_rows() {
            return Err(ConnectorError::Internal(format!(
                "IBM MQ serializer returned {} records for {} rows",
                records.len(),
                batch.num_rows()
            )));
        }
        let outgoing = message::messages(config, &writer.base, batch, records)?;
        let bytes: u64 = outgoing
            .iter()
            .map(|message| message.data.len() as u64)
            .sum();
        let count = outgoing.len();
        let unit = config.max_uow_messages;
        let result = worker
            .call(
                "MQPUT/MQCMIT",
                config.connection.operation_timeout,
                move |session| Ok(put_all(session, &outgoing, unit)),
            )
            .await?;
        match result {
            Ok(committed) => {
                metrics.put.inc_by(committed as u64);
                metrics.bytes.inc_by(bytes);
                metrics.commits.inc_by(committed.div_ceil(unit) as u64);
                Ok(WriteResult::new(count, bytes))
            }
            Err(PutFailure { committed, error }) => {
                metrics.backouts.inc();
                if committed > 0 || error.is_outcome_unknown() {
                    metrics.ambiguous.inc();
                    Err(ConnectorError::outcome_unknown(
                        format!(
                            "IBM MQ batch partially committed ({committed} of {count}): {error}"
                        ),
                        true,
                    ))
                } else {
                    Err(error)
                }
            }
        }
    }
}

#[async_trait]
impl SinkConnector for IbmMqSink {
    fn terminal_task_tracker(&self) -> Option<ConnectorTaskTracker> {
        Some(self.task_tracker.clone())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        let parsed = IbmMqSinkConfig::from_config(config)?;
        // Only an explicitly persistent message is durable at MQCMIT; DEFPSIST is not verified.
        let consistency = match parsed.persistence {
            Persistence::Persistent => SinkConsistency::DurableAtLeastOnce,
            Persistence::NotPersistent | Persistence::AsQueueDefinition => {
                SinkConsistency::Ephemeral
            }
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
        let parsed = IbmMqSinkConfig::from_config(config)?;
        Self::writer(&parsed, &input)?;
        logical_binding(config, SchemaDirection::Sink, SchemaOrigin::Query, &input)
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        let parsed = IbmMqSinkConfig::from_config(config)?;
        let input = config
            .arrow_schema()
            .or_else(|| {
                config
                    .schema_binding()
                    .map(|binding| std::sync::Arc::new(binding.logical.clone()))
            })
            .unwrap_or_else(|| self.schema.clone());
        let writer = Self::writer(&parsed, &input)?;
        let library = library::load(parsed.connection.client_library.as_deref())?;
        let metrics = SinkMetrics::new(&parsed.queue, self.registry.as_ref())?;
        let guard = self.task_owner.track().ok_or_else(|| {
            ConnectorError::Internal("IBM MQ sink task generation is retired".into())
        })?;
        let session_config = parsed.clone();
        let worker = Worker::start(
            &format!("ibmmq-sink-{}", parsed.queue),
            guard,
            parsed.connection.operation_timeout,
            move || {
                Session::open(
                    &session_config.connection,
                    library,
                    &session_config.queue,
                    session_config.target_queue_manager.as_deref(),
                    constants::MQOO_OUTPUT,
                )
            },
        )
        .await?;
        tracing::info!(
            queue = %parsed.queue,
            persistence = ?parsed.persistence,
            connection = ?parsed.connection,
            "IBM MQ sink putting under syncpoint"
        );
        self.schema = input;
        self.writer = Some(writer);
        self.worker = Some(worker);
        self.metrics = Some(metrics);
        self.config = Some(parsed);
        self.failure = None;
        Ok(())
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        match self.put(batch).await {
            Ok(result) => Ok(result),
            Err(error) if self.failure.is_some() => Err(error),
            Err(error) => Err(self.fail(error)),
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn suggested_write_timeout(&self) -> Duration {
        self.config
            .as_ref()
            .map_or(Duration::from_secs(30), |config| {
                config.connection.operation_timeout
            })
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        match self.worker.take() {
            Some(worker) => worker.stop(CLOSE_TIMEOUT).await,
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests;

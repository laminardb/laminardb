//! Worker-side source session: syncpoint gets, poison moves, and decoding into one batch.
//!
//! Every get in a poll joins the connection's single unit of work, which stays open until the
//! checkpoint containing the poll commits it. Messages the queue's backout settings mark as
//! poison, and (with `on.decode.error = backout-queue`) messages the codec rejects, are put to
//! `BOQNAME` in the same unit of work, so the move commits or backs out with the checkpoint.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use mqi::get::{GetAttr, GetConvert, GetParam, GetState, GetWait};
use mqi::prelude::*;
use mqi::properties::{Name, Value};
use mqi::result::{Completion, Error as MqError, ResultComp};
use mqi::string::CCSID;
use mqi::types::{MessageFormat, MessageId, QueueName, MQCMHO, MQGMO, MQIMPO, MQPMO};
use mqi::{constants, structs, MqStr, Properties};

use super::super::config::{DecodeErrorPolicy, IbmMqSourceConfig, MQ_MAX_MESSAGE_BYTES};
use super::super::metadata::{Received, RenderedProperties};
use super::super::session::{classify, Conn, Session};
use crate::error::{ConnectorError, SerdeError};
use crate::serde::RecordDeserializer;

/// Queue backout settings honoured by the source.
#[derive(Debug, Clone)]
pub(super) struct BackoutRoute {
    /// `BOTHRESH`: a message whose `BackoutCount` reaches it is poison.
    pub(super) threshold: i32,
    /// `BOQNAME`, a local queue on the same queue manager.
    pub(super) queue: String,
}

/// Limits of one poll, already reduced to the unit of work's remaining capacity.
pub(super) struct Limits {
    pub(super) messages: usize,
    pub(super) bytes: usize,
    pub(super) wait_ms: i32,
}

/// Outcome of one poll on the worker.
pub(super) struct Polled {
    pub(super) records: Option<RecordBatch>,
    /// Messages added to the unit of work, handed off or moved.
    pub(super) gotten: usize,
    pub(super) bytes: usize,
    pub(super) moved: usize,
    pub(super) redelivered: usize,
    /// `MQRC_SYNCPOINT_LIMIT_REACHED`: the queue manager's `MAXUMSGS` is reached.
    pub(super) uow_full: bool,
}

/// Session state owned by the worker thread.
pub(super) struct SourceSession {
    pub(super) session: Session,
    pub(super) route: Option<BackoutRoute>,
    pub(super) config: Arc<IbmMqSourceConfig>,
    pub(super) deserializer: Box<dyn RecordDeserializer>,
    pub(super) payload: SchemaRef,
    pub(super) output: SchemaRef,
    buffer: Vec<u8>,
    /// A poll got messages but its result never reached the source, so the unit of work holds
    /// messages no checkpoint contains. It must never commit.
    tainted: bool,
}

/// Every get joins the unit of work, never implicitly: syncpoint defaults differ by platform.
const GET_OPTIONS: MQGMO = constants::MQGMO_SYNCPOINT.union(constants::MQGMO_FAIL_IF_QUIESCING);

/// One retrieved message and the handle holding its properties.
struct Got<'c> {
    received: Received,
    handle: Properties<&'c Conn>,
}

impl SourceSession {
    pub(super) fn new(
        session: Session,
        route: Option<BackoutRoute>,
        config: Arc<IbmMqSourceConfig>,
        deserializer: Box<dyn RecordDeserializer>,
        payload: SchemaRef,
        output: SchemaRef,
    ) -> Self {
        let buffer = vec![0; config.max_message_bytes];
        Self {
            session,
            route,
            config,
            deserializer,
            payload,
            output,
            buffer,
            tainted: false,
        }
    }

    /// Record a poll whose result was lost after it added messages to the unit of work.
    pub(super) fn lost(&mut self, polled: &Result<Polled, ConnectorError>) {
        if polled.as_ref().map_or(true, |polled| polled.gotten > 0) {
            self.tainted = true;
        }
    }

    fn require_untainted(&self) -> Result<(), ConnectorError> {
        if self.tainted {
            return Err(ConnectorError::InvalidState {
                expected: "a unit of work holding only handed-off messages".into(),
                actual: "a cancelled poll left messages no checkpoint contains".into(),
            });
        }
        Ok(())
    }

    /// `MQCMIT` the unit of work, unless a lost poll tainted it.
    pub(super) fn commit(&self) -> Result<(), ConnectorError> {
        self.require_untainted()?;
        self.session.commit()
    }

    /// Get, classify, and decode up to `limits` of messages.
    pub(super) fn poll(&mut self, limits: &Limits) -> Result<Polled, ConnectorError> {
        self.require_untainted()?;
        let connection = self.session.connection();
        let mut polled = Polled {
            records: None,
            gotten: 0,
            bytes: 0,
            moved: 0,
            redelivered: 0,
            uow_full: false,
        };
        let mut accepted: Vec<Got<'_>> = Vec::new();
        while accepted.len() + polled.moved < limits.messages && polled.bytes < limits.bytes {
            let wait = if polled.gotten == 0 {
                GetWait::Wait(limits.wait_ms)
            } else {
                GetWait::NoWait
            };
            let mut handle = Properties::new(connection, MQCMHO::default())
                .map_err(|error| classify("MQCRTMH", &error))?;
            let convert = if self.config.convert {
                GetConvert::ConvertTo(CCSID(1208), constants::MQENC_NATIVE)
            } else {
                GetConvert::NoConvert
            };
            let got: ResultComp<Option<Delivered<'_>>> = self.session.queue.get_data_with(
                &(GET_OPTIONS, wait, convert, &mut handle),
                &mut self.buffer[..],
            );
            let (data, md) = match got {
                Ok(Completion(None, _)) => break,
                Ok(Completion(Some((_, (md, MessageLength(length)))), Some((reason, _))))
                    if reason == constants::MQRC_TRUNCATED_MSG_FAILED =>
                {
                    // The queue manager kept the message on the queue; nothing was received.
                    self.move_oversized(connection, &md, length, &mut handle)?;
                    polled.gotten += 1;
                    polled.moved += 1;
                    continue;
                }
                Ok(Completion(Some(_), Some((reason, verb)))) => {
                    return Err(classify(
                        "MQGET",
                        &MqError(constants::MQCC_WARNING, verb, reason),
                    ));
                }
                Ok(Completion(Some((data, (md, _))), None)) => (data, md),
                Err(error) if error.2 == constants::MQRC_SYNCPOINT_LIMIT_REACHED => {
                    polled.uow_full = true;
                    break;
                }
                Err(error) => return Err(classify("MQGET", &error)),
            };
            let received = Received {
                data: data.to_vec(),
                md: *md,
                properties: Vec::new(),
            };
            polled.gotten += 1;
            polled.bytes += received.data.len();
            if received.md.BackoutCount > 0 {
                polled.redelivered += 1;
            }
            if self
                .route
                .as_ref()
                .is_some_and(|route| received.md.BackoutCount >= route.threshold)
            {
                self.move_to_backout_queue(connection, &received, &mut handle)?;
                polled.moved += 1;
                continue;
            }
            reject_segment(&received.md)?;
            accepted.push(Got { received, handle });
        }
        let (records, rejected) = self.decode(&mut accepted)?;
        for index in rejected.iter().rev() {
            let mut got = accepted.remove(*index);
            self.move_to_backout_queue(connection, &got.received, &mut got.handle)?;
            polled.moved += 1;
        }
        polled.records = records;
        Ok(polled)
    }

    /// Decode accepted messages; with the backout-queue policy, return the undecodable indices.
    fn decode(
        &self,
        accepted: &mut [Got<'_>],
    ) -> Result<(Option<RecordBatch>, Vec<usize>), ConnectorError> {
        if accepted.is_empty() {
            return Ok((None, Vec::new()));
        }
        if self.config.metadata.wants_properties() {
            for got in accepted.iter_mut() {
                got.received.properties = properties(&got.handle)?;
            }
        }
        let payloads: Vec<&[u8]> = accepted
            .iter()
            .map(|got| got.received.data.as_slice())
            .collect();
        let batch = match self
            .deserializer
            .deserialize_batch(&payloads, &self.payload)
        {
            Ok(batch) => batch,
            Err(error)
                if self.config.on_decode_error == DecodeErrorPolicy::Fail
                    || self.route.is_none() =>
            {
                return Err(ConnectorError::Serde(error));
            }
            Err(_) => return self.isolate(accepted),
        };
        let messages: Vec<&Received> = accepted.iter().map(|got| &got.received).collect();
        let counts = if self.config.metadata.is_empty() || batch.num_rows() == messages.len() {
            None
        } else {
            Some(row_counts(
                self.deserializer.as_ref(),
                &payloads,
                &self.payload,
            )?)
        };
        Ok((
            Some(self.with_metadata(batch, &messages, counts.as_deref())?),
            Vec::new(),
        ))
    }

    fn isolate(
        &self,
        accepted: &[Got<'_>],
    ) -> Result<(Option<RecordBatch>, Vec<usize>), ConnectorError> {
        let mut batches = Vec::new();
        let mut counts = Vec::new();
        let mut good = Vec::new();
        let mut rejected = Vec::new();
        for (index, got) in accepted.iter().enumerate() {
            match self
                .deserializer
                .deserialize_batch(&[got.received.data.as_slice()], &self.payload)
            {
                Ok(batch) => {
                    counts.push(batch.num_rows());
                    batches.push(batch);
                    good.push(&got.received);
                }
                Err(_) => rejected.push(index),
            }
        }
        if good.is_empty() {
            return Ok((None, rejected));
        }
        let payload =
            arrow_select::concat::concat_batches(&self.payload, &batches).map_err(|error| {
                ConnectorError::Serde(SerdeError::MalformedInput(error.to_string()))
            })?;
        Ok((
            Some(self.with_metadata(payload, &good, Some(&counts))?),
            rejected,
        ))
    }

    fn with_metadata(
        &self,
        payload: RecordBatch,
        messages: &[&Received],
        counts: Option<&[usize]>,
    ) -> Result<RecordBatch, ConnectorError> {
        if self.config.metadata.is_empty() {
            return Ok(payload);
        }
        let per_row: Vec<&Received> = match counts {
            None => messages.to_vec(),
            Some(counts) => messages
                .iter()
                .zip(counts)
                .flat_map(|(message, count)| std::iter::repeat_n(*message, *count))
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

    fn route(&self, cause: &str) -> Result<&BackoutRoute, ConnectorError> {
        self.route.as_ref().ok_or_else(|| {
            ConnectorError::ConfigurationError(format!(
                "{cause}, and the queue has no usable backout queue (BOTHRESH/BOQNAME)"
            ))
        })
    }

    /// `MQPUT1` the message to `BOQNAME` in the current unit of work, keeping its descriptor
    /// (ids, format, CCSID, encoding, persistence, priority) and properties. The queue manager
    /// supplies context for the connector's own user; no context is passed or spoofed.
    fn move_to_backout_queue(
        &self,
        connection: &Conn,
        received: &Received,
        handle: &mut Properties<&Conn>,
    ) -> Result<(), ConnectorError> {
        let route = self.route("a message must be moved")?;
        let queue = QueueName(MqStr::from_str(&route.queue).map_err(|error| {
            ConnectorError::ConfigurationError(format!(
                "BOQNAME is not a valid queue name: {error}"
            ))
        })?);
        let format = MessageFormat::from_mqmd(&structs::MQMD::new(received.md));
        let options: MQPMO = constants::MQPMO_SYNCPOINT | constants::MQPMO_FAIL_IF_QUIESCING;
        connection
            .put_message(
                &queue,
                &(options, structs::MQMD::new(received.md), handle),
                &(received.data.as_slice(), format),
            )
            .warn_as_error()
            .map_err(|error| {
                classify(
                    &format!("MQPUT1 to backout queue '{}'", route.queue),
                    &error,
                )
            })?;
        tracing::warn!(
            backout_count = received.md.BackoutCount,
            backout_queue = %route.queue,
            "IBM MQ message moved to the backout queue"
        );
        Ok(())
    }

    /// A message longer than `max.message.bytes` is never delivered partially. With the
    /// backout-queue policy it is received whole, matched by `MsgId`, and moved.
    fn move_oversized(
        &self,
        connection: &Conn,
        truncated: &structs::MQMD,
        length: usize,
        handle: &mut Properties<&Conn>,
    ) -> Result<(), ConnectorError> {
        if self.config.on_decode_error == DecodeErrorPolicy::Fail {
            return Err(ConnectorError::ReadError(format!(
                "a {length}-byte message exceeds max.message.bytes = {}; it stays on the queue",
                self.config.max_message_bytes
            )));
        }
        self.route("a message exceeds max.message.bytes")?;
        if length > MQ_MAX_MESSAGE_BYTES {
            return Err(ConnectorError::ReadError(format!(
                "IBM MQ reported a {length}-byte message, beyond its own message limit"
            )));
        }
        let mut whole = vec![0_u8; length];
        let got: Result<Option<(&[u8], structs::MQMD)>, MqError> = self
            .session
            .queue
            .get_data_with(
                &(
                    GET_OPTIONS,
                    GetWait::NoWait,
                    MessageId(truncated.MsgId),
                    &mut *handle,
                ),
                &mut whole[..],
            )
            .warn_as_error();
        let (data, md) = got
            .map_err(|error| classify("MQGET of an oversized message", &error))?
            .ok_or_else(|| {
                ConnectorError::ReadError("the oversized message left the queue".into())
            })?;
        if data.len() != length {
            return Err(ConnectorError::ReadError(
                "a different message matched the oversized message's MsgId".into(),
            ));
        }
        let received = Received {
            data: data.to_vec(),
            md: *md,
            properties: Vec::new(),
        };
        self.move_to_backout_queue(connection, &received, handle)
    }
}

/// One `MQGET` result: the payload slice, descriptor, and full message length.
type Delivered<'b> = (&'b [u8], (structs::MQMD, MessageLength));

/// Full message length reported by `MQGET`, also for a truncated (failed) get.
struct MessageLength(usize);

impl<'b, R> GetAttr<'b, R> for MessageLength {
    fn get_extract<F, B>(param: &mut GetParam, get: F) -> ResultComp<(Self, GetState<B>)>
    where
        F: FnOnce(&mut GetParam) -> ResultComp<GetState<B>>,
        B: mqi::traits::Buffer<'b, R>,
    {
        get(param).map(|completion| completion.map(|state| (Self(state.message_length), state)))
    }
}

/// Segments are partial application messages; this source delivers only complete messages.
fn reject_segment(md: &libmqm_sys::MQMD) -> Result<(), ConnectorError> {
    let segment = libmqm_sys::MQMF_SEGMENT | libmqm_sys::MQMF_LAST_SEGMENT;
    if md.MsgFlags & segment != 0 {
        return Err(ConnectorError::FeatureUnsupported(
            "segmented IBM MQ messages are not supported; segments would be delivered as \
             partial messages"
                .into(),
        ));
    }
    Ok(())
}

fn row_counts(
    deserializer: &dyn RecordDeserializer,
    payloads: &[&[u8]],
    schema: &SchemaRef,
) -> Result<Vec<usize>, ConnectorError> {
    payloads
        .iter()
        .map(|payload| {
            deserializer
                .deserialize_batch(&[payload], schema)
                .map(|batch| batch.num_rows())
                .map_err(ConnectorError::Serde)
        })
        .collect()
}

/// Every property of a message handle with its value rendered as bytes: strings as UTF-8,
/// byte strings verbatim, numbers and booleans as text, and null as NULL.
fn properties(handle: &Properties<&Conn>) -> Result<RenderedProperties, ConnectorError> {
    let mut rendered = Vec::new();
    for item in handle.property_iter::<(Value, Name<String>), _>("%", MQIMPO::default()) {
        let (value, Name(name)) = item
            .warn_as_error()
            .map_err(|error| classify("MQINQMP", &error))?;
        rendered.push((name, render(&value)));
    }
    Ok(rendered)
}

fn render(value: &Value) -> Option<Vec<u8>> {
    Some(match value {
        Value::Null => return None,
        Value::Boolean(flag) => flag.to_string().into_bytes(),
        Value::String(text) => mqi_bytes(mqi::string::EncodedString::data(text)),
        Value::ByteString(bytes) => bytes.clone(),
        Value::Int8(number) => number.to_string().into_bytes(),
        Value::Int16(number) => number.to_string().into_bytes(),
        Value::Int32(number) => number.to_string().into_bytes(),
        Value::Int64(number) => number.to_string().into_bytes(),
        Value::Float32(number) => number.to_string().into_bytes(),
        Value::Float64(number) => number.to_string().into_bytes(),
    })
}

fn mqi_bytes(characters: &[mqi::types::MQCHAR]) -> Vec<u8> {
    characters
        .iter()
        .map(|character| u8::from_ne_bytes(character.to_ne_bytes()))
        .collect()
}

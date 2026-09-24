use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, SchemaRef};
use laminar_core::serialization::BoundedBytesWriter;
use laminar_core::state::PartitionKeyCodecV1;

use super::wire;
use super::MAX_FRAME_BYTES;
use crate::error::DbError;
use crate::process_function::{
    ProcessActivation, ProcessCallback, ProcessFunctionDescriptor, TimerOperation, ValueMutation,
    ValueState,
};

pub(super) fn encode_batch(batch: &RecordBatch, schema: &SchemaRef) -> Result<Vec<u8>, DbError> {
    if batch.schema().as_ref() != schema.as_ref() {
        return Err(DbError::InvalidOperation(
            "process worker Arrow schema does not match the descriptor".into(),
        ));
    }
    let mut bounded = BoundedBytesWriter::new(MAX_FRAME_BYTES);
    {
        let mut writer = StreamWriter::try_new(&mut bounded, schema)
            .map_err(|error| DbError::InvalidOperation(format!("encode process IPC: {error}")))?;
        writer
            .write(batch)
            .and_then(|()| writer.finish())
            .map_err(|error| DbError::InvalidOperation(format!("encode process IPC: {error}")))?;
    }
    Ok(bounded.into_vec())
}

pub(super) fn decode_batch(
    bytes: &[u8],
    schema: &SchemaRef,
    max_rows: usize,
    max_body_bytes: usize,
) -> Result<RecordBatch, DbError> {
    if bytes.is_empty() || bytes.len() > MAX_FRAME_BYTES {
        return Err(DbError::InvalidOperation(
            "process worker IPC frame is empty or oversized".into(),
        ));
    }
    preflight_ipc(bytes, max_rows, max_body_bytes)?;
    let mut reader = StreamReader::try_new(Cursor::new(bytes), None)
        .map_err(|error| DbError::InvalidOperation(format!("decode process IPC: {error}")))?;
    if reader.schema().as_ref() != schema.as_ref() {
        return Err(DbError::InvalidOperation(
            "process worker IPC schema does not match the descriptor".into(),
        ));
    }
    let batch = reader
        .next()
        .ok_or_else(|| DbError::InvalidOperation("process worker IPC has no batch".into()))?
        .map_err(|error| DbError::InvalidOperation(format!("decode process IPC: {error}")))?;
    if reader.next().is_some() {
        return Err(DbError::InvalidOperation(
            "process worker IPC must contain exactly one batch".into(),
        ));
    }
    if batch.num_rows() > max_rows || batch.get_array_memory_size() > max_body_bytes {
        return Err(DbError::InvalidOperation(
            "process worker decoded Arrow batch exceeds frame limit".into(),
        ));
    }
    Ok(batch)
}

// The reader allocates from IPC metadata. Check framing, row count, and uncompressed body size
// before giving it untrusted bytes. The v1 schema contract excludes dictionaries and nesting.
fn preflight_ipc(bytes: &[u8], max_rows: usize, max_body_bytes: usize) -> Result<(), DbError> {
    let mut offset = 0usize;
    let mut message_count = 0usize;
    loop {
        let prefix = bytes
            .get(offset..offset.saturating_add(4))
            .ok_or_else(|| DbError::InvalidOperation("truncated process IPC prefix".into()))?;
        offset += 4;
        let mut metadata_len = u32::from_le_bytes(
            prefix
                .try_into()
                .map_err(|_| DbError::InvalidOperation("invalid process IPC prefix".into()))?,
        );
        if metadata_len == u32::MAX {
            let length = bytes.get(offset..offset.saturating_add(4)).ok_or_else(|| {
                DbError::InvalidOperation("truncated process IPC continuation".into())
            })?;
            offset += 4;
            metadata_len = u32::from_le_bytes(length.try_into().map_err(|_| {
                DbError::InvalidOperation("invalid process IPC continuation".into())
            })?);
        }
        if metadata_len == 0 {
            if offset != bytes.len() || message_count != 2 {
                return Err(DbError::InvalidOperation(
                    "noncanonical process IPC ending".into(),
                ));
            }
            return Ok(());
        }
        let metadata_end = offset
            .checked_add(metadata_len as usize)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| DbError::InvalidOperation("truncated process IPC metadata".into()))?;
        let message =
            arrow_ipc::root_as_message(&bytes[offset..metadata_end]).map_err(|error| {
                DbError::InvalidOperation(format!("invalid process IPC metadata: {error}"))
            })?;
        offset = metadata_end;
        let body_len = usize::try_from(message.bodyLength())
            .map_err(|_| DbError::InvalidOperation("invalid process IPC body length".into()))?;
        if body_len > max_body_bytes || body_len > MAX_FRAME_BYTES {
            return Err(DbError::InvalidOperation(
                "process IPC body exceeds budget".into(),
            ));
        }
        offset = offset
            .checked_add(body_len)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| DbError::InvalidOperation("truncated process IPC body".into()))?;
        match (message_count, message.header_type()) {
            (0, arrow_ipc::MessageHeader::Schema) if body_len == 0 => {}
            (1, arrow_ipc::MessageHeader::RecordBatch) => {
                let batch = message.header_as_record_batch().ok_or_else(|| {
                    DbError::InvalidOperation("missing process IPC record batch header".into())
                })?;
                let rows = usize::try_from(batch.length()).map_err(|_| {
                    DbError::InvalidOperation("invalid process IPC row count".into())
                })?;
                if batch.compression().is_some() || rows > max_rows {
                    return Err(DbError::InvalidOperation(
                        "process IPC compression or row count is unsupported".into(),
                    ));
                }
            }
            _ => {
                return Err(DbError::InvalidOperation(
                    "unexpected process IPC message".into(),
                ))
            }
        }
        message_count += 1;
    }
}

pub(super) fn encode_activation(
    activation: &ProcessActivation,
    descriptor: &ProcessFunctionDescriptor,
) -> Result<wire::Activation, DbError> {
    validate_key(&activation.key, &activation.key_text)?;
    let callback = match &activation.callback {
        ProcessCallback::Input(batch) => {
            validate_input_row(
                batch,
                descriptor,
                &activation.key_text,
                activation.event_time_us,
            )?;
            wire::activation::Callback::InputIpc(encode_batch(batch, &descriptor.input_schema)?)
        }
        ProcessCallback::Timer { name } => {
            if !descriptor.timer_names.contains(name) {
                return Err(DbError::InvalidOperation("undeclared process timer".into()));
            }
            wire::activation::Callback::TimerName(name.clone())
        }
    };
    let state = match activation.state {
        ValueState::Absent => wire::activation::State::Absent(true),
        ValueState::Null => wire::activation::State::NullValue(true),
        ValueState::Value(value) => wire::activation::State::Value(value),
    };
    Ok(wire::Activation {
        id: activation.id,
        canonical_key: activation.key.to_vec(),
        key_text: activation.key_text.clone(),
        event_time_us: activation.event_time_us,
        callback: Some(callback),
        state: Some(state),
    })
}

pub(super) fn decode_activation(
    activation: wire::Activation,
    descriptor: &ProcessFunctionDescriptor,
) -> Result<ProcessActivation, DbError> {
    validate_key(&activation.canonical_key, &activation.key_text)?;
    let callback = match activation.callback {
        Some(wire::activation::Callback::InputIpc(ipc)) => {
            let batch = decode_batch(
                &ipc,
                &descriptor.input_schema,
                1,
                descriptor.limits.max_input_bytes,
            )?;
            validate_input_row(
                &batch,
                descriptor,
                &activation.key_text,
                activation.event_time_us,
            )?;
            ProcessCallback::Input(batch)
        }
        Some(wire::activation::Callback::TimerName(name))
            if descriptor.timer_names.contains(&name) =>
        {
            ProcessCallback::Timer { name }
        }
        _ => return Err(DbError::InvalidOperation("invalid process callback".into())),
    };
    let state = match activation.state {
        Some(wire::activation::State::Absent(true)) => ValueState::Absent,
        Some(wire::activation::State::NullValue(true)) => ValueState::Null,
        Some(wire::activation::State::Value(value)) => ValueState::Value(value),
        _ => {
            return Err(DbError::InvalidOperation(
                "invalid process state view".into(),
            ))
        }
    };
    Ok(ProcessActivation {
        id: activation.id,
        key: Arc::from(activation.canonical_key),
        key_text: activation.key_text,
        event_time_us: activation.event_time_us,
        callback,
        state,
    })
}

fn validate_key(key: &[u8], key_text: &str) -> Result<(), DbError> {
    if key.is_empty() || key.len() > MAX_FRAME_BYTES || key_text.len() > MAX_FRAME_BYTES {
        return Err(DbError::InvalidOperation(
            "invalid process worker key size".into(),
        ));
    }
    let codec = PartitionKeyCodecV1::try_new([DataType::Utf8])
        .map_err(|error| DbError::InvalidOperation(format!("process key codec: {error}")))?;
    let encoded = codec
        .encode_columns(&[Arc::new(StringArray::from(vec![key_text]))])
        .map_err(|error| DbError::InvalidOperation(format!("encode process key: {error}")))?;
    if encoded.row(0).data() != key {
        return Err(DbError::InvalidOperation(
            "process worker key text differs from canonical key".into(),
        ));
    }
    Ok(())
}

fn validate_input_row(
    batch: &RecordBatch,
    descriptor: &ProcessFunctionDescriptor,
    key_text: &str,
    event_time_us: i64,
) -> Result<(), DbError> {
    if batch.num_rows() != 1 || batch.schema().as_ref() != descriptor.input_schema.as_ref() {
        return Err(DbError::InvalidOperation(
            "process worker input must be one row with the declared schema".into(),
        ));
    }
    let key_index = descriptor
        .input_schema
        .index_of(&descriptor.key_columns[0])
        .map_err(|_| {
            DbError::InvalidOperation("process worker input key field is absent".into())
        })?;
    let time_index = descriptor
        .input_schema
        .index_of(&descriptor.event_time_column)
        .map_err(|_| {
            DbError::InvalidOperation("process worker event-time field is absent".into())
        })?;
    let key = batch
        .column(key_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| DbError::InvalidOperation("process worker key field type differs".into()))?;
    let time = batch
        .column(time_index)
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| {
            DbError::InvalidOperation("process worker event-time field type differs".into())
        })?;
    if key.is_null(0)
        || time.is_null(0)
        || key.value(0) != key_text
        || time.value(0) != event_time_us
    {
        return Err(DbError::InvalidOperation(
            "process worker activation metadata differs from the input row".into(),
        ));
    }
    Ok(())
}

pub(super) fn encode_mutation(value: ValueMutation) -> wire::result::Mutation {
    match value {
        ValueMutation::Unchanged => wire::result::Mutation::Unchanged(true),
        ValueMutation::Clear => wire::result::Mutation::Clear(true),
        ValueMutation::SetNull => wire::result::Mutation::SetNull(true),
        ValueMutation::Set(value) => wire::result::Mutation::SetValue(value),
    }
}

pub(super) fn decode_mutation(
    value: Option<wire::result::Mutation>,
) -> Result<ValueMutation, DbError> {
    match value {
        Some(wire::result::Mutation::Unchanged(true)) => Ok(ValueMutation::Unchanged),
        Some(wire::result::Mutation::Clear(true)) => Ok(ValueMutation::Clear),
        Some(wire::result::Mutation::SetNull(true)) => Ok(ValueMutation::SetNull),
        Some(wire::result::Mutation::SetValue(value)) => Ok(ValueMutation::Set(value)),
        _ => Err(DbError::InvalidOperation(
            "invalid process worker mutation".into(),
        )),
    }
}

pub(super) fn encode_timer(operation: TimerOperation) -> wire::TimerOperation {
    let kind = match operation {
        TimerOperation::Set { name, at_us } => {
            wire::timer_operation::Kind::Set(wire::TimerSet { name, at_us })
        }
        TimerOperation::Cancel { name } => wire::timer_operation::Kind::Cancel(name),
    };
    wire::TimerOperation { kind: Some(kind) }
}

pub(super) fn decode_timer(
    operation: wire::TimerOperation,
    descriptor: &ProcessFunctionDescriptor,
) -> Result<TimerOperation, DbError> {
    let value = match operation.kind {
        Some(wire::timer_operation::Kind::Set(set)) => TimerOperation::Set {
            name: set.name,
            at_us: set.at_us,
        },
        Some(wire::timer_operation::Kind::Cancel(name)) => TimerOperation::Cancel { name },
        None => {
            return Err(DbError::InvalidOperation(
                "missing process worker timer operation".into(),
            ))
        }
    };
    let name = match &value {
        TimerOperation::Set { name, .. } | TimerOperation::Cancel { name } => name,
    };
    if !descriptor.timer_names.contains(name) {
        return Err(DbError::InvalidOperation(
            "undeclared process worker timer".into(),
        ));
    }
    Ok(value)
}

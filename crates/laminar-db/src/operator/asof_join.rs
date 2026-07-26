//! ASOF join operator for the `OperatorGraph`.
//!
//! Buffers right-side data across execution cycles so that left events can match
//! against the full right-side history (up to watermark-driven eviction).

use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;

use laminar_sql::parser::join_parser::AsofSqlDirection;
use laminar_sql::translator::{AsofJoinTranslatorConfig, AsofSqlJoinType};

use crate::asof_batch::{
    execute_asof_join_with_state, validate_schema_only_ipc, AsofBufferCheckpoint, AsofRightBuffer,
};
use crate::error::DbError;
use crate::key_column::{extract_column_as_timestamps, extract_key_column};
use crate::operator::ProjectingJoinState;
use crate::operator_graph::{GraphOperator, OperatorCheckpoint};

const ASOF_CHECKPOINT_VERSION_V1: u8 = 1;
const ASOF_CHECKPOINT_VERSION: u8 = 2;
// Mirror the shuffle path's source-memory, wire, and decoded-memory schema ceilings.
const MAX_ASOF_SOURCE_SCHEMA_BYTES: usize = 256 * 1024;
const MAX_ASOF_SCHEMA_IPC_BYTES: usize = 512 * 1024;
const MAX_ASOF_DECODED_SCHEMA_BYTES: usize = 1024 * 1024;
const ASOF_SCHEMA_LENGTH_BYTES: usize = std::mem::size_of::<u32>();

fn schema_memory_size(schema: &arrow::datatypes::Schema) -> usize {
    let fields = schema
        .fields()
        .iter()
        .fold(0usize, |bytes, field| bytes.saturating_add(field.size()));
    let metadata = schema.metadata().iter().fold(
        schema
            .metadata()
            .capacity()
            .saturating_mul(std::mem::size_of::<(String, String)>()),
        |bytes, (key, value)| {
            bytes
                .saturating_add(key.capacity())
                .saturating_add(value.capacity())
        },
    );
    std::mem::size_of_val(schema)
        .saturating_add(
            schema
                .fields()
                .len()
                .saturating_mul(std::mem::size_of::<arrow::datatypes::FieldRef>()),
        )
        .saturating_add(fields)
        .saturating_add(metadata)
}

fn validate_right_schema_memory(schema: &SchemaRef) -> Result<(), DbError> {
    let source_bytes = schema_memory_size(schema.as_ref());
    if source_bytes > MAX_ASOF_SOURCE_SCHEMA_BYTES {
        return Err(DbError::SchemaMismatch(format!(
            "ASOF join right schema uses {source_bytes} bytes; limit is {MAX_ASOF_SOURCE_SCHEMA_BYTES}"
        )));
    }
    Ok(())
}

fn validate_right_schema(
    schema: &SchemaRef,
    config: &AsofJoinTranslatorConfig,
) -> Result<(), DbError> {
    validate_right_schema_memory(schema)?;
    let empty = RecordBatch::new_empty(schema.clone());
    let _ = extract_key_column(&empty, &config.key_column)?;
    let _ = extract_column_as_timestamps(&empty, &config.right_time_column)?;
    Ok(())
}

fn serialize_right_schema_ipc(schema: &SchemaRef) -> Result<Vec<u8>, String> {
    let ipc = laminar_core::serialization::serialize_batches_stream_bounded(
        schema.as_ref(),
        std::iter::empty::<&RecordBatch>(),
        MAX_ASOF_SCHEMA_IPC_BYTES,
    )
    .map_err(|error| error.to_string())?;
    if ipc.is_empty() {
        return Err("right schema checkpoint serialization returned no bytes".to_string());
    }
    validate_schema_only_ipc(&ipc, MAX_ASOF_SCHEMA_IPC_BYTES).map_err(|error| {
        format!("right schema checkpoint serialization is noncanonical: {error}")
    })?;
    Ok(ipc)
}

fn validate_checkpointable_right_schema(
    schema: &SchemaRef,
    config: &AsofJoinTranslatorConfig,
    op_name: &str,
) -> Result<(), DbError> {
    validate_right_schema(schema, config)?;
    let _ = serialize_right_schema_ipc(schema).map_err(|error| {
        DbError::SchemaMismatch(format!(
            "ASOF join [{op_name}] right schema cannot be checkpointed: {error}"
        ))
    })?;
    Ok(())
}

fn validate_right_input_schemas(
    retained: Option<&SchemaRef>,
    batches: &[RecordBatch],
    config: &AsofJoinTranslatorConfig,
    op_name: &str,
) -> Result<Option<SchemaRef>, DbError> {
    let incoming = batches.first().map(RecordBatch::schema);
    let Some(incoming_schema) = incoming.as_ref() else {
        return Ok(None);
    };
    let expected = retained.unwrap_or(incoming_schema);
    if retained.is_none() {
        validate_checkpointable_right_schema(expected, config, op_name)?;
    }

    for (batch_index, batch) in batches.iter().enumerate() {
        let actual = batch.schema();
        if Arc::ptr_eq(expected, &actual) {
            continue;
        }
        // Schema equality deliberately ignores allocation capacity. Enforce the memory ceiling
        // before accepting a pointer-distinct but logically equal schema as the retained batch's
        // new schema authority.
        validate_right_schema_memory(&actual)?;
        if expected.as_ref() != actual.as_ref() {
            return Err(DbError::SchemaMismatch(format!(
                "ASOF join right batch {batch_index} does not match the learned right schema"
            )));
        }
    }
    Ok(incoming)
}

fn encode_right_schema(schema: Option<&SchemaRef>, op_name: &str) -> Result<Vec<u8>, DbError> {
    let Some(schema) = schema else {
        return Ok(Vec::new());
    };
    let source_bytes = schema_memory_size(schema.as_ref());
    if source_bytes > MAX_ASOF_SOURCE_SCHEMA_BYTES {
        return Err(DbError::Pipeline(format!(
            "ASOF join [{op_name}]: right schema uses {source_bytes} bytes; limit is {MAX_ASOF_SOURCE_SCHEMA_BYTES}"
        )));
    }
    serialize_right_schema_ipc(schema).map_err(|error| {
        DbError::Pipeline(format!(
            "ASOF join [{op_name}]: bounded right schema checkpoint serialization: {error}"
        ))
    })
}

fn decode_right_schema(
    ipc: &[u8],
    config: &AsofJoinTranslatorConfig,
    op_name: &str,
) -> Result<Option<SchemaRef>, DbError> {
    if ipc.is_empty() {
        return Ok(None);
    }
    if ipc.len() > MAX_ASOF_SCHEMA_IPC_BYTES {
        return Err(DbError::Checkpoint(format!(
            "ASOF join [{op_name}]: right schema checkpoint is {} bytes; maximum is {MAX_ASOF_SCHEMA_IPC_BYTES}",
            ipc.len()
        )));
    }
    validate_schema_only_ipc(ipc, MAX_ASOF_SCHEMA_IPC_BYTES).map_err(|error| {
        DbError::Checkpoint(format!(
            "ASOF join [{op_name}]: right schema checkpoint framing: {error}"
        ))
    })?;
    let mut reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None)
        .map_err(|error| {
            DbError::Checkpoint(format!(
                "ASOF join [{op_name}]: right schema checkpoint deserialization: {error}"
            ))
        })?;
    let schema = reader.schema();
    match reader.next() {
        Some(Ok(_)) => {
            return Err(DbError::Checkpoint(format!(
                "ASOF join [{op_name}]: right schema checkpoint contains a record batch"
            )));
        }
        Some(Err(error)) => {
            return Err(DbError::Checkpoint(format!(
                "ASOF join [{op_name}]: right schema checkpoint stream: {error}"
            )));
        }
        None => {}
    }
    if usize::try_from(reader.get_ref().position()).ok() != Some(ipc.len()) {
        return Err(DbError::Checkpoint(format!(
            "ASOF join [{op_name}]: right schema checkpoint has trailing bytes"
        )));
    }
    let decoded_bytes = schema_memory_size(schema.as_ref());
    if decoded_bytes > MAX_ASOF_DECODED_SCHEMA_BYTES {
        return Err(DbError::Checkpoint(format!(
            "ASOF join [{op_name}]: decoded right schema uses {decoded_bytes} bytes; limit is {MAX_ASOF_DECODED_SCHEMA_BYTES}"
        )));
    }
    validate_checkpointable_right_schema(&schema, config, op_name).map_err(|error| {
        DbError::Checkpoint(format!(
            "ASOF join [{op_name}]: invalid restored right schema: {error}"
        ))
    })?;
    Ok(Some(schema))
}

fn split_v2_checkpoint<'a>(
    payload: &'a [u8],
    op_name: &str,
) -> Result<(&'a [u8], &'a [u8]), DbError> {
    if payload.len() < ASOF_SCHEMA_LENGTH_BYTES {
        return Err(DbError::Checkpoint(format!(
            "ASOF join [{op_name}]: version-2 checkpoint is missing its schema length"
        )));
    }
    let length_offset = payload.len() - ASOF_SCHEMA_LENGTH_BYTES;
    let schema_len = usize::try_from(u32::from_le_bytes(
        payload[length_offset..]
            .try_into()
            .expect("validated ASOF schema-length trailer"),
    ))
    .map_err(|_| {
        DbError::Checkpoint(format!(
            "ASOF join [{op_name}]: right schema length does not fit this process"
        ))
    })?;
    if schema_len > MAX_ASOF_SCHEMA_IPC_BYTES {
        return Err(DbError::Checkpoint(format!(
            "ASOF join [{op_name}]: right schema checkpoint declares {schema_len} bytes; maximum is {MAX_ASOF_SCHEMA_IPC_BYTES}"
        )));
    }
    let body_end = length_offset.checked_sub(schema_len).ok_or_else(|| {
        DbError::Checkpoint(format!(
            "ASOF join [{op_name}]: right schema length exceeds the version-2 checkpoint"
        ))
    })?;
    if body_end == 0 {
        return Err(DbError::Checkpoint(format!(
            "ASOF join [{op_name}]: version-2 checkpoint has an empty buffer body"
        )));
    }
    Ok((&payload[..body_end], &payload[body_end..length_offset]))
}

fn partial_apply(error: DbError) -> DbError {
    if error.requires_pipeline_recovery() || error.requires_pipeline_halt() {
        error
    } else {
        DbError::StatefulOperatorPartialApply(format!(
            "ASOF join may have changed right-side state before the cycle failed: {error}"
        ))
    }
}

fn classify_after_apply(state_changed: bool, error: DbError) -> DbError {
    if state_changed {
        partial_apply(error)
    } else {
        error
    }
}

pub(crate) struct AsofJoinOperator {
    config: AsofJoinTranslatorConfig,
    projection: ProjectingJoinState,
    right_buffer: AsofRightBuffer,
    last_evicted_watermark: i64,
    // Captured from the first right batch so a later cycle with an
    // empty right buffer can still emit left rows with null right columns.
    right_schema: Option<SchemaRef>,
}

impl AsofJoinOperator {
    pub(crate) fn new(
        name: &str,
        config: AsofJoinTranslatorConfig,
        projection_sql: Option<Arc<str>>,
        ctx: SessionContext,
    ) -> Self {
        Self {
            config,
            projection: ProjectingJoinState::new(name, ctx, projection_sql, "__asof_tmp"),
            right_buffer: AsofRightBuffer::default(),
            last_evicted_watermark: i64::MIN,
            right_schema: None,
        }
    }
}

#[async_trait]
impl GraphOperator for AsofJoinOperator {
    fn cluster_capability(&self) -> crate::operator::capability::OperatorCapability {
        crate::operator::capability::OperatorCapability::fixed(
            crate::operator::capability::OperatorImplementation::AsofJoin,
        )
    }

    async fn process(
        &mut self,
        inputs: &[Vec<RecordBatch>],
        watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        let left_batches = inputs.first().map_or(&[][..], Vec::as_slice);
        let right_batches = inputs.get(1).map_or(&[][..], Vec::as_slice);

        let incoming_schema = validate_right_input_schemas(
            self.right_schema.as_ref(),
            right_batches,
            &self.config,
            &self.projection.op_name,
        )?;

        let admitted_rows = self.right_buffer.ingest(
            right_batches,
            &self.config.key_column,
            &self.config.right_time_column,
        )?;

        let learned_schema = self.right_schema.is_none() && incoming_schema.is_some();
        if learned_schema {
            self.right_schema = incoming_schema;
        }
        let state_changed = admitted_rows || learned_schema;

        // Join before evicting: a batch's rows can still backward-match right
        // rows whose timestamps they themselves set the watermark past.
        let output = if left_batches.is_empty() {
            Vec::new()
        } else {
            let joined = execute_asof_join_with_state(
                left_batches,
                &self.right_buffer,
                &self.config,
                self.right_schema.as_ref(),
            )
            .map_err(|error| classify_after_apply(state_changed, error))?;
            if joined.num_rows() == 0 {
                Vec::new()
            } else {
                self.projection
                    .apply(vec![joined])
                    .await
                    .map_err(|error| classify_after_apply(state_changed, error))?
            }
        };

        // Prune: Backward/Nearest keep the latest right <= left_wm per key;
        // bounded tolerance also evicts rows below left_wm - tol. Forward drops
        // everything below left_wm. Driving off the watermark (not tolerance)
        // bounds memory even when tolerance is None.
        let left_wm = watermarks.first().copied().unwrap_or(i64::MIN);
        if left_wm > self.last_evicted_watermark {
            match self.config.direction {
                AsofSqlDirection::Forward => {
                    self.right_buffer
                        .evict_before(left_wm)
                        .map_err(partial_apply)?;
                }
                AsofSqlDirection::Backward | AsofSqlDirection::Nearest => {
                    self.right_buffer
                        .evict_superseded(left_wm)
                        .map_err(partial_apply)?;
                    if let Some(tol) = self
                        .config
                        .tolerance
                        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
                    {
                        self.right_buffer
                            .evict_before(left_wm.saturating_sub(tol))
                            .map_err(partial_apply)?;
                    }
                }
            }
            self.last_evicted_watermark = left_wm;
        }

        Ok(output)
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        let learned_schema_ipc = if self.right_buffer.is_logically_empty() {
            encode_right_schema(self.right_schema.as_ref(), &self.projection.op_name)?
        } else {
            Vec::new()
        };
        let cp = self
            .right_buffer
            .snapshot_checkpoint(self.last_evicted_watermark)?;
        let right_schema_ipc = if cp.right_buffer_ipc.is_empty() {
            if self.right_schema.is_some() && learned_schema_ipc.is_empty() {
                return Err(DbError::Pipeline(format!(
                    "ASOF join [{}]: checkpoint lost its learned right schema",
                    self.projection.op_name
                )));
            }
            learned_schema_ipc
        } else {
            Vec::new()
        };

        let body = rkyv::to_bytes::<rkyv::rancor::Error>(&cp).map_err(|e| {
            DbError::Pipeline(format!(
                "ASOF join [{}]: checkpoint serialization: {e}",
                self.projection.op_name
            ))
        })?;

        let schema_len = u32::try_from(right_schema_ipc.len()).map_err(|_| {
            DbError::Pipeline(format!(
                "ASOF join [{}]: right schema checkpoint length exceeds the u32 wire format",
                self.projection.op_name
            ))
        })?;
        let encoded_len = body
            .len()
            .checked_add(right_schema_ipc.len())
            .and_then(|length| length.checked_add(ASOF_SCHEMA_LENGTH_BYTES + 1))
            .ok_or_else(|| {
                DbError::Pipeline(format!(
                    "ASOF join [{}]: checkpoint length overflow",
                    self.projection.op_name
                ))
            })?;
        // V2 appends schema bytes and their length while preserving the aligned V1 rkyv body at
        // offset zero. A non-empty retained buffer is its own schema authority, so only an empty
        // buffer carries the appendix.
        let mut data = Vec::new();
        data.try_reserve_exact(encoded_len).map_err(|error| {
            DbError::Pipeline(format!(
                "ASOF join [{}]: checkpoint allocation: {error}",
                self.projection.op_name
            ))
        })?;
        data.extend_from_slice(&body);
        data.extend_from_slice(&right_schema_ipc);
        data.extend_from_slice(&schema_len.to_le_bytes());
        data.push(ASOF_CHECKPOINT_VERSION);

        Ok(Some(OperatorCheckpoint { data }))
    }

    fn restore(&mut self, checkpoint: OperatorCheckpoint) -> Result<(), DbError> {
        let Some((&version, body)) = checkpoint.data.split_last() else {
            return Err(DbError::Checkpoint(format!(
                "ASOF join [{}]: checkpoint empty (missing version trailer)",
                self.projection.op_name
            )));
        };
        let (buffer, last_wm, right_schema) = match version {
            ASOF_CHECKPOINT_VERSION_V1 => {
                let cp = rkyv::from_bytes::<AsofBufferCheckpoint, rkyv::rancor::Error>(body)
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "ASOF join [{}]: version-1 checkpoint deserialization: {error}",
                            self.projection.op_name
                        ))
                    })?;
                let (buffer, last_wm) = AsofRightBuffer::from_checkpoint(
                    &cp,
                    &self.config.key_column,
                    &self.config.right_time_column,
                    MAX_ASOF_SCHEMA_IPC_BYTES,
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "ASOF join [{}]: version-1 checkpoint restore: {error}",
                        self.projection.op_name
                    ))
                })?;
                let right_schema = buffer.retained_schema();
                if let Some(schema) = right_schema.as_ref() {
                    validate_checkpointable_right_schema(
                        schema,
                        &self.config,
                        &self.projection.op_name,
                    )
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "ASOF join [{}]: version-1 restored right schema is not checkpointable: {error}",
                            self.projection.op_name
                        ))
                    })?;
                }
                if right_schema.is_none() && self.config.join_type == AsofSqlJoinType::Left {
                    return Err(DbError::Checkpoint(format!(
                        "ASOF join [{}]: version-1 empty LEFT checkpoint cannot recover whether the right schema was learned",
                        self.projection.op_name
                    )));
                }
                (buffer, last_wm, right_schema)
            }
            ASOF_CHECKPOINT_VERSION => {
                let (body, schema_ipc) = split_v2_checkpoint(body, &self.projection.op_name)?;
                let cp = rkyv::from_bytes::<AsofBufferCheckpoint, rkyv::rancor::Error>(body)
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "ASOF join [{}]: version-2 checkpoint deserialization: {error}",
                            self.projection.op_name
                        ))
                    })?;
                let (buffer, last_wm) = AsofRightBuffer::from_checkpoint(
                    &cp,
                    &self.config.key_column,
                    &self.config.right_time_column,
                    MAX_ASOF_SCHEMA_IPC_BYTES,
                )
                .map_err(|error| {
                    DbError::Checkpoint(format!(
                        "ASOF join [{}]: version-2 checkpoint restore: {error}",
                        self.projection.op_name
                    ))
                })?;
                let right_schema = if let Some(buffer_schema) = buffer.retained_schema() {
                    if !schema_ipc.is_empty() {
                        return Err(DbError::Checkpoint(format!(
                            "ASOF join [{}]: version-2 checkpoint has both a right buffer and a schema appendix",
                            self.projection.op_name
                        )));
                    }
                    validate_checkpointable_right_schema(
                        &buffer_schema,
                        &self.config,
                        &self.projection.op_name,
                    )
                    .map_err(|error| {
                        DbError::Checkpoint(format!(
                            "ASOF join [{}]: version-2 restored right schema is not checkpointable: {error}",
                            self.projection.op_name
                        ))
                    })?;
                    Some(buffer_schema)
                } else {
                    decode_right_schema(schema_ipc, &self.config, &self.projection.op_name)?
                };
                (buffer, last_wm, right_schema)
            }
            _ => {
                return Err(DbError::Checkpoint(format!(
                    "ASOF join [{}]: unsupported checkpoint version {version} (expected {ASOF_CHECKPOINT_VERSION_V1} or {ASOF_CHECKPOINT_VERSION})",
                    self.projection.op_name
                )));
            }
        };

        self.right_buffer = buffer;
        self.last_evicted_watermark = last_wm;
        self.right_schema = right_schema;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use laminar_sql::parser::join_parser::AsofSqlDirection;
    use laminar_sql::translator::AsofSqlJoinType;

    fn trades_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("trade_ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(Int64Array::from(vec![100, 150])),
                Arc::new(Float64Array::from(vec![150.0, 2800.0])),
            ],
        )
        .unwrap()
    }

    fn trades_without_key_column() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trade_ts", DataType::Int64, false),
            Field::new("price", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Float64Array::from(vec![150.0])),
            ],
        )
        .unwrap()
    }

    fn quotes_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("quote_ts", DataType::Int64, false),
            Field::new("bid", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "GOOG"])),
                Arc::new(Int64Array::from(vec![90, 140])),
                Arc::new(Float64Array::from(vec![149.0, 2790.0])),
            ],
        )
        .unwrap()
    }

    fn quotes_batch_with_metadata_bytes(metadata_bytes: usize) -> RecordBatch {
        let quotes = quotes_batch();
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("padding".to_string(), "x".repeat(metadata_bytes));
        let schema = Arc::new(Schema::new_with_metadata(
            quotes.schema().fields().clone(),
            metadata,
        ));
        RecordBatch::try_new(schema, quotes.columns().to_vec()).unwrap()
    }

    fn quotes_batch_with_oversized_empty_metadata_capacity() -> RecordBatch {
        let quotes = quotes_batch();
        let slots = MAX_ASOF_SOURCE_SCHEMA_BYTES
            .checked_div(std::mem::size_of::<(String, String)>())
            .unwrap()
            .saturating_add(1);
        let metadata = std::collections::HashMap::with_capacity(slots);
        let schema = Arc::new(Schema::new_with_metadata(
            quotes.schema().fields().clone(),
            metadata,
        ));
        assert_eq!(schema.as_ref(), quotes.schema().as_ref());
        assert!(schema_memory_size(schema.as_ref()) > MAX_ASOF_SOURCE_SCHEMA_BYTES);
        RecordBatch::try_new(schema, quotes.columns().to_vec()).unwrap()
    }

    fn equal_timestamp_quotes_batch() -> RecordBatch {
        let schema = quotes_batch().schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL", "AAPL"])),
                Arc::new(Int64Array::from(vec![90, 90])),
                Arc::new(Float64Array::from(vec![149.0, 150.0])),
            ],
        )
        .unwrap()
    }

    fn quotes_without_time_column() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("bid", DataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["AAPL"])),
                Arc::new(Float64Array::from(vec![149.0])),
            ],
        )
        .unwrap()
    }

    fn test_config() -> AsofJoinTranslatorConfig {
        AsofJoinTranslatorConfig {
            left_table: "trades".to_string(),
            right_table: "quotes".to_string(),
            key_column: "symbol".to_string(),
            left_time_column: "trade_ts".to_string(),
            right_time_column: "quote_ts".to_string(),
            direction: AsofSqlDirection::Backward,
            tolerance: None,
            join_type: AsofSqlJoinType::Left,
        }
    }

    fn decoded_v2_checkpoint(op: &mut AsofJoinOperator) -> (AsofBufferCheckpoint, Vec<u8>) {
        let checkpoint = op.checkpoint().unwrap().unwrap();
        let (&version, payload) = checkpoint.data.split_last().unwrap();
        assert_eq!(version, ASOF_CHECKPOINT_VERSION);
        let (body, schema_ipc) = split_v2_checkpoint(payload, &op.projection.op_name).unwrap();
        (
            rkyv::from_bytes::<AsofBufferCheckpoint, rkyv::rancor::Error>(body).unwrap(),
            schema_ipc.to_vec(),
        )
    }

    fn decoded_checkpoint(op: &mut AsofJoinOperator) -> AsofBufferCheckpoint {
        decoded_v2_checkpoint(op).0
    }

    fn encoded_v1_checkpoint(buffer: &AsofBufferCheckpoint) -> OperatorCheckpoint {
        let body = rkyv::to_bytes::<rkyv::rancor::Error>(buffer).unwrap();
        let mut data = body.to_vec();
        data.push(ASOF_CHECKPOINT_VERSION_V1);
        OperatorCheckpoint { data }
    }

    fn historical_v1_nonempty_checkpoint() -> OperatorCheckpoint {
        // Frozen against the V1 wire layout at bc519b97. Cycle 45 did not change the
        // AsofBufferCheckpoint field layout or the ordinary two-row encoding captured here.
        let hex = include_str!("testdata/asof_v1_nonempty_bc519b97.hex").trim();
        assert_eq!(hex.len() % 2, 0, "V1 checkpoint fixture has odd hex length");
        let data = hex
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digits = std::str::from_utf8(pair).expect("V1 fixture must be ASCII");
                u8::from_str_radix(digits, 16).expect("V1 fixture must contain hex bytes")
            })
            .collect();
        OperatorCheckpoint { data }
    }

    fn encoded_v2_checkpoint(
        buffer: &AsofBufferCheckpoint,
        schema_ipc: &[u8],
    ) -> OperatorCheckpoint {
        let body = rkyv::to_bytes::<rkyv::rancor::Error>(buffer).unwrap();
        let schema_len = u32::try_from(schema_ipc.len()).unwrap();
        let mut data =
            Vec::with_capacity(body.len() + schema_ipc.len() + ASOF_SCHEMA_LENGTH_BYTES + 1);
        data.extend_from_slice(&body);
        data.extend_from_slice(schema_ipc);
        data.extend_from_slice(&schema_len.to_le_bytes());
        data.push(ASOF_CHECKPOINT_VERSION);
        OperatorCheckpoint { data }
    }

    fn empty_buffer_checkpoint() -> AsofBufferCheckpoint {
        AsofBufferCheckpoint {
            right_buffer_ipc: Vec::new(),
            index_entries: Vec::new(),
            last_evicted_watermark: i64::MIN,
        }
    }

    fn schema_only_ipc(schema: &SchemaRef) -> Vec<u8> {
        laminar_core::serialization::serialize_batches_stream_bounded(
            schema.as_ref(),
            std::iter::empty::<&RecordBatch>(),
            MAX_ASOF_SCHEMA_IPC_BYTES,
        )
        .unwrap()
    }

    fn overwrite_first_record_batch_body_length(mut ipc: Vec<u8>, body_length: i64) -> Vec<u8> {
        const PREFIX_BYTES: usize = 8;
        let schema_metadata_len =
            usize::try_from(u32::from_le_bytes(ipc[4..PREFIX_BYTES].try_into().unwrap())).unwrap();
        let schema_message =
            arrow_ipc::root_as_message(&ipc[PREFIX_BYTES..PREFIX_BYTES + schema_metadata_len])
                .unwrap();
        let record_offset = PREFIX_BYTES
            + schema_metadata_len
            + usize::try_from(schema_message.bodyLength()).unwrap();
        assert_eq!(&ipc[record_offset..record_offset + 4], &[0xff; 4]);
        let metadata_len = usize::try_from(u32::from_le_bytes(
            ipc[record_offset + 4..record_offset + PREFIX_BYTES]
                .try_into()
                .unwrap(),
        ))
        .unwrap();
        let metadata_start = record_offset + PREFIX_BYTES;
        let metadata = &ipc[metadata_start..metadata_start + metadata_len];
        let table = usize::try_from(u32::from_le_bytes(metadata[..4].try_into().unwrap())).unwrap();
        let vtable_distance = usize::try_from(i32::from_le_bytes(
            metadata[table..table + 4].try_into().unwrap(),
        ))
        .unwrap();
        let vtable = table.checked_sub(vtable_distance).unwrap();
        let field_offset = usize::from(u16::from_le_bytes(
            metadata[vtable + usize::from(arrow_ipc::Message::VT_BODYLENGTH)
                ..vtable + usize::from(arrow_ipc::Message::VT_BODYLENGTH) + 2]
                .try_into()
                .unwrap(),
        ));
        assert_ne!(field_offset, 0);
        let body_length_offset = metadata_start + table + field_offset;
        ipc[body_length_offset..body_length_offset + 8].copy_from_slice(&body_length.to_le_bytes());
        ipc
    }

    #[tokio::test]
    async fn test_basic_asof_join() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);

        let result = op
            .process(&[vec![trades_batch()], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_cross_cycle_match() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);

        // Cycle 1: right data only
        let result = op
            .process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        assert!(result.is_empty());

        // Cycle 2: left data arrives — should match against buffered right
        let result = op
            .process(&[vec![trades_batch()], vec![]], &[0, 0])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_eviction_on_watermark_advance() {
        let mut config = test_config();
        config.tolerance = Some(std::time::Duration::from_millis(50));
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", config, None, ctx);

        // Buffer right data at ts=90 and ts=140
        op.process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();

        // Advance watermark to 200 → cutoff = 200 - 50 = 150
        // quote@90 (< 150) evicted, quote@140 (< 150) evicted
        op.process(&[vec![], vec![]], &[200, 200]).await.unwrap();

        // Left at ts=100: backward match needs quote@90, but it's evicted
        let result = op
            .process(&[vec![trades_batch()], vec![]], &[200, 200])
            .await
            .unwrap();

        // AAPL trade@100 can't match (quote@90 evicted), GOOG trade@150 can't match (quote@140 evicted)
        // Left join: both emitted with null right columns
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
        // Right-side columns (quote_ts, bid) should all be null
        let right_start = 3; // After symbol, trade_ts, price
        for col_idx in right_start..result[0].num_columns() {
            assert!(
                result[0].column(col_idx).is_null(0),
                "col {col_idx} row 0 should be null"
            );
            assert!(
                result[0].column(col_idx).is_null(1),
                "col {col_idx} row 1 should be null"
            );
        }
    }

    #[tokio::test]
    async fn test_checkpoint_roundtrip() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx.clone());

        // Buffer right data
        op.process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();

        // Checkpoint
        let cp = op.checkpoint().unwrap().expect("should have state");
        assert!(!cp.data.is_empty());
        let (&version, payload) = cp.data.split_last().unwrap();
        assert_eq!(version, ASOF_CHECKPOINT_VERSION);
        let (_, schema_ipc) = split_v2_checkpoint(payload, "test_asof").unwrap();
        assert!(
            schema_ipc.is_empty(),
            "a retained right batch is the sole schema authority"
        );

        // Restore into new operator
        let mut op2 = AsofJoinOperator::new("test_asof", test_config(), None, ctx);
        op2.restore(cp).unwrap();

        // Left data should match against restored right buffer
        let result = op2
            .process(&[vec![trades_batch()], vec![]], &[0, 0])
            .await
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn checkpoint_preserves_right_schema_after_full_eviction() {
        let mut config = test_config();
        config.tolerance = Some(std::time::Duration::from_millis(50));
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", config.clone(), None, ctx.clone());

        op.process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        op.process(&[vec![], vec![]], &[200, 200]).await.unwrap();
        let checkpoint = op.checkpoint().unwrap().unwrap();
        let (&version, payload) = checkpoint.data.split_last().unwrap();
        assert_eq!(version, ASOF_CHECKPOINT_VERSION);
        let (body, schema_ipc) = split_v2_checkpoint(payload, "test_asof").unwrap();
        let buffer = rkyv::from_bytes::<AsofBufferCheckpoint, rkyv::rancor::Error>(body).unwrap();
        assert!(buffer.right_buffer_ipc.is_empty());
        assert!(buffer.index_entries.is_empty());
        assert!(!schema_ipc.is_empty());

        // The live operator still knows the right schema after checkpoint compaction removes all
        // retained rows, so LEFT ASOF emits null-extended right columns.
        let live = op
            .process(&[vec![trades_batch()], vec![]], &[200, 200])
            .await
            .unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].num_rows(), 2);
        assert_eq!(live[0].num_columns(), 5);

        let mut restored = AsofJoinOperator::new("test_asof", config, None, ctx);
        restored.restore(checkpoint).unwrap();
        let output = restored
            .process(&[vec![trades_batch()], vec![]], &[200, 200])
            .await
            .unwrap();

        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 2);
        assert_eq!(output[0].num_columns(), 5);
        assert_eq!(output[0].schema(), live[0].schema());
        let fields = output[0].schema();
        assert_eq!(fields.field(0).name(), "symbol");
        assert_eq!(fields.field(1).name(), "trade_ts");
        assert_eq!(fields.field(2).name(), "price");
        assert_eq!(fields.field(3).name(), "quote_ts");
        assert_eq!(fields.field(4).name(), "bid");
        assert!(fields.field(3).is_nullable());
        assert!(fields.field(4).is_nullable());
        for column in 3..5 {
            assert!(output[0].column(column).is_null(0));
            assert!(output[0].column(column).is_null(1));
        }
    }

    #[test]
    fn version_2_never_seen_schema_roundtrips_as_none() {
        let ctx = laminar_sql::create_session_context();
        let mut source = AsofJoinOperator::new("test_asof", test_config(), None, ctx.clone());
        let checkpoint = source.checkpoint().unwrap().unwrap();

        let mut restored = AsofJoinOperator::new("test_asof", test_config(), None, ctx);
        restored.restore(checkpoint).unwrap();

        assert!(restored.right_schema.is_none());
        assert!(restored.right_buffer.retained_schema().is_none());
        assert_eq!(restored.last_evicted_watermark, i64::MIN);
    }

    #[tokio::test]
    async fn version_1_nonempty_checkpoint_derives_right_schema() {
        let mut config = test_config();
        config.tolerance = Some(std::time::Duration::from_millis(50));
        let ctx = laminar_sql::create_session_context();
        let mut restored = AsofJoinOperator::new("test_asof", config, None, ctx);
        let legacy = historical_v1_nonempty_checkpoint();
        assert_eq!(legacy.data.last(), Some(&ASOF_CHECKPOINT_VERSION_V1));
        restored.restore(legacy).unwrap();

        assert_eq!(restored.right_schema, Some(quotes_batch().schema()));
        let output = restored
            .process(&[vec![trades_batch()], vec![]], &[0, 0])
            .await
            .unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 2);
        let bids = output[0]
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(bids.values(), &[149.0, 2790.0]);

        restored
            .process(&[vec![], vec![]], &[200, 200])
            .await
            .unwrap();
        let checkpoint = restored.checkpoint().unwrap().unwrap();
        let (_, payload) = checkpoint.data.split_last().unwrap();
        let (_, schema_ipc) = split_v2_checkpoint(payload, "test_asof").unwrap();
        assert!(
            !schema_ipc.is_empty(),
            "a migrated v1 schema remains checkpointable after full eviction"
        );
    }

    #[tokio::test]
    async fn version_1_empty_left_checkpoint_fails_without_replacing_live_state() {
        let ctx = laminar_sql::create_session_context();
        let mut target = AsofJoinOperator::new("test_asof", test_config(), None, ctx);
        target
            .process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        let prior_schema = target.right_schema.clone();
        let prior_watermark = target.last_evicted_watermark;

        let error = target
            .restore(encoded_v1_checkpoint(&empty_buffer_checkpoint()))
            .expect_err("an empty v1 LEFT checkpoint has no recoverable schema authority");

        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.to_string().contains("version-1 empty LEFT"));
        assert_eq!(target.right_schema, prior_schema);
        assert_eq!(target.last_evicted_watermark, prior_watermark);
        let output = target
            .process(&[vec![trades_batch()], vec![]], &[0, 0])
            .await
            .unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn empty_inner_checkpoints_never_null_extend_left_rows() {
        let mut config = test_config();
        config.join_type = AsofSqlJoinType::Inner;
        let ctx = laminar_sql::create_session_context();
        let mut restored = AsofJoinOperator::new("test_asof", config.clone(), None, ctx.clone());

        restored
            .restore(encoded_v1_checkpoint(&empty_buffer_checkpoint()))
            .unwrap();

        assert!(restored.right_schema.is_none());
        assert!(restored.right_buffer.retained_schema().is_none());
        let output = restored
            .process(&[vec![trades_batch()], vec![]], &[0, 0])
            .await
            .unwrap();
        assert!(output.is_empty());

        config.tolerance = Some(std::time::Duration::from_millis(50));
        let mut source = AsofJoinOperator::new("test_asof", config.clone(), None, ctx.clone());
        source
            .process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        source
            .process(&[vec![], vec![]], &[200, 200])
            .await
            .unwrap();
        let checkpoint = source.checkpoint().unwrap().unwrap();
        let mut restored_v2 = AsofJoinOperator::new("test_asof", config, None, ctx);
        restored_v2.restore(checkpoint).unwrap();
        assert!(restored_v2.right_schema.is_some());
        let output = restored_v2
            .process(&[vec![trades_batch()], vec![]], &[200, 200])
            .await
            .unwrap();
        assert!(output.is_empty());
    }

    #[tokio::test]
    async fn malformed_v2_schema_appendix_does_not_replace_live_state() {
        let mut config = test_config();
        config.tolerance = Some(std::time::Duration::from_millis(50));
        let ctx = laminar_sql::create_session_context();
        let mut source = AsofJoinOperator::new("test_asof", config.clone(), None, ctx.clone());
        source
            .process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        source
            .process(&[vec![], vec![]], &[200, 200])
            .await
            .unwrap();
        let (empty_buffer, canonical_schema) = decoded_v2_checkpoint(&mut source);
        assert!(!canonical_schema.is_empty());

        let mut trailing = canonical_schema.clone();
        trailing.push(0);
        let missing_eos = canonical_schema[..canonical_schema.len() - 8].to_vec();
        let zero_row_batch = laminar_core::serialization::serialize_batch_stream(
            &RecordBatch::new_empty(quotes_batch().schema()),
        )
        .unwrap();
        let malformed = [vec![1, 2, 3], trailing, missing_eos, zero_row_batch];

        let mut target = AsofJoinOperator::new("test_asof", config, None, ctx);
        target
            .process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        let prior_schema = target.right_schema.clone();
        for schema_ipc in malformed {
            let error = target
                .restore(encoded_v2_checkpoint(&empty_buffer, &schema_ipc))
                .expect_err("malformed schema appendix must fail closed");
            assert!(matches!(error, DbError::Checkpoint(_)));
            assert_eq!(target.right_schema, prior_schema);
            assert!(target.right_buffer.retained_schema().is_some());
        }

        let output = target
            .process(&[vec![trades_batch()], vec![]], &[0, 0])
            .await
            .unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 2);
    }

    #[test]
    fn version_2_trailer_bounds_fail_before_body_decode() {
        let buffer = empty_buffer_checkpoint();
        let body = rkyv::to_bytes::<rkyv::rancor::Error>(&buffer).unwrap();
        let mut cases = vec![
            OperatorCheckpoint { data: Vec::new() },
            OperatorCheckpoint { data: vec![99] },
            OperatorCheckpoint {
                data: vec![ASOF_CHECKPOINT_VERSION],
            },
        ];

        let mut oversized = body.to_vec();
        oversized.extend_from_slice(
            &u32::try_from(MAX_ASOF_SCHEMA_IPC_BYTES + 1)
                .unwrap()
                .to_le_bytes(),
        );
        oversized.push(ASOF_CHECKPOINT_VERSION);
        cases.push(OperatorCheckpoint { data: oversized });

        let mut exceeds_payload = body.to_vec();
        exceeds_payload.extend_from_slice(&u32::try_from(body.len() + 1).unwrap().to_le_bytes());
        exceeds_payload.push(ASOF_CHECKPOINT_VERSION);
        cases.push(OperatorCheckpoint {
            data: exceeds_payload,
        });

        let ctx = laminar_sql::create_session_context();
        for checkpoint in cases {
            let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx.clone());
            let error = op.restore(checkpoint).expect_err("invalid v2 framing");
            assert!(matches!(error, DbError::Checkpoint(_)));
        }
    }

    #[tokio::test]
    async fn version_2_rejects_dual_schema_authority_and_invalid_index() {
        let ctx = laminar_sql::create_session_context();
        let mut source = AsofJoinOperator::new("test_asof", test_config(), None, ctx.clone());
        source
            .process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        let buffer = decoded_checkpoint(&mut source);
        let schema_ipc = schema_only_ipc(&quotes_batch().schema());

        let mut cases = vec![encoded_v2_checkpoint(&buffer, &schema_ipc)];

        let mut out_of_range = buffer.clone();
        out_of_range.index_entries[0].2[0] = u32::MAX;
        cases.push(encoded_v2_checkpoint(&out_of_range, &[]));

        let mut wrong_timestamp = buffer.clone();
        wrong_timestamp.index_entries[0].1 = wrong_timestamp.index_entries[0].1.saturating_add(1);
        cases.push(encoded_v2_checkpoint(&wrong_timestamp, &[]));

        let mut duplicate = buffer.clone();
        duplicate
            .index_entries
            .push(duplicate.index_entries[0].clone());
        cases.push(encoded_v2_checkpoint(&duplicate, &[]));

        let mut missing = buffer.clone();
        missing.index_entries.clear();
        cases.push(encoded_v2_checkpoint(&missing, &[]));

        let mut index_without_buffer = empty_buffer_checkpoint();
        index_without_buffer.index_entries.push((1, 2, vec![0]));
        cases.push(encoded_v2_checkpoint(&index_without_buffer, &[]));

        let mut tie_source = AsofJoinOperator::new("test_asof", test_config(), None, ctx.clone());
        tie_source
            .process(&[vec![], vec![equal_timestamp_quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        let mut reversed_tie = decoded_checkpoint(&mut tie_source);
        assert_eq!(reversed_tie.index_entries.len(), 1);
        reversed_tie.index_entries[0].2.reverse();
        cases.push(encoded_v2_checkpoint(&reversed_tie, &[]));

        for checkpoint in cases {
            let mut restored = AsofJoinOperator::new("test_asof", test_config(), None, ctx.clone());
            let error = restored
                .restore(checkpoint)
                .expect_err("noncanonical v2 checkpoint must fail closed");
            assert!(matches!(error, DbError::Checkpoint(_)));
        }
    }

    #[tokio::test]
    async fn retained_buffer_ipc_requires_one_bounded_complete_batch() {
        let ctx = laminar_sql::create_session_context();
        let mut source = AsofJoinOperator::new("test_asof", test_config(), None, ctx.clone());
        source
            .process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        let buffer = decoded_checkpoint(&mut source);

        let mut trailing = buffer.clone();
        trailing.right_buffer_ipc.push(0);

        let quotes = quotes_batch();
        let mut two_batches = buffer.clone();
        two_batches.right_buffer_ipc =
            laminar_core::serialization::serialize_batches_stream_bounded(
                quotes.schema().as_ref(),
                [&quotes, &quotes],
                MAX_ASOF_SCHEMA_IPC_BYTES,
            )
            .unwrap();

        let mut oversized_body = buffer.clone();
        oversized_body.right_buffer_ipc =
            overwrite_first_record_batch_body_length(oversized_body.right_buffer_ipc, i64::MAX);

        for malformed in [trailing, two_batches, oversized_body] {
            let mut restored = AsofJoinOperator::new("test_asof", test_config(), None, ctx.clone());
            let error = restored
                .restore(encoded_v2_checkpoint(&malformed, &[]))
                .expect_err(
                    "noncanonical or overdeclared retained IPC must fail before Arrow decode",
                );
            assert!(matches!(error, DbError::Checkpoint(_)));
            assert!(restored.right_buffer.retained_schema().is_none());
        }
    }

    #[tokio::test]
    async fn right_schema_drift_is_rejected_before_state_changes() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);
        op.process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        let before = decoded_checkpoint(&mut op);
        let changed_schema = Arc::new(Schema::new(vec![
            Field::new("symbol", DataType::Utf8, false),
            Field::new("quote_ts", DataType::Int64, false),
            Field::new("bid", DataType::Int64, false),
        ]));

        let error = op
            .process(
                &[vec![], vec![RecordBatch::new_empty(changed_schema.clone())]],
                &[0, 0],
            )
            .await
            .expect_err("right schema drift must be rejected before ingest");

        assert!(matches!(error, DbError::SchemaMismatch(_)));
        assert!(!error.requires_pipeline_recovery());
        assert_eq!(op.right_schema, Some(quotes_batch().schema()));
        let after = decoded_checkpoint(&mut op);
        assert_eq!(before.right_buffer_ipc, after.right_buffer_ipc);
        assert_eq!(before.index_entries.len(), after.index_entries.len());
    }

    #[tokio::test]
    async fn oversized_right_schemas_are_rejected_before_ingest() {
        let oversized = quotes_batch_with_metadata_bytes(MAX_ASOF_SOURCE_SCHEMA_BYTES);
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);

        let error = op
            .process(&[vec![], vec![oversized]], &[0, 0])
            .await
            .expect_err("oversized right schema must fail before state admission");

        assert!(matches!(error, DbError::SchemaMismatch(_)));
        assert!(!error.requires_pipeline_recovery());
        assert!(error.to_string().contains("right schema uses"));
        assert!(op.right_schema.is_none());
        assert!(op.right_buffer.retained_schema().is_none());
        assert!(op.checkpoint().unwrap().is_some());

        op.process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        let before = decoded_checkpoint(&mut op);
        let pointer_distinct_equal = quotes_batch_with_oversized_empty_metadata_capacity();
        assert!(!Arc::ptr_eq(
            &pointer_distinct_equal.schema(),
            op.right_schema.as_ref().unwrap()
        ));

        let error = op
            .process(&[vec![], vec![pointer_distinct_equal]], &[0, 0])
            .await
            .expect_err("pointer-distinct equal schemas must not bypass the memory cap");

        assert!(matches!(error, DbError::SchemaMismatch(_)));
        assert!(!error.requires_pipeline_recovery());
        let after = decoded_checkpoint(&mut op);
        assert_eq!(after.right_buffer_ipc, before.right_buffer_ipc);
        assert_eq!(after.index_entries, before.index_entries);
        assert_eq!(after.last_evicted_watermark, before.last_evicted_watermark);
    }

    #[tokio::test]
    async fn near_limit_schema_remains_checkpointable_after_restore_and_eviction() {
        let near_limit = quotes_batch_with_metadata_bytes(MAX_ASOF_SOURCE_SCHEMA_BYTES - 8192);
        let schema_bytes = schema_memory_size(near_limit.schema().as_ref());
        assert!(schema_bytes <= MAX_ASOF_SOURCE_SCHEMA_BYTES);
        assert!(schema_bytes > MAX_ASOF_SOURCE_SCHEMA_BYTES - 16_384);

        let mut config = test_config();
        config.tolerance = Some(std::time::Duration::from_millis(50));
        let ctx = laminar_sql::create_session_context();
        let mut source = AsofJoinOperator::new("test_asof", config.clone(), None, ctx.clone());
        source
            .process(&[vec![], vec![near_limit]], &[0, 0])
            .await
            .unwrap();

        let retained = source.checkpoint().unwrap().unwrap();
        let mut restored = AsofJoinOperator::new("test_asof", config.clone(), None, ctx.clone());
        restored.restore(retained).unwrap();
        restored
            .process(&[vec![], vec![]], &[200, 200])
            .await
            .unwrap();

        let empty = restored.checkpoint().unwrap().unwrap();
        let (&version, payload) = empty.data.split_last().unwrap();
        assert_eq!(version, ASOF_CHECKPOINT_VERSION);
        let (_, schema_ipc) = split_v2_checkpoint(payload, "test_asof").unwrap();
        assert!(!schema_ipc.is_empty());

        let mut restored_again = AsofJoinOperator::new("test_asof", config, None, ctx);
        restored_again.restore(empty).unwrap();
        let output = restored_again
            .process(&[vec![trades_batch()], vec![]], &[200, 200])
            .await
            .unwrap();
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_columns(), 5);
        assert_eq!(output[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn oversized_retained_schema_is_not_restored_by_v1_or_v2() {
        let ctx = laminar_sql::create_session_context();
        let mut source = AsofJoinOperator::new("test_asof", test_config(), None, ctx.clone());
        source
            .process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        let mut buffer = decoded_checkpoint(&mut source);
        buffer.right_buffer_ipc = laminar_core::serialization::serialize_batch_stream(
            &quotes_batch_with_metadata_bytes(MAX_ASOF_SOURCE_SCHEMA_BYTES),
        )
        .unwrap();

        for checkpoint in [
            encoded_v1_checkpoint(&buffer),
            encoded_v2_checkpoint(&buffer, &[]),
        ] {
            let mut restored = AsofJoinOperator::new("test_asof", test_config(), None, ctx.clone());
            let error = restored
                .restore(checkpoint)
                .expect_err("restore must not accept state that cannot be checkpointed later");
            assert!(matches!(error, DbError::Checkpoint(_)));
            assert!(error.to_string().contains("not checkpointable"));
            assert!(restored.right_schema.is_none());
            assert!(restored.right_buffer.retained_schema().is_none());
        }
    }

    #[test]
    fn restore_rejects_decoded_schema_memory_amplification() {
        let template = Field::new("", DataType::Null, true);
        let per_field = template
            .size()
            .saturating_add(std::mem::size_of::<arrow::datatypes::FieldRef>());
        let extra_fields = MAX_ASOF_DECODED_SCHEMA_BYTES / per_field + 16;
        let mut fields = Vec::with_capacity(extra_fields + 2);
        fields.push(Field::new("symbol", DataType::Utf8, false));
        fields.push(Field::new("quote_ts", DataType::Int64, false));
        fields.extend((0..extra_fields).map(|_| template.clone()));
        let schema = Arc::new(Schema::new(fields));
        assert!(schema_memory_size(schema.as_ref()) > MAX_ASOF_DECODED_SCHEMA_BYTES);
        let ipc = schema_only_ipc(&schema);
        assert!(ipc.len() <= MAX_ASOF_SCHEMA_IPC_BYTES);

        let error = decode_right_schema(&ipc, &test_config(), "test_asof")
            .expect_err("decoded schema memory limit must fail closed");

        assert!(matches!(error, DbError::Checkpoint(_)));
        assert!(error.to_string().contains("decoded right schema uses"));
    }

    #[tokio::test]
    async fn test_empty_left() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);

        let result = op
            .process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn test_empty_inputs() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);

        let result = op.process(&[], &[0]).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn pre_apply_right_validation_failure_leaves_asof_state_unchanged() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);

        let error = op
            .process(&[vec![], vec![quotes_without_time_column()]], &[0, 0])
            .await
            .expect_err("missing right time column must fail before ingest");

        assert!(!error.requires_pipeline_recovery());
        assert!(!error.requires_pipeline_halt());
        assert!(!matches!(error, DbError::StatefulOperatorPartialApply(_)));
        assert!(op.right_schema.is_none());

        let decoded = decoded_checkpoint(&mut op);
        assert!(decoded.right_buffer_ipc.is_empty());
        assert!(decoded.index_entries.is_empty());
    }

    #[tokio::test]
    async fn pre_apply_right_validation_failure_preserves_prior_asof_state() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);
        op.process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();
        let before = decoded_checkpoint(&mut op);

        let error = op
            .process(&[vec![], vec![quotes_without_time_column()]], &[0, 0])
            .await
            .expect_err("malformed right input must not disturb prior state");
        assert!(!error.requires_pipeline_recovery());

        let after = decoded_checkpoint(&mut op);
        assert_eq!(before.right_buffer_ipc, after.right_buffer_ipc);
        let mut before_entries = before.index_entries;
        let mut after_entries = after.index_entries;
        before_entries.sort_unstable();
        after_entries.sort_unstable();
        assert_eq!(before_entries, after_entries);
    }

    #[tokio::test]
    async fn left_only_failure_after_prior_asof_state_remains_ordinary() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new("test_asof", test_config(), None, ctx);
        op.process(&[vec![], vec![quotes_batch()]], &[0, 0])
            .await
            .unwrap();

        let error = op
            .process(&[vec![trades_without_key_column()], vec![]], &[0, 0])
            .await
            .expect_err("left validation must fail without changing retained right state");

        assert!(!error.requires_pipeline_recovery());
        assert!(!error.requires_pipeline_halt());
        assert!(!matches!(error, DbError::StatefulOperatorPartialApply(_)));

        let decoded = decoded_checkpoint(&mut op);
        assert_eq!(decoded.index_entries.len(), 2);
    }

    #[tokio::test]
    async fn post_projection_failure_requires_recovery_after_asof_state_admission() {
        let ctx = laminar_sql::create_session_context();
        let mut op = AsofJoinOperator::new(
            "test_asof",
            test_config(),
            Some(Arc::from("SELECT missing FROM __asof_tmp")),
            ctx,
        );

        let error = op
            .process(&[vec![trades_batch()], vec![quotes_batch()]], &[0, 0])
            .await
            .expect_err("invalid projection must fail after right-state admission");

        assert!(matches!(
            &error,
            DbError::StatefulOperatorPartialApply(message)
                if message.contains("may have changed right-side state")
        ));
        assert!(error.requires_pipeline_recovery());

        // Forensic inspection only: production recovery propagation prevents checkpoint admission
        // after this error (covered by the coordinator recovery-exclusion test from Cycle 42).
        let decoded = decoded_checkpoint(&mut op);
        assert!(!decoded.right_buffer_ipc.is_empty());
        assert_eq!(decoded.index_entries.len(), 2);
    }

    #[test]
    fn asof_partial_apply_preserves_stronger_dispositions() {
        let recovery = partial_apply(DbError::Checkpoint("injected recovery".into()));
        assert!(matches!(recovery, DbError::Checkpoint(_)));

        let partial_send = partial_apply(DbError::ShufflePartialSend("injected recovery".into()));
        assert!(matches!(partial_send, DbError::ShufflePartialSend(_)));

        let halt = partial_apply(DbError::BackpressureFail("injected halt".into()));
        assert!(matches!(halt, DbError::BackpressureFail(_)));

        let terminal = partial_apply(DbError::ShuffleTerminal("injected halt".into()));
        assert!(matches!(terminal, DbError::ShuffleTerminal(_)));
    }

    #[test]
    fn test_name() {
        let ctx = laminar_sql::create_session_context();
        let op = AsofJoinOperator::new("my_asof_query", test_config(), None, ctx);
        assert_eq!(&*op.projection.op_name, "my_asof_query");
    }
}

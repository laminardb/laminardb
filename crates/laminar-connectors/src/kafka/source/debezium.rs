//! Debezium operation normalization and row-mutation decoding.

use super::{
    Arc, Array, ConnectorError, DataType, NormalizedDebeziumBatch, RecordBatch, RecordBatchOptions,
    SchemaRef, SourceMutation, StringArray,
};

pub(super) fn normalize_kafka_debezium_batch(
    records: &RecordBatch,
    visible_schema: &SchemaRef,
) -> Result<NormalizedDebeziumBatch, ConnectorError> {
    let operation_index = visible_schema.fields().len();
    let timestamp_index = operation_index + 1;
    let records_schema = records.schema();
    if records.num_columns() != timestamp_index + 1
        || records_schema.fields()[..operation_index] != visible_schema.fields()[..]
    {
        return Err(ConnectorError::SchemaMismatch(
            "Kafka Debezium decoder output does not match the configured visible schema".into(),
        ));
    }
    let operation_field = records_schema.field(operation_index);
    let timestamp_field = records_schema.field(timestamp_index);
    if operation_field.name() != "__op"
        || operation_field.data_type() != &DataType::Utf8
        || operation_field.is_nullable()
        || timestamp_field.name() != "__ts_ms"
        || timestamp_field.data_type() != &DataType::Int64
        || timestamp_field.is_nullable()
    {
        return Err(ConnectorError::SchemaMismatch(
            "Kafka Debezium decoder control columns are malformed or misplaced".into(),
        ));
    }
    let operations = records
        .column(operation_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            ConnectorError::SchemaMismatch("Kafka Debezium __op column must be Utf8".into())
        })?;
    let mutations = decode_debezium_mutations(operations, records.num_rows())?;
    if records.column(timestamp_index).null_count() != 0 {
        return Err(ConnectorError::SchemaMismatch(
            "Kafka Debezium __ts_ms column must not contain nulls".into(),
        ));
    }

    let options = RecordBatchOptions::new().with_row_count(Some(records.num_rows()));
    let visible = RecordBatch::try_new_with_options(
        Arc::clone(visible_schema),
        records.columns()[..operation_index].to_vec(),
        &options,
    )
    .map_err(|error| {
        ConnectorError::SchemaMismatch(format!(
            "failed to remove Kafka Debezium decoder control columns: {error}"
        ))
    })?;
    Ok((visible, mutations))
}

pub(super) fn decode_debezium_mutations(
    operations: &StringArray,
    row_count: usize,
) -> Result<Option<Box<[SourceMutation]>>, ConnectorError> {
    if operations.len() != row_count {
        return Err(ConnectorError::SchemaMismatch(format!(
            "Kafka Debezium operation count {} does not match decoded row count {}",
            operations.len(),
            row_count
        )));
    }

    let mut mutations: Option<Vec<SourceMutation>> = None;
    for (row, operation) in operations.iter().enumerate() {
        let operation = operation.ok_or_else(|| {
            ConnectorError::SchemaMismatch(format!(
                "Kafka Debezium __op is null at decoded row {row}"
            ))
        })?;
        match operation {
            "c" | "u" | "r" => {
                if let Some(mutations) = mutations.as_mut() {
                    mutations.push(SourceMutation::Put);
                }
            }
            "d" => {
                let mutations = mutations.get_or_insert_with(|| {
                    let mut mutations = Vec::with_capacity(row_count);
                    mutations.resize(row, SourceMutation::Put);
                    mutations
                });
                mutations.push(SourceMutation::Tombstone);
            }
            unknown => {
                return Err(ConnectorError::SchemaMismatch(format!(
                    "Kafka Debezium __op has unknown value '{unknown}' at decoded row {row}"
                )));
            }
        }
    }
    Ok(mutations.map(Vec::into_boxed_slice))
}

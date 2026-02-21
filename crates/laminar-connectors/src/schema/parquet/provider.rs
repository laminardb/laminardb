//! Parquet schema provider implementing [`SchemaProvider`].
//!
//! Reads the Parquet file footer to extract an authoritative Arrow schema
//! with optional per-field metadata.

use std::collections::HashMap;

use arrow_schema::SchemaRef;
use async_trait::async_trait;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use crate::schema::error::{SchemaError, SchemaResult};
use crate::schema::traits::SchemaProvider;
use crate::schema::types::FieldMeta;

/// Provides an Arrow schema extracted from a Parquet file's footer.
///
/// This provider is **authoritative** — Parquet files carry their schema
/// in the footer, so the schema is exact (no inference needed).
pub struct ParquetSchemaProvider {
    /// The Parquet file bytes (complete file including footer).
    data: Vec<u8>,
}

impl ParquetSchemaProvider {
    /// Creates a new provider from complete Parquet file bytes.
    #[must_use]
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }
}

impl std::fmt::Debug for ParquetSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetSchemaProvider")
            .field("data_len", &self.data.len())
            .finish()
    }
}

#[async_trait]
impl SchemaProvider for ParquetSchemaProvider {
    async fn provide_schema(&self) -> SchemaResult<SchemaRef> {
        let bytes = bytes::Bytes::copy_from_slice(&self.data);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|e| {
            SchemaError::InferenceFailed(format!("cannot read Parquet footer: {e}"))
        })?;

        Ok(builder.schema().clone())
    }

    fn is_authoritative(&self) -> bool {
        true
    }

    async fn field_metadata(&self) -> SchemaResult<HashMap<String, FieldMeta>> {
        let bytes = bytes::Bytes::copy_from_slice(&self.data);
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(|e| {
            SchemaError::InferenceFailed(format!("cannot read Parquet footer: {e}"))
        })?;

        let schema = builder.schema();
        let parquet_meta = builder.metadata();
        let file_meta = parquet_meta.file_metadata();

        let mut result = HashMap::new();
        for (idx, field) in schema.fields().iter().enumerate() {
            let mut meta = FieldMeta::new()
                .with_field_id(idx as u32)
                .with_source_type(format!("{}", field.data_type()));

            // Copy Arrow field metadata as properties.
            for (k, v) in field.metadata() {
                meta = meta.with_property(k, v);
            }

            // Add Parquet-level metadata if available.
            if let Some(created_by) = file_meta.created_by() {
                meta = meta.with_property("parquet.created_by", created_by);
            }

            result.insert(field.name().clone(), meta);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    fn sample_parquet_bytes() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        buf
    }

    #[tokio::test]
    async fn test_provide_schema() {
        let data = sample_parquet_bytes();
        let provider = ParquetSchemaProvider::new(data);
        let schema = provider.provide_schema().await.unwrap();

        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).name(), "name");
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn test_is_authoritative() {
        let provider = ParquetSchemaProvider::new(vec![]);
        assert!(provider.is_authoritative());
    }

    #[tokio::test]
    async fn test_field_metadata() {
        let data = sample_parquet_bytes();
        let provider = ParquetSchemaProvider::new(data);
        let meta = provider.field_metadata().await.unwrap();

        assert!(meta.contains_key("id"));
        assert!(meta.contains_key("name"));
        let id_meta = &meta["id"];
        assert_eq!(id_meta.field_id, Some(0));
        assert!(id_meta.source_type.as_ref().unwrap().contains("Int64"));
    }

    #[tokio::test]
    async fn test_invalid_bytes() {
        let provider = ParquetSchemaProvider::new(b"not parquet".to_vec());
        let result = provider.provide_schema().await;
        assert!(result.is_err());
    }
}

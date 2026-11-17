//! The format decoders a file source reads with, and the `_metadata` column it can append.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::config::{FileFormat, FileSourceConfig};
use super::json_documents::JsonDocumentsDecoder;
use super::text_decoder::TextLineDecoder;
use crate::config::ConnectorConfig;
#[cfg(feature = "fpml")]
use crate::connector::ConnectorTaskGuard;
use crate::error::ConnectorError;
use crate::schema::traits::FormatDecoder;

pub(super) fn build_decoder_and_schema(
    format: FileFormat,
    src_config: &FileSourceConfig,
    connector_config: &ConnectorConfig,
) -> Result<(Arc<dyn FormatDecoder>, SchemaRef), ConnectorError> {
    match format {
        FileFormat::Csv => {
            let schema = connector_config.arrow_schema().unwrap_or_else(|| {
                Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
            });
            let csv_config = crate::schema::CsvDecoderConfig {
                delimiter: src_config.csv_delimiter,
                has_header: src_config.csv_has_header,
                ..crate::schema::CsvDecoderConfig::default()
            };
            let decoder = crate::schema::CsvDecoder::with_config(schema.clone(), csv_config);
            Ok((Arc::new(decoder), schema))
        }
        FileFormat::Json => {
            let schema = connector_config.arrow_schema().unwrap_or_else(|| {
                Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
            });
            let json_config = crate::schema::JsonDecoderConfig::default();
            let decoder = crate::schema::JsonDecoder::with_config(schema.clone(), json_config);
            Ok((Arc::new(JsonDocumentsDecoder::new(decoder)), schema))
        }
        FileFormat::Text => {
            let decoder = TextLineDecoder::new();
            let schema = decoder.output_schema();
            Ok((Arc::new(decoder), schema))
        }
        FileFormat::Parquet => {
            // For Parquet, schema comes from the file footer (authoritative).
            // Use a placeholder schema; it will be refined on the first file read.
            let schema = connector_config.arrow_schema().unwrap_or_else(|| {
                Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
            });
            let decoder = crate::schema::parquet::ParquetDecoder::new(schema.clone());
            Ok((Arc::new(decoder), schema))
        }
        FileFormat::ArrowIpc => {
            // Arrow IPC files embed their schema in the header — use it as
            // authoritative. The DDL schema is used as placeholder until
            // the first file is read.
            let schema = connector_config.arrow_schema().unwrap_or_else(|| {
                Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]))
            });
            let decoder = super::arrow_ipc_codec::ArrowIpcDecoder::new(schema.clone());
            Ok((Arc::new(decoder), schema))
        }
        #[cfg(feature = "fpml")]
        FileFormat::Fpml => Err(ConnectorError::Internal(
            "FpML decoders are started asynchronously".into(),
        )),
    }
}

/// Resolve the closure and profile off the runtime, check them against the committed schema
/// contract, and start the validator worker. Files larger than one document are rejected before
/// they are read.
#[cfg(feature = "fpml")]
pub(super) async fn start_fpml_decoder(
    src_config: &mut FileSourceConfig,
    config: &ConnectorConfig,
    registry: Option<prometheus::Registry>,
    guard: ConnectorTaskGuard,
) -> Result<(Arc<dyn FormatDecoder>, SchemaRef), ConnectorError> {
    let fpml = crate::fpml::FpmlConfig::from_connector_config(config)?;
    src_config.max_file_bytes = src_config
        .max_file_bytes
        .min(fpml.limits.max_document_bytes);
    let resolving = fpml.clone();
    let (artifact, profile) = tokio::task::spawn_blocking(move || resolving.resolve())
        .await
        .map_err(|error| {
            ConnectorError::Internal(format!("FpML profile loader failed: {error}"))
        })??;
    if let Some(committed) = config
        .schema_binding()
        .and_then(|binding| binding.value.as_ref())
    {
        if committed.identity != profile.native_schema(&artifact).identity {
            return Err(ConnectorError::SchemaMismatch(
                "the FpML profile or schema closure differs from the one this source was created with; recreate the source to change it".into(),
            ));
        }
    }
    let source = config.get("laminar.source.name").unwrap_or("files");
    let decoder = crate::fpml::FpmlDecoder::start(
        artifact,
        &profile,
        &fpml,
        source,
        registry.as_ref(),
        Some(guard),
    )
    .await?;
    Ok((Arc::new(decoder), profile.schema()))
}

pub(super) fn append_metadata_column(
    batch: &RecordBatch,
    file_path: &str,
    file_size: u64,
    modified_ms: u64,
) -> Result<RecordBatch, ConnectorError> {
    use arrow_array::{ArrayRef, StringArray, StructArray, UInt64Array};

    let n = batch.num_rows();
    let file_name = std::path::Path::new(file_path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(file_path);

    let path_array: ArrayRef = Arc::new(StringArray::from(vec![file_path; n]));
    let name_array: ArrayRef = Arc::new(StringArray::from(vec![file_name; n]));
    let size_array: ArrayRef = Arc::new(UInt64Array::from(vec![file_size; n]));
    #[allow(clippy::cast_possible_wrap)]
    let mod_array: ArrayRef = Arc::new(arrow_array::TimestampMillisecondArray::from(vec![
        modified_ms as i64;
        n
    ]));

    let fields = vec![
        Field::new("file_path", DataType::Utf8, false),
        Field::new("file_name", DataType::Utf8, false),
        Field::new("file_size", DataType::UInt64, false),
        Field::new(
            "file_modification_time",
            DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
            true,
        ),
    ];
    let struct_array = StructArray::try_new(
        fields.into(),
        vec![path_array, name_array, size_array, mod_array],
        None,
    )
    .map_err(|e| ConnectorError::ReadError(format!("metadata struct error: {e}")))?;

    // Append struct column to batch.
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(struct_array));

    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.as_ref().clone())
        .collect();
    fields.push(Field::new(
        "_metadata",
        DataType::Struct(
            vec![
                Field::new("file_path", DataType::Utf8, false),
                Field::new("file_name", DataType::Utf8, false),
                Field::new("file_size", DataType::UInt64, false),
                Field::new(
                    "file_modification_time",
                    DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None),
                    true,
                ),
            ]
            .into(),
        ),
        false,
    ));

    RecordBatch::try_new(
        Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema().metadata().clone(),
        )),
        columns,
    )
    .map_err(|e| ConnectorError::ReadError(format!("metadata append error: {e}")))
}

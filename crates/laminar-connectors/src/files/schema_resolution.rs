//! Bounded metadata discovery and opt-in file sampling, without an ingestion cursor.

use std::collections::BTreeMap;
use std::io::{BufReader, Cursor, Read, Seek, SeekFrom};
use std::sync::{Arc, OnceLock};

use arrow_schema::SchemaRef;

use super::config::{FileFormat, FileSinkConfig, FileSourceConfig};
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::schema::resolution::{
    bind_external, logical_binding, NativeSchema, SchemaBinding, SchemaDirection, SchemaOrigin,
};

static WORKERS: OnceLock<Arc<tokio::sync::Semaphore>> = OnceLock::new();

const MAX_ENTRIES: usize = 4096;
const MAX_FILES: usize = 64;
const SAMPLE_BYTES: usize = 1024 * 1024;
const SAMPLE_ROWS: usize = 1000;

pub(super) async fn resolve(
    config: &ConnectorConfig,
    explicit: Option<SchemaRef>,
) -> Result<SchemaBinding, ConnectorError> {
    let parsed = FileSourceConfig::from_connector_config(config)?;
    let format = parsed
        .format
        .or_else(|| FileFormat::from_extension(&parsed.path))
        .ok_or_else(|| {
            ConnectorError::ConfigurationError(
                "file schema resolution requires an explicit format for a directory".into(),
            )
        })?;
    let workers = Arc::clone(WORKERS.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(4))));
    let permit = tokio::time::timeout(std::time::Duration::from_secs(10), workers.acquire_owned())
        .await
        .map_err(|_| ConnectorError::Timeout(10_000))?
        .map_err(|_| ConnectorError::Internal("file schema workers are closed".into()))?;
    let config = config.clone();
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            resolve_files(&config, &parsed, format, explicit)
        }),
    )
    .await
    .map_err(|_| ConnectorError::Timeout(10_000))?
    .map_err(|_| ConnectorError::Internal("file schema worker failed".into()))?
}

fn resolve_files(
    config: &ConnectorConfig,
    parsed: &FileSourceConfig,
    format: FileFormat,
    explicit: Option<SchemaRef>,
) -> Result<SchemaBinding, ConnectorError> {
    if format == FileFormat::Text {
        let native = crate::schema::traits::FormatDecoder::output_schema(
            &super::text_decoder::TextLineDecoder::new(),
        );
        return fixed_binding(
            config,
            explicit,
            &native,
            SchemaOrigin::BuiltIn,
            parsed.include_metadata,
        );
    }
    if !matches!(format, FileFormat::Parquet | FileFormat::ArrowIpc) {
        if let Some(explicit) = explicit {
            crate::serde::schema_contract::validate_reader(
                if format == FileFormat::Csv {
                    crate::serde::Format::Csv
                } else {
                    crate::serde::Format::Json
                },
                &explicit,
            )?;
            return logical_binding(
                config,
                SchemaDirection::Source,
                SchemaOrigin::Explicit,
                &output_schema(&explicit, parsed.include_metadata)?,
            );
        }
        if !config
            .get_parsed::<bool>("schema.inference")?
            .unwrap_or(false)
        {
            return Err(ConnectorError::FeatureUnsupported(
                "CSV/JSON file fields require columns or explicit schema.inference=true (4 files, 1 MiB, 1000 rows)".into()));
        }
        let files = files(parsed, None)?;
        let schema = sample_schema(parsed, format, &files)?;
        crate::serde::schema_contract::validate_reader(
            if format == FileFormat::Csv {
                crate::serde::Format::Csv
            } else {
                crate::serde::Format::Json
            },
            &schema,
        )?;
        return logical_binding(
            config,
            SchemaDirection::Source,
            SchemaOrigin::Sample,
            &output_schema(&schema, parsed.include_metadata)?,
        );
    }
    metadata_binding(config, parsed, format, explicit, files(parsed, None)?)
}

fn metadata_binding(
    config: &ConnectorConfig,
    parsed: &FileSourceConfig,
    format: FileFormat,
    explicit: Option<SchemaRef>,
    files: Vec<std::path::PathBuf>,
) -> Result<SchemaBinding, ConnectorError> {
    let mut discovered: Option<SchemaRef> = None;
    let mut evidence = Vec::new();
    for path in files {
        let mut file = std::fs::File::open(&path).map_err(io_error)?;
        let metadata = file.metadata().map_err(io_error)?;
        if metadata.len() > parsed.max_file_bytes as u64 {
            return Err(ConnectorError::SchemaMismatch(
                "discovery file exceeds max_file_bytes".into(),
            ));
        }
        validate_footer(&mut file, format)?;
        let schema = match format {
            FileFormat::Parquet => {
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(|error| {
                        ConnectorError::SchemaMismatch(format!("Parquet metadata: {error}"))
                    })?
                    .schema()
                    .clone()
            }
            FileFormat::ArrowIpc => arrow_ipc::reader::FileReader::try_new(file, None)
                .map_err(|error| {
                    ConnectorError::SchemaMismatch(format!("Arrow IPC metadata: {error}"))
                })?
                .schema(),
            _ => {
                return Err(ConnectorError::Internal(
                    "non-metadata format in metadata discovery".into(),
                ))
            }
        };
        if let Some(previous) = &discovered {
            if let Some(explicit) = &explicit {
                let mut projected = logical_binding(
                    config,
                    SchemaDirection::Source,
                    SchemaOrigin::Explicit,
                    explicit,
                )?;
                bind_external(&mut projected, &schema)?;
            } else if previous.as_ref() != schema.as_ref() {
                return Err(ConnectorError::SchemaMismatch(
                    "file set has heterogeneous embedded schemas; use explicit compatible projections or separate datasets".into()));
            }
        } else {
            discovered = Some(schema);
        }
        evidence.push(serde_json::json!({"name": path.file_name().and_then(|name| name.to_str()), "size": metadata.len(),
            "modified_ms": metadata.modified().ok().and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok()).map(|duration| duration.as_millis().to_string())}));
    }
    let native = discovered.ok_or_else(|| ConnectorError::SchemaMismatch(
        "empty file set has no embedded schema; declare columns or place a schema-bearing file in the dataset".into()))?;
    let mut binding = fixed_binding(
        config,
        explicit,
        &native,
        SchemaOrigin::Metadata,
        parsed.include_metadata,
    )?;
    binding.value = Some(NativeSchema {
        format: match format {
            FileFormat::Parquet => "parquet",
            _ => "arrow_ipc",
        }
        .into(),
        identity: BTreeMap::from([("dataset".into(), parsed.path.clone())]),
        definition: serde_json::json!({"files": evidence, "schema": binding.external}),
        references: Vec::new(),
    });
    binding
        .canonical_bytes()
        .map_err(crate::schema::resolution::binding_error)?;
    Ok(binding)
}

pub(super) fn sink_binding(
    config: &ConnectorConfig,
    input: &SchemaRef,
) -> Result<SchemaBinding, ConnectorError> {
    let parsed = FileSinkConfig::from_connector_config(config)?;
    let mut binding = logical_binding(config, SchemaDirection::Sink, SchemaOrigin::Query, input)?;
    if !matches!(parsed.format, FileFormat::Parquet | FileFormat::ArrowIpc) {
        if matches!(parsed.format, FileFormat::Csv | FileFormat::Json) {
            crate::serde::schema_contract::validate_writer(
                if parsed.format == FileFormat::Csv {
                    crate::serde::Format::Csv
                } else {
                    crate::serde::Format::Json
                },
                input,
            )?;
        }
        return Ok(binding);
    }
    match std::fs::metadata(&parsed.path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(binding),
        Err(error) => return Err(io_error(error)),
        Ok(metadata) if !metadata.is_dir() => {
            return Err(ConnectorError::SchemaMismatch(
                "file sink path must be a directory".into(),
            ))
        }
        Ok(_) => {}
    }
    let mut properties = config.properties().clone();
    properties.remove("glob_pattern");
    let mut source_config = ConnectorConfig::with_properties("files", properties);
    source_config.set("include_metadata", "false");
    let source = FileSourceConfig::from_connector_config(&source_config)?;
    let matching = files(&source, Some((&parsed.prefix, parsed.format.extension())))?;
    if matching.is_empty() {
        return Ok(binding);
    }
    let native = metadata_binding(config, &source, parsed.format, None, matching)?;
    bind_external(&mut binding, &Arc::new(native.logical))?;
    binding.value = native.value;
    Ok(binding)
}

pub(super) async fn resolve_sink(
    config: &ConnectorConfig,
    input: SchemaRef,
) -> Result<SchemaBinding, ConnectorError> {
    let workers = Arc::clone(WORKERS.get_or_init(|| Arc::new(tokio::sync::Semaphore::new(4))));
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    let permit = tokio::time::timeout_at(deadline, workers.acquire_owned())
        .await
        .map_err(|_| ConnectorError::Timeout(10_000))?
        .map_err(|_| ConnectorError::Internal("file schema workers are closed".into()))?;
    let config = config.clone();
    tokio::time::timeout_at(
        deadline,
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            sink_binding(&config, &input)
        }),
    )
    .await
    .map_err(|_| ConnectorError::Timeout(10_000))?
    .map_err(|_| ConnectorError::Internal("file target discovery worker failed".into()))?
}

fn files(
    parsed: &FileSourceConfig,
    output: Option<(&str, &str)>,
) -> Result<Vec<std::path::PathBuf>, ConnectorError> {
    let (directory, path_glob) = super::discovery::split_dir_and_glob(&parsed.path);
    let matcher = parsed
        .glob_pattern
        .as_ref()
        .or(path_glob.as_ref())
        .map(|pattern| {
            globset::Glob::new(pattern)
                .map(|glob| glob.compile_matcher())
                .map_err(|_| ConnectorError::ConfigurationError("invalid file glob_pattern".into()))
        })
        .transpose()?;
    let mut files = Vec::new();
    for (index, entry) in std::fs::read_dir(directory).map_err(io_error)?.enumerate() {
        if index >= MAX_ENTRIES {
            return Err(ConnectorError::SchemaMismatch(
                "file discovery exceeds 4096 directory entries; narrow the dataset".into(),
            ));
        }
        let entry = entry.map_err(io_error)?;
        let filename = entry.file_name();
        if output.is_some_and(|(prefix, extension)| {
            filename.to_str().is_none_or(|name| {
                !name.starts_with(&format!("{prefix}_"))
                    || !name.ends_with(&format!(".{extension}"))
            })
        }) {
            continue;
        }
        if !entry.file_type().map_err(io_error)?.is_file()
            || matcher
                .as_ref()
                .is_some_and(|matcher| !matcher.is_match(entry.file_name()))
        {
            continue;
        }
        files.push(entry.path());
        if files.len() > MAX_FILES {
            return Err(ConnectorError::SchemaMismatch(
                "file schema discovery exceeds 64 files; narrow glob_pattern".into(),
            ));
        }
    }
    files.sort();
    Ok(files)
}

fn sample_schema(
    parsed: &FileSourceConfig,
    format: FileFormat,
    files: &[std::path::PathBuf],
) -> Result<SchemaRef, ConnectorError> {
    let mut schemas = Vec::new();
    let mut bytes_left = SAMPLE_BYTES;
    let mut rows_left = SAMPLE_ROWS;
    for path in files.iter().take(4) {
        if bytes_left == 0 || rows_left == 0 {
            break;
        }
        let file = std::fs::File::open(path).map_err(io_error)?;
        let mut bytes = Vec::new();
        file.take(bytes_left as u64)
            .read_to_end(&mut bytes)
            .map_err(io_error)?;
        bytes_left -= bytes.len();
        let Some(end) = bytes.iter().rposition(|byte| *byte == b'\n') else {
            continue;
        };
        bytes.truncate(end + 1);
        if format == FileFormat::Csv {
            validate_csv_sample(parsed, &bytes, rows_left)?;
        }
        let (schema, rows) = match format {
            FileFormat::Json => arrow_json::reader::infer_json_schema(
                &mut BufReader::new(Cursor::new(bytes)),
                Some(rows_left),
            ),
            FileFormat::Csv => arrow_csv::reader::Format::default()
                .with_header(parsed.csv_has_header)
                .with_delimiter(parsed.csv_delimiter)
                .infer_schema(Cursor::new(bytes), Some(rows_left)),
            _ => {
                return Err(ConnectorError::FeatureUnsupported(
                    "sampling is supported only for CSV and newline-delimited JSON files".into(),
                ))
            }
        }
        .map_err(|error| {
            ConnectorError::SchemaMismatch(format!("bounded sample is inconclusive: {error}"))
        })?;
        rows_left = rows_left.saturating_sub(rows);
        schemas.push(schema);
    }
    if schemas.is_empty() {
        return Err(ConnectorError::SchemaMismatch(
            "empty sample cannot establish fields".into(),
        ));
    }
    arrow_schema::Schema::try_merge(schemas)
        .map(Arc::new)
        .map_err(|error| {
            ConnectorError::SchemaMismatch(format!(
                "heterogeneous samples require explicit fields: {error}"
            ))
        })
}

fn fixed_binding(
    config: &ConnectorConfig,
    explicit: Option<SchemaRef>,
    native: &SchemaRef,
    origin: SchemaOrigin,
    metadata: bool,
) -> Result<SchemaBinding, ConnectorError> {
    let selected_origin = if explicit.is_some() {
        SchemaOrigin::Explicit
    } else {
        origin
    };
    let logical = output_schema(&explicit.unwrap_or_else(|| Arc::clone(native)), metadata)?;
    let external = output_schema(native, metadata)?;
    let mut binding = logical_binding(config, SchemaDirection::Source, selected_origin, &logical)?;
    bind_external(&mut binding, &external)?;
    Ok(binding)
}

pub(super) fn output_schema(
    payload: &SchemaRef,
    metadata: bool,
) -> Result<SchemaRef, ConnectorError> {
    if !metadata {
        return Ok(Arc::clone(payload));
    }
    if payload.index_of("_metadata").is_ok() {
        return Err(ConnectorError::SchemaMismatch(
            "_metadata is reserved when include_metadata=true".into(),
        ));
    }
    let empty = arrow_array::RecordBatch::new_empty(Arc::clone(payload));
    super::source::append_metadata_column(&empty, "", 0, 0).map(|batch| batch.schema())
}

#[allow(clippy::needless_pass_by_value)] // Adapter for owned Result::map_err errors.
fn io_error(error: std::io::Error) -> ConnectorError {
    ConnectorError::ReadError(format!("file metadata unavailable: {error}"))
}

fn validate_footer(file: &mut std::fs::File, format: FileFormat) -> Result<(), ConnectorError> {
    let footer_bytes = if format == FileFormat::Parquet {
        8_usize
    } else {
        10
    };
    let mut footer = [0_u8; 10];
    file.seek(SeekFrom::End(-i64::try_from(footer_bytes).map_err(
        |_| ConnectorError::SchemaMismatch("embedded schema footer is too large".into()),
    )?))
    .map_err(io_error)?;
    file.read_exact(&mut footer[..footer_bytes])
        .map_err(io_error)?;
    let bytes =
        u32::from_le_bytes(footer[..4].try_into().map_err(|_| {
            ConnectorError::SchemaMismatch("invalid embedded schema footer".into())
        })?);
    if bytes > 1024 * 1024 {
        return Err(ConnectorError::SchemaMismatch(
            "embedded schema footer exceeds 1 MiB".into(),
        ));
    }
    file.seek(SeekFrom::Start(0)).map_err(io_error)?;
    Ok(())
}

fn validate_csv_sample(
    parsed: &FileSourceConfig,
    bytes: &[u8],
    max_rows: usize,
) -> Result<(), ConnectorError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(parsed.csv_has_header)
        .delimiter(parsed.csv_delimiter)
        .from_reader(bytes);
    let mut populated = Vec::new();
    let mut rows = 0;
    for record in reader.records().take(max_rows) {
        let record = record.map_err(|_| {
            ConnectorError::SchemaMismatch("bounded CSV sample ends in a malformed record".into())
        })?;
        if record.len() > 4096 {
            return Err(ConnectorError::SchemaMismatch(
                "CSV sample exceeds 4096 columns".into(),
            ));
        }
        populated.resize(record.len(), false);
        for (index, value) in record.iter().enumerate() {
            populated[index] |= !value.is_empty();
        }
        rows += 1;
    }
    if rows == 0 || populated.iter().any(|present| !present) {
        return Err(ConnectorError::SchemaMismatch(
            "empty or all-null CSV sample columns cannot establish types; declare columns".into(),
        ));
    }
    Ok(())
}

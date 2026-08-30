//! Versioned participant descriptors for coordinated Iceberg publication.

use std::collections::HashSet;
use std::fmt;

use apache_avro::types::Value as AvroValue;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use iceberg::spec::{DataFile, DataFileFormat, FormatVersion};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::connector::MAX_COORDINATED_COMMIT_PAYLOAD_BYTES;
use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::{
    stable_catalog_identity, IcebergSinkConfig, ICEBERG_MAX_FILES_PER_CHECKPOINT,
};
use crate::lakehouse::iceberg_io::effective_data_location;

use super::epoch_writer::{EpochIdentity, EpochOutput};
use super::file_finalizer::validate_coordinated_file_name;
use super::fingerprint::{data_file_fingerprint, hash_count, hash_len_prefixed};

const DESCRIPTOR_VERSION: u8 = 1;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IcebergTableBindingV1 {
    pub(super) catalog_implementation: String,
    pub(super) catalog_identity: String,
    pub(super) table_uuid: String,
    pub(super) table_identifier: String,
    pub(super) table_location: String,
    pub(super) table_ref: String,
    pub(super) base_snapshot_id: Option<i64>,
    pub(super) metadata_location: String,
    pub(super) schema_id: i32,
    pub(super) partition_spec_id: i32,
    pub(super) sort_order_id: i64,
    pub(super) format_version: u8,
}

impl IcebergTableBindingV1 {
    pub(super) fn from_table(
        table: &iceberg::table::Table,
        config: &IcebergSinkConfig,
    ) -> Result<Self, ConnectorError> {
        let metadata = table.metadata();
        let binding = Self {
            catalog_implementation: config.catalog.catalog_type.to_string(),
            catalog_identity: stable_catalog_identity(&config.catalog, &config.storage),
            table_uuid: metadata.uuid().to_string(),
            table_identifier: table.identifier().to_string(),
            table_location: metadata.location().to_string(),
            table_ref: config.table_ref.clone(),
            base_snapshot_id: metadata.current_snapshot_id(),
            metadata_location: table.metadata_location().unwrap_or_default().to_string(),
            schema_id: metadata.current_schema_id(),
            partition_spec_id: metadata.default_partition_spec_id(),
            sort_order_id: metadata.default_sort_order_id(),
            format_version: format_version_number(metadata.format_version()),
        };
        validate_table_binding_shape(&binding)?;
        Ok(binding)
    }

    pub(super) fn has_same_append_target(&self, other: &Self) -> bool {
        self.catalog_implementation == other.catalog_implementation
            && self.catalog_identity == other.catalog_identity
            && self.table_uuid == other.table_uuid
            && self.table_identifier == other.table_identifier
            && self.table_location == other.table_location
            && self.table_ref == other.table_ref
            && self.schema_id == other.schema_id
            && self.partition_spec_id == other.partition_spec_id
            && self.sort_order_id == other.sort_order_id
            && self.format_version == other.format_version
    }
}

impl fmt::Debug for IcebergTableBindingV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IcebergTableBindingV1")
            .field("base_snapshot_id", &self.base_snapshot_id)
            .field("schema_id", &self.schema_id)
            .field("partition_spec_id", &self.partition_spec_id)
            .field("sort_order_id", &self.sort_order_id)
            .field("format_version", &self.format_version)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IcebergFileFingerprintV1 {
    pub(super) path: String,
    pub(super) metadata_sha256: String,
    pub(super) records: u64,
    pub(super) bytes: u64,
}

impl fmt::Debug for IcebergFileFingerprintV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IcebergFileFingerprintV1")
            .field("records", &self.records)
            .field("bytes", &self.bytes)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct IcebergCommitDescriptorV1 {
    pub(super) version: u8,
    pub(super) table: IcebergTableBindingV1,
    pub(super) deployment_id: String,
    pub(super) sink_id: String,
    pub(super) epoch_id: u64,
    pub(super) participant_id: u64,
    pub(super) batch_fingerprint: String,
    pub(super) data_files_avro: String,
    pub(super) files: Vec<IcebergFileFingerprintV1>,
    pub(super) file_count: u64,
    pub(super) record_count: u64,
    pub(super) file_bytes: u64,
    pub(super) input_bytes: u64,
}

impl fmt::Debug for IcebergCommitDescriptorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IcebergCommitDescriptorV1")
            .field("version", &self.version)
            .field("table", &self.table)
            .field("epoch_id", &self.epoch_id)
            .field("participant_id", &self.participant_id)
            .field("file_count", &self.file_count)
            .field("record_count", &self.record_count)
            .field("file_bytes", &self.file_bytes)
            .field("input_bytes", &self.input_bytes)
            .finish_non_exhaustive()
    }
}

impl IcebergCommitDescriptorV1 {
    pub(super) fn encode(
        table: &iceberg::table::Table,
        config: &IcebergSinkConfig,
        identity: &EpochIdentity,
        mut output: EpochOutput,
    ) -> Result<Vec<u8>, ConnectorError> {
        for file in &output.data_files {
            validate_descriptor_data_file(table, identity, file)?;
        }
        output
            .data_files
            .sort_by(|left, right| left.file_path().cmp(right.file_path()));
        let files = output
            .data_files
            .iter()
            .map(data_file_fingerprint)
            .collect::<Result<Vec<_>, _>>()?;
        let record_count = checked_sum(
            output.data_files.iter().map(DataFile::record_count),
            "record count",
        )?;
        if record_count != output.rows {
            return Err(ConnectorError::Internal(format!(
                "Iceberg row accounting mismatch: writer {}, files {record_count}",
                output.rows
            )));
        }
        let file_bytes = checked_sum(
            output.data_files.iter().map(DataFile::file_size_in_bytes),
            "file bytes",
        )?;
        let binding = IcebergTableBindingV1::from_table(table, config)?;
        let data_files_avro = encode_data_files(table, &output.data_files, &files)?;
        let batch_fingerprint = participant_fingerprint(
            &binding,
            identity,
            &files,
            record_count,
            file_bytes,
            output.bytes,
        )?;
        let descriptor = Self {
            version: DESCRIPTOR_VERSION,
            table: binding,
            deployment_id: identity.deployment_id.clone(),
            sink_id: identity.sink_id.clone(),
            epoch_id: identity.epoch,
            participant_id: identity.participant_id,
            batch_fingerprint,
            data_files_avro,
            file_count: u64::try_from(files.len())
                .map_err(|_| ConnectorError::Internal("Iceberg file count exceeds u64".into()))?,
            files,
            record_count,
            file_bytes,
            input_bytes: output.bytes,
        };
        let encoded = serde_json::to_vec(&descriptor).map_err(|error| {
            ConnectorError::Internal(format!("encode Iceberg descriptor: {error}"))
        })?;
        let limit = config
            .max_descriptor_bytes
            .min(MAX_COORDINATED_COMMIT_PAYLOAD_BYTES);
        if encoded.len() > limit {
            return Err(ConnectorError::WriteError(format!(
                "Iceberg commit descriptor is {} bytes; configured limit is {limit}",
                encoded.len()
            )));
        }
        Ok(encoded)
    }

    pub(super) fn decode(payload: &[u8]) -> Result<Self, ConnectorError> {
        if payload.len() > MAX_COORDINATED_COMMIT_PAYLOAD_BYTES {
            return Err(ConnectorError::TransactionError(format!(
                "Iceberg commit descriptor exceeds the fixed {MAX_COORDINATED_COMMIT_PAYLOAD_BYTES}-byte limit"
            )));
        }
        let descriptor: Self = serde_json::from_slice(payload).map_err(|error| {
            ConnectorError::TransactionError(format!(
                "[LDB-ICEBERG-DESCRIPTOR-JSON] invalid Iceberg commit descriptor ({:?} at line {} column {})",
                error.classify(),
                error.line(),
                error.column()
            ))
        })?;
        if descriptor.version != DESCRIPTOR_VERSION {
            return Err(ConnectorError::TransactionError(format!(
                "unsupported Iceberg commit descriptor version {}",
                descriptor.version
            )));
        }
        validate_table_binding_shape(&descriptor.table)?;
        if descriptor.participant_id == 0 {
            return Err(ConnectorError::TransactionError(
                "Iceberg commit descriptor has an invalid runtime identity".into(),
            ));
        }
        validate_descriptor_identity("deployment identity", &descriptor.deployment_id)?;
        validate_descriptor_identity("sink identity", &descriptor.sink_id)?;
        if descriptor.files.len() > ICEBERG_MAX_FILES_PER_CHECKPOINT {
            return Err(ConnectorError::TransactionError(format!(
                "Iceberg commit descriptor exceeds the fixed {ICEBERG_MAX_FILES_PER_CHECKPOINT}-file limit"
            )));
        }
        if descriptor.file_count != descriptor.files.len() as u64 {
            return Err(ConnectorError::TransactionError(
                "Iceberg commit descriptor file count does not match its entries".into(),
            ));
        }
        validate_sha256(
            &descriptor.batch_fingerprint,
            "participant batch fingerprint",
        )?;
        for file in &descriptor.files {
            validate_coordinated_file_name(
                &file.path,
                &descriptor.deployment_id,
                &descriptor.sink_id,
                descriptor.participant_id,
                descriptor.epoch_id,
            )?;
            validate_sha256(&file.metadata_sha256, "data-file metadata fingerprint")?;
        }
        if checked_untrusted_sum(
            descriptor.files.iter().map(|file| file.records),
            "envelope record count",
        )? != descriptor.record_count
            || checked_untrusted_sum(
                descriptor.files.iter().map(|file| file.bytes),
                "envelope file bytes",
            )? != descriptor.file_bytes
        {
            return Err(ConnectorError::TransactionError(
                "Iceberg commit descriptor aggregate counts do not match its envelope".into(),
            ));
        }
        let mut paths = HashSet::with_capacity(descriptor.files.len());
        let mut previous = None;
        for file in &descriptor.files {
            if !paths.insert(file.path.as_str()) {
                return Err(ConnectorError::TransactionError(
                    "Iceberg commit descriptor repeats a data file".into(),
                ));
            }
            if previous.is_some_and(|value: &str| value >= file.path.as_str()) {
                return Err(ConnectorError::TransactionError(
                    "Iceberg commit descriptor files are not canonically ordered".into(),
                ));
            }
            previous = Some(file.path.as_str());
        }
        Ok(descriptor)
    }

    pub(super) fn decode_data_files(
        &self,
        table: &iceberg::table::Table,
    ) -> Result<Vec<DataFile>, ConnectorError> {
        let metadata = table.metadata();
        let schema = metadata.schema_by_id(self.table.schema_id).ok_or_else(|| {
            ConnectorError::TransactionError(format!(
                "Iceberg descriptor schema {} is absent from table metadata",
                self.table.schema_id
            ))
        })?;
        let spec = metadata
            .partition_spec_by_id(self.table.partition_spec_id)
            .ok_or_else(|| {
                ConnectorError::TransactionError(format!(
                    "Iceberg descriptor partition spec {} is absent from table metadata",
                    self.table.partition_spec_id
                ))
            })?;
        let partition_type = spec.partition_type(schema).map_err(|error| {
            ConnectorError::TransactionError(format!(
                "resolve Iceberg descriptor partition type ({})",
                error.kind()
            ))
        })?;
        let encoded = BASE64.decode(&self.data_files_avro).map_err(|error| {
            ConnectorError::TransactionError(format!(
                "decode Iceberg descriptor data files: {error}"
            ))
        })?;
        validate_data_file_record_count(&encoded, self.files.len())?;
        let mut reader = encoded.as_slice();
        let mut data_files = iceberg::spec::read_data_files_from_avro(
            &mut reader,
            schema,
            self.table.partition_spec_id,
            &partition_type,
            parse_format_version(self.table.format_version)?,
        )
        .map_err(|error| {
            ConnectorError::TransactionError(format!(
                "[LDB-ICEBERG-DESCRIPTOR-DATA-FILES] parse Iceberg descriptor data files ({})",
                error.kind()
            ))
        })?;
        let identity = EpochIdentity {
            deployment_id: self.deployment_id.clone(),
            sink_id: self.sink_id.clone(),
            participant_id: self.participant_id,
            epoch: self.epoch_id,
        };
        for file in &data_files {
            validate_descriptor_data_file(table, &identity, file)?;
        }
        data_files.sort_by(|left, right| left.file_path().cmp(right.file_path()));
        let observed = data_files
            .iter()
            .map(data_file_fingerprint)
            .collect::<Result<Vec<_>, _>>()?;
        if observed != self.files {
            return Err(ConnectorError::TransactionError(
                "Iceberg descriptor data-file fingerprints do not match its envelope".into(),
            ));
        }
        let record_count = checked_sum(
            data_files.iter().map(DataFile::record_count),
            "decoded record count",
        )?;
        let file_bytes = checked_sum(
            data_files.iter().map(DataFile::file_size_in_bytes),
            "decoded file bytes",
        )?;
        if record_count != self.record_count || file_bytes != self.file_bytes {
            return Err(ConnectorError::TransactionError(
                "Iceberg descriptor aggregate counts do not match decoded data files".into(),
            ));
        }
        let fingerprint = participant_fingerprint(
            &self.table,
            &identity,
            &self.files,
            self.record_count,
            self.file_bytes,
            self.input_bytes,
        )?;
        if fingerprint != self.batch_fingerprint {
            return Err(ConnectorError::TransactionError(
                "Iceberg descriptor participant fingerprint does not match its contents".into(),
            ));
        }
        Ok(data_files)
    }
}

pub(super) fn validate_table_binding_shape(
    binding: &IcebergTableBindingV1,
) -> Result<(), ConnectorError> {
    validate_descriptor_identity("catalog implementation", &binding.catalog_implementation)?;
    validate_sha256(&binding.catalog_identity, "catalog identity")?;
    validate_descriptor_identity("table identifier", &binding.table_identifier)?;
    validate_descriptor_identity("table ref", &binding.table_ref)?;
    let table_uuid = uuid::Uuid::parse_str(&binding.table_uuid).map_err(|_| {
        ConnectorError::TransactionError("Iceberg descriptor table UUID is not canonical".into())
    })?;
    if table_uuid.is_nil() || table_uuid.to_string() != binding.table_uuid {
        return Err(ConnectorError::TransactionError(
            "Iceberg descriptor table UUID is not canonical".into(),
        ));
    }
    validate_descriptor_location("table location", &binding.table_location)?;
    validate_descriptor_location("metadata location", &binding.metadata_location)?;
    parse_format_version(binding.format_version)?;
    Ok(())
}

fn validate_descriptor_identity(label: &str, value: &str) -> Result<(), ConnectorError> {
    if value.is_empty()
        || value.chars().any(char::is_control)
        || crate::security::value_contains_uri_secret(value, false)
    {
        return Err(ConnectorError::TransactionError(format!(
            "Iceberg descriptor {label} is invalid"
        )));
    }
    Ok(())
}

fn validate_descriptor_location(label: &str, value: &str) -> Result<(), ConnectorError> {
    if value.is_empty()
        || value.chars().any(char::is_control)
        || crate::security::value_contains_uri_secret(value, false)
    {
        return Err(ConnectorError::TransactionError(format!(
            "[LDB-ICEBERG-DESCRIPTOR-LOCATION] Iceberg descriptor {label} is absent or unsafe"
        )));
    }
    Ok(())
}

fn validate_data_file_record_count(encoded: &[u8], expected: usize) -> Result<(), ConnectorError> {
    validate_data_file_avro_codec(encoded)?;
    let mut reader = apache_avro::Reader::new(encoded).map_err(|_| {
        ConnectorError::TransactionError(
            "[LDB-ICEBERG-DESCRIPTOR-AVRO] Iceberg descriptor data files are not valid Avro".into(),
        )
    })?;
    for _ in 0..expected {
        match reader.next() {
            Some(Ok(_)) => {}
            Some(Err(_)) => return Err(invalid_data_file_avro()),
            None => return Err(data_file_count_error()),
        }
    }
    match reader.next() {
        None => Ok(()),
        Some(Ok(_)) => Err(data_file_count_error()),
        Some(Err(_)) => Err(invalid_data_file_avro()),
    }
}

fn validate_data_file_avro_codec(encoded: &[u8]) -> Result<(), ConnectorError> {
    let mut header = encoded
        .strip_prefix(b"Obj\x01")
        .ok_or_else(invalid_data_file_avro)?;
    let metadata_schema = apache_avro::Schema::map(apache_avro::Schema::Bytes);
    let metadata = apache_avro::from_avro_datum(&metadata_schema, &mut header, None)
        .map_err(|_| invalid_data_file_avro())?;
    let AvroValue::Map(metadata) = metadata else {
        return Err(invalid_data_file_avro());
    };
    let codec = metadata.get("avro.codec");
    if matches!(codec, Some(AvroValue::Bytes(value)) if value == b"null") {
        return Ok(());
    }
    Err(ConnectorError::TransactionError(
        "[LDB-ICEBERG-DESCRIPTOR-AVRO-CODEC] Iceberg descriptor data files must use the uncompressed canonical Avro codec"
            .into(),
    ))
}

fn invalid_data_file_avro() -> ConnectorError {
    ConnectorError::TransactionError(
        "[LDB-ICEBERG-DESCRIPTOR-AVRO] Iceberg descriptor data files are not valid Avro".into(),
    )
}

fn data_file_count_error() -> ConnectorError {
    ConnectorError::TransactionError(
        "[LDB-ICEBERG-DESCRIPTOR-FILE-COUNT] Iceberg descriptor Avro record count does not match its bounded envelope"
            .into(),
    )
}

fn validate_descriptor_data_file(
    table: &iceberg::table::Table,
    identity: &EpochIdentity,
    file: &DataFile,
) -> Result<(), ConnectorError> {
    validate_coordinated_file_name(
        file.file_path(),
        &identity.deployment_id,
        &identity.sink_id,
        identity.participant_id,
        identity.epoch,
    )?;
    if file.file_format() != DataFileFormat::Parquet {
        return Err(ConnectorError::TransactionError(
            "[LDB-ICEBERG-DATA-FILE-FORMAT] coordinated append descriptors require Parquet data files"
                .into(),
        ));
    }
    let data_location = effective_data_location(table);
    let prefix = format!("{}/", data_location.trim_end_matches('/'));
    let relative = file.file_path().strip_prefix(&prefix).ok_or_else(|| {
        ConnectorError::TransactionError(
            "[LDB-ICEBERG-DATA-FILE-LOCATION] coordinated data file is outside the table data location"
                .into(),
        )
    })?;
    if relative.is_empty()
        || relative
            .bytes()
            .any(|byte| byte.is_ascii_control() || matches!(byte, b'\\' | b'?' | b'#'))
        || relative
            .split('/')
            .any(|component| component.is_empty() || component == "." || component == "..")
    {
        return Err(ConnectorError::TransactionError(
            "[LDB-ICEBERG-DATA-FILE-LOCATION] coordinated data file has an invalid relative location"
                .into(),
        ));
    }
    Ok(())
}

fn encode_data_files(
    table: &iceberg::table::Table,
    files: &[DataFile],
    fingerprints: &[IcebergFileFingerprintV1],
) -> Result<String, ConnectorError> {
    let mut generated = Vec::new();
    iceberg::spec::write_data_files_to_avro(
        &mut generated,
        files.iter().cloned(),
        table.metadata().default_partition_type(),
        table.metadata().format_version(),
    )
    .map_err(|error| {
        ConnectorError::Internal(format!("encode Iceberg descriptor data files: {error}"))
    })?;
    let encoded = canonicalize_data_files_avro(&generated, fingerprints)?;
    Ok(BASE64.encode(encoded))
}

fn canonicalize_data_files_avro(
    generated: &[u8],
    fingerprints: &[IcebergFileFingerprintV1],
) -> Result<Vec<u8>, ConnectorError> {
    let mut reader = apache_avro::Reader::new(generated).map_err(|error| {
        ConnectorError::Internal(format!("read generated Iceberg data-file Avro: {error}"))
    })?;
    let schema_json = canonical_schema_json(reader.writer_schema())?;
    let schema_text = std::str::from_utf8(&schema_json).map_err(|error| {
        ConnectorError::Internal(format!("read canonical Iceberg data-file schema: {error}"))
    })?;
    let schema = apache_avro::Schema::parse_str(schema_text).map_err(|error| {
        ConnectorError::Internal(format!("parse canonical Iceberg data-file schema: {error}"))
    })?;
    let mut values = reader
        .by_ref()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            ConnectorError::Internal(format!("decode generated Iceberg data-file Avro: {error}"))
        })?;
    for value in &mut values {
        canonicalize_avro_value(value)?;
    }

    let marker = descriptor_sync_marker(fingerprints)?;
    let mut encoded = canonical_avro_header(&schema_json, marker)?;
    {
        let mut writer = apache_avro::Writer::builder()
            .schema(&schema)
            .writer(&mut encoded)
            .marker(marker)
            .has_header(true)
            .build();
        for value in values {
            writer.append(value).map_err(|error| {
                ConnectorError::Internal(format!(
                    "encode canonical Iceberg data-file Avro: {error}"
                ))
            })?;
        }
        writer.flush().map_err(|error| {
            ConnectorError::Internal(format!("flush canonical Iceberg data-file Avro: {error}"))
        })?;
    }
    Ok(encoded)
}

fn canonical_schema_json(schema: &apache_avro::Schema) -> Result<Vec<u8>, ConnectorError> {
    let value = serde_json::to_value(schema).map_err(|error| {
        ConnectorError::Internal(format!("normalize Iceberg data-file schema: {error}"))
    })?;
    serde_json::to_vec(&value).map_err(|error| {
        ConnectorError::Internal(format!("serialize Iceberg data-file schema: {error}"))
    })
}

fn canonicalize_avro_value(value: &mut AvroValue) -> Result<(), ConnectorError> {
    match value {
        AvroValue::Array(values) => {
            for value in values.iter_mut() {
                canonicalize_avro_value(value)?;
            }
            if values.iter().all(|value| avro_record_key(value).is_some()) {
                values.sort_by_key(avro_record_key);
            }
        }
        AvroValue::Record(fields) => {
            for (_, value) in fields {
                canonicalize_avro_value(value)?;
            }
        }
        AvroValue::Union(_, value) => canonicalize_avro_value(value)?,
        AvroValue::Map(_) => {
            return Err(ConnectorError::Internal(
                "Iceberg DataFile Avro unexpectedly contains an unordered map".into(),
            ));
        }
        _ => {}
    }
    Ok(())
}

fn avro_record_key(value: &AvroValue) -> Option<i64> {
    let AvroValue::Record(fields) = value else {
        return None;
    };
    fields.iter().find_map(|(name, value)| {
        if name != "key" {
            return None;
        }
        match value {
            AvroValue::Int(value) => Some(i64::from(*value)),
            AvroValue::Long(value) => Some(*value),
            _ => None,
        }
    })
}

fn descriptor_sync_marker(
    fingerprints: &[IcebergFileFingerprintV1],
) -> Result<[u8; 16], ConnectorError> {
    let mut hash = Sha256::new();
    hash.update(b"laminardb-iceberg-descriptor-avro-v2\0");
    hash_count(&mut hash, fingerprints.len())?;
    for file in fingerprints {
        hash_len_prefixed(&mut hash, file.path.as_bytes())?;
        hash_len_prefixed(&mut hash, file.metadata_sha256.as_bytes())?;
    }
    let digest: [u8; 32] = hash.finalize().into();
    let mut marker = [0; 16];
    marker.copy_from_slice(&digest[..16]);
    Ok(marker)
}

fn canonical_avro_header(schema_json: &[u8], marker: [u8; 16]) -> Result<Vec<u8>, ConnectorError> {
    let mut header = Vec::with_capacity(schema_json.len() + 64);
    header.extend_from_slice(b"Obj\x01");
    write_avro_long(&mut header, 2)?;
    write_avro_bytes(&mut header, b"avro.codec")?;
    write_avro_bytes(&mut header, b"null")?;
    write_avro_bytes(&mut header, b"avro.schema")?;
    write_avro_bytes(&mut header, schema_json)?;
    write_avro_long(&mut header, 0)?;
    header.extend_from_slice(&marker);
    Ok(header)
}

fn write_avro_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), ConnectorError> {
    let length = i64::try_from(value.len())
        .map_err(|_| ConnectorError::Internal("Iceberg Avro value exceeds i64".into()))?;
    write_avro_long(output, length)?;
    output.extend_from_slice(value);
    Ok(())
}

fn write_avro_long(output: &mut Vec<u8>, value: i64) -> Result<(), ConnectorError> {
    if value < 0 {
        return Err(ConnectorError::Internal(
            "Iceberg descriptor Avro length cannot be negative".into(),
        ));
    }
    let mut encoded = u64::try_from(value)
        .map_err(|_| ConnectorError::Internal("Iceberg Avro length exceeds u64".into()))?
        << 1;
    while encoded & !0x7f != 0 {
        let byte = u8::try_from((encoded & 0x7f) | 0x80)
            .map_err(|_| ConnectorError::Internal("Iceberg Avro byte overflow".into()))?;
        output.push(byte);
        encoded >>= 7;
    }
    let byte = u8::try_from(encoded)
        .map_err(|_| ConnectorError::Internal("Iceberg Avro byte overflow".into()))?;
    output.push(byte);
    Ok(())
}

fn participant_fingerprint(
    table: &IcebergTableBindingV1,
    identity: &EpochIdentity,
    files: &[IcebergFileFingerprintV1],
    records: u64,
    file_bytes: u64,
    input_bytes: u64,
) -> Result<String, ConnectorError> {
    let mut hash = Sha256::new();
    hash.update(b"laminardb-iceberg-participant-v2\0");
    for value in [
        table.catalog_implementation.as_str(),
        table.catalog_identity.as_str(),
        table.table_uuid.as_str(),
        table.table_identifier.as_str(),
        table.table_location.as_str(),
        table.table_ref.as_str(),
        table.metadata_location.as_str(),
        identity.deployment_id.as_str(),
        identity.sink_id.as_str(),
    ] {
        hash_len_prefixed(&mut hash, value.as_bytes())?;
    }
    hash.update(table.base_snapshot_id.unwrap_or(-1).to_be_bytes());
    hash.update(table.schema_id.to_be_bytes());
    hash.update(table.partition_spec_id.to_be_bytes());
    hash.update(table.sort_order_id.to_be_bytes());
    hash.update([table.format_version]);
    hash.update(identity.epoch.to_be_bytes());
    hash.update(identity.participant_id.to_be_bytes());
    hash.update(records.to_be_bytes());
    hash.update(file_bytes.to_be_bytes());
    hash.update(input_bytes.to_be_bytes());
    hash_count(&mut hash, files.len())?;
    for file in files {
        hash_len_prefixed(&mut hash, file.path.as_bytes())?;
        hash_len_prefixed(&mut hash, file.metadata_sha256.as_bytes())?;
        hash.update(file.records.to_be_bytes());
        hash.update(file.bytes.to_be_bytes());
    }
    Ok(format!("{:x}", hash.finalize()))
}

fn checked_sum(mut values: impl Iterator<Item = u64>, label: &str) -> Result<u64, ConnectorError> {
    values.try_fold(0u64, |total, value| {
        total
            .checked_add(value)
            .ok_or_else(|| ConnectorError::Internal(format!("Iceberg descriptor {label} overflow")))
    })
}

fn checked_untrusted_sum(
    mut values: impl Iterator<Item = u64>,
    label: &str,
) -> Result<u64, ConnectorError> {
    values.try_fold(0u64, |total, value| {
        total.checked_add(value).ok_or_else(|| {
            ConnectorError::TransactionError(format!("Iceberg descriptor {label} overflow"))
        })
    })
}

pub(super) fn format_version_number(version: FormatVersion) -> u8 {
    match version {
        FormatVersion::V1 => 1,
        FormatVersion::V2 => 2,
        FormatVersion::V3 => 3,
    }
}

fn parse_format_version(version: u8) -> Result<FormatVersion, ConnectorError> {
    match version {
        1 => Ok(FormatVersion::V1),
        2 => Ok(FormatVersion::V2),
        3 => Ok(FormatVersion::V3),
        other => Err(ConnectorError::TransactionError(format!(
            "unsupported Iceberg descriptor format version {other}"
        ))),
    }
}

fn validate_sha256(value: &str, label: &str) -> Result<(), ConnectorError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        Ok(())
    } else {
        Err(ConnectorError::TransactionError(format!(
            "Iceberg descriptor {label} is not a canonical SHA-256 digest"
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{DataContentType, DataFileBuilder, DataFileFormat, Struct, StructType};

    use super::*;

    fn data_file(path: &str) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(path.to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .record_count(2)
            .file_size_in_bytes(100)
            .column_sizes(HashMap::from([(3, 30), (1, 10), (2, 20)]))
            .value_counts(HashMap::from([(2, 2), (1, 2), (3, 2)]))
            .build()
            .unwrap()
    }

    fn canonical_payload(files: &[DataFile]) -> Vec<u8> {
        let mut generated = Vec::new();
        iceberg::spec::write_data_files_to_avro(
            &mut generated,
            files.iter().cloned(),
            &StructType::new(Vec::new()),
            FormatVersion::V2,
        )
        .unwrap();
        let fingerprints = files
            .iter()
            .map(data_file_fingerprint)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        canonicalize_data_files_avro(&generated, &fingerprints).unwrap()
    }

    fn descriptor() -> IcebergCommitDescriptorV1 {
        IcebergCommitDescriptorV1 {
            version: DESCRIPTOR_VERSION,
            table: IcebergTableBindingV1 {
                catalog_implementation: "rest".into(),
                catalog_identity: "0".repeat(64),
                table_uuid: "018f0f9d-7b2f-7a61-b72d-f4be1c7f43e1".into(),
                table_identifier: "ns.table".into(),
                table_location: "s3://warehouse/ns/table".into(),
                table_ref: "main".into(),
                base_snapshot_id: None,
                metadata_location: "s3://warehouse/ns/table/metadata/v1.json".into(),
                schema_id: 0,
                partition_spec_id: 0,
                sort_order_id: 0,
                format_version: 2,
            },
            deployment_id: "deployment".into(),
            sink_id: "sink".into(),
            epoch_id: 1,
            participant_id: 1,
            batch_fingerprint: "0".repeat(64),
            data_files_avro: String::new(),
            files: Vec::new(),
            file_count: 0,
            record_count: 0,
            file_bytes: 0,
            input_bytes: 0,
        }
    }

    #[test]
    fn file_fingerprint_is_deterministic_and_content_sensitive() {
        assert_eq!(
            data_file_fingerprint(&data_file("a")).unwrap(),
            data_file_fingerprint(&data_file("a")).unwrap()
        );
        assert_ne!(
            data_file_fingerprint(&data_file("a")).unwrap(),
            data_file_fingerprint(&data_file("b")).unwrap()
        );
    }

    #[test]
    fn descriptor_data_file_payload_is_byte_deterministic() {
        let files = vec![data_file("b"), data_file("a")];
        let expected = canonical_payload(&files);
        for _ in 0..32 {
            assert_eq!(canonical_payload(&files), expected);
        }
        assert_eq!(
            apache_avro::Reader::new(expected.as_slice())
                .unwrap()
                .count(),
            2
        );
    }

    #[test]
    fn descriptor_avro_count_is_checked_before_bulk_decode() {
        let encoded = canonical_payload(&[data_file("a"), data_file("b")]);
        validate_data_file_record_count(&encoded, 2).unwrap();
        for expected in [0, 1, 3] {
            let error = validate_data_file_record_count(&encoded, expected)
                .expect_err("mismatched Avro record count must fail")
                .to_string();
            assert!(error.contains("LDB-ICEBERG-DESCRIPTOR-FILE-COUNT"));
        }
    }

    #[test]
    fn compressed_descriptor_avro_is_rejected_before_decode() {
        let generated = canonical_payload(&[data_file("a")]);
        let mut reader = apache_avro::Reader::new(generated.as_slice()).unwrap();
        let schema = reader.writer_schema().clone();
        let values = reader.by_ref().collect::<Result<Vec<_>, _>>().unwrap();
        let mut compressed = Vec::new();
        {
            let mut writer = apache_avro::Writer::with_codec(
                &schema,
                &mut compressed,
                apache_avro::Codec::Deflate(apache_avro::DeflateSettings::default()),
            );
            for value in values {
                writer.append(value).unwrap();
            }
            writer.flush().unwrap();
        }

        let error = validate_data_file_record_count(&compressed, 1)
            .expect_err("compressed descriptor Avro must fail closed")
            .to_string();
        assert!(error.contains("LDB-ICEBERG-DESCRIPTOR-AVRO-CODEC"));
    }

    #[tokio::test]
    async fn descriptor_rejects_a_foreign_data_location_without_echoing_it() {
        let fixture = crate::lakehouse::iceberg::test_support::create_test_table(false).await;
        let identity = EpochIdentity {
            deployment_id: "018f0000-0000-7000-8000-000000000001".into(),
            sink_id: "events".into(),
            participant_id: 1,
            epoch: 7,
        };
        let prefix = super::super::file_finalizer::replay_safe_prefix(
            &identity.deployment_id,
            &identity.sink_id,
            identity.participant_id,
            identity.epoch,
        );
        let path = format!(
            "memory:///foreign/data/{prefix}-00000000-{}.parquet",
            "a".repeat(64)
        );
        let error = validate_descriptor_data_file(&fixture.table, &identity, &data_file(&path))
            .unwrap_err()
            .to_string();
        assert!(error.contains("LDB-ICEBERG-DATA-FILE-LOCATION"));
        assert!(!error.contains("memory:///foreign"));
    }

    #[test]
    fn future_descriptor_version_is_rejected() {
        let mut descriptor = descriptor();
        descriptor.version = DESCRIPTOR_VERSION + 1;
        let payload = serde_json::to_vec(&descriptor).unwrap();
        assert!(IcebergCommitDescriptorV1::decode(&payload).is_err());
    }

    #[test]
    fn descriptor_binding_locations_are_redacted_and_fail_closed() {
        let mut descriptor = descriptor();
        descriptor.table.metadata_location =
            "https://user:descriptor-secret@objects.test/metadata/v1.json".into();
        let debug = format!("{descriptor:?}");
        assert!(!debug.contains("descriptor-secret"));
        assert!(!debug.contains("objects.test"));

        let mut untrusted_debug = descriptor.clone();
        untrusted_debug.table.catalog_implementation = "catalog-secret".into();
        untrusted_debug.table.catalog_identity = "identity-secret".into();
        untrusted_debug.table.table_uuid = "uuid-secret".into();
        untrusted_debug.deployment_id = "deployment-secret".into();
        untrusted_debug.sink_id = "sink-secret".into();
        untrusted_debug.batch_fingerprint = "fingerprint-secret".into();
        untrusted_debug.files.push(IcebergFileFingerprintV1 {
            path: "https://user:file-secret@objects.test/data.parquet".into(),
            metadata_sha256: "metadata-secret".into(),
            records: 1,
            bytes: 1,
        });
        let debug = format!("{untrusted_debug:?}");
        for secret in [
            "catalog-secret",
            "identity-secret",
            "uuid-secret",
            "deployment-secret",
            "sink-secret",
            "fingerprint-secret",
            "file-secret",
            "metadata-secret",
        ] {
            assert!(!debug.contains(secret));
        }

        let payload = serde_json::to_vec(&descriptor).unwrap();
        let error = IcebergCommitDescriptorV1::decode(&payload)
            .unwrap_err()
            .to_string();
        assert!(error.contains("LDB-ICEBERG-DESCRIPTOR-LOCATION"));
        assert!(!error.contains("descriptor-secret"));

        descriptor.table.metadata_location.clear();
        let payload = serde_json::to_vec(&descriptor).unwrap();
        assert!(IcebergCommitDescriptorV1::decode(&payload)
            .unwrap_err()
            .to_string()
            .contains("LDB-ICEBERG-DESCRIPTOR-LOCATION"));
    }

    #[test]
    fn descriptor_intrinsic_limits_and_aggregates_fail_closed() {
        let oversized = vec![b' '; MAX_COORDINATED_COMMIT_PAYLOAD_BYTES + 1];
        assert!(IcebergCommitDescriptorV1::decode(&oversized)
            .unwrap_err()
            .to_string()
            .contains("fixed"));

        let mut too_many_files = descriptor();
        too_many_files.files = vec![
            IcebergFileFingerprintV1 {
                path: "s3://warehouse/ns/table/data/file.parquet".into(),
                metadata_sha256: "0".repeat(64),
                records: 0,
                bytes: 0,
            };
            ICEBERG_MAX_FILES_PER_CHECKPOINT + 1
        ];
        too_many_files.file_count = u64::try_from(too_many_files.files.len()).unwrap();
        let payload = serde_json::to_vec(&too_many_files).unwrap();
        assert!(IcebergCommitDescriptorV1::decode(&payload)
            .unwrap_err()
            .to_string()
            .contains("file limit"));

        let mut aggregate_mismatch = descriptor();
        aggregate_mismatch.record_count = 1;
        let payload = serde_json::to_vec(&aggregate_mismatch).unwrap();
        assert!(IcebergCommitDescriptorV1::decode(&payload)
            .unwrap_err()
            .to_string()
            .contains("aggregate counts"));

        let mut overflowing = descriptor();
        let prefix = super::super::file_finalizer::replay_safe_prefix(
            &overflowing.deployment_id,
            &overflowing.sink_id,
            overflowing.participant_id,
            overflowing.epoch_id,
        );
        overflowing.files = [u64::MAX, 1]
            .into_iter()
            .enumerate()
            .map(|(ordinal, records)| IcebergFileFingerprintV1 {
                path: format!(
                    "s3://warehouse/ns/table/data/{prefix}-{ordinal:08}-{}.parquet",
                    "0".repeat(64)
                ),
                metadata_sha256: "0".repeat(64),
                records,
                bytes: 0,
            })
            .collect();
        overflowing.file_count = 2;
        let payload = serde_json::to_vec(&overflowing).unwrap();
        let error = IcebergCommitDescriptorV1::decode(&payload).unwrap_err();
        assert!(matches!(error, ConnectorError::TransactionError(_)));
        assert!(error.to_string().contains("overflow"));
    }

    #[test]
    fn malformed_descriptor_errors_do_not_echo_untrusted_values() {
        let payload = br#"{"version":"https://user:descriptor-secret@objects.test"}"#;
        let error = IcebergCommitDescriptorV1::decode(payload)
            .unwrap_err()
            .to_string();
        assert!(error.contains("LDB-ICEBERG-DESCRIPTOR-JSON"));
        assert!(!error.contains("descriptor-secret"));

        let mut payload = serde_json::to_value(descriptor()).unwrap();
        payload["table"]["unknown-secret-field"] =
            serde_json::Value::String("unknown-secret-value".into());
        let error = IcebergCommitDescriptorV1::decode(&serde_json::to_vec(&payload).unwrap())
            .unwrap_err()
            .to_string();
        assert!(error.contains("LDB-ICEBERG-DESCRIPTOR-JSON"));
        assert!(!error.contains("unknown-secret"));
    }

    #[tokio::test]
    async fn configured_descriptor_limit_is_enforced() {
        let fixture = crate::lakehouse::iceberg::test_support::create_test_table(false).await;
        let mut config = fixture.config.clone();
        config.delivery_guarantee = crate::connector::DeliveryGuarantee::ExactlyOnce;
        let identity = EpochIdentity {
            deployment_id: "018f0000-0000-7000-8000-000000000001".into(),
            sink_id: "events".into(),
            participant_id: 1,
            epoch: 7,
        };
        let mut writer = super::super::epoch_writer::IcebergEpochWriter::new(
            &fixture.table,
            &config,
            &identity,
            super::super::metrics::IcebergMetrics::new(None),
        )
        .unwrap();
        writer
            .write(crate::lakehouse::iceberg::test_support::batch(
                &fixture.table,
                &[(1, Some("a")), (2, Some("b"))],
            ))
            .await
            .unwrap();
        let output = writer.close().await.unwrap();
        config.max_descriptor_bytes = 1;
        let error = IcebergCommitDescriptorV1::encode(&fixture.table, &config, &identity, output)
            .expect_err("descriptor must respect configured byte limit");
        assert!(error.to_string().contains("configured limit is 1"));
    }
}

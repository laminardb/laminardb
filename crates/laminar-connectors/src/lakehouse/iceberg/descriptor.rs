//! Versioned participant descriptors for coordinated Iceberg publication.

use std::collections::HashSet;

use apache_avro::types::Value as AvroValue;
use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use iceberg::spec::{DataFile, FormatVersion};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::connector::MAX_COORDINATED_COMMIT_PAYLOAD_BYTES;
use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::{stable_catalog_identity, IcebergSinkConfig};

use super::epoch_writer::{EpochIdentity, EpochOutput};

const DESCRIPTOR_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    pub(super) fn from_table(table: &iceberg::table::Table, config: &IcebergSinkConfig) -> Self {
        let metadata = table.metadata();
        Self {
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
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct IcebergFileFingerprintV1 {
    pub(super) path: String,
    pub(super) metadata_sha256: String,
    pub(super) records: u64,
    pub(super) bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

impl IcebergCommitDescriptorV1 {
    pub(super) fn encode(
        table: &iceberg::table::Table,
        config: &IcebergSinkConfig,
        identity: &EpochIdentity,
        mut output: EpochOutput,
    ) -> Result<Vec<u8>, ConnectorError> {
        output
            .data_files
            .sort_by(|left, right| left.file_path().cmp(right.file_path()));
        let files = output
            .data_files
            .iter()
            .map(file_fingerprint)
            .collect::<Vec<_>>();
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
        let binding = IcebergTableBindingV1::from_table(table, config);
        let data_files_avro = encode_data_files(table, &output.data_files, &files)?;
        let batch_fingerprint = participant_fingerprint(
            &binding,
            identity,
            &files,
            record_count,
            file_bytes,
            output.bytes,
        );
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
        let descriptor: Self = serde_json::from_slice(payload).map_err(|error| {
            ConnectorError::TransactionError(format!("decode Iceberg commit descriptor: {error}"))
        })?;
        if descriptor.version != DESCRIPTOR_VERSION {
            return Err(ConnectorError::TransactionError(format!(
                "unsupported Iceberg commit descriptor version {}",
                descriptor.version
            )));
        }
        if descriptor.participant_id == 0 || descriptor.deployment_id.is_empty() {
            return Err(ConnectorError::TransactionError(
                "Iceberg commit descriptor has an invalid runtime identity".into(),
            ));
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
            validate_sha256(&file.metadata_sha256, "data-file metadata fingerprint")?;
        }
        let mut paths = HashSet::with_capacity(descriptor.files.len());
        let mut previous = None;
        for file in &descriptor.files {
            if !paths.insert(file.path.as_str()) {
                return Err(ConnectorError::TransactionError(format!(
                    "Iceberg commit descriptor repeats data file '{}'",
                    file.path
                )));
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
                "resolve Iceberg descriptor partition type: {error}"
            ))
        })?;
        let encoded = BASE64.decode(&self.data_files_avro).map_err(|error| {
            ConnectorError::TransactionError(format!(
                "decode Iceberg descriptor data files: {error}"
            ))
        })?;
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
                "parse Iceberg descriptor data files: {error}"
            ))
        })?;
        data_files.sort_by(|left, right| left.file_path().cmp(right.file_path()));
        let observed = data_files.iter().map(file_fingerprint).collect::<Vec<_>>();
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
        let identity = EpochIdentity {
            deployment_id: self.deployment_id.clone(),
            sink_id: self.sink_id.clone(),
            participant_id: self.participant_id,
            epoch: self.epoch_id,
        };
        let fingerprint = participant_fingerprint(
            &self.table,
            &identity,
            &self.files,
            self.record_count,
            self.file_bytes,
            self.input_bytes,
        );
        if fingerprint != self.batch_fingerprint {
            return Err(ConnectorError::TransactionError(
                "Iceberg descriptor participant fingerprint does not match its contents".into(),
            ));
        }
        Ok(data_files)
    }
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

    let marker = descriptor_sync_marker(fingerprints);
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

fn descriptor_sync_marker(fingerprints: &[IcebergFileFingerprintV1]) -> [u8; 16] {
    let mut hash = Sha256::new();
    hash.update(b"laminardb-iceberg-descriptor-avro-v1\0");
    for file in fingerprints {
        hash_len_prefixed(&mut hash, file.path.as_bytes());
        hash_len_prefixed(&mut hash, file.metadata_sha256.as_bytes());
    }
    hash.finalize()[..16]
        .try_into()
        .expect("SHA-256 is 32 bytes")
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
) -> String {
    let mut hash = Sha256::new();
    hash.update(b"laminardb-iceberg-participant-v1\0");
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
        hash_len_prefixed(&mut hash, value.as_bytes());
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
    for file in files {
        hash_len_prefixed(&mut hash, file.path.as_bytes());
        hash_len_prefixed(&mut hash, file.metadata_sha256.as_bytes());
        hash.update(file.records.to_be_bytes());
        hash.update(file.bytes.to_be_bytes());
    }
    format!("{:x}", hash.finalize())
}

fn file_fingerprint(file: &DataFile) -> IcebergFileFingerprintV1 {
    let mut hash = Sha256::new();
    hash.update(b"laminardb-iceberg-data-file-v1\0");
    hash_len_prefixed(&mut hash, file.file_path().as_bytes());
    hash_len_prefixed(&mut hash, file.file_format().to_string().as_bytes());
    hash.update((file.content_type() as i32).to_be_bytes());
    hash.update(file.record_count().to_be_bytes());
    hash.update(file.file_size_in_bytes().to_be_bytes());
    hash_len_prefixed(&mut hash, format!("{:?}", file.partition()).as_bytes());
    hash_i32_u64_map(&mut hash, file.column_sizes());
    hash_i32_u64_map(&mut hash, file.value_counts());
    hash_i32_u64_map(&mut hash, file.null_value_counts());
    hash_i32_u64_map(&mut hash, file.nan_value_counts());
    hash_i32_debug_map(&mut hash, file.lower_bounds());
    hash_i32_debug_map(&mut hash, file.upper_bounds());
    hash_len_prefixed(&mut hash, file.key_metadata().unwrap_or_default());
    for offset in file.split_offsets().unwrap_or_default() {
        hash.update(offset.to_be_bytes());
    }
    for field_id in file.equality_ids().unwrap_or_default() {
        hash.update(field_id.to_be_bytes());
    }
    hash.update(file.first_row_id().unwrap_or(-1).to_be_bytes());
    hash.update(file.sort_order_id().unwrap_or(-1).to_be_bytes());
    IcebergFileFingerprintV1 {
        path: file.file_path().to_string(),
        metadata_sha256: format!("{:x}", hash.finalize()),
        records: file.record_count(),
        bytes: file.file_size_in_bytes(),
    }
}

fn hash_i32_u64_map(hash: &mut Sha256, values: &std::collections::HashMap<i32, u64>) {
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_unstable_by_key(|(key, _)| **key);
    for (key, value) in entries {
        hash.update(key.to_be_bytes());
        hash.update(value.to_be_bytes());
    }
}

fn hash_i32_debug_map<T: std::fmt::Debug>(
    hash: &mut Sha256,
    values: &std::collections::HashMap<i32, T>,
) {
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_unstable_by_key(|(key, _)| **key);
    for (key, value) in entries {
        hash.update(key.to_be_bytes());
        hash_len_prefixed(hash, format!("{value:?}").as_bytes());
    }
}

fn hash_len_prefixed(hash: &mut Sha256, bytes: &[u8]) {
    hash.update(bytes.len().to_be_bytes());
    hash.update(bytes);
}

fn checked_sum(mut values: impl Iterator<Item = u64>, label: &str) -> Result<u64, ConnectorError> {
    values.try_fold(0u64, |total, value| {
        total
            .checked_add(value)
            .ok_or_else(|| ConnectorError::Internal(format!("Iceberg descriptor {label} overflow")))
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
        let fingerprints = files.iter().map(file_fingerprint).collect::<Vec<_>>();
        canonicalize_data_files_avro(&generated, &fingerprints).unwrap()
    }

    #[test]
    fn file_fingerprint_is_deterministic_and_content_sensitive() {
        assert_eq!(
            file_fingerprint(&data_file("a")),
            file_fingerprint(&data_file("a"))
        );
        assert_ne!(
            file_fingerprint(&data_file("a")),
            file_fingerprint(&data_file("b"))
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
    fn future_descriptor_version_is_rejected() {
        let descriptor = IcebergCommitDescriptorV1 {
            version: 2,
            table: IcebergTableBindingV1 {
                catalog_implementation: "rest".into(),
                catalog_identity: "catalog".into(),
                table_uuid: "uuid".into(),
                table_identifier: "ns.table".into(),
                table_location: "s3://warehouse/ns/table".into(),
                table_ref: "main".into(),
                base_snapshot_id: None,
                metadata_location: String::new(),
                schema_id: 0,
                partition_spec_id: 0,
                sort_order_id: 0,
                format_version: 2,
            },
            deployment_id: "deployment".into(),
            sink_id: "sink".into(),
            epoch_id: 1,
            participant_id: 1,
            batch_fingerprint: "fingerprint".into(),
            data_files_avro: String::new(),
            files: Vec::new(),
            file_count: 0,
            record_count: 0,
            file_bytes: 0,
            input_bytes: 0,
        };
        let payload = serde_json::to_vec(&descriptor).unwrap();
        assert!(IcebergCommitDescriptorV1::decode(&payload).is_err());
    }

    #[tokio::test]
    async fn configured_descriptor_limit_is_enforced() {
        let fixture = crate::lakehouse::iceberg::test_support::create_test_table(false).await;
        let mut config = fixture.config;
        config.max_descriptor_bytes = 1;
        let output = EpochOutput {
            data_files: vec![data_file("file:///warehouse/data/a.parquet")],
            rows: 2,
            bytes: 100,
        };
        let identity = EpochIdentity {
            deployment_id: "018f0000-0000-7000-8000-000000000001".into(),
            sink_id: "events".into(),
            participant_id: 1,
            epoch: 7,
        };
        let error = IcebergCommitDescriptorV1::encode(&fixture.table, &config, &identity, output)
            .expect_err("descriptor must respect configured byte limit");
        assert!(error.to_string().contains("configured limit is 1"));
    }
}

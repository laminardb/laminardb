//! Coordinated table binding and bounded commit-descriptor validation.

use super::{
    BTreeMap, ConnectorError, DeltaCommitDescriptor, DeltaTable, DeltaTableBinding, HashSet, Url,
    MAX_COORDINATED_COMMIT_BATCH_BYTES,
};
#[cfg(test)]
use std::time::Duration;

#[cfg(feature = "delta-lake")]
pub(in crate::lakehouse) const MAX_COORDINATED_ADD_ACTIONS: usize = 4_096;
#[cfg(feature = "delta-lake")]
pub(super) const MAX_COORDINATED_PATH_BYTES: usize = 1_024;
#[cfg(feature = "delta-lake")]
pub(super) const MAX_COORDINATED_STATS_BYTES: usize = 1024 * 1024;
#[cfg(feature = "delta-lake")]
const MAX_COORDINATED_PARTITION_ENTRIES: usize = 1_024;
#[cfg(feature = "delta-lake")]
pub(super) const MAX_COORDINATED_PARTITION_BYTES: usize = 256 * 1024;
#[cfg(feature = "delta-lake")]
const MAX_COORDINATED_TAG_ENTRIES: usize = 1_024;
#[cfg(feature = "delta-lake")]
const MAX_COORDINATED_TAG_BYTES: usize = 256 * 1024;
#[cfg(feature = "delta-lake")]
const MAX_COORDINATED_TABLE_ID_BYTES: usize = 1_024;

#[cfg(feature = "delta-lake")]
#[derive(serde::Serialize)]
struct DeltaProtocolFingerprint {
    min_reader_version: i32,
    min_writer_version: i32,
    reader_features: Vec<String>,
    writer_features: Vec<String>,
}

#[cfg(feature = "delta-lake")]
#[derive(serde::Serialize)]
struct DeltaWriteMetadataFingerprint<'a> {
    table_id: &'a str,
    schema: &'a deltalake::kernel::StructType,
    partition_columns: &'a [String],
    configuration: BTreeMap<&'a str, &'a str>,
    protocol: DeltaProtocolFingerprint,
}

#[cfg(feature = "delta-lake")]
fn sorted_protocol_features<T: ToString>(features: Option<&[T]>) -> Vec<String> {
    let mut features: Vec<String> = features
        .unwrap_or_default()
        .iter()
        .map(ToString::to_string)
        .collect();
    features.sort_unstable();
    features
}

#[cfg(feature = "delta-lake")]
pub(in crate::lakehouse) fn coordinated_table_binding(
    table: &DeltaTable,
) -> Result<DeltaTableBinding, ConnectorError> {
    let snapshot = table.snapshot().map_err(|error| {
        ConnectorError::TransactionError(format!("read Delta staging snapshot: {error}"))
    })?;
    let metadata = snapshot.metadata();
    let table_id = metadata.id();
    if table_id.is_empty() || table_id.len() > MAX_COORDINATED_TABLE_ID_BYTES {
        return Err(ConnectorError::TransactionError(
            "Delta table id is empty or exceeds the coordinated descriptor limit".into(),
        ));
    }
    let schema = metadata.parse_schema().map_err(|error| {
        ConnectorError::TransactionError(format!("parse Delta table schema: {error}"))
    })?;
    let configuration = metadata
        .configuration()
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    let protocol = snapshot.protocol();
    let fingerprint = DeltaWriteMetadataFingerprint {
        table_id,
        schema: &schema,
        partition_columns: metadata.partition_columns(),
        configuration,
        protocol: DeltaProtocolFingerprint {
            min_reader_version: protocol.min_reader_version(),
            min_writer_version: protocol.min_writer_version(),
            reader_features: sorted_protocol_features(protocol.reader_features()),
            writer_features: sorted_protocol_features(protocol.writer_features()),
        },
    };
    let write_metadata_sha256 = laminar_core::checkpoint::canonical_json_sha256(&fingerprint)
        .map_err(|error| {
            ConnectorError::TransactionError(format!("canonicalize Delta write metadata: {error}"))
        })?;
    Ok(DeltaTableBinding {
        table_id: table_id.to_owned(),
        write_metadata_sha256,
    })
}

#[cfg(feature = "delta-lake")]
pub(super) fn validate_table_binding(binding: &DeltaTableBinding) -> Result<(), ConnectorError> {
    if binding.table_id.is_empty() || binding.table_id.len() > MAX_COORDINATED_TABLE_ID_BYTES {
        return Err(ConnectorError::TransactionError(
            "Delta coordinated descriptor has an invalid table id".into(),
        ));
    }
    if binding.write_metadata_sha256.len() != 64
        || !binding
            .write_metadata_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ConnectorError::TransactionError(
            "Delta coordinated descriptor has a non-canonical metadata digest".into(),
        ));
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
pub(super) fn ensure_publication_deadline(
    deadline: tokio::time::Instant,
    operation: &str,
) -> Result<(), ConnectorError> {
    if deadline <= tokio::time::Instant::now() {
        Err(ConnectorError::TransactionError(format!(
            "Delta coordinated publication deadline elapsed during {operation}; the external outcome must be reconciled from its cursor"
        )))
    } else {
        Ok(())
    }
}

/// Serialize table-bound `Add` actions into one durable descriptor.
#[cfg(feature = "delta-lake")]
pub(in crate::lakehouse) fn encode_commit_descriptor(
    binding: &DeltaTableBinding,
    adds: &[deltalake::kernel::Add],
) -> Result<Vec<u8>, ConnectorError> {
    super::super::commit_descriptor::encode(binding, adds)
}

#[cfg(feature = "delta-lake")]
pub(in crate::lakehouse) fn encoded_add_array_len(
    adds: &[deltalake::kernel::Add],
) -> Result<usize, ConnectorError> {
    super::super::commit_descriptor::encoded_add_array_len(adds)
}

#[cfg(feature = "delta-lake")]
pub(super) fn validate_descriptor_batch_lengths(
    lengths: impl IntoIterator<Item = usize>,
    deadline: tokio::time::Instant,
) -> Result<(), ConnectorError> {
    let mut total_bytes = 0usize;
    for (index, length) in lengths.into_iter().enumerate() {
        if index % 64 == 0 {
            ensure_publication_deadline(deadline, "descriptor batch admission")?;
        }
        if length > crate::connector::MAX_COORDINATED_COMMIT_PAYLOAD_BYTES {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated descriptor exceeds the fixed {} byte per-participant limit",
                crate::connector::MAX_COORDINATED_COMMIT_PAYLOAD_BYTES
            )));
        }
        total_bytes = total_bytes.checked_add(length).ok_or_else(|| {
            ConnectorError::TransactionError(
                "Delta coordinated descriptor byte count overflow".into(),
            )
        })?;
        if total_bytes > MAX_COORDINATED_COMMIT_BATCH_BYTES {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated descriptors exceed the fixed {MAX_COORDINATED_COMMIT_BATCH_BYTES} byte batch limit"
            )));
        }
    }
    ensure_publication_deadline(deadline, "descriptor batch admission")
}

#[cfg(feature = "delta-lake")]
pub(super) fn decode_commit_descriptors_until(
    descriptors: &[Vec<u8>],
    deadline: tokio::time::Instant,
) -> Result<Option<DeltaCommitDescriptor>, ConnectorError> {
    validate_descriptor_batch_lengths(descriptors.iter().map(Vec::len), deadline)?;

    let mut binding = None;
    let mut adds = Vec::new();
    for bytes in descriptors {
        ensure_publication_deadline(deadline, "descriptor decoding")?;
        let descriptor = super::super::commit_descriptor::decode(bytes)?;
        ensure_publication_deadline(deadline, "descriptor decoding")?;
        validate_table_binding(&descriptor.binding)?;
        if descriptor.adds.is_empty() {
            return Err(ConnectorError::TransactionError(
                "Delta coordinated payload contains an empty descriptor".into(),
            ));
        }
        match &binding {
            Some(expected) if expected != &descriptor.binding => {
                return Err(ConnectorError::TransactionError(
                    "Delta coordinated descriptors bind different table metadata".into(),
                ));
            }
            None => binding = Some(descriptor.binding),
            Some(_) => {}
        }
        let projected = adds
            .len()
            .checked_add(descriptor.adds.len())
            .ok_or_else(|| {
                ConnectorError::TransactionError(
                    "Delta coordinated Add action count overflow".into(),
                )
            })?;
        if projected > MAX_COORDINATED_ADD_ACTIONS {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated publication exceeds the fixed {MAX_COORDINATED_ADD_ACTIONS} Add action limit"
            )));
        }
        adds.extend(descriptor.adds);
    }
    Ok(binding.map(|binding| DeltaCommitDescriptor { binding, adds }))
}

#[cfg(all(feature = "delta-lake", test))]
pub(in crate::lakehouse) fn decode_commit_descriptors(
    descriptors: &[Vec<u8>],
) -> Result<Option<DeltaCommitDescriptor>, ConnectorError> {
    decode_commit_descriptors_until(
        descriptors,
        tokio::time::Instant::now() + Duration::from_secs(30),
    )
}

#[cfg(feature = "delta-lake")]
#[derive(Clone)]
pub(super) struct CoordinatedObject {
    pub(super) path: deltalake::Path,
    pub(super) expected_size: u64,
}

#[cfg(feature = "delta-lake")]
fn decode_percent_once(value: &str) -> Result<Option<String>, ConnectorError> {
    fn hex(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }

    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    let mut changed = false;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let (Some(high), Some(low)) = (hex(bytes[index + 1]), hex(bytes[index + 2])) {
                decoded.push((high << 4) | low);
                index += 3;
                changed = true;
                continue;
            }
        }
        decoded.push(bytes[index]);
        index += 1;
    }
    if !changed {
        return Ok(None);
    }
    String::from_utf8(decoded).map(Some).map_err(|_| {
        ConnectorError::TransactionError(
            "Delta coordinated Add path contains non-UTF-8 percent encoding".into(),
        )
    })
}

#[cfg(feature = "delta-lake")]
fn validate_path_segment(segment: &str, first: bool) -> Result<String, ConnectorError> {
    let mut current = segment.to_owned();
    for _ in 0..=4 {
        let trimmed = current.trim_end_matches(['.', ' ']);
        if trimmed.len() != current.len()
            || trimmed.is_empty()
            || trimmed == "."
            || trimmed == ".."
            || current.contains('/')
            || current.contains('\\')
            || (first && trimmed.eq_ignore_ascii_case("_delta_log"))
            || (first
                && trimmed.as_bytes().get(1) == Some(&b':')
                && trimmed.as_bytes()[0].is_ascii_alphabetic())
        {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated Add path has an unsafe segment: '{segment}'"
            )));
        }
        let Some(decoded) = decode_percent_once(&current)? else {
            return Ok(current.to_ascii_lowercase());
        };
        if decoded == current {
            return Ok(current.to_ascii_lowercase());
        }
        current = decoded;
    }
    Err(ConnectorError::TransactionError(format!(
        "Delta coordinated Add path has excessive percent-encoding depth: '{segment}'"
    )))
}

#[cfg(feature = "delta-lake")]
fn bounded_map_bytes<'a>(
    entries: impl Iterator<Item = (&'a String, Option<&'a String>)>,
    limit: usize,
    context: &str,
) -> Result<(), ConnectorError> {
    let mut total = 0usize;
    for (key, value) in entries {
        total = total
            .checked_add(key.len())
            .and_then(|bytes| bytes.checked_add(value.map_or(0, String::len)))
            .ok_or_else(|| {
                ConnectorError::TransactionError(format!(
                    "Delta coordinated Add {context} byte count overflow"
                ))
            })?;
        if total > limit {
            return Err(ConnectorError::TransactionError(format!(
                "Delta coordinated Add {context} exceeds the fixed {limit} byte limit"
            )));
        }
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
fn validate_coordinated_path(
    raw_path: &str,
    normalized_paths: &mut HashSet<String>,
) -> Result<deltalake::Path, ConnectorError> {
    if raw_path.is_empty()
        || raw_path.len() > MAX_COORDINATED_PATH_BYTES
        || raw_path.starts_with('/')
        || raw_path.starts_with('\\')
        || raw_path.ends_with('/')
        || raw_path.contains('\\')
        || Url::parse(raw_path).is_ok()
    {
        return Err(ConnectorError::TransactionError(format!(
            "Delta coordinated Add path must be a non-empty relative object path: '{raw_path}'"
        )));
    }
    let path = deltalake::Path::parse(raw_path).map_err(|error| {
        ConnectorError::TransactionError(format!(
            "invalid Delta coordinated Add path '{raw_path}': {error}"
        ))
    })?;
    let mut normalized_path = String::with_capacity(raw_path.len());
    for (segment_index, segment) in raw_path.split('/').enumerate() {
        if segment_index != 0 {
            normalized_path.push('/');
        }
        normalized_path.push_str(&validate_path_segment(segment, segment_index == 0)?);
    }
    if !path
        .extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case("parquet"))
    {
        return Err(ConnectorError::TransactionError(format!(
            "Delta coordinated Add path is not a Parquet data file: '{raw_path}'"
        )));
    }
    if !normalized_paths.insert(normalized_path) {
        return Err(ConnectorError::TransactionError(format!(
            "duplicate Windows-equivalent Delta coordinated Add path '{raw_path}'"
        )));
    }
    Ok(path)
}

#[cfg(feature = "delta-lake")]
fn validate_coordinated_add_properties(
    add: &deltalake::kernel::Add,
    raw_path: &str,
) -> Result<u64, ConnectorError> {
    let expected_size = u64::try_from(add.size).map_err(|_| {
        ConnectorError::TransactionError(format!(
            "Delta coordinated Add '{raw_path}' has negative size {}",
            add.size
        ))
    })?;
    if expected_size == 0 {
        return Err(ConnectorError::TransactionError(format!(
            "Delta coordinated Add '{raw_path}' has zero size"
        )));
    }
    if add.modification_time < 0 {
        return Err(ConnectorError::TransactionError(format!(
            "Delta coordinated Add '{raw_path}' has negative modification time {}",
            add.modification_time
        )));
    }
    if !add.data_change {
        return Err(ConnectorError::TransactionError(format!(
            "Delta coordinated Add '{raw_path}' must be a data change"
        )));
    }
    if add.deletion_vector.is_some() {
        return Err(ConnectorError::TransactionError(format!(
            "Delta coordinated append Add '{raw_path}' cannot reference a deletion vector (the fixed limit is zero)"
        )));
    }
    Ok(expected_size)
}

#[cfg(feature = "delta-lake")]
fn validate_coordinated_partitions(
    add: &deltalake::kernel::Add,
    raw_path: &str,
    expected_partitions: &HashSet<&str>,
) -> Result<(), ConnectorError> {
    if add.partition_values.len() > MAX_COORDINATED_PARTITION_ENTRIES {
        return Err(ConnectorError::TransactionError(format!(
            "Delta coordinated Add '{raw_path}' exceeds the fixed {MAX_COORDINATED_PARTITION_ENTRIES} partition entry limit"
        )));
    }
    if add.partition_values.len() != expected_partitions.len()
        || !add
            .partition_values
            .keys()
            .all(|column| expected_partitions.contains(column.as_str()))
    {
        return Err(ConnectorError::TransactionError(format!(
            "Delta coordinated Add '{raw_path}' partition values do not match the live table"
        )));
    }
    bounded_map_bytes(
        add.partition_values
            .iter()
            .map(|(key, value)| (key, value.as_ref())),
        MAX_COORDINATED_PARTITION_BYTES,
        "partition metadata",
    )
}

#[cfg(feature = "delta-lake")]
fn validate_coordinated_statistics(
    stats: Option<&String>,
    raw_path: &str,
) -> Result<(), ConnectorError> {
    let Some(stats) = stats else {
        return Ok(());
    };
    if stats.len() > MAX_COORDINATED_STATS_BYTES {
        return Err(ConnectorError::TransactionError(format!(
            "Delta coordinated Add '{raw_path}' statistics exceed the fixed {MAX_COORDINATED_STATS_BYTES} byte limit"
        )));
    }
    let value: serde_json::Value = serde_json::from_str(stats).map_err(|error| {
        ConnectorError::TransactionError(format!(
            "Delta coordinated Add '{raw_path}' has invalid statistics: {error}"
        ))
    })?;
    if !value.is_object() {
        return Err(ConnectorError::TransactionError(format!(
            "Delta coordinated Add '{raw_path}' statistics must be a JSON object"
        )));
    }
    Ok(())
}

#[cfg(feature = "delta-lake")]
fn validate_coordinated_tags(
    add: &deltalake::kernel::Add,
    raw_path: &str,
) -> Result<(), ConnectorError> {
    let Some(tags) = &add.tags else {
        return Ok(());
    };
    if tags.len() > MAX_COORDINATED_TAG_ENTRIES {
        return Err(ConnectorError::TransactionError(format!(
            "Delta coordinated Add '{raw_path}' exceeds the fixed {MAX_COORDINATED_TAG_ENTRIES} tag entry limit"
        )));
    }
    bounded_map_bytes(
        tags.iter().map(|(key, value)| (key, value.as_ref())),
        MAX_COORDINATED_TAG_BYTES,
        "tag metadata",
    )
}

#[cfg(feature = "delta-lake")]
fn validate_coordinated_add(
    add: &deltalake::kernel::Add,
    expected_partitions: &HashSet<&str>,
    normalized_paths: &mut HashSet<String>,
) -> Result<CoordinatedObject, ConnectorError> {
    let raw_path = add.path.as_str();
    let path = validate_coordinated_path(raw_path, normalized_paths)?;
    let expected_size = validate_coordinated_add_properties(add, raw_path)?;
    validate_coordinated_partitions(add, raw_path, expected_partitions)?;
    validate_coordinated_statistics(add.stats.as_ref(), raw_path)?;
    validate_coordinated_tags(add, raw_path)?;
    if add
        .clustering_provider
        .as_ref()
        .is_some_and(|provider| provider.len() > 256)
    {
        return Err(ConnectorError::TransactionError(format!(
            "Delta coordinated Add '{raw_path}' clustering provider exceeds 256 bytes"
        )));
    }
    Ok(CoordinatedObject {
        path,
        expected_size,
    })
}

#[cfg(feature = "delta-lake")]
pub(super) fn validate_coordinated_descriptors(
    adds: &[deltalake::kernel::Add],
    partition_columns: &[String],
    deadline: tokio::time::Instant,
) -> Result<Vec<CoordinatedObject>, ConnectorError> {
    if adds.len() > MAX_COORDINATED_ADD_ACTIONS {
        return Err(ConnectorError::TransactionError(format!(
            "Delta coordinated publication exceeds the fixed {MAX_COORDINATED_ADD_ACTIONS} Add action limit"
        )));
    }
    let expected_partitions: HashSet<&str> = partition_columns.iter().map(String::as_str).collect();
    let mut normalized_paths = HashSet::with_capacity(adds.len());
    let mut objects = Vec::with_capacity(adds.len());

    for (index, add) in adds.iter().enumerate() {
        if index % 64 == 0 {
            ensure_publication_deadline(deadline, "descriptor validation")?;
        }
        objects.push(validate_coordinated_add(
            add,
            &expected_partitions,
            &mut normalized_paths,
        )?);
    }

    ensure_publication_deadline(deadline, "descriptor validation")?;
    Ok(objects)
}

//! Canonical durable fingerprints for Iceberg file metadata.

use iceberg::spec::{
    DataContentType, DataFile, DataFileFormat, Datum, Literal, PrimitiveLiteral, PrimitiveType,
    Struct,
};
use sha2::{Digest, Sha256};

use crate::error::ConnectorError;

use super::descriptor::IcebergFileFingerprintV1;

pub(super) fn data_file_fingerprint(
    file: &DataFile,
) -> Result<IcebergFileFingerprintV1, ConnectorError> {
    // RECOVERY: This layout is durable; change its domain and locked test together.
    let mut hash = Sha256::new();
    hash.update(b"laminardb-iceberg-data-file-v2\0");
    hash_len_prefixed(&mut hash, file.file_path().as_bytes())?;
    hash.update([file_format_tag(file.file_format())]);
    hash.update([content_type_tag(file.content_type())]);
    hash.update(file.record_count().to_be_bytes());
    hash.update(file.file_size_in_bytes().to_be_bytes());
    hash_partition(&mut hash, file.partition())?;
    hash_i32_u64_map(&mut hash, file.column_sizes())?;
    hash_i32_u64_map(&mut hash, file.value_counts())?;
    hash_i32_u64_map(&mut hash, file.null_value_counts())?;
    hash_i32_u64_map(&mut hash, file.nan_value_counts())?;
    hash_i32_datum_map(&mut hash, file.lower_bounds())?;
    hash_i32_datum_map(&mut hash, file.upper_bounds())?;
    hash_optional_bytes(&mut hash, file.key_metadata())?;
    hash_optional_i64_slice(&mut hash, file.split_offsets())?;
    let equality_ids = file.equality_ids();
    hash_optional_i32_slice(&mut hash, equality_ids.as_deref())?;
    hash_optional_i64(&mut hash, file.first_row_id());
    hash_optional_i32(&mut hash, file.sort_order_id());
    let referenced_data_file = file.referenced_data_file();
    hash_optional_bytes(
        &mut hash,
        referenced_data_file.as_deref().map(str::as_bytes),
    )?;
    hash_optional_i64(&mut hash, file.content_offset());
    hash_optional_i64(&mut hash, file.content_size_in_bytes());
    Ok(IcebergFileFingerprintV1 {
        path: file.file_path().to_string(),
        metadata_sha256: format!("{:x}", hash.finalize()),
        records: file.record_count(),
        bytes: file.file_size_in_bytes(),
    })
}

fn file_format_tag(format: DataFileFormat) -> u8 {
    match format {
        DataFileFormat::Avro => 0,
        DataFileFormat::Orc => 1,
        DataFileFormat::Parquet => 2,
        DataFileFormat::Puffin => 3,
    }
}

fn content_type_tag(content: DataContentType) -> u8 {
    match content {
        DataContentType::Data => 0,
        DataContentType::PositionDeletes => 1,
        DataContentType::EqualityDeletes => 2,
    }
}

fn hash_partition(hash: &mut Sha256, partition: &Struct) -> Result<(), ConnectorError> {
    hash_count(hash, partition.fields().len())?;
    for field in partition.iter() {
        match field {
            Some(Literal::Primitive(value)) => {
                hash.update([1]);
                hash_primitive_literal(hash, value)?;
            }
            Some(_) => {
                return Err(ConnectorError::TransactionError(
                    "[LDB-ICEBERG-DATA-FILE-FINGERPRINT] partition value is not primitive".into(),
                ));
            }
            None => hash.update([0]),
        }
    }
    Ok(())
}

fn hash_primitive_literal(
    hash: &mut Sha256,
    value: &PrimitiveLiteral,
) -> Result<(), ConnectorError> {
    match value {
        PrimitiveLiteral::Boolean(value) => hash.update([0, u8::from(*value)]),
        PrimitiveLiteral::Int(value) => {
            hash.update([1]);
            hash.update(value.to_le_bytes());
        }
        PrimitiveLiteral::Long(value) => {
            hash.update([2]);
            hash.update(value.to_le_bytes());
        }
        PrimitiveLiteral::Float(value) => {
            hash.update([3]);
            hash.update(value.to_le_bytes());
        }
        PrimitiveLiteral::Double(value) => {
            hash.update([4]);
            hash.update(value.to_le_bytes());
        }
        PrimitiveLiteral::String(value) => {
            hash.update([5]);
            hash_len_prefixed(hash, value.as_bytes())?;
        }
        PrimitiveLiteral::Binary(value) => {
            hash.update([6]);
            hash_len_prefixed(hash, value)?;
        }
        PrimitiveLiteral::Int128(value) => {
            hash.update([7]);
            hash.update(value.to_be_bytes());
        }
        PrimitiveLiteral::UInt128(value) => {
            hash.update([8]);
            hash.update(value.to_be_bytes());
        }
        PrimitiveLiteral::AboveMax | PrimitiveLiteral::BelowMin => {
            return Err(ConnectorError::TransactionError(
                "[LDB-ICEBERG-DATA-FILE-FINGERPRINT] partition value is not concrete".into(),
            ));
        }
    }
    Ok(())
}

fn hash_i32_u64_map(
    hash: &mut Sha256,
    values: &std::collections::HashMap<i32, u64>,
) -> Result<(), ConnectorError> {
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_unstable_by_key(|(key, _)| **key);
    hash_count(hash, entries.len())?;
    for (key, value) in entries {
        hash.update(key.to_be_bytes());
        hash.update(value.to_be_bytes());
    }
    Ok(())
}

fn hash_i32_datum_map(
    hash: &mut Sha256,
    values: &std::collections::HashMap<i32, Datum>,
) -> Result<(), ConnectorError> {
    let mut entries = values.iter().collect::<Vec<_>>();
    entries.sort_unstable_by_key(|(key, _)| **key);
    hash_count(hash, entries.len())?;
    for (key, value) in entries {
        hash.update(key.to_be_bytes());
        hash_primitive_type(hash, value.data_type());
        let bytes = value.to_bytes().map_err(|error| {
            ConnectorError::TransactionError(format!(
                "[LDB-ICEBERG-DATA-FILE-FINGERPRINT] metric bound cannot be canonically encoded ({})",
                error.kind()
            ))
        })?;
        hash_len_prefixed(hash, bytes.as_ref())?;
    }
    Ok(())
}

fn hash_primitive_type(hash: &mut Sha256, data_type: &PrimitiveType) {
    match data_type {
        PrimitiveType::Boolean => hash.update([0]),
        PrimitiveType::Int => hash.update([1]),
        PrimitiveType::Long => hash.update([2]),
        PrimitiveType::Float => hash.update([3]),
        PrimitiveType::Double => hash.update([4]),
        PrimitiveType::Decimal { precision, scale } => {
            hash.update([5]);
            hash.update(precision.to_be_bytes());
            hash.update(scale.to_be_bytes());
        }
        PrimitiveType::Date => hash.update([6]),
        PrimitiveType::Time => hash.update([7]),
        PrimitiveType::Timestamp => hash.update([8]),
        PrimitiveType::Timestamptz => hash.update([9]),
        PrimitiveType::TimestampNs => hash.update([10]),
        PrimitiveType::TimestamptzNs => hash.update([11]),
        PrimitiveType::String => hash.update([12]),
        PrimitiveType::Uuid => hash.update([13]),
        PrimitiveType::Fixed(length) => {
            hash.update([14]);
            hash.update(length.to_be_bytes());
        }
        PrimitiveType::Binary => hash.update([15]),
    }
}

fn hash_optional_bytes(hash: &mut Sha256, value: Option<&[u8]>) -> Result<(), ConnectorError> {
    match value {
        Some(value) => {
            hash.update([1]);
            hash_len_prefixed(hash, value)?;
        }
        None => hash.update([0]),
    }
    Ok(())
}

fn hash_optional_i64_slice(
    hash: &mut Sha256,
    values: Option<&[i64]>,
) -> Result<(), ConnectorError> {
    match values {
        Some(values) => {
            hash.update([1]);
            hash_count(hash, values.len())?;
            for value in values {
                hash.update(value.to_be_bytes());
            }
        }
        None => hash.update([0]),
    }
    Ok(())
}

fn hash_optional_i32_slice(
    hash: &mut Sha256,
    values: Option<&[i32]>,
) -> Result<(), ConnectorError> {
    match values {
        Some(values) => {
            hash.update([1]);
            hash_count(hash, values.len())?;
            for value in values {
                hash.update(value.to_be_bytes());
            }
        }
        None => hash.update([0]),
    }
    Ok(())
}

fn hash_optional_i64(hash: &mut Sha256, value: Option<i64>) {
    match value {
        Some(value) => {
            hash.update([1]);
            hash.update(value.to_be_bytes());
        }
        None => hash.update([0]),
    }
}

fn hash_optional_i32(hash: &mut Sha256, value: Option<i32>) {
    match value {
        Some(value) => {
            hash.update([1]);
            hash.update(value.to_be_bytes());
        }
        None => hash.update([0]),
    }
}

pub(super) fn hash_len_prefixed(hash: &mut Sha256, bytes: &[u8]) -> Result<(), ConnectorError> {
    hash_count(hash, bytes.len())?;
    hash.update(bytes);
    Ok(())
}

pub(super) fn hash_count(hash: &mut Sha256, count: usize) -> Result<(), ConnectorError> {
    let count = u64::try_from(count)
        .map_err(|_| ConnectorError::Internal("Iceberg fingerprint length exceeds u64".into()))?;
    hash.update(count.to_be_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use iceberg::spec::{DataFileBuilder, Datum, Literal};

    use super::*;

    fn data_file(reverse_maps: bool, partition: &str, lower: i32) -> DataFile {
        let mut column_sizes = HashMap::new();
        let mut lower_bounds = HashMap::new();
        let mut upper_bounds = HashMap::new();
        let entries = if reverse_maps { [2, 1] } else { [1, 2] };
        for field_id in entries {
            column_sizes.insert(field_id, u64::try_from(field_id * 10).unwrap());
            lower_bounds.insert(field_id, Datum::int(lower + field_id));
            upper_bounds.insert(field_id, Datum::int(lower + field_id + 20));
        }
        let partition = vec![
            Some(Literal::string(partition)),
            None,
            Some(Literal::int(7)),
        ]
        .into_iter()
        .collect();
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("memory:///data/canonical.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(partition)
            .record_count(9)
            .file_size_in_bytes(321)
            .column_sizes(column_sizes)
            .value_counts(HashMap::from([(1, 9), (2, 9)]))
            .null_value_counts(HashMap::from([(1, 0), (2, 1)]))
            .lower_bounds(lower_bounds)
            .upper_bounds(upper_bounds)
            .key_metadata(Some(vec![1, 2, 3]))
            .split_offsets(Some(vec![4, 64]))
            .sort_order_id(3)
            .build()
            .unwrap()
    }

    #[test]
    fn fingerprint_is_canonical_and_content_sensitive() {
        let forward = data_file_fingerprint(&data_file(false, "eu", 10)).unwrap();
        let reverse = data_file_fingerprint(&data_file(true, "eu", 10)).unwrap();
        assert_eq!(forward, reverse);

        let different_partition = data_file_fingerprint(&data_file(false, "us", 10)).unwrap();
        let different_bound = data_file_fingerprint(&data_file(false, "eu", 11)).unwrap();
        assert_ne!(forward, different_partition);
        assert_ne!(forward, different_bound);
        assert_eq!(
            forward.metadata_sha256,
            "ec2f81c6169ad02ea023db0a4dee228686fe12489352441070dbdce65919c61a"
        );
    }

    #[test]
    fn nested_partition_value_fails_closed() {
        let file = DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("memory:///data/invalid.parquet".to_string())
            .file_format(DataFileFormat::Parquet)
            .partition(
                vec![Some(Literal::Struct(Struct::empty()))]
                    .into_iter()
                    .collect(),
            )
            .record_count(1)
            .file_size_in_bytes(1)
            .build()
            .unwrap();
        let error = data_file_fingerprint(&file).unwrap_err().to_string();
        assert!(error.contains("LDB-ICEBERG-DATA-FILE-FINGERPRINT"));
    }
}

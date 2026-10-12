//! Exactly-once epoch staging: records routed during an epoch, and the versioned compressed
//! descriptor that carries them through the checkpoint to the designated committer.
//!
//! Descriptor layout: `b"LDBK"`, format version, codec (1 = zstd) and the little-endian
//! uncompressed length, followed by the compressed body. The body holds the target topic, the
//! partition count records were routed with, the record count, then per record its partition and
//! length-prefixed key and value (`-1` for NULL).

use crate::connector::MAX_COORDINATED_COMMIT_PAYLOAD_BYTES;
use crate::error::ConnectorError;
use crate::kafka::sink_config::MAX_EPOCH_BYTES;

const MAGIC: &[u8; 4] = b"LDBK";
const FORMAT_VERSION: u8 = 1;
const CODEC_ZSTD: u8 = 1;
const HEADER_LEN: usize = MAGIC.len() + 2 + 4;
const ZSTD_LEVEL: i32 = 3;
/// Bytes one staged record adds to the body besides its key and value.
const RECORD_FRAMING: usize = 12;
/// Decompression bound for any participant's descriptor: the largest epoch plus the topic
/// header. It is a protocol constant so a committer never rejects a descriptor that another
/// participant, configured differently, durably staged.
const MAX_BODY_BYTES: usize = MAX_EPOCH_BYTES + 2 + u16::MAX as usize + 8;

/// One routed record held until its checkpoint commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StagedRecord {
    partition: i32,
    key: Option<(usize, usize)>,
    value: Option<(usize, usize)>,
}

/// Records of the open epoch in arrival order, bounded by `max_bytes` of encoded records (key,
/// value and framing) and by `max_record_bytes` per record.
#[derive(Debug)]
pub(super) struct StagedEpoch {
    data: Vec<u8>,
    records: Vec<StagedRecord>,
    encoded_bytes: usize,
    max_bytes: usize,
    max_record_bytes: usize,
}

impl StagedEpoch {
    pub(super) fn new(max_bytes: usize, max_record_bytes: usize) -> Self {
        Self {
            data: Vec::new(),
            records: Vec::new(),
            encoded_bytes: 0,
            max_bytes,
            max_record_bytes,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub(super) fn clear(&mut self) {
        self.data.clear();
        self.records.clear();
        self.encoded_bytes = 0;
    }

    /// Stage one record. Fails without staging anything when the record could never be produced
    /// or the epoch would exceed its bound, so a committed checkpoint never holds output that
    /// publication must reject.
    pub(super) fn push(
        &mut self,
        partition: i32,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<(), ConnectorError> {
        let record = key.map_or(0, <[u8]>::len) + value.map_or(0, <[u8]>::len);
        if record > self.max_record_bytes {
            return Err(ConnectorError::ConfigurationError(format!(
                "a Kafka output record of {record} key and value bytes exceeds the producer's \
                 message.max.bytes ({})",
                self.max_record_bytes
            )));
        }
        let encoded = self.encoded_bytes.saturating_add(record + RECORD_FRAMING);
        if encoded > self.max_bytes {
            return Err(ConnectorError::WriteError(format!(
                "exactly-once Kafka epoch output exceeds epoch.max.bytes ({}); checkpoint more \
                 often or raise the bound (output bytes per second x checkpoint interval)",
                self.max_bytes
            )));
        }
        self.encoded_bytes = encoded;
        let key = key.map(|key| self.append(key));
        let value = value.map(|value| self.append(value));
        self.records.push(StagedRecord {
            partition,
            key,
            value,
        });
        Ok(())
    }

    fn append(&mut self, bytes: &[u8]) -> (usize, usize) {
        let start = self.data.len();
        self.data.extend_from_slice(bytes);
        (start, bytes.len())
    }

    /// Encode and compress the epoch. The caller keeps the stage until the engine accepts the
    /// descriptor, so a failed encoding loses nothing.
    pub(super) fn encode(
        &self,
        topic: &str,
        partition_count: i32,
    ) -> Result<Vec<u8>, ConnectorError> {
        let mut body = Vec::with_capacity(self.data.len() + self.records.len() * 12 + 64);
        let topic_len = u16::try_from(topic.len()).map_err(|_| {
            ConnectorError::ConfigurationError("Kafka topic name exceeds 65535 bytes".into())
        })?;
        body.extend_from_slice(&topic_len.to_le_bytes());
        body.extend_from_slice(topic.as_bytes());
        body.extend_from_slice(&partition_count.to_le_bytes());
        let count = u32::try_from(self.records.len())
            .map_err(|_| ConnectorError::WriteError("too many staged Kafka records".into()))?;
        body.extend_from_slice(&count.to_le_bytes());
        for record in &self.records {
            body.extend_from_slice(&record.partition.to_le_bytes());
            for part in [record.key, record.value] {
                match part {
                    None => body.extend_from_slice(&(-1_i32).to_le_bytes()),
                    Some((start, len)) => {
                        let encoded = i32::try_from(len).map_err(|_| {
                            ConnectorError::WriteError("staged Kafka field exceeds 2 GiB".into())
                        })?;
                        body.extend_from_slice(&encoded.to_le_bytes());
                        body.extend_from_slice(&self.data[start..start + len]);
                    }
                }
            }
        }
        let raw_len = u32::try_from(body.len())
            .map_err(|_| ConnectorError::WriteError("staged Kafka epoch exceeds 4 GiB".into()))?;
        let compressed = zstd::bulk::compress(&body, ZSTD_LEVEL).map_err(|error| {
            ConnectorError::Internal(format!("compress Kafka epoch descriptor: {error}"))
        })?;
        let mut descriptor = Vec::with_capacity(HEADER_LEN + compressed.len());
        descriptor.extend_from_slice(MAGIC);
        descriptor.push(FORMAT_VERSION);
        descriptor.push(CODEC_ZSTD);
        descriptor.extend_from_slice(&raw_len.to_le_bytes());
        descriptor.extend_from_slice(&compressed);
        if descriptor.len() > MAX_COORDINATED_COMMIT_PAYLOAD_BYTES {
            return Err(ConnectorError::WriteError(format!(
                "exactly-once Kafka epoch compresses to {} bytes, above the {} byte checkpoint \
                 descriptor limit; checkpoint more often",
                descriptor.len(),
                MAX_COORDINATED_COMMIT_PAYLOAD_BYTES
            )));
        }
        Ok(descriptor)
    }
}

/// A decoded descriptor: the routed records of one participant's epoch.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct StagedOutput {
    pub(super) topic: String,
    pub(super) partition_count: i32,
    pub(super) records: Vec<OutputRecord>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct OutputRecord {
    pub(super) partition: i32,
    pub(super) key: Option<Vec<u8>>,
    pub(super) value: Option<Vec<u8>>,
}

impl StagedOutput {
    /// Decode a descriptor read back from checkpoint storage, rejecting any malformed input.
    pub(super) fn decode(descriptor: &[u8]) -> Result<Self, ConnectorError> {
        let corrupt = |reason: &str| {
            ConnectorError::TransactionError(format!(
                "Kafka exactly-once descriptor is invalid: {reason}"
            ))
        };
        let header = descriptor
            .get(..HEADER_LEN)
            .ok_or_else(|| corrupt("truncated header"))?;
        if &header[..4] != MAGIC {
            return Err(corrupt("bad magic"));
        }
        if header[4] != FORMAT_VERSION || header[5] != CODEC_ZSTD {
            return Err(corrupt("unsupported format version or codec"));
        }
        let raw_len = u32::from_le_bytes([header[6], header[7], header[8], header[9]]) as usize;
        if raw_len > MAX_BODY_BYTES {
            return Err(corrupt("uncompressed size exceeds the protocol bound"));
        }
        let body = zstd::bulk::decompress(&descriptor[HEADER_LEN..], raw_len)
            .map_err(|_| corrupt("compressed body does not decode"))?;
        if body.len() != raw_len {
            return Err(corrupt("uncompressed size mismatch"));
        }
        let mut reader = Reader {
            bytes: &body,
            position: 0,
        };
        let topic_len = usize::from(u16::from_le_bytes(reader.array()?));
        let topic = std::str::from_utf8(reader.take(topic_len)?)
            .map_err(|_| corrupt("topic is not UTF-8"))?
            .to_owned();
        let partition_count = i32::from_le_bytes(reader.array()?);
        if partition_count <= 0 {
            return Err(corrupt("partition count is not positive"));
        }
        let count = u32::from_le_bytes(reader.array()?) as usize;
        // Each record occupies at least 12 bytes, which bounds the allocation by the body size.
        if count > body.len() / 12 {
            return Err(corrupt("record count exceeds the body"));
        }
        let mut records = Vec::with_capacity(count);
        for _ in 0..count {
            let partition = i32::from_le_bytes(reader.array()?);
            if !(0..partition_count).contains(&partition) {
                return Err(corrupt(
                    "record partition is outside the routed partition count",
                ));
            }
            let key = reader.optional_field()?;
            let value = reader.optional_field()?;
            records.push(OutputRecord {
                partition,
                key,
                value,
            });
        }
        if reader.position != body.len() {
            return Err(corrupt("trailing bytes"));
        }
        Ok(Self {
            topic,
            partition_count,
            records,
        })
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], ConnectorError> {
        let end = self
            .position
            .checked_add(len)
            .filter(|end| *end <= self.bytes.len());
        let end = end.ok_or_else(|| {
            ConnectorError::TransactionError(
                "Kafka exactly-once descriptor is invalid: truncated body".into(),
            )
        })?;
        let slice = &self.bytes[self.position..end];
        self.position = end;
        Ok(slice)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ConnectorError> {
        let mut array = [0_u8; N];
        array.copy_from_slice(self.take(N)?);
        Ok(array)
    }

    fn optional_field(&mut self) -> Result<Option<Vec<u8>>, ConnectorError> {
        match i32::from_le_bytes(self.array()?) {
            -1 => Ok(None),
            len => {
                let len = usize::try_from(len).map_err(|_| {
                    ConnectorError::TransactionError(
                        "Kafka exactly-once descriptor is invalid: negative field length".into(),
                    )
                })?;
                Ok(Some(self.take(len)?.to_vec()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptors_round_trip_null_and_empty_fields_in_order() {
        let mut stage = StagedEpoch::new(1024, 1024);
        stage.push(2, Some(b"k"), Some(b"v1")).unwrap();
        stage.push(0, None, Some(b"")).unwrap();
        stage.push(1, Some(b""), None).unwrap();
        let descriptor = stage.encode("out", 3).unwrap();
        let decoded = StagedOutput::decode(&descriptor).unwrap();
        assert_eq!(decoded.topic, "out");
        assert_eq!(decoded.partition_count, 3);
        assert_eq!(
            decoded.records,
            vec![
                OutputRecord {
                    partition: 2,
                    key: Some(b"k".to_vec()),
                    value: Some(b"v1".to_vec()),
                },
                OutputRecord {
                    partition: 0,
                    key: None,
                    value: Some(Vec::new()),
                },
                OutputRecord {
                    partition: 1,
                    key: Some(Vec::new()),
                    value: None,
                },
            ]
        );
    }

    #[test]
    fn the_epoch_bound_counts_framing_and_rejects_without_staging() {
        let mut stage = StagedEpoch::new(4 + RECORD_FRAMING, 1024);
        stage.push(0, Some(b"ab"), Some(b"cd")).unwrap();
        let error = stage.push(0, None, Some(b"")).unwrap_err();
        assert!(error.to_string().contains("epoch.max.bytes"), "{error}");
        let decoded = StagedOutput::decode(&stage.encode("t", 1).unwrap()).unwrap();
        assert_eq!(decoded.records.len(), 1);

        let mut empty_records = StagedEpoch::new(3 * RECORD_FRAMING, 1024);
        for _ in 0..3 {
            empty_records.push(0, None, Some(b"")).unwrap();
        }
        assert!(empty_records.push(0, None, Some(b"")).is_err());
    }

    #[test]
    fn records_larger_than_message_max_bytes_are_rejected_at_staging() {
        let mut stage = StagedEpoch::new(1024, 3);
        stage.push(0, Some(b"a"), Some(b"bc")).unwrap();
        let error = stage.push(0, Some(b"a"), Some(b"bcd")).unwrap_err();
        assert!(error.to_string().contains("message.max.bytes"), "{error}");
    }

    #[test]
    fn a_full_epoch_at_the_largest_bound_round_trips() {
        let record = vec![7_u8; 64 * 1024];
        let records = 8;
        let mut stage = StagedEpoch::new(records * (record.len() + RECORD_FRAMING), usize::MAX);
        for _ in 0..records {
            stage.push(0, None, Some(&record)).unwrap();
        }
        assert!(stage.push(0, None, Some(b"")).is_err());
        let decoded = StagedOutput::decode(&stage.encode("t", 1).unwrap()).unwrap();
        assert_eq!(decoded.records.len(), records);
    }

    #[test]
    fn malformed_descriptors_are_rejected() {
        let mut stage = StagedEpoch::new(1024, 1024);
        stage.push(0, Some(b"k"), Some(b"v")).unwrap();
        let descriptor = stage.encode("t", 1).unwrap();

        for truncated in [3, HEADER_LEN, descriptor.len() - 1] {
            assert!(StagedOutput::decode(&descriptor[..truncated]).is_err());
        }
        let mut wrong_version = descriptor.clone();
        wrong_version[4] = 9;
        assert!(StagedOutput::decode(&wrong_version).is_err());
        let mut oversized = descriptor.clone();
        oversized[6..10].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(StagedOutput::decode(&oversized).is_err());

        let mut outside = StagedEpoch::new(1024, 1024);
        outside.push(5, None, None).unwrap();
        assert!(StagedOutput::decode(&outside.encode("t", 2).unwrap()).is_err());
    }
}

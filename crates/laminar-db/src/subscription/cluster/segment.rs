use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use bytes::Bytes;
use laminar_core::checkpoint::{
    canonical_json_bytes, CheckpointAttempt, CheckpointParticipant, OutputPartitionId,
    OutputSegmentRef, PartitionSequence, StreamGeneration, SubscriptionDigest,
    SubscriptionProtocolVersion, MAX_OUTPUT_FRAMES_PER_SEGMENT, MAX_OUTPUT_SEGMENT_BYTES,
};
use serde::{Deserialize, Serialize};

use crate::error::DbError;

const SEGMENT_MAGIC: &[u8; 8] = b"LDBSUB01";
const SEGMENT_PREFIX_BYTES: usize = SEGMENT_MAGIC.len() + size_of::<u32>();
const MAX_SEGMENT_HEADER_BYTES: usize = 64 * 1024;

/// Exact append authority sampled before a compute cycle publishes output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OutputWriterAuthority {
    pub(crate) participant: CheckpointParticipant,
    pub(crate) process_term: u64,
    pub(crate) assignment_version: u64,
    pub(crate) assignment_digest: [u8; 32],
}

/// Metadata shared by every frame encoded into one immutable segment.
pub(crate) struct OutputSegmentIdentity<'a> {
    pub(crate) deployment_id: &'a str,
    pub(crate) stream_id: &'a str,
    pub(crate) stream_generation: StreamGeneration,
    pub(crate) partition: OutputPartitionId,
    pub(crate) schema_fingerprint: SubscriptionDigest,
    pub(crate) attempt: CheckpointAttempt,
    pub(crate) authority: OutputWriterAuthority,
}

/// Encoded immutable object and its exact manifest reference.
pub(crate) struct EncodedOutputSegment {
    pub(crate) reference: OutputSegmentRef,
    pub(crate) bytes: Bytes,
}

/// Authoritative checkpoint fields that are intentionally not repeated in a segment reference.
pub(crate) struct OutputSegmentBinding<'a> {
    pub(crate) deployment_id: &'a str,
    pub(crate) stream_id: &'a str,
    pub(crate) attempt: CheckpointAttempt,
    pub(crate) participant: CheckpointParticipant,
    pub(crate) assignment_version: u64,
    pub(crate) assignment_digest: [u8; 32],
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SegmentHeader {
    protocol_version: SubscriptionProtocolVersion,
    deployment_id: String,
    stream_id: String,
    stream_generation: StreamGeneration,
    partition: OutputPartitionId,
    first_sequence: PartitionSequence,
    exclusive_end_sequence: PartitionSequence,
    frame_count: u64,
    row_count: u64,
    schema_fingerprint: SubscriptionDigest,
    ipc_length: u64,
    ipc_digest: SubscriptionDigest,
    checkpoint_id: u64,
    epoch: u64,
    participant: CheckpointParticipant,
    process_term: u64,
    assignment_version: u64,
    assignment_digest: [u8; 32],
}

/// Encode ordered frames as one bounded Arrow IPC stream with authenticated metadata.
pub(crate) fn encode_output_segment(
    identity: &OutputSegmentIdentity<'_>,
    frames: &[RecordBatch],
    first_sequence: PartitionSequence,
) -> Result<EncodedOutputSegment, DbError> {
    validate_encode_input(identity, frames)?;
    let frame_count = u64::try_from(frames.len())
        .map_err(|_| DbError::Checkpoint("subscription frame count exceeds u64".into()))?;
    let exclusive_end_sequence = PartitionSequence::new(
        first_sequence
            .get()
            .checked_add(frame_count)
            .ok_or_else(|| DbError::Checkpoint("subscription sequence range overflow".into()))?,
    );
    let row_count = frames.iter().try_fold(0_u64, |rows, batch| {
        rows.checked_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX))
            .ok_or_else(|| DbError::Checkpoint("subscription segment row count overflow".into()))
    })?;
    let schema = frames
        .first()
        .ok_or_else(|| DbError::Checkpoint("subscription segment has no frames".into()))?
        .schema();
    let ipc = laminar_core::serialization::serialize_batches_stream_bounded(
        schema.as_ref(),
        frames,
        MAX_OUTPUT_SEGMENT_BYTES - MAX_SEGMENT_HEADER_BYTES - SEGMENT_PREFIX_BYTES,
    )
    .map_err(|error| DbError::Checkpoint(format!("encode subscription Arrow segment: {error}")))?;
    let ipc_length = u64::try_from(ipc.len())
        .map_err(|_| DbError::Checkpoint("subscription IPC length exceeds u64".into()))?;
    let ipc_digest = SubscriptionDigest::for_bytes(b"laminardb-subscription-ipc-v1", &ipc);
    let header = SegmentHeader {
        protocol_version: SubscriptionProtocolVersion::CURRENT,
        deployment_id: identity.deployment_id.to_owned(),
        stream_id: identity.stream_id.to_owned(),
        stream_generation: identity.stream_generation,
        partition: identity.partition,
        first_sequence,
        exclusive_end_sequence,
        frame_count,
        row_count,
        schema_fingerprint: identity.schema_fingerprint,
        ipc_length,
        ipc_digest,
        checkpoint_id: identity.attempt.checkpoint_id,
        epoch: identity.attempt.epoch,
        participant: identity.authority.participant,
        process_term: identity.authority.process_term,
        assignment_version: identity.authority.assignment_version,
        assignment_digest: identity.authority.assignment_digest,
    };
    let header = canonical_json_bytes(&header).map_err(|error| {
        DbError::Checkpoint(format!("encode subscription segment header: {error}"))
    })?;
    if header.len() > MAX_SEGMENT_HEADER_BYTES {
        return Err(DbError::Checkpoint(format!(
            "subscription segment header is {} bytes; maximum is {MAX_SEGMENT_HEADER_BYTES}",
            header.len()
        )));
    }
    let total = SEGMENT_PREFIX_BYTES
        .checked_add(header.len())
        .and_then(|length| length.checked_add(ipc.len()))
        .filter(|length| *length <= MAX_OUTPUT_SEGMENT_BYTES)
        .ok_or_else(|| {
            DbError::Checkpoint(format!(
                "subscription segment exceeds the {MAX_OUTPUT_SEGMENT_BYTES}-byte limit"
            ))
        })?;
    let mut encoded = Vec::with_capacity(total);
    encoded.extend_from_slice(SEGMENT_MAGIC);
    encoded.extend_from_slice(
        &u32::try_from(header.len())
            .map_err(|_| DbError::Checkpoint("subscription segment header exceeds u32".into()))?
            .to_le_bytes(),
    );
    encoded.extend_from_slice(&header);
    encoded.extend_from_slice(&ipc);
    let payload_digest =
        SubscriptionDigest::for_bytes(b"laminardb-subscription-segment-v1", &encoded);
    let object_key = output_segment_object_key(
        identity.deployment_id,
        identity.stream_id,
        identity.stream_generation,
        identity.partition,
        identity.attempt,
        first_sequence,
        exclusive_end_sequence,
        payload_digest,
    );
    let encoded_length = u64::try_from(encoded.len())
        .map_err(|_| DbError::Checkpoint("subscription segment length exceeds u64".into()))?;
    Ok(EncodedOutputSegment {
        reference: OutputSegmentRef {
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            object_key,
            stream_generation: identity.stream_generation,
            partition: identity.partition,
            first_sequence,
            exclusive_end_sequence,
            frame_count,
            row_count,
            encoded_length,
            schema_fingerprint: identity.schema_fingerprint,
            payload_digest,
        },
        bytes: Bytes::from(encoded),
    })
}

fn validate_encode_input(
    identity: &OutputSegmentIdentity<'_>,
    frames: &[RecordBatch],
) -> Result<(), DbError> {
    if !identity.attempt.is_canonical()
        || identity.deployment_id.is_empty()
        || identity.stream_id.is_empty()
        || identity.authority.participant.node_id == 0
        || identity.authority.participant.boot_incarnation.is_nil()
        || identity.authority.process_term == 0
        || identity.authority.assignment_version == 0
        || identity.authority.assignment_digest == [0; 32]
    {
        return Err(DbError::Checkpoint(
            "subscription segment identity is not canonical".into(),
        ));
    }
    if frames.is_empty() || frames.iter().any(|batch| batch.num_rows() == 0) {
        return Err(DbError::Checkpoint(
            "subscription segment frames must be non-empty".into(),
        ));
    }
    let expected_schema = frames[0].schema();
    if frames
        .iter()
        .any(|batch| batch.schema().as_ref() != expected_schema.as_ref())
    {
        return Err(DbError::Checkpoint(
            "subscription segment frames do not share one schema".into(),
        ));
    }
    let actual = crate::pipeline_identity::subscription_schema_fingerprint(&expected_schema)?;
    if actual != identity.schema_fingerprint {
        return Err(DbError::Checkpoint(
            "subscription segment schema fingerprint mismatch".into(),
        ));
    }
    Ok(())
}

/// Verify and decode an immutable output segment without trusting its object key or header.
pub(crate) fn decode_output_segment(
    reference: &OutputSegmentRef,
    encoded: &[u8],
) -> Result<Vec<RecordBatch>, DbError> {
    decode_output_segment_inner(reference, encoded, None)
}

/// Decode a segment and bind its writer metadata to the authoritative checkpoint cut.
pub(crate) fn decode_bound_output_segment(
    reference: &OutputSegmentRef,
    encoded: &[u8],
    binding: &OutputSegmentBinding<'_>,
) -> Result<Vec<RecordBatch>, DbError> {
    decode_output_segment_inner(reference, encoded, Some(binding))
}

fn decode_output_segment_inner(
    reference: &OutputSegmentRef,
    encoded: &[u8],
    binding: Option<&OutputSegmentBinding<'_>>,
) -> Result<Vec<RecordBatch>, DbError> {
    let frame_capacity = validate_decode_reference(reference)?;
    if encoded.len() > MAX_OUTPUT_SEGMENT_BYTES
        || u64::try_from(encoded.len()).ok() != Some(reference.encoded_length)
        || encoded.get(..SEGMENT_MAGIC.len()) != Some(SEGMENT_MAGIC.as_slice())
    {
        return Err(DbError::Checkpoint(
            "subscription segment length or magic mismatch".into(),
        ));
    }
    let actual_digest =
        SubscriptionDigest::for_bytes(b"laminardb-subscription-segment-v1", encoded);
    if actual_digest != reference.payload_digest {
        return Err(DbError::Checkpoint(
            "subscription segment payload checksum mismatch".into(),
        ));
    }
    let (header, header_end) = decode_segment_header(reference, encoded, binding)?;
    let ipc = &encoded[header_end..];
    if u64::try_from(ipc.len()).ok() != Some(header.ipc_length)
        || SubscriptionDigest::for_bytes(b"laminardb-subscription-ipc-v1", ipc) != header.ipc_digest
    {
        return Err(DbError::Checkpoint(
            "subscription segment Arrow payload mismatch".into(),
        ));
    }
    let reader = StreamReader::try_new(std::io::Cursor::new(ipc), None).map_err(|error| {
        DbError::Checkpoint(format!("decode subscription Arrow schema: {error}"))
    })?;
    let mut frames = Vec::with_capacity(frame_capacity);
    let mut rows = 0_u64;
    for batch in reader {
        let batch = batch.map_err(|error| {
            DbError::Checkpoint(format!("decode subscription Arrow frame: {error}"))
        })?;
        if batch.num_rows() == 0
            || crate::pipeline_identity::subscription_schema_fingerprint(&batch.schema())?
                != reference.schema_fingerprint
        {
            return Err(DbError::Checkpoint(
                "subscription Arrow frame metadata mismatch".into(),
            ));
        }
        rows = rows
            .checked_add(u64::try_from(batch.num_rows()).unwrap_or(u64::MAX))
            .ok_or_else(|| DbError::Checkpoint("subscription row count overflow".into()))?;
        frames.push(batch);
    }
    if u64::try_from(frames.len()).ok() != Some(reference.frame_count)
        || rows != reference.row_count
    {
        return Err(DbError::Checkpoint(
            "subscription segment frame or row count mismatch".into(),
        ));
    }
    Ok(frames)
}

fn decode_segment_header(
    reference: &OutputSegmentRef,
    encoded: &[u8],
    binding: Option<&OutputSegmentBinding<'_>>,
) -> Result<(SegmentHeader, usize), DbError> {
    let header_len = encoded
        .get(SEGMENT_MAGIC.len()..SEGMENT_PREFIX_BYTES)
        .and_then(|bytes| <[u8; 4]>::try_from(bytes).ok())
        .map(u32::from_le_bytes)
        .and_then(|length| usize::try_from(length).ok())
        .filter(|length| *length <= MAX_SEGMENT_HEADER_BYTES)
        .ok_or_else(|| DbError::Checkpoint("subscription segment header is invalid".into()))?;
    let header_end = SEGMENT_PREFIX_BYTES
        .checked_add(header_len)
        .filter(|end| *end <= encoded.len())
        .ok_or_else(|| DbError::Checkpoint("subscription segment header is truncated".into()))?;
    let header: SegmentHeader = serde_json::from_slice(&encoded[SEGMENT_PREFIX_BYTES..header_end])
        .map_err(|error| {
            DbError::Checkpoint(format!("decode subscription segment header: {error}"))
        })?;
    let canonical_header = canonical_json_bytes(&header).map_err(|error| {
        DbError::Checkpoint(format!("encode subscription segment header: {error}"))
    })?;
    let object_key = output_segment_object_key(
        &header.deployment_id,
        &header.stream_id,
        header.stream_generation,
        header.partition,
        CheckpointAttempt::new(header.epoch, header.checkpoint_id),
        header.first_sequence,
        header.exclusive_end_sequence,
        reference.payload_digest,
    );
    if canonical_header.as_slice() != &encoded[SEGMENT_PREFIX_BYTES..header_end]
        || header.protocol_version != reference.protocol_version
        || header.stream_generation != reference.stream_generation
        || header.partition != reference.partition
        || header.first_sequence != reference.first_sequence
        || header.exclusive_end_sequence != reference.exclusive_end_sequence
        || header.frame_count != reference.frame_count
        || header.row_count != reference.row_count
        || header.schema_fingerprint != reference.schema_fingerprint
        || header.checkpoint_id == 0
        || header.epoch != header.checkpoint_id
        || header.process_term == 0
        || header.assignment_version == 0
        || header.assignment_digest == [0; 32]
        || header.participant.node_id == 0
        || header.participant.boot_incarnation.is_nil()
        || reference.object_key != object_key
    {
        return Err(DbError::Checkpoint(
            "subscription segment metadata mismatch".into(),
        ));
    }
    if binding.is_some_and(|binding| {
        header.deployment_id != binding.deployment_id
            || header.stream_id != binding.stream_id
            || header.epoch != binding.attempt.epoch
            || header.checkpoint_id != binding.attempt.checkpoint_id
            || header.participant != binding.participant
            || header.assignment_version != binding.assignment_version
            || header.assignment_digest != binding.assignment_digest
    }) {
        return Err(DbError::Checkpoint(
            "subscription segment checkpoint binding mismatch".into(),
        ));
    }
    Ok((header, header_end))
}

fn validate_decode_reference(reference: &OutputSegmentRef) -> Result<usize, DbError> {
    let range = reference
        .exclusive_end_sequence
        .get()
        .checked_sub(reference.first_sequence.get());
    if reference.protocol_version != SubscriptionProtocolVersion::CURRENT
        || reference.frame_count == 0
        || reference.frame_count > MAX_OUTPUT_FRAMES_PER_SEGMENT
        || range != Some(reference.frame_count)
        || reference.row_count == 0
        || reference.encoded_length == 0
        || reference.encoded_length > u64::try_from(MAX_OUTPUT_SEGMENT_BYTES).unwrap_or(u64::MAX)
    {
        return Err(DbError::Checkpoint(
            "subscription segment reference exceeds its canonical bounds".into(),
        ));
    }
    usize::try_from(reference.frame_count)
        .map_err(|_| DbError::Checkpoint("subscription frame count exceeds usize".into()))
}

fn output_segment_object_key(
    deployment_id: &str,
    stream_id: &str,
    generation: StreamGeneration,
    partition: OutputPartitionId,
    attempt: CheckpointAttempt,
    first_sequence: PartitionSequence,
    exclusive_end_sequence: PartitionSequence,
    payload_digest: SubscriptionDigest,
) -> String {
    let stream_key = SubscriptionDigest::for_bytes(
        b"laminardb-subscription-stream-key-v1",
        stream_id.as_bytes(),
    );
    format!(
        "subscription-output/{deployment_id}/{stream_key}/{generation}/{}/checkpoint={:020}/{:020}-{:020}-{payload_digest}.arrow",
        partition.get(),
        attempt.checkpoint_id,
        first_sequence.get(),
        exclusive_end_sequence.get(),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    fn identity(schema: &arrow::datatypes::Schema) -> OutputSegmentIdentity<'static> {
        OutputSegmentIdentity {
            deployment_id: "11111111-1111-4111-8111-111111111111",
            stream_id: "positions",
            stream_generation: StreamGeneration::from_digest(SubscriptionDigest::from_bytes(
                [1; 32],
            )),
            partition: OutputPartitionId::new(3),
            schema_fingerprint: crate::pipeline_identity::subscription_schema_fingerprint(schema)
                .unwrap(),
            attempt: CheckpointAttempt::canonical(7),
            authority: OutputWriterAuthority {
                participant: CheckpointParticipant {
                    node_id: 9,
                    boot_incarnation: uuid::Uuid::parse_str("99999999-9999-4999-8999-999999999999")
                        .unwrap(),
                },
                process_term: 4,
                assignment_version: 6,
                assignment_digest: [8; 32],
            },
        }
    }

    fn frames() -> (Arc<Schema>, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let frames = vec![
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![1, 2]))],
            )
            .unwrap(),
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![3]))],
            )
            .unwrap(),
        ];
        (schema, frames)
    }

    #[test]
    fn segment_round_trip_preserves_exact_frame_range() {
        let (schema, frames) = frames();
        let encoded =
            encode_output_segment(&identity(&schema), &frames, PartitionSequence::new(4)).unwrap();
        assert_eq!(encoded.reference.first_sequence.get(), 4);
        assert_eq!(encoded.reference.exclusive_end_sequence.get(), 6);
        assert_eq!(
            decode_output_segment(&encoded.reference, &encoded.bytes).unwrap(),
            frames
        );
    }

    #[test]
    fn segment_checksum_and_truncation_fail_closed() {
        let (schema, frames) = frames();
        let encoded =
            encode_output_segment(&identity(&schema), &frames, PartitionSequence::FIRST).unwrap();
        let mut corrupt = encoded.bytes.to_vec();
        *corrupt.last_mut().unwrap() ^= 1;
        assert!(decode_output_segment(&encoded.reference, &corrupt).is_err());
        assert!(decode_output_segment(
            &encoded.reference,
            &encoded.bytes[..encoded.bytes.len() - 1]
        )
        .is_err());
    }

    #[test]
    fn segment_checkpoint_binding_is_verified() {
        let (schema, frames) = frames();
        let identity = identity(&schema);
        let encoded = encode_output_segment(&identity, &frames, PartitionSequence::FIRST).unwrap();
        let valid = OutputSegmentBinding {
            deployment_id: identity.deployment_id,
            stream_id: identity.stream_id,
            attempt: identity.attempt,
            participant: identity.authority.participant,
            assignment_version: identity.authority.assignment_version,
            assignment_digest: identity.authority.assignment_digest,
        };
        decode_bound_output_segment(&encoded.reference, &encoded.bytes, &valid).unwrap();

        let wrong = OutputSegmentBinding {
            assignment_version: valid.assignment_version + 1,
            ..valid
        };
        assert!(decode_bound_output_segment(&encoded.reference, &encoded.bytes, &wrong).is_err());
    }

    #[test]
    fn segment_decode_rejects_unbounded_counts_and_noncanonical_paths() {
        let (schema, frames) = frames();
        let encoded =
            encode_output_segment(&identity(&schema), &frames, PartitionSequence::FIRST).unwrap();

        let mut oversized = encoded.reference.clone();
        oversized.frame_count = MAX_OUTPUT_FRAMES_PER_SEGMENT + 1;
        oversized.exclusive_end_sequence =
            PartitionSequence::new(MAX_OUTPUT_FRAMES_PER_SEGMENT + 1);
        assert!(decode_output_segment(&oversized, &encoded.bytes).is_err());

        let mut wrong_path = encoded.reference.clone();
        wrong_path.object_key = "subscription-output/wrong/segment.arrow".into();
        assert!(decode_output_segment(&wrong_path, &encoded.bytes).is_err());
    }
}

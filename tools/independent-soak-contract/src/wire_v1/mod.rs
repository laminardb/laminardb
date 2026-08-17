#![cfg_attr(not(test), allow(dead_code))]

//! Frozen, dependency-free wire contract used by the independent soak oracle.
//!
//! This module deliberately has no runtime integration. It describes only the compact bytes that
//! an independent reader must be able to validate without trusting LaminarDB implementation state.

use std::fmt;

pub const MAGIC: [u8; 4] = *b"LDBO";
pub const VERSION: u8 = 1;
pub const PIPELINE_IDENTITY_VERSION: u16 = 5;
pub const KEY_TO_VNODE_ABI_VERSION: u16 = 1;
pub const SINK_PARTITIONING_ABI_VERSION: u16 = 1;
pub const DATA_KIND: u8 = 1;
pub const MARKER_KIND: u8 = 2;
pub const PREFIX_LEN: usize = 10;

const PREDECESSOR_FLAG: u16 = 1;
const DATA_BODY_LEN: usize = 56;
pub const DATA_ENCODED_LEN: usize = PREFIX_LEN + DATA_BODY_LEN;
pub const MAX_DATA_HEADERS_PER_BATCH: usize = 65_536;
pub const MAX_DATA_HEADER_BYTES_PER_BATCH: usize = MAX_DATA_HEADERS_PER_BATCH * DATA_ENCODED_LEN;

pub const MARKER_FIXED_BODY_LEN: usize = 296;
pub const MAX_SINK_ID_LEN: usize = 128;
pub const MAX_OPERATOR_ID_LEN: usize = 128;
pub const MAX_OUTPUT_ID_LEN: usize = 128;
pub const MAX_SHARD_ID_LEN: usize = 64;
const MAX_VNODE_BITMAP_LEN: usize = 8_192;
pub const MAX_MARKER_BODY_LEN: usize = 8_940;
pub const MAX_MARKER_ENCODED_LEN: usize = PREFIX_LEN + MAX_MARKER_BODY_LEN;

const CURRENT_INTERVAL_OFFSET: usize = 0;
const PREDECESSOR_INTERVAL_OFFSET: usize = 16;
const DEPLOYMENT_UUID_OFFSET: usize = 32;
const PIPELINE_INCARNATION_OFFSET: usize = 48;
const PIPELINE_IDENTITY_VERSION_OFFSET: usize = 64;
const PIPELINE_IDENTITY_SHA256_OFFSET: usize = 66;
const KEY_TO_VNODE_ABI_VERSION_OFFSET: usize = 98;
const SINK_PARTITIONING_ABI_VERSION_OFFSET: usize = 100;
const VNODE_COUNT_OFFSET: usize = 102;
const CURRENT_ASSIGNMENT_VERSION_OFFSET: usize = 104;
const CURRENT_ASSIGNMENT_SHA256_OFFSET: usize = 112;
const WRITER_NODE_ID_OFFSET: usize = 144;
const WRITER_BOOT_UUID_OFFSET: usize = 152;
const DURABLE_PROCESS_TERM_OFFSET: usize = 168;
const RECOVERY_EPOCH_OFFSET: usize = 176;
const RECOVERY_CHECKPOINT_ID_OFFSET: usize = 184;
const COMMITTED_INDEX_SHA256_OFFSET: usize = 192;
const RECOVERY_BASE_ASSIGNMENT_VERSION_OFFSET: usize = 224;
const RECOVERY_BASE_ASSIGNMENT_SHA256_OFFSET: usize = 232;
const TOPOLOGY_SHA256_OFFSET: usize = 264;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WireError {
    Truncated,
    TrailingBytes,
    InvalidMagic,
    UnsupportedVersion(u8),
    InvalidKind(u8),
    UnexpectedKind { expected: u8, actual: u8 },
    InvalidFlags(u16),
    InvalidLength(&'static str),
    InvalidField(&'static str),
    UnsupportedFieldVersion { field: &'static str, observed: u16 },
    LimitExceeded(&'static str),
    InvalidUtf8(&'static str),
    ArithmeticOverflow,
    Allocation,
}

impl fmt::Display for WireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => formatter.write_str("wire value is truncated"),
            Self::TrailingBytes => formatter.write_str("wire value has trailing bytes"),
            Self::InvalidMagic => formatter.write_str("wire magic is invalid"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "wire version {version} is unsupported")
            }
            Self::InvalidKind(kind) => write!(formatter, "wire kind {kind} is invalid"),
            Self::UnexpectedKind { expected, actual } => {
                write!(
                    formatter,
                    "wire kind {actual} does not match expected kind {expected}"
                )
            }
            Self::InvalidFlags(flags) => write!(formatter, "wire flags 0x{flags:04x} are invalid"),
            Self::InvalidLength(field) => write!(formatter, "{field} has an invalid length"),
            Self::InvalidField(field) => write!(formatter, "{field} is invalid"),
            Self::UnsupportedFieldVersion { field, observed } => {
                write!(formatter, "{field} version {observed} is unsupported")
            }
            Self::LimitExceeded(field) => write!(formatter, "{field} exceeds its wire limit"),
            Self::InvalidUtf8(field) => write!(formatter, "{field} is not canonical UTF-8"),
            Self::ArithmeticOverflow => formatter.write_str("wire length arithmetic overflowed"),
            Self::Allocation => formatter.write_str("wire output allocation failed"),
        }
    }
}

impl std::error::Error for WireError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataHeaderRef<'a> {
    pub operation_id: &'a [u8; 32],
    pub writer_interval_id: &'a [u8; 16],
    pub admission_sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MarkerRef<'a> {
    pub current_interval_id: &'a [u8; 16],
    pub predecessor_interval_id: Option<&'a [u8; 16]>,
    pub deployment_uuid: &'a [u8; 16],
    pub pipeline_incarnation_id: &'a [u8; 16],
    pub pipeline_identity_version: u16,
    pub pipeline_identity_sha256: &'a [u8; 32],
    pub key_to_vnode_abi_version: u16,
    pub sink_partitioning_abi_version: u16,
    pub vnode_count: u16,
    /// Evidence reference resolved by the oracle; this is not an assignment certificate.
    pub current_assignment_version: u64,
    pub current_assignment_sha256: &'a [u8; 32],
    pub writer_node_id: u64,
    pub writer_boot_uuid: &'a [u8; 16],
    pub durable_process_term: u64,
    pub recovery_epoch: u64,
    pub recovery_checkpoint_id: u64,
    pub committed_index_sha256: &'a [u8; 32],
    pub recovery_base_assignment_version: u64,
    pub recovery_base_assignment_sha256: &'a [u8; 32],
    pub topology_sha256: &'a [u8; 32],
    pub sink_id: &'a str,
    pub operator_id: &'a str,
    pub output_id: &'a str,
    pub shard_id: &'a str,
    pub vnode_bitmap: &'a [u8],
}

impl MarkerRef<'_> {
    #[must_use]
    pub fn owns_vnode(&self, vnode: u16) -> bool {
        if vnode >= self.vnode_count {
            return false;
        }
        let byte = usize::from(vnode / 8);
        let mask = 1_u8 << (vnode % 8);
        self.vnode_bitmap
            .get(byte)
            .is_some_and(|value| value & mask != 0)
    }
}

struct Envelope<'a> {
    flags: u16,
    body: &'a [u8],
}

pub fn encode_data(header: &DataHeaderRef<'_>) -> Result<[u8; DATA_ENCODED_LEN], WireError> {
    require_nonzero(header.operation_id, "operation_id")?;
    require_nonzero(header.writer_interval_id, "writer_interval_id")?;

    let mut encoded = [0_u8; DATA_ENCODED_LEN];
    encoded[..4].copy_from_slice(&MAGIC);
    encoded[4] = VERSION;
    encoded[5] = DATA_KIND;
    encoded[6..8].copy_from_slice(&0_u16.to_be_bytes());
    encoded[8..10].copy_from_slice(&(DATA_BODY_LEN as u16).to_be_bytes());
    encoded[10..42].copy_from_slice(header.operation_id);
    encoded[42..58].copy_from_slice(header.writer_interval_id);
    encoded[58..66].copy_from_slice(&header.admission_sequence.to_be_bytes());
    Ok(encoded)
}

pub fn decode_data(bytes: &[u8]) -> Result<DataHeaderRef<'_>, WireError> {
    let envelope = decode_envelope(bytes, DATA_KIND, DATA_BODY_LEN)?;
    if envelope.flags != 0 {
        return Err(WireError::InvalidFlags(envelope.flags));
    }
    if envelope.body.len() != DATA_BODY_LEN {
        return Err(WireError::InvalidLength("data body"));
    }

    let operation_id = array_at::<32>(envelope.body, 0)?;
    let writer_interval_id = array_at::<16>(envelope.body, 32)?;
    require_nonzero(operation_id, "operation_id")?;
    require_nonzero(writer_interval_id, "writer_interval_id")?;

    Ok(DataHeaderRef {
        operation_id,
        writer_interval_id,
        admission_sequence: u64_at(envelope.body, 48)?,
    })
}

pub fn data_header_batch_bytes(header_count: usize) -> Result<usize, WireError> {
    if header_count > MAX_DATA_HEADERS_PER_BATCH {
        return Err(WireError::LimitExceeded("data header batch count"));
    }
    header_count
        .checked_mul(DATA_ENCODED_LEN)
        .ok_or(WireError::ArithmeticOverflow)
}

/// Validate one already-bounded batch of borrowed Kafka header values without retaining or
/// allocating decoded records. The count/aggregate-byte cap is checked before the first header is
/// decoded; each header must then consume exactly its 66-byte slice.
pub fn validate_data_header_batch(headers: &[&[u8]]) -> Result<usize, WireError> {
    let total_bytes = data_header_batch_bytes(headers.len())?;
    let observed_bytes = headers.iter().try_fold(0_usize, |total, header| {
        total
            .checked_add(header.len())
            .ok_or(WireError::ArithmeticOverflow)
    })?;
    if observed_bytes > MAX_DATA_HEADER_BYTES_PER_BATCH {
        return Err(WireError::LimitExceeded("data header batch bytes"));
    }
    if observed_bytes != total_bytes {
        return Err(WireError::InvalidLength("data header batch"));
    }
    for header in headers {
        decode_data(header)?;
    }
    Ok(total_bytes)
}

pub fn encoded_marker_len(marker: &MarkerRef<'_>) -> Result<usize, WireError> {
    marker_body_len(marker)?
        .checked_add(PREFIX_LEN)
        .ok_or(WireError::ArithmeticOverflow)
}

pub fn encode_marker_into(
    marker: &MarkerRef<'_>,
    output: &mut Vec<u8>,
) -> Result<usize, WireError> {
    let body_len = marker_body_len(marker)?;
    let encoded_len = body_len
        .checked_add(PREFIX_LEN)
        .ok_or(WireError::ArithmeticOverflow)?;

    if output.capacity() < encoded_len {
        let additional = encoded_len
            .checked_sub(output.len())
            .ok_or(WireError::ArithmeticOverflow)?;
        output
            .try_reserve_exact(additional)
            .map_err(|_| WireError::Allocation)?;
    }

    output.clear();
    output.resize(PREFIX_LEN + MARKER_FIXED_BODY_LEN, 0);
    output[..4].copy_from_slice(&MAGIC);
    output[4] = VERSION;
    output[5] = MARKER_KIND;
    let flags = if marker.predecessor_interval_id.is_some() {
        PREDECESSOR_FLAG
    } else {
        0
    };
    output[6..8].copy_from_slice(&flags.to_be_bytes());
    output[8..10].copy_from_slice(
        &u16::try_from(body_len)
            .map_err(|_| WireError::ArithmeticOverflow)?
            .to_be_bytes(),
    );

    copy_body(output, CURRENT_INTERVAL_OFFSET, marker.current_interval_id);
    if let Some(predecessor) = marker.predecessor_interval_id {
        copy_body(output, PREDECESSOR_INTERVAL_OFFSET, predecessor);
    }
    copy_body(output, DEPLOYMENT_UUID_OFFSET, marker.deployment_uuid);
    copy_body(
        output,
        PIPELINE_INCARNATION_OFFSET,
        marker.pipeline_incarnation_id,
    );
    copy_body(
        output,
        PIPELINE_IDENTITY_VERSION_OFFSET,
        &marker.pipeline_identity_version.to_be_bytes(),
    );
    copy_body(
        output,
        PIPELINE_IDENTITY_SHA256_OFFSET,
        marker.pipeline_identity_sha256,
    );
    copy_body(
        output,
        KEY_TO_VNODE_ABI_VERSION_OFFSET,
        &marker.key_to_vnode_abi_version.to_be_bytes(),
    );
    copy_body(
        output,
        SINK_PARTITIONING_ABI_VERSION_OFFSET,
        &marker.sink_partitioning_abi_version.to_be_bytes(),
    );
    copy_body(
        output,
        VNODE_COUNT_OFFSET,
        &marker.vnode_count.to_be_bytes(),
    );
    copy_body(
        output,
        CURRENT_ASSIGNMENT_VERSION_OFFSET,
        &marker.current_assignment_version.to_be_bytes(),
    );
    copy_body(
        output,
        CURRENT_ASSIGNMENT_SHA256_OFFSET,
        marker.current_assignment_sha256,
    );
    copy_body(
        output,
        WRITER_NODE_ID_OFFSET,
        &marker.writer_node_id.to_be_bytes(),
    );
    copy_body(output, WRITER_BOOT_UUID_OFFSET, marker.writer_boot_uuid);
    copy_body(
        output,
        DURABLE_PROCESS_TERM_OFFSET,
        &marker.durable_process_term.to_be_bytes(),
    );
    copy_body(
        output,
        RECOVERY_EPOCH_OFFSET,
        &marker.recovery_epoch.to_be_bytes(),
    );
    copy_body(
        output,
        RECOVERY_CHECKPOINT_ID_OFFSET,
        &marker.recovery_checkpoint_id.to_be_bytes(),
    );
    copy_body(
        output,
        COMMITTED_INDEX_SHA256_OFFSET,
        marker.committed_index_sha256,
    );
    copy_body(
        output,
        RECOVERY_BASE_ASSIGNMENT_VERSION_OFFSET,
        &marker.recovery_base_assignment_version.to_be_bytes(),
    );
    copy_body(
        output,
        RECOVERY_BASE_ASSIGNMENT_SHA256_OFFSET,
        marker.recovery_base_assignment_sha256,
    );
    copy_body(output, TOPOLOGY_SHA256_OFFSET, marker.topology_sha256);

    push_text(output, marker.sink_id);
    push_text(output, marker.operator_id);
    push_text(output, marker.output_id);
    push_text(output, marker.shard_id);
    output.extend_from_slice(marker.vnode_bitmap);

    if output.len() != encoded_len || output.len() > MAX_MARKER_ENCODED_LEN {
        return Err(WireError::InvalidLength("encoded marker"));
    }
    Ok(encoded_len)
}

pub fn decode_marker(bytes: &[u8]) -> Result<MarkerRef<'_>, WireError> {
    let envelope = decode_envelope(bytes, MARKER_KIND, MAX_MARKER_BODY_LEN)?;
    if envelope.flags & !PREDECESSOR_FLAG != 0 {
        return Err(WireError::InvalidFlags(envelope.flags));
    }

    let body = envelope.body;
    let current_interval_id = array_at::<16>(body, CURRENT_INTERVAL_OFFSET)?;
    let predecessor_bytes = array_at::<16>(body, PREDECESSOR_INTERVAL_OFFSET)?;
    let deployment_uuid = array_at::<16>(body, DEPLOYMENT_UUID_OFFSET)?;
    let pipeline_incarnation_id = array_at::<16>(body, PIPELINE_INCARNATION_OFFSET)?;
    let pipeline_identity_version = u16_at(body, PIPELINE_IDENTITY_VERSION_OFFSET)?;
    let pipeline_identity_sha256 = array_at::<32>(body, PIPELINE_IDENTITY_SHA256_OFFSET)?;
    let key_to_vnode_abi_version = u16_at(body, KEY_TO_VNODE_ABI_VERSION_OFFSET)?;
    let sink_partitioning_abi_version = u16_at(body, SINK_PARTITIONING_ABI_VERSION_OFFSET)?;
    let vnode_count = u16_at(body, VNODE_COUNT_OFFSET)?;
    let current_assignment_version = u64_at(body, CURRENT_ASSIGNMENT_VERSION_OFFSET)?;
    let current_assignment_sha256 = array_at::<32>(body, CURRENT_ASSIGNMENT_SHA256_OFFSET)?;
    let writer_node_id = u64_at(body, WRITER_NODE_ID_OFFSET)?;
    let writer_boot_uuid = array_at::<16>(body, WRITER_BOOT_UUID_OFFSET)?;
    let durable_process_term = u64_at(body, DURABLE_PROCESS_TERM_OFFSET)?;
    let recovery_epoch = u64_at(body, RECOVERY_EPOCH_OFFSET)?;
    let recovery_checkpoint_id = u64_at(body, RECOVERY_CHECKPOINT_ID_OFFSET)?;
    let committed_index_sha256 = array_at::<32>(body, COMMITTED_INDEX_SHA256_OFFSET)?;
    let recovery_base_assignment_version = u64_at(body, RECOVERY_BASE_ASSIGNMENT_VERSION_OFFSET)?;
    let recovery_base_assignment_sha256 =
        array_at::<32>(body, RECOVERY_BASE_ASSIGNMENT_SHA256_OFFSET)?;
    let topology_sha256 = array_at::<32>(body, TOPOLOGY_SHA256_OFFSET)?;

    require_nonzero(current_interval_id, "current_interval_id")?;
    let predecessor_interval_id = match envelope.flags & PREDECESSOR_FLAG {
        PREDECESSOR_FLAG => {
            require_nonzero(predecessor_bytes, "predecessor_interval_id")?;
            if predecessor_bytes == current_interval_id {
                return Err(WireError::InvalidField("predecessor_interval_id"));
            }
            Some(predecessor_bytes)
        }
        0 if predecessor_bytes.iter().all(|byte| *byte == 0) => None,
        0 => return Err(WireError::InvalidField("predecessor flag")),
        _ => unreachable!("unknown marker flags were rejected"),
    };
    require_nonzero(deployment_uuid, "deployment_uuid")?;
    require_nonzero(pipeline_incarnation_id, "pipeline_incarnation_id")?;
    require_version(
        pipeline_identity_version,
        PIPELINE_IDENTITY_VERSION,
        "pipeline_identity_version",
    )?;
    require_nonzero(pipeline_identity_sha256, "pipeline_identity_sha256")?;
    require_version(
        key_to_vnode_abi_version,
        KEY_TO_VNODE_ABI_VERSION,
        "key_to_vnode_abi_version",
    )?;
    require_version(
        sink_partitioning_abi_version,
        SINK_PARTITIONING_ABI_VERSION,
        "sink_partitioning_abi_version",
    )?;
    require_nonzero_u16(vnode_count, "vnode_count")?;
    require_nonzero_u64(current_assignment_version, "current_assignment_version")?;
    require_nonzero(current_assignment_sha256, "current_assignment_sha256")?;
    require_nonzero_u64(writer_node_id, "writer_node_id")?;
    require_nonzero(writer_boot_uuid, "writer_boot_uuid")?;
    require_nonzero_u64(durable_process_term, "durable_process_term")?;
    require_nonzero_u64(recovery_epoch, "recovery_epoch")?;
    require_nonzero_u64(recovery_checkpoint_id, "recovery_checkpoint_id")?;
    if recovery_checkpoint_id != recovery_epoch {
        return Err(WireError::InvalidField("recovery_checkpoint_id"));
    }
    require_nonzero(committed_index_sha256, "committed_index_sha256")?;
    require_nonzero_u64(
        recovery_base_assignment_version,
        "recovery_base_assignment_version",
    )?;
    require_nonzero(
        recovery_base_assignment_sha256,
        "recovery_base_assignment_sha256",
    )?;
    require_nonzero(topology_sha256, "topology_sha256")?;

    let mut cursor = MARKER_FIXED_BODY_LEN;
    let sink_id = take_text(body, &mut cursor, MAX_SINK_ID_LEN, "sink_id")?;
    let operator_id = take_text(body, &mut cursor, MAX_OPERATOR_ID_LEN, "operator_id")?;
    let output_id = take_text(body, &mut cursor, MAX_OUTPUT_ID_LEN, "output_id")?;
    let shard_id = take_text(body, &mut cursor, MAX_SHARD_ID_LEN, "shard_id")?;
    let expected_bitmap_len = vnode_bitmap_len(vnode_count)?;
    let actual_bitmap_len = body
        .len()
        .checked_sub(cursor)
        .ok_or(WireError::InvalidLength("vnode bitmap"))?;
    if actual_bitmap_len != expected_bitmap_len {
        return Err(WireError::InvalidLength("vnode bitmap"));
    }
    let vnode_bitmap = body.get(cursor..).ok_or(WireError::Truncated)?;
    validate_vnode_bitmap(vnode_count, vnode_bitmap)?;

    Ok(MarkerRef {
        current_interval_id,
        predecessor_interval_id,
        deployment_uuid,
        pipeline_incarnation_id,
        pipeline_identity_version,
        pipeline_identity_sha256,
        key_to_vnode_abi_version,
        sink_partitioning_abi_version,
        vnode_count,
        current_assignment_version,
        current_assignment_sha256,
        writer_node_id,
        writer_boot_uuid,
        durable_process_term,
        recovery_epoch,
        recovery_checkpoint_id,
        committed_index_sha256,
        recovery_base_assignment_version,
        recovery_base_assignment_sha256,
        topology_sha256,
        sink_id,
        operator_id,
        output_id,
        shard_id,
        vnode_bitmap,
    })
}

fn marker_body_len(marker: &MarkerRef<'_>) -> Result<usize, WireError> {
    validate_marker(marker)?;
    let text_bytes = [
        marker.sink_id.len(),
        marker.operator_id.len(),
        marker.output_id.len(),
        marker.shard_id.len(),
    ]
    .into_iter()
    .try_fold(0_usize, |total, length| {
        total
            .checked_add(1)
            .and_then(|value| value.checked_add(length))
            .ok_or(WireError::ArithmeticOverflow)
    })?;
    let body_len = MARKER_FIXED_BODY_LEN
        .checked_add(text_bytes)
        .and_then(|value| value.checked_add(marker.vnode_bitmap.len()))
        .ok_or(WireError::ArithmeticOverflow)?;
    if body_len > MAX_MARKER_BODY_LEN {
        return Err(WireError::LimitExceeded("marker body"));
    }
    Ok(body_len)
}

fn validate_marker(marker: &MarkerRef<'_>) -> Result<(), WireError> {
    require_nonzero(marker.current_interval_id, "current_interval_id")?;
    if let Some(predecessor) = marker.predecessor_interval_id {
        require_nonzero(predecessor, "predecessor_interval_id")?;
        if predecessor == marker.current_interval_id {
            return Err(WireError::InvalidField("predecessor_interval_id"));
        }
    }
    require_nonzero(marker.deployment_uuid, "deployment_uuid")?;
    require_nonzero(marker.pipeline_incarnation_id, "pipeline_incarnation_id")?;
    require_version(
        marker.pipeline_identity_version,
        PIPELINE_IDENTITY_VERSION,
        "pipeline_identity_version",
    )?;
    require_nonzero(marker.pipeline_identity_sha256, "pipeline_identity_sha256")?;
    require_version(
        marker.key_to_vnode_abi_version,
        KEY_TO_VNODE_ABI_VERSION,
        "key_to_vnode_abi_version",
    )?;
    require_version(
        marker.sink_partitioning_abi_version,
        SINK_PARTITIONING_ABI_VERSION,
        "sink_partitioning_abi_version",
    )?;
    require_nonzero_u16(marker.vnode_count, "vnode_count")?;
    require_nonzero_u64(
        marker.current_assignment_version,
        "current_assignment_version",
    )?;
    require_nonzero(
        marker.current_assignment_sha256,
        "current_assignment_sha256",
    )?;
    require_nonzero_u64(marker.writer_node_id, "writer_node_id")?;
    require_nonzero(marker.writer_boot_uuid, "writer_boot_uuid")?;
    require_nonzero_u64(marker.durable_process_term, "durable_process_term")?;
    require_nonzero_u64(marker.recovery_epoch, "recovery_epoch")?;
    require_nonzero_u64(marker.recovery_checkpoint_id, "recovery_checkpoint_id")?;
    if marker.recovery_checkpoint_id != marker.recovery_epoch {
        return Err(WireError::InvalidField("recovery_checkpoint_id"));
    }
    require_nonzero(marker.committed_index_sha256, "committed_index_sha256")?;
    require_nonzero_u64(
        marker.recovery_base_assignment_version,
        "recovery_base_assignment_version",
    )?;
    require_nonzero(
        marker.recovery_base_assignment_sha256,
        "recovery_base_assignment_sha256",
    )?;
    require_nonzero(marker.topology_sha256, "topology_sha256")?;
    validate_text(marker.sink_id, MAX_SINK_ID_LEN, "sink_id")?;
    validate_text(marker.operator_id, MAX_OPERATOR_ID_LEN, "operator_id")?;
    validate_text(marker.output_id, MAX_OUTPUT_ID_LEN, "output_id")?;
    validate_text(marker.shard_id, MAX_SHARD_ID_LEN, "shard_id")?;
    let expected_bitmap_len = vnode_bitmap_len(marker.vnode_count)?;
    if marker.vnode_bitmap.len() != expected_bitmap_len {
        return Err(WireError::InvalidLength("vnode bitmap"));
    }
    validate_vnode_bitmap(marker.vnode_count, marker.vnode_bitmap)
}

fn decode_envelope(
    bytes: &[u8],
    expected_kind: u8,
    body_len_limit: usize,
) -> Result<Envelope<'_>, WireError> {
    let prefix = bytes.get(..PREFIX_LEN).ok_or(WireError::Truncated)?;
    if prefix[..4] != MAGIC {
        return Err(WireError::InvalidMagic);
    }
    if prefix[4] != VERSION {
        return Err(WireError::UnsupportedVersion(prefix[4]));
    }
    let kind = prefix[5];
    if kind != DATA_KIND && kind != MARKER_KIND {
        return Err(WireError::InvalidKind(kind));
    }
    if kind != expected_kind {
        return Err(WireError::UnexpectedKind {
            expected: expected_kind,
            actual: kind,
        });
    }
    let flags = u16::from_be_bytes([prefix[6], prefix[7]]);
    let body_len = usize::from(u16::from_be_bytes([prefix[8], prefix[9]]));
    if body_len > body_len_limit {
        return Err(WireError::LimitExceeded("body"));
    }
    let total_len = PREFIX_LEN
        .checked_add(body_len)
        .ok_or(WireError::ArithmeticOverflow)?;
    if bytes.len() < total_len {
        return Err(WireError::Truncated);
    }
    if bytes.len() > total_len {
        return Err(WireError::TrailingBytes);
    }
    Ok(Envelope {
        flags,
        body: bytes.get(PREFIX_LEN..).ok_or(WireError::Truncated)?,
    })
}

fn take_text<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
    cap: usize,
    field: &'static str,
) -> Result<&'a str, WireError> {
    let length = usize::from(*bytes.get(*cursor).ok_or(WireError::Truncated)?);
    *cursor = cursor.checked_add(1).ok_or(WireError::ArithmeticOverflow)?;
    if length == 0 {
        return Err(WireError::InvalidField(field));
    }
    if length > cap {
        return Err(WireError::LimitExceeded(field));
    }
    let end = cursor
        .checked_add(length)
        .ok_or(WireError::ArithmeticOverflow)?;
    let raw = bytes.get(*cursor..end).ok_or(WireError::Truncated)?;
    *cursor = end;
    if raw.contains(&0) {
        return Err(WireError::InvalidField(field));
    }
    std::str::from_utf8(raw).map_err(|_| WireError::InvalidUtf8(field))
}

fn validate_text(value: &str, cap: usize, field: &'static str) -> Result<(), WireError> {
    if value.is_empty() || value.as_bytes().contains(&0) {
        return Err(WireError::InvalidField(field));
    }
    if value.len() > cap {
        return Err(WireError::LimitExceeded(field));
    }
    Ok(())
}

fn validate_vnode_bitmap(vnode_count: u16, bitmap: &[u8]) -> Result<(), WireError> {
    if bitmap.len() > MAX_VNODE_BITMAP_LEN {
        return Err(WireError::LimitExceeded("vnode bitmap"));
    }
    if bitmap.iter().all(|byte| *byte == 0) {
        return Err(WireError::InvalidField("vnode bitmap"));
    }
    let used_bits = vnode_count % 8;
    if used_bits != 0 {
        let allowed = (1_u8 << used_bits) - 1;
        if bitmap.last().is_none_or(|last| last & !allowed != 0) {
            return Err(WireError::InvalidField("vnode bitmap padding"));
        }
    }
    Ok(())
}

fn vnode_bitmap_len(vnode_count: u16) -> Result<usize, WireError> {
    usize::from(vnode_count)
        .checked_add(7)
        .map(|bits| bits / 8)
        .ok_or(WireError::ArithmeticOverflow)
}

fn require_nonzero(value: &[u8], field: &'static str) -> Result<(), WireError> {
    if value.iter().all(|byte| *byte == 0) {
        Err(WireError::InvalidField(field))
    } else {
        Ok(())
    }
}

fn require_nonzero_u16(value: u16, field: &'static str) -> Result<(), WireError> {
    if value == 0 {
        Err(WireError::InvalidField(field))
    } else {
        Ok(())
    }
}

fn require_nonzero_u64(value: u64, field: &'static str) -> Result<(), WireError> {
    if value == 0 {
        Err(WireError::InvalidField(field))
    } else {
        Ok(())
    }
}

fn require_version(version: u16, expected: u16, field: &'static str) -> Result<(), WireError> {
    if version == expected {
        Ok(())
    } else {
        Err(WireError::UnsupportedFieldVersion {
            field,
            observed: version,
        })
    }
}

fn array_at<const N: usize>(bytes: &[u8], offset: usize) -> Result<&[u8; N], WireError> {
    let end = offset.checked_add(N).ok_or(WireError::ArithmeticOverflow)?;
    bytes
        .get(offset..end)
        .ok_or(WireError::Truncated)?
        .try_into()
        .map_err(|_| WireError::InvalidLength("fixed field"))
}

fn u16_at(bytes: &[u8], offset: usize) -> Result<u16, WireError> {
    Ok(u16::from_be_bytes(*array_at::<2>(bytes, offset)?))
}

fn u64_at(bytes: &[u8], offset: usize) -> Result<u64, WireError> {
    Ok(u64::from_be_bytes(*array_at::<8>(bytes, offset)?))
}

fn copy_body(output: &mut [u8], offset: usize, value: &[u8]) {
    let start = PREFIX_LEN + offset;
    let end = start + value.len();
    output[start..end].copy_from_slice(value);
}

fn push_text(output: &mut Vec<u8>, value: &str) {
    debug_assert!(u8::try_from(value.len()).is_ok());
    output.push(value.len() as u8);
    output.extend_from_slice(value.as_bytes());
}

#[cfg(test)]
mod tests;

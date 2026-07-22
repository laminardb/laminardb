//! Admission-neutral `VnodePartialV2` wire codec.
//!
//! This module deliberately has no manifest, object-store, restore, or admission integration. A
//! future manifest-selected reader may consume it only after the bounded fetch and whole-transition
//! contracts in ADR-008 are implemented.

#![cfg_attr(not(test), allow(dead_code))]

use std::cmp::Ordering;

use laminar_core::state::{CheckpointAttempt, KeyGroupCount, PARTITIONING_ABI_VERSION};
use sha2::{Digest, Sha256};

const MAGIC: &[u8; 8] = b"LDBVPD\0\0";
const FORMAT_VERSION: u16 = 2;
const HEADER_LEN: usize = 160;
const HEADER_LEN_U16: u16 = 160;
const ENTRY_LEN: usize = 168;
const ENTRY_LEN_U16: u16 = 168;
const MANAGED_ENVELOPE_VERSION: u16 = 1;
const ENTRY_DIGEST_DOMAIN: &[u8] = b"laminardb-vnode-partial-v2-entry-sha256\0";
const SHA256_LEN: usize = 32;
const ZERO_SHA256: [u8; SHA256_LEN] = [0; SHA256_LEN];

const ENTRY_KIND_BODY: u8 = 1;
const ENTRY_KIND_REFERENCE: u8 = 2;
const ARTIFACT_KIND_NONE: u8 = 0;
const ARTIFACT_KIND_FULL: u8 = 1;
const ARTIFACT_KIND_DELTA: u8 = 2;
const ARTIFACT_KIND_EMPTY: u8 = 3;

/// Numerical limits that can be enforced by the outer directory without decoding a BODY.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(clippy::struct_field_names)] // Names intentionally mirror the qualification profile.
pub(crate) struct VnodePartialV2Limits {
    pub(crate) encoded_artifact_bytes_max: u64,
    pub(crate) envelope_metadata_bytes_max: u64,
    pub(crate) directory_entries_per_artifact_max: u32,
}

/// One plan-owned state-table identity expected in this vnode partial.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ExpectedRosterEntry {
    pub(crate) operator_identity_sha256: [u8; SHA256_LEN],
    pub(crate) state_table_identity_sha256: [u8; SHA256_LEN],
    pub(crate) vnode: u32,
    pub(crate) managed_envelope_version: u16,
}

/// Trusted context derived before artifact fetch from the plan and sealed checkpoint inventory.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExpectedContext<'a> {
    pub(crate) attempt: CheckpointAttempt,
    pub(crate) assignment_version: u64,
    pub(crate) partitioning_abi_version: u16,
    pub(crate) vnode_count: u32,
    pub(crate) vnode: u32,
    pub(crate) assignment_certificate_sha256: [u8; SHA256_LEN],
    pub(crate) roster: &'a [ExpectedRosterEntry],
}

/// An exact link to one entry in an older sealed checkpoint attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ParentEntryLink {
    pub(crate) attempt: CheckpointAttempt,
    pub(crate) entry_sha256: [u8; SHA256_LEN],
}

/// The three BODY artifact shapes understood by managed envelope version 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ArtifactKind {
    Full,
    Delta,
    Empty,
}

impl ArtifactKind {
    const fn encoded(self) -> u8 {
        match self {
            Self::Full => ARTIFACT_KIND_FULL,
            Self::Delta => ARTIFACT_KIND_DELTA,
            Self::Empty => ARTIFACT_KIND_EMPTY,
        }
    }

    fn decode(value: u8) -> Result<Self, VnodePartialV2Error> {
        match value {
            ARTIFACT_KIND_FULL => Ok(Self::Full),
            ARTIFACT_KIND_DELTA => Ok(Self::Delta),
            ARTIFACT_KIND_EMPTY => Ok(Self::Empty),
            _ => Err(invalid("BODY has an invalid artifact kind")),
        }
    }
}

/// One already-sorted entry supplied to the directory encoder.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EncodeEntry<'a> {
    pub(crate) operator_identity_sha256: [u8; SHA256_LEN],
    pub(crate) state_table_identity_sha256: [u8; SHA256_LEN],
    pub(crate) payload: EncodeEntryPayload<'a>,
}

/// BODY bytes or an exact REFERENCE parent.
#[derive(Clone, Copy, Debug)]
pub(crate) enum EncodeEntryPayload<'a> {
    Body {
        artifact_kind: ArtifactKind,
        body: &'a [u8],
        parent: Option<ParentEntryLink>,
    },
    Reference {
        parent: ParentEntryLink,
    },
}

/// An outer-structurally validated borrowed directory entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DecodedEntry<'a> {
    pub(crate) operator_identity_sha256: [u8; SHA256_LEN],
    pub(crate) state_table_identity_sha256: [u8; SHA256_LEN],
    pub(crate) vnode: u32,
    pub(crate) managed_envelope_version: u16,
    pub(crate) contextual_sha256: [u8; SHA256_LEN],
    pub(crate) payload: DecodedEntryPayload<'a>,
}

/// Borrowed opaque BODY data or a REFERENCE link from an outer-validated directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DecodedEntryPayload<'a> {
    Body {
        artifact_kind: ArtifactKind,
        body: &'a [u8],
        body_sha256: [u8; SHA256_LEN],
        parent: Option<ParentEntryLink>,
    },
    Reference {
        parent: ParentEntryLink,
    },
}

/// An outer-structurally validated view over the caller-owned V2 bytes.
///
/// BODY bytes have integrity protection here, but remain semantically opaque until the selected
/// managed-envelope decoder validates them against its own trusted context.
#[derive(Debug)]
pub(crate) struct DecodedVnodePartialV2<'a> {
    bytes: &'a [u8],
    directory: &'a [u8],
    digest_context: EntryDigestContext,
    entry_count: u32,
}

impl<'a> DecodedVnodePartialV2<'a> {
    pub(crate) const fn attempt(&self) -> CheckpointAttempt {
        self.digest_context.attempt
    }

    pub(crate) const fn assignment_version(&self) -> u64 {
        self.digest_context.assignment_version
    }

    pub(crate) const fn partitioning_abi_version(&self) -> u16 {
        self.digest_context.partitioning_abi_version
    }

    pub(crate) const fn vnode_count(&self) -> u32 {
        self.digest_context.vnode_count
    }

    pub(crate) const fn vnode(&self) -> u32 {
        self.digest_context.vnode
    }

    pub(crate) const fn assignment_certificate_sha256(&self) -> [u8; SHA256_LEN] {
        self.digest_context.assignment_certificate_sha256
    }

    pub(crate) const fn entry_count(&self) -> u32 {
        self.entry_count
    }

    pub(crate) fn entries(&self) -> DecodedEntryIter<'a> {
        DecodedEntryIter {
            chunks: self.directory.chunks_exact(ENTRY_LEN),
            bytes: self.bytes,
            digest_context: self.digest_context,
        }
    }
}

/// Allocation-free iterator over entries in an outer-validated borrowed directory.
pub(crate) struct DecodedEntryIter<'a> {
    chunks: std::slice::ChunksExact<'a, u8>,
    bytes: &'a [u8],
    digest_context: EntryDigestContext,
}

impl<'a> Iterator for DecodedEntryIter<'a> {
    type Item = Result<DecodedEntry<'a>, VnodePartialV2Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.chunks
            .next()
            .map(|raw| decode_outer_validated_entry(raw, self.bytes, self.digest_context))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.chunks.size_hint()
    }
}

impl ExactSizeIterator for DecodedEntryIter<'_> {}

/// A structural or expected-context mismatch in V2 bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid VnodePartialV2: {0}")]
pub(crate) struct VnodePartialV2Error(&'static str);

const fn invalid(message: &'static str) -> VnodePartialV2Error {
    VnodePartialV2Error(message)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct EntryDigestContext {
    attempt: CheckpointAttempt,
    assignment_version: u64,
    partitioning_abi_version: u16,
    vnode_count: u32,
    vnode: u32,
    assignment_certificate_sha256: [u8; SHA256_LEN],
}

#[derive(Clone, Copy, Debug)]
struct RawEntry {
    operator_identity_sha256: [u8; SHA256_LEN],
    state_table_identity_sha256: [u8; SHA256_LEN],
    vnode: u32,
    entry_kind: u8,
    artifact_kind: u8,
    managed_envelope_version: u16,
    body_offset: u64,
    body_len: u64,
    body_sha256: [u8; SHA256_LEN],
    parent: CheckpointAttempt,
    parent_entry_sha256: [u8; SHA256_LEN],
}

/// Validate the V2 outer directory without allocating or trusting self-declared context.
///
/// This verifies canonical ranges, roster/provenance, directory integrity, and each BODY digest.
/// It does not interpret or semantically validate the opaque managed-envelope BODY bytes.
pub(crate) fn decode<'a>(
    bytes: &'a [u8],
    expected: ExpectedContext<'_>,
    limits: VnodePartialV2Limits,
) -> Result<DecodedVnodePartialV2<'a>, VnodePartialV2Error> {
    validate_expected_context(expected, limits)?;

    let supplied_len = u64::try_from(bytes.len()).map_err(|_| invalid("length exceeds u64"))?;
    if supplied_len > limits.encoded_artifact_bytes_max {
        return Err(invalid(
            "encoded artifact exceeds its configured byte limit",
        ));
    }
    if bytes.len() < HEADER_LEN {
        return Err(invalid("truncated directory header"));
    }
    if read_array::<8>(bytes, 0)? != *MAGIC {
        return Err(invalid("wrong directory magic"));
    }
    if read_u16(bytes, 8)? != FORMAT_VERSION {
        return Err(invalid("unsupported directory version"));
    }
    if read_u16(bytes, 10)? != HEADER_LEN_U16 {
        return Err(invalid("wrong directory header length"));
    }
    if read_u16(bytes, 12)? != ENTRY_LEN_U16 {
        return Err(invalid("wrong directory entry length"));
    }
    if read_u16(bytes, 14)? != 0 || read_u16(bytes, 50)? != 0 {
        return Err(invalid("nonzero reserved directory field"));
    }
    if read_u64(bytes, 16)? != supplied_len {
        return Err(invalid(
            "declared total length does not equal supplied bytes",
        ));
    }

    let actual_context = EntryDigestContext {
        attempt: CheckpointAttempt::new(read_u64(bytes, 24)?, read_u64(bytes, 32)?),
        assignment_version: read_u64(bytes, 40)?,
        partitioning_abi_version: read_u16(bytes, 48)?,
        vnode_count: read_u32(bytes, 52)?,
        vnode: read_u32(bytes, 56)?,
        assignment_certificate_sha256: read_array(bytes, 96)?,
    };
    let expected_digest_context = digest_context(expected);
    if actual_context != expected_digest_context {
        return Err(invalid(
            "directory provenance does not match expected context",
        ));
    }

    let entry_count = read_u32(bytes, 60)?;
    if entry_count == 0
        || entry_count > limits.directory_entries_per_artifact_max
        || usize::try_from(entry_count).ok() != Some(expected.roster.len())
    {
        return Err(invalid(
            "entry count is zero, over limit, or not the expected roster",
        ));
    }

    let directory_offset = read_u64(bytes, 64)?;
    let directory_len = read_u64(bytes, 72)?;
    let body_offset = read_u64(bytes, 80)?;
    let body_len = read_u64(bytes, 88)?;
    let expected_directory_len = u64::from(entry_count)
        .checked_mul(u64::from(ENTRY_LEN_U16))
        .ok_or_else(|| invalid("directory length arithmetic overflow"))?;
    let expected_body_offset = u64::try_from(HEADER_LEN)
        .map_err(|_| invalid("header length exceeds u64"))?
        .checked_add(expected_directory_len)
        .ok_or_else(|| invalid("body offset arithmetic overflow"))?;
    let expected_total = expected_body_offset
        .checked_add(body_len)
        .ok_or_else(|| invalid("body end arithmetic overflow"))?;
    if expected_body_offset > limits.envelope_metadata_bytes_max {
        return Err(invalid(
            "directory metadata exceeds its configured byte limit",
        ));
    }
    if directory_offset != u64::try_from(HEADER_LEN).unwrap_or(u64::MAX)
        || directory_len != expected_directory_len
        || body_offset != expected_body_offset
        || expected_total != supplied_len
    {
        return Err(invalid("directory and body ranges are not canonical"));
    }

    let directory = checked_slice(bytes, directory_offset, directory_len)?;
    if sha256(directory) != read_array::<SHA256_LEN>(bytes, 128)? {
        return Err(invalid("directory SHA-256 mismatch"));
    }

    let mut next_body_offset = body_offset;
    for (index, raw) in directory.chunks_exact(ENTRY_LEN).enumerate() {
        let roster = expected
            .roster
            .get(index)
            .ok_or_else(|| invalid("directory exceeds expected roster"))?;
        let entry = parse_raw_entry(raw)?;
        validate_entry(
            entry,
            *roster,
            actual_context,
            supplied_len,
            &mut next_body_offset,
        )?;
    }
    if next_body_offset != supplied_len {
        return Err(invalid("BODY ranges do not exactly cover the body region"));
    }
    for raw in directory.chunks_exact(ENTRY_LEN) {
        let entry = parse_raw_entry(raw)?;
        if entry.entry_kind == ENTRY_KIND_BODY {
            let body = checked_slice(bytes, entry.body_offset, entry.body_len)?;
            if sha256(body) != entry.body_sha256 {
                return Err(invalid("BODY entry SHA-256 mismatch"));
            }
        }
    }

    Ok(DecodedVnodePartialV2 {
        bytes,
        directory,
        digest_context: actual_context,
        entry_count,
    })
}

/// Encode one canonical V2 outer directory from caller-sorted entries.
///
/// BODY bytes are treated as opaque and must be produced by the selected managed-envelope writer.
pub(crate) fn encode(
    expected: ExpectedContext<'_>,
    entries: &[EncodeEntry<'_>],
    limits: VnodePartialV2Limits,
) -> Result<Vec<u8>, VnodePartialV2Error> {
    validate_expected_context(expected, limits)?;
    if entries.len() != expected.roster.len() {
        return Err(invalid(
            "encoder entries do not exactly cover the expected roster",
        ));
    }

    let entry_count =
        u32::try_from(entries.len()).map_err(|_| invalid("encoder entry count exceeds u32"))?;
    let directory_len = u64::from(entry_count)
        .checked_mul(u64::from(ENTRY_LEN_U16))
        .ok_or_else(|| invalid("encoder directory length overflow"))?;
    let body_offset = u64::try_from(HEADER_LEN)
        .map_err(|_| invalid("header length exceeds u64"))?
        .checked_add(directory_len)
        .ok_or_else(|| invalid("encoder body offset overflow"))?;
    if body_offset > limits.envelope_metadata_bytes_max {
        return Err(invalid(
            "directory metadata exceeds its configured byte limit",
        ));
    }
    let mut body_len = 0_u64;

    for (entry, roster) in entries.iter().zip(expected.roster) {
        if entry.operator_identity_sha256 != roster.operator_identity_sha256
            || entry.state_table_identity_sha256 != roster.state_table_identity_sha256
        {
            return Err(invalid("encoder entries are not in exact roster order"));
        }
        match entry.payload {
            EncodeEntryPayload::Body {
                artifact_kind,
                body,
                parent,
            } => {
                if body.is_empty() {
                    return Err(invalid("BODY bytes must be nonempty"));
                }
                validate_body_parent(artifact_kind, parent, expected.attempt)?;
                body_len = body_len
                    .checked_add(
                        u64::try_from(body.len())
                            .map_err(|_| invalid("BODY length exceeds u64"))?,
                    )
                    .ok_or_else(|| invalid("encoder body length overflow"))?;
            }
            EncodeEntryPayload::Reference { parent } => {
                validate_reference_parent(parent, expected.attempt)?;
            }
        }
    }

    let total_len = body_offset
        .checked_add(body_len)
        .ok_or_else(|| invalid("encoder total length overflow"))?;
    if total_len > limits.encoded_artifact_bytes_max {
        return Err(invalid(
            "encoded artifact exceeds its configured byte limit",
        ));
    }
    let total_len_usize =
        usize::try_from(total_len).map_err(|_| invalid("encoded artifact length exceeds usize"))?;
    let body_offset_usize =
        usize::try_from(body_offset).map_err(|_| invalid("encoder body offset exceeds usize"))?;

    let mut output = Vec::new();
    output
        .try_reserve_exact(total_len_usize)
        .map_err(|_| invalid("could not reserve encoded artifact bytes"))?;
    output.resize(HEADER_LEN, 0);

    let mut next_body_offset = body_offset;
    for (entry, roster) in entries.iter().zip(expected.roster) {
        let mut raw = [0_u8; ENTRY_LEN];
        put(&mut raw, 0, &entry.operator_identity_sha256)?;
        put(&mut raw, 32, &entry.state_table_identity_sha256)?;
        put(&mut raw, 64, &roster.vnode.to_be_bytes())?;
        put(&mut raw, 70, &roster.managed_envelope_version.to_be_bytes())?;

        match entry.payload {
            EncodeEntryPayload::Body {
                artifact_kind,
                body,
                parent,
            } => {
                raw[68] = ENTRY_KIND_BODY;
                raw[69] = artifact_kind.encoded();
                put(&mut raw, 72, &next_body_offset.to_be_bytes())?;
                let encoded_body_len =
                    u64::try_from(body.len()).map_err(|_| invalid("BODY length exceeds u64"))?;
                put(&mut raw, 80, &encoded_body_len.to_be_bytes())?;
                put(&mut raw, 88, &sha256(body))?;
                if let Some(parent) = parent {
                    put_parent(&mut raw, parent)?;
                }
                next_body_offset = next_body_offset
                    .checked_add(encoded_body_len)
                    .ok_or_else(|| invalid("encoder BODY offset overflow"))?;
            }
            EncodeEntryPayload::Reference { parent } => {
                raw[68] = ENTRY_KIND_REFERENCE;
                raw[69] = ARTIFACT_KIND_NONE;
                put_parent(&mut raw, parent)?;
            }
        }
        output.extend_from_slice(&raw);
    }
    for entry in entries {
        if let EncodeEntryPayload::Body { body, .. } = entry.payload {
            output.extend_from_slice(body);
        }
    }
    if output.len() != total_len_usize || next_body_offset != total_len {
        return Err(invalid("encoder produced a noncanonical total length"));
    }

    put(&mut output, 0, MAGIC)?;
    put(&mut output, 8, &FORMAT_VERSION.to_be_bytes())?;
    put(&mut output, 10, &HEADER_LEN_U16.to_be_bytes())?;
    put(&mut output, 12, &ENTRY_LEN_U16.to_be_bytes())?;
    put(&mut output, 16, &total_len.to_be_bytes())?;
    put(&mut output, 24, &expected.attempt.epoch.to_be_bytes())?;
    put(
        &mut output,
        32,
        &expected.attempt.checkpoint_id.to_be_bytes(),
    )?;
    put(&mut output, 40, &expected.assignment_version.to_be_bytes())?;
    put(
        &mut output,
        48,
        &expected.partitioning_abi_version.to_be_bytes(),
    )?;
    put(&mut output, 52, &expected.vnode_count.to_be_bytes())?;
    put(&mut output, 56, &expected.vnode.to_be_bytes())?;
    put(&mut output, 60, &entry_count.to_be_bytes())?;
    put(
        &mut output,
        64,
        &u64::try_from(HEADER_LEN)
            .map_err(|_| invalid("header length exceeds u64"))?
            .to_be_bytes(),
    )?;
    put(&mut output, 72, &directory_len.to_be_bytes())?;
    put(&mut output, 80, &body_offset.to_be_bytes())?;
    put(&mut output, 88, &body_len.to_be_bytes())?;
    put(&mut output, 96, &expected.assignment_certificate_sha256)?;
    let directory_sha256 = sha256(
        output
            .get(HEADER_LEN..body_offset_usize)
            .ok_or_else(|| invalid("encoder directory range is unavailable"))?,
    );
    put(&mut output, 128, &directory_sha256)?;
    Ok(output)
}

fn validate_expected_context(
    expected: ExpectedContext<'_>,
    limits: VnodePartialV2Limits,
) -> Result<(), VnodePartialV2Error> {
    if limits.encoded_artifact_bytes_max < u64::try_from(HEADER_LEN).unwrap_or(u64::MAX)
        || limits.envelope_metadata_bytes_max < u64::try_from(HEADER_LEN).unwrap_or(u64::MAX)
        || limits.directory_entries_per_artifact_max == 0
    {
        return Err(invalid("configured V2 limits cannot contain one directory"));
    }
    if !expected.attempt.is_canonical()
        || expected.assignment_version == 0
        || expected.partitioning_abi_version != PARTITIONING_ABI_VERSION
        || KeyGroupCount::try_from(expected.vnode_count).is_err()
        || expected.vnode >= expected.vnode_count
        || expected.assignment_certificate_sha256 == ZERO_SHA256
    {
        return Err(invalid("expected checkpoint context is not canonical"));
    }
    if expected.roster.is_empty()
        || expected.roster.len()
            > usize::try_from(limits.directory_entries_per_artifact_max).unwrap_or(usize::MAX)
        || u32::try_from(expected.roster.len()).is_err()
    {
        return Err(invalid(
            "expected roster is empty or over its configured limit",
        ));
    }
    for entry in expected.roster {
        if entry.operator_identity_sha256 == ZERO_SHA256
            || entry.state_table_identity_sha256 == ZERO_SHA256
            || entry.vnode != expected.vnode
            || entry.managed_envelope_version != MANAGED_ENVELOPE_VERSION
        {
            return Err(invalid("expected roster entry is not canonical"));
        }
    }
    if expected
        .roster
        .windows(2)
        .any(|pair| compare_roster(&pair[0], &pair[1]) != Ordering::Less)
    {
        return Err(invalid("expected roster is not strictly sorted and unique"));
    }
    Ok(())
}

fn validate_entry(
    entry: RawEntry,
    expected: ExpectedRosterEntry,
    context: EntryDigestContext,
    supplied_len: u64,
    next_body_offset: &mut u64,
) -> Result<(), VnodePartialV2Error> {
    if entry.operator_identity_sha256 != expected.operator_identity_sha256
        || entry.state_table_identity_sha256 != expected.state_table_identity_sha256
        || entry.vnode != expected.vnode
        || entry.managed_envelope_version != expected.managed_envelope_version
    {
        return Err(invalid(
            "directory entry does not match the expected roster",
        ));
    }
    match entry.entry_kind {
        ENTRY_KIND_BODY => {
            let artifact_kind = ArtifactKind::decode(entry.artifact_kind)?;
            if entry.body_len == 0
                || entry.body_offset != *next_body_offset
                || entry.body_sha256 == ZERO_SHA256
            {
                return Err(invalid("BODY range or digest is not canonical"));
            }
            let body_end = entry
                .body_offset
                .checked_add(entry.body_len)
                .ok_or_else(|| invalid("BODY range overflow"))?;
            if body_end > supplied_len {
                return Err(invalid("BODY range exceeds supplied bytes"));
            }
            let parent = raw_parent(entry);
            validate_body_parent(artifact_kind, parent, context.attempt)?;
            *next_body_offset = body_end;
        }
        ENTRY_KIND_REFERENCE => {
            if entry.artifact_kind != ARTIFACT_KIND_NONE
                || entry.body_offset != 0
                || entry.body_len != 0
                || entry.body_sha256 != ZERO_SHA256
            {
                return Err(invalid("REFERENCE carries BODY fields"));
            }
            let parent =
                raw_parent(entry).ok_or_else(|| invalid("REFERENCE has no canonical parent"))?;
            validate_reference_parent(parent, context.attempt)?;
        }
        _ => return Err(invalid("unknown directory entry kind")),
    }
    Ok(())
}

fn validate_body_parent(
    artifact_kind: ArtifactKind,
    parent: Option<ParentEntryLink>,
    current: CheckpointAttempt,
) -> Result<(), VnodePartialV2Error> {
    match artifact_kind {
        ArtifactKind::Full | ArtifactKind::Empty if parent.is_none() => Ok(()),
        ArtifactKind::Delta => {
            let parent = parent.ok_or_else(|| invalid("DELTA has no parent"))?;
            if parent.entry_sha256 == ZERO_SHA256
                || !parent.attempt.is_canonical()
                || parent.attempt.epoch.checked_add(1) != Some(current.epoch)
                || parent.attempt.checkpoint_id.checked_add(1) != Some(current.checkpoint_id)
            {
                return Err(invalid(
                    "DELTA parent is not the immediately preceding attempt",
                ));
            }
            Ok(())
        }
        ArtifactKind::Full | ArtifactKind::Empty => Err(invalid("FULL or EMPTY carries a parent")),
    }
}

fn validate_reference_parent(
    parent: ParentEntryLink,
    current: CheckpointAttempt,
) -> Result<(), VnodePartialV2Error> {
    if parent.entry_sha256 == ZERO_SHA256
        || !parent.attempt.is_canonical()
        || parent.attempt.epoch >= current.epoch
        || parent.attempt.checkpoint_id >= current.checkpoint_id
    {
        return Err(invalid(
            "REFERENCE parent is not a strictly older canonical attempt",
        ));
    }
    Ok(())
}

fn decode_outer_validated_entry<'a>(
    raw: &[u8],
    bytes: &'a [u8],
    context: EntryDigestContext,
) -> Result<DecodedEntry<'a>, VnodePartialV2Error> {
    let entry = parse_raw_entry(raw)?;
    let payload = match entry.entry_kind {
        ENTRY_KIND_BODY => {
            let artifact_kind = ArtifactKind::decode(entry.artifact_kind)?;
            DecodedEntryPayload::Body {
                artifact_kind,
                body: checked_slice(bytes, entry.body_offset, entry.body_len)?,
                body_sha256: entry.body_sha256,
                parent: raw_parent(entry),
            }
        }
        ENTRY_KIND_REFERENCE => DecodedEntryPayload::Reference {
            parent: raw_parent(entry)
                .ok_or_else(|| invalid("outer-validated REFERENCE lost its parent"))?,
        },
        _ => return Err(invalid("outer-validated entry has an unknown kind")),
    };
    Ok(DecodedEntry {
        operator_identity_sha256: entry.operator_identity_sha256,
        state_table_identity_sha256: entry.state_table_identity_sha256,
        vnode: entry.vnode,
        managed_envelope_version: entry.managed_envelope_version,
        contextual_sha256: contextual_entry_sha256(context, raw)?,
        payload,
    })
}

fn parse_raw_entry(raw: &[u8]) -> Result<RawEntry, VnodePartialV2Error> {
    if raw.len() != ENTRY_LEN {
        return Err(invalid("truncated directory entry"));
    }
    Ok(RawEntry {
        operator_identity_sha256: read_array(raw, 0)?,
        state_table_identity_sha256: read_array(raw, 32)?,
        vnode: read_u32(raw, 64)?,
        entry_kind: read_u8(raw, 68)?,
        artifact_kind: read_u8(raw, 69)?,
        managed_envelope_version: read_u16(raw, 70)?,
        body_offset: read_u64(raw, 72)?,
        body_len: read_u64(raw, 80)?,
        body_sha256: read_array(raw, 88)?,
        parent: CheckpointAttempt::new(read_u64(raw, 120)?, read_u64(raw, 128)?),
        parent_entry_sha256: read_array(raw, 136)?,
    })
}

fn raw_parent(entry: RawEntry) -> Option<ParentEntryLink> {
    if entry.parent == CheckpointAttempt::new(0, 0) && entry.parent_entry_sha256 == ZERO_SHA256 {
        None
    } else {
        Some(ParentEntryLink {
            attempt: entry.parent,
            entry_sha256: entry.parent_entry_sha256,
        })
    }
}

fn put_parent(raw: &mut [u8], parent: ParentEntryLink) -> Result<(), VnodePartialV2Error> {
    put(raw, 120, &parent.attempt.epoch.to_be_bytes())?;
    put(raw, 128, &parent.attempt.checkpoint_id.to_be_bytes())?;
    put(raw, 136, &parent.entry_sha256)
}

fn digest_context(expected: ExpectedContext<'_>) -> EntryDigestContext {
    EntryDigestContext {
        attempt: expected.attempt,
        assignment_version: expected.assignment_version,
        partitioning_abi_version: expected.partitioning_abi_version,
        vnode_count: expected.vnode_count,
        vnode: expected.vnode,
        assignment_certificate_sha256: expected.assignment_certificate_sha256,
    }
}

fn contextual_entry_sha256(
    context: EntryDigestContext,
    exact_entry_bytes: &[u8],
) -> Result<[u8; SHA256_LEN], VnodePartialV2Error> {
    if exact_entry_bytes.len() != ENTRY_LEN {
        return Err(invalid("contextual digest entry length is not 168 bytes"));
    }
    let mut digest = Sha256::new();
    digest.update(ENTRY_DIGEST_DOMAIN);
    digest.update(context.attempt.epoch.to_be_bytes());
    digest.update(context.attempt.checkpoint_id.to_be_bytes());
    digest.update(context.assignment_version.to_be_bytes());
    digest.update(context.partitioning_abi_version.to_be_bytes());
    digest.update(context.vnode_count.to_be_bytes());
    digest.update(context.vnode.to_be_bytes());
    digest.update(context.assignment_certificate_sha256);
    digest.update(exact_entry_bytes);
    Ok(digest.finalize().into())
}

fn compare_roster(left: &ExpectedRosterEntry, right: &ExpectedRosterEntry) -> Ordering {
    left.operator_identity_sha256
        .cmp(&right.operator_identity_sha256)
        .then_with(|| {
            left.state_table_identity_sha256
                .cmp(&right.state_table_identity_sha256)
        })
        .then_with(|| left.vnode.cmp(&right.vnode))
}

fn sha256(bytes: &[u8]) -> [u8; SHA256_LEN] {
    Sha256::digest(bytes).into()
}

fn checked_slice(bytes: &[u8], offset: u64, len: u64) -> Result<&[u8], VnodePartialV2Error> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| invalid("slice range overflow"))?;
    let start = usize::try_from(offset).map_err(|_| invalid("slice offset exceeds usize"))?;
    let end = usize::try_from(end).map_err(|_| invalid("slice end exceeds usize"))?;
    bytes
        .get(start..end)
        .ok_or_else(|| invalid("slice range exceeds supplied bytes"))
}

fn read_u8(bytes: &[u8], offset: usize) -> Result<u8, VnodePartialV2Error> {
    bytes
        .get(offset)
        .copied()
        .ok_or_else(|| invalid("truncated u8 field"))
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, VnodePartialV2Error> {
    Ok(u16::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, VnodePartialV2Error> {
    Ok(u32::from_be_bytes(read_array(bytes, offset)?))
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, VnodePartialV2Error> {
    Ok(u64::from_be_bytes(read_array(bytes, offset)?))
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], VnodePartialV2Error> {
    let end = offset
        .checked_add(N)
        .ok_or_else(|| invalid("field offset overflow"))?;
    bytes
        .get(offset..end)
        .and_then(|field| field.try_into().ok())
        .ok_or_else(|| invalid("truncated fixed-width field"))
}

fn put(target: &mut [u8], offset: usize, value: &[u8]) -> Result<(), VnodePartialV2Error> {
    let end = offset
        .checked_add(value.len())
        .ok_or_else(|| invalid("encoder field offset overflow"))?;
    let field = target
        .get_mut(offset..end)
        .ok_or_else(|| invalid("encoder field exceeds target buffer"))?;
    field.copy_from_slice(value);
    Ok(())
}

#[cfg(test)]
mod tests;

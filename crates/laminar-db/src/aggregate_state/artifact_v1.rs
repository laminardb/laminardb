//! Portable codec for the first managed grouped COUNT/SUM state.
//!
//! This module is intentionally unwired. A later manifest-selected restore path will consume it;
//! until then it must not change cluster admission or legacy checkpoint dispatch.

// Temporary reader-first compatibility seam. DKS-P1-001 owns removal of this allowance in the
// first trusted manifest-selected restore-composition commit, before any capability advertisement.
#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "DKS-P1-001: remove at trusted manifest-selected restore integration"
    )
)]

use std::num::NonZeroU32;

use laminar_core::state::{
    CheckpointAttempt, PartitionKeyCodecV1, PartitionKeySchemaV1, MAX_KEY_GROUP_COUNT,
    PARTITIONING_ABI_VERSION,
};
use sha2::{Digest, Sha256};

const CONTRACT_MAGIC: &[u8; 8] = b"LDBMAC\0\0";
const CONTRACT_VERSION: u16 = 1;
const CONTRACT_LEN: usize = 64;
const ARTIFACT_MAGIC: &[u8; 8] = b"LDBMGA\0\0";
const ARTIFACT_VERSION: u16 = 1;
const ARTIFACT_HEADER_LEN: usize = 384;
const STATE_CODEC_ID: u32 = 1;
const STATE_CODEC_VERSION: u16 = 1;
const KEY_MODE_VNODE_KEYED: u8 = 1;
pub(super) const STATE_WIDTH: usize = 24;
const MAX_SQL_COUNT: u64 = i64::MAX.unsigned_abs();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum ArtifactKind {
    Full = 1,
    Delta = 2,
    Empty = 3,
}

impl ArtifactKind {
    fn from_byte(value: u8) -> Result<Self, ArtifactError> {
        match value {
            1 => Ok(Self::Full),
            2 => Ok(Self::Delta),
            3 => Ok(Self::Empty),
            _ => Err(ArtifactError::Invalid("artifact kind")),
        }
    }
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub(super) enum ArtifactError {
    #[error("managed aggregate artifact is truncated")]
    Truncated,
    #[error("managed aggregate artifact has invalid {0}")]
    Invalid(&'static str),
    #[error("managed aggregate artifact exceeds {0}")]
    Limit(&'static str),
    #[error("managed aggregate artifact arithmetic overflow")]
    ArithmeticOverflow,
    #[error("managed aggregate artifact allocation failed")]
    Allocation,
    #[error("COUNT(*) overflow")]
    CountOverflow,
    #[error("SUM(Int64) overflow")]
    SumOverflow,
}

/// Canonical working and persisted state for codec 1.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct CountSumStateV1 {
    count: u64,
    sum_non_null_count: u64,
    sum: i64,
}

impl CountSumStateV1 {
    pub(super) const fn empty() -> Self {
        Self {
            count: 0,
            sum_non_null_count: 0,
            sum: 0,
        }
    }

    pub(super) fn persisted(
        count: u64,
        sum_non_null_count: u64,
        sum: i64,
    ) -> Result<Self, ArtifactError> {
        let state = Self {
            count,
            sum_non_null_count,
            sum,
        };
        state.validate_persisted()?;
        Ok(state)
    }

    pub(super) const fn count(self) -> u64 {
        self.count
    }

    pub(super) const fn sum_non_null_count(self) -> u64 {
        self.sum_non_null_count
    }

    pub(super) const fn sum(self) -> Option<i64> {
        if self.sum_non_null_count == 0 {
            None
        } else {
            Some(self.sum)
        }
    }

    /// Preview an append in source order without mutating the current state.
    pub(super) fn preview_append(self, values: &[Option<i64>]) -> Result<Self, ArtifactError> {
        let mut candidate = self;
        candidate.validate_working()?;
        for value in values {
            candidate.count = candidate
                .count
                .checked_add(1)
                .filter(|count| i64::try_from(*count).is_ok())
                .ok_or(ArtifactError::CountOverflow)?;
            if let Some(value) = value {
                candidate.sum_non_null_count = candidate
                    .sum_non_null_count
                    .checked_add(1)
                    .ok_or(ArtifactError::ArithmeticOverflow)?;
                candidate.sum = candidate
                    .sum
                    .checked_add(*value)
                    .ok_or(ArtifactError::SumOverflow)?;
            }
        }
        Ok(candidate)
    }

    fn validate_working(self) -> Result<(), ArtifactError> {
        if self.count > MAX_SQL_COUNT {
            return Err(ArtifactError::Invalid("COUNT(*) state"));
        }
        if self.sum_non_null_count > self.count {
            return Err(ArtifactError::Invalid("SUM non-null count"));
        }
        if self.sum_non_null_count == 0 && self.sum != 0 {
            return Err(ArtifactError::Invalid("null SUM accumulator"));
        }
        Ok(())
    }

    fn validate_persisted(self) -> Result<(), ArtifactError> {
        self.validate_working()?;
        if self.count == 0 {
            return Err(ArtifactError::Invalid("zero persisted COUNT(*)"));
        }
        Ok(())
    }

    fn encode(self) -> Result<[u8; STATE_WIDTH], ArtifactError> {
        self.validate_persisted()?;
        let mut bytes = [0; STATE_WIDTH];
        bytes[0..8].copy_from_slice(&self.count.to_be_bytes());
        bytes[8..16].copy_from_slice(&self.sum_non_null_count.to_be_bytes());
        bytes[16..24].copy_from_slice(&self.sum.to_be_bytes());
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> Result<Self, ArtifactError> {
        if bytes.len() != STATE_WIDTH {
            return Err(ArtifactError::Truncated);
        }
        Self::persisted(
            u64::from_be_bytes(field(bytes, 0)?),
            u64::from_be_bytes(field(bytes, 8)?),
            i64::from_be_bytes(field(bytes, 16)?),
        )
    }
}

/// Exact 64-byte semantic contract for codec 1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AggregateContractV1 {
    sum_input_nullable: bool,
    routing_schema_sha256: [u8; 32],
}

impl AggregateContractV1 {
    pub(super) fn new(routing_schema: &PartitionKeySchemaV1, sum_input_nullable: bool) -> Self {
        Self {
            sum_input_nullable,
            routing_schema_sha256: routing_schema.sha256(),
        }
    }

    pub(super) fn matches_routing_schema(self, routing_schema: &PartitionKeySchemaV1) -> bool {
        self.routing_schema_sha256 == routing_schema.sha256()
    }

    fn encode(self) -> [u8; CONTRACT_LEN] {
        let mut bytes = [0; CONTRACT_LEN];
        bytes[0..8].copy_from_slice(CONTRACT_MAGIC);
        bytes[8..10].copy_from_slice(&CONTRACT_VERSION.to_be_bytes());
        bytes[10..12].copy_from_slice(&64_u16.to_be_bytes());
        bytes[12..16].copy_from_slice(&STATE_CODEC_ID.to_be_bytes());
        bytes[16..18].copy_from_slice(&STATE_CODEC_VERSION.to_be_bytes());
        bytes[18..20].copy_from_slice(&PARTITIONING_ABI_VERSION.to_be_bytes());
        bytes[20] = KEY_MODE_VNODE_KEYED;
        bytes[21] = 1; // APPEND_ONLY
        bytes[22] = 1; // COUNT_STAR
        bytes[23] = 0; // reserved
        bytes[24] = 1; // SUM_INT64
        bytes[25] = u8::from(self.sum_input_nullable);
        bytes[26] = 1; // INT64
        bytes[27] = 0; // COUNT output is non-null
        bytes[28] = 1; // INT64
        bytes[29] = 1; // SUM output is nullable
        bytes[30..32].copy_from_slice(&24_u16.to_be_bytes());
        bytes[32..64].copy_from_slice(&self.routing_schema_sha256);
        bytes
    }

    pub(super) fn validate_state(self, state: CountSumStateV1) -> Result<(), ArtifactError> {
        state.validate_persisted()?;
        if !self.sum_input_nullable && state.sum_non_null_count != state.count {
            return Err(ArtifactError::Invalid("non-null SUM count"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ParentLink {
    attempt: CheckpointAttempt,
    entry_sha256: [u8; 32],
}

impl ParentLink {
    pub(super) const fn new(attempt: CheckpointAttempt, entry_sha256: [u8; 32]) -> Self {
        Self {
            attempt,
            entry_sha256,
        }
    }
}

/// Trusted, plan-owned context supplied out of band to the decoder.
#[derive(Clone, Copy, Debug)]
pub(super) struct ArtifactContext<'a> {
    pub(super) kind: ArtifactKind,
    pub(super) attempt: CheckpointAttempt,
    pub(super) parent: Option<ParentLink>,
    pub(super) assignment_version: u64,
    pub(super) assignment_certificate_sha256: [u8; 32],
    pub(super) operator_identity_sha256: [u8; 32],
    pub(super) state_table_identity_sha256: [u8; 32],
    pub(super) vnode_count: NonZeroU32,
    pub(super) vnode: u32,
    pub(super) routing_schema: &'a PartitionKeySchemaV1,
    pub(super) contract: AggregateContractV1,
}

/// Monotonic per-V2-object budget plus fixed per-envelope limits.
///
/// Every successfully encoded or decoded BODY consumes this same non-`Copy` ledger. Failed
/// operations leave it unchanged.
#[derive(Debug, Eq, PartialEq)]
#[cfg_attr(test, derive(Clone))]
pub(super) struct AggregateObjectBudget {
    pub(super) envelope_metadata_bytes_max: u64,
    pub(super) routing_schema_bytes_max: u64,
    pub(super) state_contract_bytes_max: u64,
    pub(super) encoded_key_bytes_max: u64,
    pub(super) stored_state_bytes_max: u64,
    pub(super) remaining_artifact_bytes: u64,
    pub(super) remaining_rows: u64,
    pub(super) remaining_key_bytes: u64,
    pub(super) remaining_state_bytes: u64,
}

impl AggregateObjectBudget {
    fn charge(
        &mut self,
        artifact_bytes: u64,
        rows: u64,
        key_bytes: u64,
        state_bytes: u64,
    ) -> Result<(), ArtifactError> {
        let remaining_artifact_bytes = self
            .remaining_artifact_bytes
            .checked_sub(artifact_bytes)
            .ok_or(ArtifactError::Limit("remaining artifact byte limit"))?;
        let remaining_rows = self
            .remaining_rows
            .checked_sub(rows)
            .ok_or(ArtifactError::Limit("remaining row limit"))?;
        let remaining_key_bytes = self
            .remaining_key_bytes
            .checked_sub(key_bytes)
            .ok_or(ArtifactError::Limit("remaining key byte limit"))?;
        let remaining_state_bytes = self
            .remaining_state_bytes
            .checked_sub(state_bytes)
            .ok_or(ArtifactError::Limit("remaining state byte limit"))?;

        self.remaining_artifact_bytes = remaining_artifact_bytes;
        self.remaining_rows = remaining_rows;
        self.remaining_key_bytes = remaining_key_bytes;
        self.remaining_state_bytes = remaining_state_bytes;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AggregateRow<'a> {
    pub(super) key: &'a [u8],
    pub(super) state: CountSumStateV1,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct DecodedArtifact<'a> {
    rows: &'a [u8],
    row_count: u64,
    key_bytes: u64,
    state_bytes: u64,
}

impl<'a> DecodedArtifact<'a> {
    pub(super) const fn row_count(&self) -> u64 {
        self.row_count
    }

    pub(super) const fn key_bytes(&self) -> u64 {
        self.key_bytes
    }

    pub(super) const fn state_bytes(&self) -> u64 {
        self.state_bytes
    }

    pub(super) const fn rows(&self) -> DecodedRows<'a> {
        DecodedRows {
            bytes: self.rows,
            offset: 0,
        }
    }
}

pub(super) struct DecodedRows<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Iterator for DecodedRows<'a> {
    type Item = Result<AggregateRow<'a>, ArtifactError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.bytes.len() {
            return None;
        }
        let result = (|| {
            let key_len = usize::try_from(u32::from_be_bytes(field(self.bytes, self.offset)?))
                .map_err(|_| ArtifactError::ArithmeticOverflow)?;
            let key_start = self
                .offset
                .checked_add(4)
                .ok_or(ArtifactError::ArithmeticOverflow)?;
            let key_end = key_start
                .checked_add(key_len)
                .ok_or(ArtifactError::ArithmeticOverflow)?;
            let state_end = key_end
                .checked_add(STATE_WIDTH)
                .ok_or(ArtifactError::ArithmeticOverflow)?;
            let key = self
                .bytes
                .get(key_start..key_end)
                .ok_or(ArtifactError::Truncated)?;
            let state = CountSumStateV1::decode(
                self.bytes
                    .get(key_end..state_end)
                    .ok_or(ArtifactError::Truncated)?,
            )?;
            self.offset = state_end;
            Ok(AggregateRow { key, state })
        })();
        if result.is_err() {
            self.offset = self.bytes.len();
        }
        Some(result)
    }
}

pub(super) fn encode(
    context: ArtifactContext<'_>,
    rows: &[AggregateRow<'_>],
    budget: &mut AggregateObjectBudget,
) -> Result<Vec<u8>, ArtifactError> {
    validate_context(context)?;
    validate_fixed_limits(budget)?;

    let routing = context.routing_schema.as_bytes();
    let contract = context.contract.encode();
    let routing_len = as_u64(routing.len())?;
    let contract_len = as_u64(contract.len())?;
    if routing_len > budget.routing_schema_bytes_max {
        return Err(ArtifactError::Limit("routing schema byte limit"));
    }
    if contract_len > budget.state_contract_bytes_max {
        return Err(ArtifactError::Limit("state contract byte limit"));
    }
    let metadata_len = as_u64(ARTIFACT_HEADER_LEN)?
        .checked_add(routing_len)
        .and_then(|value| value.checked_add(contract_len))
        .ok_or(ArtifactError::ArithmeticOverflow)?;
    if metadata_len > budget.envelope_metadata_bytes_max {
        return Err(ArtifactError::Limit("envelope metadata byte limit"));
    }

    let row_count = as_u64(rows.len())?;
    match context.kind {
        ArtifactKind::Empty if !rows.is_empty() => {
            return Err(ArtifactError::Invalid("EMPTY rows"));
        }
        ArtifactKind::Full | ArtifactKind::Delta if rows.is_empty() => {
            return Err(ArtifactError::Invalid("zero-row FULL/DELTA"));
        }
        _ => {}
    }
    if row_count > budget.remaining_rows {
        return Err(ArtifactError::Limit("remaining row limit"));
    }

    let mut key_bytes = 0_u64;
    let mut rows_len = 0_u64;
    let mut previous_key: Option<&[u8]> = None;
    for row in rows {
        let key_len = as_u64(row.key.len())?;
        if key_len > u64::from(u32::MAX) || key_len > budget.encoded_key_bytes_max {
            return Err(ArtifactError::Limit("encoded key byte limit"));
        }
        if previous_key.is_some_and(|previous| previous >= row.key) {
            return Err(ArtifactError::Invalid("row key order"));
        }
        if PartitionKeyCodecV1::vnode_for_encoded(row.key, context.vnode_count) != context.vnode {
            return Err(ArtifactError::Invalid("row vnode"));
        }
        context.contract.validate_state(row.state)?;
        previous_key = Some(row.key);
        key_bytes = key_bytes
            .checked_add(key_len)
            .ok_or(ArtifactError::ArithmeticOverflow)?;
        rows_len = rows_len
            .checked_add(4)
            .and_then(|value| value.checked_add(key_len))
            .and_then(|value| value.checked_add(24_u64))
            .ok_or(ArtifactError::ArithmeticOverflow)?;
    }
    let state_bytes = row_count
        .checked_mul(24_u64)
        .ok_or(ArtifactError::ArithmeticOverflow)?;
    validate_totals(key_bytes, state_bytes, budget)?;

    let total_len = metadata_len
        .checked_add(rows_len)
        .ok_or(ArtifactError::ArithmeticOverflow)?;
    if total_len > budget.remaining_artifact_bytes {
        return Err(ArtifactError::Limit("remaining artifact byte limit"));
    }
    let total_len_usize = usize::try_from(total_len).map_err(|_| ArtifactError::Allocation)?;
    let routing_offset = 384_u64;
    let contract_offset = routing_offset
        .checked_add(routing_len)
        .ok_or(ArtifactError::ArithmeticOverflow)?;
    let rows_offset = contract_offset
        .checked_add(contract_len)
        .ok_or(ArtifactError::ArithmeticOverflow)?;

    let mut output = Vec::new();
    output
        .try_reserve_exact(total_len_usize)
        .map_err(|_| ArtifactError::Allocation)?;
    output.resize(ARTIFACT_HEADER_LEN, 0);
    output.extend_from_slice(routing);
    output.extend_from_slice(&contract);
    for row in rows {
        let key_len = u32::try_from(row.key.len())
            .map_err(|_| ArtifactError::Limit("encoded key byte limit"))?;
        output.extend_from_slice(&key_len.to_be_bytes());
        output.extend_from_slice(row.key);
        output.extend_from_slice(&row.state.encode()?);
    }
    if output.len() != total_len_usize {
        return Err(ArtifactError::ArithmeticOverflow);
    }

    let rows_offset_usize = usize::try_from(rows_offset).map_err(|_| ArtifactError::Allocation)?;
    let rows_digest = sha256(
        output
            .get(rows_offset_usize..)
            .ok_or(ArtifactError::ArithmeticOverflow)?,
    );
    write_header(
        &mut output,
        context,
        total_len,
        row_count,
        key_bytes,
        state_bytes,
        routing_offset,
        routing_len,
        contract_offset,
        rows_offset,
        rows_len,
        sha256(routing),
        sha256(&contract),
        rows_digest,
    )?;
    budget.charge(total_len, row_count, key_bytes, state_bytes)?;
    Ok(output)
}

#[allow(clippy::too_many_arguments)]
fn write_header(
    output: &mut [u8],
    context: ArtifactContext<'_>,
    total_len: u64,
    row_count: u64,
    key_bytes: u64,
    state_bytes: u64,
    routing_offset: u64,
    routing_len: u64,
    contract_offset: u64,
    rows_offset: u64,
    rows_len: u64,
    routing_digest: [u8; 32],
    contract_digest: [u8; 32],
    rows_digest: [u8; 32],
) -> Result<(), ArtifactError> {
    put(output, 0, ARTIFACT_MAGIC)?;
    put(output, 8, &ARTIFACT_VERSION.to_be_bytes())?;
    put(output, 10, &384_u16.to_be_bytes())?;
    put(output, 12, &[context.kind as u8])?;
    put(output, 13, &[KEY_MODE_VNODE_KEYED])?;
    put(output, 14, &0_u16.to_be_bytes())?;
    put(output, 16, &total_len.to_be_bytes())?;
    put(output, 24, &context.attempt.epoch.to_be_bytes())?;
    put(output, 32, &context.attempt.checkpoint_id.to_be_bytes())?;
    let (parent_attempt, parent_digest) = context
        .parent
        .map_or((CheckpointAttempt::new(0, 0), [0; 32]), |parent| {
            (parent.attempt, parent.entry_sha256)
        });
    put(output, 40, &parent_attempt.epoch.to_be_bytes())?;
    put(output, 48, &parent_attempt.checkpoint_id.to_be_bytes())?;
    put(output, 56, &context.assignment_version.to_be_bytes())?;
    put(output, 64, &PARTITIONING_ABI_VERSION.to_be_bytes())?;
    put(output, 66, &STATE_CODEC_VERSION.to_be_bytes())?;
    put(output, 68, &STATE_CODEC_ID.to_be_bytes())?;
    put(output, 72, &context.vnode_count.get().to_be_bytes())?;
    put(output, 76, &context.vnode.to_be_bytes())?;
    put(output, 80, &row_count.to_be_bytes())?;
    put(output, 88, &key_bytes.to_be_bytes())?;
    put(output, 96, &state_bytes.to_be_bytes())?;
    put(output, 104, &24_u32.to_be_bytes())?;
    put(output, 108, &0_u32.to_be_bytes())?;
    put(output, 112, &routing_offset.to_be_bytes())?;
    put(output, 120, &routing_len.to_be_bytes())?;
    put(output, 128, &contract_offset.to_be_bytes())?;
    put(output, 136, &64_u64.to_be_bytes())?;
    put(output, 144, &rows_offset.to_be_bytes())?;
    put(output, 152, &rows_len.to_be_bytes())?;
    put(output, 160, &context.assignment_certificate_sha256)?;
    put(output, 192, &context.operator_identity_sha256)?;
    put(output, 224, &context.state_table_identity_sha256)?;
    put(output, 256, &routing_digest)?;
    put(output, 288, &contract_digest)?;
    put(output, 320, &rows_digest)?;
    put(output, 352, &parent_digest)?;
    Ok(())
}

pub(super) fn decode<'a>(
    bytes: &'a [u8],
    expected: ArtifactContext<'_>,
    budget: &mut AggregateObjectBudget,
) -> Result<DecodedArtifact<'a>, ArtifactError> {
    validate_context(expected)?;
    validate_fixed_limits(budget)?;
    if as_u64(bytes.len())? > budget.remaining_artifact_bytes {
        return Err(ArtifactError::Limit("remaining artifact byte limit"));
    }
    if bytes.len() < ARTIFACT_HEADER_LEN {
        return Err(ArtifactError::Truncated);
    }
    if bytes.get(0..8) != Some(ARTIFACT_MAGIC.as_slice()) {
        return Err(ArtifactError::Invalid("magic"));
    }
    if u16::from_be_bytes(field(bytes, 8)?) != ARTIFACT_VERSION {
        return Err(ArtifactError::Invalid("version"));
    }
    if usize::from(u16::from_be_bytes(field(bytes, 10)?)) != ARTIFACT_HEADER_LEN {
        return Err(ArtifactError::Invalid("header length"));
    }
    if ArtifactKind::from_byte(field::<1>(bytes, 12)?[0])? != expected.kind {
        return Err(ArtifactError::Invalid("expected artifact kind"));
    }
    if field::<1>(bytes, 13)?[0] != KEY_MODE_VNODE_KEYED {
        return Err(ArtifactError::Invalid("key mode"));
    }
    if u16::from_be_bytes(field(bytes, 14)?) != 0 {
        return Err(ArtifactError::Invalid("flags/reserved field"));
    }
    let total_len = u64::from_be_bytes(field(bytes, 16)?);
    if total_len != as_u64(bytes.len())? {
        return Err(ArtifactError::Invalid("total length"));
    }

    let attempt = CheckpointAttempt::new(
        u64::from_be_bytes(field(bytes, 24)?),
        u64::from_be_bytes(field(bytes, 32)?),
    );
    let parent_attempt = CheckpointAttempt::new(
        u64::from_be_bytes(field(bytes, 40)?),
        u64::from_be_bytes(field(bytes, 48)?),
    );
    let parent_digest = field::<32>(bytes, 352)?;
    let expected_parent = expected
        .parent
        .map_or((CheckpointAttempt::new(0, 0), [0; 32]), |parent| {
            (parent.attempt, parent.entry_sha256)
        });
    if attempt != expected.attempt
        || parent_attempt != expected_parent.0
        || parent_digest != expected_parent.1
    {
        return Err(ArtifactError::Invalid("attempt/parent context"));
    }
    if u64::from_be_bytes(field(bytes, 56)?) != expected.assignment_version
        || u16::from_be_bytes(field(bytes, 64)?) != PARTITIONING_ABI_VERSION
        || u16::from_be_bytes(field(bytes, 66)?) != STATE_CODEC_VERSION
        || u32::from_be_bytes(field(bytes, 68)?) != STATE_CODEC_ID
        || u32::from_be_bytes(field(bytes, 72)?) != expected.vnode_count.get()
        || u32::from_be_bytes(field(bytes, 76)?) != expected.vnode
    {
        return Err(ArtifactError::Invalid("codec/routing context"));
    }
    if field::<32>(bytes, 160)? != expected.assignment_certificate_sha256
        || field::<32>(bytes, 192)? != expected.operator_identity_sha256
        || field::<32>(bytes, 224)? != expected.state_table_identity_sha256
    {
        return Err(ArtifactError::Invalid("identity context"));
    }
    if u32::from_be_bytes(field(bytes, 104)?) != 24_u32
        || u32::from_be_bytes(field(bytes, 108)?) != 0
    {
        return Err(ArtifactError::Invalid("state width/reserved field"));
    }

    let row_count = u64::from_be_bytes(field(bytes, 80)?);
    let key_bytes = u64::from_be_bytes(field(bytes, 88)?);
    let state_bytes = u64::from_be_bytes(field(bytes, 96)?);
    let routing_offset = u64::from_be_bytes(field(bytes, 112)?);
    let routing_len = u64::from_be_bytes(field(bytes, 120)?);
    let contract_offset = u64::from_be_bytes(field(bytes, 128)?);
    let contract_len = u64::from_be_bytes(field(bytes, 136)?);
    let rows_offset = u64::from_be_bytes(field(bytes, 144)?);
    let rows_len = u64::from_be_bytes(field(bytes, 152)?);

    if routing_offset != 384_u64
        || contract_offset
            != routing_offset
                .checked_add(routing_len)
                .ok_or(ArtifactError::ArithmeticOverflow)?
        || rows_offset
            != contract_offset
                .checked_add(contract_len)
                .ok_or(ArtifactError::ArithmeticOverflow)?
        || total_len
            != rows_offset
                .checked_add(rows_len)
                .ok_or(ArtifactError::ArithmeticOverflow)?
        || contract_len != 64_u64
    {
        return Err(ArtifactError::Invalid("section layout"));
    }
    if routing_len > budget.routing_schema_bytes_max {
        return Err(ArtifactError::Limit("routing schema byte limit"));
    }
    if contract_len > budget.state_contract_bytes_max {
        return Err(ArtifactError::Limit("state contract byte limit"));
    }
    if rows_offset > budget.envelope_metadata_bytes_max {
        return Err(ArtifactError::Limit("envelope metadata byte limit"));
    }
    if row_count > budget.remaining_rows {
        return Err(ArtifactError::Limit("remaining row limit"));
    }
    validate_totals(key_bytes, state_bytes, budget)?;
    if state_bytes
        != row_count
            .checked_mul(24_u64)
            .ok_or(ArtifactError::ArithmeticOverflow)?
    {
        return Err(ArtifactError::Invalid("state byte total"));
    }

    let routing = checked_section(bytes, routing_offset, routing_len)?;
    let contract = checked_section(bytes, contract_offset, contract_len)?;
    let rows = checked_section(bytes, rows_offset, rows_len)?;
    if routing != expected.routing_schema.as_bytes()
        || field::<32>(bytes, 256)? != sha256(routing)
        || field::<32>(bytes, 256)? != expected.routing_schema.sha256()
    {
        return Err(ArtifactError::Invalid("routing schema"));
    }
    let expected_contract = expected.contract.encode();
    if contract != expected_contract
        || field::<32>(bytes, 288)? != sha256(contract)
        || field::<32>(bytes, 320)? != sha256(rows)
    {
        return Err(ArtifactError::Invalid("contract/payload digest"));
    }

    match expected.kind {
        ArtifactKind::Empty => {
            if row_count != 0 || key_bytes != 0 || state_bytes != 0 || !rows.is_empty() {
                return Err(ArtifactError::Invalid("EMPTY payload"));
            }
        }
        ArtifactKind::Full | ArtifactKind::Delta if row_count == 0 => {
            return Err(ArtifactError::Invalid("zero-row FULL/DELTA"));
        }
        ArtifactKind::Full | ArtifactKind::Delta => {}
    }

    validate_rows(rows, row_count, key_bytes, expected, budget)?;
    budget.charge(total_len, row_count, key_bytes, state_bytes)?;
    Ok(DecodedArtifact {
        rows,
        row_count,
        key_bytes,
        state_bytes,
    })
}

fn validate_rows(
    rows: &[u8],
    expected_rows: u64,
    expected_key_bytes: u64,
    context: ArtifactContext<'_>,
    budget: &AggregateObjectBudget,
) -> Result<(), ArtifactError> {
    let mut offset = 0_usize;
    let mut row_count = 0_u64;
    let mut key_bytes = 0_u64;
    let mut previous_key: Option<&[u8]> = None;
    while offset < rows.len() {
        let key_len = usize::try_from(u32::from_be_bytes(field(rows, offset)?))
            .map_err(|_| ArtifactError::ArithmeticOverflow)?;
        let key_len_u64 = as_u64(key_len)?;
        if key_len_u64 > budget.encoded_key_bytes_max {
            return Err(ArtifactError::Limit("encoded key byte limit"));
        }
        let key_start = offset
            .checked_add(4)
            .ok_or(ArtifactError::ArithmeticOverflow)?;
        let key_end = key_start
            .checked_add(key_len)
            .ok_or(ArtifactError::ArithmeticOverflow)?;
        let state_end = key_end
            .checked_add(STATE_WIDTH)
            .ok_or(ArtifactError::ArithmeticOverflow)?;
        let key = rows
            .get(key_start..key_end)
            .ok_or(ArtifactError::Truncated)?;
        let state = rows
            .get(key_end..state_end)
            .ok_or(ArtifactError::Truncated)?;
        if previous_key.is_some_and(|previous| previous >= key) {
            return Err(ArtifactError::Invalid("row key order"));
        }
        if PartitionKeyCodecV1::vnode_for_encoded(key, context.vnode_count) != context.vnode {
            return Err(ArtifactError::Invalid("row vnode"));
        }
        context
            .contract
            .validate_state(CountSumStateV1::decode(state)?)?;
        previous_key = Some(key);
        offset = state_end;
        row_count = row_count
            .checked_add(1)
            .ok_or(ArtifactError::ArithmeticOverflow)?;
        key_bytes = key_bytes
            .checked_add(key_len_u64)
            .ok_or(ArtifactError::ArithmeticOverflow)?;
    }
    if offset != rows.len() || row_count != expected_rows || key_bytes != expected_key_bytes {
        return Err(ArtifactError::Invalid("row/key totals"));
    }
    Ok(())
}

fn validate_context(context: ArtifactContext<'_>) -> Result<(), ArtifactError> {
    if !context.attempt.is_canonical()
        || context.assignment_version == 0
        || context.assignment_certificate_sha256 == [0; 32]
        || context.operator_identity_sha256 == [0; 32]
        || context.state_table_identity_sha256 == [0; 32]
        || context.vnode_count.get() > MAX_KEY_GROUP_COUNT
        || context.vnode >= context.vnode_count.get()
        || context.routing_schema.as_bytes().is_empty()
        || context.contract.routing_schema_sha256 != context.routing_schema.sha256()
    {
        return Err(ArtifactError::Invalid("expected context"));
    }
    match (context.kind, context.parent) {
        (ArtifactKind::Delta, Some(parent))
            if parent.attempt.is_canonical()
                && parent.entry_sha256 != [0; 32]
                && parent.attempt.epoch.checked_add(1) == Some(context.attempt.epoch)
                && parent.attempt.checkpoint_id.checked_add(1)
                    == Some(context.attempt.checkpoint_id) =>
        {
            Ok(())
        }
        (ArtifactKind::Full | ArtifactKind::Empty, None) => Ok(()),
        _ => Err(ArtifactError::Invalid("expected parent context")),
    }
}

fn validate_fixed_limits(budget: &AggregateObjectBudget) -> Result<(), ArtifactError> {
    if 24_u64 > budget.stored_state_bytes_max {
        return Err(ArtifactError::Limit("stored state byte limit"));
    }
    Ok(())
}

fn validate_totals(
    key_bytes: u64,
    state_bytes: u64,
    budget: &AggregateObjectBudget,
) -> Result<(), ArtifactError> {
    if key_bytes > budget.remaining_key_bytes {
        return Err(ArtifactError::Limit("remaining key byte limit"));
    }
    if state_bytes > budget.remaining_state_bytes {
        return Err(ArtifactError::Limit("remaining state byte limit"));
    }
    Ok(())
}

fn checked_section(bytes: &[u8], offset: u64, len: u64) -> Result<&[u8], ArtifactError> {
    let end = offset
        .checked_add(len)
        .ok_or(ArtifactError::ArithmeticOverflow)?;
    let start = usize::try_from(offset).map_err(|_| ArtifactError::Truncated)?;
    let end = usize::try_from(end).map_err(|_| ArtifactError::Truncated)?;
    bytes.get(start..end).ok_or(ArtifactError::Truncated)
}

fn field<const N: usize>(bytes: &[u8], start: usize) -> Result<[u8; N], ArtifactError> {
    let end = start
        .checked_add(N)
        .ok_or(ArtifactError::ArithmeticOverflow)?;
    bytes
        .get(start..end)
        .and_then(|value| value.try_into().ok())
        .ok_or(ArtifactError::Truncated)
}

fn put(output: &mut [u8], start: usize, value: &[u8]) -> Result<(), ArtifactError> {
    let end = start
        .checked_add(value.len())
        .ok_or(ArtifactError::ArithmeticOverflow)?;
    output
        .get_mut(start..end)
        .ok_or(ArtifactError::Truncated)?
        .copy_from_slice(value);
    Ok(())
}

fn as_u64(value: usize) -> Result<u64, ArtifactError> {
    u64::try_from(value).map_err(|_| ArtifactError::ArithmeticOverflow)
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[cfg(test)]
mod tests;

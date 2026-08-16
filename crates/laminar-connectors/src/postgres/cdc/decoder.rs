//! `PostgreSQL` `pgoutput` logical replication protocol decoder.
//!
//! Implements a binary protocol parser for the `pgoutput` output plugin
//! used by `PostgreSQL` logical replication (PG 10+). Parses WAL stream
//! bytes into structured [`WalMessage`] variants.
//!
//! # Protocol Reference
//!
//! See `PostgreSQL` docs: "Logical Replication Message Formats"
//! (<https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html>)

use super::lsn::Lsn;
use super::types::PgColumn;
use bytes::Bytes;

/// Offset from `PostgreSQL` epoch (2000-01-01) to Unix epoch (1970-01-01)
/// in microseconds.
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;
const MAX_POSTGRES_COLUMNS: usize = 1_600;

/// A decoded WAL message from the `pgoutput` protocol.
#[derive(Debug, Clone, PartialEq)]
pub enum WalMessage {
    /// Transaction begin.
    Begin(BeginMessage),
    /// Transaction commit.
    Commit(CommitMessage),
    /// Relation (table) metadata.
    Relation(RelationMessage),
    /// Row inserted.
    Insert(InsertMessage),
    /// Row updated.
    Update(UpdateMessage),
    /// Row deleted.
    Delete(DeleteMessage),
    /// Table(s) truncated.
    Truncate(TruncateMessage),
    /// Origin information.
    Origin(OriginMessage),
    /// Custom type definition.
    Type(TypeMessage),
}

/// Transaction begin message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginMessage {
    /// LSN of the final record of the transaction.
    pub final_lsn: Lsn,
    /// Commit timestamp in milliseconds since Unix epoch.
    pub commit_ts_ms: i64,
    /// Transaction ID (XID).
    pub xid: u32,
}

/// Transaction commit message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitMessage {
    /// Flags (currently unused by `PostgreSQL`).
    pub flags: u8,
    /// LSN of the commit record.
    pub commit_lsn: Lsn,
    /// End LSN of the transaction.
    pub end_lsn: Lsn,
    /// Commit timestamp in milliseconds since Unix epoch.
    pub commit_ts_ms: i64,
}

/// Relation (table schema) message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationMessage {
    /// Relation OID.
    pub relation_id: u32,
    /// Schema (namespace) name.
    pub namespace: String,
    /// Table name.
    pub name: String,
    /// Replica identity setting: 'd', 'n', 'f', or 'i'.
    pub replica_identity: u8,
    /// Column descriptors.
    pub columns: Vec<PgColumn>,
}

/// Row insert message.
#[derive(Debug, Clone, PartialEq)]
pub struct InsertMessage {
    /// Relation OID of the target table.
    pub relation_id: u32,
    /// The new row data.
    pub new_tuple: TupleData,
}

/// Old tuple representation identified by the pgoutput wire tag.
#[derive(Debug, Clone, PartialEq)]
pub enum OldTuple {
    /// Replica-identity key (`K`); non-key column positions are unavailable.
    Key(TupleData),
    /// Old tuple (`O`) emitted for `REPLICA IDENTITY FULL`.
    Full(TupleData),
}

/// Row update message.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateMessage {
    /// Relation OID of the target table.
    pub relation_id: u32,
    /// Old identity/full-row data when pgoutput includes a `K` or `O` tuple.
    pub old_tuple: Option<OldTuple>,
    /// The new row data.
    pub new_tuple: TupleData,
}

/// Row delete message.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteMessage {
    /// Relation OID of the target table.
    pub relation_id: u32,
    /// The old identity or full-row tuple.
    pub old_tuple: OldTuple,
}

/// Table truncate message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruncateMessage {
    /// Relation OIDs of truncated tables.
    pub relation_ids: Vec<u32>,
    /// Option flags: bit 0 = CASCADE, bit 1 = RESTART IDENTITY.
    pub options: u8,
}

/// Origin message (for replication from a downstream).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OriginMessage {
    /// Origin LSN.
    pub origin_lsn: Lsn,
    /// Origin name.
    pub name: String,
}

/// Custom type definition message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeMessage {
    /// Type OID.
    pub type_id: u32,
    /// Schema (namespace) name.
    pub namespace: String,
    /// Type name.
    pub name: String,
}

/// Tuple data containing column values.
#[derive(Debug, Clone, PartialEq)]
pub struct TupleData {
    /// Column values in ordinal order.
    pub columns: Vec<ColumnValue>,
}

/// A single column value in a tuple.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnValue {
    /// NULL value.
    Null,
    /// Unchanged TOAST value (not sent by server).
    Unchanged,
    /// Text-format value.
    Text(Bytes),
}

impl ColumnValue {
    /// Returns the text value if present.
    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            ColumnValue::Text(bytes) => std::str::from_utf8(bytes).ok(),
            _ => None,
        }
    }

    /// Returns `true` if the value is NULL.
    #[must_use]
    pub fn is_null(&self) -> bool {
        matches!(self, ColumnValue::Null)
    }
}

/// Errors from the `pgoutput` protocol decoder.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DecoderError {
    /// Not enough bytes to read the expected value.
    #[error("unexpected end of data at offset {offset}, need {needed} bytes")]
    UnexpectedEof {
        /// Current position in the buffer.
        offset: usize,
        /// Number of bytes needed.
        needed: usize,
    },

    /// Invalid message type byte.
    #[error("unknown message type: 0x{0:02X}")]
    UnknownMessageType(u8),

    /// Invalid or corrupted data.
    #[error("invalid data: {0}")]
    InvalidData(String),

    /// Invalid UTF-8 in a string field.
    #[error("invalid UTF-8 at offset {0}")]
    InvalidUtf8(usize),
}

/// A cursor for reading binary data from a byte buffer.
struct Cursor {
    data: Bytes,
    pos: usize,
}

impl Cursor {
    fn new(data: Bytes) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn read_u8(&mut self) -> Result<u8, DecoderError> {
        if self.pos >= self.data.len() {
            return Err(DecoderError::UnexpectedEof {
                offset: self.pos,
                needed: 1,
            });
        }
        let val = self.data[self.pos];
        self.pos += 1;
        Ok(val)
    }

    fn read_i16(&mut self) -> Result<i16, DecoderError> {
        self.check_remaining(2)?;
        let val = i16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(val)
    }

    fn read_i32(&mut self) -> Result<i32, DecoderError> {
        self.check_remaining(4)?;
        let bytes: [u8; 4] = self.data[self.pos..self.pos + 4].try_into().map_err(|_| {
            DecoderError::UnexpectedEof {
                offset: self.pos,
                needed: 4,
            }
        })?;
        let val = i32::from_be_bytes(bytes);
        self.pos += 4;
        Ok(val)
    }

    fn read_u32(&mut self) -> Result<u32, DecoderError> {
        self.check_remaining(4)?;
        let bytes: [u8; 4] = self.data[self.pos..self.pos + 4].try_into().map_err(|_| {
            DecoderError::UnexpectedEof {
                offset: self.pos,
                needed: 4,
            }
        })?;
        let val = u32::from_be_bytes(bytes);
        self.pos += 4;
        Ok(val)
    }

    fn read_i64(&mut self) -> Result<i64, DecoderError> {
        self.check_remaining(8)?;
        let bytes: [u8; 8] = self.data[self.pos..self.pos + 8].try_into().map_err(|_| {
            DecoderError::UnexpectedEof {
                offset: self.pos,
                needed: 8,
            }
        })?;
        let val = i64::from_be_bytes(bytes);
        self.pos += 8;
        Ok(val)
    }

    fn read_u64(&mut self) -> Result<u64, DecoderError> {
        self.check_remaining(8)?;
        let bytes: [u8; 8] = self.data[self.pos..self.pos + 8].try_into().map_err(|_| {
            DecoderError::UnexpectedEof {
                offset: self.pos,
                needed: 8,
            }
        })?;
        let val = u64::from_be_bytes(bytes);
        self.pos += 8;
        Ok(val)
    }

    /// Reads a null-terminated string.
    fn read_cstring(&mut self) -> Result<String, DecoderError> {
        let start = self.pos;
        let nul_pos = self.data[self.pos..]
            .iter()
            .position(|&b| b == 0)
            .ok_or(DecoderError::InvalidData("unterminated string".to_string()))?;

        let s = std::str::from_utf8(&self.data[self.pos..self.pos + nul_pos])
            .map_err(|_| DecoderError::InvalidUtf8(start))?;

        self.pos += nul_pos + 1; // skip the NUL byte
        Ok(s.to_string())
    }

    fn read_bytes(&mut self, len: usize) -> Result<Bytes, DecoderError> {
        self.check_remaining(len)?;
        let slice = self.data.slice(self.pos..self.pos + len);
        self.pos += len;
        Ok(slice)
    }

    fn check_remaining(&self, needed: usize) -> Result<(), DecoderError> {
        if self.remaining() < needed {
            return Err(DecoderError::UnexpectedEof {
                offset: self.pos,
                needed,
            });
        }
        Ok(())
    }
}

/// Converts a `PostgreSQL` timestamp (microseconds since 2000-01-01) to
/// milliseconds since Unix epoch (1970-01-01).
///
/// # Errors
///
/// Returns [`DecoderError`] when converting to the Unix epoch overflows.
pub(super) fn pg_timestamp_to_unix_ms(pg_us: i64) -> Result<i64, DecoderError> {
    pg_us
        .checked_add(PG_EPOCH_OFFSET_US)
        .map(|unix_us| unix_us.div_euclid(1000))
        .ok_or_else(|| DecoderError::InvalidData("PostgreSQL timestamp overflow".into()))
}

/// Decodes a single `pgoutput` WAL message from raw bytes.
///
/// # Errors
///
/// Returns [`DecoderError`] if the data is truncated, malformed, or
/// contains an unknown message type.
pub(super) fn decode_message(data: Bytes) -> Result<WalMessage, DecoderError> {
    if data.is_empty() {
        return Err(DecoderError::InvalidData("empty message".to_string()));
    }

    let mut cur = Cursor::new(data);
    let msg_type = cur.read_u8()?;

    let message = match msg_type {
        b'B' => decode_begin(&mut cur),
        b'C' => decode_commit(&mut cur),
        b'R' => decode_relation(&mut cur),
        b'I' => decode_insert(&mut cur),
        b'U' => decode_update(&mut cur),
        b'D' => decode_delete(&mut cur),
        b'T' => decode_truncate(&mut cur),
        b'O' => decode_origin(&mut cur),
        b'Y' => decode_type(&mut cur),
        _ => Err(DecoderError::UnknownMessageType(msg_type)),
    }?;
    if cur.remaining() != 0 {
        return Err(DecoderError::InvalidData(format!(
            "trailing bytes after pgoutput message: {}",
            cur.remaining()
        )));
    }
    Ok(message)
}

fn decode_begin(cur: &mut Cursor) -> Result<WalMessage, DecoderError> {
    let final_lsn = Lsn::new(cur.read_u64()?);
    let commit_ts_us = cur.read_i64()?;
    let xid = cur.read_u32()?;
    Ok(WalMessage::Begin(BeginMessage {
        final_lsn,
        commit_ts_ms: pg_timestamp_to_unix_ms(commit_ts_us)?,
        xid,
    }))
}

fn decode_commit(cur: &mut Cursor) -> Result<WalMessage, DecoderError> {
    let flags = cur.read_u8()?;
    if flags != 0 {
        return Err(DecoderError::InvalidData(format!(
            "unsupported COMMIT flags: 0x{flags:02X}"
        )));
    }
    let commit_lsn = Lsn::new(cur.read_u64()?);
    let end_lsn = Lsn::new(cur.read_u64()?);
    let commit_ts_us = cur.read_i64()?;
    Ok(WalMessage::Commit(CommitMessage {
        flags,
        commit_lsn,
        end_lsn,
        commit_ts_ms: pg_timestamp_to_unix_ms(commit_ts_us)?,
    }))
}

fn decode_relation(cur: &mut Cursor) -> Result<WalMessage, DecoderError> {
    let relation_id = cur.read_u32()?;
    let namespace = cur.read_cstring()?;
    let name = cur.read_cstring()?;
    let replica_identity = cur.read_u8()?;
    let n_cols_raw = cur.read_i16()?;
    let n_cols = usize::try_from(n_cols_raw)
        .map_err(|_| DecoderError::InvalidData(format!("negative column count: {n_cols_raw}")))?;
    validate_column_count(n_cols)?;

    let mut columns = Vec::with_capacity(n_cols);
    for _ in 0..n_cols {
        let flags = cur.read_u8()?;
        let col_name = cur.read_cstring()?;
        let type_oid = cur.read_u32()?;
        let type_modifier = cur.read_i32()?;
        columns.push(PgColumn::new(
            col_name,
            type_oid,
            type_modifier,
            flags & 1 != 0,
        ));
    }

    Ok(WalMessage::Relation(RelationMessage {
        relation_id,
        namespace,
        name,
        replica_identity,
        columns,
    }))
}

fn decode_insert(cur: &mut Cursor) -> Result<WalMessage, DecoderError> {
    let relation_id = cur.read_u32()?;
    let tag = cur.read_u8()?;
    if tag != b'N' {
        return Err(DecoderError::InvalidData(format!(
            "expected 'N' tag in INSERT, got 0x{tag:02X}"
        )));
    }
    let new_tuple = decode_tuple_data(cur)?;
    Ok(WalMessage::Insert(InsertMessage {
        relation_id,
        new_tuple,
    }))
}

fn decode_update(cur: &mut Cursor) -> Result<WalMessage, DecoderError> {
    let relation_id = cur.read_u32()?;
    let tag = cur.read_u8()?;

    let (old_tuple, new_tuple) = match tag {
        // No old tuple, just new
        b'N' => (None, decode_tuple_data(cur)?),
        // Old replica-identity key or full tuple followed by new.
        b'K' | b'O' => {
            let old_data = decode_tuple_data(cur)?;
            let old = if tag == b'K' {
                OldTuple::Key(old_data)
            } else {
                OldTuple::Full(old_data)
            };
            let new_tag = cur.read_u8()?;
            if new_tag != b'N' {
                return Err(DecoderError::InvalidData(format!(
                    "expected 'N' tag after old tuple in UPDATE, got 0x{new_tag:02X}"
                )));
            }
            let new = decode_tuple_data(cur)?;
            (Some(old), new)
        }
        _ => {
            return Err(DecoderError::InvalidData(format!(
                "unexpected tag in UPDATE: 0x{tag:02X}"
            )));
        }
    };

    Ok(WalMessage::Update(UpdateMessage {
        relation_id,
        old_tuple,
        new_tuple,
    }))
}

fn decode_delete(cur: &mut Cursor) -> Result<WalMessage, DecoderError> {
    let relation_id = cur.read_u32()?;
    let tag = cur.read_u8()?;
    if tag != b'K' && tag != b'O' {
        return Err(DecoderError::InvalidData(format!(
            "expected 'K' or 'O' tag in DELETE, got 0x{tag:02X}"
        )));
    }
    let old_data = decode_tuple_data(cur)?;
    let old_tuple = if tag == b'K' {
        OldTuple::Key(old_data)
    } else {
        OldTuple::Full(old_data)
    };
    Ok(WalMessage::Delete(DeleteMessage {
        relation_id,
        old_tuple,
    }))
}

fn decode_truncate(cur: &mut Cursor) -> Result<WalMessage, DecoderError> {
    let n_relations_raw = cur.read_u32()?;
    let n_relations = usize::try_from(n_relations_raw).map_err(|_| {
        DecoderError::InvalidData(format!(
            "TRUNCATE relation count {n_relations_raw} does not fit this platform"
        ))
    })?;
    let options = cur.read_u8()?;
    let relation_bytes = n_relations
        .checked_mul(std::mem::size_of::<u32>())
        .ok_or_else(|| DecoderError::InvalidData("TRUNCATE relation count overflow".into()))?;
    if relation_bytes != cur.remaining() {
        return Err(DecoderError::InvalidData(format!(
            "TRUNCATE relation count {n_relations} requires {relation_bytes} bytes, but {} remain",
            cur.remaining()
        )));
    }
    let mut relation_ids = Vec::with_capacity(n_relations);
    for _ in 0..n_relations {
        relation_ids.push(cur.read_u32()?);
    }
    Ok(WalMessage::Truncate(TruncateMessage {
        relation_ids,
        options,
    }))
}

fn decode_origin(cur: &mut Cursor) -> Result<WalMessage, DecoderError> {
    let origin_lsn = Lsn::new(cur.read_u64()?);
    let name = cur.read_cstring()?;
    Ok(WalMessage::Origin(OriginMessage { origin_lsn, name }))
}

fn decode_type(cur: &mut Cursor) -> Result<WalMessage, DecoderError> {
    let type_id = cur.read_u32()?;
    let namespace = cur.read_cstring()?;
    let name = cur.read_cstring()?;
    Ok(WalMessage::Type(TypeMessage {
        type_id,
        namespace,
        name,
    }))
}

fn decode_tuple_data(cur: &mut Cursor) -> Result<TupleData, DecoderError> {
    let n_cols_raw = cur.read_i16()?;
    let n_cols = usize::try_from(n_cols_raw)
        .map_err(|_| DecoderError::InvalidData(format!("negative column count: {n_cols_raw}")))?;
    validate_column_count(n_cols)?;
    let mut columns = Vec::with_capacity(n_cols);

    for _ in 0..n_cols {
        let col_type = cur.read_u8()?;
        match col_type {
            b'n' => columns.push(ColumnValue::Null),
            b'u' => columns.push(ColumnValue::Unchanged),
            b't' => {
                let len_raw = cur.read_i32()?;
                let len = usize::try_from(len_raw).map_err(|_| {
                    DecoderError::InvalidData(format!("negative text length: {len_raw}"))
                })?;
                let data = cur.read_bytes(len)?;
                std::str::from_utf8(&data).map_err(|_| DecoderError::InvalidUtf8(cur.pos - len))?;
                columns.push(ColumnValue::Text(data));
            }
            _ => {
                return Err(DecoderError::InvalidData(format!(
                    "unknown column type: 0x{col_type:02X}"
                )));
            }
        }
    }

    Ok(TupleData { columns })
}

fn validate_column_count(count: usize) -> Result<(), DecoderError> {
    if count > MAX_POSTGRES_COLUMNS {
        return Err(DecoderError::InvalidData(format!(
            "column count {count} exceeds PostgreSQL maximum {MAX_POSTGRES_COLUMNS}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

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

/// Offset from `PostgreSQL` epoch (2000-01-01) to Unix epoch (1970-01-01)
/// in microseconds.
const PG_EPOCH_OFFSET_US: i64 = 946_684_800_000_000;

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

/// Row update message.
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateMessage {
    /// Relation OID of the target table.
    pub relation_id: u32,
    /// Old row data (present when replica identity is FULL or INDEX).
    pub old_tuple: Option<TupleData>,
    /// The new row data.
    pub new_tuple: TupleData,
}

/// Row delete message.
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteMessage {
    /// Relation OID of the target table.
    pub relation_id: u32,
    /// The old row data (key columns only unless REPLICA IDENTITY FULL).
    pub old_tuple: TupleData,
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
    Text(String),
}

impl ColumnValue {
    /// Returns the text value if present.
    #[must_use]
    pub fn as_text(&self) -> Option<&str> {
        match self {
            ColumnValue::Text(s) => Some(s),
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
struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
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

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], DecoderError> {
        self.check_remaining(len)?;
        let slice = &self.data[self.pos..self.pos + len];
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
fn pg_timestamp_to_unix_ms(pg_us: i64) -> i64 {
    (pg_us + PG_EPOCH_OFFSET_US) / 1000
}

/// Decodes a single `pgoutput` WAL message from raw bytes.
///
/// # Errors
///
/// Returns [`DecoderError`] if the data is truncated, malformed, or
/// contains an unknown message type.
pub fn decode_message(data: &[u8]) -> Result<WalMessage, DecoderError> {
    if data.is_empty() {
        return Err(DecoderError::InvalidData("empty message".to_string()));
    }

    let mut cur = Cursor::new(data);
    let msg_type = cur.read_u8()?;

    match msg_type {
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
    }
}

fn decode_begin(cur: &mut Cursor<'_>) -> Result<WalMessage, DecoderError> {
    let final_lsn = Lsn::new(cur.read_u64()?);
    let commit_ts_us = cur.read_i64()?;
    let xid = cur.read_u32()?;
    Ok(WalMessage::Begin(BeginMessage {
        final_lsn,
        commit_ts_ms: pg_timestamp_to_unix_ms(commit_ts_us),
        xid,
    }))
}

fn decode_commit(cur: &mut Cursor<'_>) -> Result<WalMessage, DecoderError> {
    let flags = cur.read_u8()?;
    let commit_lsn = Lsn::new(cur.read_u64()?);
    let end_lsn = Lsn::new(cur.read_u64()?);
    let commit_ts_us = cur.read_i64()?;
    Ok(WalMessage::Commit(CommitMessage {
        flags,
        commit_lsn,
        end_lsn,
        commit_ts_ms: pg_timestamp_to_unix_ms(commit_ts_us),
    }))
}

fn decode_relation(cur: &mut Cursor<'_>) -> Result<WalMessage, DecoderError> {
    let relation_id = cur.read_u32()?;
    let namespace = cur.read_cstring()?;
    let name = cur.read_cstring()?;
    let replica_identity = cur.read_u8()?;
    let n_cols_raw = cur.read_i16()?;
    let n_cols = usize::try_from(n_cols_raw)
        .map_err(|_| DecoderError::InvalidData(format!("negative column count: {n_cols_raw}")))?;

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

fn decode_insert(cur: &mut Cursor<'_>) -> Result<WalMessage, DecoderError> {
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

fn decode_update(cur: &mut Cursor<'_>) -> Result<WalMessage, DecoderError> {
    let relation_id = cur.read_u32()?;
    let tag = cur.read_u8()?;

    let (old_tuple, new_tuple) = match tag {
        // No old tuple, just new
        b'N' => (None, decode_tuple_data(cur)?),
        // Old key tuple followed by new
        b'K' | b'O' => {
            let old = decode_tuple_data(cur)?;
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

fn decode_delete(cur: &mut Cursor<'_>) -> Result<WalMessage, DecoderError> {
    let relation_id = cur.read_u32()?;
    let tag = cur.read_u8()?;
    if tag != b'K' && tag != b'O' {
        return Err(DecoderError::InvalidData(format!(
            "expected 'K' or 'O' tag in DELETE, got 0x{tag:02X}"
        )));
    }
    let old_tuple = decode_tuple_data(cur)?;
    Ok(WalMessage::Delete(DeleteMessage {
        relation_id,
        old_tuple,
    }))
}

fn decode_truncate(cur: &mut Cursor<'_>) -> Result<WalMessage, DecoderError> {
    let n_relations = cur.read_u32()? as usize;
    let options = cur.read_u8()?;
    let mut relation_ids = Vec::with_capacity(n_relations);
    for _ in 0..n_relations {
        relation_ids.push(cur.read_u32()?);
    }
    Ok(WalMessage::Truncate(TruncateMessage {
        relation_ids,
        options,
    }))
}

fn decode_origin(cur: &mut Cursor<'_>) -> Result<WalMessage, DecoderError> {
    let origin_lsn = Lsn::new(cur.read_u64()?);
    let name = cur.read_cstring()?;
    Ok(WalMessage::Origin(OriginMessage { origin_lsn, name }))
}

fn decode_type(cur: &mut Cursor<'_>) -> Result<WalMessage, DecoderError> {
    let type_id = cur.read_u32()?;
    let namespace = cur.read_cstring()?;
    let name = cur.read_cstring()?;
    Ok(WalMessage::Type(TypeMessage {
        type_id,
        namespace,
        name,
    }))
}

fn decode_tuple_data(cur: &mut Cursor<'_>) -> Result<TupleData, DecoderError> {
    let n_cols_raw = cur.read_i16()?;
    let n_cols = usize::try_from(n_cols_raw)
        .map_err(|_| DecoderError::InvalidData(format!("negative column count: {n_cols_raw}")))?;
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
                let text = std::str::from_utf8(data)
                    .map_err(|_| DecoderError::InvalidUtf8(cur.pos - len))?;
                columns.push(ColumnValue::Text(text.to_string()));
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

#[cfg(test)]
mod tests {
    use super::*;

    // ── Test helpers: build binary pgoutput messages ──

    /// Helper to build binary messages for testing.
    struct MessageBuilder {
        buf: Vec<u8>,
    }

    impl MessageBuilder {
        fn new(msg_type: u8) -> Self {
            Self {
                buf: vec![msg_type],
            }
        }

        fn u8(mut self, v: u8) -> Self {
            self.buf.push(v);
            self
        }

        fn i16(mut self, v: i16) -> Self {
            self.buf.extend_from_slice(&v.to_be_bytes());
            self
        }

        fn i32(mut self, v: i32) -> Self {
            self.buf.extend_from_slice(&v.to_be_bytes());
            self
        }

        fn u32(mut self, v: u32) -> Self {
            self.buf.extend_from_slice(&v.to_be_bytes());
            self
        }

        fn i64(mut self, v: i64) -> Self {
            self.buf.extend_from_slice(&v.to_be_bytes());
            self
        }

        fn u64(mut self, v: u64) -> Self {
            self.buf.extend_from_slice(&v.to_be_bytes());
            self
        }

        fn cstring(mut self, s: &str) -> Self {
            self.buf.extend_from_slice(s.as_bytes());
            self.buf.push(0);
            self
        }

        fn text_col(mut self, s: &str) -> Self {
            self.buf.push(b't');
            self.buf.extend_from_slice(&(s.len() as i32).to_be_bytes());
            self.buf.extend_from_slice(s.as_bytes());
            self
        }

        fn null_col(mut self) -> Self {
            self.buf.push(b'n');
            self
        }

        fn unchanged_col(mut self) -> Self {
            self.buf.push(b'u');
            self
        }

        fn build(self) -> Vec<u8> {
            self.buf
        }
    }

    // ── Begin ──

    #[test]
    fn test_decode_begin() {
        // Timestamp: 2024-01-01 00:00:00 UTC in PG microseconds
        // PG epoch = 2000-01-01, so 24 years = ~757382400 seconds
        let pg_ts_us: i64 = 757_382_400_000_000;
        let data = MessageBuilder::new(b'B')
            .u64(0x1234_ABCD) // final_lsn
            .i64(pg_ts_us) // commit_ts (PG epoch)
            .u32(42) // xid
            .build();

        let msg = decode_message(&data).unwrap();
        match msg {
            WalMessage::Begin(b) => {
                assert_eq!(b.final_lsn.as_u64(), 0x1234_ABCD);
                assert_eq!(b.xid, 42);
                assert_eq!(b.commit_ts_ms, (pg_ts_us + PG_EPOCH_OFFSET_US) / 1000);
            }
            _ => panic!("expected Begin"),
        }
    }

    // ── Commit ──

    #[test]
    fn test_decode_commit() {
        let pg_ts_us: i64 = 757_382_400_000_000;
        let data = MessageBuilder::new(b'C')
            .u8(0) // flags
            .u64(0x100) // commit_lsn
            .u64(0x200) // end_lsn
            .i64(pg_ts_us) // commit_ts
            .build();

        let msg = decode_message(&data).unwrap();
        match msg {
            WalMessage::Commit(c) => {
                assert_eq!(c.flags, 0);
                assert_eq!(c.commit_lsn.as_u64(), 0x100);
                assert_eq!(c.end_lsn.as_u64(), 0x200);
            }
            _ => panic!("expected Commit"),
        }
    }

    // ── Relation ──

    #[test]
    fn test_decode_relation() {
        let data = MessageBuilder::new(b'R')
            .u32(16384) // relation_id
            .cstring("public") // namespace
            .cstring("users") // name
            .u8(b'd') // replica_identity = default
            .i16(2) // n_cols
            // Column 1: id (key)
            .u8(1) // flags = key
            .cstring("id")
            .u32(20) // int8 OID
            .i32(-1) // type_modifier
            // Column 2: name (not key)
            .u8(0) // flags
            .cstring("name")
            .u32(25) // text OID
            .i32(-1)
            .build();

        let msg = decode_message(&data).unwrap();
        match msg {
            WalMessage::Relation(r) => {
                assert_eq!(r.relation_id, 16384);
                assert_eq!(r.namespace, "public");
                assert_eq!(r.name, "users");
                assert_eq!(r.replica_identity, b'd');
                assert_eq!(r.columns.len(), 2);
                assert_eq!(r.columns[0].name, "id");
                assert!(r.columns[0].is_key);
                assert_eq!(r.columns[0].type_oid, 20);
                assert_eq!(r.columns[1].name, "name");
                assert!(!r.columns[1].is_key);
            }
            _ => panic!("expected Relation"),
        }
    }

    // ── Insert ──

    #[test]
    fn test_decode_insert() {
        let data = MessageBuilder::new(b'I')
            .u32(16384) // relation_id
            .u8(b'N') // new tuple tag
            .i16(3) // n_cols
            .text_col("42") // id
            .text_col("Alice") // name
            .null_col() // nullable field
            .build();

        let msg = decode_message(&data).unwrap();
        match msg {
            WalMessage::Insert(ins) => {
                assert_eq!(ins.relation_id, 16384);
                assert_eq!(ins.new_tuple.columns.len(), 3);
                assert_eq!(ins.new_tuple.columns[0].as_text(), Some("42"));
                assert_eq!(ins.new_tuple.columns[1].as_text(), Some("Alice"));
                assert!(ins.new_tuple.columns[2].is_null());
            }
            _ => panic!("expected Insert"),
        }
    }

    // ── Update (no old tuple) ──

    #[test]
    fn test_decode_update_no_old() {
        let data = MessageBuilder::new(b'U')
            .u32(16384)
            .u8(b'N') // new tuple directly (no old)
            .i16(2)
            .text_col("42")
            .text_col("Bob")
            .build();

        let msg = decode_message(&data).unwrap();
        match msg {
            WalMessage::Update(upd) => {
                assert!(upd.old_tuple.is_none());
                assert_eq!(upd.new_tuple.columns[1].as_text(), Some("Bob"));
            }
            _ => panic!("expected Update"),
        }
    }

    // ── Update (with old tuple, REPLICA IDENTITY FULL) ──

    #[test]
    fn test_decode_update_with_old() {
        let data = MessageBuilder::new(b'U')
            .u32(16384)
            .u8(b'O') // old tuple (FULL identity)
            .i16(2) // old: 2 cols
            .text_col("42")
            .text_col("Alice")
            .u8(b'N') // new tuple tag
            .i16(2) // new: 2 cols
            .text_col("42")
            .text_col("Bob")
            .build();

        let msg = decode_message(&data).unwrap();
        match msg {
            WalMessage::Update(upd) => {
                assert!(upd.old_tuple.is_some());
                let old = upd.old_tuple.unwrap();
                assert_eq!(old.columns[1].as_text(), Some("Alice"));
                assert_eq!(upd.new_tuple.columns[1].as_text(), Some("Bob"));
            }
            _ => panic!("expected Update"),
        }
    }

    // ── Delete ──

    #[test]
    fn test_decode_delete_key() {
        let data = MessageBuilder::new(b'D')
            .u32(16384)
            .u8(b'K') // key columns only
            .i16(1)
            .text_col("42")
            .build();

        let msg = decode_message(&data).unwrap();
        match msg {
            WalMessage::Delete(del) => {
                assert_eq!(del.relation_id, 16384);
                assert_eq!(del.old_tuple.columns[0].as_text(), Some("42"));
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn test_decode_delete_full() {
        let data = MessageBuilder::new(b'D')
            .u32(16384)
            .u8(b'O') // full old row
            .i16(2)
            .text_col("42")
            .text_col("Alice")
            .build();

        let msg = decode_message(&data).unwrap();
        match msg {
            WalMessage::Delete(del) => {
                assert_eq!(del.old_tuple.columns.len(), 2);
            }
            _ => panic!("expected Delete"),
        }
    }

    // ── Truncate ──

    #[test]
    fn test_decode_truncate() {
        let data = MessageBuilder::new(b'T')
            .u32(2) // 2 relations
            .u8(1) // CASCADE
            .u32(16384) // first relation
            .u32(16385) // second relation
            .build();

        let msg = decode_message(&data).unwrap();
        match msg {
            WalMessage::Truncate(t) => {
                assert_eq!(t.relation_ids, vec![16384, 16385]);
                assert_eq!(t.options, 1);
            }
            _ => panic!("expected Truncate"),
        }
    }

    // ── Origin ──

    #[test]
    fn test_decode_origin() {
        let data = MessageBuilder::new(b'O')
            .u64(0xABCD)
            .cstring("upstream")
            .build();

        let msg = decode_message(&data).unwrap();
        match msg {
            WalMessage::Origin(o) => {
                assert_eq!(o.origin_lsn.as_u64(), 0xABCD);
                assert_eq!(o.name, "upstream");
            }
            _ => panic!("expected Origin"),
        }
    }

    // ── Type ──

    #[test]
    fn test_decode_type() {
        let data = MessageBuilder::new(b'Y')
            .u32(12345)
            .cstring("public")
            .cstring("my_enum")
            .build();

        let msg = decode_message(&data).unwrap();
        match msg {
            WalMessage::Type(t) => {
                assert_eq!(t.type_id, 12345);
                assert_eq!(t.namespace, "public");
                assert_eq!(t.name, "my_enum");
            }
            _ => panic!("expected Type"),
        }
    }

    // ── Tuple data with unchanged TOAST column ──

    #[test]
    fn test_decode_insert_with_unchanged() {
        let data = MessageBuilder::new(b'I')
            .u32(16384)
            .u8(b'N')
            .i16(2)
            .text_col("42")
            .unchanged_col()
            .build();

        let msg = decode_message(&data).unwrap();
        match msg {
            WalMessage::Insert(ins) => {
                assert_eq!(ins.new_tuple.columns[0].as_text(), Some("42"));
                assert!(matches!(ins.new_tuple.columns[1], ColumnValue::Unchanged));
            }
            _ => panic!("expected Insert"),
        }
    }

    // ── Error cases ──

    #[test]
    fn test_decode_empty_data() {
        assert!(decode_message(&[]).is_err());
    }

    #[test]
    fn test_decode_unknown_type() {
        let err = decode_message(&[0xFF]).unwrap_err();
        assert!(matches!(err, DecoderError::UnknownMessageType(0xFF)));
    }

    #[test]
    fn test_decode_truncated_begin() {
        // Begin needs 20 bytes after type, only give 4
        let data = MessageBuilder::new(b'B').u32(0).build();
        assert!(decode_message(&data).is_err());
    }

    #[test]
    fn test_decode_invalid_insert_tag() {
        let data = MessageBuilder::new(b'I')
            .u32(16384)
            .u8(b'X') // invalid tag
            .build();
        assert!(decode_message(&data).is_err());
    }

    // ── Timestamp conversion ──

    #[test]
    fn test_pg_timestamp_to_unix_ms() {
        // 2000-01-01 00:00:00 UTC in PG epoch = 0
        // In Unix epoch = 946684800 seconds = 946684800000 ms
        assert_eq!(pg_timestamp_to_unix_ms(0), 946_684_800_000);

        // 2024-01-01 00:00:00 UTC
        // PG epoch: 757382400 seconds = 757382400000000 us
        let pg_us: i64 = 757_382_400_000_000;
        let expected_unix_ms = (pg_us + PG_EPOCH_OFFSET_US) / 1000;
        assert_eq!(pg_timestamp_to_unix_ms(pg_us), expected_unix_ms);
    }

    // ── ColumnValue methods ──

    #[test]
    fn test_column_value_as_text() {
        let text = ColumnValue::Text("hello".to_string());
        assert_eq!(text.as_text(), Some("hello"));
        assert!(!text.is_null());

        let null = ColumnValue::Null;
        assert_eq!(null.as_text(), None);
        assert!(null.is_null());

        let unchanged = ColumnValue::Unchanged;
        assert_eq!(unchanged.as_text(), None);
        assert!(!unchanged.is_null());
    }

    // ── Update with key identity ──

    #[test]
    fn test_decode_update_with_key_identity() {
        let data = MessageBuilder::new(b'U')
            .u32(16384)
            .u8(b'K') // key identity
            .i16(1) // old: 1 col (key only)
            .text_col("42")
            .u8(b'N') // new tuple
            .i16(2) // new: 2 cols
            .text_col("42")
            .text_col("Updated")
            .build();

        let msg = decode_message(&data).unwrap();
        match msg {
            WalMessage::Update(upd) => {
                let old = upd.old_tuple.unwrap();
                assert_eq!(old.columns.len(), 1);
                assert_eq!(upd.new_tuple.columns.len(), 2);
            }
            _ => panic!("expected Update"),
        }
    }
}

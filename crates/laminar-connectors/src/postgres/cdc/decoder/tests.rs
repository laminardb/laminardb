use super::*;

fn decode_message(data: &[u8]) -> Result<WalMessage, DecoderError> {
    super::decode_message(Bytes::copy_from_slice(data))
}

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
            let Some(OldTuple::Full(old)) = upd.old_tuple else {
                panic!("expected full old tuple");
            };
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
            let OldTuple::Key(old) = del.old_tuple else {
                panic!("expected key old tuple");
            };
            assert_eq!(old.columns[0].as_text(), Some("42"));
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
            let OldTuple::Full(old) = del.old_tuple else {
                panic!("expected full old tuple");
            };
            assert_eq!(old.columns.len(), 2);
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

#[test]
fn nonzero_commit_flags_are_rejected() {
    let data = MessageBuilder::new(b'C')
        .u8(1)
        .u64(0x100)
        .u64(0x200)
        .i64(0)
        .build();
    let error = decode_message(&data).unwrap_err();
    assert!(error.to_string().contains("COMMIT flags"), "{error}");
}

#[test]
fn trailing_bytes_are_rejected() {
    let mut data = MessageBuilder::new(b'B').u64(1).i64(0).u32(1).build();
    data.push(0xff);
    let error = decode_message(&data).unwrap_err();
    assert!(error.to_string().contains("trailing bytes"), "{error}");
}

// ── Timestamp conversion ──

#[test]
fn test_pg_timestamp_to_unix_ms() {
    // 2000-01-01 00:00:00 UTC in PG epoch = 0
    // In Unix epoch = 946684800 seconds = 946684800000 ms
    assert_eq!(pg_timestamp_to_unix_ms(0).unwrap(), 946_684_800_000);

    // 2024-01-01 00:00:00 UTC
    // PG epoch: 757382400 seconds = 757382400000000 us
    let pg_us: i64 = 757_382_400_000_000;
    let expected_unix_ms = (pg_us + PG_EPOCH_OFFSET_US).div_euclid(1000);
    assert_eq!(pg_timestamp_to_unix_ms(pg_us).unwrap(), expected_unix_ms);

    // Sub-millisecond instants before Unix epoch round toward negative infinity.
    assert_eq!(
        pg_timestamp_to_unix_ms(-PG_EPOCH_OFFSET_US - 1).unwrap(),
        -1
    );
    assert_eq!(
        pg_timestamp_to_unix_ms(-PG_EPOCH_OFFSET_US - 1_001).unwrap(),
        -2
    );
    assert!(pg_timestamp_to_unix_ms(i64::MAX).is_err());
}

// ── ColumnValue methods ──

#[test]
fn test_column_value_as_text() {
    let text = ColumnValue::Text(Bytes::from_static(b"hello"));
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
        .i16(2) // old tuple retains the relation's published-column cardinality
        .text_col("42")
        .null_col() // non-key position is unavailable
        .u8(b'N') // new tuple
        .i16(2) // new: 2 cols
        .text_col("42")
        .text_col("Updated")
        .build();

    let msg = decode_message(&data).unwrap();
    match msg {
        WalMessage::Update(upd) => {
            let Some(OldTuple::Key(old)) = upd.old_tuple else {
                panic!("expected key old tuple");
            };
            assert_eq!(old.columns.len(), 2);
            assert!(old.columns[1].is_null());
            assert_eq!(upd.new_tuple.columns.len(), 2);
        }
        _ => panic!("expected Update"),
    }
}

#[test]
fn tuple_text_is_a_zero_copy_slice_of_the_wal_frame() {
    let frame = Bytes::from(
        MessageBuilder::new(b'I')
            .u32(16_384)
            .u8(b'N')
            .i16(1)
            .text_col("allocation-free")
            .build(),
    );
    let start = frame.as_ptr() as usize;
    let end = start + frame.len();

    let message = super::decode_message(frame).unwrap();
    let WalMessage::Insert(insert) = message else {
        panic!("expected insert");
    };
    let ColumnValue::Text(value) = &insert.new_tuple.columns[0] else {
        panic!("expected text");
    };
    let value_ptr = value.as_ptr() as usize;
    assert!(value_ptr >= start && value_ptr + value.len() <= end);
    assert_eq!(
        insert.new_tuple.columns[0].as_text(),
        Some("allocation-free")
    );
}

#[test]
fn tuple_column_count_above_postgres_limit_is_rejected_before_values() {
    let data = MessageBuilder::new(b'I')
        .u32(16_384)
        .u8(b'N')
        .i16((MAX_POSTGRES_COLUMNS + 1) as i16)
        .build();
    let error = decode_message(&data).unwrap_err();
    assert!(error.to_string().contains("PostgreSQL maximum"), "{error}");
}

#[test]
fn maximum_postgres_tuple_column_count_is_accepted() {
    let mut data = MessageBuilder::new(b'I')
        .u32(16_384)
        .u8(b'N')
        .i16(MAX_POSTGRES_COLUMNS as i16)
        .build();
    data.extend(std::iter::repeat_n(b'n', MAX_POSTGRES_COLUMNS));

    let message = decode_message(&data).unwrap();
    let WalMessage::Insert(insert) = message else {
        panic!("expected insert");
    };
    assert_eq!(insert.new_tuple.columns.len(), MAX_POSTGRES_COLUMNS);
}

#[test]
fn invalid_tuple_utf8_is_rejected() {
    let mut data = MessageBuilder::new(b'I')
        .u32(16_384)
        .u8(b'N')
        .i16(1)
        .build();
    data.push(b't');
    data.extend_from_slice(&1_i32.to_be_bytes());
    data.push(0xff);

    assert!(matches!(
        decode_message(&data),
        Err(DecoderError::InvalidUtf8(_))
    ));
}

#[test]
fn truncate_count_is_validated_before_allocation() {
    let data = MessageBuilder::new(b'T').u32(u32::MAX).u8(0).build();

    let error = decode_message(&data).unwrap_err();
    assert!(error.to_string().contains("requires"), "{error}");
}

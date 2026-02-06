# F028A: MySQL CDC Binlog I/O Integration

## Metadata

| Field | Value |
|-------|-------|
| **ID** | F028A |
| **Status** | ✅ Done |
| **Phase** | 3 |
| **Priority** | P1 |
| **Effort** | M (3-5 days) |
| **Dependencies** | F028 (MySQL CDC Source) |
| **Blocks** | None |
| **Blocked By** | None |
| **Created** | 2026-02-06 |
| **Updated** | 2026-02-06 |

## Summary

Replace the stubbed I/O methods in `MySqlCdcSource` with actual `mysql_async` crate integration for real MySQL binlog replication. This covers: connecting to MySQL with SSL, starting a binlog stream (GTID or file/position mode), reading and decoding binlog events, and converting them to the existing `BinlogMessage` types.

F028 implemented all business logic (binlog decoder, GTID tracking, table cache, changelog conversion, metrics — 119 tests). F028A wires that logic to the `mysql_async` crate for actual binlog replication I/O.

### Implementation Completed

**Date**: 2026-02-06

**Files Created**:
- `crates/laminar-connectors/src/cdc/mysql/mysql_io.rs` (NEW) — All mysql_async crate integration

**Files Modified**:
- `crates/laminar-connectors/src/cdc/mysql/source.rs` — Added binlog stream field, real I/O in open/poll_batch/close
- `crates/laminar-connectors/src/cdc/mysql/mod.rs` — Added `mysql_io` module declaration
- `crates/laminar-connectors/src/cdc/mysql/gtid.rs` — Added `iter_sets()` method to `GtidSet`
- `crates/laminar-connectors/Cargo.toml` — Added `tokio-stream` optional dependency to `mysql-cdc` feature

**Test Results**:
- 21 new I/O tests in `mysql_io.rs` (with `mysql-cdc` feature)
- 119 existing business logic tests pass (without feature)
- 122 total MySQL tests pass (with feature; 2 stub-only tests gated to `not(mysql-cdc)`)
- 427 total connector tests pass (without feature)
- Full workspace clippy clean with `-D warnings`

## Requirements

### Functional Requirements

- **FR-1**: ✅ `connect()` establishes MySQL connection via `mysql_async::Conn::new()` with configurable host/port/user/password/database
- **FR-2**: ✅ SSL/TLS configuration maps `SslMode` enum to `mysql_async::SslOpts` (Disabled/Preferred/Required/VerifyCa/VerifyIdentity)
- **FR-3**: ✅ `start_binlog_stream()` creates `BinlogStreamRequest` with server_id, GTID set, or file/position
- **FR-4**: ✅ GTID-based replication converts `GtidSet` to `mysql_async::Sid` with `GnoInterval` ranges
- **FR-5**: ✅ File/position-based replication sets binlog filename and position on the request
- **FR-6**: ✅ `read_events()` polls the binlog stream with configurable timeout and max_events limit
- **FR-7**: ✅ `decode_binlog_event()` converts mysql_async event types to our `BinlogMessage` enum
- **FR-8**: ✅ TABLE_MAP events decoded to `TableMapMessage` with column names and types
- **FR-9**: ✅ WRITE_ROWS/UPDATE_ROWS/DELETE_ROWS events decoded to Insert/Update/Delete messages with `ColumnValue` data
- **FR-10**: ✅ GTID/XID/ROTATE/QUERY/HEARTBEAT events decoded to corresponding message types
- **FR-11**: ✅ `poll_batch()` processes decoded events: table cache updates, changelog conversion, GTID tracking, position updates
- **FR-12**: ✅ Table filtering applied during event processing (include/exclude patterns)

### Non-Functional Requirements

- **NFR-1**: ✅ All I/O in Ring 1 (async methods, never blocks Ring 0)
- **NFR-2**: ✅ Feature-gated behind `mysql-cdc` Cargo feature
- **NFR-3**: ✅ Uses rustls TLS backend (no OpenSSL dependency)
- **NFR-4**: ✅ Existing 119 business-logic tests work without the feature

## Technical Design

### Architecture

```text
MySQL Server
     │
     │ Binlog Replication Protocol (mysql_async)
     ▼
┌─────────────────────────────────────────────────┐
│              mysql_io.rs (NEW)                   │
│  ┌──────────┐  ┌──────────────────────────────┐ │
│  │ connect()│  │ start_binlog_stream()         │ │
│  │ (Opts →  │  │ (BinlogStreamRequest with     │ │
│  │  Conn)   │  │  GTID or file/position)       │ │
│  └──────────┘  └──────────────────────────────┘ │
│  ┌──────────┐  ┌──────────────────────────────┐ │
│  │read_     │  │ decode_binlog_event()         │ │
│  │events()  │  │ (mysql_async Event →          │ │
│  │(stream → │  │  BinlogMessage enum)          │ │
│  │ Vec)     │  │                               │ │
│  └──────────┘  └──────────────────────────────┘ │
└─────────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────────┐
│              source.rs (MODIFIED)                │
│  open()  → mysql_io::connect() +                │
│             mysql_io::start_binlog_stream()      │
│  poll()  → mysql_io::read_events() +            │
│             mysql_io::decode_binlog_event() +    │
│             changelog conversion + metrics       │
│  close() → stream.close()                       │
└─────────────────────────────────────────────────┘
```

### Key Design Decisions

1. **Separate I/O module**: All mysql_async integration in `mysql_io.rs`, keeping `source.rs` focused on orchestration
2. **Feature gating**: `#[cfg(feature = "mysql-cdc")]` on all I/O code; business logic always compiles
3. **Helper functions exposed**: `build_opts()`, `build_ssl_opts()`, `build_binlog_request()` are public for unit testing without a real MySQL server
4. **Value conversion**: `binlog_value_to_column_value()` maps mysql_async `BinlogValue`/`Value` types to our `ColumnValue` enum
5. **Stream ownership**: `BinlogStream` stored in `MySqlCdcSource` struct; `get_binlog_stream()` consumes the connection

### mysql_async API Mapping

| mysql_async Type | Our Type |
|-----------------|----------|
| `EventData::TableMapEvent` | `BinlogMessage::TableMap(TableMapMessage)` |
| `EventData::RowsEvent(WriteRowsEvent)` | `BinlogMessage::Insert(InsertMessage)` |
| `EventData::RowsEvent(UpdateRowsEvent)` | `BinlogMessage::Update(UpdateMessage)` |
| `EventData::RowsEvent(DeleteRowsEvent)` | `BinlogMessage::Delete(DeleteMessage)` |
| `EventData::GtidEvent` | `BinlogMessage::Begin(BeginMessage)` |
| `EventData::XidEvent` | `BinlogMessage::Commit(CommitMessage)` |
| `EventData::RotateEvent` | `BinlogMessage::Rotate(RotateMessage)` |
| `EventData::QueryEvent` | `BinlogMessage::Query(QueryMessage)` |
| `EventData::HeartbeatEvent` | `BinlogMessage::Heartbeat` |

### Value Conversion

| mysql_async Value | Our ColumnValue |
|-------------------|-----------------|
| `Value::NULL` | `ColumnValue::Null` |
| `Value::Int(i)` | `ColumnValue::Int(i)` |
| `Value::UInt(u)` | `ColumnValue::UInt(u)` |
| `Value::Float(f)` | `ColumnValue::Float(f as f64)` |
| `Value::Double(d)` | `ColumnValue::Float(d)` |
| `Value::Bytes(b)` | `ColumnValue::String` or `ColumnValue::Bytes` (UTF-8 check) |
| `Value::Date(y,m,d,h,mi,s,us)` | `ColumnValue::DateTime` or `ColumnValue::Date` |
| `Value::Time(neg,d,h,m,s,us)` | `ColumnValue::Time` |
| `BinlogValue::Jsonb(v)` | `ColumnValue::Json(debug_string)` |
| `BinlogValue::JsonDiff(d)` | `ColumnValue::Json(debug_string)` |

## Testing

All 21 tests are gated with `#[cfg(all(test, feature = "mysql-cdc"))]`:

| Test | What It Verifies |
|------|-----------------|
| `test_connect_builds_opts` | OptsBuilder from MySqlCdcConfig with all fields |
| `test_connect_builds_opts_no_password` | OptsBuilder without password |
| `test_ssl_opts_disabled` | SslMode::Disabled → None |
| `test_ssl_opts_preferred` | SslMode::Preferred → Some(SslOpts) |
| `test_ssl_opts_required` | SslMode::Required → Some(SslOpts) |
| `test_ssl_opts_verify_ca` | SslMode::VerifyCa → Some(SslOpts) |
| `test_ssl_opts_verify_identity` | SslMode::VerifyIdentity → Some(SslOpts) |
| `test_binlog_request_gtid_mode` | BinlogStreamRequest with GTID set |
| `test_binlog_request_file_position` | BinlogStreamRequest with filename + position |
| `test_binlog_request_config_filename` | BinlogStreamRequest with config binlog_filename |
| `test_gtid_set_to_sids_empty` | Empty GtidSet → empty Sid vec |
| `test_gtid_set_to_sids_single` | Single GTID range → one Sid |
| `test_gtid_set_to_sids_multiple` | Multiple ranges → multiple Sids |
| `test_sid_bytes_to_gtid` | UUID bytes → Gtid conversion |
| `test_mysql_value_to_column_value_null` | NULL value mapping |
| `test_mysql_value_to_column_value_int` | Int/UInt value mapping |
| `test_mysql_value_to_column_value_float` | Float/Double value mapping |
| `test_mysql_value_to_column_value_string` | UTF-8 Bytes → String |
| `test_mysql_value_to_column_value_bytes` | Non-UTF-8 Bytes → Bytes |
| `test_mysql_value_to_column_value_date` | Date value mapping |
| `test_mysql_value_to_column_value_time` | Time value mapping |

## Verification

```bash
# Business logic tests (no feature, existing 119 tests)
cargo test -p laminar-connectors --lib

# I/O integration tests (with feature, 122 MySQL tests)
cargo test -p laminar-connectors --lib --features mysql-cdc -- mysql

# Clippy
cargo clippy -p laminar-connectors --features mysql-cdc -- -D warnings

# Full workspace
cargo test --all --lib
cargo clippy --all -- -D warnings
```

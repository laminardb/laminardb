//! JSONB binary format type tags.
//!
//! These are the canonical tag constants for the LaminarDB JSONB wire format.
//! Both `laminar-sql` and `laminar-connectors` import from here to avoid
//! duplication.
//!
//! | Tag | Type | Data |
//! |-----|------|------|
//! | 0x00 | Null | (none) |
//! | 0x01 | Boolean false | (none) |
//! | 0x02 | Boolean true | (none) |
//! | 0x03 | Int64 | 8 bytes LE |
//! | 0x04 | Float64 | 8 bytes IEEE 754 LE |
//! | 0x05 | String | 4-byte LE length + UTF-8 bytes |
//! | 0x06 | Array | 4-byte count + offset table + elements |
//! | 0x07 | Object | 4-byte count + offset table + key-value data |

/// Null value.
pub const NULL: u8 = 0x00;
/// Boolean false.
pub const BOOL_FALSE: u8 = 0x01;
/// Boolean true.
pub const BOOL_TRUE: u8 = 0x02;
/// Int64 (8 bytes little-endian).
pub const INT64: u8 = 0x03;
/// Float64 (8 bytes IEEE 754 little-endian).
pub const FLOAT64: u8 = 0x04;
/// String (4-byte LE length + UTF-8 bytes).
pub const STRING: u8 = 0x05;
/// Array (4-byte count + offset table + elements).
pub const ARRAY: u8 = 0x06;
/// Object (4-byte count + offset table + key/value data).
pub const OBJECT: u8 = 0x07;

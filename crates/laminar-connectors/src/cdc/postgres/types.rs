//! `PostgreSQL` type OID to Arrow `DataType` mapping.
//!
//! Maps `PostgreSQL`'s internal type OIDs to Apache Arrow data types
//! for zero-copy CDC event conversion. Also provides text-format
//! value parsing for `pgoutput` protocol data.

use arrow_schema::DataType;

// ── Well-known PostgreSQL type OIDs ──

/// `bool` — boolean
pub const BOOL_OID: u32 = 16;
/// `bytea` — variable-length binary string
pub const BYTEA_OID: u32 = 17;
/// `char` — single character (internal type)
pub const CHAR_OID: u32 = 18;
/// `int8` (bigint) — 8-byte signed integer
pub const INT8_OID: u32 = 20;
/// `int2` (smallint) — 2-byte signed integer
pub const INT2_OID: u32 = 21;
/// `int4` (integer) — 4-byte signed integer
pub const INT4_OID: u32 = 23;
/// `text` — variable-length text
pub const TEXT_OID: u32 = 25;
/// `oid` — object identifier (unsigned 4 bytes)
pub const OID_OID: u32 = 26;
/// `float4` (real) — single precision floating-point
pub const FLOAT4_OID: u32 = 700;
/// `float8` (double precision) — double precision floating-point
pub const FLOAT8_OID: u32 = 701;
/// `varchar` — variable-length character string
pub const VARCHAR_OID: u32 = 1043;
/// `date` — calendar date
pub const DATE_OID: u32 = 1082;
/// `time` — time of day (without timezone)
pub const TIME_OID: u32 = 1083;
/// `timestamp` — date and time (without timezone)
pub const TIMESTAMP_OID: u32 = 1114;
/// `timestamptz` — date and time with timezone
pub const TIMESTAMPTZ_OID: u32 = 1184;
/// `interval` — time interval
pub const INTERVAL_OID: u32 = 1186;
/// `numeric` — exact numeric with arbitrary precision
pub const NUMERIC_OID: u32 = 1700;
/// `uuid` — universally unique identifier
pub const UUID_OID: u32 = 2950;
/// `json` — JSON data
pub const JSON_OID: u32 = 114;
/// `jsonb` — binary JSON data
pub const JSONB_OID: u32 = 3802;
/// `xml` — XML data
pub const XML_OID: u32 = 142;
/// `inet` — IPv4/IPv6 host address
pub const INET_OID: u32 = 869;
/// `cidr` — IPv4/IPv6 network address
pub const CIDR_OID: u32 = 650;
/// `macaddr` — MAC address
pub const MACADDR_OID: u32 = 829;
/// `bpchar` — fixed-length character (char(n))
pub const BPCHAR_OID: u32 = 1042;
/// `name` — 63-byte internal name type
pub const NAME_OID: u32 = 19;

// ── Array type OIDs ──

/// `bool[]`
pub const BOOL_ARRAY_OID: u32 = 1000;
/// `int2[]`
pub const INT2_ARRAY_OID: u32 = 1005;
/// `int4[]`
pub const INT4_ARRAY_OID: u32 = 1007;
/// `int8[]`
pub const INT8_ARRAY_OID: u32 = 1016;
/// `float4[]`
pub const FLOAT4_ARRAY_OID: u32 = 1021;
/// `float8[]`
pub const FLOAT8_ARRAY_OID: u32 = 1022;
/// `text[]`
pub const TEXT_ARRAY_OID: u32 = 1009;
/// `varchar[]`
pub const VARCHAR_ARRAY_OID: u32 = 1015;

/// A column descriptor from a `PostgreSQL` relation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgColumn {
    /// Column name.
    pub name: String,

    /// `PostgreSQL` type OID.
    pub type_oid: u32,

    /// Type modifier (e.g., precision for numeric, length for varchar).
    /// -1 means no modifier.
    pub type_modifier: i32,

    /// Whether this column is part of the replica identity key.
    pub is_key: bool,
}

impl PgColumn {
    /// Creates a new column descriptor.
    #[must_use]
    pub fn new(name: String, type_oid: u32, type_modifier: i32, is_key: bool) -> Self {
        Self {
            name,
            type_oid,
            type_modifier,
            is_key,
        }
    }

    /// Returns the Arrow `DataType` for this column.
    #[must_use]
    pub fn arrow_type(&self) -> DataType {
        pg_type_to_arrow(self.type_oid)
    }
}

/// Maps a `PostgreSQL` type OID to an Arrow `DataType`.
///
/// Covers the most common `PostgreSQL` types. Unknown OIDs map to
/// `DataType::Utf8` as a safe fallback (text representation).
#[must_use]
#[allow(clippy::match_same_arms)] // Arms kept separate to document type categories
pub fn pg_type_to_arrow(oid: u32) -> DataType {
    match oid {
        // Boolean
        BOOL_OID => DataType::Boolean,

        // Integer types
        INT2_OID => DataType::Int16,
        INT4_OID | OID_OID => DataType::Int32,
        INT8_OID => DataType::Int64,

        // Floating point
        FLOAT4_OID => DataType::Float32,
        FLOAT8_OID => DataType::Float64,

        // Numeric (arbitrary precision → string to preserve precision)
        NUMERIC_OID => DataType::Utf8,

        // Character types
        CHAR_OID | BPCHAR_OID | VARCHAR_OID | TEXT_OID | NAME_OID => DataType::Utf8,

        // Binary
        BYTEA_OID => DataType::Binary,

        // Date/Time
        DATE_OID => DataType::Date32,
        TIME_OID => DataType::Time64(arrow_schema::TimeUnit::Microsecond),
        TIMESTAMP_OID => DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
        TIMESTAMPTZ_OID => {
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into()))
        }
        INTERVAL_OID => DataType::Utf8,

        // UUID
        UUID_OID => DataType::Utf8,

        // JSON
        JSON_OID | JSONB_OID => DataType::Utf8,

        // Network types
        INET_OID | CIDR_OID | MACADDR_OID => DataType::Utf8,

        // XML
        XML_OID => DataType::Utf8,

        // Array types (represented as JSON strings)
        BOOL_ARRAY_OID | INT2_ARRAY_OID | INT4_ARRAY_OID | INT8_ARRAY_OID | FLOAT4_ARRAY_OID
        | FLOAT8_ARRAY_OID | TEXT_ARRAY_OID | VARCHAR_ARRAY_OID => DataType::Utf8,

        // Unknown types → text fallback
        _ => DataType::Utf8,
    }
}

/// Returns a human-readable name for a `PostgreSQL` type OID.
#[must_use]
pub fn pg_type_name(oid: u32) -> &'static str {
    match oid {
        BOOL_OID => "bool",
        BYTEA_OID => "bytea",
        CHAR_OID => "char",
        INT2_OID => "int2",
        INT4_OID => "int4",
        INT8_OID => "int8",
        TEXT_OID => "text",
        OID_OID => "oid",
        FLOAT4_OID => "float4",
        FLOAT8_OID => "float8",
        VARCHAR_OID => "varchar",
        BPCHAR_OID => "bpchar",
        NAME_OID => "name",
        DATE_OID => "date",
        TIME_OID => "time",
        TIMESTAMP_OID => "timestamp",
        TIMESTAMPTZ_OID => "timestamptz",
        INTERVAL_OID => "interval",
        NUMERIC_OID => "numeric",
        UUID_OID => "uuid",
        JSON_OID => "json",
        JSONB_OID => "jsonb",
        XML_OID => "xml",
        INET_OID => "inet",
        CIDR_OID => "cidr",
        MACADDR_OID => "macaddr",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integer_type_mapping() {
        assert_eq!(pg_type_to_arrow(INT2_OID), DataType::Int16);
        assert_eq!(pg_type_to_arrow(INT4_OID), DataType::Int32);
        assert_eq!(pg_type_to_arrow(INT8_OID), DataType::Int64);
    }

    #[test]
    fn test_float_type_mapping() {
        assert_eq!(pg_type_to_arrow(FLOAT4_OID), DataType::Float32);
        assert_eq!(pg_type_to_arrow(FLOAT8_OID), DataType::Float64);
    }

    #[test]
    fn test_text_type_mapping() {
        assert_eq!(pg_type_to_arrow(TEXT_OID), DataType::Utf8);
        assert_eq!(pg_type_to_arrow(VARCHAR_OID), DataType::Utf8);
        assert_eq!(pg_type_to_arrow(BPCHAR_OID), DataType::Utf8);
        assert_eq!(pg_type_to_arrow(NAME_OID), DataType::Utf8);
    }

    #[test]
    fn test_bool_type_mapping() {
        assert_eq!(pg_type_to_arrow(BOOL_OID), DataType::Boolean);
    }

    #[test]
    fn test_timestamp_type_mapping() {
        assert!(matches!(
            pg_type_to_arrow(TIMESTAMP_OID),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None)
        ));
        assert!(matches!(
            pg_type_to_arrow(TIMESTAMPTZ_OID),
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some(_))
        ));
    }

    #[test]
    fn test_date_time_mapping() {
        assert_eq!(pg_type_to_arrow(DATE_OID), DataType::Date32);
        assert!(matches!(
            pg_type_to_arrow(TIME_OID),
            DataType::Time64(arrow_schema::TimeUnit::Microsecond)
        ));
    }

    #[test]
    fn test_binary_type_mapping() {
        assert_eq!(pg_type_to_arrow(BYTEA_OID), DataType::Binary);
    }

    #[test]
    fn test_json_type_mapping() {
        assert_eq!(pg_type_to_arrow(JSON_OID), DataType::Utf8);
        assert_eq!(pg_type_to_arrow(JSONB_OID), DataType::Utf8);
    }

    #[test]
    fn test_unknown_type_fallback() {
        assert_eq!(pg_type_to_arrow(99999), DataType::Utf8);
    }

    #[test]
    fn test_pg_column() {
        let col = PgColumn::new("id".to_string(), INT8_OID, -1, true);
        assert_eq!(col.name, "id");
        assert_eq!(col.type_oid, INT8_OID);
        assert!(col.is_key);
        assert_eq!(col.arrow_type(), DataType::Int64);
    }

    #[test]
    fn test_pg_type_name() {
        assert_eq!(pg_type_name(INT4_OID), "int4");
        assert_eq!(pg_type_name(TEXT_OID), "text");
        assert_eq!(pg_type_name(BOOL_OID), "bool");
        assert_eq!(pg_type_name(99999), "unknown");
    }

    #[test]
    fn test_numeric_maps_to_utf8() {
        // Numeric must be Utf8 to preserve arbitrary precision
        assert_eq!(pg_type_to_arrow(NUMERIC_OID), DataType::Utf8);
    }

    #[test]
    fn test_array_types_map_to_utf8() {
        assert_eq!(pg_type_to_arrow(INT4_ARRAY_OID), DataType::Utf8);
        assert_eq!(pg_type_to_arrow(TEXT_ARRAY_OID), DataType::Utf8);
    }
}

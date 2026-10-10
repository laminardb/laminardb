//! `PostgreSQL` column types admitted by CDC and their Arrow bindings.
//!
//! Every supported type has one lossless text-to-Arrow conversion shared by the initial
//! snapshot and `pgoutput`, so a row read either way decodes to the same Arrow value.
//! Types without such a conversion are rejected at binding time; nothing falls back to text.

use arrow_schema::{DataType, TimeUnit};

// ── Well-known PostgreSQL type OIDs ──

/// `bool` — boolean
pub const BOOL_OID: u32 = 16;
/// `bytea` — variable-length binary string
pub const BYTEA_OID: u32 = 17;
/// `char` — single character (internal type)
pub const CHAR_OID: u32 = 18;
/// `name` — 63-byte internal name type
pub const NAME_OID: u32 = 19;
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
/// `json` — JSON data
pub const JSON_OID: u32 = 114;
/// `float4` (real) — single precision floating-point
pub const FLOAT4_OID: u32 = 700;
/// `float8` (double precision) — double precision floating-point
pub const FLOAT8_OID: u32 = 701;
/// `bpchar` — fixed-length character (char(n))
pub const BPCHAR_OID: u32 = 1042;
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
/// `jsonb` — binary JSON data
pub const JSONB_OID: u32 = 3802;
/// `int4[]`
pub const INT4_ARRAY_OID: u32 = 1007;
/// `text[]`
pub const TEXT_ARRAY_OID: u32 = 1009;

const VARHDRSZ: i32 = 4;
const MAX_DECIMAL128_PRECISION: u16 = 38;

/// A column descriptor from a `PostgreSQL` relation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
}

/// The text conversion for one bound column, fixed by its `PostgreSQL` type and declared type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValueKind {
    Bool,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    Utf8,
    Binary,
    Date32,
    Time64Micros,
    /// `timestamp`: wall-clock microseconds with no zone.
    TimestampMicros,
    /// `timestamptz`: the UTC instant in microseconds.
    TimestampTzMicros,
}

/// Bind a `PostgreSQL` column to the Arrow type declared for it.
///
/// # Errors
/// Returns a description of the supported mapping when the pair is not lossless.
pub(crate) fn bind_value_kind(column: &PgColumn, declared: &DataType) -> Result<ValueKind, String> {
    let kind = match (column.type_oid, declared) {
        (BOOL_OID, DataType::Boolean) => ValueKind::Bool,
        (INT2_OID, DataType::Int16) => ValueKind::Int16,
        (INT4_OID, DataType::Int32) => ValueKind::Int32,
        (INT8_OID, DataType::Int64) => ValueKind::Int64,
        (FLOAT4_OID, DataType::Float32) => ValueKind::Float32,
        (FLOAT8_OID, DataType::Float64) => ValueKind::Float64,
        (NUMERIC_OID, DataType::Decimal128(precision, scale)) => {
            let (pg_precision, pg_scale) = numeric_precision_scale(column.type_modifier)?;
            if (pg_precision, pg_scale) != (u16::from(*precision), i16::from(*scale)) {
                return Err(format!(
                    "numeric({pg_precision},{pg_scale}) must be declared DECIMAL({pg_precision},{pg_scale})"
                ));
            }
            ValueKind::Decimal128 {
                precision: *precision,
                scale: *scale,
            }
        }
        (
            TEXT_OID | VARCHAR_OID | BPCHAR_OID | NAME_OID | JSON_OID | JSONB_OID | UUID_OID,
            DataType::Utf8,
        ) => ValueKind::Utf8,
        (BYTEA_OID, DataType::Binary) => ValueKind::Binary,
        (DATE_OID, DataType::Date32) => ValueKind::Date32,
        (TIME_OID, DataType::Time64(TimeUnit::Microsecond)) => ValueKind::Time64Micros,
        (TIMESTAMP_OID, DataType::Timestamp(TimeUnit::Microsecond, None)) => {
            ValueKind::TimestampMicros
        }
        (TIMESTAMPTZ_OID, DataType::Timestamp(TimeUnit::Microsecond, zone))
            if zone.as_deref().is_none_or(is_utc_zone) =>
        {
            ValueKind::TimestampTzMicros
        }
        (oid, declared) => {
            return Err(format!(
                "PostgreSQL {} cannot be declared as {declared}; supported: bool→BOOLEAN, \
                 int2/int4/int8→SMALLINT/INT/BIGINT, float4/float8→REAL/DOUBLE, \
                 numeric(p,s≥0, p≤38)→DECIMAL(p,s), text/varchar/char/name/json/jsonb/uuid→VARCHAR, \
                 bytea→BYTEA, date→DATE, time→TIME, timestamp/timestamptz→TIMESTAMP",
                pg_type_name(oid)
            ));
        }
    };
    Ok(kind)
}

fn is_utc_zone(zone: &str) -> bool {
    matches!(zone, "UTC" | "+00:00" | "Z")
}

/// Precision and scale of a constrained `numeric(p,s)` type modifier.
fn numeric_precision_scale(type_modifier: i32) -> Result<(u16, i16), String> {
    if type_modifier < VARHDRSZ {
        return Err(
            "unconstrained numeric has no fixed precision; declare the PostgreSQL column as numeric(p,s)"
                .into(),
        );
    }
    let packed = type_modifier - VARHDRSZ;
    let precision = u16::try_from((packed >> 16) & 0xffff).map_err(|error| error.to_string())?;
    // PostgreSQL 15+ stores the scale as an 11-bit two's-complement field.
    let scale =
        i16::try_from(((packed & 0x7ff) ^ 0x400) - 0x400).map_err(|error| error.to_string())?;
    if precision == 0 || precision > MAX_DECIMAL128_PRECISION || !(0..=38).contains(&scale) {
        return Err(format!(
            "numeric({precision},{scale}) has no exact DECIMAL128 mapping (precision 1..=38, scale 0..=38)"
        ));
    }
    Ok((precision, scale))
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
        INT4_ARRAY_OID => "int4[]",
        TEXT_ARRAY_OID => "text[]",
        _ => "an unsupported type",
    }
}

#[cfg(test)]
mod tests;

//! Arrow to `PostgreSQL` type mapping for sink operations.
//!
//! Maps Apache Arrow `DataType` to `PostgreSQL` SQL type names for:
//! - UNNEST array casts in upsert queries
//! - CREATE TABLE DDL generation
//! - COPY BINARY column type declarations

use arrow_schema::DataType;

/// Maps an Arrow `DataType` to a `PostgreSQL` SQL type name for UNNEST casts.
///
/// Used in batched upsert queries:
/// ```sql
/// INSERT INTO t (col) SELECT * FROM UNNEST($1::int8[])
/// ```
///
/// # Examples
///
/// ```rust,ignore
/// use arrow_schema::DataType;
/// assert_eq!(arrow_type_to_pg_sql(&DataType::Int64), "int8");
/// assert_eq!(arrow_type_to_pg_sql(&DataType::Utf8), "text");
/// ```
#[must_use]
#[allow(clippy::match_same_arms)]
pub fn arrow_type_to_pg_sql(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "bool",
        DataType::Int8 | DataType::UInt8 => "int2",
        DataType::Int16 | DataType::UInt16 => "int2",
        DataType::Int32 => "int4",
        DataType::UInt32 => "int8", // Widened: no unsigned in PG
        DataType::Int64 | DataType::UInt64 => "int8",
        DataType::Float16 | DataType::Float32 => "float4",
        DataType::Float64 => "float8",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "numeric",
        DataType::Utf8 | DataType::LargeUtf8 => "text",
        DataType::Binary | DataType::LargeBinary => "bytea",
        DataType::FixedSizeBinary(16) => "uuid",
        DataType::FixedSizeBinary(_) => "bytea",
        DataType::Date32 | DataType::Date64 => "date",
        DataType::Time32(_) | DataType::Time64(_) => "time",
        DataType::Timestamp(_, None) => "timestamp",
        DataType::Timestamp(_, Some(_)) => "timestamptz",
        DataType::Duration(_) => "interval",
        _ => "text", // Fallback for complex/nested types
    }
}

/// Maps an Arrow `DataType` to a `PostgreSQL` DDL type for CREATE TABLE.
///
/// Returns a type suitable for `CREATE TABLE` column definitions.
/// More verbose than [`arrow_type_to_pg_sql`] where needed (e.g., `DOUBLE PRECISION`).
///
/// # Examples
///
/// ```rust,ignore
/// use arrow_schema::DataType;
/// assert_eq!(arrow_to_pg_ddl_type(&DataType::Int64), "BIGINT");
/// assert_eq!(arrow_to_pg_ddl_type(&DataType::Float64), "DOUBLE PRECISION");
/// ```
#[must_use]
#[allow(clippy::match_same_arms)]
pub fn arrow_to_pg_ddl_type(dt: &DataType) -> &'static str {
    match dt {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 | DataType::UInt8 => "SMALLINT",
        DataType::Int16 | DataType::UInt16 => "SMALLINT",
        DataType::Int32 => "INTEGER",
        DataType::UInt32 => "BIGINT",
        DataType::Int64 | DataType::UInt64 => "BIGINT",
        DataType::Float16 | DataType::Float32 => "REAL",
        DataType::Float64 => "DOUBLE PRECISION",
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "NUMERIC",
        DataType::Utf8 | DataType::LargeUtf8 => "TEXT",
        DataType::Binary | DataType::LargeBinary => "BYTEA",
        DataType::FixedSizeBinary(16) => "UUID",
        DataType::FixedSizeBinary(_) => "BYTEA",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Time32(_) | DataType::Time64(_) => "TIME",
        DataType::Timestamp(_, None) => "TIMESTAMP",
        DataType::Timestamp(_, Some(_)) => "TIMESTAMPTZ",
        DataType::Duration(_) => "INTERVAL",
        _ => "TEXT",
    }
}

/// Returns the `PostgreSQL` array type suffix for UNNEST cast expressions.
///
/// Combines [`arrow_type_to_pg_sql`] with `[]` for array parameter casting:
/// `$1::int8[]`
#[must_use]
pub fn arrow_type_to_pg_array_cast(dt: &DataType, param_index: usize) -> String {
    format!("${}::{}[]", param_index, arrow_type_to_pg_sql(dt))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::TimeUnit;

    #[test]
    fn test_boolean_mapping() {
        assert_eq!(arrow_type_to_pg_sql(&DataType::Boolean), "bool");
        assert_eq!(arrow_to_pg_ddl_type(&DataType::Boolean), "BOOLEAN");
    }

    #[test]
    fn test_integer_mappings() {
        assert_eq!(arrow_type_to_pg_sql(&DataType::Int8), "int2");
        assert_eq!(arrow_type_to_pg_sql(&DataType::Int16), "int2");
        assert_eq!(arrow_type_to_pg_sql(&DataType::Int32), "int4");
        assert_eq!(arrow_type_to_pg_sql(&DataType::Int64), "int8");

        assert_eq!(arrow_to_pg_ddl_type(&DataType::Int32), "INTEGER");
        assert_eq!(arrow_to_pg_ddl_type(&DataType::Int64), "BIGINT");
    }

    #[test]
    fn test_unsigned_widening() {
        // UInt32 must widen to int8 (no unsigned in PG)
        assert_eq!(arrow_type_to_pg_sql(&DataType::UInt32), "int8");
        assert_eq!(arrow_to_pg_ddl_type(&DataType::UInt32), "BIGINT");
        assert_eq!(arrow_type_to_pg_sql(&DataType::UInt64), "int8");
    }

    #[test]
    fn test_float_mappings() {
        assert_eq!(arrow_type_to_pg_sql(&DataType::Float32), "float4");
        assert_eq!(arrow_type_to_pg_sql(&DataType::Float64), "float8");
        assert_eq!(arrow_to_pg_ddl_type(&DataType::Float64), "DOUBLE PRECISION");
    }

    #[test]
    fn test_decimal_mapping() {
        assert_eq!(
            arrow_type_to_pg_sql(&DataType::Decimal128(10, 2)),
            "numeric"
        );
        assert_eq!(
            arrow_to_pg_ddl_type(&DataType::Decimal128(10, 2)),
            "NUMERIC"
        );
    }

    #[test]
    fn test_string_mappings() {
        assert_eq!(arrow_type_to_pg_sql(&DataType::Utf8), "text");
        assert_eq!(arrow_type_to_pg_sql(&DataType::LargeUtf8), "text");
        assert_eq!(arrow_to_pg_ddl_type(&DataType::Utf8), "TEXT");
    }

    #[test]
    fn test_binary_mappings() {
        assert_eq!(arrow_type_to_pg_sql(&DataType::Binary), "bytea");
        assert_eq!(arrow_type_to_pg_sql(&DataType::LargeBinary), "bytea");
        assert_eq!(arrow_to_pg_ddl_type(&DataType::Binary), "BYTEA");
    }

    #[test]
    fn test_uuid_mapping() {
        assert_eq!(arrow_type_to_pg_sql(&DataType::FixedSizeBinary(16)), "uuid");
        assert_eq!(arrow_to_pg_ddl_type(&DataType::FixedSizeBinary(16)), "UUID");
        // Non-16 byte fixed binary falls back to bytea
        assert_eq!(
            arrow_type_to_pg_sql(&DataType::FixedSizeBinary(32)),
            "bytea"
        );
    }

    #[test]
    fn test_date_time_mappings() {
        assert_eq!(arrow_type_to_pg_sql(&DataType::Date32), "date");
        assert_eq!(arrow_to_pg_ddl_type(&DataType::Date32), "DATE");

        assert_eq!(
            arrow_type_to_pg_sql(&DataType::Time64(TimeUnit::Microsecond)),
            "time"
        );
        assert_eq!(
            arrow_to_pg_ddl_type(&DataType::Time64(TimeUnit::Microsecond)),
            "TIME"
        );
    }

    #[test]
    fn test_timestamp_mappings() {
        assert_eq!(
            arrow_type_to_pg_sql(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            "timestamp"
        );
        assert_eq!(
            arrow_to_pg_ddl_type(&DataType::Timestamp(TimeUnit::Microsecond, None)),
            "TIMESTAMP"
        );

        assert_eq!(
            arrow_type_to_pg_sql(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            )),
            "timestamptz"
        );
        assert_eq!(
            arrow_to_pg_ddl_type(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            )),
            "TIMESTAMPTZ"
        );
    }

    #[test]
    fn test_fallback_to_text() {
        // Complex types fall back to text
        assert_eq!(
            arrow_type_to_pg_sql(&DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Int32,
                true
            )))),
            "text"
        );
    }

    #[test]
    fn test_array_cast_expression() {
        assert_eq!(
            arrow_type_to_pg_array_cast(&DataType::Int64, 1),
            "$1::int8[]"
        );
        assert_eq!(
            arrow_type_to_pg_array_cast(&DataType::Utf8, 3),
            "$3::text[]"
        );
        assert_eq!(
            arrow_type_to_pg_array_cast(&DataType::Boolean, 2),
            "$2::bool[]"
        );
    }

    use std::sync::Arc;
}

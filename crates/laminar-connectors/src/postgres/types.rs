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

/// Converts an Arrow array column to a boxed `PostgreSQL` array parameter for UNNEST queries.
///
/// Each Arrow type maps to the corresponding Rust type that implements
/// `postgres_types::ToSql`. The returned `Box` is passed as a bind parameter
/// to `tokio_postgres::Client::execute`.
///
/// # Supported Types
///
/// `Boolean`, `Int8`–`Int64`, `UInt8`–`UInt64` (widened), `Float32`/`Float64`,
/// `Utf8`, `LargeUtf8`, `Binary`, `LargeBinary`, `Date32`,
/// `Timestamp` (all units, with/without tz).
/// Unsupported types fall back to string representation.
///
/// # Errors
///
/// Returns `ConnectorError::Internal` if the array cannot be downcast to
/// the expected Arrow array type.
#[cfg(feature = "postgres-sink")]
#[allow(
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::missing_panics_doc
)]
pub fn arrow_column_to_pg_array(
    array: &dyn arrow_array::Array,
) -> Result<Box<dyn postgres_types::ToSql + Sync + Send>, crate::error::ConnectorError> {
    use crate::error::ConnectorError;
    use arrow_array::{
        Array as _, BinaryArray, BooleanArray, Date32Array, Float32Array, Float64Array, Int16Array,
        Int32Array, Int64Array, Int8Array, LargeStringArray, StringArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow_schema::TimeUnit;

    macro_rules! extract_primitive {
        ($array:expr, $arrow_ty:ty, $rust_ty:ty) => {{
            let arr = $array.as_any().downcast_ref::<$arrow_ty>().ok_or_else(|| {
                ConnectorError::Internal(format!("downcast to {} failed", stringify!($arrow_ty)))
            })?;
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_possible_wrap,
                clippy::cast_sign_loss
            )]
            let vals: Vec<Option<$rust_ty>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i) as $rust_ty)
                    }
                })
                .collect();
            Ok(Box::new(vals))
        }};
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    ConnectorError::Internal("downcast to BooleanArray failed".into())
                })?;
            let vals: Vec<Option<bool>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i))
                    }
                })
                .collect();
            Ok(Box::new(vals))
        }
        DataType::Int8 => extract_primitive!(array, Int8Array, i16),
        DataType::UInt8 => extract_primitive!(array, UInt8Array, i16),
        DataType::Int16 => extract_primitive!(array, Int16Array, i16),
        DataType::UInt16 => extract_primitive!(array, UInt16Array, i32),
        DataType::Int32 => extract_primitive!(array, Int32Array, i32),
        DataType::UInt32 => extract_primitive!(array, UInt32Array, i64),
        DataType::Int64 => extract_primitive!(array, Int64Array, i64),
        DataType::UInt64 => {
            // PG has no unsigned 64-bit; cast to i64 (wraps for > i64::MAX).
            let arr = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| ConnectorError::Internal("downcast to UInt64Array failed".into()))?;
            #[allow(clippy::cast_possible_wrap)]
            let vals: Vec<Option<i64>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i) as i64)
                    }
                })
                .collect();
            Ok(Box::new(vals))
        }
        DataType::Float32 => extract_primitive!(array, Float32Array, f32),
        DataType::Float64 => extract_primitive!(array, Float64Array, f64),
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ConnectorError::Internal("downcast to StringArray failed".into()))?;
            let vals: Vec<Option<String>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i).to_owned())
                    }
                })
                .collect();
            Ok(Box::new(vals))
        }
        DataType::LargeUtf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    ConnectorError::Internal("downcast to LargeStringArray failed".into())
                })?;
            let vals: Vec<Option<String>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i).to_owned())
                    }
                })
                .collect();
            Ok(Box::new(vals))
        }
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| ConnectorError::Internal("downcast to BinaryArray failed".into()))?;
            let vals: Vec<Option<Vec<u8>>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        Some(arr.value(i).to_vec())
                    }
                })
                .collect();
            Ok(Box::new(vals))
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| ConnectorError::Internal("downcast to Date32Array failed".into()))?;
            let epoch =
                chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("1970-01-01 is a valid date");
            let vals: Vec<Option<chrono::NaiveDate>> = (0..arr.len())
                .map(|i| {
                    if arr.is_null(i) {
                        None
                    } else {
                        let days = i64::from(arr.value(i));
                        if days >= 0 {
                            epoch.checked_add_days(chrono::Days::new(days.unsigned_abs()))
                        } else {
                            epoch.checked_sub_days(chrono::Days::new(days.unsigned_abs()))
                        }
                    }
                })
                .collect();
            Ok(Box::new(vals))
        }
        DataType::Timestamp(unit, tz) => {
            let arr = array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .map(|a| {
                    (0..a.len())
                        .map(|i| {
                            if a.is_null(i) {
                                None
                            } else {
                                to_naive_datetime(a.value(i), &TimeUnit::Microsecond)
                            }
                        })
                        .collect::<Vec<_>>()
                })
                .or_else(|| {
                    array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .map(|a| {
                            (0..a.len())
                                .map(|i| {
                                    if a.is_null(i) {
                                        None
                                    } else {
                                        to_naive_datetime(a.value(i), &TimeUnit::Millisecond)
                                    }
                                })
                                .collect()
                        })
                })
                .or_else(|| {
                    array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .map(|a| {
                            (0..a.len())
                                .map(|i| {
                                    if a.is_null(i) {
                                        None
                                    } else {
                                        to_naive_datetime(a.value(i), &TimeUnit::Second)
                                    }
                                })
                                .collect()
                        })
                })
                .or_else(|| {
                    array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .map(|a| {
                            (0..a.len())
                                .map(|i| {
                                    if a.is_null(i) {
                                        None
                                    } else {
                                        to_naive_datetime(a.value(i), &TimeUnit::Nanosecond)
                                    }
                                })
                                .collect()
                        })
                });

            let vals = arr.ok_or_else(|| {
                ConnectorError::Internal(format!(
                    "unsupported timestamp unit {unit:?} for pg array conversion"
                ))
            })?;

            if tz.is_some() {
                // TIMESTAMPTZ: wrap as DateTime<Utc>
                let tz_vals: Vec<Option<chrono::DateTime<chrono::Utc>>> = vals
                    .into_iter()
                    .map(|opt| opt.map(|ndt| ndt.and_utc()))
                    .collect();
                Ok(Box::new(tz_vals))
            } else {
                Ok(Box::new(vals))
            }
        }
        // Fallback: convert to string representation
        other => {
            let formatter = arrow_cast::display::ArrayFormatter::try_new(
                array,
                &arrow_cast::display::FormatOptions::default(),
            )
            .map_err(|e| ConnectorError::Internal(format!("arrow format error: {e}")))?;
            let vals: Vec<Option<String>> = (0..array.len())
                .map(|i| {
                    if array.is_null(i) {
                        None
                    } else {
                        Some(formatter.value(i).to_string())
                    }
                })
                .collect();
            tracing::debug!(
                data_type = ?other,
                "falling back to text conversion for unsupported Arrow type"
            );
            Ok(Box::new(vals))
        }
    }
}

/// Converts a raw timestamp value to [`chrono::NaiveDateTime`] based on the Arrow `TimeUnit`.
#[cfg(feature = "postgres-sink")]
#[allow(clippy::trivially_copy_pass_by_ref, clippy::cast_possible_truncation)]
fn to_naive_datetime(value: i64, unit: &arrow_schema::TimeUnit) -> Option<chrono::NaiveDateTime> {
    use arrow_schema::TimeUnit;
    let (secs, nanos) = match unit {
        TimeUnit::Second => (value, 0_u32),
        TimeUnit::Millisecond => (
            value / 1_000,
            ((value % 1_000).unsigned_abs() as u32) * 1_000_000,
        ),
        TimeUnit::Microsecond => (
            value / 1_000_000,
            ((value % 1_000_000).unsigned_abs() as u32) * 1_000,
        ),
        TimeUnit::Nanosecond => (
            value / 1_000_000_000,
            (value % 1_000_000_000).unsigned_abs() as u32,
        ),
    };
    chrono::DateTime::from_timestamp(secs, nanos).map(|dt| dt.naive_utc())
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

//! Exact JSON rendering of projected batches for golden files and assertions.
//!
//! Decimals are rendered as strings with their full scale, dates as `YYYY-MM-DD`, and
//! timestamps as RFC 3339 strings (an offset only for UTC columns), so no value passes through
//! a float. Null struct fields and empty values are omitted to keep goldens readable.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Decimal128Type, Int32Type, Int64Type, TimestampNanosecondType, UInt32Type,
    UInt64Type,
};
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};
use serde_json::{Map, Value};

/// One JSON object per row.
pub(crate) fn rows(batch: &RecordBatch) -> Vec<Value> {
    (0..batch.num_rows())
        .map(|row| {
            let mut object = Map::new();
            for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
                if let Some(value) = render(column.as_ref(), row) {
                    object.insert(field.name().clone(), value);
                }
            }
            Value::Object(object)
        })
        .collect()
}

fn render(array: &dyn Array, row: usize) -> Option<Value> {
    if array.is_null(row) {
        return None;
    }
    Some(match array.data_type() {
        DataType::Utf8 => Value::String(array.as_string::<i32>().value(row).to_owned()),
        DataType::Boolean => Value::Bool(array.as_boolean().value(row)),
        DataType::Int32 => Value::from(array.as_primitive::<Int32Type>().value(row)),
        DataType::Int64 => Value::from(array.as_primitive::<Int64Type>().value(row)),
        DataType::UInt32 => Value::from(array.as_primitive::<UInt32Type>().value(row)),
        DataType::UInt64 => Value::from(array.as_primitive::<UInt64Type>().value(row)),
        DataType::Decimal128(_, _) => {
            Value::String(array.as_primitive::<Decimal128Type>().value_as_string(row))
        }
        DataType::Date32 => {
            let days = array.as_primitive::<Date32Type>().value(row);
            Value::String(
                chrono::NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                    .map_or_else(|| days.to_string(), |date| date.to_string()),
            )
        }
        DataType::Timestamp(TimeUnit::Nanosecond, zone) => {
            let nanos = array.as_primitive::<TimestampNanosecondType>().value(row);
            let instant = chrono::DateTime::from_timestamp_nanos(nanos);
            Value::String(if zone.is_some() {
                instant.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true)
            } else {
                instant
                    .naive_utc()
                    .format("%Y-%m-%dT%H:%M:%S%.f")
                    .to_string()
            })
        }
        DataType::Struct(fields) => {
            let array = array.as_struct();
            let mut object = Map::new();
            for (field, child) in fields.iter().zip(array.columns()) {
                if let Some(value) = render(child.as_ref(), row) {
                    object.insert(field.name().clone(), value);
                }
            }
            Value::Object(object)
        }
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let values = list.value(row);
            Value::Array(
                (0..values.len())
                    .map(|index| render(values.as_ref(), index).unwrap_or(Value::Null))
                    .collect(),
            )
        }
        other => Value::String(format!("<unrendered {other}>")),
    })
}

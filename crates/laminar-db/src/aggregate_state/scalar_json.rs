//! JSON roundtrip for `ScalarValue`. Type tags are part of the on-disk
//! checkpoint format — don't rename them.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use datafusion_common::ScalarValue;
use serde_json::json;

use crate::error::DbError;

pub(crate) fn scalar_to_json(sv: &ScalarValue) -> serde_json::Value {
    match sv {
        ScalarValue::Null => json!({"t": "N"}),
        ScalarValue::Boolean(None) => json!({"t": "B", "v": null}),
        ScalarValue::Boolean(Some(b)) => json!({"t": "B", "v": b}),
        ScalarValue::Int8(None)
        | ScalarValue::Int16(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Int64(None) => json!({"t": "I64", "v": null}),
        ScalarValue::Int8(Some(n)) => json!({"t": "I64", "v": i64::from(*n)}),
        ScalarValue::Int16(Some(n)) => json!({"t": "I64", "v": i64::from(*n)}),
        ScalarValue::Int32(Some(n)) => json!({"t": "I64", "v": i64::from(*n)}),
        ScalarValue::Int64(Some(n)) => json!({"t": "I64", "v": n}),
        ScalarValue::UInt8(None)
        | ScalarValue::UInt16(None)
        | ScalarValue::UInt32(None)
        | ScalarValue::UInt64(None) => json!({"t": "U64", "v": null}),
        ScalarValue::UInt8(Some(n)) => json!({"t": "U64", "v": u64::from(*n)}),
        ScalarValue::UInt16(Some(n)) => json!({"t": "U64", "v": u64::from(*n)}),
        ScalarValue::UInt32(Some(n)) => json!({"t": "U64", "v": u64::from(*n)}),
        ScalarValue::UInt64(Some(n)) => json!({"t": "U64", "v": n}),
        ScalarValue::Float32(None) | ScalarValue::Float64(None) => {
            json!({"t": "F64", "v": null})
        }
        ScalarValue::Float32(Some(f)) => json!({"t": "F64", "v": f64::from(*f)}),
        ScalarValue::Float64(Some(f)) => json!({"t": "F64", "v": f}),
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Utf8View(None) => {
            json!({"t": "S", "v": null})
        }
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => json!({"t": "S", "v": s}),
        ScalarValue::TimestampNanosecond(v, tz) => {
            json!({"t": "TSns", "v": v, "tz": tz.as_ref().map(ToString::to_string)})
        }
        ScalarValue::TimestampMicrosecond(v, tz) => {
            json!({"t": "TSus", "v": v, "tz": tz.as_ref().map(ToString::to_string)})
        }
        ScalarValue::TimestampMillisecond(v, tz) => {
            json!({"t": "TSms", "v": v, "tz": tz.as_ref().map(ToString::to_string)})
        }
        ScalarValue::TimestampSecond(v, tz) => {
            json!({"t": "TSs", "v": v, "tz": tz.as_ref().map(ToString::to_string)})
        }
        ScalarValue::Date32(v) => json!({"t": "D32", "v": v}),
        ScalarValue::Date64(v) => json!({"t": "D64", "v": v}),
        ScalarValue::List(arr) => {
            use arrow::array::Array;
            let list_arr: Option<&arrow::array::ListArray> = arr.as_any().downcast_ref();
            match list_arr {
                Some(list) if !list.is_empty() => {
                    let values = list.value(0);
                    let mut items = Vec::with_capacity(values.len());
                    for i in 0..values.len() {
                        let sv =
                            ScalarValue::try_from_array(&values, i).unwrap_or(ScalarValue::Null);
                        items.push(scalar_to_json(&sv));
                    }
                    json!({"t": "L", "v": items})
                }
                _ => json!({"t": "L", "v": []}),
            }
        }
        // Fallback: encode as string representation with type tag
        other => json!({"t": "STR", "v": other.to_string()}),
    }
}

pub(crate) fn json_to_scalar(v: &serde_json::Value) -> Result<ScalarValue, DbError> {
    let t = v
        .get("t")
        .and_then(|t| t.as_str())
        .ok_or_else(|| DbError::Pipeline("missing type tag in scalar JSON".to_string()))?;
    let val = v.get("v");
    match t {
        "N" => Ok(ScalarValue::Null),
        "B" => match val.and_then(serde_json::Value::as_bool) {
            Some(b) => Ok(ScalarValue::Boolean(Some(b))),
            None => Ok(ScalarValue::Boolean(None)),
        },
        "I64" => match val {
            Some(serde_json::Value::Number(n)) => Ok(ScalarValue::Int64(n.as_i64())),
            _ => Ok(ScalarValue::Int64(None)),
        },
        "U64" => match val {
            Some(serde_json::Value::Number(n)) => Ok(ScalarValue::UInt64(n.as_u64())),
            _ => Ok(ScalarValue::UInt64(None)),
        },
        "F64" => match val {
            Some(serde_json::Value::Number(n)) => Ok(ScalarValue::Float64(n.as_f64())),
            _ => Ok(ScalarValue::Float64(None)),
        },
        "S" => match val.and_then(|v| v.as_str()) {
            Some(s) => Ok(ScalarValue::Utf8(Some(s.to_string()))),
            None => Ok(ScalarValue::Utf8(None)),
        },
        "TSns" | "TSus" | "TSms" | "TSs" => {
            let ts = val.and_then(serde_json::Value::as_i64);
            let tz: Option<Arc<str>> = v.get("tz").and_then(|v| v.as_str()).map(Arc::from);
            match t {
                "TSns" => Ok(ScalarValue::TimestampNanosecond(ts, tz)),
                "TSus" => Ok(ScalarValue::TimestampMicrosecond(ts, tz)),
                "TSms" => Ok(ScalarValue::TimestampMillisecond(ts, tz)),
                _ => Ok(ScalarValue::TimestampSecond(ts, tz)),
            }
        }
        "D32" => {
            #[allow(clippy::cast_possible_truncation)]
            let d = val.and_then(serde_json::Value::as_i64).map(|n| n as i32);
            Ok(ScalarValue::Date32(d))
        }
        "D64" => {
            let d = val.and_then(serde_json::Value::as_i64);
            Ok(ScalarValue::Date64(d))
        }
        "L" => {
            let items = val
                .and_then(|v| v.as_array())
                .ok_or_else(|| DbError::Pipeline("expected array for List scalar".to_string()))?;
            let scalars: Result<Vec<ScalarValue>, _> = items.iter().map(json_to_scalar).collect();
            let scalars = scalars?;
            if scalars.is_empty() {
                Ok(ScalarValue::List(Arc::new(
                    arrow::array::GenericListArray::new_null(
                        Arc::new(Field::new("item", DataType::Null, true)),
                        1,
                    ),
                )))
            } else {
                let arr = ScalarValue::new_list(&scalars, &scalars[0].data_type(), true);
                Ok(ScalarValue::List(arr))
            }
        }
        "STR" => match val.and_then(|v| v.as_str()) {
            Some(s) => Ok(ScalarValue::Utf8(Some(s.to_string()))),
            None => Ok(ScalarValue::Utf8(None)),
        },
        other => Err(DbError::Pipeline(format!(
            "unsupported scalar type tag in checkpoint: {other}"
        ))),
    }
}

//! PgWire row encoding: Arrow-to-Postgres type mapping plus text and binary
//! field encoding for result rows.
//!
//! COMPAT: `UInt64` above PostgreSQL BIGINT fails with SQLSTATE 22003 rather than
//! wrapping; `text[]` serializes as a Postgres array literal. Wire output must
//! stay byte-identical across releases.

use std::sync::Arc;

use futures::{stream, StreamExt};
use pgwire::api::portal::Format;
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::api::Type;
use pgwire::error::{PgWireError, PgWireResult};

use super::dispatch::user_error;

/// Single-row `text` response with one column.
pub(super) fn text_response(col: &str, ty: Type, value: String) -> Response {
    let schema = Arc::new(vec![FieldInfo::new(
        col.into(),
        None,
        None,
        ty,
        FieldFormat::Text,
    )]);
    let schema_for_row = Arc::clone(&schema);
    let row_stream = stream::iter(std::iter::once(Ok::<_, PgWireError>(()))).map(move |_| {
        let mut enc = DataRowEncoder::new(Arc::clone(&schema_for_row));
        enc.encode_field(&Some(value.as_str()))?;
        Ok(enc.take_row())
    });
    Response::Query(QueryResponse::new(schema, row_stream))
}

pub(super) fn record_batch_response(batch: arrow_array::RecordBatch) -> Response {
    let fields = Arc::new(field_infos(&batch.schema(), None));
    let nrows = batch.num_rows();

    // Encode rows eagerly: SHOW outputs are tiny and this avoids the
    // !Send formatter dance.
    let mut rows = Vec::with_capacity(nrows);
    {
        let opts = arrow_cast::display::FormatOptions::default();
        let formatters: Vec<_> = batch
            .columns()
            .iter()
            .map(|c| arrow_cast::display::ArrayFormatter::try_new(c.as_ref(), &opts))
            .collect::<Result<_, _>>()
            .unwrap_or_default();
        for row in 0..nrows {
            rows.push(encode_row(&batch, row, &fields, &formatters));
        }
    }

    let row_stream = stream::iter(rows);
    Response::Query(QueryResponse::new(fields, row_stream))
}

/// Build pgwire `FieldInfo`s from an Arrow schema. `result_format` (from a
/// `Bind`) sets per-column text/binary; `None` defaults all-text.
pub(super) fn field_infos(
    schema: &arrow_schema::Schema,
    result_format: Option<&Format>,
) -> Vec<FieldInfo> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let format = result_format.map_or(FieldFormat::Text, |rf| safe_format_for(rf, i));
            FieldInfo::new(
                f.name().clone(),
                None,
                None,
                arrow_to_pg_type(f.data_type()),
                format,
            )
        })
        .collect()
}

pub(super) fn safe_format_for(format: &Format, index: usize) -> FieldFormat {
    match format {
        Format::UnifiedText => FieldFormat::Text,
        Format::UnifiedBinary => FieldFormat::Binary,
        Format::Individual(codes) => codes
            .get(index)
            .copied()
            .map(FieldFormat::from)
            .unwrap_or(FieldFormat::Text),
    }
}

pub(super) fn encode_row(
    batch: &arrow_array::RecordBatch,
    row: usize,
    fields: &Arc<Vec<FieldInfo>>,
    formatters: &[arrow_cast::display::ArrayFormatter<'_>],
) -> PgWireResult<pgwire::messages::data::DataRow> {
    if fields.len() != batch.num_columns() || formatters.len() != batch.num_columns() {
        return Err(user_error(
            "XX000",
            "result schema does not match the emitted batch",
        ));
    }
    let mut enc = DataRowEncoder::new(Arc::clone(fields));
    for (i, col) in batch.columns().iter().enumerate() {
        let info = &fields[i];
        match info.format() {
            FieldFormat::Text => encode_field_text(&mut enc, col.as_ref(), row, &formatters[i])?,
            FieldFormat::Binary => encode_field_binary(&mut enc, col.as_ref(), row, info.name())?,
        }
    }
    Ok(enc.take_row())
}

pub(super) fn encode_field_text(
    enc: &mut DataRowEncoder,
    col: &dyn arrow_array::Array,
    row: usize,
    formatter: &arrow_cast::display::ArrayFormatter<'_>,
) -> PgWireResult<()> {
    use arrow_schema::DataType;
    if col.is_null(row) {
        return enc.encode_field(&None::<&str>);
    }
    if matches!(col.data_type(), DataType::UInt64) {
        let values = col
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .ok_or_else(|| user_error("XX000", "UInt64 column has an invalid Arrow array"))?;
        let value = values.value(row);
        let value = i64::try_from(value)
            .map_err(|_| user_error("22003", "UInt64 value exceeds PostgreSQL BIGINT"))?;
        return enc.encode_field(&Some(value.to_string()));
    }
    // A TEXT[] column must serialize as a Postgres array literal `{..}`, not
    // Arrow's `[..]` display, so text-mode clients parse it as an array.
    if matches!(col.data_type(), DataType::List(f) if matches!(f.data_type(), DataType::Utf8 | DataType::LargeUtf8))
    {
        return enc.encode_field(&Some(pg_text_array_literal(&list_text_elements(col, row))));
    }
    enc.encode_field(&Some(formatter.value(row).to_string()))
}

/// Owned elements of a `List<Utf8|LargeUtf8>` row, NULLs preserved.
pub(super) fn list_text_elements(col: &dyn arrow_array::Array, row: usize) -> Vec<Option<String>> {
    use arrow_array::cast::AsArray;
    use arrow_array::Array;
    use arrow_schema::DataType;
    let values = col.as_list::<i32>().value(row);
    if matches!(values.data_type(), DataType::LargeUtf8) {
        let s = values.as_string::<i64>();
        (0..s.len())
            .map(|i| (!s.is_null(i)).then(|| s.value(i).to_owned()))
            .collect()
    } else {
        let s = values.as_string::<i32>();
        (0..s.len())
            .map(|i| (!s.is_null(i)).then(|| s.value(i).to_owned()))
            .collect()
    }
}

/// Postgres `text[]` literal, e.g. `{"en","ja",NULL}`. Every element is quoted
/// (NULL excepted) so commas/braces/quotes in values are unambiguous.
pub(super) fn pg_text_array_literal(elements: &[Option<String>]) -> String {
    let mut out = String::from("{");
    for (i, elem) in elements.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        match elem {
            None => out.push_str("NULL"),
            Some(v) => {
                out.push('"');
                for ch in v.chars() {
                    if ch == '"' || ch == '\\' {
                        out.push('\\');
                    }
                    out.push(ch);
                }
                out.push('"');
            }
        }
    }
    out.push('}');
    out
}

/// Binary-encode a single Arrow value via `postgres-types` `ToSql`.
///
/// Coverage: Int{8,16,32,64}, UInt{8,16,32,64}, Float{32,64}, Bool,
/// Utf8/LargeUtf8, Timestamp (any unit, naive), Date32, Date64, and
/// `List<Utf8>` (as `text[]`). UInt64 values outside PostgreSQL BIGINT fail
/// with `22003`. Any other column type yields `0A000`.
pub(super) fn encode_field_binary(
    enc: &mut DataRowEncoder,
    col: &dyn arrow_array::Array,
    row: usize,
    name: &str,
) -> PgWireResult<()> {
    use arrow_array::{cast::AsArray, types::*};
    use arrow_schema::DataType;

    if col.is_null(row) {
        return enc.encode_field(&None::<&str>);
    }

    // Pull the typed Arrow value and pass it to `DataRowEncoder`, which
    // calls `postgres-types::ToSql` for the wire format. The `as $cast`
    // arm widens a narrower Arrow int to the matching Postgres OID (see
    // `arrow_to_pg_type`); only lossless `From` casts go through here.
    macro_rules! prim {
        ($ty:ty as $cast:ty) => {
            enc.encode_field(&Some(<$cast>::from(col.as_primitive::<$ty>().value(row))))
        };
        ($ty:ty) => {
            enc.encode_field(&Some(col.as_primitive::<$ty>().value(row)))
        };
    }

    match col.data_type() {
        DataType::Int8 => prim!(Int8Type as i32),
        DataType::Int16 => prim!(Int16Type as i32),
        DataType::Int32 => prim!(Int32Type),
        DataType::Int64 => prim!(Int64Type),
        DataType::UInt8 => prim!(UInt8Type as i32),
        DataType::UInt16 => prim!(UInt16Type as i32),
        DataType::UInt32 => prim!(UInt32Type as i64),
        DataType::UInt64 => {
            let v = col.as_primitive::<UInt64Type>().value(row);
            let v = i64::try_from(v)
                .map_err(|_| user_error("22003", "UInt64 value exceeds PostgreSQL BIGINT"))?;
            enc.encode_field(&Some(v))
        }
        DataType::Float32 => prim!(Float32Type as f64),
        DataType::Float64 => prim!(Float64Type),
        DataType::Boolean => enc.encode_field(&Some(col.as_boolean().value(row))),
        DataType::Utf8 => enc.encode_field(&Some(col.as_string::<i32>().value(row))),
        DataType::LargeUtf8 => enc.encode_field(&Some(col.as_string::<i64>().value(row))),
        DataType::Timestamp(unit, _tz) => {
            // Each unit has its own Arrow type — `PrimitiveArray<TimestampMicrosecondType>`
            // is *not* `PrimitiveArray<Int64Type>`, so the downcast must match the unit.
            use arrow_array::temporal_conversions::{
                timestamp_ms_to_datetime, timestamp_ns_to_datetime, timestamp_s_to_datetime,
                timestamp_us_to_datetime,
            };
            use arrow_schema::TimeUnit;
            let (raw, dt) = match unit {
                TimeUnit::Second => {
                    let v = col.as_primitive::<TimestampSecondType>().value(row);
                    (v, timestamp_s_to_datetime(v))
                }
                TimeUnit::Millisecond => {
                    let v = col.as_primitive::<TimestampMillisecondType>().value(row);
                    (v, timestamp_ms_to_datetime(v))
                }
                TimeUnit::Microsecond => {
                    let v = col.as_primitive::<TimestampMicrosecondType>().value(row);
                    (v, timestamp_us_to_datetime(v))
                }
                TimeUnit::Nanosecond => {
                    let v = col.as_primitive::<TimestampNanosecondType>().value(row);
                    (v, timestamp_ns_to_datetime(v))
                }
            };
            let dt =
                dt.ok_or_else(|| user_error("22008", format!("timestamp out of range: {raw}")))?;
            enc.encode_field(&Some(dt))
        }
        DataType::Date32 => {
            let v = col.as_primitive::<Date32Type>().value(row);
            let dt = arrow_array::temporal_conversions::date32_to_datetime(v)
                .ok_or_else(|| user_error("22008", format!("DATE out of range: {v}")))?;
            enc.encode_field(&Some(dt.date()))
        }
        DataType::Date64 => {
            let v = col.as_primitive::<Date64Type>().value(row);
            let dt = arrow_array::temporal_conversions::date64_to_datetime(v)
                .ok_or_else(|| user_error("22008", format!("DATE out of range: {v}")))?;
            enc.encode_field(&Some(dt.date()))
        }
        DataType::List(field)
            if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            // `postgres-types` encodes Vec<Option<String>> as the binary
            // text[] wire format (the column's OID is TEXT_ARRAY).
            enc.encode_field(&Some(list_text_elements(col, row)))
        }
        other => Err(user_error(
            "0A000",
            format!("binary format not supported for column '{name}' (type {other:?})"),
        )),
    }
}

pub(super) fn arrow_to_pg_type(dt: &arrow_schema::DataType) -> Type {
    use arrow_schema::DataType;
    match dt {
        DataType::Int8 | DataType::Int16 | DataType::Int32 => Type::INT4,
        DataType::Int64 | DataType::UInt32 | DataType::UInt64 => Type::INT8,
        DataType::UInt8 | DataType::UInt16 => Type::INT4,
        DataType::Float32 | DataType::Float64 => Type::FLOAT8,
        DataType::Utf8 | DataType::LargeUtf8 => Type::VARCHAR,
        DataType::Boolean => Type::BOOL,
        DataType::Timestamp(_, _) => Type::TIMESTAMP,
        DataType::Date32 | DataType::Date64 => Type::DATE,
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Type::NUMERIC,
        DataType::List(field)
            if matches!(field.data_type(), DataType::Utf8 | DataType::LargeUtf8) =>
        {
            Type::TEXT_ARRAY
        }
        _ => Type::TEXT,
    }
}

//! Arrow builders mirroring a compiled column tree, including nested structs and lists.

use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Date32Builder, Decimal128Builder, Int32Builder, Int64Builder,
    NullBufferBuilder, OffsetBufferBuilder, StringBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::types::{
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow_array::{ArrayRef, Int32Array, ListArray, PrimitiveArray, StructArray};
use arrow_schema::{DataType, FieldRef, Fields, TimeUnit};

use super::values::{self, ValueError};
use crate::fpml::profile::{Conversion, UTC_ZONE};

pub(super) enum Builder {
    Utf8 {
        builder: StringBuilder,
        collapse: bool,
    },
    Bool(BooleanBuilder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    UInt32(UInt32Builder),
    UInt64(UInt64Builder),
    Decimal {
        builder: Decimal128Builder,
        precision: u8,
        scale: i8,
    },
    Date32(Date32Builder),
    Timestamp {
        values: Vec<Option<i64>>,
        unit: TimeUnit,
        utc: bool,
        offset_optional: bool,
    },
    DateTime {
        utc: Vec<Option<i64>>,
        local: Vec<Option<i64>>,
        offset: Vec<Option<i32>>,
        valid: NullBufferBuilder,
        fields: Fields,
    },
    Struct {
        children: Vec<Builder>,
        fields: Fields,
        valid: NullBufferBuilder,
    },
    List {
        offsets: Vec<i32>,
        valid: NullBufferBuilder,
        item: Box<Builder>,
        field: FieldRef,
    },
}

impl Builder {
    pub(super) fn new(conversion: &Conversion, data_type: &DataType) -> Self {
        match (conversion, data_type) {
            (Conversion::Utf8 { collapse }, _) => Self::Utf8 {
                builder: StringBuilder::new(),
                collapse: *collapse,
            },
            (Conversion::Bool, _) => Self::Bool(BooleanBuilder::new()),
            (Conversion::Int32, _) => Self::Int32(Int32Builder::new()),
            (Conversion::Int64, _) => Self::Int64(Int64Builder::new()),
            (Conversion::UInt32, _) => Self::UInt32(UInt32Builder::new()),
            (Conversion::UInt64, _) => Self::UInt64(UInt64Builder::new()),
            (Conversion::Decimal { precision, scale }, _) => Self::Decimal {
                builder: Decimal128Builder::new(),
                precision: *precision,
                scale: *scale,
            },
            (Conversion::Date32, _) => Self::Date32(Date32Builder::new()),
            (
                Conversion::Timestamp {
                    unit,
                    utc,
                    offset_optional,
                },
                _,
            ) => Self::Timestamp {
                values: Vec::new(),
                unit: *unit,
                utc: *utc,
                offset_optional: *offset_optional,
            },
            (Conversion::DateTime, DataType::Struct(fields)) => Self::DateTime {
                utc: Vec::new(),
                local: Vec::new(),
                offset: Vec::new(),
                valid: NullBufferBuilder::new(0),
                fields: fields.clone(),
            },
            (Conversion::Struct(columns), DataType::Struct(fields)) => Self::Struct {
                children: columns
                    .iter()
                    .zip(fields.iter())
                    .map(|(column, field)| Self::new(&column.conversion, field.data_type()))
                    .collect(),
                fields: fields.clone(),
                valid: NullBufferBuilder::new(0),
            },
            (Conversion::List(item), DataType::List(field)) => Self::List {
                offsets: vec![0],
                valid: NullBufferBuilder::new(0),
                item: Box::new(Self::new(&item.conversion, field.data_type())),
                field: Arc::clone(field),
            },
            (conversion, data_type) => {
                unreachable!("plan conversion {conversion:?} was compiled with type {data_type}")
            }
        }
    }

    /// Append a null, recursively for struct children so they stay row-aligned.
    pub(super) fn append_null(&mut self) {
        match self {
            Self::Utf8 { builder, .. } => builder.append_null(),
            Self::Bool(builder) => builder.append_null(),
            Self::Int32(builder) => builder.append_null(),
            Self::Int64(builder) => builder.append_null(),
            Self::UInt32(builder) => builder.append_null(),
            Self::UInt64(builder) => builder.append_null(),
            Self::Decimal { builder, .. } => builder.append_null(),
            Self::Date32(builder) => builder.append_null(),
            Self::Timestamp { values, .. } => values.push(None),
            Self::DateTime {
                utc,
                local,
                offset,
                valid,
                ..
            } => {
                utc.push(None);
                local.push(None);
                offset.push(None);
                valid.append_null();
            }
            Self::Struct {
                children, valid, ..
            } => {
                for child in children {
                    child.append_null();
                }
                valid.append_null();
            }
            Self::List { offsets, valid, .. } => {
                let end = offsets.last().copied().unwrap_or(0);
                offsets.push(end);
                valid.append_null();
            }
        }
    }

    /// Close one list value whose `count` items were appended to the item builder.
    pub(super) fn close_list(&mut self, count: usize) -> Result<(), ValueError> {
        let Self::List { offsets, valid, .. } = self else {
            return Err("not a list column");
        };
        let last = offsets.last().copied().unwrap_or(0);
        let next = i32::try_from(count)
            .ok()
            .and_then(|count| last.checked_add(count))
            .ok_or("list offsets overflow")?;
        offsets.push(next);
        valid.append_non_null();
        Ok(())
    }

    /// Mark one struct value present after its children were appended.
    pub(super) fn close_struct(&mut self) {
        if let Self::Struct { valid, .. } = self {
            valid.append_non_null();
        }
    }

    pub(super) fn children_mut(&mut self) -> &mut [Builder] {
        match self {
            Self::Struct { children, .. } => children,
            Self::List { item, .. } => std::slice::from_mut(item.as_mut()),
            _ => &mut [],
        }
    }

    /// Convert and append one scalar value; returns the bytes it staged.
    pub(super) fn append_text(&mut self, text: &str) -> Result<usize, ValueError> {
        Ok(match self {
            Self::Utf8 { builder, collapse } => {
                let value = if *collapse {
                    values::collapse(text)
                } else {
                    std::borrow::Cow::Borrowed(text)
                };
                builder.append_value(&value);
                value.len() + 4
            }
            Self::Bool(builder) => {
                builder.append_value(values::boolean(text)?);
                1
            }
            Self::Int32(builder) => {
                builder.append_value(narrow(values::integer(text)?)?);
                4
            }
            Self::Int64(builder) => {
                builder.append_value(narrow(values::integer(text)?)?);
                8
            }
            Self::UInt32(builder) => {
                builder.append_value(narrow(values::integer(text)?)?);
                4
            }
            Self::UInt64(builder) => {
                builder.append_value(narrow(values::integer(text)?)?);
                8
            }
            Self::Decimal {
                builder,
                precision,
                scale,
            } => {
                builder.append_value(values::decimal(text, *precision, *scale)?);
                16
            }
            Self::Date32(builder) => {
                builder.append_value(values::date(text)?);
                4
            }
            Self::Timestamp {
                values: out,
                unit,
                utc,
                offset_optional,
            } => {
                let parsed = values::date_time(text)?;
                let nanos = match (*utc, parsed.utc_nanos()) {
                    (true, Some(instant)) => Some(instant?),
                    (true, None) if *offset_optional => None,
                    (true, None) => {
                        return Err("date-time has no offset; the column requires one (or set offset = \"optional\")")
                    }
                    (false, None) => Some(parsed.local_nanos),
                    (false, Some(_)) => {
                        return Err("date-time has an offset; use timestamp(unit, UTC) or datetime")
                    }
                };
                out.push(
                    nanos
                        .map(|nanos| values::nanos_to_unit(nanos, *unit))
                        .transpose()?,
                );
                8
            }
            Self::DateTime {
                utc,
                local,
                offset,
                valid,
                ..
            } => {
                let parsed = values::date_time(text)?;
                utc.push(parsed.utc_nanos().transpose()?);
                local.push(Some(parsed.local_nanos));
                offset.push(parsed.offset_minutes);
                valid.append_non_null();
                20
            }
            Self::Struct { .. } | Self::List { .. } => {
                return Err("a struct or list column cannot hold a scalar value")
            }
        })
    }

    pub(super) fn finish(&mut self) -> Result<ArrayRef, arrow_schema::ArrowError> {
        Ok(match self {
            Self::Utf8 { builder, .. } => Arc::new(builder.finish()),
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::Int32(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::UInt32(builder) => Arc::new(builder.finish()),
            Self::UInt64(builder) => Arc::new(builder.finish()),
            Self::Decimal {
                builder,
                precision,
                scale,
            } => Arc::new(
                builder
                    .finish()
                    .with_precision_and_scale(*precision, *scale)?,
            ),
            Self::Date32(builder) => Arc::new(builder.finish()),
            Self::Timestamp {
                values, unit, utc, ..
            } => timestamp_array(std::mem::take(values), *unit, *utc),
            Self::DateTime {
                utc,
                local,
                offset,
                valid,
                fields,
            } => Arc::new(StructArray::try_new(
                fields.clone(),
                vec![
                    timestamp_array(std::mem::take(utc), TimeUnit::Nanosecond, true),
                    timestamp_array(std::mem::take(local), TimeUnit::Nanosecond, false),
                    Arc::new(Int32Array::from(std::mem::take(offset))),
                ],
                valid.finish(),
            )?),
            Self::Struct {
                children,
                fields,
                valid,
            } => {
                let arrays = children
                    .iter_mut()
                    .map(Self::finish)
                    .collect::<Result<Vec<_>, _>>()?;
                Arc::new(StructArray::try_new(
                    fields.clone(),
                    arrays,
                    valid.finish(),
                )?)
            }
            Self::List {
                offsets,
                valid,
                item,
                field,
            } => {
                let boundaries = std::mem::replace(offsets, vec![0]);
                let mut lengths = OffsetBufferBuilder::<i32>::new(boundaries.len());
                for pair in boundaries.windows(2) {
                    // Offsets only grow; `close_list` checks every addition.
                    lengths.push_length(usize::try_from(pair[1] - pair[0]).unwrap_or(0));
                }
                let offsets = lengths.finish();
                Arc::new(ListArray::try_new(
                    Arc::clone(field),
                    offsets,
                    item.finish()?,
                    valid.finish(),
                )?)
            }
        })
    }
}

fn timestamp_array(values: Vec<Option<i64>>, unit: TimeUnit, utc: bool) -> ArrayRef {
    let zone: Option<Arc<str>> = utc.then(|| Arc::from(UTC_ZONE));
    match unit {
        TimeUnit::Second => {
            Arc::new(PrimitiveArray::<TimestampSecondType>::from(values).with_timezone_opt(zone))
        }
        TimeUnit::Millisecond => Arc::new(
            PrimitiveArray::<TimestampMillisecondType>::from(values).with_timezone_opt(zone),
        ),
        TimeUnit::Microsecond => Arc::new(
            PrimitiveArray::<TimestampMicrosecondType>::from(values).with_timezone_opt(zone),
        ),
        TimeUnit::Nanosecond => Arc::new(
            PrimitiveArray::<TimestampNanosecondType>::from(values).with_timezone_opt(zone),
        ),
    }
}

fn narrow<T: TryFrom<i128>>(value: i128) -> Result<T, ValueError> {
    T::try_from(value).map_err(|_| "integer does not fit the column type")
}

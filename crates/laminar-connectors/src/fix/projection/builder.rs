//! Arrow column builders for compiled projection plans, including nested `List<Struct>`.

use std::sync::Arc;

use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Int32Builder, Int64Builder,
    NullBufferBuilder, OffsetBufferBuilder, StringBuilder, UInt32Builder, UInt64Builder,
};
use arrow_array::types::{
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow_array::{ArrayRef, ListArray, PrimitiveArray, StructArray};
use arrow_schema::{FieldRef, Fields, TimeUnit};

use super::plan::{ColumnPlan, Conversion, Source};
use super::values::{self, ValueError};
use crate::error::ConnectorError;

pub(super) struct ListState {
    offsets: Vec<i32>,
    nulls: NullBufferBuilder,
    pub(super) children: Vec<ColumnBuilder>,
    item: FieldRef,
    /// Struct item fields; `None` when the single child holds the item values.
    fields: Option<Fields>,
}

impl ListState {
    /// Close one list value after its `count` entries were appended to the children.
    pub(super) fn close_entry(&mut self, count: usize) -> Result<(), ValueError> {
        let last = self.offsets.last().copied().unwrap_or(0);
        let next = i32::try_from(count)
            .ok()
            .and_then(|count| last.checked_add(count))
            .ok_or("list offsets overflow")?;
        self.offsets.push(next);
        self.nulls.append_non_null();
        Ok(())
    }
}

pub(super) enum ColumnBuilder {
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Boolean(BooleanBuilder),
    Int64(Int64Builder),
    Int32(Int32Builder),
    UInt64(UInt64Builder),
    UInt32(UInt32Builder),
    Decimal {
        builder: Decimal128Builder,
        precision: u8,
        scale: i8,
    },
    Timestamp {
        values: Vec<Option<i64>>,
        unit: TimeUnit,
        timezone: Option<Arc<str>>,
        truncate: bool,
    },
    Date32(Date32Builder),
    List(ListState),
}

impl ColumnBuilder {
    pub(super) fn new(plan: &ColumnPlan) -> Self {
        match &plan.conversion {
            Conversion::Utf8 => Self::Utf8(StringBuilder::new()),
            Conversion::Binary => Self::Binary(BinaryBuilder::new()),
            Conversion::Boolean => Self::Boolean(BooleanBuilder::new()),
            Conversion::Int64 => Self::Int64(Int64Builder::new()),
            Conversion::Int32 => Self::Int32(Int32Builder::new()),
            Conversion::UInt64 => Self::UInt64(UInt64Builder::new()),
            Conversion::UInt32 => Self::UInt32(UInt32Builder::new()),
            Conversion::Decimal { precision, scale } => Self::Decimal {
                builder: Decimal128Builder::new(),
                precision: *precision,
                scale: *scale,
            },
            Conversion::Timestamp {
                unit,
                timezone,
                truncate,
            } => Self::Timestamp {
                values: Vec::new(),
                unit: *unit,
                timezone: timezone.clone(),
                truncate: *truncate,
            },
            Conversion::Date32 => Self::Date32(Date32Builder::new()),
            Conversion::List { item, fields } => {
                let children = match &plan.source {
                    Source::Group { children, .. } => children.iter().map(Self::new).collect(),
                    _ => Vec::new(),
                };
                Self::List(ListState {
                    offsets: vec![0],
                    nulls: NullBufferBuilder::new(0),
                    children,
                    item: item.clone(),
                    fields: fields.clone(),
                })
            }
        }
    }

    pub(super) fn list_mut(&mut self) -> Option<&mut ListState> {
        match self {
            Self::List(list) => Some(list),
            _ => None,
        }
    }

    pub(super) fn append_null(&mut self) {
        match self {
            Self::Utf8(builder) => builder.append_null(),
            Self::Binary(builder) => builder.append_null(),
            Self::Boolean(builder) => builder.append_null(),
            Self::Int64(builder) => builder.append_null(),
            Self::Int32(builder) => builder.append_null(),
            Self::UInt64(builder) => builder.append_null(),
            Self::UInt32(builder) => builder.append_null(),
            Self::Decimal { builder, .. } => builder.append_null(),
            Self::Timestamp { values, .. } => values.push(None),
            Self::Date32(builder) => builder.append_null(),
            Self::List(list) => {
                let end = list.offsets.last().copied().unwrap_or(0);
                list.offsets.push(end);
                list.nulls.append_null();
            }
        }
    }

    /// Convert and append one FIX field value.
    pub(super) fn append_bytes(&mut self, bytes: &[u8]) -> Result<(), ValueError> {
        match self {
            Self::Utf8(builder) => {
                builder.append_value(std::str::from_utf8(bytes).map_err(|_| "value is not UTF-8")?);
            }
            Self::Binary(builder) => builder.append_value(bytes),
            Self::Boolean(builder) => builder.append_value(values::boolean(bytes)?),
            Self::Int64(builder) => builder.append_value(narrow(values::integer(bytes)?)?),
            Self::Int32(builder) => builder.append_value(narrow(values::integer(bytes)?)?),
            Self::UInt64(builder) => builder.append_value(narrow(values::integer(bytes)?)?),
            Self::UInt32(builder) => builder.append_value(narrow(values::integer(bytes)?)?),
            Self::Decimal {
                builder,
                precision,
                scale,
            } => builder.append_value(values::decimal(bytes, *precision, *scale)?),
            Self::Timestamp {
                values: out,
                unit,
                truncate,
                ..
            } => out.push(Some(values::utc_timestamp(bytes, *unit, *truncate)?)),
            Self::Date32(builder) => builder.append_value(values::date(bytes)?),
            Self::List(_) => return Err("a list column cannot hold a scalar field"),
        }
        Ok(())
    }

    pub(super) fn append_str(&mut self, value: &str) -> Result<(), ValueError> {
        match self {
            Self::Utf8(builder) => {
                builder.append_value(value);
                Ok(())
            }
            _ => Err("text metadata needs a VARCHAR column"),
        }
    }

    pub(super) fn append_raw(&mut self, value: &[u8]) -> Result<(), ValueError> {
        match self {
            Self::Binary(builder) => {
                builder.append_value(value);
                Ok(())
            }
            _ => Err("byte metadata needs a BYTEA column"),
        }
    }

    pub(super) fn append_bool(&mut self, value: bool) -> Result<(), ValueError> {
        match self {
            Self::Boolean(builder) => {
                builder.append_value(value);
                Ok(())
            }
            _ => Err("flag metadata needs a BOOLEAN column"),
        }
    }

    pub(super) fn append_unsigned(&mut self, value: u128) -> Result<(), ValueError> {
        let value = i128::try_from(value).map_err(|_| "value is out of range")?;
        match self {
            Self::Int64(builder) => builder.append_value(narrow(value)?),
            Self::Int32(builder) => builder.append_value(narrow(value)?),
            Self::UInt64(builder) => builder.append_value(narrow(value)?),
            Self::UInt32(builder) => builder.append_value(narrow(value)?),
            _ => return Err("numeric metadata needs an integer column"),
        }
        Ok(())
    }

    pub(super) fn append_timestamp(&mut self, value: i64) {
        if let Self::Timestamp { values, .. } = self {
            values.push(Some(value));
        }
    }

    pub(super) fn finish(&mut self) -> Result<ArrayRef, ConnectorError> {
        let array: ArrayRef = match self {
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::Binary(builder) => Arc::new(builder.finish()),
            Self::Boolean(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Int32(builder) => Arc::new(builder.finish()),
            Self::UInt64(builder) => Arc::new(builder.finish()),
            Self::UInt32(builder) => Arc::new(builder.finish()),
            Self::Decimal {
                builder,
                precision,
                scale,
            } => Arc::new(
                builder
                    .finish()
                    .with_precision_and_scale(*precision, *scale)
                    .map_err(schema_error)?,
            ),
            Self::Timestamp {
                values,
                unit,
                timezone,
                ..
            } => {
                let values = std::mem::take(values);
                let timezone = timezone.clone();
                match unit {
                    TimeUnit::Second => Arc::new(
                        PrimitiveArray::<TimestampSecondType>::from(values)
                            .with_timezone_opt(timezone),
                    ),
                    TimeUnit::Millisecond => Arc::new(
                        PrimitiveArray::<TimestampMillisecondType>::from(values)
                            .with_timezone_opt(timezone),
                    ),
                    TimeUnit::Microsecond => Arc::new(
                        PrimitiveArray::<TimestampMicrosecondType>::from(values)
                            .with_timezone_opt(timezone),
                    ),
                    TimeUnit::Nanosecond => Arc::new(
                        PrimitiveArray::<TimestampNanosecondType>::from(values)
                            .with_timezone_opt(timezone),
                    ),
                }
            }
            Self::Date32(builder) => Arc::new(builder.finish()),
            Self::List(list) => {
                let children = list
                    .children
                    .iter_mut()
                    .map(Self::finish)
                    .collect::<Result<Vec<_>, _>>()?;
                let entries: ArrayRef = match &list.fields {
                    Some(fields) => Arc::new(
                        StructArray::try_new(fields.clone(), children, None)
                            .map_err(schema_error)?,
                    ),
                    None => children.into_iter().next().ok_or_else(|| {
                        ConnectorError::Internal("scalar list has no item builder".into())
                    })?,
                };
                let boundaries = std::mem::replace(&mut list.offsets, vec![0]);
                let mut offsets = OffsetBufferBuilder::<i32>::new(boundaries.len());
                for pair in boundaries.windows(2) {
                    // Offsets are nondecreasing and overflow-checked in `close_entry`.
                    offsets.push_length(usize::try_from(pair[1] - pair[0]).unwrap_or(0));
                }
                let offsets = offsets.finish();
                Arc::new(
                    ListArray::try_new(list.item.clone(), offsets, entries, list.nulls.finish())
                        .map_err(schema_error)?,
                )
            }
        };
        Ok(array)
    }
}

fn narrow<T: TryFrom<i128>>(value: i128) -> Result<T, ValueError> {
    T::try_from(value).map_err(|_| "integer does not fit the column type")
}

#[allow(clippy::needless_pass_by_value)] // Adapter for owned Result::map_err errors.
fn schema_error(error: arrow_schema::ArrowError) -> ConnectorError {
    ConnectorError::SchemaMismatch(error.to_string())
}

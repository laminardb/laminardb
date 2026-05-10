//! Trait-object wrapper for heterogeneous Arrow column builders.
//!
//! Used by the CSV and JSON decoders to drive a `Vec<Box<dyn ColumnBuilder>>`
//! through the type-coerce + append loop without per-type match branches.

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::builder::{
    BooleanBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder,
    Int8Builder, LargeBinaryBuilder, LargeStringBuilder, StringBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder, UInt16Builder,
    UInt32Builder, UInt64Builder, UInt8Builder, Date32Builder,
};

/// Trait-object wrapper so we can store heterogeneous builders in a `Vec`.
pub(crate) trait ColumnBuilder: Send {
    fn finish(&mut self) -> ArrayRef;
    fn append_null_value(&mut self);
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

macro_rules! impl_column_builder {
    ($builder:ty) => {
        impl ColumnBuilder for $builder {
            fn finish(&mut self) -> ArrayRef {
                Arc::new(<$builder>::finish(self))
            }
            fn append_null_value(&mut self) {
                self.append_null();
            }
            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                self
            }
        }
    };
}

impl_column_builder!(BooleanBuilder);
impl_column_builder!(Int8Builder);
impl_column_builder!(Int16Builder);
impl_column_builder!(Int32Builder);
impl_column_builder!(Int64Builder);
impl_column_builder!(UInt8Builder);
impl_column_builder!(UInt16Builder);
impl_column_builder!(UInt32Builder);
impl_column_builder!(UInt64Builder);
impl_column_builder!(Float32Builder);
impl_column_builder!(Float64Builder);
impl_column_builder!(StringBuilder);
impl_column_builder!(LargeStringBuilder);
impl_column_builder!(LargeBinaryBuilder);
impl_column_builder!(Date32Builder);
impl_column_builder!(TimestampSecondBuilder);
impl_column_builder!(TimestampMillisecondBuilder);
impl_column_builder!(TimestampMicrosecondBuilder);
impl_column_builder!(TimestampNanosecondBuilder);

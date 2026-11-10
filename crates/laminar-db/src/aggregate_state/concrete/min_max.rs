//! MIN and MAX folded from a batch's columns in place. Primitive, boolean, string and binary
//! columns compare their admitted rows where they are; other types copy the admitted rows for
//! `min_batch`/`max_batch`. Both order values as `MinAccumulator`/`MaxAccumulator` do: floats in
//! IEEE total order, strings and binaries in byte order.

use std::borrow::Borrow;
use std::cmp::Ordering;

use arrow::array::{ArrayAccessor, ArrowNativeTypeOp, AsArray};
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal256Type, Decimal32Type, Decimal64Type,
    DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType, DurationSecondType,
    Float16Type, Float32Type, Int16Type, Int32Type, Int8Type, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type,
    UInt8Type,
};
use datafusion_common::ScalarValue;
use datafusion_functions_aggregate_common::min_max::{max_batch, min_batch};

use super::input::TypedValues;
use super::sum::require_array_type;
use super::{aggregate_error, AggregateInput};
use crate::error::DbError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Extremum {
    Min,
    Max,
}

impl Extremum {
    /// Whether a value ordered `ordering` against the current extreme replaces it.
    fn prefers(self, ordering: Ordering) -> bool {
        matches!(
            (self, ordering),
            (Self::Min, Ordering::Less) | (Self::Max, Ordering::Greater)
        )
    }

    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::Min => "MIN",
            Self::Max => "MAX",
        }
    }
}

/// The extreme admitted value so far, NULL until one arrives.
#[derive(Clone, Debug)]
pub(crate) struct ExtremumState {
    input_type: DataType,
    value: ScalarValue,
}

impl ExtremumState {
    pub(super) fn try_new(input_type: &DataType, output_type: &DataType) -> Result<Self, DbError> {
        Ok(Self {
            input_type: input_type.clone(),
            value: ScalarValue::try_from(output_type)
                .map_err(|error| aggregate_error("MIN/MAX state type", error))?,
        })
    }

    pub(super) fn check_input(
        &self,
        extremum: Extremum,
        input: &AggregateInput<'_>,
    ) -> Result<(), DbError> {
        require_array_type(input.value, &self.input_type, extremum.name())
    }

    pub(super) fn update_rows(
        &mut self,
        extremum: Extremum,
        input: &AggregateInput<'_>,
        rows: &[u32],
    ) -> Result<(), DbError> {
        if self.update_in_place(extremum, input, rows) {
            return Ok(());
        }
        let values = input.take_selected(rows)?;
        let candidate = match extremum {
            Extremum::Min => min_batch(&values),
            Extremum::Max => max_batch(&values),
        }
        .map_err(|error| aggregate_error(&format!("{} batch", extremum.name()), error))?;
        self.keep(extremum, candidate)
    }

    /// Folds `rows` where they are when the column has a native comparison; false otherwise.
    fn update_in_place(
        &mut self,
        extremum: Extremum,
        input: &AggregateInput<'_>,
        rows: &[u32],
    ) -> bool {
        let group = GroupRows {
            extremum,
            input,
            rows,
        };
        let values = input.value;
        macro_rules! in_place {
            ($($variant:ident => $arrow:ty),+ $(,)?) => {
                match (&mut self.value, input.values) {
                    (ScalarValue::Int64(current), TypedValues::Int64(typed)) => {
                        group.keep_copied(current, Some(typed), native_order)
                    }
                    (ScalarValue::UInt64(current), TypedValues::UInt64(typed)) => {
                        group.keep_copied(current, Some(typed), native_order)
                    }
                    (ScalarValue::Float64(current), TypedValues::Float64(typed)) => {
                        group.keep_copied(current, Some(typed), native_order)
                    }
                    (ScalarValue::Decimal128(current, ..), TypedValues::Decimal128(typed)) => {
                        group.keep_copied(current, Some(typed), native_order)
                    }
                    $((ScalarValue::$variant(current, ..), _) => {
                        group.keep_copied(current, values.as_primitive_opt::<$arrow>(), native_order)
                    })+
                    (ScalarValue::Boolean(current), _) => {
                        group.keep_copied(current, values.as_boolean_opt(), bool::cmp)
                    }
                    (ScalarValue::Utf8(current), _) => {
                        group.keep_owned(current, values.as_string_opt::<i32>())
                    }
                    (ScalarValue::LargeUtf8(current), _) => {
                        group.keep_owned(current, values.as_string_opt::<i64>())
                    }
                    (ScalarValue::Utf8View(current), _) => {
                        group.keep_owned(current, values.as_string_view_opt())
                    }
                    (ScalarValue::Binary(current), _) => {
                        group.keep_owned(current, values.as_binary_opt::<i32>())
                    }
                    (ScalarValue::LargeBinary(current), _) => {
                        group.keep_owned(current, values.as_binary_opt::<i64>())
                    }
                    (ScalarValue::BinaryView(current), _) => {
                        group.keep_owned(current, values.as_binary_view_opt())
                    }
                    _ => false,
                }
            };
        }
        in_place!(
            Int8 => Int8Type, Int16 => Int16Type, Int32 => Int32Type,
            UInt8 => UInt8Type, UInt16 => UInt16Type, UInt32 => UInt32Type,
            Float16 => Float16Type, Float32 => Float32Type,
            Decimal32 => Decimal32Type, Decimal64 => Decimal64Type, Decimal256 => Decimal256Type,
            Date32 => Date32Type, Date64 => Date64Type,
            Time32Second => Time32SecondType, Time32Millisecond => Time32MillisecondType,
            Time64Microsecond => Time64MicrosecondType, Time64Nanosecond => Time64NanosecondType,
            TimestampSecond => TimestampSecondType,
            TimestampMillisecond => TimestampMillisecondType,
            TimestampMicrosecond => TimestampMicrosecondType,
            TimestampNanosecond => TimestampNanosecondType,
            DurationSecond => DurationSecondType, DurationMillisecond => DurationMillisecondType,
            DurationMicrosecond => DurationMicrosecondType,
            DurationNanosecond => DurationNanosecondType,
        )
    }

    /// The extremum of `rows` alone, in `checkpoint_state` form.
    #[cfg(feature = "cluster")]
    pub(super) fn batch_delta(
        &self,
        extremum: Extremum,
        input: &AggregateInput<'_>,
        rows: &[u32],
    ) -> Result<Vec<ScalarValue>, DbError> {
        let mut delta = Self::try_new(&self.input_type, &self.value.data_type())?;
        delta.update_rows(extremum, input, rows)?;
        Ok(vec![delta.value])
    }

    pub(super) fn value(&self) -> &ScalarValue {
        &self.value
    }

    pub(super) fn checkpoint_state(&self) -> Vec<ScalarValue> {
        vec![self.value.clone()]
    }

    pub(super) fn merge_checkpoint_state(
        &mut self,
        extremum: Extremum,
        checkpoint: &[ScalarValue],
    ) -> Result<(), DbError> {
        let [value] = checkpoint else {
            return Err(DbError::Pipeline(format!(
                "{} checkpoint state must contain one scalar",
                extremum.name()
            )));
        };
        self.keep(extremum, value.clone())
    }

    pub(super) fn nested_retained_bytes(&self) -> usize {
        self.value
            .size()
            .saturating_sub(std::mem::size_of_val(&self.value))
    }

    /// Replaces the extreme with `candidate` when it is preferred. A NULL current value cannot be
    /// ordered against `candidate`, so the type is checked first.
    fn keep(&mut self, extremum: Extremum, mut candidate: ScalarValue) -> Result<(), DbError> {
        let name = extremum.name();
        let data_type = self.value.data_type();
        if candidate.data_type() != data_type {
            return Err(DbError::Pipeline(format!(
                "{name} state {data_type:?} cannot take a {:?} value",
                candidate.data_type()
            )));
        }
        if candidate.is_null() {
            return Ok(());
        }
        if !self.value.is_null() {
            let ordering = candidate
                .try_cmp(&self.value)
                .map_err(|error| aggregate_error(&format!("{name} comparison"), error))?;
            if !extremum.prefers(ordering) {
                return Ok(());
            }
        }
        candidate.compact();
        self.value = candidate;
        Ok(())
    }
}

fn native_order<N: ArrowNativeTypeOp>(left: &N, right: &N) -> Ordering {
    left.compare(*right)
}

/// One group's rows of one batch, read for one extremum.
#[derive(Clone, Copy)]
struct GroupRows<'i, 'a> {
    extremum: Extremum,
    input: &'i AggregateInput<'a>,
    rows: &'i [u32],
}

impl GroupRows<'_, '_> {
    /// The admitted non-NULL value that the extremum prefers under `order`, if any.
    fn extreme<A: ArrayAccessor>(
        self,
        values: &A,
        order: impl Fn(&A::Item, &A::Item) -> Ordering,
    ) -> Option<A::Item> {
        let mut extreme = None;
        for row in self.input.selected(self.rows) {
            if values.is_null(row) {
                continue;
            }
            let value = values.value(row);
            if extreme
                .as_ref()
                .is_none_or(|extreme| self.extremum.prefers(order(&value, extreme)))
            {
                extreme = Some(value);
            }
        }
        extreme
    }

    /// Folds a fixed-width column into `current`; false when the column is another type.
    fn keep_copied<A: ArrayAccessor>(
        self,
        current: &mut Option<A::Item>,
        values: Option<A>,
        order: impl Fn(&A::Item, &A::Item) -> Ordering,
    ) -> bool
    where
        A::Item: Copy,
    {
        let Some(values) = values else {
            return false;
        };
        if let Some(candidate) = self.extreme(&values, &order) {
            if current
                .as_ref()
                .is_none_or(|current| self.extremum.prefers(order(&candidate, current)))
            {
                *current = Some(candidate);
            }
        }
        true
    }

    /// Folds a string or binary column into `current`, copying a value only when it replaces
    /// the extreme; false when the column is another type.
    fn keep_owned<'v, B, A>(self, current: &mut Option<B::Owned>, values: Option<A>) -> bool
    where
        B: Ord + ToOwned + ?Sized + 'v,
        A: ArrayAccessor<Item = &'v B>,
    {
        let Some(values) = values else {
            return false;
        };
        if let Some(candidate) = self.extreme(&values, Ord::cmp) {
            if current.as_ref().is_none_or(|current| {
                let current: &B = current.borrow();
                self.extremum.prefers(candidate.cmp(current))
            }) {
                *current = Some(candidate.to_owned());
            }
        }
        true
    }
}

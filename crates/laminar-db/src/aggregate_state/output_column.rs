//! One aggregate's output column, filled with each emitted group's value as the group is visited.

use std::borrow::Cow;
use std::sync::Arc;

use arrow::array::{
    new_empty_array, ArrayRef, Float64Builder, Int64Builder, StringBuilder, UInt64Builder,
};
use datafusion_common::ScalarValue;

use super::AggFuncSpec;
use crate::error::DbError;

/// The column takes its type from the first value, as `ScalarValue::iter_to_array` does, and a
/// later value of another type is an error. Types without a typed builder keep their values for
/// `iter_to_array`.
enum Values {
    Empty,
    Int64(Int64Builder),
    UInt64(UInt64Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Scalars(Vec<ScalarValue>),
}

impl Values {
    /// An empty column for values of `value`'s type.
    fn for_type_of(value: &ScalarValue, capacity: usize) -> Self {
        match value {
            ScalarValue::Int64(_) => Self::Int64(Int64Builder::with_capacity(capacity)),
            ScalarValue::UInt64(_) => Self::UInt64(UInt64Builder::with_capacity(capacity)),
            ScalarValue::Float64(_) => Self::Float64(Float64Builder::with_capacity(capacity)),
            ScalarValue::Utf8(_) => Self::Utf8(StringBuilder::with_capacity(capacity, 0)),
            _ => Self::Scalars(Vec::with_capacity(capacity)),
        }
    }
}

pub(crate) struct OutputColumn {
    capacity: usize,
    values: Values,
}

impl OutputColumn {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            values: Values::Empty,
        }
    }

    /// Appends `value`. A column without a typed builder keeps an owned value as it is and clones
    /// a lent one.
    #[inline]
    pub(crate) fn push(&mut self, value: Cow<'_, ScalarValue>) -> Result<(), DbError> {
        if matches!(self.values, Values::Empty) {
            self.values = Values::for_type_of(&value, self.capacity);
        }
        if let Values::Scalars(values) = &mut self.values {
            values.push(value.into_owned());
            return Ok(());
        }
        match (&mut self.values, value.as_ref()) {
            (Values::Int64(builder), ScalarValue::Int64(value)) => builder.append_option(*value),
            (Values::UInt64(builder), ScalarValue::UInt64(value)) => builder.append_option(*value),
            (Values::Float64(builder), ScalarValue::Float64(value)) => {
                builder.append_option(*value);
            }
            (Values::Utf8(builder), ScalarValue::Utf8(value)) => {
                builder.append_option(value.as_deref());
            }
            (_, value) => {
                return Err(DbError::Pipeline(format!(
                    "aggregate output column mixes value types: got {}",
                    value.data_type()
                )))
            }
        }
        Ok(())
    }

    /// The column, cast to `spec`'s return type when its values have another type.
    pub(crate) fn finish(self, spec: &AggFuncSpec) -> Result<ArrayRef, DbError> {
        let array: ArrayRef = match self.values {
            Values::Empty => return Ok(new_empty_array(&spec.return_type)),
            Values::Int64(mut builder) => Arc::new(builder.finish()),
            Values::UInt64(mut builder) => Arc::new(builder.finish()),
            Values::Float64(mut builder) => Arc::new(builder.finish()),
            Values::Utf8(mut builder) => Arc::new(builder.finish()),
            Values::Scalars(values) => ScalarValue::iter_to_array(values).map_err(|error| {
                DbError::Pipeline(format!(
                    "aggregate '{}' result array build: {error}",
                    spec.output_name
                ))
            })?,
        };
        if array.data_type() == &spec.return_type {
            return Ok(array);
        }
        arrow::compute::cast(&array, &spec.return_type).map_err(|error| {
            DbError::Pipeline(format!(
                "aggregate '{}' result cast failed: {error}",
                spec.output_name
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, AsArray, Int64Array};
    use arrow::datatypes::{DataType, Float64Type, Int64Type};
    use datafusion::functions_aggregate::count::count_udaf;

    use super::*;

    fn spec(return_type: DataType) -> AggFuncSpec {
        AggFuncSpec {
            udf: count_udaf(),
            input_types: Vec::new(),
            input_col_indices: Vec::new(),
            output_name: "out".into(),
            return_type,
            is_count_star: false,
            filter_col_index: None,
        }
    }

    /// Pushes the values lent and owned in turn; a column builds the same array from either.
    fn column(values: Vec<ScalarValue>, return_type: DataType) -> Result<ArrayRef, DbError> {
        let mut column = OutputColumn::with_capacity(values.len());
        for (index, value) in values.iter().enumerate() {
            if index % 2 == 0 {
                column.push(Cow::Borrowed(value))?;
            } else {
                column.push(Cow::Owned(value.clone()))?;
            }
        }
        column.finish(&spec(return_type))
    }

    #[test]
    fn typed_columns_equal_iter_to_array_with_nulls() {
        let cases = [
            vec![ScalarValue::Int64(Some(3)), ScalarValue::Int64(None)],
            vec![ScalarValue::UInt64(None), ScalarValue::UInt64(Some(7))],
            vec![
                ScalarValue::Float64(Some(0.5)),
                ScalarValue::Float64(Some(-1.0)),
            ],
            vec![
                ScalarValue::Utf8(Some("b".into())),
                ScalarValue::Utf8(None),
                ScalarValue::Utf8(Some(String::new())),
            ],
        ];
        for values in cases {
            let return_type = values[0].data_type();
            let expected = ScalarValue::iter_to_array(values.clone()).unwrap();
            let array = column(values, return_type).unwrap();
            assert_eq!(array.to_data(), expected.to_data());
            assert_eq!(array.null_count(), expected.null_count());
        }
    }

    #[test]
    fn untyped_values_go_through_iter_to_array() {
        let values = vec![
            ScalarValue::Date32(Some(2)),
            ScalarValue::Date32(None),
            ScalarValue::Date32(Some(1)),
        ];
        let expected = ScalarValue::iter_to_array(values.clone()).unwrap();
        let array = column(values, DataType::Date32).unwrap();
        assert_eq!(array.to_data(), expected.to_data());
    }

    #[test]
    fn values_of_another_type_cast_to_the_return_type() {
        let values = vec![ScalarValue::Int64(Some(2)), ScalarValue::Int64(None)];
        let array = column(values, DataType::Float64).unwrap();
        let array = array.as_primitive::<Float64Type>();
        assert_eq!(array.value(0), 2.0);
        assert!(array.is_null(1));
    }

    #[test]
    fn mixed_value_types_are_rejected() {
        let values = vec![ScalarValue::Int64(Some(1)), ScalarValue::Float64(Some(1.0))];
        assert!(column(values, DataType::Int64).is_err());
        let values = vec![
            ScalarValue::Utf8(Some("a".into())),
            ScalarValue::Int64(Some(1)),
        ];
        assert!(column(values, DataType::Utf8).is_err());
    }

    #[test]
    fn an_empty_column_has_the_return_type() {
        let array = column(Vec::new(), DataType::Int64).unwrap();
        assert_eq!(array.len(), 0);
        assert_eq!(
            array.as_primitive::<Int64Type>(),
            &Int64Array::from(Vec::<i64>::new())
        );
    }
}

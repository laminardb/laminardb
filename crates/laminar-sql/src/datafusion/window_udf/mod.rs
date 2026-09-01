//! Scalar UDFs that compute window boundary timestamps for streaming
//! `GROUP BY TUMBLE(...)` style queries.
//!
//! Pairs `tumble`/`tumble_end`, `hop`/`hop_end`, `cumulate`/`cumulate_end`
//! must both appear in `GROUP BY`: DataFusion treats them as independent
//! group keys, even though within a fixed interval they are functionally
//! redundant. `session(ts, gap)` is a passthrough — real session
//! boundaries are computed by Ring 0 operators.
//!
//! ```sql
//! SELECT tumble(ts, INTERVAL '1' MINUTE)     AS window_start,
//!        tumble_end(ts, INTERVAL '1' MINUTE) AS window_end, ...
//! FROM ev
//! GROUP BY tumble(ts, INTERVAL '1' MINUTE),
//!          tumble_end(ts, INTERVAL '1' MINUTE), ...
//! ```
//!
//! Math runs in milliseconds (matching the Ring 0 watermark). The SQL
//! boundary returns `Timestamp(Microsecond, None)` so lakehouse sinks
//! (Iceberg, Delta, Parquet) accept the columns directly — Iceberg
//! rejects `Timestamp(Millisecond)`.

use std::hash::{Hash, Hasher};
use std::ops::RangeInclusive;
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use arrow_array::{ArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use laminar_core::time::cast_to_millis_array;

// ─── window_udf! macro ──────────────────────────────────────────────────────
//
// Generates the trait boilerplate (Default + PartialEq + Eq + Hash +
// ScalarUDFImpl) for a zero-state singleton UDF that returns a
// `Timestamp(Microsecond)`. Each UDF declaration below is just SQL name,
// arity, and a body closure.

macro_rules! window_udf {
    (
        $(#[$attr:meta])*
        $type:ident, $sql_name:literal, $arity:expr, |$args:ident| $body:expr
    ) => {
        $(#[$attr])*
        #[derive(Debug)]
        pub struct $type {
            signature: Signature,
        }

        impl $type {
            /// Constructs the UDF instance.
            #[must_use]
            pub fn new() -> Self {
                Self { signature: variadic_signature($arity) }
            }
        }

        impl Default for $type {
            fn default() -> Self { Self::new() }
        }

        impl PartialEq for $type {
            fn eq(&self, _: &Self) -> bool { true }
        }

        impl Eq for $type {}

        impl Hash for $type {
            fn hash<H: Hasher>(&self, state: &mut H) { $sql_name.hash(state); }
        }

        impl ScalarUDFImpl for $type {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn name(&self) -> &'static str { $sql_name }
            fn signature(&self) -> &Signature { &self.signature }
            fn return_type(&self, _: &[DataType]) -> Result<DataType> {
                Ok(DataType::Timestamp(TimeUnit::Microsecond, None))
            }
            fn invoke_with_args(&self, sfa: ScalarFunctionArgs) -> Result<ColumnarValue> {
                let $args = sfa.args;
                check_arity(&$args, $arity, $sql_name)?;
                $body
            }
        }
    };
}

// ─── UDF declarations ───────────────────────────────────────────────────────

window_udf!(
    /// `tumble(ts, interval [, offset])` — start of the non-overlapping
    /// window containing `ts`. Returns `Timestamp(Microsecond, None)`.
    TumbleWindowStart,
    "tumble",
    2..=3,
    |args| {
        let interval_ms = positive_interval(&args[1], "tumble", "interval")?;
        let offset_ms = optional_offset(&args, 2)?;
        into_us_columnar(&args[0], |ts| tumble_start_ms(ts, interval_ms, offset_ms))
    }
);

window_udf!(
    /// `tumble_end(ts, interval [, offset])` — exclusive upper bound of
    /// the tumble window containing `ts` (i.e. `tumble(...) + interval`).
    TumbleWindowEnd,
    "tumble_end",
    2..=3,
    |args| {
        let interval_ms = positive_interval(&args[1], "tumble_end", "interval")?;
        let offset_ms = optional_offset(&args, 2)?;
        into_us_columnar(&args[0], |ts| {
            tumble_start_ms(ts, interval_ms, offset_ms).saturating_add(interval_ms)
        })
    }
);

window_udf!(
    /// `hop(ts, slide, size [, offset])` — earliest sliding window
    /// (size `size`, sliding by `slide`) that contains `ts`. Full
    /// multi-window assignment is handled by Ring 0.
    HopWindowStart,
    "hop",
    3..=4,
    |args| {
        let slide_ms = positive_interval(&args[1], "hop", "slide")?;
        let size_ms = positive_interval(&args[2], "hop", "size")?;
        let offset_ms = optional_offset(&args, 3)?;
        into_us_columnar(&args[0], |ts| {
            hop_start_ms(ts, slide_ms, size_ms, offset_ms)
        })
    }
);

window_udf!(
    /// `hop_end(ts, slide, size [, offset])` — end of the earliest
    /// sliding window containing `ts` (i.e. `hop(...) + size`).
    HopWindowEnd,
    "hop_end",
    3..=4,
    |args| {
        let slide_ms = positive_interval(&args[1], "hop_end", "slide")?;
        let size_ms = positive_interval(&args[2], "hop_end", "size")?;
        let offset_ms = optional_offset(&args, 3)?;
        into_us_columnar(&args[0], |ts| {
            hop_start_ms(ts, slide_ms, size_ms, offset_ms).saturating_add(size_ms)
        })
    }
);

window_udf!(
    /// `session(ts, gap)` — passthrough that converts `ts` to
    /// microsecond resolution. Real session start/end are data-dependent
    /// and computed by Ring 0; this UDF exists so `GROUP BY session(ts,
    /// gap)` parses. There is no `session_end` UDF for the same reason.
    SessionWindowStart,
    "session",
    2..=2,
    |args| into_us_columnar(&args[0], |ts| ts)
);

window_udf!(
    /// `cumulate(ts, step, size)` — epoch start (size-aligned bucket)
    /// containing `ts`. Per-step cumulating boundaries (which depend on
    /// data) are exposed by Ring 0.
    CumulateWindowStart,
    "cumulate",
    3..=3,
    |args| {
        let (_step_ms, size_ms) = cumulate_intervals(&args, "cumulate")?;
        into_us_columnar(&args[0], |ts| tumble_start_ms(ts, size_ms, 0))
    }
);

window_udf!(
    /// `cumulate_end(ts, step, size)` — exclusive upper bound of the
    /// epoch (size-aligned bucket) containing `ts`.
    CumulateWindowEnd,
    "cumulate_end",
    3..=3,
    |args| {
        let (_step_ms, size_ms) = cumulate_intervals(&args, "cumulate_end")?;
        into_us_columnar(&args[0], |ts| {
            tumble_start_ms(ts, size_ms, 0).saturating_add(size_ms)
        })
    }
);

// ─── Math helpers ──────────────────────────────────────────────────────────
//
// All in milliseconds. Pure i64 → i64.

/// Tumble window start: `floor((ts - offset) / interval) * interval + offset`.
fn tumble_start_ms(ts: i64, interval_ms: i64, offset_ms: i64) -> i64 {
    let adj = ts - offset_ms;
    (adj - adj.rem_euclid(interval_ms)) + offset_ms
}

/// Earliest start of a hopping window of `size` (sliding by `slide`)
/// that contains `ts`.
fn hop_start_ms(ts: i64, slide_ms: i64, size_ms: i64, offset_ms: i64) -> i64 {
    let adj = ts - size_ms + slide_ms - offset_ms;
    (adj - adj.rem_euclid(slide_ms)) + offset_ms
}

// ─── Argument parsing helpers ──────────────────────────────────────────────

/// Builds `Signature::any(N)` or `Signature::one_of([Any(N), …])` for a
/// scalar UDF whose arity covers a contiguous range.
fn variadic_signature(arity: RangeInclusive<usize>) -> Signature {
    let mut arities: Vec<TypeSignature> = arity.map(TypeSignature::Any).collect();
    let inner = if arities.len() == 1 {
        arities.pop().unwrap()
    } else {
        TypeSignature::OneOf(arities)
    };
    Signature::new(inner, Volatility::Immutable)
}

fn check_arity(args: &[ColumnarValue], arity: RangeInclusive<usize>, fn_name: &str) -> Result<()> {
    if arity.contains(&args.len()) {
        return Ok(());
    }
    let expected = if arity.start() == arity.end() {
        format!("exactly {}", arity.start())
    } else {
        format!("{}-{}", arity.start(), arity.end())
    };
    Err(DataFusionError::Plan(format!(
        "{}() requires {} arguments, got {}",
        fn_name,
        expected,
        args.len()
    )))
}

fn positive_interval(value: &ColumnarValue, fn_name: &str, arg_name: &str) -> Result<i64> {
    let ms = extract_interval_ms(value)?;
    if ms <= 0 {
        return Err(DataFusionError::Plan(format!(
            "{fn_name}() {arg_name} must be positive"
        )));
    }
    Ok(ms)
}

fn optional_offset(args: &[ColumnarValue], idx: usize) -> Result<i64> {
    match args.get(idx) {
        Some(value) => extract_interval_ms(value),
        None => Ok(0),
    }
}

fn cumulate_intervals(args: &[ColumnarValue], fn_name: &str) -> Result<(i64, i64)> {
    let step_ms = positive_interval(&args[1], fn_name, "step")?;
    let size_ms = positive_interval(&args[2], fn_name, "size")?;
    if step_ms > size_ms {
        return Err(DataFusionError::Plan(format!(
            "{fn_name}() step must not exceed size"
        )));
    }
    if size_ms % step_ms != 0 {
        return Err(DataFusionError::Plan(format!(
            "{fn_name}() size must be evenly divisible by step"
        )));
    }
    Ok((step_ms, size_ms))
}

/// Extracts a scalar interval in milliseconds. Array intervals are
/// rejected — per-row window sizes are not a valid streaming pattern.
fn extract_interval_ms(value: &ColumnarValue) -> Result<i64> {
    match value {
        ColumnarValue::Scalar(scalar) => scalar_interval_to_ms(scalar),
        ColumnarValue::Array(_) => Err(DataFusionError::NotImplemented(
            "Array interval arguments not supported for window functions".to_string(),
        )),
    }
}

fn scalar_interval_to_ms(scalar: &ScalarValue) -> Result<i64> {
    match scalar {
        ScalarValue::IntervalDayTime(Some(v)) => {
            Ok(i64::from(v.days) * 86_400_000 + i64::from(v.milliseconds))
        }
        ScalarValue::IntervalMonthDayNano(Some(v)) => {
            if v.months != 0 {
                return Err(DataFusionError::NotImplemented(
                    "Month-based intervals not supported for window functions \
                     (use days/hours/minutes/seconds)"
                        .to_string(),
                ));
            }
            Ok(i64::from(v.days) * 86_400_000 + v.nanoseconds / 1_000_000)
        }
        ScalarValue::IntervalYearMonth(_) => Err(DataFusionError::NotImplemented(
            "Year-month intervals not supported for window functions".to_string(),
        )),
        ScalarValue::Int64(Some(ms)) => Ok(*ms),
        _ => Err(DataFusionError::Plan(format!(
            "Expected interval argument for window function, got: {scalar:?}"
        ))),
    }
}

// ─── Dispatch helper ───────────────────────────────────────────────────────

/// Applies `transform` (ms → ms) to every non-null input timestamp and
/// returns a `Timestamp(Microsecond)` result. One allocation per call;
/// null inputs propagate.
fn into_us_columnar(
    value: &ColumnarValue,
    transform: impl Fn(i64) -> i64,
) -> Result<ColumnarValue> {
    match value {
        ColumnarValue::Array(array) => {
            let input = to_millis_array(array)?;
            let result: TimestampMicrosecondArray = input
                .iter()
                .map(|opt| opt.map(|ms| transform(ms).saturating_mul(1000)))
                .collect();
            Ok(ColumnarValue::Array(Arc::new(result)))
        }
        ColumnarValue::Scalar(scalar) => {
            let result =
                scalar_to_timestamp_ms(scalar)?.map(|ms| transform(ms).saturating_mul(1000));
            Ok(ColumnarValue::Scalar(ScalarValue::TimestampMicrosecond(
                result, None,
            )))
        }
    }
}

fn to_millis_array(array: &ArrayRef) -> Result<TimestampMillisecondArray> {
    cast_to_millis_array(array.as_ref()).map_err(|e| DataFusionError::Plan(e.to_string()))
}

fn scalar_to_timestamp_ms(scalar: &ScalarValue) -> Result<Option<i64>> {
    match scalar {
        ScalarValue::TimestampMillisecond(v, _) | ScalarValue::Int64(v) => Ok(*v),
        ScalarValue::TimestampMicrosecond(v, _) => Ok(v.map(|v| v / 1_000)),
        ScalarValue::TimestampNanosecond(v, _) => Ok(v.map(|v| v / 1_000_000)),
        ScalarValue::TimestampSecond(v, _) => Ok(v.map(|v| v * 1_000)),
        _ => Err(DataFusionError::Plan(format!(
            "Expected timestamp argument for window function, got: {scalar:?}"
        ))),
    }
}

#[cfg(test)]
mod tests;

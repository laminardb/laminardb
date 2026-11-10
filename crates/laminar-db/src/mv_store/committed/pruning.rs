//! What a scan's filters let a committed read skip: every vnode frame but one when the filters
//! fix a keyed view's whole key, and the segments of an append view whose event times fall
//! outside the filtered range. Pruning only skips artifacts that hold no matching row; the
//! filters still run over the rows that are read.

use std::num::NonZeroU32;

use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::row::{RowConverter, SortField};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use laminar_core::checkpoint::EventTimeBounds;

use super::ViewShape;
use crate::mv_store::slices::vnode_for_encoded;
use crate::mv_store::{MvStorageMode, ViewKey};

/// Inclusive event-time range in milliseconds, rounded outward.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct TimeRange {
    min: Option<i64>,
    max: Option<i64>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct ScanPruning {
    vnode: Option<u64>,
    time: Option<TimeRange>,
}

impl ScanPruning {
    pub(super) fn new(shape: &ViewShape, filters: &[Expr]) -> Self {
        match &shape.mode {
            MvStorageMode::Changelog { key, key_groups } => {
                let Some(count) = NonZeroU32::new(u32::from(key_groups.get())) else {
                    return Self::default();
                };
                let columns = match key {
                    ViewKey::Keyed(columns) | ViewKey::MultisetByKey(columns) => columns,
                    ViewKey::Global | ViewKey::MultisetByRow => return Self::default(),
                };
                Self {
                    vnode: key_vnode(&shape.schema, columns, count, filters).map(u64::from),
                    time: None,
                }
            }
            MvStorageMode::AppendLog { retention } => match retention.event_time_column() {
                Some(column) => Self {
                    vnode: None,
                    time: Some(time_range(shape.schema.field(column).name(), filters)),
                },
                None => Self::default(),
            },
        }
    }

    pub(super) fn keeps_vnode(&self, vnode: u64) -> bool {
        self.vnode.is_none_or(|wanted| wanted == vnode)
    }

    pub(super) fn keeps_segment(&self, bounds: Option<EventTimeBounds>) -> bool {
        let Some(range) = self.time else {
            return true;
        };
        if range == TimeRange::default() {
            return true;
        }
        // A segment without bounds holds only NULL event times, which no range matches.
        bounds.is_some_and(|bounds| {
            range.min.is_none_or(|min| bounds.max >= min)
                && range.max.is_none_or(|max| bounds.min <= max)
        })
    }
}

/// `column = literal` in either order, with the column's name.
fn equality(filter: &Expr) -> Option<(&str, &ScalarValue)> {
    let Expr::BinaryExpr(BinaryExpr {
        left,
        op: Operator::Eq,
        right,
    }) = filter
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (Expr::Column(column), Expr::Literal(value, _))
        | (Expr::Literal(value, _), Expr::Column(column)) => Some((&column.name, value)),
        _ => None,
    }
}

/// The vnode holding every row whose key the filters fix, when they fix all of it with
/// literals of the key's own types.
fn key_vnode(
    schema: &SchemaRef,
    columns: &[usize],
    count: NonZeroU32,
    filters: &[Expr],
) -> Option<u32> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    let mut fields = Vec::with_capacity(columns.len());
    for &column in columns {
        let field = schema.field(column);
        let value = filters
            .iter()
            .filter_map(equality)
            .find(|(name, _)| *name == field.name())?
            .1;
        if value.is_null() || value.data_type() != *field.data_type() {
            return None;
        }
        arrays.push(value.to_array().ok()?);
        fields.push(SortField::new(field.data_type().clone()));
    }
    let rows = RowConverter::new(fields)
        .ok()?
        .convert_columns(&arrays)
        .ok()?;
    Some(vnode_for_encoded(rows.row(0).as_ref(), count))
}

/// The event-time range the filters on `column` allow.
fn time_range(column: &str, filters: &[Expr]) -> TimeRange {
    let mut range = TimeRange::default();
    for filter in filters {
        for (op, value) in comparisons(column, filter) {
            let Some(lower) = timestamp_millis(value, false) else {
                continue;
            };
            let Some(upper) = timestamp_millis(value, true) else {
                continue;
            };
            if matches!(op, Operator::Gt | Operator::GtEq | Operator::Eq) {
                range.min = Some(range.min.map_or(lower, |min| min.max(lower)));
            }
            if matches!(op, Operator::Lt | Operator::LtEq | Operator::Eq) {
                range.max = Some(range.max.map_or(upper, |max| max.min(upper)));
            }
        }
    }
    range
}

/// `column <op> literal` comparisons in `filter`, with the column on the left.
fn comparisons<'a>(column: &str, filter: &'a Expr) -> Vec<(Operator, &'a ScalarValue)> {
    match filter {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(found), Expr::Literal(value, _)) if found.name == column => {
                vec![(*op, value)]
            }
            (Expr::Literal(value, _), Expr::Column(found)) if found.name == column => {
                op.swap().map(|op| vec![(op, value)]).unwrap_or_default()
            }
            _ => Vec::new(),
        },
        Expr::Between(between) if !between.negated => {
            let (Expr::Column(found), Expr::Literal(low, _), Expr::Literal(high, _)) = (
                between.expr.as_ref(),
                between.low.as_ref(),
                between.high.as_ref(),
            ) else {
                return Vec::new();
            };
            if found.name != column {
                return Vec::new();
            }
            vec![(Operator::GtEq, low), (Operator::LtEq, high)]
        }
        _ => Vec::new(),
    }
}

/// A timestamp literal in milliseconds, rounded down for a lower bound and up for an upper one,
/// the same rounding segment bounds use.
fn timestamp_millis(value: &ScalarValue, upper: bool) -> Option<i64> {
    let (value, per_milli) = match value {
        ScalarValue::TimestampSecond(Some(value), _) => return Some(value.saturating_mul(1000)),
        ScalarValue::TimestampMillisecond(Some(value), _) => return Some(*value),
        ScalarValue::TimestampMicrosecond(Some(value), _) => (*value, 1_000),
        ScalarValue::TimestampNanosecond(Some(value), _) => (*value, 1_000_000),
        _ => return None,
    };
    Some(value.div_euclid(per_milli) + i64::from(upper && value.rem_euclid(per_milli) != 0))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::prelude::{col, lit};

    use super::*;
    use crate::mv_store::admission::MvLimits;
    use crate::mv_store::append_log::ViewRetention;

    fn shape(mode: MvStorageMode) -> ViewShape {
        ViewShape {
            name: "v".into(),
            schema: Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, false),
                Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            ])),
            mode,
            limits: MvLimits {
                rows: 10,
                bytes: 1_000,
            },
        }
    }

    fn ts(millis: i64) -> Expr {
        lit(ScalarValue::TimestampMillisecond(Some(millis), None))
    }

    fn bounds(min: i64, max: i64) -> Option<EventTimeBounds> {
        Some(EventTimeBounds { min, max })
    }

    #[test]
    fn a_full_key_equality_selects_the_vnode_the_view_places_the_key_on() {
        let keyed = shape(MvStorageMode::changelog(ViewKey::Keyed(vec![0])));
        let pruning = ScanPruning::new(&keyed, &[col("k").eq(lit(42_i64))]);
        let mut store = crate::mv_store::MvStore::new();
        store
            .create_mv("v", Arc::clone(&keyed.schema), keyed.mode.clone())
            .unwrap();
        let row = arrow::array::RecordBatch::try_from_iter(vec![
            (
                "k",
                Arc::new(arrow::array::Int64Array::from(vec![42])) as ArrayRef,
            ),
            (
                "ts",
                Arc::new(arrow::array::TimestampMillisecondArray::from(vec![1])) as ArrayRef,
            ),
            (
                laminar_core::changelog::WEIGHT_COLUMN,
                Arc::new(arrow::array::Int64Array::from(vec![1])) as ArrayRef,
            ),
        ])
        .unwrap();
        store.update("v", &row);
        let placed = store
            .full_view_frames("v")
            .into_iter()
            .find(|(_, frame)| frame.as_ref() != crate::mv_store::frames::EMPTY_SLICE_FRAME)
            .map(|(vnode, _)| u64::from(vnode));
        assert_eq!(pruning.vnode, placed);

        for filters in [
            vec![col("k").gt(lit(42_i64))],
            vec![col("k").eq(lit(42_i32))],
            vec![col("ts").eq(ts(1))],
        ] {
            assert_eq!(
                ScanPruning::new(&keyed, &filters).vnode,
                None,
                "{filters:?}"
            );
        }
    }

    #[test]
    fn time_filters_skip_segments_outside_their_range() {
        let append = shape(MvStorageMode::AppendLog {
            retention: ViewRetention::EventTime {
                column: 1,
                interval_ms: 1_000,
            },
        });
        let pruning = ScanPruning::new(&append, &[col("ts").gt_eq(ts(100)), ts(200).gt(col("ts"))]);
        assert!(!pruning.keeps_segment(bounds(0, 99)));
        assert!(pruning.keeps_segment(bounds(0, 100)));
        assert!(pruning.keeps_segment(bounds(200, 300)));
        assert!(!pruning.keeps_segment(bounds(201, 300)));
        assert!(!pruning.keeps_segment(None));

        let between = ScanPruning::new(&append, &[col("ts").between(ts(10), ts(20))]);
        assert!(!between.keeps_segment(bounds(21, 30)));
        assert!(between.keeps_segment(bounds(5, 10)));

        let unfiltered = ScanPruning::new(&append, &[col("k").eq(lit(1_i64))]);
        assert!(unfiltered.keeps_segment(None));

        let capped = shape(MvStorageMode::append_rows(10));
        assert!(ScanPruning::new(&capped, &[col("ts").gt(ts(100))]).keeps_segment(bounds(0, 1)));
    }

    #[test]
    fn sub_millisecond_literals_round_outward() {
        let micros = |value| ScalarValue::TimestampMicrosecond(Some(value), None);
        assert_eq!(timestamp_millis(&micros(1_500), false), Some(1));
        assert_eq!(timestamp_millis(&micros(1_500), true), Some(2));
        assert_eq!(timestamp_millis(&micros(-1_500), false), Some(-2));
        assert_eq!(timestamp_millis(&micros(2_000), true), Some(2));
    }
}

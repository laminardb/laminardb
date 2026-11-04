//! The column whose watermark orders a stream's rows, derived per operator: declared by a source,
//! forwarded by stateless projections, carried through interval joins and closed windows.

use std::time::Duration;

use datafusion_expr::LogicalPlan;
use laminar_sql::parser::join_parser::JoinType;
use laminar_sql::translator::{StreamJoinConfig, WindowOperatorConfig, WindowType};

use super::derivation::{interval_join_plan, projected_join_column, window_boundary_column};

/// A stream carries at most one event-time column. For a derived column,
/// `max_out_of_orderness` is how far the stream's frontier trails its inputs' frontiers; it is
/// `None` when any input's watermark is unbounded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EventTime {
    pub(crate) column: String,
    pub(crate) max_out_of_orderness: Option<Duration>,
    pub(crate) origin: EventTimeOrigin,
}

/// Which operator defines an event-time column. A stateless projection that forwards the column
/// keeps its origin.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EventTimeOrigin {
    /// A source's declared watermark column.
    Source,
    /// An interval join's time column of the side present on every output row.
    IntervalJoin(JoinSide),
    /// A closed `TUMBLE` or `HOP` window's boundary marker.
    Window(WindowBoundary),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinSide {
    Left,
    Right,
}

/// The window boundary a windowed aggregate's output carries as its event time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WindowBoundary {
    /// The start marker, or no boundary column: a later output row may hold any time inside a
    /// window that is still open.
    Start,
    /// A `tumble_end` or `hop_end` marker projected as a bare column.
    End,
}

impl JoinSide {
    /// The sides whose columns every output row of `join_type` carries; a full outer join's
    /// unmatched rows carry only one.
    pub(super) const fn present_on_every_row(join_type: JoinType) -> &'static [Self] {
        match join_type {
            JoinType::Inner => &[Self::Left, Self::Right],
            JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti => &[Self::Left],
            JoinType::Right | JoinType::RightSemi | JoinType::RightAnti => &[Self::Right],
            JoinType::Full => &[],
        }
    }
}

/// An interval join carries the time column of the first side present on every output row that
/// projects it unchanged: for an inner join the left side, then the right. A later output row is
/// never behind the join's frontier `min(L, R - bound)` on either column, because
/// `0 <= right - left <= bound`.
pub(super) fn interval_join_event_time(
    logical: &LogicalPlan,
    config: &StreamJoinConfig,
    (left, right): (Option<&EventTime>, Option<&EventTime>),
) -> Option<EventTime> {
    let (projection, join) = interval_join_plan(logical)?;
    let bounded = left.zip(right).is_some_and(|(left, right)| {
        left.max_out_of_orderness.is_some() && right.max_out_of_orderness.is_some()
    });
    JoinSide::present_on_every_row(config.join_type)
        .iter()
        .find_map(|&side| {
            let time_column = match side {
                JoinSide::Left => &config.left_time_column,
                JoinSide::Right => &config.right_time_column,
            };
            let index = projected_join_column(projection, join, side, time_column)?;
            Some(EventTime {
                column: logical.schema().field(index).name().clone(),
                max_out_of_orderness: bounded.then_some(config.time_bound),
                origin: EventTimeOrigin::IntervalJoin(side),
            })
        })
}

/// A closed `TUMBLE` or `HOP` window over an input ordered by its time column carries its
/// projected end marker, or else its start marker. Its output frontier is held back by the
/// allowed lateness, plus the window size for the start marker. A session's start trails its end
/// without bound, so a session carries none.
pub(super) fn window_event_time(
    logical: &LogicalPlan,
    window: &WindowOperatorConfig,
    input: Option<&EventTime>,
) -> Option<EventTime> {
    let input = input.filter(|input| input.column == window.time_column)?;
    match window.window_type {
        WindowType::Tumbling | WindowType::Sliding => {}
        WindowType::Session | WindowType::Cumulate => return None,
    }
    let (index, boundary) = window_boundary_column(logical)?;
    let hold = match boundary {
        WindowBoundary::End => window.allowed_lateness,
        WindowBoundary::Start => window.allowed_lateness.saturating_add(window.size),
    };
    Some(EventTime {
        column: logical.schema().field(index).name().clone(),
        max_out_of_orderness: input.max_out_of_orderness.map(|_| hold),
        origin: EventTimeOrigin::Window(boundary),
    })
}

//! Plan-time exchange plan: the distribution and changelog kind every stream provides, and how
//! each consumer input port receives rows.
//!
//! The same plan is derived in every mode. It records what the operators do: every keyed port
//! repartitions, and a global aggregate gathers to one vnode.

mod derivation;
mod properties;
#[cfg(all(debug_assertions, feature = "cluster"))]
mod shadow;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use rustc_hash::{FxHashMap, FxHashSet};

pub(crate) use derivation::derive;
pub(crate) use properties::{
    ChangelogKind, Distribution, EventTime, InputPort, KeyColumn, PartitionKey, PortExchange,
    StreamProperties, GLOBAL_VNODE,
};

use crate::error::DbError;

/// One stream's input ports and the properties of its output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StreamPlan {
    pub(crate) inputs: BTreeMap<InputPort, PortExchange>,
    pub(crate) output: StreamProperties,
}

/// Plans for every stream of one catalog, by stream name, with the event time each source
/// declares.
#[derive(Debug, Default)]
pub(crate) struct ExchangePlan {
    streams: BTreeMap<String, StreamPlan>,
    sources: FxHashMap<String, EventTime>,
}

impl ExchangePlan {
    #[cfg(test)]
    pub(crate) fn from_streams(streams: impl IntoIterator<Item = (String, StreamPlan)>) -> Self {
        Self {
            streams: streams.into_iter().collect(),
            sources: FxHashMap::default(),
        }
    }

    pub(crate) fn stream(&self, name: &str) -> Option<&StreamPlan> {
        self.streams.get(name)
    }

    /// Streams whose aggregate input is colocated with its upstream.
    #[cfg(feature = "cluster")]
    pub(crate) fn colocated_aggregates(&self) -> FxHashSet<String> {
        self.streams
            .iter()
            .filter(|(_, plan)| {
                plan.inputs
                    .values()
                    .any(|exchange| matches!(exchange, PortExchange::Colocated(_)))
            })
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Streams that keep one input's rows and event time unchanged (a stateless projection that
    /// forwards the event-time column), so a managed consumer may take that input's frontier.
    #[cfg(feature = "cluster")]
    pub(crate) fn event_time_forwards(&self) -> FxHashSet<String> {
        self.streams
            .iter()
            .filter(|(_, plan)| {
                plan.output.event_time.is_some()
                    && plan.inputs.len() == 1
                    && plan
                        .inputs
                        .values()
                        .all(|exchange| matches!(exchange, PortExchange::Forward))
            })
            .map(|(name, _)| name.clone())
            .collect()
    }

    /// Longest chain of remote exchanges from a source to `stream`: one for each stream on the
    /// chain that reads an input through any port other than `Forward`.
    pub(crate) fn remote_depth(&self, stream: &str) -> usize {
        fn depth<'a>(
            plan: &'a ExchangePlan,
            stream: &'a str,
            memo: &mut FxHashMap<&'a str, usize>,
        ) -> usize {
            if let Some(&known) = memo.get(stream) {
                return known;
            }
            let Some(stream_plan) = plan.streams.get(stream) else {
                return 0;
            };
            let upstream = stream_plan
                .inputs
                .keys()
                .map(|port| depth(plan, port.upstream(), memo))
                .max()
                .unwrap_or(0);
            let exchanges = stream_plan
                .inputs
                .values()
                .any(|exchange| !matches!(exchange, PortExchange::Forward));
            let known = upstream + usize::from(exchanges);
            memo.insert(stream, known);
            known
        }
        depth(self, stream, &mut FxHashMap::default())
    }

    /// What an input port reads from `upstream`: a planned stream's output, otherwise a source,
    /// which is a weighted changelog when `changelog` names it and carries its declared event time.
    pub(crate) fn upstream_properties(
        &self,
        upstream: &str,
        changelog: &FxHashSet<String>,
    ) -> StreamProperties {
        self.streams.get(upstream).map_or_else(
            || StreamProperties {
                distribution: Distribution::Arbitrary,
                changelog: if changelog.contains(upstream) {
                    ChangelogKind::Weighted
                } else {
                    ChangelogKind::AppendOnly
                },
                event_time: self.sources.get(upstream).cloned(),
            },
            |stream| stream.output.clone(),
        )
    }
}

/// Each catalog source's event time.
pub(crate) fn source_event_times(
    catalog: &crate::catalog::SourceCatalog,
) -> FxHashMap<String, EventTime> {
    catalog
        .list_sources()
        .into_iter()
        .filter_map(|name| {
            let event_time = source_event_time(&*catalog.get_source(&name)?)?;
            Some((name, event_time))
        })
        .collect()
}

/// A source's watermark column, unless it orders by processing time.
pub(crate) fn source_event_time(source: &crate::catalog::SourceEntry) -> Option<EventTime> {
    if source
        .is_processing_time
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return None;
    }
    Some(EventTime {
        column: source.watermark_column.clone()?,
        max_out_of_orderness: source.max_out_of_orderness,
    })
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ExchangePlanError {
    #[error("streams [{}] read their own output through a cycle", .0.join(", "))]
    Cycle(Vec<String>),
    #[error("stream '{stream}' has no exchange plan: {reason}")]
    Unplannable { stream: String, reason: String },
}

impl From<ExchangePlanError> for DbError {
    fn from(error: ExchangePlanError) -> Self {
        match error {
            ExchangePlanError::Cycle(_) => Self::InvalidOperation(error.to_string()),
            ExchangePlanError::Unplannable { .. } => Self::Pipeline(error.to_string()),
        }
    }
}

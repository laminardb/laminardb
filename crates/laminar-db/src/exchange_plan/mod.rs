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

use rustc_hash::FxHashSet;

pub(crate) use derivation::derive;
pub(crate) use properties::{
    ChangelogKind, Distribution, InputPort, KeyColumn, PartitionKey, PortExchange,
    StreamProperties, GLOBAL_VNODE,
};

use crate::error::DbError;

/// One stream's input ports and the properties of its output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StreamPlan {
    pub(crate) inputs: BTreeMap<InputPort, PortExchange>,
    pub(crate) output: StreamProperties,
}

/// Plans for every stream of one catalog, by stream name.
#[derive(Debug, Default)]
pub(crate) struct ExchangePlan {
    streams: BTreeMap<String, StreamPlan>,
}

impl ExchangePlan {
    #[cfg(test)]
    pub(crate) fn from_streams(streams: impl IntoIterator<Item = (String, StreamPlan)>) -> Self {
        Self {
            streams: streams.into_iter().collect(),
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

    /// What an input port reads from `upstream`: a planned stream's output, otherwise a source,
    /// which is a weighted changelog when `changelog` names it.
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
            },
            |stream| stream.output.clone(),
        )
    }
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

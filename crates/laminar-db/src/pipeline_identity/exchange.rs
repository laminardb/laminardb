//! Canonical form of a stream's exchange plan: only what decides routing and state layout, with
//! stable field names, so EXPLAIN wording and admission-only properties can change freely.

use serde::Serialize;

use crate::exchange_plan::{
    ChangelogKind, Distribution, InputPort, KeyColumn, PartitionKey, PortExchange, StreamPlan,
};

#[derive(Serialize)]
pub(super) struct CanonicalExchange {
    inputs: Vec<CanonicalPort>,
    distribution: CanonicalDistribution,
    changelog: &'static str,
}

#[derive(Serialize)]
struct CanonicalPort {
    port: &'static str,
    upstream: String,
    exchange: CanonicalPortExchange,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CanonicalPortExchange {
    Forward,
    Colocated { key: Vec<CanonicalKeyColumn> },
    Repartition { key: Vec<CanonicalKeyColumn> },
    Gather { vnode: u32, partial: bool },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CanonicalDistribution {
    Arbitrary,
    Hash { key: Vec<CanonicalKeyColumn> },
    Singleton { vnode: u32 },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CanonicalKeyColumn {
    Column { name: String, data_type: String },
    Computed { expr: String, data_type: String },
}

pub(super) fn canonical_exchange(plan: &StreamPlan) -> CanonicalExchange {
    CanonicalExchange {
        inputs: plan
            .inputs
            .iter()
            .map(|(port, exchange)| {
                let (kind, upstream) = match port {
                    InputPort::Input(name) => ("input", name),
                    InputPort::Left(name) => ("left", name),
                    InputPort::Right(name) => ("right", name),
                };
                CanonicalPort {
                    port: kind,
                    upstream: upstream.clone(),
                    exchange: match exchange {
                        PortExchange::Forward => CanonicalPortExchange::Forward,
                        PortExchange::Colocated(key) => CanonicalPortExchange::Colocated {
                            key: canonical_key(key),
                        },
                        PortExchange::Repartition(key) => CanonicalPortExchange::Repartition {
                            key: canonical_key(key),
                        },
                        PortExchange::Gather { vnode, partial } => CanonicalPortExchange::Gather {
                            vnode: *vnode,
                            partial: *partial,
                        },
                    },
                }
            })
            .collect(),
        distribution: match &plan.output.distribution {
            Distribution::Arbitrary => CanonicalDistribution::Arbitrary,
            Distribution::Hash(key) => CanonicalDistribution::Hash {
                key: canonical_key(key),
            },
            Distribution::Singleton { vnode } => CanonicalDistribution::Singleton { vnode: *vnode },
        },
        changelog: match plan.output.changelog {
            ChangelogKind::AppendOnly => "append_only",
            ChangelogKind::Weighted => "weighted",
            ChangelogKind::RunningState => "running_state",
        },
    }
}

fn canonical_key(key: &PartitionKey) -> Vec<CanonicalKeyColumn> {
    key.0
        .iter()
        .map(|column| match column {
            KeyColumn::Column { name, data_type } => CanonicalKeyColumn::Column {
                name: name.clone(),
                data_type: data_type.to_string(),
            },
            KeyColumn::Computed { expr, data_type } => CanonicalKeyColumn::Computed {
                expr: expr.clone(),
                data_type: data_type.to_string(),
            },
        })
        .collect()
}

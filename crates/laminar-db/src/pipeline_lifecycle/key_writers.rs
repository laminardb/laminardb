//! Whether every row for one destination key of a sink is produced on one cluster node at a time.
//!
//! A sink that applies rows by key needs that: writes for one key from two nodes land in no
//! defined order, and a key's retraction on one node with its new value on another net to a
//! delete.

use laminar_connectors::connector::{SourceContract, SourceTopology};

use crate::exchange_plan::{Distribution, PortExchange};

use super::ResolvedStreamOutputs;

/// Where the rows of a sink's input are produced across the nodes of a cluster.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum InputPlacement {
    /// Every row is produced on one node at a time.
    OneNode,
    /// Every row is produced on the owner of the vnode of these columns.
    HashedBy(Vec<String>),
    /// Rows may be produced on any node.
    AnyNode,
}

impl InputPlacement {
    /// Whether every row for one value of `destination_key` is produced on one node at a time.
    /// Rows that share a destination key share every hash column when the key contains them all,
    /// and so share one vnode. A destination key inside the hash key is not enough: `GROUP BY a,
    /// b` into a sink keyed by `a` produces each `a` on several nodes.
    pub(crate) fn writes_each_key_on_one_node(&self, destination_key: Option<&[String]>) -> bool {
        match (self, destination_key) {
            (_, None) | (Self::AnyNode, _) => false,
            (Self::OneNode, Some(_)) => true,
            (Self::HashedBy(columns), Some(key)) => {
                columns.iter().all(|column| key.contains(column))
            }
        }
    }
}

/// Whether a source with this contract runs on one cluster node at a time: a singleton the engine
/// places on the owner of its home vnode.
pub(crate) fn runs_on_one_node(contract: SourceContract) -> bool {
    contract.topology == SourceTopology::Singleton
        && contract.supports_cluster_singleton_placement()
}

impl ResolvedStreamOutputs {
    /// Where `input`'s rows are produced. A source is on one node when `single_node_source`
    /// says so, as a placed singleton is; a projection is on one node when its one input is,
    /// because it forwards rows where they are.
    pub(crate) fn input_placement(
        &self,
        input: &str,
        single_node_source: &dyn Fn(&str) -> bool,
    ) -> InputPlacement {
        let Some(stream) = self.exchange_plan.stream(input) else {
            return if single_node_source(input) {
                InputPlacement::OneNode
            } else {
                InputPlacement::AnyNode
            };
        };
        match &stream.output.distribution {
            Distribution::Singleton { .. } => InputPlacement::OneNode,
            Distribution::Hash(key) => {
                key.column_names().map_or(InputPlacement::AnyNode, |names| {
                    InputPlacement::HashedBy(names.into_iter().map(str::to_owned).collect())
                })
            }
            Distribution::Arbitrary => {
                let mut ports = stream.inputs.iter();
                let forwards_one_node = self.projections.contains(input)
                    && match (ports.next(), ports.next()) {
                        (Some((port, PortExchange::Forward)), None) => {
                            self.input_placement(port.upstream(), single_node_source)
                                == InputPlacement::OneNode
                        }
                        _ => false,
                    };
                if forwards_one_node {
                    InputPlacement::OneNode
                } else {
                    InputPlacement::AnyNode
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(columns: &[&str]) -> Vec<String> {
        columns.iter().map(|column| (*column).to_owned()).collect()
    }

    #[test]
    fn a_destination_key_holding_every_hash_column_has_one_writer() {
        let hashed = InputPlacement::HashedBy(key(&["a"]));
        assert!(hashed.writes_each_key_on_one_node(Some(&key(&["a"]))));
        assert!(hashed.writes_each_key_on_one_node(Some(&key(&["b", "a"]))));
    }

    #[test]
    fn a_destination_key_inside_the_hash_key_has_several_writers() {
        let hashed = InputPlacement::HashedBy(key(&["a", "b"]));
        assert!(!hashed.writes_each_key_on_one_node(Some(&key(&["a"]))));
    }

    #[test]
    fn placement_without_a_key_proves_nothing() {
        assert!(!InputPlacement::OneNode.writes_each_key_on_one_node(None));
        assert!(!InputPlacement::AnyNode.writes_each_key_on_one_node(Some(&key(&["a"]))));
        assert!(InputPlacement::OneNode.writes_each_key_on_one_node(Some(&key(&["a"]))));
    }
}

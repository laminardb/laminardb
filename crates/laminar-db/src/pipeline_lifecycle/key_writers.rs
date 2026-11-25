//! Whether every row for one destination key of a sink is produced on one cluster node at a time.
//!
//! A sink that applies rows by key needs that: writes for one key from two nodes land in no
//! defined order, and a key's retraction on one node with its new value on another net to a
//! delete.

use crate::exchange_plan::{Distribution, KeyColumn, PartitionKey, PortExchange};

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

/// How many nodes can produce rows for one destination key of a sink at a time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyWriters {
    One,
    Several,
}

impl InputPlacement {
    /// Rows that share a destination key share every hash column when the destination key
    /// contains them all, and so share one vnode. A destination key inside the hash key is not
    /// enough: `GROUP BY a, b` into a sink keyed by `a` produces each `a` on several nodes.
    pub(crate) fn key_writers(&self, destination_key: Option<&[String]>) -> KeyWriters {
        let Some(destination_key) = destination_key else {
            return KeyWriters::Several;
        };
        let one = match self {
            Self::OneNode => true,
            Self::HashedBy(columns) => columns.iter().all(|column| destination_key.contains(column)),
            Self::AnyNode => false,
        };
        if one {
            KeyWriters::One
        } else {
            KeyWriters::Several
        }
    }
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
            Distribution::Hash(PartitionKey(columns)) => columns
                .iter()
                .map(|column| match column {
                    KeyColumn::Column { name, .. } => Some(name.clone()),
                    KeyColumn::Computed { .. } => None,
                })
                .collect::<Option<Vec<_>>>()
                .map_or(InputPlacement::AnyNode, InputPlacement::HashedBy),
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
        assert_eq!(hashed.key_writers(Some(&key(&["a"]))), KeyWriters::One);
        assert_eq!(hashed.key_writers(Some(&key(&["b", "a"]))), KeyWriters::One);
    }

    #[test]
    fn a_destination_key_inside_the_hash_key_has_several_writers() {
        let hashed = InputPlacement::HashedBy(key(&["a", "b"]));
        assert_eq!(hashed.key_writers(Some(&key(&["a"]))), KeyWriters::Several);
    }

    #[test]
    fn placement_without_a_key_proves_nothing() {
        assert_eq!(InputPlacement::OneNode.key_writers(None), KeyWriters::Several);
        assert_eq!(
            InputPlacement::AnyNode.key_writers(Some(&key(&["a"]))),
            KeyWriters::Several
        );
        assert_eq!(
            InputPlacement::OneNode.key_writers(Some(&key(&["a"]))),
            KeyWriters::One
        );
    }
}

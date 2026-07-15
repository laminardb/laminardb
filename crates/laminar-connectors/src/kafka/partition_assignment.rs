//! Deterministic Kafka partition-to-vnode assignment.
//!
//! Cluster sources use a stable source and input identity to map each external
//! partition to one engine vnode. The resulting vnode is then resolved through
//! the current immutable [`VnodeRegistry`](laminar_core::state::VnodeRegistry)
//! publication.

use laminar_core::state::{key_hash, NodeId};

use crate::error::ConnectorError;

const KAFKA_PARTITION_ROUTE_DOMAIN: &[u8] = b"laminardb.kafka.partition-route.v1\0";

fn invalid_assignment(message: impl Into<String>) -> ConnectorError {
    ConnectorError::ConfigurationError(message.into())
}

fn route_prefix(source_identity: &str, topic: &str) -> Result<Vec<u8>, ConnectorError> {
    if source_identity.is_empty() {
        return Err(invalid_assignment(
            "Kafka vnode assignment requires a non-empty canonical source identity",
        ));
    }
    if topic.is_empty() {
        return Err(invalid_assignment(
            "Kafka vnode assignment requires a non-empty topic",
        ));
    }
    let source_len = u64::try_from(source_identity.len())
        .map_err(|_| invalid_assignment("Kafka source identity is too long"))?;
    let topic_len =
        u64::try_from(topic.len()).map_err(|_| invalid_assignment("Kafka topic is too long"))?;
    let capacity = KAFKA_PARTITION_ROUTE_DOMAIN
        .len()
        .checked_add(8)
        .and_then(|size| size.checked_add(source_identity.len()))
        .and_then(|size| size.checked_add(8))
        .and_then(|size| size.checked_add(topic.len()))
        .and_then(|size| size.checked_add(4))
        .ok_or_else(|| invalid_assignment("Kafka partition route identity is too long"))?;
    let mut encoded = Vec::with_capacity(capacity);
    encoded.extend_from_slice(KAFKA_PARTITION_ROUTE_DOMAIN);
    encoded.extend_from_slice(&source_len.to_le_bytes());
    encoded.extend_from_slice(source_identity.as_bytes());
    encoded.extend_from_slice(&topic_len.to_le_bytes());
    encoded.extend_from_slice(topic.as_bytes());
    Ok(encoded)
}

fn vnode_from_prefix(
    prefix: &mut Vec<u8>,
    partition: i32,
    vnode_count: u32,
) -> Result<u32, ConnectorError> {
    if partition < 0 {
        return Err(invalid_assignment(format!(
            "Kafka partition id must be nonnegative, got {partition}"
        )));
    }
    if vnode_count == 0 {
        return Err(invalid_assignment(
            "Kafka vnode assignment cannot use an empty owner map",
        ));
    }
    let partition = u32::try_from(partition)
        .map_err(|_| invalid_assignment("Kafka partition id is outside the u32 route domain"))?;
    prefix.extend_from_slice(&partition.to_le_bytes());
    let hash = key_hash(prefix);
    prefix.truncate(prefix.len() - 4);
    let vnode = hash % u64::from(vnode_count);
    u32::try_from(vnode)
        .map_err(|_| invalid_assignment("Kafka vnode count exceeds the supported u32 range"))
}

/// Map one Kafka input partition to an engine vnode.
///
/// The ABI is domain separated and length delimited. Source and topic names are
/// both part of the identity, preventing unrelated inputs with the same Kafka
/// partition number from being forced onto the same vnode.
///
/// # Errors
/// Returns a configuration error for an empty identity/topic/owner map or a
/// negative partition id.
pub fn partition_vnode(
    source_identity: &str,
    topic: &str,
    partition: i32,
    vnode_count: u32,
) -> Result<u32, ConnectorError> {
    let mut prefix = route_prefix(source_identity, topic)?;
    vnode_from_prefix(&mut prefix, partition, vnode_count)
}

/// Kafka partitions owned under one immutable vnode-owner publication.
///
/// This avoids constructing a mixed ownership set if a rotation lands while
/// the caller enumerates partitions.
///
/// # Errors
/// Returns a configuration error when the external inventory or owner map is
/// not canonical.
pub(crate) fn owned_partitions_in_assignment(
    source_identity: &str,
    topic: &str,
    total_partitions: i32,
    assignment: &[NodeId],
    self_id: NodeId,
) -> Result<Vec<i32>, ConnectorError> {
    if total_partitions < 0 {
        return Err(invalid_assignment(format!(
            "Kafka partition count must be nonnegative, got {total_partitions}"
        )));
    }
    if assignment.is_empty() {
        return Err(invalid_assignment(
            "Kafka vnode assignment cannot use an empty owner map",
        ));
    }
    if self_id.is_unassigned() {
        return Err(invalid_assignment(
            "Kafka vnode ownership requires a nonzero node identity",
        ));
    }
    let vnode_count = u32::try_from(assignment.len())
        .map_err(|_| invalid_assignment("Kafka vnode owner map exceeds the supported u32 range"))?;
    let mut prefix = route_prefix(source_identity, topic)?;
    let mut owned = Vec::new();
    for partition in 0..total_partitions {
        let vnode = vnode_from_prefix(&mut prefix, partition, vnode_count)?;
        let vnode_index = usize::try_from(vnode).map_err(|_| {
            invalid_assignment("Kafka vnode id cannot be represented on this platform")
        })?;
        if assignment[vnode_index] == self_id {
            owned.push(partition);
        }
    }
    Ok(owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use laminar_core::state::VnodeRegistry;
    use std::sync::Arc;

    const SOURCE: &str = "orders_source";
    const TOPIC: &str = "orders.v1";

    #[test]
    fn mapping_has_hard_coded_abi_vectors() {
        let actual: Vec<u32> = (0..8)
            .map(|partition| partition_vnode(SOURCE, TOPIC, partition, 256).unwrap())
            .collect();
        assert_eq!(actual, [106, 108, 234, 55, 225, 196, 217, 62]);
    }

    #[test]
    fn source_and_topic_are_both_routing_domains() {
        let base = partition_vnode(SOURCE, TOPIC, 7, 65_521).unwrap();
        assert_ne!(
            base,
            partition_vnode("returns_source", TOPIC, 7, 65_521).unwrap()
        );
        assert_ne!(
            base,
            partition_vnode(SOURCE, "returns.v1", 7, 65_521).unwrap()
        );
        assert_ne!(
            partition_vnode("ab", "c", 7, 65_521).unwrap(),
            partition_vnode("a", "bc", 7, 65_521).unwrap(),
            "length prefixes must prevent concatenation ambiguity"
        );
    }

    #[test]
    fn union_of_owners_covers_every_partition_once() {
        let owners = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
        let assignment: Vec<NodeId> = (0..256).map(|vnode| owners[vnode % owners.len()]).collect();
        let total = 1_024;
        let mut all = Vec::new();
        for owner in owners {
            all.extend(
                owned_partitions_in_assignment(SOURCE, TOPIC, total, &assignment, owner).unwrap(),
            );
        }
        all.sort_unstable();
        assert_eq!(all, (0..total).collect::<Vec<_>>());
    }

    #[test]
    fn registry_reassignment_moves_the_complete_inventory() {
        let registry = VnodeRegistry::new(8);
        registry.set_assignment(Arc::from([NodeId(1); 8]));
        assert_eq!(
            owned_partitions_in_assignment(SOURCE, TOPIC, 16, &registry.snapshot(), NodeId(1),)
                .unwrap(),
            (0..16).collect::<Vec<_>>()
        );
        registry.set_assignment(Arc::from([NodeId(2); 8]));
        let assignment = registry.snapshot();
        assert!(
            owned_partitions_in_assignment(SOURCE, TOPIC, 16, &assignment, NodeId(1))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            owned_partitions_in_assignment(SOURCE, TOPIC, 16, &assignment, NodeId(2)).unwrap(),
            (0..16).collect::<Vec<_>>()
        );
    }

    #[test]
    fn malformed_inputs_are_rejected() {
        assert!(partition_vnode("", TOPIC, 0, 8).is_err());
        assert!(partition_vnode(SOURCE, "", 0, 8).is_err());
        assert!(partition_vnode(SOURCE, TOPIC, -1, 8).is_err());
        assert!(partition_vnode(SOURCE, TOPIC, 0, 0).is_err());
        assert!(owned_partitions_in_assignment(SOURCE, TOPIC, 1, &[], NodeId(1)).is_err());
        assert!(owned_partitions_in_assignment(
            SOURCE,
            TOPIC,
            1,
            &[NodeId::UNASSIGNED],
            NodeId::UNASSIGNED
        )
        .is_err());
        assert!(
            owned_partitions_in_assignment(SOURCE, TOPIC, -1, &[NodeId(1)], NodeId(1)).is_err()
        );
    }
}

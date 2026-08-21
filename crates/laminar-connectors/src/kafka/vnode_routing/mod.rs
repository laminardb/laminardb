//! Deterministic Kafka partition-to-vnode routing.
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

pub(super) fn validate_owner_map(
    assignment: &[NodeId],
    self_id: NodeId,
) -> Result<(), ConnectorError> {
    if assignment.is_empty() {
        return Err(invalid_assignment(
            "Kafka vnode assignment cannot use an empty owner map",
        ));
    }
    if let Some(vnode) = assignment.iter().position(NodeId::is_unassigned) {
        return Err(invalid_assignment(format!(
            "Kafka vnode owner map contains an unassigned owner at vnode {vnode}"
        )));
    }
    if self_id.is_unassigned() {
        return Err(invalid_assignment(
            "Kafka vnode ownership requires a nonzero node identity",
        ));
    }
    Ok(())
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
    if vnode_count == 0 {
        return Err(invalid_assignment(
            "Kafka vnode assignment cannot use an empty owner map",
        ));
    }
    let partition = u32::try_from(partition).map_err(|_| {
        invalid_assignment(format!(
            "Kafka partition id must be nonnegative, got {partition}"
        ))
    })?;
    prefix.extend_from_slice(&partition.to_le_bytes());
    let hash = key_hash(prefix);
    prefix.truncate(prefix.len() - 4);
    let vnode = hash % u64::from(vnode_count);
    u32::try_from(vnode).map_err(|_| invalid_assignment("computed Kafka vnode does not fit in u32"))
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
#[cfg(test)]
pub(super) fn partition_vnode(
    source_identity: &str,
    topic: &str,
    partition: i32,
    vnode_count: u32,
) -> Result<u32, ConnectorError> {
    let mut prefix = route_prefix(source_identity, topic)?;
    vnode_from_prefix(&mut prefix, partition, vnode_count)
}

/// Map a complete Kafka topic inventory to engine vnodes.
///
/// # Errors
/// Returns a configuration error for a negative partition count, empty
/// identity/topic/owner map, or a count unsupported by the current platform.
pub(super) fn partition_vnodes(
    source_identity: &str,
    topic: &str,
    total_partitions: i32,
    vnode_count: u32,
) -> Result<Vec<u32>, ConnectorError> {
    if total_partitions < 0 {
        return Err(invalid_assignment(format!(
            "Kafka partition count must be nonnegative, got {total_partitions}"
        )));
    }
    if vnode_count == 0 {
        return Err(invalid_assignment(
            "Kafka vnode assignment cannot use an empty owner map",
        ));
    }
    let capacity = usize::try_from(total_partitions).map_err(|_| {
        invalid_assignment("Kafka partition count cannot be represented on this platform")
    })?;
    let mut prefix = route_prefix(source_identity, topic)?;
    let mut routes = Vec::with_capacity(capacity);
    for partition in 0..total_partitions {
        routes.push(vnode_from_prefix(&mut prefix, partition, vnode_count)?);
    }
    Ok(routes)
}

/// Kafka partitions owned under one immutable vnode-owner publication.
///
/// This avoids constructing a mixed ownership set if a rotation lands while
/// the caller enumerates partitions.
///
/// # Errors
/// Returns a configuration error when the external inventory or owner map is
/// not canonical.
pub(super) fn owned_partitions_in_assignment(
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
    validate_owner_map(assignment, self_id)?;
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
mod tests;

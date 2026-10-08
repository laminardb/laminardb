//! Complete partition rounds reuse the single-channel replay and watermark contract.

use sha2::{Digest, Sha256};

use super::{
    Arc, BinaryBuilder, ConnectorError, KafkaPartitionSet, KafkaReplayOrder, KafkaSourceConfig,
    SourceRowPositions, UInt32Array,
};

pub(super) fn validate_round_inventory(
    config: &KafkaSourceConfig,
    inventory: &KafkaPartitionSet,
) -> Result<(), ConnectorError> {
    if config.replay_order == KafkaReplayOrder::PartitionRounds
        && (inventory.is_empty() || inventory.len() > config.max_poll_records)
    {
        return Err(ConnectorError::ConfigurationError(format!(
            "Kafka partition_rounds inventory ({}) must fit max.poll.records ({})",
            inventory.len(),
            config.max_poll_records
        )));
    }
    Ok(())
}

pub(super) fn round_input_channels(channels: &[Vec<u8>]) -> Arc<[Vec<u8>]> {
    if channels.is_empty() {
        return Arc::from([]);
    }
    let mut digest = Sha256::new();
    digest.update(b"laminardb:kafka:partition-rounds:v1\0");
    for channel in channels {
        digest.update((channel.len() as u64).to_be_bytes());
        digest.update(channel);
    }
    let mut channel = b"kafka-partition-rounds-v1\0".to_vec();
    channel.extend_from_slice(&digest.finalize());
    Arc::from([channel])
}

pub(super) fn round_row_positions(
    channel: &[u8],
    positions: &[(Arc<str>, i32, i64)],
) -> Result<SourceRowPositions, ConnectorError> {
    if positions.is_empty()
        || channel.is_empty()
        || positions
            .iter()
            .any(|(_, partition, offset)| *partition < 0 || *offset < 0)
        || positions
            .windows(2)
            .any(|pair| (pair[0].0.as_ref(), pair[0].1) >= (pair[1].0.as_ref(), pair[1].1))
    {
        return Err(ConnectorError::Internal(
            "Kafka partition_rounds received invalid or unordered partition positions".into(),
        ));
    }
    // Each partition advances in a complete round, so the maximum native offset strictly
    // increases without a separate counter. Native offset vectors remain the recovery cursor.
    let max_offset = positions
        .iter()
        .fold(0, |maximum, (_, _, offset)| maximum.max(*offset));
    let mut order_key = max_offset.to_be_bytes();
    order_key[0] ^= 0x80;
    let mut partitions = BinaryBuilder::with_capacity(
        positions.len(),
        positions.len().saturating_mul(channel.len()),
    );
    let mut orders =
        BinaryBuilder::with_capacity(positions.len(), positions.len().saturating_mul(8));
    let mut ordinals = Vec::with_capacity(positions.len());
    for index in 0..positions.len() {
        partitions.append_value(channel);
        orders.append_value(order_key);
        ordinals.push(
            u32::try_from(index).map_err(|_| {
                ConnectorError::Internal("Kafka partition ordinal exceeds u32".into())
            })?,
        );
    }
    SourceRowPositions::try_new(
        partitions.finish(),
        orders.finish(),
        UInt32Array::from(ordinals),
    )
}

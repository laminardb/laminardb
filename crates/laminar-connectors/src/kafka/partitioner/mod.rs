//! Kafka partitioning strategies.
//!
//! [`KafkaPartitioner`] determines which Kafka partition each record is
//! sent to. Three strategies are provided:
//!
//! - [`KeyHashPartitioner`]: Murmur2 hash (Kafka-compatible default)
//! - [`RoundRobinPartitioner`]: Cycle through partitions
//! - [`StickyPartitioner`]: Batch to one partition until threshold

/// Trait for determining the target Kafka partition for a record.
///
/// Implementations may be stateful (e.g., round-robin counter).
pub trait KafkaPartitioner: Send + Sync {
    /// Returns the target partition for the given key.
    ///
    /// Returns `None` if the partitioner defers to the broker (librdkafka)
    /// default partitioning.
    fn partition(&mut self, key: Option<&[u8]>, num_partitions: i32) -> Option<i32>;
}

/// Key-hash partitioner using Murmur2 (Kafka-compatible).
///
/// Produces the same partition assignment as Kafka's `DefaultPartitioner`
/// for the same key bytes.
#[derive(Debug, Default)]
pub struct KeyHashPartitioner;

impl KeyHashPartitioner {
    /// Uses Kafka-compatible Murmur2 hash on the key bytes.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl KafkaPartitioner for KeyHashPartitioner {
    fn partition(&mut self, key: Option<&[u8]>, num_partitions: i32) -> Option<i32> {
        key.map(|k| {
            let hash = murmur2(k) & 0x7fff_ffff;
            #[allow(
                clippy::cast_possible_truncation,
                clippy::cast_possible_wrap,
                clippy::cast_sign_loss
            )]
            let partition = (hash % num_partitions as u32) as i32;
            partition
        })
    }
}

/// Round-robin partitioner distributing records evenly across partitions.
#[derive(Debug)]
pub struct RoundRobinPartitioner {
    counter: u64,
}

impl RoundRobinPartitioner {
    /// Cycles through partitions sequentially, ignoring keys.
    #[must_use]
    pub fn new() -> Self {
        Self { counter: 0 }
    }
}

impl Default for RoundRobinPartitioner {
    fn default() -> Self {
        Self::new()
    }
}

impl KafkaPartitioner for RoundRobinPartitioner {
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn partition(&mut self, _key: Option<&[u8]>, num_partitions: i32) -> Option<i32> {
        let partition = (self.counter % num_partitions as u64) as i32;
        self.counter += 1;
        Some(partition)
    }
}

/// Sticky partitioner that batches records to the same partition.
///
/// Once `batch_threshold` records have been sent to one partition, rotates
/// to the next. Reduces broker round-trips (Kafka KIP-794).
#[derive(Debug)]
pub struct StickyPartitioner {
    current_partition: i32,
    records_in_batch: usize,
    batch_threshold: usize,
}

impl StickyPartitioner {
    /// Creates a new sticky partitioner with the given batch threshold.
    #[must_use]
    pub fn new(batch_threshold: usize) -> Self {
        Self {
            current_partition: 0,
            records_in_batch: 0,
            batch_threshold,
        }
    }
}

impl KafkaPartitioner for StickyPartitioner {
    fn partition(&mut self, _key: Option<&[u8]>, num_partitions: i32) -> Option<i32> {
        if self.records_in_batch >= self.batch_threshold {
            self.current_partition = (self.current_partition + 1) % num_partitions;
            self.records_in_batch = 0;
        }
        self.records_in_batch += 1;
        Some(self.current_partition)
    }
}

/// Murmur2 hash function compatible with Kafka's `DefaultPartitioner`.
///
/// This is the 32-bit version used by Kafka for key-based partitioning.
fn murmur2(data: &[u8]) -> u32 {
    let seed: u32 = 0x9747_b28c;
    let m: u32 = 0x5bd1_e995;
    let r: u32 = 24;

    let len = data.len();
    #[allow(clippy::cast_possible_truncation)] // MurmurHash2 spec: seed XOR with 32-bit length
    let mut h: u32 = seed ^ (len as u32);

    let chunks = len / 4;
    for i in 0..chunks {
        let offset = i * 4;
        let mut k = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        k = k.wrapping_mul(m);
        k ^= k >> r;
        k = k.wrapping_mul(m);
        h = h.wrapping_mul(m);
        h ^= k;
    }

    let remainder = len % 4;
    let tail_start = chunks * 4;
    if remainder >= 3 {
        h ^= u32::from(data[tail_start + 2]) << 16;
    }
    if remainder >= 2 {
        h ^= u32::from(data[tail_start + 1]) << 8;
    }
    if remainder >= 1 {
        h ^= u32::from(data[tail_start]);
        h = h.wrapping_mul(m);
    }

    h ^= h >> 13;
    h = h.wrapping_mul(m);
    h ^= h >> 15;

    h
}

#[cfg(test)]
mod tests;

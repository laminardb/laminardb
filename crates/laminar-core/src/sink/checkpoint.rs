//! Sink checkpoint management for exactly-once recovery

#![allow(clippy::cast_possible_truncation)]

use std::collections::HashMap;

use super::traits::TransactionId;
use super::error::SinkError;

/// Offset tracking for sink partitions
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkOffset {
    /// Numeric offset (Kafka-style)
    Numeric(u64),
    /// String offset (some systems use string identifiers)
    String(String),
    /// Binary offset (opaque bytes)
    Binary(Vec<u8>),
}

impl SinkOffset {
    /// Serialize to bytes
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Self::Numeric(n) => {
                let mut bytes = vec![0u8]; // Type tag
                bytes.extend_from_slice(&n.to_le_bytes());
                bytes
            }
            Self::String(s) => {
                let mut bytes = vec![1u8]; // Type tag
                bytes.extend_from_slice(&(s.len() as u32).to_le_bytes());
                bytes.extend_from_slice(s.as_bytes());
                bytes
            }
            Self::Binary(b) => {
                let mut bytes = vec![2u8]; // Type tag
                bytes.extend_from_slice(&(b.len() as u32).to_le_bytes());
                bytes.extend_from_slice(b);
                bytes
            }
        }
    }

    /// Deserialize from bytes, returns `(offset, bytes_consumed)`
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<(Self, usize)> {
        if bytes.is_empty() {
            return None;
        }

        match bytes[0] {
            0 => {
                // Numeric
                if bytes.len() < 9 {
                    return None;
                }
                let n = u64::from_le_bytes(bytes[1..9].try_into().ok()?);
                Some((Self::Numeric(n), 9))
            }
            1 => {
                // String
                if bytes.len() < 5 {
                    return None;
                }
                let len = u32::from_le_bytes(bytes[1..5].try_into().ok()?) as usize;
                if bytes.len() < 5 + len {
                    return None;
                }
                let s = String::from_utf8_lossy(&bytes[5..5 + len]).to_string();
                Some((Self::String(s), 5 + len))
            }
            2 => {
                // Binary
                if bytes.len() < 5 {
                    return None;
                }
                let len = u32::from_le_bytes(bytes[1..5].try_into().ok()?) as usize;
                if bytes.len() < 5 + len {
                    return None;
                }
                let b = bytes[5..5 + len].to_vec();
                Some((Self::Binary(b), 5 + len))
            }
            _ => None,
        }
    }
}

/// Checkpoint data for exactly-once sinks
///
/// This captures the complete state needed to recover a sink after failure:
/// - Sink identification
/// - Per-partition offsets (for resumption)
/// - Pending transaction ID (for rollback on recovery)
/// - Custom metadata
#[derive(Debug, Clone)]
pub struct SinkCheckpoint {
    /// Unique identifier for the sink
    sink_id: String,

    /// Offsets per partition/topic
    offsets: HashMap<String, SinkOffset>,

    /// Pending transaction ID (if any) - needs rollback on recovery
    pending_transaction: Option<TransactionId>,

    /// Epoch number for this checkpoint
    epoch: u64,

    /// Timestamp when checkpoint was created
    timestamp: u64,

    /// Custom metadata for sink-specific state
    metadata: HashMap<String, Vec<u8>>,
}

impl SinkCheckpoint {
    /// Create a new sink checkpoint
    #[must_use]
    pub fn new(sink_id: impl Into<String>) -> Self {
        Self {
            sink_id: sink_id.into(),
            offsets: HashMap::new(),
            pending_transaction: None,
            epoch: 0,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            metadata: HashMap::new(),
        }
    }

    /// Get the sink ID
    #[must_use]
    pub fn sink_id(&self) -> &str {
        &self.sink_id
    }

    /// Get the epoch number
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Set the epoch number
    pub fn set_epoch(&mut self, epoch: u64) {
        self.epoch = epoch;
    }

    /// Get the timestamp
    #[must_use]
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Set an offset for a partition
    pub fn set_offset(&mut self, partition: impl Into<String>, offset: SinkOffset) {
        self.offsets.insert(partition.into(), offset);
    }

    /// Get an offset for a partition
    #[must_use]
    pub fn get_offset(&self, partition: &str) -> Option<&SinkOffset> {
        self.offsets.get(partition)
    }

    /// Get all offsets
    #[must_use]
    pub fn offsets(&self) -> &HashMap<String, SinkOffset> {
        &self.offsets
    }

    /// Set the pending transaction ID
    pub fn set_transaction_id(&mut self, tx_id: Option<TransactionId>) {
        self.pending_transaction = tx_id;
    }

    /// Get the pending transaction ID
    #[must_use]
    pub fn pending_transaction_id(&self) -> Option<&TransactionId> {
        self.pending_transaction.as_ref()
    }

    /// Set custom metadata
    pub fn set_metadata(&mut self, key: impl Into<String>, value: Vec<u8>) {
        self.metadata.insert(key.into(), value);
    }

    /// Get custom metadata
    #[must_use]
    pub fn get_metadata(&self, key: &str) -> Option<&[u8]> {
        self.metadata.get(key).map(Vec::as_slice)
    }

    /// Serialize checkpoint to bytes for persistence
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        // Format:
        // [version: 1][sink_id_len: 4][sink_id][epoch: 8][timestamp: 8]
        // [has_tx: 1][tx_bytes_len: 4][tx_bytes]?
        // [num_offsets: 4][offset entries...]
        // [num_metadata: 4][metadata entries...]

        let mut bytes = Vec::new();

        // Version
        bytes.push(1u8);

        // Sink ID
        bytes.extend_from_slice(&(self.sink_id.len() as u32).to_le_bytes());
        bytes.extend_from_slice(self.sink_id.as_bytes());

        // Epoch and timestamp
        bytes.extend_from_slice(&self.epoch.to_le_bytes());
        bytes.extend_from_slice(&self.timestamp.to_le_bytes());

        // Pending transaction
        if let Some(ref tx) = self.pending_transaction {
            bytes.push(1u8);
            let tx_bytes = tx.to_bytes();
            bytes.extend_from_slice(&(tx_bytes.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&tx_bytes);
        } else {
            bytes.push(0u8);
        }

        // Offsets
        bytes.extend_from_slice(&(self.offsets.len() as u32).to_le_bytes());
        for (partition, offset) in &self.offsets {
            bytes.extend_from_slice(&(partition.len() as u32).to_le_bytes());
            bytes.extend_from_slice(partition.as_bytes());
            let offset_bytes = offset.to_bytes();
            bytes.extend_from_slice(&(offset_bytes.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&offset_bytes);
        }

        // Metadata
        bytes.extend_from_slice(&(self.metadata.len() as u32).to_le_bytes());
        for (key, value) in &self.metadata {
            bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
            bytes.extend_from_slice(key.as_bytes());
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value);
        }

        bytes
    }

    /// Deserialize checkpoint from bytes
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are malformed or the version is unsupported.
    ///
    /// # Panics
    ///
    /// Will not panic - all array conversions are bounds-checked before unwrapping.
    #[allow(clippy::missing_panics_doc)]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SinkError> {
        if bytes.is_empty() {
            return Err(SinkError::CheckpointError("Empty checkpoint data".to_string()));
        }

        let mut pos = 0;

        // Version
        let version = bytes[pos];
        pos += 1;
        if version != 1 {
            return Err(SinkError::CheckpointError(format!(
                "Unsupported checkpoint version: {version}"
            )));
        }

        // Helper to read u32 length
        let read_u32 = |pos: &mut usize| -> Result<u32, SinkError> {
            if *pos + 4 > bytes.len() {
                return Err(SinkError::CheckpointError("Unexpected end of data".to_string()));
            }
            let val = u32::from_le_bytes(bytes[*pos..*pos + 4].try_into().unwrap());
            *pos += 4;
            Ok(val)
        };

        // Helper to read u64
        let read_u64 = |pos: &mut usize| -> Result<u64, SinkError> {
            if *pos + 8 > bytes.len() {
                return Err(SinkError::CheckpointError("Unexpected end of data".to_string()));
            }
            let val = u64::from_le_bytes(bytes[*pos..*pos + 8].try_into().unwrap());
            *pos += 8;
            Ok(val)
        };

        // Sink ID
        let sink_id_len = read_u32(&mut pos)? as usize;
        if pos + sink_id_len > bytes.len() {
            return Err(SinkError::CheckpointError("Invalid sink_id length".to_string()));
        }
        let sink_id = String::from_utf8_lossy(&bytes[pos..pos + sink_id_len]).to_string();
        pos += sink_id_len;

        // Epoch and timestamp
        let epoch = read_u64(&mut pos)?;
        let timestamp = read_u64(&mut pos)?;

        // Pending transaction
        if pos >= bytes.len() {
            return Err(SinkError::CheckpointError("Unexpected end of data".to_string()));
        }
        let has_tx = bytes[pos] == 1;
        pos += 1;

        let pending_transaction = if has_tx {
            let tx_len = read_u32(&mut pos)? as usize;
            if pos + tx_len > bytes.len() {
                return Err(SinkError::CheckpointError("Invalid transaction length".to_string()));
            }
            let tx = TransactionId::from_bytes(&bytes[pos..pos + tx_len])
                .ok_or_else(|| SinkError::CheckpointError("Invalid transaction data".to_string()))?;
            pos += tx_len;
            Some(tx)
        } else {
            None
        };

        // Offsets
        let num_offsets = read_u32(&mut pos)?;
        let mut offsets = HashMap::new();
        for _ in 0..num_offsets {
            let partition_len = read_u32(&mut pos)? as usize;
            if pos + partition_len > bytes.len() {
                return Err(SinkError::CheckpointError("Invalid partition length".to_string()));
            }
            let partition = String::from_utf8_lossy(&bytes[pos..pos + partition_len]).to_string();
            pos += partition_len;

            let offset_len = read_u32(&mut pos)? as usize;
            if pos + offset_len > bytes.len() {
                return Err(SinkError::CheckpointError("Invalid offset length".to_string()));
            }
            let (offset, _) = SinkOffset::from_bytes(&bytes[pos..pos + offset_len])
                .ok_or_else(|| SinkError::CheckpointError("Invalid offset data".to_string()))?;
            pos += offset_len;

            offsets.insert(partition, offset);
        }

        // Metadata
        let num_metadata = read_u32(&mut pos)?;
        let mut metadata = HashMap::new();
        for _ in 0..num_metadata {
            let key_len = read_u32(&mut pos)? as usize;
            if pos + key_len > bytes.len() {
                return Err(SinkError::CheckpointError("Invalid metadata key length".to_string()));
            }
            let key = String::from_utf8_lossy(&bytes[pos..pos + key_len]).to_string();
            pos += key_len;

            let value_len = read_u32(&mut pos)? as usize;
            if pos + value_len > bytes.len() {
                return Err(SinkError::CheckpointError("Invalid metadata value length".to_string()));
            }
            let value = bytes[pos..pos + value_len].to_vec();
            pos += value_len;

            metadata.insert(key, value);
        }

        Ok(Self {
            sink_id,
            offsets,
            pending_transaction,
            epoch,
            timestamp,
            metadata,
        })
    }
}

/// Manager for sink checkpoints
///
/// Coordinates checkpointing across multiple sinks and integrates
/// with the main checkpoint system.
pub struct SinkCheckpointManager {
    /// Sink ID to checkpoint mapping
    checkpoints: HashMap<String, SinkCheckpoint>,

    /// Current epoch
    current_epoch: u64,
}

impl SinkCheckpointManager {
    /// Create a new checkpoint manager
    #[must_use]
    pub fn new() -> Self {
        Self {
            checkpoints: HashMap::new(),
            current_epoch: 0,
        }
    }

    /// Register a sink checkpoint
    pub fn register(&mut self, checkpoint: SinkCheckpoint) {
        let sink_id = checkpoint.sink_id.clone();
        self.checkpoints.insert(sink_id, checkpoint);
    }

    /// Get a sink's checkpoint
    #[must_use]
    pub fn get(&self, sink_id: &str) -> Option<&SinkCheckpoint> {
        self.checkpoints.get(sink_id)
    }

    /// Get a mutable reference to a sink's checkpoint
    pub fn get_mut(&mut self, sink_id: &str) -> Option<&mut SinkCheckpoint> {
        self.checkpoints.get_mut(sink_id)
    }

    /// Advance the epoch for all sinks
    pub fn advance_epoch(&mut self) -> u64 {
        self.current_epoch += 1;
        for checkpoint in self.checkpoints.values_mut() {
            checkpoint.set_epoch(self.current_epoch);
        }
        self.current_epoch
    }

    /// Get the current epoch
    #[must_use]
    pub fn current_epoch(&self) -> u64 {
        self.current_epoch
    }

    /// Serialize all checkpoints to bytes
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // Epoch
        bytes.extend_from_slice(&self.current_epoch.to_le_bytes());

        // Number of checkpoints
        bytes.extend_from_slice(&(self.checkpoints.len() as u32).to_le_bytes());

        // Each checkpoint
        for checkpoint in self.checkpoints.values() {
            let cp_bytes = checkpoint.to_bytes();
            bytes.extend_from_slice(&(cp_bytes.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&cp_bytes);
        }

        bytes
    }

    /// Deserialize from bytes
    ///
    /// # Errors
    ///
    /// Returns an error if the bytes are malformed.
    ///
    /// # Panics
    ///
    /// Will not panic - all array conversions are bounds-checked before unwrapping.
    #[allow(clippy::missing_panics_doc)]
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, SinkError> {
        if bytes.len() < 12 {
            return Err(SinkError::CheckpointError("Checkpoint data too short".to_string()));
        }

        let mut pos = 0;

        // Epoch
        let current_epoch = u64::from_le_bytes(bytes[pos..pos + 8].try_into().unwrap());
        pos += 8;

        // Number of checkpoints
        let num_checkpoints = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        let mut checkpoints = HashMap::new();

        for _ in 0..num_checkpoints {
            if pos + 4 > bytes.len() {
                return Err(SinkError::CheckpointError("Unexpected end of data".to_string()));
            }
            let cp_len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;

            if pos + cp_len > bytes.len() {
                return Err(SinkError::CheckpointError("Invalid checkpoint length".to_string()));
            }
            let checkpoint = SinkCheckpoint::from_bytes(&bytes[pos..pos + cp_len])?;
            pos += cp_len;

            checkpoints.insert(checkpoint.sink_id.clone(), checkpoint);
        }

        Ok(Self {
            checkpoints,
            current_epoch,
        })
    }
}

impl Default for SinkCheckpointManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sink_offset_numeric() {
        let offset = SinkOffset::Numeric(12345);
        let bytes = offset.to_bytes();
        let (restored, _) = SinkOffset::from_bytes(&bytes).unwrap();
        assert_eq!(offset, restored);
    }

    #[test]
    fn test_sink_offset_string() {
        let offset = SinkOffset::String("offset-abc-123".to_string());
        let bytes = offset.to_bytes();
        let (restored, _) = SinkOffset::from_bytes(&bytes).unwrap();
        assert_eq!(offset, restored);
    }

    #[test]
    fn test_sink_offset_binary() {
        let offset = SinkOffset::Binary(vec![1, 2, 3, 4, 5]);
        let bytes = offset.to_bytes();
        let (restored, _) = SinkOffset::from_bytes(&bytes).unwrap();
        assert_eq!(offset, restored);
    }

    #[test]
    fn test_sink_checkpoint_new() {
        let checkpoint = SinkCheckpoint::new("my-sink");
        assert_eq!(checkpoint.sink_id(), "my-sink");
        assert_eq!(checkpoint.epoch(), 0);
        assert!(checkpoint.pending_transaction_id().is_none());
    }

    #[test]
    fn test_sink_checkpoint_with_offsets() {
        let mut checkpoint = SinkCheckpoint::new("kafka-sink");
        checkpoint.set_offset("topic-0", SinkOffset::Numeric(100));
        checkpoint.set_offset("topic-1", SinkOffset::Numeric(200));

        assert_eq!(checkpoint.get_offset("topic-0"), Some(&SinkOffset::Numeric(100)));
        assert_eq!(checkpoint.get_offset("topic-1"), Some(&SinkOffset::Numeric(200)));
        assert_eq!(checkpoint.get_offset("topic-2"), None);
    }

    #[test]
    fn test_sink_checkpoint_serialization() {
        let mut checkpoint = SinkCheckpoint::new("test-sink");
        checkpoint.set_epoch(42);
        checkpoint.set_offset("partition-0", SinkOffset::Numeric(1000));
        checkpoint.set_offset("partition-1", SinkOffset::String("abc".to_string()));
        checkpoint.set_transaction_id(Some(TransactionId::new(999)));
        checkpoint.set_metadata("custom-key", b"custom-value".to_vec());

        let bytes = checkpoint.to_bytes();
        let restored = SinkCheckpoint::from_bytes(&bytes).unwrap();

        assert_eq!(restored.sink_id(), "test-sink");
        assert_eq!(restored.epoch(), 42);
        assert_eq!(restored.get_offset("partition-0"), Some(&SinkOffset::Numeric(1000)));
        assert_eq!(restored.get_offset("partition-1"), Some(&SinkOffset::String("abc".to_string())));
        assert!(restored.pending_transaction_id().is_some());
        assert_eq!(restored.get_metadata("custom-key"), Some(b"custom-value".as_ref()));
    }

    #[test]
    fn test_checkpoint_manager() {
        let mut manager = SinkCheckpointManager::new();

        let mut cp1 = SinkCheckpoint::new("sink-1");
        cp1.set_offset("p0", SinkOffset::Numeric(100));

        let mut cp2 = SinkCheckpoint::new("sink-2");
        cp2.set_offset("p0", SinkOffset::Numeric(200));

        manager.register(cp1);
        manager.register(cp2);

        assert_eq!(manager.current_epoch(), 0);
        manager.advance_epoch();
        assert_eq!(manager.current_epoch(), 1);

        let cp = manager.get("sink-1").unwrap();
        assert_eq!(cp.epoch(), 1);
    }

    #[test]
    fn test_checkpoint_manager_serialization() {
        let mut manager = SinkCheckpointManager::new();

        let mut cp = SinkCheckpoint::new("sink-1");
        cp.set_offset("p0", SinkOffset::Numeric(100));
        manager.register(cp);

        manager.advance_epoch();
        manager.advance_epoch();

        let bytes = manager.to_bytes();
        let restored = SinkCheckpointManager::from_bytes(&bytes).unwrap();

        assert_eq!(restored.current_epoch(), 2);
        assert!(restored.get("sink-1").is_some());
    }
}

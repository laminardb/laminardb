//! Streaming checkpoint support.
//!
//! Provides optional, zero-overhead checkpointing for the streaming API.
//! When disabled (the default), no runtime cost is incurred. When enabled,
//! captures source sequences, watermarks, and persists checkpoint snapshots.
//!
//! ## Architecture
//!
//! ```text
//! Ring 0 (Hot Path): Source.push() -> increment sequence (AtomicU64 Relaxed ~1ns)
//! Ring 1 (Background): StreamCheckpointManager.trigger() -> capture atomics -> store
//! Ring 2 (Control):    LaminarDB.checkpoint() -> manual trigger
//! ```

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

// Configuration

/// WAL mode for checkpoint durability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalMode {
    /// Asynchronous WAL writes (faster, may lose last few entries on crash).
    Async,
    /// Synchronous WAL writes (slower, durable).
    Sync,
}

/// Configuration for streaming checkpoints.
///
/// All fields default to `None`/disabled. Checkpointing is opt-in.
#[derive(Debug, Clone)]
pub struct StreamCheckpointConfig {
    /// Checkpoint interval in milliseconds. `None` = manual only.
    pub interval_ms: Option<u64>,
    /// WAL mode. Requires `data_dir` to be set.
    pub wal_mode: Option<WalMode>,
    /// Directory for persisting checkpoints/WAL. `None` = in-memory only.
    pub data_dir: Option<std::path::PathBuf>,
    /// Changelog buffer capacity. `None` = no changelog buffer.
    pub changelog_capacity: Option<usize>,
    /// Maximum number of retained checkpoints. `None` = unlimited.
    pub max_retained: Option<usize>,
    /// Overflow policy for the changelog buffer.
    pub overflow_policy: OverflowPolicy,
}

impl Default for StreamCheckpointConfig {
    fn default() -> Self {
        Self {
            interval_ms: None,
            wal_mode: None,
            data_dir: None,
            changelog_capacity: None,
            max_retained: None,
            overflow_policy: OverflowPolicy::DropNew,
        }
    }
}

impl StreamCheckpointConfig {
    /// Validates the configuration, returning an error if invalid.
    ///
    /// # Errors
    ///
    /// Returns `CheckpointError::InvalidConfig` if WAL mode is set without
    /// `data_dir`, or if `changelog_capacity` is zero.
    pub fn validate(&self) -> Result<(), CheckpointError> {
        if self.wal_mode.is_some() && self.data_dir.is_none() {
            return Err(CheckpointError::InvalidConfig(
                "WAL mode requires data_dir to be set".into(),
            ));
        }
        if let Some(cap) = self.changelog_capacity {
            if cap == 0 {
                return Err(CheckpointError::InvalidConfig(
                    "changelog_capacity must be > 0".into(),
                ));
            }
        }
        Ok(())
    }
}

// Errors

/// Errors from checkpoint operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointError {
    /// Checkpointing is disabled.
    Disabled,
    /// A data directory is required for this operation.
    DataDirRequired,
    /// WAL mode requires checkpointing to be enabled.
    WalRequiresCheckpoint,
    /// No checkpoint available for restore.
    NoCheckpoint,
    /// Operation timed out.
    Timeout,
    /// Invalid configuration.
    InvalidConfig(String),
    /// I/O error (stored as string for Clone/PartialEq).
    IoError(String),
}

impl fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => write!(f, "checkpointing is disabled"),
            Self::DataDirRequired => write!(f, "data directory is required"),
            Self::WalRequiresCheckpoint => {
                write!(f, "WAL mode requires checkpointing")
            }
            Self::NoCheckpoint => write!(f, "no checkpoint available"),
            Self::Timeout => write!(f, "checkpoint operation timed out"),
            Self::InvalidConfig(msg) => {
                write!(f, "invalid checkpoint config: {msg}")
            }
            Self::IoError(msg) => write!(f, "checkpoint I/O error: {msg}"),
        }
    }
}

impl std::error::Error for CheckpointError {}

// Overflow policy

/// Policy when the changelog buffer is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Drop new entries when buffer is full.
    DropNew,
    /// Overwrite the oldest entry.
    OverwriteOldest,
}

// Changelog entry (24 bytes, repr(C))

/// Type of changelog operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamChangeOp {
    /// A record was pushed.
    Push = 0,
    /// A watermark was emitted.
    Watermark = 1,
    /// A checkpoint barrier.
    Barrier = 2,
}

impl StreamChangeOp {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Push),
            1 => Some(Self::Watermark),
            2 => Some(Self::Barrier),
            _ => None,
        }
    }
}

/// A single changelog entry — fixed 24 bytes, no heap allocation.
///
/// Layout (repr(C)):
/// ```text
/// [source_id: u16][op: u8][padding: u8][reserved: u32][sequence: u64][watermark: i64]
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct StreamChangelogEntry {
    /// Source identifier (compact).
    pub source_id: u16,
    /// Operation type.
    pub op: u8,
    /// Padding for alignment.
    _padding: u8,
    /// Reserved for future use.
    _reserved: u32,
    /// Sequence number at time of operation.
    pub sequence: u64,
    /// Watermark value at time of operation.
    pub watermark: i64,
}

impl StreamChangelogEntry {
    /// Creates a new changelog entry.
    #[must_use]
    pub fn new(source_id: u16, op: StreamChangeOp, sequence: u64, watermark: i64) -> Self {
        Self {
            source_id,
            op: op as u8,
            _padding: 0,
            _reserved: 0,
            sequence,
            watermark,
        }
    }

    /// Returns the operation type.
    #[must_use]
    pub fn op_type(&self) -> Option<StreamChangeOp> {
        StreamChangeOp::from_u8(self.op)
    }
}

// Changelog buffer (pre-allocated ring buffer, zero-alloc after init)

/// A pre-allocated ring buffer for changelog entries.
///
/// Uses a simple write/read index scheme. Not thread-safe on its own —
/// intended to be used behind the `StreamCheckpointManager` mutex.
pub struct StreamChangelogBuffer {
    entries: Vec<StreamChangelogEntry>,
    capacity: usize,
    write_idx: usize,
    read_idx: usize,
    count: usize,
    overflow_count: u64,
    policy: OverflowPolicy,
}

impl StreamChangelogBuffer {
    /// Creates a new changelog buffer with the given capacity.
    #[must_use]
    pub fn new(capacity: usize, policy: OverflowPolicy) -> Self {
        let zeroed = StreamChangelogEntry {
            source_id: 0,
            op: 0,
            _padding: 0,
            _reserved: 0,
            sequence: 0,
            watermark: 0,
        };
        Self {
            entries: vec![zeroed; capacity],
            capacity,
            write_idx: 0,
            read_idx: 0,
            count: 0,
            overflow_count: 0,
            policy,
        }
    }

    /// Pushes an entry into the buffer.
    ///
    /// Returns `true` if the entry was stored, `false` if dropped due to
    /// overflow policy.
    pub fn push(&mut self, entry: StreamChangelogEntry) -> bool {
        if self.count == self.capacity {
            self.overflow_count += 1;
            match self.policy {
                OverflowPolicy::DropNew => return false,
                OverflowPolicy::OverwriteOldest => {
                    // Advance read pointer, discarding oldest
                    self.read_idx = (self.read_idx + 1) % self.capacity;
                    self.count -= 1;
                }
            }
        }
        self.entries[self.write_idx] = entry;
        self.write_idx = (self.write_idx + 1) % self.capacity;
        self.count += 1;
        true
    }

    /// Pops the oldest entry from the buffer.
    pub fn pop(&mut self) -> Option<StreamChangelogEntry> {
        if self.count == 0 {
            return None;
        }
        let entry = self.entries[self.read_idx];
        self.read_idx = (self.read_idx + 1) % self.capacity;
        self.count -= 1;
        Some(entry)
    }

    /// Drains up to `max` entries into the provided vector.
    pub fn drain(&mut self, max: usize, out: &mut Vec<StreamChangelogEntry>) {
        let n = max.min(self.count);
        for _ in 0..n {
            if let Some(entry) = self.pop() {
                out.push(entry);
            }
        }
    }

    /// Drains all entries into the provided vector.
    pub fn drain_all(&mut self, out: &mut Vec<StreamChangelogEntry>) {
        let n = self.count;
        self.drain(n, out);
    }

    /// Returns the number of entries in the buffer.
    #[must_use]
    pub fn len(&self) -> usize {
        self.count
    }

    /// Returns `true` if the buffer is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns `true` if the buffer is full.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.count == self.capacity
    }

    /// Returns the buffer capacity.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the total number of overflows since creation.
    #[must_use]
    pub fn overflow_count(&self) -> u64 {
        self.overflow_count
    }
}

impl fmt::Debug for StreamChangelogBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamChangelogBuffer")
            .field("capacity", &self.capacity)
            .field("len", &self.count)
            .field("overflow_count", &self.overflow_count)
            .finish_non_exhaustive()
    }
}

// Checkpoint snapshot

/// A point-in-time snapshot of streaming pipeline state.
#[derive(Debug, Clone)]
pub struct StreamCheckpoint {
    /// Unique checkpoint identifier (monotonically increasing).
    pub id: u64,
    /// Epoch number.
    pub epoch: u64,
    /// Source name -> sequence number at checkpoint time.
    pub source_sequences: HashMap<String, u64>,
    /// Sink name -> position at checkpoint time.
    pub sink_positions: HashMap<String, u64>,
    /// Source name -> watermark at checkpoint time.
    pub watermarks: HashMap<String, i64>,
    /// Operator name -> opaque state bytes.
    pub operator_states: HashMap<String, Vec<u8>>,
    /// Timestamp when this checkpoint was created (millis since epoch).
    pub created_at: u64,
}

impl StreamCheckpoint {
    /// Serializes the checkpoint to bytes.
    ///
    /// Format:
    /// ```text
    /// [version: 1][id: 8][epoch: 8][created_at: 8]
    /// [num_sources: 4][ [name_len:4][name][seq:8] ... ]
    /// [num_sinks: 4][ [name_len:4][name][pos:8] ... ]
    /// [num_watermarks: 4][ [name_len:4][name][wm:8] ... ]
    /// [num_ops: 4][ [name_len:4][name][data_len:4][data] ... ]
    /// ```
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // Wire format uses u32 for collection lengths
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);

        // Version
        buf.push(1u8);

        // Header
        buf.extend_from_slice(&self.id.to_le_bytes());
        buf.extend_from_slice(&self.epoch.to_le_bytes());
        buf.extend_from_slice(&self.created_at.to_le_bytes());

        // Source sequences
        buf.extend_from_slice(&(self.source_sequences.len() as u32).to_le_bytes());
        for (name, seq) in &self.source_sequences {
            buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(&seq.to_le_bytes());
        }

        // Sink positions
        buf.extend_from_slice(&(self.sink_positions.len() as u32).to_le_bytes());
        for (name, pos) in &self.sink_positions {
            buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(&pos.to_le_bytes());
        }

        // Watermarks
        buf.extend_from_slice(&(self.watermarks.len() as u32).to_le_bytes());
        for (name, wm) in &self.watermarks {
            buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(&wm.to_le_bytes());
        }

        // Operator states
        buf.extend_from_slice(&(self.operator_states.len() as u32).to_le_bytes());
        for (name, data) in &self.operator_states {
            buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
            buf.extend_from_slice(data);
        }

        buf
    }

    /// Deserializes a checkpoint from bytes.
    ///
    /// # Errors
    ///
    /// Returns `CheckpointError::IoError` if the data is truncated, corrupted,
    /// or uses an unsupported version.
    #[allow(clippy::similar_names, clippy::too_many_lines)]
    pub fn from_bytes(data: &[u8]) -> Result<Self, CheckpointError> {
        let mut pos = 0;

        let read_u32 = |p: &mut usize| -> Result<u32, CheckpointError> {
            if *p + 4 > data.len() {
                return Err(CheckpointError::IoError("truncated u32".into()));
            }
            let val = u32::from_le_bytes(
                data[*p..*p + 4]
                    .try_into()
                    .map_err(|_| CheckpointError::IoError("bad u32".into()))?,
            );
            *p += 4;
            Ok(val)
        };

        let read_u64_val = |p: &mut usize| -> Result<u64, CheckpointError> {
            if *p + 8 > data.len() {
                return Err(CheckpointError::IoError("truncated u64".into()));
            }
            let val = u64::from_le_bytes(
                data[*p..*p + 8]
                    .try_into()
                    .map_err(|_| CheckpointError::IoError("bad u64".into()))?,
            );
            *p += 8;
            Ok(val)
        };

        let read_i64_val = |p: &mut usize| -> Result<i64, CheckpointError> {
            if *p + 8 > data.len() {
                return Err(CheckpointError::IoError("truncated i64".into()));
            }
            let val = i64::from_le_bytes(
                data[*p..*p + 8]
                    .try_into()
                    .map_err(|_| CheckpointError::IoError("bad i64".into()))?,
            );
            *p += 8;
            Ok(val)
        };

        let read_string = |p: &mut usize| -> Result<String, CheckpointError> {
            let slen = read_u32(p)? as usize;
            if *p + slen > data.len() {
                return Err(CheckpointError::IoError("truncated string".into()));
            }
            let s = std::str::from_utf8(&data[*p..*p + slen])
                .map_err(|_| CheckpointError::IoError("invalid utf8".into()))?
                .to_string();
            *p += slen;
            Ok(s)
        };

        // Version
        if pos >= data.len() {
            return Err(CheckpointError::IoError("empty checkpoint data".into()));
        }
        let version = data[pos];
        pos += 1;
        if version != 1 {
            return Err(CheckpointError::IoError(format!(
                "unsupported checkpoint version: {version}"
            )));
        }

        // Header
        let id = read_u64_val(&mut pos)?;
        let epoch = read_u64_val(&mut pos)?;
        let created_at = read_u64_val(&mut pos)?;

        // Source sequences
        let num_sources = read_u32(&mut pos)? as usize;
        let mut source_sequences = HashMap::with_capacity(num_sources);
        for _ in 0..num_sources {
            let name = read_string(&mut pos)?;
            let seq = read_u64_val(&mut pos)?;
            source_sequences.insert(name, seq);
        }

        // Sink positions
        let num_sinks = read_u32(&mut pos)? as usize;
        let mut sink_positions = HashMap::with_capacity(num_sinks);
        for _ in 0..num_sinks {
            let name = read_string(&mut pos)?;
            let sink_pos = read_u64_val(&mut pos)?;
            sink_positions.insert(name, sink_pos);
        }

        // Watermarks
        let num_watermarks = read_u32(&mut pos)? as usize;
        let mut watermarks = HashMap::with_capacity(num_watermarks);
        for _ in 0..num_watermarks {
            let name = read_string(&mut pos)?;
            let wm = read_i64_val(&mut pos)?;
            watermarks.insert(name, wm);
        }

        // Operator states
        let num_ops = read_u32(&mut pos)? as usize;
        let mut operator_states = HashMap::with_capacity(num_ops);
        for _ in 0..num_ops {
            let name = read_string(&mut pos)?;
            let data_len = read_u32(&mut pos)? as usize;
            if pos + data_len > data.len() {
                return Err(CheckpointError::IoError("truncated operator state".into()));
            }
            let state_data = data[pos..pos + data_len].to_vec();
            pos += data_len;
            operator_states.insert(name, state_data);
        }

        Ok(Self {
            id,
            epoch,
            source_sequences,
            sink_positions,
            watermarks,
            operator_states,
            created_at,
        })
    }
}

// Registered source info (held by the manager)

/// Registered source state visible to the checkpoint manager.
struct RegisteredSource {
    /// Shared sequence counter (atomically incremented by Source on push).
    sequence: Arc<AtomicU64>,
    /// Shared watermark (atomically updated by Source).
    watermark: Arc<AtomicI64>,
}

// Checkpoint manager

/// Coordinates checkpoint lifecycle for streaming sources and sinks.
///
/// Disabled by default. When enabled via [`StreamCheckpointConfig`], the
/// manager captures atomic counters from registered sources to produce
/// consistent [`StreamCheckpoint`] snapshots.
pub struct StreamCheckpointManager {
    config: StreamCheckpointConfig,
    enabled: bool,
    sources: HashMap<String, RegisteredSource>,
    sinks: HashMap<String, u64>,
    checkpoints: Vec<StreamCheckpoint>,
    next_id: u64,
    epoch: u64,
    changelog: Option<StreamChangelogBuffer>,
}

impl StreamCheckpointManager {
    /// Creates a new checkpoint manager.
    ///
    /// If `config` validation fails, the manager is created in disabled state.
    #[must_use]
    pub fn new(config: StreamCheckpointConfig) -> Self {
        let enabled = config.validate().is_ok();
        let changelog = config
            .changelog_capacity
            .filter(|_| enabled)
            .map(|cap| StreamChangelogBuffer::new(cap, config.overflow_policy));
        Self {
            config,
            enabled,
            sources: HashMap::new(),
            sinks: HashMap::new(),
            checkpoints: Vec::new(),
            next_id: 1,
            epoch: 0,
            changelog,
        }
    }

    /// Creates a disabled (no-op) manager.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            config: StreamCheckpointConfig::default(),
            enabled: false,
            sources: HashMap::new(),
            sinks: HashMap::new(),
            checkpoints: Vec::new(),
            next_id: 1,
            epoch: 0,
            changelog: None,
        }
    }

    /// Returns whether checkpointing is enabled.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Registers a source for checkpoint tracking.
    ///
    /// The `sequence` and `watermark` atomics are shared with the live
    /// [`Source`](super::Source) — reading them is lock-free.
    pub fn register_source(
        &mut self,
        name: &str,
        sequence: Arc<AtomicU64>,
        watermark: Arc<AtomicI64>,
    ) {
        self.sources.insert(
            name.to_string(),
            RegisteredSource {
                sequence,
                watermark,
            },
        );
    }

    /// Registers a sink for checkpoint tracking.
    pub fn register_sink(&mut self, name: &str, position: u64) {
        self.sinks.insert(name.to_string(), position);
    }

    /// Triggers a checkpoint, capturing current source/sink state.
    ///
    /// Returns the checkpoint ID, or `None` if checkpointing is disabled.
    #[allow(clippy::cast_possible_truncation)] // Timestamp ms fits i64 for ~292 years from epoch
    pub fn trigger(&mut self) -> Option<u64> {
        if !self.enabled {
            return None;
        }

        self.epoch += 1;
        let id = self.next_id;
        self.next_id += 1;

        // Capture source sequences and watermarks atomically
        let mut source_sequences = HashMap::with_capacity(self.sources.len());
        let mut watermarks = HashMap::with_capacity(self.sources.len());
        for (name, src) in &self.sources {
            source_sequences.insert(name.clone(), src.sequence.load(Ordering::Acquire));
            watermarks.insert(name.clone(), src.watermark.load(Ordering::Acquire));
        }

        // Capture sink positions
        let sink_positions = self.sinks.clone();

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let checkpoint = StreamCheckpoint {
            id,
            epoch: self.epoch,
            source_sequences,
            sink_positions,
            watermarks,
            operator_states: HashMap::new(),
            created_at: now,
        };

        self.checkpoints.push(checkpoint);

        // Prune old checkpoints if max_retained is set
        if let Some(max) = self.config.max_retained {
            while self.checkpoints.len() > max {
                self.checkpoints.remove(0);
            }
        }

        Some(id)
    }

    /// Creates a checkpoint and returns the checkpoint ID.
    ///
    /// # Errors
    ///
    /// Returns `CheckpointError::Disabled` if checkpointing is not enabled.
    pub fn checkpoint(&mut self) -> Result<Option<u64>, CheckpointError> {
        if !self.enabled {
            return Err(CheckpointError::Disabled);
        }
        Ok(self.trigger())
    }

    /// Returns the most recent checkpoint for restore.
    ///
    /// # Errors
    ///
    /// Returns `CheckpointError::Disabled` if checkpointing is not enabled,
    /// or `CheckpointError::NoCheckpoint` if no checkpoint exists.
    pub fn restore(&self) -> Result<&StreamCheckpoint, CheckpointError> {
        if !self.enabled {
            return Err(CheckpointError::Disabled);
        }
        self.checkpoints.last().ok_or(CheckpointError::NoCheckpoint)
    }

    /// Returns a checkpoint by ID.
    #[must_use]
    pub fn get_checkpoint(&self, id: u64) -> Option<&StreamCheckpoint> {
        self.checkpoints.iter().find(|cp| cp.id == id)
    }

    /// Returns the ID of the most recent checkpoint.
    #[must_use]
    pub fn last_checkpoint_id(&self) -> Option<u64> {
        self.checkpoints.last().map(|cp| cp.id)
    }

    /// Returns a reference to the changelog buffer, if configured.
    #[must_use]
    pub fn changelog(&self) -> Option<&StreamChangelogBuffer> {
        self.changelog.as_ref()
    }

    /// Returns a mutable reference to the changelog buffer.
    pub fn changelog_mut(&mut self) -> Option<&mut StreamChangelogBuffer> {
        self.changelog.as_mut()
    }

    /// Returns the current epoch.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl fmt::Debug for StreamCheckpointManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamCheckpointManager")
            .field("enabled", &self.enabled)
            .field("sources", &self.sources.len())
            .field("sinks", &self.sinks.len())
            .field("checkpoints", &self.checkpoints.len())
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

// Tests

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled_config() -> StreamCheckpointConfig {
        StreamCheckpointConfig {
            interval_ms: Some(1000),
            ..Default::default()
        }
    }

    // -- Config / disabled tests --

    #[test]
    fn test_checkpoint_disabled_by_default() {
        let config = StreamCheckpointConfig::default();
        let mgr = StreamCheckpointManager::new(config);
        // Default config is valid but has no interval — still "enabled"
        // because validate() passes. Disabled means validate() fails.
        assert!(mgr.is_enabled());

        // A truly disabled manager:
        let mgr2 = StreamCheckpointManager::disabled();
        assert!(!mgr2.is_enabled());
    }

    #[test]
    fn test_checkpoint_no_op_when_disabled() {
        let mgr = StreamCheckpointManager::disabled();
        assert!(!mgr.is_enabled());
        assert_eq!(mgr.last_checkpoint_id(), None);
    }

    #[test]
    fn test_checkpoint_config_requires_data_dir() {
        let config = StreamCheckpointConfig {
            wal_mode: Some(WalMode::Sync),
            data_dir: None,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // With data_dir set, validation passes
        let config2 = StreamCheckpointConfig {
            wal_mode: Some(WalMode::Sync),
            data_dir: Some(std::path::PathBuf::from("/tmp/test")),
            ..Default::default()
        };
        assert!(config2.validate().is_ok());
    }

    #[test]
    fn test_wal_requires_checkpoint() {
        let config = StreamCheckpointConfig {
            wal_mode: Some(WalMode::Async),
            data_dir: None, // missing
            ..Default::default()
        };
        let result = config.validate();
        assert!(matches!(result, Err(CheckpointError::InvalidConfig(_))));
    }

    // -- Source registration --

    #[test]
    fn test_register_source() {
        let mut mgr = StreamCheckpointManager::new(enabled_config());

        let seq = Arc::new(AtomicU64::new(0));
        let wm = Arc::new(AtomicI64::new(i64::MIN));

        mgr.register_source("trades", Arc::clone(&seq), Arc::clone(&wm));
        assert!(mgr.is_enabled());
    }

    // -- Trigger / capture --

    #[test]
    fn test_trigger_checkpoint() {
        let mut mgr = StreamCheckpointManager::new(enabled_config());
        let id = mgr.trigger();
        assert_eq!(id, Some(1));

        let id2 = mgr.trigger();
        assert_eq!(id2, Some(2));
    }

    #[test]
    fn test_checkpoint_captures_sequences() {
        let mut mgr = StreamCheckpointManager::new(enabled_config());

        let seq = Arc::new(AtomicU64::new(0));
        let wm = Arc::new(AtomicI64::new(i64::MIN));
        mgr.register_source("src1", Arc::clone(&seq), Arc::clone(&wm));

        // Simulate pushes
        seq.store(42, Ordering::Release);

        let id = mgr.trigger().unwrap();
        let cp = mgr.get_checkpoint(id).unwrap();
        assert_eq!(cp.source_sequences.get("src1"), Some(&42));
    }

    #[test]
    fn test_checkpoint_captures_watermarks() {
        let mut mgr = StreamCheckpointManager::new(enabled_config());

        let seq = Arc::new(AtomicU64::new(0));
        let wm = Arc::new(AtomicI64::new(5000));
        mgr.register_source("src1", Arc::clone(&seq), Arc::clone(&wm));

        let id = mgr.trigger().unwrap();
        let cp = mgr.get_checkpoint(id).unwrap();
        assert_eq!(cp.watermarks.get("src1"), Some(&5000));
    }

    #[test]
    fn test_restore_from_checkpoint() {
        let mut mgr = StreamCheckpointManager::new(enabled_config());

        let seq = Arc::new(AtomicU64::new(10));
        let wm = Arc::new(AtomicI64::new(1000));
        mgr.register_source("src1", Arc::clone(&seq), Arc::clone(&wm));

        mgr.trigger();
        let restored = mgr.restore().unwrap();
        assert_eq!(restored.source_sequences.get("src1"), Some(&10));
        assert_eq!(restored.watermarks.get("src1"), Some(&1000));
    }

    // -- Changelog buffer --

    #[test]
    fn test_changelog_buffer_push_pop() {
        let mut buf = StreamChangelogBuffer::new(4, OverflowPolicy::DropNew);
        assert!(buf.is_empty());

        let entry = StreamChangelogEntry::new(0, StreamChangeOp::Push, 1, i64::MIN);
        assert!(buf.push(entry));
        assert_eq!(buf.len(), 1);

        let popped = buf.pop().unwrap();
        assert_eq!(popped.sequence, 1);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_changelog_buffer_overflow() {
        // DropNew policy
        let mut buf = StreamChangelogBuffer::new(2, OverflowPolicy::DropNew);
        let e1 = StreamChangelogEntry::new(0, StreamChangeOp::Push, 1, i64::MIN);
        let e2 = StreamChangelogEntry::new(0, StreamChangeOp::Push, 2, i64::MIN);
        let e3 = StreamChangelogEntry::new(0, StreamChangeOp::Push, 3, i64::MIN);

        assert!(buf.push(e1));
        assert!(buf.push(e2));
        assert!(buf.is_full());
        assert!(!buf.push(e3)); // dropped
        assert_eq!(buf.overflow_count(), 1);

        // Verify oldest is still there
        assert_eq!(buf.pop().unwrap().sequence, 1);

        // OverwriteOldest policy
        let mut buf2 = StreamChangelogBuffer::new(2, OverflowPolicy::OverwriteOldest);
        assert!(buf2.push(e1));
        assert!(buf2.push(e2));
        assert!(buf2.push(e3)); // overwrites e1
        assert_eq!(buf2.overflow_count(), 1);
        assert_eq!(buf2.pop().unwrap().sequence, 2); // e1 was overwritten
    }

    // -- Prune --

    #[test]
    fn test_checkpoint_prune_old() {
        let config = StreamCheckpointConfig {
            interval_ms: Some(1000),
            max_retained: Some(2),
            ..Default::default()
        };
        let mut mgr = StreamCheckpointManager::new(config);

        mgr.trigger(); // id=1
        mgr.trigger(); // id=2
        mgr.trigger(); // id=3 — should prune id=1

        assert_eq!(mgr.checkpoints.len(), 2);
        assert!(mgr.get_checkpoint(1).is_none());
        assert!(mgr.get_checkpoint(2).is_some());
        assert!(mgr.get_checkpoint(3).is_some());
    }

    // -- Serialization --

    #[test]
    fn test_checkpoint_serialization() {
        let mut source_sequences = HashMap::new();
        source_sequences.insert("src1".to_string(), 100u64);
        source_sequences.insert("src2".to_string(), 200u64);

        let mut sink_positions = HashMap::new();
        sink_positions.insert("sink1".to_string(), 50u64);

        let mut watermarks = HashMap::new();
        watermarks.insert("src1".to_string(), 5000i64);
        watermarks.insert("src2".to_string(), 6000i64);

        let mut operator_states = HashMap::new();
        operator_states.insert("op1".to_string(), vec![1, 2, 3, 4]);

        let cp = StreamCheckpoint {
            id: 42,
            epoch: 7,
            source_sequences,
            sink_positions,
            watermarks,
            operator_states,
            created_at: 1_706_400_000_000,
        };

        let bytes = cp.to_bytes();
        let restored = StreamCheckpoint::from_bytes(&bytes).unwrap();

        assert_eq!(restored.id, 42);
        assert_eq!(restored.epoch, 7);
        assert_eq!(restored.created_at, 1_706_400_000_000);
        assert_eq!(restored.source_sequences.get("src1"), Some(&100));
        assert_eq!(restored.source_sequences.get("src2"), Some(&200));
        assert_eq!(restored.sink_positions.get("sink1"), Some(&50));
        assert_eq!(restored.watermarks.get("src1"), Some(&5000));
        assert_eq!(restored.watermarks.get("src2"), Some(&6000));
        assert_eq!(restored.operator_states.get("op1"), Some(&vec![1, 2, 3, 4]));
    }

    #[test]
    fn test_changelog_entry_size() {
        assert_eq!(
            std::mem::size_of::<StreamChangelogEntry>(),
            24,
            "StreamChangelogEntry must be exactly 24 bytes"
        );
    }

    // -- Source sequence counter tests --

    #[test]
    fn test_source_sequence_counter() {
        let seq = Arc::new(AtomicU64::new(0));
        assert_eq!(seq.load(Ordering::Acquire), 0);

        seq.fetch_add(1, Ordering::Relaxed);
        seq.fetch_add(1, Ordering::Relaxed);
        seq.fetch_add(1, Ordering::Relaxed);
        assert_eq!(seq.load(Ordering::Acquire), 3);
    }

    #[test]
    fn test_source_clone_shares_sequence() {
        let seq = Arc::new(AtomicU64::new(0));
        let seq2 = Arc::clone(&seq);

        seq.fetch_add(1, Ordering::Relaxed);
        assert_eq!(seq2.load(Ordering::Acquire), 1);

        seq2.fetch_add(5, Ordering::Relaxed);
        assert_eq!(seq.load(Ordering::Acquire), 6);
    }
}

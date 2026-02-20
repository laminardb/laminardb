//! Typed source position tracking for checkpoint recovery.
//!
//! Provides `SourcePosition` — a strongly typed enum representing
//! connector-specific offsets — alongside conversion methods to/from
//! the existing `ConnectorCheckpoint` format.
//!
//! Also provides:
//! - `SourceId` — newtype for source identifiers within a pipeline.
//! - `SourceOffset` — combines a `SourceId` with a `SourcePosition`.
//! - `RecoveryPlan` — recovery plan built from checkpoint manifests.
//! - `DeterminismWarning` — warnings about non-determinism during recovery.

use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::checkpoint::layout::SourceOffsetEntry;
use crate::checkpoint_manifest::ConnectorCheckpoint;

/// Unique identifier for a source within a pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourceId(pub String);

impl SourceId {
    /// Create a new source identifier.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Return the inner string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<&str> for SourceId {
    fn from(s: &str) -> Self {
        Self(s.to_string())
    }
}

impl From<String> for SourceId {
    fn from(s: String) -> Self {
        Self(s)
    }
}

/// A source's read position at checkpoint time.
///
/// Combines a [`SourceId`] with a [`SourcePosition`] to fully identify
/// where a specific source was reading when the checkpoint was taken.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceOffset {
    /// Which source this offset belongs to.
    pub source_id: SourceId,
    /// The source-specific position.
    pub position: SourcePosition,
}

/// Kafka partition-level offset.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KafkaPartitionOffset {
    /// Topic name.
    pub topic: String,
    /// Partition number.
    pub partition: i32,
    /// Offset within the partition.
    pub offset: i64,
}

/// Kafka source position (all partitions for a consumer group).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KafkaPosition {
    /// Consumer group ID.
    pub group_id: String,
    /// Per-partition offsets.
    pub partitions: Vec<KafkaPartitionOffset>,
}

/// `PostgreSQL` CDC position tracked via replication slot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PostgresCdcPosition {
    /// Confirmed flush LSN (hex string, e.g. "0/1234ABCD").
    pub confirmed_flush_lsn: String,
    /// Write LSN (may be ahead of confirmed flush).
    pub write_lsn: Option<String>,
    /// Replication slot name.
    pub slot_name: String,
}

/// `MySQL` CDC position tracked via GTID set or binlog coordinates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MysqlCdcPosition {
    /// GTID set (e.g. "uuid:1-5").
    pub gtid_set: Option<String>,
    /// Binlog file name.
    pub binlog_file: Option<String>,
    /// Binlog position within the file.
    pub binlog_position: Option<u64>,
}

/// File source position.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FilePosition {
    /// Path to the file being read.
    pub path: String,
    /// Byte offset into the file.
    pub byte_offset: u64,
}

/// Generic position for custom connectors.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GenericPosition {
    /// Connector type identifier.
    pub connector_type: String,
    /// Opaque key-value offsets.
    pub offsets: HashMap<String, String>,
}

/// Strongly typed source position.
///
/// Each variant captures the native offset format for a connector type,
/// enabling type-safe validation and recovery planning.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum SourcePosition {
    /// Kafka consumer group offsets.
    Kafka(KafkaPosition),
    /// `PostgreSQL` CDC replication slot position.
    PostgresCdc(PostgresCdcPosition),
    /// `MySQL` CDC binlog/GTID position.
    MysqlCdc(MysqlCdcPosition),
    /// File source byte offset.
    File(FilePosition),
    /// Generic connector offsets.
    Generic(GenericPosition),
}

impl SourcePosition {
    /// Convert to a [`ConnectorCheckpoint`] for storage.
    #[must_use]
    pub fn to_connector_checkpoint(&self, epoch: u64) -> ConnectorCheckpoint {
        let mut cp = ConnectorCheckpoint::new(epoch);
        match self {
            Self::Kafka(pos) => {
                cp.metadata.insert("connector_type".into(), "kafka".into());
                cp.metadata.insert("group_id".into(), pos.group_id.clone());
                for p in &pos.partitions {
                    cp.offsets
                        .insert(format!("{}-{}", p.topic, p.partition), p.offset.to_string());
                }
            }
            Self::PostgresCdc(pos) => {
                cp.metadata
                    .insert("connector_type".into(), "postgres_cdc".into());
                cp.offsets.insert(
                    "confirmed_flush_lsn".into(),
                    pos.confirmed_flush_lsn.clone(),
                );
                if let Some(ref lsn) = pos.write_lsn {
                    cp.offsets.insert("write_lsn".into(), lsn.clone());
                }
                cp.metadata
                    .insert("slot_name".into(), pos.slot_name.clone());
            }
            Self::MysqlCdc(pos) => {
                cp.metadata
                    .insert("connector_type".into(), "mysql_cdc".into());
                if let Some(ref gtid) = pos.gtid_set {
                    cp.offsets.insert("gtid_set".into(), gtid.clone());
                }
                if let Some(ref file) = pos.binlog_file {
                    cp.offsets.insert("binlog_file".into(), file.clone());
                }
                if let Some(binlog_pos) = pos.binlog_position {
                    cp.offsets
                        .insert("binlog_position".into(), binlog_pos.to_string());
                }
            }
            Self::File(pos) => {
                cp.metadata.insert("connector_type".into(), "file".into());
                cp.offsets.insert("path".into(), pos.path.clone());
                cp.offsets
                    .insert("byte_offset".into(), pos.byte_offset.to_string());
            }
            Self::Generic(pos) => {
                cp.metadata
                    .insert("connector_type".into(), pos.connector_type.clone());
                cp.offsets.clone_from(&pos.offsets);
            }
        }
        cp
    }

    /// Try to reconstruct a typed position from a [`ConnectorCheckpoint`].
    ///
    /// Uses the `connector_type` metadata key to determine which variant
    /// to build. Returns `None` if the type is unknown or the offsets
    /// are incomplete.
    #[must_use]
    pub fn from_connector_checkpoint(
        cp: &ConnectorCheckpoint,
        type_hint: Option<&str>,
    ) -> Option<Self> {
        let connector_type =
            type_hint.or_else(|| cp.metadata.get("connector_type").map(String::as_str))?;

        match connector_type {
            "kafka" => {
                let group_id = cp.metadata.get("group_id").cloned().unwrap_or_default();
                let mut partitions = Vec::new();
                for (key, value) in &cp.offsets {
                    // Keys are "topic-partition"
                    if let Some(dash_pos) = key.rfind('-') {
                        let topic = key[..dash_pos].to_string();
                        if let Ok(partition) = key[dash_pos + 1..].parse::<i32>() {
                            if let Ok(offset) = value.parse::<i64>() {
                                partitions.push(KafkaPartitionOffset {
                                    topic,
                                    partition,
                                    offset,
                                });
                            }
                        }
                    }
                }
                Some(Self::Kafka(KafkaPosition {
                    group_id,
                    partitions,
                }))
            }
            "postgres_cdc" => {
                let confirmed_flush_lsn = cp.offsets.get("confirmed_flush_lsn")?.clone();
                let write_lsn = cp.offsets.get("write_lsn").cloned();
                let slot_name = cp.metadata.get("slot_name").cloned().unwrap_or_default();
                Some(Self::PostgresCdc(PostgresCdcPosition {
                    confirmed_flush_lsn,
                    write_lsn,
                    slot_name,
                }))
            }
            "mysql_cdc" => Some(Self::MysqlCdc(MysqlCdcPosition {
                gtid_set: cp.offsets.get("gtid_set").cloned(),
                binlog_file: cp.offsets.get("binlog_file").cloned(),
                binlog_position: cp
                    .offsets
                    .get("binlog_position")
                    .and_then(|s| s.parse().ok()),
            })),
            "file" => {
                let path = cp.offsets.get("path")?.clone();
                let byte_offset = cp
                    .offsets
                    .get("byte_offset")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                Some(Self::File(FilePosition { path, byte_offset }))
            }
            _ => Some(Self::Generic(GenericPosition {
                connector_type: connector_type.to_string(),
                offsets: cp.offsets.clone(),
            })),
        }
    }

    /// Convert to a [`SourceOffsetEntry`] for the V2 manifest format.
    #[must_use]
    pub fn to_offset_entry(&self, epoch: u64) -> SourceOffsetEntry {
        let (source_type, offsets) = match self {
            Self::Kafka(pos) => {
                let mut offsets = HashMap::new();
                offsets.insert("group_id".into(), pos.group_id.clone());
                for p in &pos.partitions {
                    offsets.insert(format!("{}-{}", p.topic, p.partition), p.offset.to_string());
                }
                ("kafka".to_string(), offsets)
            }
            Self::PostgresCdc(pos) => {
                let mut offsets = HashMap::new();
                offsets.insert(
                    "confirmed_flush_lsn".into(),
                    pos.confirmed_flush_lsn.clone(),
                );
                if let Some(ref lsn) = pos.write_lsn {
                    offsets.insert("write_lsn".into(), lsn.clone());
                }
                offsets.insert("slot_name".into(), pos.slot_name.clone());
                ("postgres_cdc".to_string(), offsets)
            }
            Self::MysqlCdc(pos) => {
                let mut offsets = HashMap::new();
                if let Some(ref gtid) = pos.gtid_set {
                    offsets.insert("gtid_set".into(), gtid.clone());
                }
                if let Some(ref file) = pos.binlog_file {
                    offsets.insert("binlog_file".into(), file.clone());
                }
                if let Some(binlog_pos) = pos.binlog_position {
                    offsets.insert("binlog_position".into(), binlog_pos.to_string());
                }
                ("mysql_cdc".to_string(), offsets)
            }
            Self::File(pos) => {
                let mut offsets = HashMap::new();
                offsets.insert("path".into(), pos.path.clone());
                offsets.insert("byte_offset".into(), pos.byte_offset.to_string());
                ("file".to_string(), offsets)
            }
            Self::Generic(pos) => (pos.connector_type.clone(), pos.offsets.clone()),
        };
        SourceOffsetEntry {
            source_type,
            offsets,
            epoch,
        }
    }

    /// Reconstruct a typed position from a [`SourceOffsetEntry`].
    ///
    /// Returns `None` if the entry's source type is unrecognized or the
    /// offsets are incomplete.
    #[must_use]
    pub fn from_offset_entry(entry: &SourceOffsetEntry) -> Option<Self> {
        match entry.source_type.as_str() {
            "kafka" => {
                let group_id = entry.offsets.get("group_id").cloned().unwrap_or_default();
                let mut partitions = Vec::new();
                for (key, value) in &entry.offsets {
                    if key == "group_id" {
                        continue;
                    }
                    if let Some(dash_pos) = key.rfind('-') {
                        let topic = key[..dash_pos].to_string();
                        if let Ok(partition) = key[dash_pos + 1..].parse::<i32>() {
                            if let Ok(offset) = value.parse::<i64>() {
                                partitions.push(KafkaPartitionOffset {
                                    topic,
                                    partition,
                                    offset,
                                });
                            }
                        }
                    }
                }
                Some(Self::Kafka(KafkaPosition {
                    group_id,
                    partitions,
                }))
            }
            "postgres_cdc" => {
                let confirmed_flush_lsn = entry.offsets.get("confirmed_flush_lsn")?.clone();
                let write_lsn = entry.offsets.get("write_lsn").cloned();
                let slot_name = entry.offsets.get("slot_name").cloned().unwrap_or_default();
                Some(Self::PostgresCdc(PostgresCdcPosition {
                    confirmed_flush_lsn,
                    write_lsn,
                    slot_name,
                }))
            }
            "mysql_cdc" => Some(Self::MysqlCdc(MysqlCdcPosition {
                gtid_set: entry.offsets.get("gtid_set").cloned(),
                binlog_file: entry.offsets.get("binlog_file").cloned(),
                binlog_position: entry
                    .offsets
                    .get("binlog_position")
                    .and_then(|s| s.parse().ok()),
            })),
            "file" => {
                let path = entry.offsets.get("path")?.clone();
                let byte_offset = entry
                    .offsets
                    .get("byte_offset")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                Some(Self::File(FilePosition { path, byte_offset }))
            }
            _ => Some(Self::Generic(GenericPosition {
                connector_type: entry.source_type.clone(),
                offsets: entry.offsets.clone(),
            })),
        }
    }
}

/// Severity level for recovery warnings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WarningSeverity {
    /// Informational — recovery can proceed normally.
    Info,
    /// Warning — recovery can proceed but results may differ.
    Warning,
    /// Error — recovery may produce incorrect results.
    Error,
}

/// A warning generated during recovery planning.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeterminismWarning {
    /// Source name that generated the warning.
    pub source_name: String,
    /// Human-readable description of the issue.
    pub message: String,
    /// Severity level.
    pub severity: WarningSeverity,
}

/// Recovery plan built from a checkpoint manifest.
///
/// Describes where each source should resume and any warnings
/// about determinism or data availability.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecoveryPlan {
    /// Per-source typed positions for resuming.
    pub source_positions: HashMap<String, SourcePosition>,
    /// Epoch to resume from.
    pub resume_epoch: u64,
    /// Warnings generated during planning.
    pub warnings: Vec<DeterminismWarning>,
}

impl RecoveryPlan {
    /// Build a recovery plan from a checkpoint manifest's source offsets.
    #[must_use]
    pub fn from_manifest(
        source_offsets: &HashMap<String, ConnectorCheckpoint>,
        epoch: u64,
    ) -> Self {
        let mut source_positions = HashMap::new();
        let mut warnings = Vec::new();

        for (name, cp) in source_offsets {
            if let Some(pos) = SourcePosition::from_connector_checkpoint(cp, None) {
                source_positions.insert(name.clone(), pos);
            } else {
                warnings.push(DeterminismWarning {
                    source_name: name.clone(),
                    message: "Could not reconstruct typed position; \
                              will use raw offsets for recovery"
                        .into(),
                    severity: WarningSeverity::Warning,
                });
            }
        }

        Self {
            source_positions,
            resume_epoch: epoch,
            warnings,
        }
    }

    /// Build a recovery plan from a V2 manifest's source offset entries.
    #[must_use]
    pub fn from_manifest_v2(
        source_offsets: &HashMap<String, SourceOffsetEntry>,
        epoch: u64,
    ) -> Self {
        let mut source_positions = HashMap::new();
        let mut warnings = Vec::new();

        for (name, entry) in source_offsets {
            if let Some(pos) = SourcePosition::from_offset_entry(entry) {
                source_positions.insert(name.clone(), pos);
            } else {
                warnings.push(DeterminismWarning {
                    source_name: name.clone(),
                    message: "Could not reconstruct typed position from V2 \
                              offset entry; will use raw offsets for recovery"
                        .into(),
                    severity: WarningSeverity::Warning,
                });
            }
        }

        Self {
            source_positions,
            resume_epoch: epoch,
            warnings,
        }
    }
}

/// Trait describing an operator's determinism properties.
///
/// Used by [`DeterminismValidator`] to check whether operators in an
/// exactly-once pipeline are deterministic (same input + same state = same
/// output). Non-deterministic operators break the replay-based exactly-once
/// guarantee.
pub trait OperatorDescriptor {
    /// Unique operator identifier.
    fn id(&self) -> &str;

    /// Whether the operator reads wall-clock / processing time.
    fn uses_wall_clock(&self) -> bool;

    /// Whether the operator uses randomness (e.g., `rand::random()`).
    fn uses_random(&self) -> bool;

    /// Whether the operator has external side effects (e.g., HTTP calls).
    fn has_external_side_effects(&self) -> bool;
}

/// Warning about potential non-determinism in an operator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorDeterminismWarning {
    /// Operator uses wall-clock / processing time.
    WallClockUsage {
        /// Operator that uses wall-clock time.
        operator_id: String,
        /// Suggested fix.
        suggestion: String,
    },
    /// Operator uses randomness.
    RandomUsage {
        /// Operator that uses randomness.
        operator_id: String,
        /// Suggested fix.
        suggestion: String,
    },
    /// Operator has external side effects.
    ExternalSideEffect {
        /// Operator with side effects.
        operator_id: String,
        /// Suggested fix.
        suggestion: String,
    },
}

/// Validates that operators in a pipeline are deterministic.
///
/// Deterministic operators produce identical output given identical input
/// and state. This validator checks known non-determinism sources and
/// returns warnings for operators that may break replay-based exactly-once.
pub struct DeterminismValidator;

impl DeterminismValidator {
    /// Validate that an operator is deterministic.
    ///
    /// Returns a list of warnings for potential non-determinism sources.
    /// An empty list means the operator appears deterministic.
    #[must_use]
    pub fn validate(operator: &dyn OperatorDescriptor) -> Vec<OperatorDeterminismWarning> {
        let mut warnings = Vec::new();

        if operator.uses_wall_clock() {
            warnings.push(OperatorDeterminismWarning::WallClockUsage {
                operator_id: operator.id().to_string(),
                suggestion: "Use event-time instead of processing-time".into(),
            });
        }

        if operator.uses_random() {
            warnings.push(OperatorDeterminismWarning::RandomUsage {
                operator_id: operator.id().to_string(),
                suggestion: "Use deterministic seed or remove randomness".into(),
            });
        }

        if operator.has_external_side_effects() {
            warnings.push(OperatorDeterminismWarning::ExternalSideEffect {
                operator_id: operator.id().to_string(),
                suggestion: "Move side effects to a sink connector".into(),
            });
        }

        warnings
    }

    /// Validate all operators in a pipeline.
    ///
    /// Returns warnings for all non-deterministic operators.
    #[must_use]
    pub fn validate_all(operators: &[&dyn OperatorDescriptor]) -> Vec<OperatorDeterminismWarning> {
        operators
            .iter()
            .flat_map(|op| Self::validate(*op))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_position_serde_roundtrip() {
        let pos = SourcePosition::Kafka(KafkaPosition {
            group_id: "my-group".into(),
            partitions: vec![
                KafkaPartitionOffset {
                    topic: "events".into(),
                    partition: 0,
                    offset: 1234,
                },
                KafkaPartitionOffset {
                    topic: "events".into(),
                    partition: 1,
                    offset: 5678,
                },
            ],
        });
        let json = serde_json::to_string(&pos).unwrap();
        let restored: SourcePosition = serde_json::from_str(&json).unwrap();
        assert_eq!(pos, restored);
    }

    #[test]
    fn test_postgres_cdc_position_serde_roundtrip() {
        let pos = SourcePosition::PostgresCdc(PostgresCdcPosition {
            confirmed_flush_lsn: "0/1234ABCD".into(),
            write_lsn: Some("0/1234ABCE".into()),
            slot_name: "laminar_slot".into(),
        });
        let json = serde_json::to_string(&pos).unwrap();
        let restored: SourcePosition = serde_json::from_str(&json).unwrap();
        assert_eq!(pos, restored);
    }

    #[test]
    fn test_mysql_cdc_position_serde_roundtrip() {
        let pos = SourcePosition::MysqlCdc(MysqlCdcPosition {
            gtid_set: Some("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5".into()),
            binlog_file: Some("mysql-bin.000003".into()),
            binlog_position: Some(154),
        });
        let json = serde_json::to_string(&pos).unwrap();
        let restored: SourcePosition = serde_json::from_str(&json).unwrap();
        assert_eq!(pos, restored);
    }

    #[test]
    fn test_file_position_serde_roundtrip() {
        let pos = SourcePosition::File(FilePosition {
            path: "/data/events.csv".into(),
            byte_offset: 4096,
        });
        let json = serde_json::to_string(&pos).unwrap();
        let restored: SourcePosition = serde_json::from_str(&json).unwrap();
        assert_eq!(pos, restored);
    }

    #[test]
    fn test_generic_position_serde_roundtrip() {
        let pos = SourcePosition::Generic(GenericPosition {
            connector_type: "custom".into(),
            offsets: HashMap::from([("cursor".into(), "abc123".into())]),
        });
        let json = serde_json::to_string(&pos).unwrap();
        let restored: SourcePosition = serde_json::from_str(&json).unwrap();
        assert_eq!(pos, restored);
    }

    #[test]
    fn test_kafka_to_connector_checkpoint() {
        let pos = SourcePosition::Kafka(KafkaPosition {
            group_id: "my-group".into(),
            partitions: vec![KafkaPartitionOffset {
                topic: "events".into(),
                partition: 0,
                offset: 1234,
            }],
        });
        let cp = pos.to_connector_checkpoint(5);
        assert_eq!(cp.epoch, 5);
        assert_eq!(cp.offsets.get("events-0"), Some(&"1234".to_string()));
        assert_eq!(
            cp.metadata.get("connector_type"),
            Some(&"kafka".to_string())
        );
        assert_eq!(cp.metadata.get("group_id"), Some(&"my-group".to_string()));
    }

    #[test]
    fn test_postgres_to_from_checkpoint() {
        let original = SourcePosition::PostgresCdc(PostgresCdcPosition {
            confirmed_flush_lsn: "0/ABCD".into(),
            write_lsn: Some("0/ABCE".into()),
            slot_name: "test_slot".into(),
        });
        let cp = original.to_connector_checkpoint(10);
        let restored = SourcePosition::from_connector_checkpoint(&cp, None).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_mysql_to_from_checkpoint() {
        let original = SourcePosition::MysqlCdc(MysqlCdcPosition {
            gtid_set: Some("uuid:1-5".into()),
            binlog_file: Some("mysql-bin.000003".into()),
            binlog_position: Some(154),
        });
        let cp = original.to_connector_checkpoint(3);
        let restored = SourcePosition::from_connector_checkpoint(&cp, None).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_file_to_from_checkpoint() {
        let original = SourcePosition::File(FilePosition {
            path: "/data/file.csv".into(),
            byte_offset: 4096,
        });
        let cp = original.to_connector_checkpoint(1);
        let restored = SourcePosition::from_connector_checkpoint(&cp, None).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_from_checkpoint_with_type_hint() {
        let mut cp = ConnectorCheckpoint::new(1);
        cp.offsets.insert("events-0".into(), "100".into());
        // No metadata — use type hint instead
        let pos = SourcePosition::from_connector_checkpoint(&cp, Some("kafka")).unwrap();
        match pos {
            SourcePosition::Kafka(k) => {
                assert_eq!(k.partitions.len(), 1);
                assert_eq!(k.partitions[0].offset, 100);
            }
            _ => panic!("Expected Kafka position"),
        }
    }

    #[test]
    fn test_from_checkpoint_no_type_returns_none() {
        let cp = ConnectorCheckpoint::new(1);
        assert!(SourcePosition::from_connector_checkpoint(&cp, None).is_none());
    }

    #[test]
    fn test_recovery_plan_from_manifest() {
        let mut source_offsets = HashMap::new();

        let mut kafka_cp = ConnectorCheckpoint::new(5);
        kafka_cp
            .metadata
            .insert("connector_type".into(), "kafka".into());
        kafka_cp.metadata.insert("group_id".into(), "g1".into());
        kafka_cp.offsets.insert("topic-0".into(), "100".into());
        source_offsets.insert("kafka-src".into(), kafka_cp);

        // CP with no type info — should generate warning
        let empty_cp = ConnectorCheckpoint::new(5);
        source_offsets.insert("unknown-src".into(), empty_cp);

        let plan = RecoveryPlan::from_manifest(&source_offsets, 5);
        assert_eq!(plan.resume_epoch, 5);
        assert_eq!(plan.source_positions.len(), 1);
        assert!(plan.source_positions.contains_key("kafka-src"));
        assert_eq!(plan.warnings.len(), 1);
        assert_eq!(plan.warnings[0].source_name, "unknown-src");
    }

    #[test]
    fn test_source_id_display() {
        let id = SourceId::new("kafka-orders");
        assert_eq!(id.to_string(), "kafka-orders");
        assert_eq!(id.as_str(), "kafka-orders");
    }

    #[test]
    fn test_source_id_from_str() {
        let id: SourceId = "my-source".into();
        assert_eq!(id.0, "my-source");
    }

    #[test]
    fn test_source_offset_serde_roundtrip() {
        let offset = SourceOffset {
            source_id: SourceId::new("kafka-src"),
            position: SourcePosition::Kafka(KafkaPosition {
                group_id: "g1".into(),
                partitions: vec![KafkaPartitionOffset {
                    topic: "events".into(),
                    partition: 0,
                    offset: 100,
                }],
            }),
        };
        let json = serde_json::to_string(&offset).unwrap();
        let restored: SourceOffset = serde_json::from_str(&json).unwrap();
        assert_eq!(offset, restored);
    }

    #[test]
    fn test_kafka_to_offset_entry() {
        let pos = SourcePosition::Kafka(KafkaPosition {
            group_id: "g1".into(),
            partitions: vec![KafkaPartitionOffset {
                topic: "events".into(),
                partition: 0,
                offset: 1234,
            }],
        });
        let entry = pos.to_offset_entry(5);
        assert_eq!(entry.source_type, "kafka");
        assert_eq!(entry.epoch, 5);
        assert_eq!(entry.offsets.get("group_id"), Some(&"g1".to_string()));
        assert_eq!(entry.offsets.get("events-0"), Some(&"1234".to_string()));
    }

    #[test]
    fn test_kafka_offset_entry_roundtrip() {
        let pos = SourcePosition::Kafka(KafkaPosition {
            group_id: "g1".into(),
            partitions: vec![KafkaPartitionOffset {
                topic: "events".into(),
                partition: 0,
                offset: 1234,
            }],
        });
        let entry = pos.to_offset_entry(5);
        let restored = SourcePosition::from_offset_entry(&entry).unwrap();
        assert_eq!(pos, restored);
    }

    #[test]
    fn test_postgres_offset_entry_roundtrip() {
        let pos = SourcePosition::PostgresCdc(PostgresCdcPosition {
            confirmed_flush_lsn: "0/ABCD".into(),
            write_lsn: Some("0/ABCE".into()),
            slot_name: "test_slot".into(),
        });
        let entry = pos.to_offset_entry(10);
        assert_eq!(entry.source_type, "postgres_cdc");
        let restored = SourcePosition::from_offset_entry(&entry).unwrap();
        assert_eq!(pos, restored);
    }

    #[test]
    fn test_mysql_offset_entry_roundtrip() {
        let pos = SourcePosition::MysqlCdc(MysqlCdcPosition {
            gtid_set: Some("uuid:1-5".into()),
            binlog_file: Some("mysql-bin.000003".into()),
            binlog_position: Some(154),
        });
        let entry = pos.to_offset_entry(3);
        assert_eq!(entry.source_type, "mysql_cdc");
        let restored = SourcePosition::from_offset_entry(&entry).unwrap();
        assert_eq!(pos, restored);
    }

    #[test]
    fn test_file_offset_entry_roundtrip() {
        let pos = SourcePosition::File(FilePosition {
            path: "/data/file.csv".into(),
            byte_offset: 4096,
        });
        let entry = pos.to_offset_entry(1);
        assert_eq!(entry.source_type, "file");
        let restored = SourcePosition::from_offset_entry(&entry).unwrap();
        assert_eq!(pos, restored);
    }

    #[test]
    fn test_generic_offset_entry_roundtrip() {
        let pos = SourcePosition::Generic(GenericPosition {
            connector_type: "custom".into(),
            offsets: HashMap::from([("cursor".into(), "abc123".into())]),
        });
        let entry = pos.to_offset_entry(1);
        assert_eq!(entry.source_type, "custom");
        let restored = SourcePosition::from_offset_entry(&entry).unwrap();
        assert_eq!(pos, restored);
    }

    #[test]
    fn test_recovery_plan_from_manifest_v2() {
        use crate::checkpoint::layout::SourceOffsetEntry;

        let mut source_offsets = HashMap::new();
        source_offsets.insert(
            "kafka-src".into(),
            SourceOffsetEntry {
                source_type: "kafka".into(),
                offsets: HashMap::from([
                    ("group_id".into(), "g1".into()),
                    ("topic-0".into(), "100".into()),
                ]),
                epoch: 5,
            },
        );
        // Entry with empty source_type that won't match any known type
        // but will still parse as Generic
        source_offsets.insert(
            "custom-src".into(),
            SourceOffsetEntry {
                source_type: "redis".into(),
                offsets: HashMap::from([("cursor".into(), "42".into())]),
                epoch: 5,
            },
        );

        let plan = RecoveryPlan::from_manifest_v2(&source_offsets, 5);
        assert_eq!(plan.resume_epoch, 5);
        assert_eq!(plan.source_positions.len(), 2);
        assert!(plan.source_positions.contains_key("kafka-src"));
        assert!(plan.source_positions.contains_key("custom-src"));
        assert!(plan.warnings.is_empty());
    }

    #[test]
    fn test_warning_severity_serde() {
        let warning = DeterminismWarning {
            source_name: "src".into(),
            message: "test".into(),
            severity: WarningSeverity::Warning,
        };
        let json = serde_json::to_string(&warning).unwrap();
        let restored: DeterminismWarning = serde_json::from_str(&json).unwrap();
        assert_eq!(warning, restored);
    }

    // ---- DeterminismValidator tests ----

    struct TestOperator {
        id: String,
        wall_clock: bool,
        random: bool,
        side_effects: bool,
    }

    impl TestOperator {
        fn deterministic(id: &str) -> Self {
            Self {
                id: id.into(),
                wall_clock: false,
                random: false,
                side_effects: false,
            }
        }
    }

    impl OperatorDescriptor for TestOperator {
        fn id(&self) -> &str {
            &self.id
        }
        fn uses_wall_clock(&self) -> bool {
            self.wall_clock
        }
        fn uses_random(&self) -> bool {
            self.random
        }
        fn has_external_side_effects(&self) -> bool {
            self.side_effects
        }
    }

    #[test]
    fn test_determinism_validator_clean_operator() {
        let op = TestOperator::deterministic("agg-1");
        let warnings = DeterminismValidator::validate(&op);
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_determinism_validator_wall_clock_warning() {
        let op = TestOperator {
            wall_clock: true,
            ..TestOperator::deterministic("timer-op")
        };
        let warnings = DeterminismValidator::validate(&op);
        assert_eq!(warnings.len(), 1);
        assert!(matches!(
            &warnings[0],
            OperatorDeterminismWarning::WallClockUsage { operator_id, .. }
            if operator_id == "timer-op"
        ));
    }

    #[test]
    fn test_determinism_validator_random_warning() {
        let op = TestOperator {
            random: true,
            ..TestOperator::deterministic("shuffle-op")
        };
        let warnings = DeterminismValidator::validate(&op);
        assert_eq!(warnings.len(), 1);
        assert!(matches!(
            &warnings[0],
            OperatorDeterminismWarning::RandomUsage { operator_id, .. }
            if operator_id == "shuffle-op"
        ));
    }

    #[test]
    fn test_determinism_validator_side_effect_warning() {
        let op = TestOperator {
            side_effects: true,
            ..TestOperator::deterministic("http-op")
        };
        let warnings = DeterminismValidator::validate(&op);
        assert_eq!(warnings.len(), 1);
        assert!(matches!(
            &warnings[0],
            OperatorDeterminismWarning::ExternalSideEffect { operator_id, .. }
            if operator_id == "http-op"
        ));
    }

    #[test]
    fn test_determinism_validator_multiple_warnings() {
        let op = TestOperator {
            id: "bad-op".into(),
            wall_clock: true,
            random: true,
            side_effects: true,
        };
        let warnings = DeterminismValidator::validate(&op);
        assert_eq!(warnings.len(), 3);
    }

    #[test]
    fn test_determinism_validator_validate_all() {
        let clean = TestOperator::deterministic("clean");
        let bad = TestOperator {
            wall_clock: true,
            ..TestOperator::deterministic("bad")
        };
        let warnings = DeterminismValidator::validate_all(&[&clean, &bad]);
        assert_eq!(warnings.len(), 1);
        assert!(matches!(
            &warnings[0],
            OperatorDeterminismWarning::WallClockUsage { operator_id, .. }
            if operator_id == "bad"
        ));
    }
}

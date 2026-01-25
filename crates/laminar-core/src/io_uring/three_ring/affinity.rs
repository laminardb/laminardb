//! Ring affinity classification for I/O operations.
//!
//! Operations are classified by their latency requirements:
//! - **Latency**: Always polled, sub-microsecond response required
//! - **Main**: Normal I/O, can wait for batch completion
//! - **Poll**: Storage I/O via IOPOLL (NVMe only)

/// Ring affinity for I/O operations.
///
/// Determines which ring an operation should be submitted to based on
/// its latency requirements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RingAffinity {
    /// Latency-critical operations (always polled first).
    ///
    /// Use for:
    /// - Network receives
    /// - Network sends (time-critical)
    /// - Timer expirations
    /// - Urgent reads
    Latency,

    /// Normal I/O operations (can block when idle).
    ///
    /// Use for:
    /// - WAL writes
    /// - WAL syncs
    /// - Checkpoint writes
    /// - Background reads
    Main,

    /// Storage polling operations (NVMe only).
    ///
    /// Use for:
    /// - High-throughput storage reads
    /// - High-throughput storage writes
    /// - Direct I/O to NVMe devices
    ///
    /// Note: IOPOLL rings cannot be used with socket operations.
    Poll,
}

impl RingAffinity {
    /// Determine the ring affinity for an operation type.
    #[must_use]
    pub const fn for_operation(op: &OperationType) -> Self {
        match op {
            // Latency-critical: network and urgent operations
            OperationType::NetworkRecv
            | OperationType::NetworkSend
            | OperationType::TimerExpiry
            | OperationType::UrgentRead
            | OperationType::UrgentWrite
            | OperationType::Accept
            | OperationType::Connect => Self::Latency,

            // Normal I/O: WAL, checkpoints, background work
            OperationType::WalWrite
            | OperationType::WalSync
            | OperationType::WalRead
            | OperationType::CheckpointWrite
            | OperationType::CheckpointRead
            | OperationType::BackgroundRead
            | OperationType::BackgroundWrite
            | OperationType::MetadataSync => Self::Main,

            // Storage polling: NVMe operations
            OperationType::StorageRead
            | OperationType::StorageWrite
            | OperationType::DirectRead
            | OperationType::DirectWrite => Self::Poll,
        }
    }

    /// Check if this affinity is for latency-critical operations.
    #[must_use]
    pub const fn is_latency_critical(&self) -> bool {
        matches!(self, Self::Latency)
    }

    /// Check if this affinity allows blocking.
    #[must_use]
    pub const fn allows_blocking(&self) -> bool {
        matches!(self, Self::Main)
    }

    /// Check if this affinity requires IOPOLL.
    #[must_use]
    pub const fn requires_iopoll(&self) -> bool {
        matches!(self, Self::Poll)
    }

    /// Get the fallback ring if the preferred ring is unavailable.
    ///
    /// Poll ring operations fall back to Main ring.
    /// Latency and Main have no fallback.
    #[must_use]
    pub const fn fallback(&self) -> Option<Self> {
        match self {
            Self::Poll => Some(Self::Main),
            _ => None,
        }
    }
}

impl std::fmt::Display for RingAffinity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Latency => write!(f, "latency"),
            Self::Main => write!(f, "main"),
            Self::Poll => write!(f, "poll"),
        }
    }
}

/// Types of I/O operations for ring classification.
///
/// Each operation type maps to a specific ring affinity based on
/// its latency requirements and characteristics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    // === Latency Ring Operations ===
    /// Network receive (socket read).
    NetworkRecv,
    /// Network send (socket write).
    NetworkSend,
    /// Timer expiration.
    TimerExpiry,
    /// Urgent read (low-latency required).
    UrgentRead,
    /// Urgent write (low-latency required).
    UrgentWrite,
    /// Socket accept.
    Accept,
    /// Socket connect.
    Connect,

    // === Main Ring Operations ===
    /// WAL write operation.
    WalWrite,
    /// WAL sync (fdatasync).
    WalSync,
    /// WAL read (recovery).
    WalRead,
    /// Checkpoint write.
    CheckpointWrite,
    /// Checkpoint read (recovery).
    CheckpointRead,
    /// Background read.
    BackgroundRead,
    /// Background write.
    BackgroundWrite,
    /// Metadata sync operation.
    MetadataSync,

    // === Poll Ring Operations (NVMe) ===
    /// High-throughput storage read.
    StorageRead,
    /// High-throughput storage write.
    StorageWrite,
    /// Direct I/O read (O_DIRECT).
    DirectRead,
    /// Direct I/O write (O_DIRECT).
    DirectWrite,
}

impl OperationType {
    /// Get the ring affinity for this operation type.
    #[must_use]
    pub const fn affinity(&self) -> RingAffinity {
        RingAffinity::for_operation(self)
    }

    /// Check if this operation type is a read.
    #[must_use]
    pub const fn is_read(&self) -> bool {
        matches!(
            self,
            Self::NetworkRecv
                | Self::UrgentRead
                | Self::WalRead
                | Self::CheckpointRead
                | Self::BackgroundRead
                | Self::StorageRead
                | Self::DirectRead
        )
    }

    /// Check if this operation type is a write.
    #[must_use]
    pub const fn is_write(&self) -> bool {
        matches!(
            self,
            Self::NetworkSend
                | Self::UrgentWrite
                | Self::WalWrite
                | Self::CheckpointWrite
                | Self::BackgroundWrite
                | Self::StorageWrite
                | Self::DirectWrite
        )
    }

    /// Check if this operation type is a sync.
    #[must_use]
    pub const fn is_sync(&self) -> bool {
        matches!(self, Self::WalSync | Self::MetadataSync)
    }

    /// Check if this is a network operation.
    #[must_use]
    pub const fn is_network(&self) -> bool {
        matches!(
            self,
            Self::NetworkRecv | Self::NetworkSend | Self::Accept | Self::Connect
        )
    }

    /// Check if this is a storage operation.
    #[must_use]
    pub const fn is_storage(&self) -> bool {
        matches!(
            self,
            Self::WalWrite
                | Self::WalRead
                | Self::WalSync
                | Self::CheckpointWrite
                | Self::CheckpointRead
                | Self::StorageRead
                | Self::StorageWrite
                | Self::DirectRead
                | Self::DirectWrite
        )
    }
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let name = match self {
            Self::NetworkRecv => "network_recv",
            Self::NetworkSend => "network_send",
            Self::TimerExpiry => "timer_expiry",
            Self::UrgentRead => "urgent_read",
            Self::UrgentWrite => "urgent_write",
            Self::Accept => "accept",
            Self::Connect => "connect",
            Self::WalWrite => "wal_write",
            Self::WalSync => "wal_sync",
            Self::WalRead => "wal_read",
            Self::CheckpointWrite => "checkpoint_write",
            Self::CheckpointRead => "checkpoint_read",
            Self::BackgroundRead => "background_read",
            Self::BackgroundWrite => "background_write",
            Self::MetadataSync => "metadata_sync",
            Self::StorageRead => "storage_read",
            Self::StorageWrite => "storage_write",
            Self::DirectRead => "direct_read",
            Self::DirectWrite => "direct_write",
        };
        write!(f, "{name}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_affinity_for_operation() {
        // Latency operations
        assert_eq!(
            RingAffinity::for_operation(&OperationType::NetworkRecv),
            RingAffinity::Latency
        );
        assert_eq!(
            RingAffinity::for_operation(&OperationType::NetworkSend),
            RingAffinity::Latency
        );
        assert_eq!(
            RingAffinity::for_operation(&OperationType::TimerExpiry),
            RingAffinity::Latency
        );
        assert_eq!(
            RingAffinity::for_operation(&OperationType::Accept),
            RingAffinity::Latency
        );

        // Main operations
        assert_eq!(
            RingAffinity::for_operation(&OperationType::WalWrite),
            RingAffinity::Main
        );
        assert_eq!(
            RingAffinity::for_operation(&OperationType::WalSync),
            RingAffinity::Main
        );
        assert_eq!(
            RingAffinity::for_operation(&OperationType::CheckpointWrite),
            RingAffinity::Main
        );

        // Poll operations
        assert_eq!(
            RingAffinity::for_operation(&OperationType::StorageRead),
            RingAffinity::Poll
        );
        assert_eq!(
            RingAffinity::for_operation(&OperationType::StorageWrite),
            RingAffinity::Poll
        );
        assert_eq!(
            RingAffinity::for_operation(&OperationType::DirectRead),
            RingAffinity::Poll
        );
    }

    #[test]
    fn test_ring_affinity_properties() {
        assert!(RingAffinity::Latency.is_latency_critical());
        assert!(!RingAffinity::Main.is_latency_critical());
        assert!(!RingAffinity::Poll.is_latency_critical());

        assert!(!RingAffinity::Latency.allows_blocking());
        assert!(RingAffinity::Main.allows_blocking());
        assert!(!RingAffinity::Poll.allows_blocking());

        assert!(!RingAffinity::Latency.requires_iopoll());
        assert!(!RingAffinity::Main.requires_iopoll());
        assert!(RingAffinity::Poll.requires_iopoll());
    }

    #[test]
    fn test_ring_affinity_fallback() {
        assert_eq!(RingAffinity::Latency.fallback(), None);
        assert_eq!(RingAffinity::Main.fallback(), None);
        assert_eq!(RingAffinity::Poll.fallback(), Some(RingAffinity::Main));
    }

    #[test]
    fn test_operation_type_properties() {
        assert!(OperationType::NetworkRecv.is_read());
        assert!(!OperationType::NetworkRecv.is_write());
        assert!(OperationType::NetworkRecv.is_network());

        assert!(!OperationType::NetworkSend.is_read());
        assert!(OperationType::NetworkSend.is_write());

        assert!(OperationType::WalSync.is_sync());
        assert!(!OperationType::WalWrite.is_sync());

        assert!(OperationType::WalWrite.is_storage());
        assert!(OperationType::StorageRead.is_storage());
        assert!(!OperationType::NetworkRecv.is_storage());
    }

    #[test]
    fn test_operation_type_affinity() {
        assert_eq!(OperationType::NetworkRecv.affinity(), RingAffinity::Latency);
        assert_eq!(OperationType::WalWrite.affinity(), RingAffinity::Main);
        assert_eq!(OperationType::StorageRead.affinity(), RingAffinity::Poll);
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", RingAffinity::Latency), "latency");
        assert_eq!(format!("{}", RingAffinity::Main), "main");
        assert_eq!(format!("{}", RingAffinity::Poll), "poll");

        assert_eq!(format!("{}", OperationType::NetworkRecv), "network_recv");
        assert_eq!(format!("{}", OperationType::WalWrite), "wal_write");
    }
}

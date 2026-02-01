//! Packet header for XDP processing.

/// UDP packet header for XDP processing.
///
/// This header is placed at the beginning of UDP payloads to enable
/// XDP filtering and CPU steering by partition key.
///
/// ## Wire Format
///
/// ```text
/// +--------+--------+--------+--------+
/// |     MAGIC (4 bytes) = "LAMI"      |
/// +--------+--------+--------+--------+
/// |   PARTITION_KEY (4 bytes, BE)     |
/// +--------+--------+--------+--------+
/// |     PAYLOAD_LENGTH (4 bytes, BE)  |
/// +--------+--------+--------+--------+
/// |          SEQUENCE (8 bytes, BE)   |
/// |                                   |
/// +--------+--------+--------+--------+
/// |           PAYLOAD ...             |
/// +--------+--------+--------+--------+
/// ```
///
/// ## Fields
///
/// - `magic`: Protocol identifier (0x4C414D49 = "LAMI")
/// - `partition_key`: Used by XDP for CPU routing
/// - `payload_len`: Length of the payload following the header
/// - `sequence`: Monotonic sequence number for ordering
///
/// ## Example
///
/// ```rust
/// use laminar_core::xdp::LaminarHeader;
///
/// // Create header for partition 42
/// let header = LaminarHeader::new(42, 1024);
/// assert!(header.validate());
///
/// // Serialize to bytes
/// let bytes = header.to_bytes();
/// assert_eq!(bytes.len(), LaminarHeader::SIZE);
///
/// // Parse from bytes
/// let parsed = LaminarHeader::from_bytes(&bytes).unwrap();
/// assert_eq!(parsed.partition_key(), 42);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct LaminarHeader {
    /// Magic bytes: 0x4C414D49 ("LAMI")
    magic: u32,
    /// Partition key for CPU steering
    partition_key: u32,
    /// Payload length
    payload_len: u32,
    /// Sequence number for ordering
    sequence: u64,
}

impl LaminarHeader {
    /// Magic bytes identifying the protocol ("LAMI").
    pub const MAGIC: u32 = 0x4C41_4D49;

    /// Header size in bytes.
    pub const SIZE: usize = 20;

    /// Creates a new header with the given partition key and payload length.
    ///
    /// Sequence number defaults to 0.
    #[must_use]
    pub const fn new(partition_key: u32, payload_len: u32) -> Self {
        Self {
            magic: Self::MAGIC.to_be(),
            partition_key: partition_key.to_be(),
            payload_len: payload_len.to_be(),
            sequence: 0,
        }
    }

    /// Creates a new header with all fields specified.
    #[must_use]
    pub const fn with_sequence(partition_key: u32, payload_len: u32, sequence: u64) -> Self {
        Self {
            magic: Self::MAGIC.to_be(),
            partition_key: partition_key.to_be(),
            payload_len: payload_len.to_be(),
            sequence: sequence.to_be(),
        }
    }

    /// Validates that the magic bytes are correct.
    #[must_use]
    pub fn validate(&self) -> bool {
        u32::from_be(self.magic) == Self::MAGIC
    }

    /// Returns the partition key.
    #[must_use]
    pub fn partition_key(&self) -> u32 {
        u32::from_be(self.partition_key)
    }

    /// Returns the payload length.
    #[must_use]
    pub fn payload_len(&self) -> u32 {
        u32::from_be(self.payload_len)
    }

    /// Returns the sequence number.
    #[must_use]
    pub fn sequence(&self) -> u64 {
        u64::from_be(self.sequence)
    }

    /// Converts the header to bytes (network byte order).
    #[must_use]
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..4].copy_from_slice(&self.magic.to_ne_bytes());
        bytes[4..8].copy_from_slice(&self.partition_key.to_ne_bytes());
        bytes[8..12].copy_from_slice(&self.payload_len.to_ne_bytes());
        bytes[12..20].copy_from_slice(&self.sequence.to_ne_bytes());
        bytes
    }

    /// Parses a header from bytes.
    ///
    /// Returns `None` if the bytes are too short or the magic is invalid.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < Self::SIZE {
            return None;
        }

        let magic = u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let partition_key = u32::from_ne_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let payload_len = u32::from_ne_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        let sequence = u64::from_ne_bytes([
            bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19],
        ]);

        let header = Self {
            magic,
            partition_key,
            payload_len,
            sequence,
        };

        if header.validate() {
            Some(header)
        } else {
            None
        }
    }

    /// Parses a header from bytes without validation.
    ///
    /// # Safety
    ///
    /// The caller must ensure the bytes contain a valid header.
    #[must_use]
    pub fn from_bytes_unchecked(bytes: &[u8]) -> Self {
        debug_assert!(bytes.len() >= Self::SIZE);

        Self {
            magic: u32::from_ne_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            partition_key: u32::from_ne_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
            payload_len: u32::from_ne_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            sequence: u64::from_ne_bytes([
                bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18],
                bytes[19],
            ]),
        }
    }

    /// Computes the target CPU for this packet.
    #[must_use]
    pub fn target_cpu(&self, num_cpus: u32) -> u32 {
        if num_cpus == 0 {
            return 0;
        }
        self.partition_key() % num_cpus
    }
}

/// Builder for creating packets with headers.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct PacketBuilder {
    partition_key: u32,
    sequence: u64,
}

#[allow(dead_code)]
impl PacketBuilder {
    /// Creates a new packet builder.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            partition_key: 0,
            sequence: 0,
        }
    }

    /// Sets the partition key.
    #[must_use]
    pub const fn partition_key(mut self, key: u32) -> Self {
        self.partition_key = key;
        self
    }

    /// Sets the sequence number.
    #[must_use]
    pub const fn sequence(mut self, seq: u64) -> Self {
        self.sequence = seq;
        self
    }

    /// Builds a packet with the given payload.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)] // Packet payloads bounded by MTU (< u32::MAX)
    pub fn build(&self, payload: &[u8]) -> Vec<u8> {
        let header =
            LaminarHeader::with_sequence(self.partition_key, payload.len() as u32, self.sequence);

        let mut packet = Vec::with_capacity(LaminarHeader::SIZE + payload.len());
        packet.extend_from_slice(&header.to_bytes());
        packet.extend_from_slice(payload);
        packet
    }

    /// Writes a packet to the given buffer.
    ///
    /// Returns the total packet size, or `None` if the buffer is too small.
    #[allow(clippy::cast_possible_truncation)] // Packet payloads bounded by MTU (< u32::MAX)
    pub fn build_into(&self, payload: &[u8], buffer: &mut [u8]) -> Option<usize> {
        let total_size = LaminarHeader::SIZE + payload.len();
        if buffer.len() < total_size {
            return None;
        }

        let header =
            LaminarHeader::with_sequence(self.partition_key, payload.len() as u32, self.sequence);

        buffer[..LaminarHeader::SIZE].copy_from_slice(&header.to_bytes());
        buffer[LaminarHeader::SIZE..total_size].copy_from_slice(payload);

        Some(total_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_new() {
        let header = LaminarHeader::new(42, 1024);
        assert!(header.validate());
        assert_eq!(header.partition_key(), 42);
        assert_eq!(header.payload_len(), 1024);
        assert_eq!(header.sequence(), 0);
    }

    #[test]
    fn test_header_with_sequence() {
        let header = LaminarHeader::with_sequence(100, 512, 999);
        assert!(header.validate());
        assert_eq!(header.partition_key(), 100);
        assert_eq!(header.payload_len(), 512);
        assert_eq!(header.sequence(), 999);
    }

    #[test]
    fn test_header_size() {
        assert_eq!(LaminarHeader::SIZE, 20);
        assert_eq!(std::mem::size_of::<LaminarHeader>(), 20);
    }

    #[test]
    fn test_header_roundtrip() {
        let original = LaminarHeader::with_sequence(12345, 67890, 0xDEAD_BEEF_CAFE_BABE);
        let bytes = original.to_bytes();
        let parsed = LaminarHeader::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.partition_key(), original.partition_key());
        assert_eq!(parsed.payload_len(), original.payload_len());
        assert_eq!(parsed.sequence(), original.sequence());
    }

    #[test]
    fn test_header_from_bytes_too_short() {
        let bytes = [0u8; 10];
        assert!(LaminarHeader::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_header_from_bytes_invalid_magic() {
        let mut bytes = LaminarHeader::new(1, 1).to_bytes();
        bytes[0] = 0xFF; // Corrupt magic
        assert!(LaminarHeader::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_target_cpu() {
        let header = LaminarHeader::new(100, 0);
        assert_eq!(header.target_cpu(4), 100 % 4);
        assert_eq!(header.target_cpu(16), 100 % 16);
        assert_eq!(header.target_cpu(0), 0); // Edge case
    }

    #[test]
    fn test_magic_value() {
        // "LAMI" in ASCII
        assert_eq!(LaminarHeader::MAGIC, 0x4C41_4D49);
        assert_eq!(&LaminarHeader::MAGIC.to_be_bytes(), b"LAMI");
    }

    #[test]
    fn test_packet_builder() {
        let payload = b"Hello, World!";
        let packet = PacketBuilder::new()
            .partition_key(42)
            .sequence(100)
            .build(payload);

        assert_eq!(packet.len(), LaminarHeader::SIZE + payload.len());

        let header = LaminarHeader::from_bytes(&packet).unwrap();
        assert_eq!(header.partition_key(), 42);
        assert_eq!(header.sequence(), 100);
        assert_eq!(header.payload_len(), u32::try_from(payload.len()).unwrap());

        assert_eq!(&packet[LaminarHeader::SIZE..], payload);
    }

    #[test]
    fn test_packet_builder_into() {
        let payload = b"Test";
        let mut buffer = [0u8; 100];

        let size = PacketBuilder::new()
            .partition_key(1)
            .build_into(payload, &mut buffer)
            .unwrap();

        assert_eq!(size, LaminarHeader::SIZE + payload.len());
    }

    #[test]
    fn test_packet_builder_into_too_small() {
        let payload = b"Test";
        let mut buffer = [0u8; 5]; // Too small

        assert!(PacketBuilder::new()
            .partition_key(1)
            .build_into(payload, &mut buffer)
            .is_none());
    }
}

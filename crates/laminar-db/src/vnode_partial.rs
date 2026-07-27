//! Per-vnode durable partial state in an immutable checkpoint-attempt namespace.
//!
//! An empty `operators` map is valid: it seals the epoch carrying no state.

use laminar_core::state::{CheckpointAttempt, SealedVnodePartial};
use sha2::{Digest, Sha256};

use crate::error::DbError;

#[cfg(not(test))]
mod v2;
#[cfg(test)]
pub(crate) mod v2;

const VNODE_PARTIAL_MAGIC: [u8; 8] = *b"LDBVNODE";
const VNODE_PARTIAL_VERSION: u16 = 1;
const VNODE_PARTIAL_HEADER_LEN: usize = 16;
const PARENT_ATTESTATION_DOMAIN: &[u8] = b"laminardb/vnode-parent-attestation/v1\0";

/// Exact immutable parent artifact named by a reference or delta partial.
///
/// The fingerprint covers the parent attempt and the full seal attestation, including vnode,
/// assignment generation, writer certificate, payload length, and payload digest. The child
/// payload is itself sealed, so this link prevents a self-consistent replacement of an older seal
/// and body from silently changing the child's ancestry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct SealedVnodeParentLink {
    pub(crate) attempt: CheckpointAttempt,
    pub(crate) attestation_sha256: [u8; 32],
}

impl SealedVnodeParentLink {
    pub(crate) fn new(
        attempt: CheckpointAttempt,
        sealed: &SealedVnodePartial,
    ) -> Result<Self, DbError> {
        if !attempt.is_canonical() {
            return Err(DbError::Checkpoint(format!(
                "vnode parent link requires a canonical checkpoint attempt; received {attempt:?}"
            )));
        }
        if sealed.payload_sha256.len() != 64
            || !sealed
                .payload_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(DbError::Checkpoint(format!(
                "vnode parent link has an invalid payload digest for vnode {}",
                sealed.vnode
            )));
        }

        let mut digest = Sha256::new();
        digest.update(PARENT_ATTESTATION_DOMAIN);
        digest.update(attempt.epoch.to_le_bytes());
        digest.update(attempt.checkpoint_id.to_le_bytes());
        digest.update(sealed.vnode.to_le_bytes());
        digest.update(sealed.assignment_version.to_le_bytes());
        match &sealed.writer {
            Some(writer) => {
                digest.update([1]);
                digest.update(writer.node_id.to_le_bytes());
                digest.update(writer.boot_incarnation.as_bytes());
                digest.update(writer.assignment_certificate_digest);
            }
            None => digest.update([0]),
        }
        digest.update(sealed.payload_len.to_le_bytes());
        digest.update(sealed.payload_sha256.as_bytes());
        Ok(Self {
            attempt,
            attestation_sha256: digest.finalize().into(),
        })
    }

    #[cfg(any(feature = "cluster", test))]
    pub(crate) fn matches(
        self,
        attempt: CheckpointAttempt,
        sealed: &SealedVnodePartial,
    ) -> Result<bool, DbError> {
        Ok(self == Self::new(attempt, sealed)?)
    }
}

/// Operator-state slices for one vnode at one epoch, in one of three shapes:
///
/// - FULL: `operators` non-empty, `base = None`, `deltas` empty.
/// - REFERENCE: `operators`/`deltas` empty, `base = Some(link)` — byte-identical to the base.
/// - DELTA: `deltas` non-empty, `base = Some(link)` — per-operator changes since the parent.
///
/// The reader walks exact sealed parent links back to a FULL and replays deltas forward. The
/// writer re-bases before the base leaves the prune window.
#[derive(Debug, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct VnodePartial {
    /// `(operator_name, vnode-slice bytes)`: FULL slices; also operators that re-based this epoch.
    pub operators: Vec<(String, Vec<u8>)>,
    /// Exact sealed parent artifact for a reference or delta chain.
    pub base: Option<SealedVnodeParentLink>,
    /// `(operator_name, changed-state bytes)`. Non-empty only for delta partials.
    pub deltas: Vec<(String, Vec<u8>)>,
}

/// Exact mirror of the former unframed rkyv payload. Only parent-free FULL artifacts can be
/// upgraded safely: legacy references and deltas name an attempt but do not bind its attestation.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct LegacyVnodePartial {
    operators: Vec<(String, Vec<u8>)>,
    base: Option<CheckpointAttempt>,
    deltas: Vec<(String, Vec<u8>)>,
}

impl VnodePartial {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, DbError> {
        self.validate_shape()?;
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(self).map_err(|error| {
            DbError::Checkpoint(format!("vnode partial serialization: {error}"))
        })?;
        let payload_len = u32::try_from(payload.len()).map_err(|_| {
            DbError::Checkpoint("vnode partial serialization exceeds the wire limit".into())
        })?;
        let mut encoded = Vec::with_capacity(VNODE_PARTIAL_HEADER_LEN + payload.len());
        encoded.extend_from_slice(&VNODE_PARTIAL_MAGIC);
        encoded.extend_from_slice(&VNODE_PARTIAL_VERSION.to_le_bytes());
        encoded.extend_from_slice(&0_u16.to_le_bytes());
        encoded.extend_from_slice(&payload_len.to_le_bytes());
        encoded.extend_from_slice(&payload);
        Ok(encoded)
    }

    /// Deserialize a `partial.bin` blob. Former parent-free FULL payloads remain readable; former
    /// reference/delta payloads fail closed because their bare attempt does not prove ancestry.
    #[cfg_attr(not(feature = "cluster"), allow(dead_code))]
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.starts_with(&VNODE_PARTIAL_MAGIC) {
            return Self::decode_framed(bytes);
        }
        Self::decode_legacy(bytes)
    }

    fn decode_framed(bytes: &[u8]) -> Result<Self, DbError> {
        if bytes.len() < VNODE_PARTIAL_HEADER_LEN {
            return Err(DbError::Checkpoint(
                "vnode partial frame is truncated".into(),
            ));
        }
        let version = u16::from_le_bytes([bytes[8], bytes[9]]);
        if version != VNODE_PARTIAL_VERSION {
            return Err(DbError::Checkpoint(format!(
                "unsupported vnode partial version {version}; expected {VNODE_PARTIAL_VERSION}"
            )));
        }
        if bytes[10..12] != [0, 0] {
            return Err(DbError::Checkpoint(
                "vnode partial frame has nonzero reserved flags".into(),
            ));
        }
        let payload_len = u32::from_le_bytes(bytes[12..16].try_into().expect("fixed slice"));
        let payload_len = usize::try_from(payload_len)
            .map_err(|_| DbError::Checkpoint("vnode partial length does not fit usize".into()))?;
        if payload_len != bytes.len() - VNODE_PARTIAL_HEADER_LEN {
            return Err(DbError::Checkpoint(format!(
                "vnode partial frame length {payload_len} does not match {} payload bytes",
                bytes.len() - VNODE_PARTIAL_HEADER_LEN
            )));
        }
        let partial = Self::decode_current_archive(&bytes[VNODE_PARTIAL_HEADER_LEN..])?;
        partial.validate_shape()?;
        Ok(partial)
    }

    fn decode_legacy(bytes: &[u8]) -> Result<Self, DbError> {
        let legacy = Self::decode_legacy_archive(bytes)?;
        if let Some(parent) = legacy.base {
            return Err(DbError::Checkpoint(format!(
                "legacy vnode partial references unbound parent {parent:?}; a sealed parent attestation is required"
            )));
        }
        if !legacy.deltas.is_empty() {
            return Err(DbError::Checkpoint(
                "legacy vnode delta partial has no sealed parent attestation".into(),
            ));
        }
        Ok(Self {
            operators: legacy.operators,
            base: None,
            deltas: legacy.deltas,
        })
    }

    fn decode_current_archive(bytes: &[u8]) -> Result<Self, DbError> {
        const ARCHIVE_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;
        if bytes.as_ptr().align_offset(ARCHIVE_ALIGNMENT) == 0 {
            return rkyv::from_bytes::<Self, rkyv::rancor::Error>(bytes).map_err(|error| {
                DbError::Checkpoint(format!("vnode partial deserialization: {error}"))
            });
        }
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(&aligned)
            .map_err(|error| DbError::Checkpoint(format!("vnode partial deserialization: {error}")))
    }

    fn decode_legacy_archive(bytes: &[u8]) -> Result<LegacyVnodePartial, DbError> {
        const ARCHIVE_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;
        if bytes.as_ptr().align_offset(ARCHIVE_ALIGNMENT) == 0 {
            return rkyv::from_bytes::<LegacyVnodePartial, rkyv::rancor::Error>(bytes).map_err(
                |error| {
                    DbError::Checkpoint(format!("legacy vnode partial deserialization: {error}"))
                },
            );
        }
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        rkyv::from_bytes::<LegacyVnodePartial, rkyv::rancor::Error>(&aligned).map_err(|error| {
            DbError::Checkpoint(format!("legacy vnode partial deserialization: {error}"))
        })
    }

    fn validate_shape(&self) -> Result<(), DbError> {
        if self.base.is_some_and(|base| !base.attempt.is_canonical()) {
            return Err(DbError::Checkpoint(
                "vnode partial has a noncanonical sealed parent attempt".into(),
            ));
        }
        if !self.deltas.is_empty() && self.base.is_none() {
            return Err(DbError::Checkpoint(
                "vnode delta partial has no sealed parent link".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sealed(vnode: u32, payload_sha256: &str) -> SealedVnodePartial {
        SealedVnodePartial {
            vnode,
            assignment_version: 0,
            writer: None,
            payload_len: 3,
            payload_sha256: payload_sha256.into(),
        }
    }

    fn link(attempt: CheckpointAttempt) -> SealedVnodeParentLink {
        SealedVnodeParentLink::new(attempt, &sealed(0, &"ab".repeat(32))).unwrap()
    }

    #[test]
    fn full_round_trips() {
        let partial = VnodePartial {
            operators: vec![
                ("agg".to_string(), vec![1, 2, 3]),
                ("other".to_string(), vec![]),
            ],
            base: None,
            deltas: Vec::new(),
        };
        let bytes = partial.encode().unwrap();
        let restored = VnodePartial::decode(&bytes).unwrap();
        assert_eq!(restored.operators.len(), 2);
        assert_eq!(restored.operators[0].0, "agg");
        assert_eq!(restored.operators[0].1, vec![1, 2, 3]);
    }

    #[test]
    fn empty_full_round_trips() {
        let partial = VnodePartial::default();
        let restored = VnodePartial::decode(&partial.encode().unwrap()).unwrap();
        assert!(restored.operators.is_empty());
        assert!(restored.base.is_none());
    }

    #[test]
    fn reference_round_trips_sealed_parent() {
        let parent = link(CheckpointAttempt::canonical(9));
        let partial = VnodePartial {
            operators: Vec::new(),
            base: Some(parent),
            deltas: Vec::new(),
        };
        let restored = VnodePartial::decode(&partial.encode().unwrap()).unwrap();
        assert_eq!(restored.base, Some(parent));
    }

    #[test]
    fn delta_round_trips_sealed_parent() {
        let parent = link(CheckpointAttempt::canonical(9));
        let partial = VnodePartial {
            operators: Vec::new(),
            base: Some(parent),
            deltas: vec![("agg".to_string(), vec![4, 5, 6])],
        };
        let restored = VnodePartial::decode(&partial.encode().unwrap()).unwrap();
        assert_eq!(restored.base, Some(parent));
        assert_eq!(restored.deltas, vec![("agg".to_string(), vec![4, 5, 6])]);
    }

    #[test]
    fn legacy_full_remains_readable_but_unbound_parent_fails_closed() {
        let full = LegacyVnodePartial {
            operators: vec![("agg".into(), vec![1])],
            base: None,
            deltas: Vec::new(),
        };
        let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&full).unwrap();
        assert_eq!(VnodePartial::decode(&encoded).unwrap().operators.len(), 1);

        let reference = LegacyVnodePartial {
            operators: Vec::new(),
            base: Some(CheckpointAttempt::canonical(8)),
            deltas: Vec::new(),
        };
        let encoded = rkyv::to_bytes::<rkyv::rancor::Error>(&reference).unwrap();
        let error = VnodePartial::decode(&encoded).unwrap_err().to_string();
        assert!(error.contains("unbound parent"), "{error}");
    }

    #[test]
    fn unknown_version_and_delta_without_parent_fail_closed() {
        let mut encoded = VnodePartial::default().encode().unwrap();
        encoded[8..10].copy_from_slice(&99_u16.to_le_bytes());
        assert!(VnodePartial::decode(&encoded).is_err());

        let invalid = VnodePartial {
            operators: Vec::new(),
            base: None,
            deltas: vec![("agg".into(), vec![1])],
        };
        assert!(invalid.encode().is_err());
    }

    #[test]
    fn parent_fingerprint_covers_attempt_and_complete_attestation() {
        let attempt = CheckpointAttempt::canonical(9);
        let original = sealed(4, &"ab".repeat(32));
        let link = SealedVnodeParentLink::new(attempt, &original).unwrap();
        assert!(link.matches(attempt, &original).unwrap());

        let mut changed = original.clone();
        changed.payload_len += 1;
        assert!(!link.matches(attempt, &changed).unwrap());
        assert!(!link
            .matches(CheckpointAttempt::canonical(8), &original)
            .unwrap());
    }

    #[test]
    fn legacy_marker_decode_is_err_not_panic() {
        assert!(VnodePartial::decode(b"ckpt:123").is_err());
    }

    #[test]
    fn decodes_unaligned_transport_buffer() {
        let partial = VnodePartial {
            operators: vec![("agg".to_string(), vec![1, 2, 3])],
            base: None,
            deltas: Vec::new(),
        };
        let encoded = partial.encode().unwrap();
        let required_alignment = std::mem::align_of::<<VnodePartial as rkyv::Archive>::Archived>();
        assert!(required_alignment > 1);

        let mut transport = vec![0_u8; encoded.len() + required_alignment];
        let payload_len = encoded.len() - VNODE_PARTIAL_HEADER_LEN;
        let root_position =
            rkyv::api::root_position::<<VnodePartial as rkyv::Archive>::Archived>(payload_len);
        let offset = (0..required_alignment)
            .find(|offset| {
                !(transport.as_ptr() as usize + offset + VNODE_PARTIAL_HEADER_LEN + root_position)
                    .is_multiple_of(required_alignment)
            })
            .expect("an unaligned offset exists");
        transport[offset..offset + encoded.len()].copy_from_slice(&encoded);
        let unaligned = &transport[offset..offset + encoded.len()];
        let restored = VnodePartial::decode(unaligned).unwrap();
        assert_eq!(restored.operators[0].0, "agg");
        assert_eq!(restored.operators[0].1, vec![1, 2, 3]);
    }
}

//! Per-vnode durable partial state in an immutable checkpoint-attempt namespace.
//!
//! An empty `operators` map is valid: it seals the epoch carrying no state.

use crate::error::DbError;

mod v2;

/// Operator-state slices for one vnode at one epoch, in one of three shapes:
///
/// - FULL: `operators` non-empty, `base = None`, `deltas` empty.
/// - REFERENCE: `operators`/`deltas` empty, `base = Some(attempt)` — byte-identical to the base.
/// - DELTA: `deltas` non-empty, `base = Some(parent)` — per-operator changes since `parent`.
///
/// `base` is an exact parent-attempt link; the reader walks it back to a FULL and replays deltas
/// forward. The writer re-bases before the base leaves the prune window.
#[derive(Debug, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct VnodePartial {
    /// `(operator_name, vnode-slice bytes)`: FULL slices; also operators that re-based this epoch.
    pub operators: Vec<(String, Vec<u8>)>,
    /// Exact parent attempt for a reference or delta chain.
    pub base: Option<laminar_core::state::CheckpointAttempt>,
    /// `(operator_name, changed-state bytes)`. Non-empty only for delta partials.
    pub deltas: Vec<(String, Vec<u8>)>,
}

impl VnodePartial {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, DbError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map(|v| v.to_vec())
            .map_err(|e| DbError::Checkpoint(format!("vnode partial serialization: {e}")))
    }

    /// Deserialize a `partial.bin` blob; returns `Err` for legacy markers so callers can skip them.
    #[cfg_attr(not(feature = "cluster"), allow(dead_code))]
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DbError> {
        const ARCHIVE_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;

        if bytes.as_ptr().align_offset(ARCHIVE_ALIGNMENT) == 0 {
            return Self::decode_aligned(bytes);
        }

        // `Bytes` and arbitrary byte slices do not promise the alignment required by rkyv's
        // archived format. This is normally handled once by ObjectStoreBackend, but keeping the
        // decoder total over arbitrary slices also covers custom and in-process backends.
        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        Self::decode_aligned(&aligned)
    }

    fn decode_aligned(bytes: &[u8]) -> Result<Self, DbError> {
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(bytes)
            .map_err(|e| DbError::Checkpoint(format!("vnode partial deserialization: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips() {
        let p = VnodePartial {
            operators: vec![
                ("agg".to_string(), vec![1, 2, 3]),
                ("other".to_string(), vec![]),
            ],
            base: None,
            deltas: Vec::new(),
        };
        let bytes = p.encode().unwrap();
        let back = VnodePartial::decode(&bytes).unwrap();
        assert_eq!(back.operators.len(), 2);
        assert_eq!(back.operators[0].0, "agg");
        assert_eq!(back.operators[0].1, vec![1, 2, 3]);
    }

    #[test]
    fn empty_operators_round_trips() {
        let p = VnodePartial {
            operators: Vec::new(),
            base: None,
            deltas: Vec::new(),
        };
        let bytes = p.encode().unwrap();
        let back = VnodePartial::decode(&bytes).unwrap();
        assert!(back.operators.is_empty());
    }

    #[test]
    fn reference_round_trips() {
        let p = VnodePartial {
            operators: Vec::new(),
            base: Some(laminar_core::state::CheckpointAttempt::canonical(9)),
            deltas: Vec::new(),
        };
        let bytes = p.encode().unwrap();
        let back = VnodePartial::decode(&bytes).unwrap();
        assert_eq!(
            back.base,
            Some(laminar_core::state::CheckpointAttempt::canonical(9))
        );
        assert!(back.operators.is_empty());
    }

    #[test]
    fn delta_round_trips_changed_state() {
        let parent = laminar_core::state::CheckpointAttempt::canonical(9);
        let p = VnodePartial {
            operators: Vec::new(),
            base: Some(parent),
            deltas: vec![("agg".to_string(), vec![4, 5, 6])],
        };
        let bytes = p.encode().unwrap();
        let back = VnodePartial::decode(&bytes).unwrap();
        assert_eq!(back.base, Some(parent));
        assert_eq!(back.deltas, vec![("agg".to_string(), vec![4, 5, 6])]);
    }

    #[test]
    fn legacy_marker_decode_is_err_not_panic() {
        // The old payload was the literal string `ckpt:{id}`.
        let err = VnodePartial::decode(b"ckpt:123");
        assert!(err.is_err(), "legacy marker must fail to decode, not panic");
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

        // Object-store clients may return a `Bytes` view into a larger network buffer. Recreate
        // that shape with an offset which makes the archive root unaligned.
        let mut transport = vec![0_u8; encoded.len() + required_alignment];
        let root_position =
            rkyv::api::root_position::<<VnodePartial as rkyv::Archive>::Archived>(encoded.len());
        let offset = (0..required_alignment)
            .find(|offset| {
                !(transport.as_ptr() as usize + offset + root_position)
                    .is_multiple_of(required_alignment)
            })
            .expect("an unaligned offset exists");
        transport[offset..offset + encoded.len()].copy_from_slice(&encoded);
        let unaligned = &transport[offset..offset + encoded.len()];
        assert!(!(unaligned.as_ptr() as usize + root_position).is_multiple_of(required_alignment));

        let restored = VnodePartial::decode(unaligned).unwrap();
        assert_eq!(restored.operators[0].0, "agg");
        assert_eq!(restored.operators[0].1, vec![1, 2, 3]);
    }
}

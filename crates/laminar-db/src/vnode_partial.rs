//! Per-vnode durable partial state in an immutable checkpoint-attempt namespace.
//!
//! An empty `operators` map is valid: it seals the epoch carrying no state.

use crate::error::DbError;

#[cfg(not(test))]
mod v2;
#[cfg(test)]
pub(crate) mod v2;

/// Operator-state slices for one vnode at one epoch, in one of three shapes:
///
/// - FULL/EMPTY: `base = None`, `deltas` empty, and `operators` may be empty.
/// - REFERENCE: `operators`/`deltas` empty, `base = Some(attempt)` — byte-identical to the base.
/// - DELTA: `deltas` non-empty, `base = Some(parent)` — per-operator changes since `parent`.
///
/// `base` is an exact parent-attempt link; the reader walks it back to a FULL and replays deltas
/// forward. The writer re-bases before the base leaves the prune window.
///
/// This raw rkyv layout is also used by the currently admitted cluster global aggregate. Do not
/// change its persisted shape until a version-fenced rolling-upgrade and rollback policy exists.
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
            .map(|value| value.to_vec())
            .map_err(|error| DbError::Checkpoint(format!("vnode partial serialization: {error}")))
    }

    /// Deserialize a `partial.bin` blob; returns `Err` for older literal checkpoint markers.
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
            .map_err(|error| DbError::Checkpoint(format!("vnode partial deserialization: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let restored = VnodePartial::decode(&partial.encode().unwrap()).unwrap();
        assert_eq!(restored.operators.len(), 2);
        assert_eq!(restored.operators[0].0, "agg");
        assert_eq!(restored.operators[0].1, vec![1, 2, 3]);
    }

    #[test]
    fn empty_full_round_trips() {
        let restored = VnodePartial::decode(&VnodePartial::default().encode().unwrap()).unwrap();
        assert!(restored.operators.is_empty());
        assert!(restored.base.is_none());
    }

    #[test]
    fn reference_round_trips() {
        let parent = laminar_core::state::CheckpointAttempt::canonical(9);
        let partial = VnodePartial {
            operators: Vec::new(),
            base: Some(parent),
            deltas: Vec::new(),
        };
        let restored = VnodePartial::decode(&partial.encode().unwrap()).unwrap();
        assert_eq!(restored.base, Some(parent));
        assert!(restored.operators.is_empty());
    }

    #[test]
    fn delta_round_trips_changed_state() {
        let parent = laminar_core::state::CheckpointAttempt::canonical(9);
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
    fn admitted_global_aggregate_delta_wire_is_stable() {
        // Frozen from the raw rkyv layout emitted before the managed-keyed-state work. The
        // admitted cluster global aggregate can persist DELTA artifacts, so an accidental layout
        // change here would strand an otherwise valid rolling-upgrade checkpoint.
        const ESTABLISHED_DELTA: &[u8] = &[
            4, 5, 6, 0, 97, 103, 103, 255, 255, 255, 255, 255, 244, 255, 255, 255, 3, 0, 0, 0, 0,
            0, 0, 0, 232, 255, 255, 255, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0,
            0, 9, 0, 0, 0, 0, 0, 0, 0, 204, 255, 255, 255, 1, 0, 0, 0,
        ];
        let parent = laminar_core::state::CheckpointAttempt::canonical(9);
        let partial = VnodePartial {
            operators: Vec::new(),
            base: Some(parent),
            deltas: vec![("agg".to_string(), vec![4, 5, 6])],
        };

        assert_eq!(partial.encode().unwrap(), ESTABLISHED_DELTA);
        let restored = VnodePartial::decode(ESTABLISHED_DELTA).unwrap();
        assert_eq!(restored.base, Some(parent));
        assert_eq!(restored.deltas, vec![("agg".to_string(), vec![4, 5, 6])]);
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

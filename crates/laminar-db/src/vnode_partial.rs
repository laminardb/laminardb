//! Per-vnode durable partial state in an immutable checkpoint-attempt namespace.
//!
//! An empty `operators` map is valid only when the bound graph has no managed participant for the
//! vnode. A stateful semantic empty is a named operator payload, not an omitted participant.

use crate::error::DbError;
#[cfg(any(feature = "cluster", test))]
use laminar_core::checkpoint::VnodeRestoreLimitProfile;

#[cfg(not(test))]
mod v2;
#[cfg(test)]
pub(crate) mod v2;

/// Operator-state slices for one vnode at one epoch, in one of three shapes:
///
/// - FULL/EMPTY: `base = None`, `deltas` empty, and `operators` names every managed participant;
///   it may be empty only for a graph-authoritative empty roster.
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
    #[cfg(test)]
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

    /// Validate the current durable restore profile before allocating owned outer containers.
    ///
    /// The legacy raw-rkyv format predates an explicit directory-entry bound. A checked archived
    /// view keeps corrupt offsets and self-declared vector lengths borrowed while the committed
    /// profile's roster ceiling is enforced. Only then may deserialization allocate the owned
    /// `String`/`Vec` values consumed by recovery.
    #[cfg(any(feature = "cluster", test))]
    pub(crate) fn decode_for_restore(
        bytes: &[u8],
        profile: VnodeRestoreLimitProfile,
    ) -> Result<Self, DbError> {
        let max_entries = match profile {
            VnodeRestoreLimitProfile::GlobalSingletonCompatibility => 1,
        };
        Self::decode_for_restore_with_entry_limit(bytes, profile, max_entries)
    }

    /// Test-only extension for multi-participant transition-protocol fixtures. Production remains
    /// bound to the committed profile above; these fixtures deliberately exercise prepare-all /
    /// abort-all behavior that the currently admitted single-global-operator profile cannot emit.
    #[cfg(all(test, feature = "cluster"))]
    pub(crate) fn decode_for_restore_test_roster(
        bytes: &[u8],
        profile: VnodeRestoreLimitProfile,
        max_entries: usize,
    ) -> Result<Self, DbError> {
        Self::decode_for_restore_with_entry_limit(bytes, profile, max_entries)
    }

    #[cfg(any(feature = "cluster", test))]
    fn decode_for_restore_with_entry_limit(
        bytes: &[u8],
        profile: VnodeRestoreLimitProfile,
        max_entries: usize,
    ) -> Result<Self, DbError> {
        const ARCHIVE_ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;

        if bytes.as_ptr().align_offset(ARCHIVE_ALIGNMENT) == 0 {
            return Self::decode_aligned_for_restore(bytes, profile, max_entries);
        }

        let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
        aligned.extend_from_slice(bytes);
        Self::decode_aligned_for_restore(&aligned, profile, max_entries)
    }

    #[cfg(test)]
    fn decode_aligned(bytes: &[u8]) -> Result<Self, DbError> {
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(bytes)
            .map_err(|error| DbError::Checkpoint(format!("vnode partial deserialization: {error}")))
    }

    #[cfg(any(feature = "cluster", test))]
    fn decode_aligned_for_restore(
        bytes: &[u8],
        profile: VnodeRestoreLimitProfile,
        max_entries: usize,
    ) -> Result<Self, DbError> {
        let archived = rkyv::access::<<Self as rkyv::Archive>::Archived, rkyv::rancor::Error>(
            bytes,
        )
        .map_err(|error| DbError::Checkpoint(format!("vnode partial deserialization: {error}")))?;
        let entries = archived
            .operators
            .len()
            .checked_add(archived.deltas.len())
            .ok_or_else(|| {
                DbError::Checkpoint("vnode partial outer entry count overflow".into())
            })?;
        if entries > max_entries {
            return Err(DbError::Checkpoint(format!(
                "vnode partial has {entries} outer state entries; restore profile {profile:?} allows at most {max_entries}"
            )));
        }
        if archived.base.is_none() && !archived.deltas.is_empty() {
            return Err(DbError::Checkpoint(
                "vnode partial root contains delta state without a parent".into(),
            ));
        }

        // Reuse the checked view so the accepted archive is not validated a second time inside
        // this call. With the roster ceiling established, owned outer allocation is bounded by
        // the admitted name/body pairs and the already-bounded serialized payload. Production's
        // current compatibility profile admits one pair.
        rkyv::deserialize::<Self, rkyv::rancor::Error>(archived)
            .map_err(|error| DbError::Checkpoint(format!("vnode partial deserialization: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESTORE_PROFILE: VnodeRestoreLimitProfile =
        VnodeRestoreLimitProfile::GlobalSingletonCompatibility;

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

    #[test]
    fn restore_decode_accepts_current_profile_shapes() {
        let parent = laminar_core::state::CheckpointAttempt::canonical(9);
        let cases = [
            VnodePartial {
                operators: vec![("agg".to_string(), vec![1, 2, 3])],
                base: None,
                deltas: Vec::new(),
            },
            VnodePartial {
                operators: Vec::new(),
                base: Some(parent),
                deltas: vec![("agg".to_string(), vec![4, 5, 6])],
            },
            VnodePartial {
                operators: Vec::new(),
                base: Some(parent),
                deltas: Vec::new(),
            },
        ];

        for expected in cases {
            let restored =
                VnodePartial::decode_for_restore(&expected.encode().unwrap(), RESTORE_PROFILE)
                    .unwrap();
            assert_eq!(restored.operators, expected.operators);
            assert_eq!(restored.base, expected.base);
            assert_eq!(restored.deltas, expected.deltas);
        }
    }

    #[test]
    fn restore_decode_rejects_outer_entry_amplification() {
        let encoded = VnodePartial {
            operators: vec![
                ("agg".to_string(), Vec::new()),
                ("unexpected".to_string(), Vec::new()),
            ],
            base: None,
            deltas: Vec::new(),
        }
        .encode()
        .unwrap();

        let error = VnodePartial::decode_for_restore(&encoded, RESTORE_PROFILE).unwrap_err();
        assert!(error.to_string().contains("allows at most 1"));
    }

    #[test]
    fn restore_decode_rejects_delta_without_parent() {
        let encoded = VnodePartial {
            operators: Vec::new(),
            base: None,
            deltas: vec![("agg".to_string(), Vec::new())],
        }
        .encode()
        .unwrap();

        let error = VnodePartial::decode_for_restore(&encoded, RESTORE_PROFILE).unwrap_err();
        assert!(error.to_string().contains("without a parent"));
    }

    #[test]
    fn restore_decode_rejects_truncated_unaligned_input_without_panicking() {
        let encoded = VnodePartial {
            operators: vec![("agg".to_string(), vec![1, 2, 3])],
            base: None,
            deltas: Vec::new(),
        }
        .encode()
        .unwrap();
        const ALIGNMENT: usize = rkyv::util::AlignedVec::<16>::ALIGNMENT;
        let mut transport = vec![0_u8; encoded.len() + ALIGNMENT];
        let offset = (0..ALIGNMENT)
            .find(|offset| {
                transport
                    .as_ptr()
                    .wrapping_add(*offset)
                    .align_offset(ALIGNMENT)
                    != 0
            })
            .expect("an unaligned transport offset exists");
        transport[offset..offset + encoded.len()].copy_from_slice(&encoded);
        let unaligned = &transport[offset..offset + encoded.len()];
        assert_ne!(unaligned.as_ptr().align_offset(ALIGNMENT), 0);
        VnodePartial::decode_for_restore(unaligned, RESTORE_PROFILE).unwrap();

        let error =
            VnodePartial::decode_for_restore(&unaligned[..unaligned.len() - 1], RESTORE_PROFILE)
                .unwrap_err();
        assert!(error.to_string().contains("vnode partial deserialization"));
    }
}

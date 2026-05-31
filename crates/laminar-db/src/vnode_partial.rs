//! Per-vnode durable partial state.
//!
//! Replaces the old `ckpt:{id}` marker that `partial.bin` used to hold. A
//! `VnodePartial` is the slice of operator state belonging to a single vnode
//! at a single committed epoch: when a node acquires that vnode in a
//! rebalance it reads this blob from the shared object store
//! (`epoch=N/vnode=V/partial.bin`) and merges each operator's slice into the
//! corresponding live operator.
//!
//! The presence of `partial.bin` still drives the durability gate
//! (`StateBackend::epoch_complete`), so an empty `operators` map (a vnode this
//! node owns but for which no operator carried state) is valid and seals the
//! epoch exactly as the marker did.

use crate::error::DbError;

/// Operator-state slices for one vnode at one epoch.
///
/// Each entry is `(operator_name, that operator's serialized vnode slice)`.
/// The bytes are exactly what the operator's per-vnode checkpoint produced
/// (e.g. an rkyv-encoded `AggStateCheckpoint` holding only this vnode's
/// groups), so the apply path can hand them straight back to the operator.
#[derive(Debug, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct VnodePartial {
    /// Checkpoint id this partial was sealed under — for audit/logging.
    pub checkpoint_id: u64,
    /// `(operator_name, vnode-slice bytes)`.
    pub operators: Vec<(String, Vec<u8>)>,
}

impl VnodePartial {
    /// Serialize for `write_partial`.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, DbError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map(|v| v.to_vec())
            .map_err(|e| DbError::Checkpoint(format!("vnode partial serialization: {e}")))
    }

    /// Deserialize a `partial.bin` blob.
    ///
    /// Best-effort by contract at the call site: a blob that isn't a
    /// `VnodePartial` (a legacy `ckpt:{id}` marker, or hand-written test
    /// bytes) returns `Err` and the caller skips it rather than aborting the
    /// rebalance.
    ///
    /// Only the cluster rehydration path decodes partials; in a non-cluster
    /// build this is exercised solely by unit tests.
    #[cfg_attr(not(feature = "cluster-unstable"), allow(dead_code))]
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, DbError> {
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
            checkpoint_id: 42,
            operators: vec![
                ("agg".to_string(), vec![1, 2, 3]),
                ("other".to_string(), vec![]),
            ],
        };
        let bytes = p.encode().unwrap();
        let back = VnodePartial::decode(&bytes).unwrap();
        assert_eq!(back.checkpoint_id, 42);
        assert_eq!(back.operators.len(), 2);
        assert_eq!(back.operators[0].0, "agg");
        assert_eq!(back.operators[0].1, vec![1, 2, 3]);
    }

    #[test]
    fn empty_operators_round_trips() {
        let p = VnodePartial {
            checkpoint_id: 7,
            operators: Vec::new(),
        };
        let bytes = p.encode().unwrap();
        let back = VnodePartial::decode(&bytes).unwrap();
        assert_eq!(back.checkpoint_id, 7);
        assert!(back.operators.is_empty());
    }

    #[test]
    fn legacy_marker_decode_is_err_not_panic() {
        // The old payload was the literal string `ckpt:{id}`.
        let err = VnodePartial::decode(b"ckpt:123");
        assert!(err.is_err(), "legacy marker must fail to decode, not panic");
    }
}

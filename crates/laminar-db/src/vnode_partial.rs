//! Per-vnode durable partial state in an immutable checkpoint-attempt namespace.
//!
//! An empty `operators` map is valid: it seals the epoch carrying no state.

use crate::error::DbError;

/// One operator's delta: changed groups + removed-key tombstones. Both empty = carry-forward.
#[derive(Debug, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct OpDelta {
    pub changed: Vec<u8>,
    pub tombstones_ipc: Vec<u8>,
}

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
    /// `(operator_name, delta)`. Non-empty only for delta partials.
    pub deltas: Vec<(String, OpDelta)>,
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
            base: Some(laminar_core::state::CheckpointAttempt::new(4, 9)),
            deltas: Vec::new(),
        };
        let bytes = p.encode().unwrap();
        let back = VnodePartial::decode(&bytes).unwrap();
        assert_eq!(
            back.base,
            Some(laminar_core::state::CheckpointAttempt::new(4, 9))
        );
        assert!(back.operators.is_empty());
    }

    #[test]
    fn legacy_marker_decode_is_err_not_panic() {
        // The old payload was the literal string `ckpt:{id}`.
        let err = VnodePartial::decode(b"ckpt:123");
        assert!(err.is_err(), "legacy marker must fail to decode, not panic");
    }
}

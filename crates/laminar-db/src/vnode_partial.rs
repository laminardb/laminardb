//! Per-vnode durable partial state (`epoch=N/vnode=V/partial.bin`).
//!
//! An empty `operators` map is valid — it seals the epoch without carrying state,
//! which is correct for vnodes whose operators hold nothing.

use crate::error::DbError;

/// One operator's incremental delta (Lever 2): the groups changed this epoch
/// (columnar `AggStateCheckpoint` bytes) plus the keys removed (tombstone IPC).
/// Both empty = a carry-forward (nothing changed since the parent).
#[derive(Debug, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct OpDelta {
    pub changed: Vec<u8>,
    pub tombstones_ipc: Vec<u8>,
}

/// Operator-state slices for one vnode at one epoch. One of three kinds:
///
/// - FULL: `operators` non-empty, `base_epoch = None`, `deltas` empty.
/// - REFERENCE (Lever 1): `operators`/`deltas` empty, `base_epoch = Some(N)` — byte-identical
///   to the full partial at epoch N; the reader follows this one hop.
/// - DELTA (Lever 2): `deltas` non-empty, `base_epoch = Some(parent)` — per-operator changes
///   since `parent` (itself a FULL, REFERENCE, or DELTA); the reader walks parents back to a
///   FULL collecting deltas, then replays FULL → deltas forward.
///
/// `base_epoch` doubles as the parent link. The writer re-bases (re-emits FULL) before the base
/// leaves the prune window.
#[derive(Debug, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub(crate) struct VnodePartial {
    /// Sealed epoch, for audit.
    pub checkpoint_id: u64,
    /// `(operator_name, vnode-slice bytes)`. Empty for references and deltas.
    pub operators: Vec<(String, Vec<u8>)>,
    /// `Some(epoch)` = parent link (reference base, or delta parent).
    pub base_epoch: Option<u64>,
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
            checkpoint_id: 42,
            operators: vec![
                ("agg".to_string(), vec![1, 2, 3]),
                ("other".to_string(), vec![]),
            ],
            base_epoch: None,
            deltas: Vec::new(),
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
            base_epoch: None,
            deltas: Vec::new(),
        };
        let bytes = p.encode().unwrap();
        let back = VnodePartial::decode(&bytes).unwrap();
        assert_eq!(back.checkpoint_id, 7);
        assert!(back.operators.is_empty());
    }

    #[test]
    fn reference_round_trips() {
        let p = VnodePartial {
            checkpoint_id: 9,
            operators: Vec::new(),
            base_epoch: Some(4),
            deltas: Vec::new(),
        };
        let bytes = p.encode().unwrap();
        let back = VnodePartial::decode(&bytes).unwrap();
        assert_eq!(back.base_epoch, Some(4));
        assert!(back.operators.is_empty());
    }

    #[test]
    fn legacy_marker_decode_is_err_not_panic() {
        // The old payload was the literal string `ckpt:{id}`.
        let err = VnodePartial::decode(b"ckpt:123");
        assert!(err.is_err(), "legacy marker must fail to decode, not panic");
    }
}

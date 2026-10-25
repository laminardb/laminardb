//! Vnode frames of changelog views: deterministic per-vnode encoding, dirty capture, and
//! restore into a private image.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::row::RowConverter;

use super::checkpoint::{
    batches_to_ipc_bounded, ipc_to_schema_and_batches, multiset_checkpoint_schema,
    multiset_counted_checkpoint_batch, upsert_checkpoint_batch, CHECKPOINT_CAPTURE_ROW_OVERHEAD,
};
use super::multiset::MultisetSlice;
use super::upsert::UpsertSlice;
use super::{MultisetState, MvEntry, MvLimits, MvStorageMode, MvStore, UpsertState};
use crate::error::DbError;
use crate::operator_graph::{CapturedVnodeState, EncodedStateFrame, StateFrameCapture};

/// An empty slice. An Arrow IPC stream begins with continuation bytes `0xFF`, so this frame
/// never parses as one.
pub(super) const EMPTY_SLICE_FRAME: [u8; 1] = [0];

impl MvEntry {
    fn changelog_slices_mut(&mut self, name: &str) -> Result<SlicesMut<'_>, DbError> {
        match (&mut self.upsert, &mut self.multiset) {
            (Some(upsert), None) => Ok(SlicesMut::Upsert(upsert)),
            (None, Some(multiset)) => Ok(SlicesMut::Multiset(multiset)),
            _ => Err(DbError::Checkpoint(format!(
                "view '{name}' is not a changelog view with vnode slices"
            ))),
        }
    }

    fn capture_vnodes(
        &mut self,
        name: &str,
        required: &[u32],
        vnode_count: u32,
        max_capture_bytes: u64,
    ) -> Result<Vec<CapturedVnodeState>, DbError> {
        let schema = Arc::clone(&self.schema);
        let mut remaining = max_capture_bytes;
        let mut captured = Vec::new();
        match self.changelog_slices_mut(name)? {
            SlicesMut::Upsert(upsert) => {
                let roster =
                    upsert
                        .slices
                        .take_capture_roster(required, vnode_count, HashMap::is_empty)?;
                for vnode in roster {
                    let slice = upsert.slices.slot(vnode);
                    let state = upsert_frame(name, vnode, &schema, slice, &mut remaining)?;
                    captured.push(CapturedVnodeState {
                        vnode,
                        state: Some(state),
                    });
                }
            }
            SlicesMut::Multiset(multiset) => {
                let roster = multiset.slices.take_capture_roster(
                    required,
                    vnode_count,
                    HashMap::is_empty,
                )?;
                for vnode in roster {
                    let slice = multiset.slices.slot(vnode);
                    let converter = &multiset.row_converter;
                    let state =
                        multiset_frame(name, vnode, &schema, converter, slice, &mut remaining)?;
                    captured.push(CapturedVnodeState {
                        vnode,
                        state: Some(state),
                    });
                }
            }
        }
        Ok(captured)
    }

    fn restore_vnode(
        &mut self,
        name: &str,
        vnode: u32,
        vnode_count: u32,
        bytes: &[u8],
        limits: MvLimits,
    ) -> Result<(), DbError> {
        let schema = Arc::clone(&self.schema);
        let slices = self.changelog_slices_mut(name)?;
        let expected_count = match &slices {
            SlicesMut::Upsert(upsert) => upsert.slices.vnode_count(),
            SlicesMut::Multiset(multiset) => multiset.slices.vnode_count(),
        };
        if vnode_count != expected_count.get() || vnode >= vnode_count {
            return Err(DbError::Checkpoint(format!(
                "view '{name}' frame for vnode {vnode} of {vnode_count} does not match its {expected_count} slices"
            )));
        }
        if bytes == EMPTY_SLICE_FRAME {
            return Ok(());
        }
        let (frame_schema, batches) = ipc_to_schema_and_batches(bytes).map_err(|error| {
            DbError::Checkpoint(format!("view '{name}' vnode {vnode} frame: {error}"))
        })?;
        let approx_bytes = match slices {
            SlicesMut::Upsert(upsert) => {
                if frame_schema != schema {
                    return Err(DbError::Checkpoint(format!(
                        "view '{name}' vnode {vnode} frame schema does not match the view"
                    )));
                }
                for batch in &batches {
                    upsert.restore_slice(name, vnode, batch, limits)?;
                }
                upsert.approx_bytes
            }
            SlicesMut::Multiset(multiset) => {
                if frame_schema != multiset_checkpoint_schema(&schema) {
                    return Err(DbError::Checkpoint(format!(
                        "view '{name}' vnode {vnode} multiset frame schema or format does not match the view"
                    )));
                }
                for batch in &batches {
                    multiset.restore_slice(name, vnode, batch, limits)?;
                }
                multiset.approx_bytes
            }
        };
        self.approx_bytes = approx_bytes;
        Ok(())
    }
}

enum SlicesMut<'a> {
    Upsert(&'a mut UpsertState),
    Multiset(&'a mut MultisetState),
}

/// A copy of one upsert slice, encoded sorted by key when the checkpoint serializes it.
fn upsert_frame(
    name: &str,
    vnode: u32,
    schema: &SchemaRef,
    slice: &UpsertSlice,
    remaining: &mut u64,
) -> Result<StateFrameCapture, DbError> {
    if slice.is_empty() {
        return Ok(StateFrameCapture::encoded_static(&EMPTY_SLICE_FRAME));
    }
    let mut bytes = 0usize;
    let mut rows = Vec::with_capacity(slice.len());
    for (key, values) in slice {
        bytes = bytes
            .saturating_add(UpsertState::row_size(key, values)?)
            .saturating_add(CHECKPOINT_CAPTURE_ROW_OVERHEAD);
        rows.push((key.clone(), values.clone()));
    }
    let retained = charge(name, vnode, bytes, remaining)?;
    let schema = Arc::clone(schema);
    Ok(StateFrameCapture::deferred(retained, move |limit| {
        let batch = upsert_checkpoint_batch(&schema, &mut rows)?;
        ipc_frame(&schema, &batch, limit)
    }))
}

/// A copy of one multiset slice, encoded as counted rows sorted by encoding.
fn multiset_frame(
    name: &str,
    vnode: u32,
    schema: &SchemaRef,
    converter: &Arc<RowConverter>,
    slice: &MultisetSlice,
    remaining: &mut u64,
) -> Result<StateFrameCapture, DbError> {
    if slice.is_empty() {
        return Ok(StateFrameCapture::encoded_static(&EMPTY_SLICE_FRAME));
    }
    let bytes = slice.keys().fold(0usize, |total, key| {
        total
            .saturating_add(key.as_ref().len())
            .saturating_add(CHECKPOINT_CAPTURE_ROW_OVERHEAD)
    });
    let mut counts: Vec<_> = slice
        .iter()
        .map(|(key, count)| (key.clone(), *count))
        .collect();
    let retained = charge(name, vnode, bytes, remaining)?;
    let (schema, converter) = (Arc::clone(schema), Arc::clone(converter));
    Ok(StateFrameCapture::deferred(retained, move |limit| {
        let batch = multiset_counted_checkpoint_batch(&schema, &converter, &mut counts)?;
        ipc_frame(&batch.schema(), &batch, limit)
    }))
}

fn charge(name: &str, vnode: u32, bytes: usize, remaining: &mut u64) -> Result<u64, DbError> {
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    *remaining = remaining.checked_sub(bytes).ok_or_else(|| {
        DbError::Checkpoint(format!(
            "view '{name}' vnode {vnode} state exceeds the remaining capture budget"
        ))
    })?;
    Ok(bytes)
}

fn ipc_frame(
    schema: &SchemaRef,
    batch: &arrow::array::RecordBatch,
    limit: usize,
) -> Result<EncodedStateFrame, DbError> {
    let limit = u64::try_from(limit).unwrap_or(u64::MAX);
    batches_to_ipc_bounded(schema, std::iter::once(batch), limit)
        .map(EncodedStateFrame::from_vec)
        .map_err(|error| DbError::Checkpoint(format!("view frame serialization failed: {error}")))
}

impl MvStore {
    /// Capture `view`'s frames for the canonical `required` roster. After the first capture
    /// only vnodes changed since the previous capture are returned; the checkpoint coordinator
    /// carries the others forward from the committed predecessor.
    pub(crate) fn capture_view_vnodes(
        &mut self,
        view: &str,
        required: &[u32],
        vnode_count: u32,
        max_capture_bytes: u64,
    ) -> Result<Vec<CapturedVnodeState>, DbError> {
        self.entries
            .get_mut(view)
            .ok_or_else(|| DbError::Checkpoint(format!("view '{view}' is not registered")))?
            .capture_vnodes(view, required, vnode_count, max_capture_bytes)
    }

    /// Make the next capture of `view` include every required vnode.
    pub(crate) fn require_full_view_capture(&mut self, view: &str) {
        let Some(entry) = self.entries.get_mut(view) else {
            return;
        };
        if let Some(upsert) = entry.upsert.as_mut() {
            upsert.slices.require_full_capture();
        }
        if let Some(multiset) = entry.multiset.as_mut() {
            multiset.slices.require_full_capture();
        }
    }

    /// An empty copy of `view` that restores frames without touching this store.
    pub(crate) fn restore_target(&self, view: &str) -> Result<RestoredView, DbError> {
        let entry = self
            .entries
            .get(view)
            .ok_or_else(|| DbError::Checkpoint(format!("view '{view}' is not registered")))?;
        Ok(RestoredView {
            name: view.to_owned(),
            entry: MvEntry::new(Arc::clone(&entry.schema), entry.mode.clone())?,
            limits: self.limits,
        })
    }
}

/// The rows of one committed vnode frame of a changelog view, decoded and validated as a
/// restore would.
pub(super) fn decode_view_frame(
    shape: &super::committed::ViewShape,
    vnode: u32,
    vnode_count: u32,
    bytes: &[u8],
) -> Result<arrow::array::RecordBatch, DbError> {
    let mut entry = MvEntry::new(Arc::clone(&shape.schema), shape.mode.clone())?;
    entry.restore_vnode(&shape.name, vnode, vnode_count, bytes, shape.limits)?;
    entry.to_record_batch()
}

/// One changelog view's slices restored from committed vnode frames, held privately until the
/// recovery image installs it.
pub(crate) struct RestoredView {
    name: String,
    entry: MvEntry,
    limits: MvLimits,
}

impl RestoredView {
    pub(crate) fn restore_vnode(
        &mut self,
        vnode: u32,
        vnode_count: u32,
        bytes: &[u8],
    ) -> Result<(), DbError> {
        self.entry
            .restore_vnode(&self.name, vnode, vnode_count, bytes, self.limits)
    }

    #[cfg(feature = "cluster")]
    pub(super) const fn entry(&self) -> &MvEntry {
        &self.entry
    }

    #[cfg(feature = "cluster")]
    pub(super) fn into_entry(self) -> MvEntry {
        self.entry
    }

    pub(super) fn install(self, entries: &mut HashMap<String, MvEntry>) -> Result<(), DbError> {
        let current = entries.get_mut(&self.name).ok_or_else(|| {
            DbError::Checkpoint(format!(
                "restored view '{}' has no matching registered materialized view",
                self.name
            ))
        })?;
        let compatible = current.schema == self.entry.schema
            && matches!(
                (&current.mode, &self.entry.mode),
                (
                    MvStorageMode::Changelog { key, key_groups },
                    MvStorageMode::Changelog { key: restored_key, key_groups: restored_groups },
                ) if key == restored_key && key_groups == restored_groups
            );
        if !compatible {
            return Err(DbError::Checkpoint(format!(
                "restored view '{}' no longer matches its registered shape",
                self.name
            )));
        }
        *current = self.entry;
        Ok(())
    }
}

#[cfg(test)]
impl MvStore {
    fn view_vnode_count(&self, view: &str) -> u32 {
        match &self.entries[view].mode {
            MvStorageMode::Changelog { key_groups, .. } => u32::from(key_groups.get()),
            MvStorageMode::AppendLog { .. } => panic!("view '{view}' has no vnode slices"),
        }
    }

    /// The encoded frames the next capture of `view` writes over every vnode.
    pub(crate) fn encoded_view_frames(&mut self, view: &str) -> Vec<(u32, bytes::Bytes)> {
        let count = self.view_vnode_count(view);
        let vnodes: Vec<u32> = (0..count).collect();
        let captures = self
            .capture_view_vnodes(view, &vnodes, count, u64::MAX)
            .unwrap();
        encode_captures(captures)
    }

    /// Every encoded frame of `view`, as a first checkpoint writes them.
    pub(crate) fn full_view_frames(&mut self, view: &str) -> Vec<(u32, bytes::Bytes)> {
        self.require_full_view_capture(view);
        self.encoded_view_frames(view)
    }

    /// A recovery image whose `view` is restored from `frames`.
    pub(crate) fn image_from_view_frames(
        &self,
        view: &str,
        frames: &[(u32, bytes::Bytes)],
    ) -> Result<Self, DbError> {
        let count = self.view_vnode_count(view);
        let mut restored = self.restore_target(view)?;
        for (vnode, bytes) in frames {
            restored.restore_vnode(*vnode, count, bytes)?;
        }
        self.recovery_image(vec![restored], None)
    }
}

#[cfg(test)]
pub(crate) fn encode_captures(captures: Vec<CapturedVnodeState>) -> Vec<(u32, bytes::Bytes)> {
    let mut staged = captures
        .iter()
        .filter_map(|captured| captured.state.as_ref())
        .map(StateFrameCapture::retained_bytes)
        .sum();
    captures
        .into_iter()
        .map(|captured| {
            let state = captured.state.expect("view captures carry their frames");
            (
                captured.vnode,
                state.materialize(&mut staged, u64::MAX).unwrap(),
            )
        })
        .collect()
}

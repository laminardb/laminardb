//! The graph node owning a changelog view's checkpoint lifecycle. The view's rows stay in the
//! shared `MvStore`, applied once per cycle for every view, so readers keep one cycle across
//! views; this node captures them as vnode frames and restores frames into a private image.

use std::sync::Arc;

use arrow::array::RecordBatch;
use async_trait::async_trait;

use super::capability::{OperatorCapability, OperatorStateClass};
use crate::error::DbError;
use crate::mv_store::{MvStore, RestoredView};
use crate::operator_graph::{CapturedVnodeState, GraphOperator, OperatorCheckpoint};

pub(crate) struct ViewResultOperator {
    view: String,
    state_class: OperatorStateClass,
    store: Arc<parking_lot::RwLock<MvStore>>,
    restored: Option<RestoredView>,
}

impl ViewResultOperator {
    pub(crate) fn new(
        view: String,
        state_class: OperatorStateClass,
        store: Arc<parking_lot::RwLock<MvStore>>,
    ) -> Self {
        Self {
            view,
            state_class,
            store,
            restored: None,
        }
    }
}

#[async_trait]
impl GraphOperator for ViewResultOperator {
    fn cluster_capability(&self) -> OperatorCapability {
        OperatorCapability::view_result(self.state_class)
    }

    async fn initialize_managed_state(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    async fn process(
        &mut self,
        _inputs: &[Vec<RecordBatch>],
        _watermarks: &[i64],
    ) -> Result<Vec<RecordBatch>, DbError> {
        Ok(Vec::new())
    }

    fn checkpoint(&mut self) -> Result<Option<OperatorCheckpoint>, DbError> {
        Ok(None)
    }

    fn checkpoint_vnodes(
        &mut self,
        required_vnodes: &[u32],
        vnode_count: u32,
        max_capture_bytes: u64,
    ) -> Result<Option<Vec<CapturedVnodeState>>, DbError> {
        self.store
            .write()
            .capture_view_vnodes(&self.view, required_vnodes, vnode_count, max_capture_bytes)
            .map(Some)
    }

    fn restore_vnode(&mut self, vnode: u32, vnode_count: u32, state: &[u8]) -> Result<(), DbError> {
        let restored = match &mut self.restored {
            Some(restored) => restored,
            None => self
                .restored
                .insert(self.store.read().restore_target(&self.view)?),
        };
        restored.restore_vnode(vnode, vnode_count, state)
    }

    fn take_restored_view(&mut self) -> Option<RestoredView> {
        self.restored.take()
    }

    fn force_full_vnode_capture(&mut self) {
        self.store.write().require_full_view_capture(&self.view);
    }
}

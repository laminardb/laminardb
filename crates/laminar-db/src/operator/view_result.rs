//! The graph node owning a changelog view's checkpoint lifecycle. The view's rows stay in the
//! shared `MvStore`, applied once per cycle for every view, so readers keep one cycle across
//! views; this node captures them as vnode frames, restores frames into a private image, and
//! moves slices with their vnodes when a cluster reassigns them.

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
    #[cfg(feature = "cluster")]
    transition: Option<crate::mv_store::ViewTransition>,
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
            #[cfg(feature = "cluster")]
            transition: None,
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

    #[cfg(feature = "cluster")]
    fn bind_view_store(&mut self, store: &Arc<parking_lot::RwLock<MvStore>>) {
        self.store = Arc::clone(store);
    }

    #[cfg(feature = "cluster")]
    fn prepare_vnode_transition(
        &mut self,
        transition: crate::operator_graph::ManagedVnodeTransition<'_>,
    ) -> Result<(), DbError> {
        if self.transition.is_some() {
            return Err(DbError::Checkpoint(format!(
                "view '{}' already holds a prepared vnode transition",
                self.view
            )));
        }
        if !transition.whole_restores.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "view '{}' has no whole-operator state to restore",
                self.view
            )));
        }
        let prepared = self.store.read().prepare_view_transition(
            &self.view,
            transition.target.vnode_count,
            transition.revoked,
            transition
                .restores
                .iter()
                .map(|restore| (restore.vnode, restore.state)),
        )?;
        self.transition = Some(prepared);
        Ok(())
    }

    #[cfg(feature = "cluster")]
    fn abort_vnode_transition(&mut self) {
        self.transition = None;
    }

    #[cfg(feature = "cluster")]
    fn publish_vnode_transition(&mut self) {
        if let Some(transition) = self.transition.take() {
            self.store.write().publish_view_transition(transition);
        }
    }

    fn force_full_vnode_capture(&mut self) {
        self.store.write().require_full_view_capture(&self.view);
    }
}

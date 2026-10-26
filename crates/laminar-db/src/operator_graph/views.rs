//! Result nodes of changelog materialized views, and the frontiers append views retain by.

use std::sync::Arc;

use super::{GraphNode, InputFrontier, OperatorGraph};
use crate::mv_store::{MvStore, RestoredView};
use crate::operator::view_result::ViewResultOperator;

impl OperatorGraph {
    /// Give every changelog view built from now on a result node over `store`.
    pub(crate) fn set_view_store(&mut self, store: Arc<parking_lot::RwLock<MvStore>>) {
        self.view_store = Some(store);
    }

    /// Add `<stream>::view` when `stream` is a changelog view. The node has no input ports;
    /// `remove_query` drops it with its stream.
    pub(super) fn attach_view_result(&mut self, stream: &str) {
        let Some(store) = &self.view_store else {
            return;
        };
        let Some(state_class) = store.read().view_state_class(stream) else {
            return;
        };
        let operator = ViewResultOperator::new(stream.to_owned(), state_class, Arc::clone(store));
        self.allocate_node(GraphNode::new(
            Arc::from(laminar_core::mv::view_result_operator(stream)),
            Box::new(operator),
            0,
        ));
        self.topo_dirty = true;
    }

    /// The output frontier of stream `name`.
    pub(crate) fn stream_frontier(&self, name: &str) -> Option<InputFrontier> {
        let node = *self.output_map.get(name)?;
        let watermark = self.output_watermarks[node];
        Some(InputFrontier {
            watermark: (watermark != i64::MIN).then_some(watermark),
            idle: self.output_idle[node],
        })
    }

    /// Bind the graph and every view result node to `store`.
    #[cfg(feature = "cluster")]
    pub(crate) fn bind_view_store(&mut self, store: &Arc<parking_lot::RwLock<MvStore>>) {
        for node in self.nodes.iter_mut().filter(|node| !node.removed) {
            node.operator.bind_view_store(store);
        }
        self.view_store = Some(Arc::clone(store));
    }

    /// Hand the view store the append-view logs a vnode transition adopts from participants
    /// that left.
    #[cfg(feature = "cluster")]
    pub(super) fn adopt_view_logs(
        &self,
        logs: &[laminar_core::checkpoint::AdoptedViewLog],
    ) -> Result<(), crate::error::DbError> {
        if logs.is_empty() {
            return Ok(());
        }
        let store = self.view_store.as_ref().ok_or_else(|| {
            crate::error::DbError::Checkpoint(
                "a vnode transition adopts view logs, but the graph has no view store".into(),
            )
        })?;
        store.write().adopt_view_logs(logs)
    }

    /// Slices that result nodes restored from the selected checkpoint.
    pub(crate) fn take_restored_views(&mut self) -> Vec<RestoredView> {
        self.nodes
            .iter_mut()
            .filter(|node| !node.removed)
            .filter_map(|node| node.operator.take_restored_view())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::mv_store::{MvStorageMode, ViewKey};
    use crate::operator::capability::OperatorStateClass;
    use crate::operator_graph::GraphStateCapture;

    fn changes(keys: &[i64], n: i64) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("k", Arc::new(Int64Array::from(keys.to_vec())) as _),
            ("n", Arc::new(Int64Array::from(vec![n; keys.len()])) as _),
            (
                laminar_core::changelog::WEIGHT_COLUMN,
                Arc::new(Int64Array::from(vec![1; keys.len()])) as _,
            ),
        ])
        .unwrap()
    }

    fn view_store() -> Arc<parking_lot::RwLock<MvStore>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let mut store = MvStore::new();
        store
            .create_mv(
                "counts",
                schema,
                MvStorageMode::changelog(ViewKey::Keyed(vec![0])),
            )
            .unwrap();
        Arc::new(parking_lot::RwLock::new(store))
    }

    fn graph(store: &Arc<parking_lot::RwLock<MvStore>>) -> OperatorGraph {
        let mut graph = OperatorGraph::new(laminar_sql::create_session_context());
        graph.set_view_store(Arc::clone(store));
        graph.attach_view_result("counts");
        graph
    }

    fn frames(capture: GraphStateCapture) -> Vec<(String, u32, bytes::Bytes)> {
        let mut staged = capture.retained_bytes();
        capture
            .vnodes
            .into_iter()
            .map(|(name, captured)| {
                let state = captured.state.expect("view frames are captured with bytes");
                let bytes = state.materialize(&mut staged, u64::MAX).unwrap();
                (name, captured.vnode, bytes)
            })
            .collect()
    }

    fn rows(store: &MvStore) -> Vec<(i64, i64)> {
        let batch = store.to_record_batch("counts").unwrap().unwrap();
        let column = |index: usize| {
            batch
                .column(index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone()
        };
        let (keys, counts) = (column(0), column(1));
        let mut rows: Vec<_> = (0..batch.num_rows())
            .map(|row| (keys.value(row), counts.value(row)))
            .collect();
        rows.sort_unstable();
        rows
    }

    #[test]
    fn a_changelog_view_checkpoints_and_restores_through_its_result_node() {
        let store = view_store();
        store.write().update("counts", &changes(&[1, 2, 3], 1));
        let mut live = graph(&store);

        let first = live.capture_state(u64::MAX).unwrap();
        assert_eq!(
            first.managed_vnode_operators,
            vec![("counts::view".to_owned(), OperatorStateClass::VnodeKeyed)]
        );
        let committed = frames(first);
        assert_eq!(committed.len(), 256);
        assert!(committed.iter().all(|(name, _, _)| name == "counts::view"));

        store.write().update("counts", &changes(&[2], 5));
        assert_eq!(frames(live.capture_state(u64::MAX).unwrap()).len(), 1);

        let (mut restored, restored_frames) = graph(&store)
            .restore_state_frames(&[], &committed, 256)
            .unwrap();
        assert_eq!(restored_frames, 256);
        let image = store
            .read()
            .recovery_image(restored.take_restored_views(), None, &[])
            .unwrap();
        assert_eq!(rows(&image), vec![(1, 1), (2, 1), (3, 1)]);
        assert_eq!(rows(&store.read()), vec![(1, 1), (2, 5), (3, 1)]);
    }
}

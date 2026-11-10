//! A view that a topology migration adds over a changelog the migration preserves starts from
//! that changelog's rows at the migration's cut.

use arrow::array::RecordBatch;
use laminar_core::cluster::control::topology::{
    ClusterTopologyObjectTransition, TopologyInitialization,
};
use laminar_core::cluster::control::{CatalogObjectKind, TopologyRestoreInput};

use super::input_admission::retained_input_bytes;
use super::{DbError, OperatorGraph};

impl OperatorGraph {
    /// Seed every view the migration adds over a changelog it preserves, when restoring the
    /// parent's checkpoint. A target checkpoint's state already holds the seed.
    pub(super) fn seed_views_at_cut(
        &mut self,
        recovered: &crate::recovery_manager::RecoveredState,
        input: &TopologyRestoreInput,
    ) -> Result<(), DbError> {
        let descriptor = input.descriptor();
        if recovered.committed.pipeline_identity != descriptor.parent_pipeline {
            return Ok(());
        }
        for view in descriptor
            .objects
            .iter()
            .filter(|object| object.initialization == TopologyInitialization::SeedFromUpstreamAtCut)
        {
            let upstreams: Vec<&str> = view
                .dependencies
                .iter()
                .filter(|dependency| {
                    descriptor.objects.iter().any(|object| {
                        &object.name == *dependency
                            && object.transition == ClusterTopologyObjectTransition::Preserve
                            && object.kind != CatalogObjectKind::Source
                    })
                })
                .map(String::as_str)
                .collect();
            self.seed_view_at_cut(&view.name, &upstreams)?;
        }
        Ok(())
    }

    /// Queue, on each input port of `view`'s operators that reads one of `upstreams`, the rows
    /// that upstream holds at the restored cut on this node, ahead of any later input.
    fn seed_view_at_cut(&mut self, view: &str, upstreams: &[&str]) -> Result<(), DbError> {
        let mut ports = Vec::new();
        for (node, sources) in self.input_sources.iter().enumerate() {
            let name = &self.nodes[node].name;
            if self.nodes[node].removed
                || (name.as_ref() != view && crate::sql_analysis::stage_owner(name) != Some(view))
            {
                continue;
            }
            for (port, &source) in sources.iter().enumerate() {
                if self
                    .nodes
                    .get(source)
                    .is_some_and(|upstream| upstreams.contains(&upstream.name.as_ref()))
                {
                    ports.push((node, port, source));
                }
            }
        }
        if ports.is_empty() {
            return Err(DbError::Checkpoint(format!(
                "view '{view}' reads none of the changelogs it seeds from"
            )));
        }
        for (node, port, source) in ports {
            let port = u8::try_from(port).map_err(|_| {
                DbError::Checkpoint(format!("view '{view}' has an input port above 255"))
            })?;
            let batches = self.changelog_at_cut(source)?;
            let bytes = retained_input_bytes(&batches);
            self.require_port_capacity(node, port, batches.len(), bytes)?;
            self.push_to_port(node, port, batches, bytes);
        }
        Ok(())
    }

    /// The rows stream `upstream` holds at the restored cut: an aggregate's emitted rows, or
    /// else the slices of the view it materializes.
    fn changelog_at_cut(&self, upstream: usize) -> Result<Vec<RecordBatch>, DbError> {
        let name = &self.nodes[upstream].name;
        let result = laminar_core::mv::view_result_operator(name);
        self.nodes[upstream]
            .operator
            .changelog_at_cut()
            .or_else(|| {
                self.find_node(&result)
                    .and_then(|node| self.nodes[node].operator.changelog_at_cut())
            })
            .unwrap_or_else(|| {
                Err(DbError::Checkpoint(format!(
                    "'{name}' keeps no rows at the cut for a new view to seed from"
                )))
            })
    }
}

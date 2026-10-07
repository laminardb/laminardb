//! Process function registration in the existing operator graph.

use super::{Arc, DbError, OperatorGraph};

impl OperatorGraph {
    pub(crate) fn add_process_function(
        &mut self,
        registration: &crate::process_function::ProcessFunctionRegistration,
    ) -> Result<(), DbError> {
        let operator = match &registration.handler {
            crate::process_function::ProcessHandler::Native(handler) => {
                crate::process_function::ProcessFunctionOperator::new(
                    registration.descriptor.clone(),
                    Arc::clone(handler),
                    u32::from(self.key_group_count),
                )?
            }
            #[cfg(feature = "process-remote")]
            crate::process_function::ProcessHandler::Remote(client) => {
                let runtime = self.main_runtime_handle.clone().ok_or_else(|| {
                    DbError::Config("process worker requires a main runtime handle".into())
                })?;
                let wake = Arc::clone(
                    self.process_work_wake
                        .get_or_insert_with(|| Arc::new(tokio::sync::Notify::new())),
                );
                crate::process_function::ProcessFunctionOperator::new_remote(
                    registration.descriptor.clone(),
                    client,
                    runtime,
                    wake,
                    registration.output_name.clone(),
                    u32::from(self.key_group_count),
                )?
            }
        };
        #[cfg(feature = "cluster")]
        let operator = {
            let mut operator = operator;
            if let Some(scope) = &self.cluster_shuffle {
                let runtime = self.main_runtime_handle.clone().ok_or_else(|| {
                    DbError::Config("process shuffle requires a main runtime handle".into())
                })?;
                operator.require_cluster_execution(
                    &registration.output_name,
                    runtime,
                    scope.self_id,
                )?;
            }
            operator
        };
        let source = self.ensure_source_node(&registration.source_name);
        let node =
            self.place_prepared_operator_node(&registration.output_name, Box::new(operator), 1);
        self.add_edge(source, node, 0);
        self.output_map
            .insert(Arc::from(registration.output_name.as_str()), node);
        self.register_intermediate_schema(
            &registration.output_name,
            &registration.descriptor.output_schema,
        );
        self.topo_dirty = true;
        Ok(())
    }

    /// Wake for completed remote process calls retained by this graph.
    #[cfg(feature = "process-remote")]
    pub(crate) fn process_work_wake(&self) -> Option<Arc<tokio::sync::Notify>> {
        self.process_work_wake.clone()
    }
}

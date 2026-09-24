use std::sync::Arc;

use datafusion::datasource::empty::EmptyTable;
use laminar_connectors::connector::DeliveryGuarantee;
use laminar_core::catalog::CatalogObjectKind;

use super::{
    NativeProcessFunction, ProcessFunctionDescriptor, ProcessFunctionInfo,
    ProcessFunctionRegistration, ProcessRuntime,
};
use crate::db::{exact_table_reference, DbState, LaminarDB};
use crate::error::DbError;

impl LaminarDB {
    /// Inspect native process functions registered on this database instance.
    #[must_use]
    pub fn native_process_functions(&self) -> Vec<ProcessFunctionInfo> {
        let manager = self.connector_manager.lock();
        let mut functions = manager
            .process_functions()
            .values()
            .map(|registration| ProcessFunctionInfo {
                output_name: registration.output_name.clone(),
                source_name: registration.source_name.clone(),
                descriptor: registration.descriptor.clone(),
            })
            .collect::<Vec<_>>();
        functions.sort_unstable_by(|left, right| left.output_name.cmp(&right.output_name));
        functions
    }

    /// Register a trusted native keyed process function over one direct in-memory source.
    /// Registration is offline: call it after creating the source and before `start()`. The
    /// caller must register the same immutable implementation when constructing a replacement
    /// database instance that restores an existing checkpoint.
    ///
    /// The current admission profile is local best-effort execution. Checkpointed state can be
    /// restored, but an in-memory source cannot replay uncommitted input after a crash.
    /// Native code runs in the compute process and must be trusted and nonblocking.
    ///
    /// # Errors
    /// Rejects incompatible schemas, identities, source modes, runtime modes, or resource limits.
    pub async fn register_native_process_function(
        &self,
        output_name: &str,
        source_name: &str,
        descriptor: ProcessFunctionDescriptor,
        handler: Arc<dyn NativeProcessFunction>,
    ) -> Result<(), DbError> {
        let _topology = self.topology_ddl_lock.write().await;
        self.ensure_topology_ddl_allowed("REGISTER NATIVE PROCESS FUNCTION")?;
        if self.is_cluster_runtime()
            || DbState::load(&self.state) != DbState::Created
            || self.config.delivery_guarantee != DeliveryGuarantee::BestEffort
        {
            return Err(DbError::Unsupported(
                "native process functions currently require an offline local best-effort pipeline"
                    .into(),
            ));
        }
        if descriptor.runtime != ProcessRuntime::NativeRust {
            return Err(DbError::Unsupported(
                "native registration requires the trusted native Rust runtime".into(),
            ));
        }
        if !valid_name(output_name) || !valid_name(source_name) || output_name == source_name {
            return Err(DbError::InvalidOperation(
                "process source and output require distinct lowercase SQL identifiers".into(),
            ));
        }
        let source = self.catalog.get_source(source_name).ok_or_else(|| {
            DbError::InvalidOperation(format!("process source '{source_name}' does not exist"))
        })?;
        if source.schema.as_ref() != descriptor.input_schema.as_ref() {
            return Err(DbError::InvalidOperation(
                "process descriptor input schema differs from its source".into(),
            ));
        }
        if source.watermark_column.as_deref() != Some(descriptor.event_time_column.as_str())
            || source
                .is_processing_time
                .load(std::sync::atomic::Ordering::Acquire)
            || !source.primary_key.is_empty()
        {
            return Err(DbError::Unsupported(
                "process source requires an append-only event-time watermark on the declared column"
                    .into(),
            ));
        }
        if self
            .connector_manager
            .lock()
            .sources()
            .get(source_name)
            .is_some_and(|reg| reg.connector_type.is_some())
        {
            return Err(DbError::Unsupported(
                "native process functions currently require a direct in-memory source".into(),
            ));
        }
        descriptor.to_manifest_json()?;
        let reservation = self
            .reserve_catalog_name(output_name, CatalogObjectKind::Stream, false)?
            .ok_or_else(|| DbError::InvalidOperation("process output already exists".into()))?;
        self.catalog.register_stream(output_name)?;
        self.connector_manager
            .lock()
            .register_process_function(ProcessFunctionRegistration {
                output_name: output_name.to_string(),
                source_name: source_name.to_string(),
                descriptor: descriptor.clone(),
                handler,
            });
        self.ctx
            .register_table(
                exact_table_reference(output_name),
                Arc::new(EmptyTable::new(Arc::clone(&descriptor.output_schema))),
            )
            .map_err(|error| {
                DbError::Pipeline(format!(
                    "could not register process output '{output_name}' for SQL planning: {error}"
                ))
            })?;
        self.stream_schemas
            .write()
            .insert(output_name.to_string(), descriptor.output_schema);
        reservation.commit();
        Ok(())
    }
}

fn valid_name(name: &str) -> bool {
    if name.len() > 128 {
        return false;
    }
    let mut bytes = name.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

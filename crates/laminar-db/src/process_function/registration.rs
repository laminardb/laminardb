use std::sync::Arc;

use datafusion::datasource::empty::EmptyTable;
use laminar_connectors::connector::DeliveryGuarantee;
use laminar_core::catalog::CatalogObjectKind;

use super::{
    NativeProcessFunction, ProcessFunctionDescriptor, ProcessFunctionInfo,
    ProcessFunctionRegistration, ProcessHandler, ProcessRuntime,
};
use crate::db::{exact_table_reference, DbState, LaminarDB};
use crate::error::DbError;

impl LaminarDB {
    /// Inspect process functions registered on this database instance.
    #[must_use]
    pub fn process_functions(&self) -> Vec<ProcessFunctionInfo> {
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

    /// Register a trusted native keyed process function over one append-only source.
    /// Registration is offline: call it after creating a local source and before `start()`. The
    /// caller must register the same immutable implementation when constructing a replacement
    /// database instance that restores an existing checkpoint.
    ///
    /// Local at-least-once execution requires checkpointing and a replayable append-only
    /// connector that reproduces one channel in fixed replay batches with deterministic positions.
    /// Only one logical source is admitted. Source and sink contracts are verified before startup I/O. A direct
    /// in-memory source is available only with best-effort delivery.
    /// Cluster execution admits at-least-once delivery with a splittable fixed-batch source.
    /// Register the binding on every owner before catalog bootstrap, then include
    /// `process_function_bootstrap_sql()` after the source in the ordered startup batch.
    /// Process bindings cannot be changed through live catalog mutations.
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
        if descriptor.runtime != ProcessRuntime::NativeRust {
            return Err(DbError::Unsupported(
                "native registration requires the trusted native Rust runtime".into(),
            ));
        }
        self.register_process_function(
            output_name,
            source_name,
            descriptor,
            ProcessHandler::Native(handler),
        )
        .await
    }

    /// Register a connected loopback Rust or Python worker. At-least-once Python requires a
    /// replay-safe descriptor and the live binding supplied by `LocalPythonWorker` on Linux.
    /// Its complete package must belong to the read-only root image; a standalone connected
    /// client does not supply that binding. Undeclared Python supports best-effort delivery.
    /// Source-order and cluster-bootstrap requirements match native registration.
    /// The caller owns the worker process lifecycle and must keep it available until shutdown.
    ///
    /// # Errors
    /// Rejects mismatched descriptors, unsupported modes, schemas, or resource limits.
    #[cfg(feature = "process-remote")]
    pub async fn register_remote_process_function(
        &self,
        output_name: &str,
        source_name: &str,
        descriptor: ProcessFunctionDescriptor,
        client: Arc<super::remote::RemoteProcessClient>,
    ) -> Result<(), DbError> {
        if descriptor.runtime == ProcessRuntime::NativeRust {
            return Err(DbError::InvalidOperation(
                "remote registration requires a remote runtime".into(),
            ));
        }
        if descriptor.to_manifest_json()? != client.descriptor().to_manifest_json()? {
            return Err(DbError::InvalidOperation(
                "connected process worker descriptor differs from registration".into(),
            ));
        }
        self.register_process_function(
            output_name,
            source_name,
            descriptor,
            ProcessHandler::Remote(client),
        )
        .await
    }

    async fn register_process_function(
        &self,
        output_name: &str,
        source_name: &str,
        descriptor: ProcessFunctionDescriptor,
        handler: ProcessHandler,
    ) -> Result<(), DbError> {
        let _topology = self.topology_ddl_lock.write().await;
        self.ensure_topology_ddl_allowed("REGISTER PROCESS FUNCTION")?;
        if DbState::load(&self.state) != DbState::Created {
            return Err(DbError::Unsupported(
                "process functions require registration before pipeline startup".into(),
            ));
        }
        if self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            return Err(DbError::Unsupported(
                "process functions do not support exactly-once delivery".into(),
            ));
        }
        if self.config.delivery_guarantee == DeliveryGuarantee::AtLeastOnce
            && !handler.supports_replay(&descriptor)
        {
            return Err(DbError::Unsupported(
                "at-least-once Python process functions require immutable dependency binding from a supervised replay-safe worker"
                    .into(),
            ));
        }
        if !valid_name(output_name) || !valid_name(source_name) || output_name == source_name {
            return Err(DbError::InvalidOperation(
                "process source and output require distinct lowercase SQL identifiers".into(),
            ));
        }
        descriptor.to_manifest_json()?;
        let registration = ProcessFunctionRegistration {
            output_name: output_name.into(),
            source_name: source_name.into(),
            descriptor,
            handler,
        };
        if self.is_cluster_runtime() {
            if self.config.delivery_guarantee != DeliveryGuarantee::AtLeastOnce {
                return Err(DbError::Unsupported(
                    "cluster process functions require at-least-once delivery".into(),
                ));
            }
            let mut manager = self.connector_manager.lock();
            if manager.process_functions().contains_key(output_name) {
                return Err(DbError::InvalidOperation(
                    "process binding already exists".into(),
                ));
            }
            manager.register_process_function(registration);
            return Ok(());
        }
        self.install_process_function(&registration)
    }

    pub(crate) fn install_process_function(
        &self,
        registration: &ProcessFunctionRegistration,
    ) -> Result<(), DbError> {
        let ProcessFunctionRegistration {
            output_name,
            source_name,
            descriptor,
            ..
        } = registration;
        let source = self.catalog.get_source(source_name).ok_or_else(|| {
            DbError::InvalidOperation(format!("process source '{source_name}' does not exist"))
        })?;
        self.validate_process_source_order(
            output_name,
            source_name,
            self.connector_manager.lock().sources(),
        )?;
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
        let reservation = self
            .reserve_catalog_name(output_name, CatalogObjectKind::Stream, false)?
            .ok_or_else(|| DbError::InvalidOperation("process output already exists".into()))?;
        self.catalog.register_stream(output_name)?;
        self.connector_manager
            .lock()
            .register_process_function(registration.clone());
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
            .insert(output_name.clone(), Arc::clone(&descriptor.output_schema));
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

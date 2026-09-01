//! Apache Iceberg append sink connector.

#[cfg(feature = "iceberg-core")]
mod aborted_cleanup;
#[cfg(feature = "iceberg-core")]
mod artifact_inventory;
pub(crate) mod capabilities;
#[cfg(feature = "iceberg-core")]
mod commit_cursor;
#[cfg(feature = "iceberg-core")]
mod coordinated_commit;
#[cfg(feature = "iceberg-core")]
mod descriptor;
#[cfg(feature = "iceberg-core")]
mod descriptor_batch;
#[cfg(feature = "iceberg-core")]
mod direct_publication;
#[cfg(feature = "iceberg-core")]
mod epoch_intent;
#[cfg(feature = "iceberg-core")]
mod epoch_writer;
#[cfg(all(test, feature = "iceberg-core"))]
mod fault_injection;
#[cfg(feature = "iceberg-core")]
mod file_finalizer;
#[cfg(feature = "iceberg-core")]
mod fingerprint;
#[cfg(feature = "iceberg-core")]
mod metrics;
#[cfg(feature = "iceberg-core")]
mod publication;
#[cfg(all(test, feature = "iceberg-core"))]
mod recovery_tests;
#[cfg(feature = "iceberg-core")]
mod schema_alignment;
#[cfg(all(test, feature = "iceberg-core"))]
pub(crate) mod test_support;

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
#[cfg(feature = "iceberg-core")]
use parking_lot::Mutex;

use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    DeliveryGuarantee, SinkConnector, SinkConsistency, SinkContract, SinkInputMode,
    SinkRuntimeContext, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;
use crate::storage::StorageProvider;

use super::iceberg_config::{IcebergSinkConfig, IcebergStorageType};
#[cfg(feature = "iceberg-core")]
use artifact_inventory::{PreparedEpochArtifacts, PreparedEpochCleanup};
#[cfg(feature = "iceberg-core")]
use descriptor::IcebergCommitDescriptorV1;
#[cfg(feature = "iceberg-core")]
use epoch_intent::IcebergEpochIntentV1;
#[cfg(feature = "iceberg-core")]
use epoch_writer::{EpochIdentity, IcebergEpochWriter};
#[cfg(feature = "iceberg-core")]
use metrics::IcebergMetrics;
#[cfg(feature = "iceberg-core")]
use publication::UnresolvedIcebergPublication;
#[cfg(feature = "iceberg-core")]
pub(in crate::lakehouse) use schema_alignment::SchemaAlignmentPlan;

/// Apache Iceberg append sink.
pub struct IcebergSink {
    config: IcebergSinkConfig,
    schema: Option<SchemaRef>,
    state: ConnectorState,
    runtime_context: Option<SinkRuntimeContext>,
    #[cfg(feature = "iceberg-core")]
    standalone_deployment_id: String,
    #[cfg(feature = "iceberg-core")]
    direct_epoch: u64,
    #[cfg(feature = "iceberg-core")]
    catalog: Option<Arc<dyn iceberg::Catalog>>,
    #[cfg(feature = "iceberg-core")]
    catalog_capabilities: super::iceberg_io::CatalogCapabilities,
    #[cfg(feature = "iceberg-core")]
    catalog_session: super::iceberg_io::CatalogSession,
    #[cfg(feature = "iceberg-core")]
    table: Option<iceberg::table::Table>,
    #[cfg(feature = "iceberg-core")]
    iceberg_arrow_schema: Option<SchemaRef>,
    #[cfg(feature = "iceberg-core")]
    alignment_plan: Option<SchemaAlignmentPlan>,
    #[cfg(feature = "iceberg-core")]
    active_epoch: Mutex<Option<IcebergEpochWriter>>,
    #[cfg(feature = "iceberg-core")]
    active_epoch_id: Option<u64>,
    #[cfg(feature = "iceberg-core")]
    admitted_epoch_intent: Option<IcebergEpochIntentV1>,
    #[cfg(feature = "iceberg-core")]
    prepared_epoch: Option<PreparedEpochArtifacts>,
    #[cfg(feature = "iceberg-core")]
    metrics: IcebergMetrics,
    #[cfg(feature = "iceberg-core")]
    unresolved_publication: Arc<Mutex<Option<UnresolvedIcebergPublication>>>,
}

impl IcebergSink {
    /// Creates an Iceberg sink from validated connector configuration.
    #[must_use]
    pub fn new(config: IcebergSinkConfig, registry: Option<&prometheus::Registry>) -> Self {
        #[cfg(not(feature = "iceberg-core"))]
        let _ = registry;
        Self {
            config,
            schema: None,
            state: ConnectorState::Created,
            runtime_context: None,
            #[cfg(feature = "iceberg-core")]
            standalone_deployment_id: uuid::Uuid::now_v7().to_string(),
            #[cfg(feature = "iceberg-core")]
            direct_epoch: 0,
            #[cfg(feature = "iceberg-core")]
            catalog: None,
            #[cfg(feature = "iceberg-core")]
            catalog_capabilities: super::iceberg_io::CatalogCapabilities::default(),
            #[cfg(feature = "iceberg-core")]
            catalog_session: super::iceberg_io::CatalogSession::default(),
            #[cfg(feature = "iceberg-core")]
            table: None,
            #[cfg(feature = "iceberg-core")]
            iceberg_arrow_schema: None,
            #[cfg(feature = "iceberg-core")]
            alignment_plan: None,
            #[cfg(feature = "iceberg-core")]
            active_epoch: Mutex::new(None),
            #[cfg(feature = "iceberg-core")]
            active_epoch_id: None,
            #[cfg(feature = "iceberg-core")]
            admitted_epoch_intent: None,
            #[cfg(feature = "iceberg-core")]
            prepared_epoch: None,
            #[cfg(feature = "iceberg-core")]
            metrics: IcebergMetrics::new(registry),
            #[cfg(feature = "iceberg-core")]
            unresolved_publication: Arc::new(Mutex::new(None)),
        }
    }

    fn is_coordinated(&self) -> bool {
        self.config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
    }

    #[cfg(feature = "iceberg-core")]
    fn table(&self) -> Result<&iceberg::table::Table, ConnectorError> {
        self.table
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open Iceberg sink".into(),
                actual: "table is not loaded".into(),
            })
    }

    #[cfg(feature = "iceberg-core")]
    fn epoch_identity(&self, epoch: u64) -> Result<EpochIdentity, ConnectorError> {
        if self.is_coordinated() {
            let context =
                self.runtime_context
                    .as_ref()
                    .ok_or_else(|| ConnectorError::InvalidState {
                        expected: "checkpoint runtime identity bound before open".into(),
                        actual: "runtime identity is absent".into(),
                    })?;
            return Ok(EpochIdentity {
                deployment_id: context.deployment_id.clone(),
                sink_id: context.sink_id.clone(),
                participant_id: context.participant_id,
                epoch,
            });
        }
        Ok(EpochIdentity {
            deployment_id: self.standalone_deployment_id.clone(),
            sink_id: format!(
                "{}.{}",
                self.config.catalog.namespace, self.config.catalog.table_name
            ),
            participant_id: 1,
            epoch,
        })
    }

    #[cfg(feature = "iceberg-core")]
    fn new_epoch_writer(&self, epoch: u64) -> Result<IcebergEpochWriter, ConnectorError> {
        IcebergEpochWriter::new(
            self.table()?,
            &self.config,
            &self.epoch_identity(epoch)?,
            self.metrics.clone(),
        )
    }

    #[cfg(feature = "iceberg-core")]
    async fn load_current_table(&self) -> Result<iceberg::table::Table, ConnectorError> {
        let catalog = self
            .catalog
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open Iceberg sink".into(),
                actual: "catalog is not initialized".into(),
            })?;
        super::iceberg_io::load_table_with_timeout(
            catalog.as_ref(),
            &self.config.catalog.namespace,
            &self.config.catalog.table_name,
            self.config.catalog.request_timeout,
        )
        .await
    }

    #[cfg(feature = "iceberg-core")]
    async fn initialize_active_writer(&mut self) -> Result<(), ConnectorError> {
        if self.active_epoch.get_mut().is_some() {
            return Ok(());
        }
        let epoch = self
            .active_epoch_id
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "an active Iceberg epoch".into(),
                actual: "no epoch identity is active".into(),
            })?;
        let current = self.load_current_table().await?;
        validate_epoch_table_refresh(self.table()?, &current)?;
        if self.is_coordinated() {
            self.admitted_epoch_intent
                .as_ref()
                .ok_or_else(|| {
                    ConnectorError::Internal("Iceberg active epoch lost its artifact intent".into())
                })?
                .validate_writer(&current, &self.config, &self.epoch_identity(epoch)?)?;
        }
        self.table = Some(current);
        let writer = self.new_epoch_writer(epoch)?;
        *self.active_epoch.get_mut() = Some(writer);
        Ok(())
    }

    #[cfg(feature = "iceberg-core")]
    fn align_batch_to_iceberg_schema(
        &self,
        batch: &RecordBatch,
    ) -> Result<RecordBatch, ConnectorError> {
        self.alignment_plan
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open Iceberg sink with a schema alignment plan".into(),
                actual: "Iceberg schema alignment is not initialized".into(),
            })?
            .align(batch)
    }

    #[cfg(feature = "iceberg-core")]
    async fn publish_direct_epoch(&mut self) -> Result<(), ConnectorError> {
        let writer = self.active_epoch.get_mut().take();
        self.active_epoch_id = None;
        let Some(writer) = writer else {
            return Ok(());
        };
        let writer_table = self.table()?.clone();
        let mut output = match writer.close().await {
            Ok(output) => output,
            Err(error) => {
                self.state = ConnectorState::Failed;
                return Err(error);
            }
        };
        if output.data_files.is_empty() {
            return Ok(());
        }
        let artifacts = std::mem::take(&mut output.artifacts);
        let catalog = self
            .catalog
            .clone()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open Iceberg sink".into(),
                actual: "catalog is not initialized".into(),
            })?;
        match direct_publication::publish_direct_append(
            &self.config,
            &catalog,
            &self.catalog_capabilities,
            &self.catalog_session,
            &writer_table,
            output.data_files,
            &self.metrics,
        )
        .await
        {
            Ok(updated) => {
                self.table = Some(updated);
                Ok(())
            }
            Err(error) => {
                self.state = ConnectorState::Failed;
                if error.is_outcome_unknown() {
                    return Err(error);
                }
                let cleanup = artifacts
                    .cleanup_aborted(writer_table.file_io().clone(), self.metrics.clone())
                    .await;
                Err(match cleanup {
                    Ok(()) => error,
                    Err(cleanup) => ConnectorError::WriteError(format!(
                        "{error}; exact artifact cleanup also failed: {cleanup}"
                    )),
                })
            }
        }
    }
}

#[async_trait]
impl SinkConnector for IcebergSink {
    fn bind_runtime_context(&mut self, context: SinkRuntimeContext) -> Result<(), ConnectorError> {
        if self.state != ConnectorState::Created {
            return Err(ConnectorError::InvalidState {
                expected: "created Iceberg sink before open".into(),
                actual: self.state.to_string(),
            });
        }
        let deployment = uuid::Uuid::parse_str(&context.deployment_id).map_err(|_| {
            ConnectorError::ConfigurationError(
                "Iceberg runtime deployment ID must be a canonical non-nil UUID".into(),
            )
        })?;
        if self.is_coordinated() && deployment.get_version_num() != 7 {
            return Err(ConnectorError::ConfigurationError(
                "[LDB-ICEBERG-RUNTIME-DEPLOYMENT-VERSION] exactly-once Iceberg requires a UUIDv7 deployment identity"
                    .into(),
            ));
        }
        if deployment.is_nil()
            || deployment.to_string() != context.deployment_id
            || context.sink_id.is_empty()
            || context.participant_id == 0
        {
            return Err(ConnectorError::ConfigurationError(
                "Iceberg runtime identity contains an invalid deployment, sink, or participant"
                    .into(),
            ));
        }
        if let Some(bound) = &self.runtime_context {
            if bound == &context {
                return Ok(());
            }
            return Err(ConnectorError::InvalidState {
                expected: "the runtime identity already bound to this Iceberg sink".into(),
                actual: "a different runtime identity".into(),
            });
        }
        self.runtime_context = Some(context);
        Ok(())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        let config = IcebergSinkConfig::from_config(config)?;
        capabilities::validate_sink(&config)?;
        let shared_warehouse = StorageProvider::is_direct_s3_uri(&config.catalog.warehouse)
            || config.storage.storage_type == Some(IcebergStorageType::S3);
        let consistency = if config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce {
            SinkConsistency::CheckpointCommittable
        } else {
            SinkConsistency::DurableAtLeastOnce
        };
        let contract = SinkContract::new(
            consistency,
            if shared_warehouse {
                SinkTopology::MultiWriter
            } else {
                SinkTopology::Singleton
            },
            SinkInputMode::AppendOnly,
        );
        Ok(
            if consistency == SinkConsistency::CheckpointCommittable
                && capabilities::cluster_exact_append_certified(&config)
            {
                contract.with_cluster_exact_delivery_certification()
            } else {
                contract
            },
        )
    }

    async fn open(&mut self, config: &ConnectorConfig) -> Result<(), ConnectorError> {
        if !config.properties().is_empty() {
            self.config = IcebergSinkConfig::from_config(config)?;
        }
        capabilities::validate_sink(&self.config)?;
        if self.is_coordinated() && self.runtime_context.is_none() {
            return Err(ConnectorError::ConfigurationError(
                "exactly-once Iceberg append requires checkpoint runtime identity".into(),
            ));
        }

        #[cfg(feature = "iceberg-core")]
        {
            let built = super::iceberg_io::build_catalog_for_access_with_metrics(
                &self.config.catalog,
                &self.config.storage,
                super::iceberg_io::CatalogAccess::Write {
                    auto_create: self.config.auto_create,
                },
                Some(self.metrics.credential_refresh_failures.clone()),
            )
            .await?;
            let catalog = built.catalog;
            let namespace = &self.config.catalog.namespace;
            let table_name = &self.config.catalog.table_name;
            if self.config.auto_create {
                if let Some(schema) = config.arrow_schema() {
                    tokio::time::timeout(
                        self.config.catalog.request_timeout,
                        super::iceberg_io::ensure_table_exists(
                            catalog.as_ref(),
                            &self.config,
                            &schema,
                        ),
                    )
                    .await
                    .map_err(|_| {
                        ConnectorError::WriteError(
                            "[LDB-ICEBERG-CATALOG-TIMEOUT] table creation exceeded catalog.request_timeout"
                                .into(),
                        )
                    })??;
                }
            }
            let table = super::iceberg_io::load_table_with_timeout(
                catalog.as_ref(),
                namespace,
                table_name,
                self.config.catalog.request_timeout,
            )
            .await?;
            schema_alignment::validate_identifier_fields(
                &self.config.identifier_fields,
                table.current_schema_ref().as_ref(),
            )?;
            let table_schema = Arc::new(
                iceberg::arrow::schema_to_arrow_schema(&table.current_schema_ref()).map_err(
                    |error| {
                        ConnectorError::SchemaMismatch(format!(
                            "convert Iceberg schema to Arrow: {error}"
                        ))
                    },
                )?,
            );
            let input_schema = config
                .arrow_schema()
                .unwrap_or_else(|| Arc::clone(&table_schema));
            self.alignment_plan = Some(SchemaAlignmentPlan::new(
                table.metadata().current_schema_id(),
                Arc::clone(&input_schema),
                Arc::clone(&table_schema),
            )?);
            self.schema = Some(input_schema);
            self.iceberg_arrow_schema = Some(table_schema);
            self.catalog_capabilities = built.capabilities;
            self.catalog_session = built.session;
            self.catalog = Some(catalog);
            self.table = Some(table);
            self.state = ConnectorState::Running;
            metrics::trace_sink_connected(&self.config, namespace, table_name);
            return Ok(());
        }

        #[cfg(not(feature = "iceberg-core"))]
        {
            self.state = ConnectorState::Failed;
            Err(ConnectorError::FeatureUnsupported(
                "Apache Iceberg requires the 'iceberg' feature".into(),
            ))
        }
    }

    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<WriteResult, ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "running Iceberg sink".into(),
                actual: self.state.to_string(),
            });
        }
        if batch.num_rows() == 0 {
            return Ok(WriteResult::new(0, 0));
        }
        #[cfg(feature = "iceberg-core")]
        {
            if self.schema.is_none() {
                self.schema = Some(batch.schema());
            }
            if self.active_epoch_id.is_none() {
                if self.is_coordinated() {
                    return Err(ConnectorError::InvalidState {
                        expected: "begin_epoch before an exactly-once Iceberg write".into(),
                        actual: "no active epoch".into(),
                    });
                }
                self.direct_epoch = self.direct_epoch.checked_add(1).ok_or_else(|| {
                    ConnectorError::WriteError("Iceberg direct epoch ID overflow".into())
                })?;
                self.active_epoch_id = Some(self.direct_epoch);
            }
            self.initialize_active_writer().await?;
            let aligned = self.align_batch_to_iceberg_schema(batch)?;
            let bytes = aligned.get_array_memory_size();
            self.active_epoch
                .get_mut()
                .as_mut()
                .ok_or_else(|| ConnectorError::Internal("Iceberg epoch writer disappeared".into()))?
                .write(aligned)
                .await?;
            return Ok(WriteResult::new(
                batch.num_rows(),
                u64::try_from(bytes).unwrap_or(u64::MAX),
            ));
        }

        #[cfg(not(feature = "iceberg-core"))]
        Err(ConnectorError::FeatureUnsupported(
            "Apache Iceberg requires the 'iceberg' feature".into(),
        ))
    }

    fn schema(&self) -> SchemaRef {
        self.schema
            .clone()
            .unwrap_or_else(|| Arc::new(arrow_schema::Schema::empty()))
    }

    async fn checkpoint_artifact_intent(
        &mut self,
        epoch: u64,
    ) -> Result<Option<Vec<u8>>, ConnectorError> {
        #[cfg(feature = "iceberg-core")]
        {
            if !self.is_coordinated() {
                return Ok(None);
            }
            self.ensure_no_unresolved_publication()?;
            if self.active_epoch_id.is_some() || self.active_epoch.get_mut().is_some() {
                return Err(ConnectorError::InvalidState {
                    expected: "no active Iceberg epoch before artifact admission".into(),
                    actual: "an Iceberg epoch is still active".into(),
                });
            }
            let identity = self.epoch_identity(epoch)?;
            if let Some(intent) = self.admitted_epoch_intent.as_ref() {
                intent.validate_writer(self.table()?, &self.config, &identity)?;
                return Ok(Some(intent.encode()?));
            }
            let intent = IcebergEpochIntentV1::capture(self.table()?, &self.config, &identity)?;
            let payload = intent.encode()?;
            self.admitted_epoch_intent = Some(intent);
            return Ok(Some(payload));
        }

        #[cfg(not(feature = "iceberg-core"))]
        {
            let _ = epoch;
            Ok(None)
        }
    }

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        #[cfg(not(feature = "iceberg-core"))]
        let _ = epoch;
        #[cfg(feature = "iceberg-core")]
        {
            if !self.is_coordinated() {
                return Ok(());
            }
            self.ensure_no_unresolved_publication()?;
            if self.active_epoch_id.is_some() || self.active_epoch.get_mut().is_some() {
                return Err(ConnectorError::InvalidState {
                    expected: "previous Iceberg epoch prepared or rolled back".into(),
                    actual: "an epoch identity or writer is still active".into(),
                });
            }
            let intent = self.admitted_epoch_intent.as_ref().ok_or_else(|| {
                ConnectorError::InvalidState {
                    expected: format!("durable Iceberg artifact intent for epoch {epoch}"),
                    actual: "artifact intent was not captured before begin_epoch".into(),
                }
            })?;
            intent.validate_writer(self.table()?, &self.config, &self.epoch_identity(epoch)?)?;
            self.cleanup_prepared_epoch(PreparedEpochCleanup::Successor { next_epoch: epoch })
                .await?;
            self.active_epoch_id = Some(epoch);
        }
        Ok(())
    }

    async fn pre_commit(&mut self, epoch: u64) -> Result<Option<Vec<u8>>, ConnectorError> {
        #[cfg(not(feature = "iceberg-core"))]
        let _ = epoch;
        #[cfg(feature = "iceberg-core")]
        {
            if !self.is_coordinated() {
                self.flush().await?;
                return Ok(None);
            }
            self.ensure_no_unresolved_publication()?;
            if self.active_epoch_id != Some(epoch) {
                return Err(ConnectorError::InvalidState {
                    expected: format!("active Iceberg epoch {epoch}"),
                    actual: format!("active epoch is {:?}", self.active_epoch_id),
                });
            }
            if self.prepared_epoch.is_some() {
                return Err(ConnectorError::InvalidState {
                    expected: "no previously prepared Iceberg epoch".into(),
                    actual: "prepared artifact ownership is still active".into(),
                });
            }
            let started = std::time::Instant::now();
            let writer = self.active_epoch.get_mut().take();
            self.active_epoch_id = None;
            let Some(writer) = writer else {
                self.admitted_epoch_intent = None;
                self.metrics
                    .pre_commit_duration
                    .observe(started.elapsed().as_secs_f64());
                return Ok(None);
            };
            self.prepared_epoch = Some(PreparedEpochArtifacts::new(
                epoch,
                writer.artifact_tracker(),
                &self.metrics,
            ));
            let output = writer.close().await?;
            self.admitted_epoch_intent = None;
            self.metrics
                .pre_commit_duration
                .observe(started.elapsed().as_secs_f64());
            if output.data_files.is_empty() {
                self.prepared_epoch = None;
                self.metrics.pending_artifact_paths.set(0);
                return Ok(None);
            }
            self.prepared_epoch
                .as_ref()
                .ok_or_else(|| {
                    ConnectorError::Internal(
                        "Iceberg prepared artifact ownership disappeared during writer close"
                            .into(),
                    )
                })?
                .seal(&output.artifacts, &self.metrics)?;
            #[cfg(test)]
            fault_injection::fail_if(fault_injection::IcebergFaultPoint::AfterFileClose)?;
            let descriptor = IcebergCommitDescriptorV1::encode(
                self.table()?,
                &self.config,
                &self.epoch_identity(epoch)?,
                output,
            )?;
            #[cfg(test)]
            fault_injection::fail_if(fault_injection::IcebergFaultPoint::AfterDescriptor)?;
            self.prepared_epoch
                .as_mut()
                .ok_or_else(|| {
                    ConnectorError::Internal(
                        "Iceberg prepared artifact ownership disappeared before descriptor issue"
                            .into(),
                    )
                })?
                .mark_descriptor_issued();
            return Ok(Some(descriptor));
        }

        #[cfg(not(feature = "iceberg-core"))]
        Err(ConnectorError::FeatureUnsupported(
            "Apache Iceberg requires the 'iceberg' feature".into(),
        ))
    }

    async fn rollback_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        #[cfg(not(feature = "iceberg-core"))]
        let _ = epoch;
        #[cfg(feature = "iceberg-core")]
        {
            if self.active_epoch_id.is_some_and(|active| active != epoch) {
                return Err(ConnectorError::InvalidState {
                    expected: format!("rollback active Iceberg epoch {epoch}"),
                    actual: format!("active epoch is {:?}", self.active_epoch_id),
                });
            }
            let active_cleanup = self.discard_active_epoch().await;
            let prepared_cleanup = self
                .cleanup_prepared_epoch(PreparedEpochCleanup::Abort { epoch })
                .await;
            match (active_cleanup, prepared_cleanup) {
                (Ok(()), Ok(())) => {}
                (Err(error), Ok(())) | (Ok(()), Err(error)) => return Err(error),
                (Err(active), Err(prepared)) => {
                    return Err(ConnectorError::WriteError(format!(
                        "{active}; prepared artifact cleanup also failed: {prepared}"
                    )));
                }
            }
            self.admitted_epoch_intent = None;
        }
        Ok(())
    }

    fn suggested_write_timeout(&self) -> Duration {
        self.config.catalog.commit_timeout
    }

    fn flush_interval(&self) -> Duration {
        self.config.max_flush_age.min(Duration::from_secs(5))
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::InvalidState {
                expected: "running Iceberg sink".into(),
                actual: self.state.to_string(),
            });
        }
        #[cfg(feature = "iceberg-core")]
        {
            if self.is_coordinated() {
                return Ok(());
            }
            return self.publish_direct_epoch().await;
        }

        #[cfg(not(feature = "iceberg-core"))]
        Err(ConnectorError::FeatureUnsupported(
            "Apache Iceberg requires the 'iceberg' feature".into(),
        ))
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        #[cfg(feature = "iceberg-core")]
        {
            if self.is_coordinated() {
                self.discard_active_epoch().await?;
                self.cleanup_prepared_epoch(PreparedEpochCleanup::Close)
                    .await?;
                self.admitted_epoch_intent = None;
            } else {
                self.flush().await?;
            }
            self.catalog = None;
            self.catalog_capabilities = super::iceberg_io::CatalogCapabilities::default();
            self.catalog_session = super::iceberg_io::CatalogSession::default();
            self.table = None;
            self.iceberg_arrow_schema = None;
            self.alignment_plan = None;
            self.metrics.set_buffer(0, 0);
            self.metrics.set_active_writers(0);
            self.metrics.pending_artifact_paths.set(0);
        }
        self.state = ConnectorState::Closed;
        Ok(())
    }

    #[cfg(feature = "iceberg-core")]
    fn as_coordinated_committer(&self) -> Option<&dyn crate::connector::CoordinatedCommitter> {
        self.is_coordinated()
            .then_some(self as &dyn crate::connector::CoordinatedCommitter)
    }

    fn coordinated_abort_cleaner(
        &self,
    ) -> Option<Arc<dyn crate::connector::CoordinatedAbortCleaner>> {
        #[cfg(feature = "iceberg-core")]
        {
            if !self.is_coordinated() {
                return None;
            }
            let catalog = self.catalog.as_ref()?.clone();
            Some(Arc::new(aborted_cleanup::IcebergAbortCleaner::new(
                catalog,
                self.config.clone(),
                self.metrics.clone(),
                Arc::clone(&self.unresolved_publication),
            )))
        }
        #[cfg(not(feature = "iceberg-core"))]
        None
    }
}

#[cfg(feature = "iceberg-core")]
fn validate_epoch_table_refresh(
    expected: &iceberg::table::Table,
    current: &iceberg::table::Table,
) -> Result<(), ConnectorError> {
    if expected.metadata().uuid() != current.metadata().uuid()
        || expected.identifier() != current.identifier()
        || expected.metadata().location() != current.metadata().location()
    {
        return Err(ConnectorError::TransactionError(
            "[LDB-ICEBERG-TABLE-REPLACED] Iceberg table identity or location changed while the sink was running"
                .into(),
        ));
    }
    if expected.metadata().current_schema_id() != current.metadata().current_schema_id() {
        return Err(ConnectorError::SchemaMismatch(
            "[LDB-ICEBERG-SCHEMA-CHANGED] Iceberg schema changed while schema.evolution.mode=strict"
                .into(),
        ));
    }
    Ok(())
}

#[cfg(feature = "iceberg-core")]
fn validate_direct_publication_table(
    writer_table: &iceberg::table::Table,
    current: &iceberg::table::Table,
) -> Result<(), ConnectorError> {
    validate_epoch_table_refresh(writer_table, current)?;
    let writer = writer_table.metadata();
    let current = current.metadata();
    if writer.default_partition_spec_id() != current.default_partition_spec_id()
        || writer.default_sort_order_id() != current.default_sort_order_id()
        || writer.format_version() != current.format_version()
    {
        return Err(ConnectorError::TransactionError(
            "[LDB-ICEBERG-DIRECT-LAYOUT-CHANGED] Iceberg partition spec, sort order, or format version changed while a direct epoch was open"
                .into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

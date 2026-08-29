//! Apache Iceberg append sink connector.

pub(crate) mod capabilities;
#[cfg(feature = "iceberg-core")]
mod commit_cursor;
#[cfg(feature = "iceberg-core")]
mod descriptor;
#[cfg(feature = "iceberg-core")]
mod epoch_writer;
#[cfg(all(test, feature = "iceberg-core"))]
mod fault_injection;
#[cfg(feature = "iceberg-core")]
mod file_finalizer;
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
#[cfg(feature = "iceberg-core")]
use tracing::info;

use crate::config::{ConnectorConfig, ConnectorState};
use crate::connector::{
    DeliveryGuarantee, SinkConnector, SinkConsistency, SinkContract, SinkInputMode,
    SinkRuntimeContext, SinkTopology, WriteResult,
};
use crate::error::ConnectorError;

use super::iceberg_config::{IcebergSinkConfig, IcebergStorageType};
#[cfg(feature = "iceberg-core")]
use descriptor::IcebergCommitDescriptorV1;
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
        let Some(writer) = self.active_epoch.get_mut().take() else {
            return Ok(());
        };
        self.active_epoch_id = None;
        let output = writer.close().await?;
        if output.data_files.is_empty() {
            return Ok(());
        }
        let catalog = self
            .catalog
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open Iceberg sink".into(),
                actual: "catalog is not initialized".into(),
            })?;
        let updated = super::iceberg_io::commit_data_files_append(
            self.table()?,
            catalog.as_ref(),
            output.data_files,
        )
        .await?;
        self.table = Some(updated);
        Ok(())
    }

    #[cfg(feature = "iceberg-core")]
    fn ensure_no_unresolved_publication(&self) -> Result<(), ConnectorError> {
        if self.unresolved_publication.lock().is_some() {
            Err(ConnectorError::InvalidState {
                expected: "reconciliation of the exact ambiguous Iceberg publication".into(),
                actual: "a prior coordinated publication remains unresolved".into(),
            })
        } else {
            Ok(())
        }
    }

    #[cfg(feature = "iceberg-core")]
    async fn discard_active_epoch(&mut self) -> Result<(), ConnectorError> {
        let writer = self.active_epoch.get_mut().take();
        self.active_epoch_id = None;
        self.metrics.set_buffer(0, 0);
        self.metrics.set_active_writers(0);
        if let Some(writer) = writer {
            let _ = writer.close().await?;
        }
        Ok(())
    }
}

#[async_trait]
impl SinkConnector for IcebergSink {
    fn bind_runtime_context(&mut self, context: SinkRuntimeContext) -> Result<(), ConnectorError> {
        let deployment = uuid::Uuid::parse_str(&context.deployment_id).map_err(|_| {
            ConnectorError::ConfigurationError(
                "Iceberg runtime deployment ID must be a canonical non-nil UUID".into(),
            )
        })?;
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
        self.runtime_context = Some(context);
        Ok(())
    }

    fn contract(&self, config: &ConnectorConfig) -> Result<SinkContract, ConnectorError> {
        let config = IcebergSinkConfig::from_config(config)?;
        capabilities::validate_sink(&config)?;
        let warehouse = config.catalog.warehouse.to_ascii_lowercase();
        let shared_warehouse = warehouse.starts_with("s3://")
            || warehouse.starts_with("s3a://")
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
            let built = super::iceberg_io::build_catalog_for_access(
                &self.config.catalog,
                &self.config.storage,
                super::iceberg_io::CatalogAccess::Write {
                    auto_create: self.config.auto_create,
                },
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
            self.catalog = Some(catalog);
            self.table = Some(table);
            self.state = ConnectorState::Running;
            info!(namespace, table = table_name, "Iceberg sink connected");
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
        if batch.num_rows() == 0 {
            return Ok(WriteResult::new(0, 0));
        }
        #[cfg(feature = "iceberg-core")]
        {
            if self.schema.is_none() {
                self.schema = Some(batch.schema());
            }
            if self.active_epoch.get_mut().is_none() {
                if self.is_coordinated() {
                    return Err(ConnectorError::InvalidState {
                        expected: "begin_epoch before an exactly-once Iceberg write".into(),
                        actual: "no active epoch".into(),
                    });
                }
                self.direct_epoch = self.direct_epoch.saturating_add(1);
                *self.active_epoch.get_mut() = Some(self.new_epoch_writer(self.direct_epoch)?);
                self.active_epoch_id = Some(self.direct_epoch);
            }
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

    async fn begin_epoch(&mut self, epoch: u64) -> Result<(), ConnectorError> {
        #[cfg(not(feature = "iceberg-core"))]
        let _ = epoch;
        #[cfg(feature = "iceberg-core")]
        {
            if !self.is_coordinated() {
                return Ok(());
            }
            self.ensure_no_unresolved_publication()?;
            if self.active_epoch.get_mut().is_some() {
                return Err(ConnectorError::InvalidState {
                    expected: "previous Iceberg epoch prepared or rolled back".into(),
                    actual: "an epoch writer is still active".into(),
                });
            }
            *self.active_epoch.get_mut() = Some(self.new_epoch_writer(epoch)?);
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
            let started = std::time::Instant::now();
            let writer = self.active_epoch.get_mut().take().ok_or_else(|| {
                ConnectorError::Internal("Iceberg active epoch has no writer".into())
            })?;
            self.active_epoch_id = None;
            let output = writer.close().await?;
            #[cfg(test)]
            fault_injection::fail_if(fault_injection::IcebergFaultPoint::AfterFileClose)?;
            self.metrics
                .pre_commit_duration
                .observe(started.elapsed().as_secs_f64());
            if output.data_files.is_empty() {
                return Ok(None);
            }
            let descriptor = IcebergCommitDescriptorV1::encode(
                self.table()?,
                &self.config,
                &self.epoch_identity(epoch)?,
                output,
            )?;
            #[cfg(test)]
            fault_injection::fail_if(fault_injection::IcebergFaultPoint::AfterDescriptor)?;
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
            self.discard_active_epoch().await?;
        }
        Ok(())
    }

    fn suggested_write_timeout(&self) -> Duration {
        self.config
            .catalog
            .commit_timeout
            .min(self.config.storage.request_timeout)
    }

    fn flush_interval(&self) -> Duration {
        self.config.max_flush_age.min(Duration::from_secs(5))
    }

    async fn flush(&mut self) -> Result<(), ConnectorError> {
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
            } else {
                self.flush().await?;
            }
            self.catalog = None;
            self.catalog_capabilities = super::iceberg_io::CatalogCapabilities::default();
            self.table = None;
            self.iceberg_arrow_schema = None;
            self.alignment_plan = None;
            self.metrics.set_buffer(0, 0);
            self.metrics.set_active_writers(0);
        }
        self.state = ConnectorState::Closed;
        Ok(())
    }

    #[cfg(feature = "iceberg-core")]
    fn as_coordinated_committer(&self) -> Option<&dyn crate::connector::CoordinatedCommitter> {
        self.is_coordinated()
            .then_some(self as &dyn crate::connector::CoordinatedCommitter)
    }
}

#[cfg(feature = "iceberg-core")]
#[async_trait]
impl crate::connector::CoordinatedCommitter for IcebergSink {
    async fn commit_aggregated(
        &self,
        batch: crate::connector::CoordinatedCommitBatch,
        context: crate::connector::CoordinatedCommitContext,
    ) -> Result<(), ConnectorError> {
        let catalog = self
            .catalog
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open Iceberg sink".into(),
                actual: "catalog is not initialized".into(),
            })?;
        let pending = publication::unresolved_publication(&self.config, &batch)?;
        {
            let mut unresolved = self.unresolved_publication.lock();
            if unresolved
                .as_ref()
                .is_some_and(|existing| existing != &pending)
            {
                return Err(ConnectorError::TransactionError(
                    "Iceberg has a different unresolved publication; only that exact cut may be reconciled"
                        .into(),
                ));
            }
            *unresolved = Some(pending.clone());
        }
        let result = publication::publish_coordinated(
            catalog,
            &self.catalog_capabilities,
            &self.config,
            &batch,
            context,
            &self.metrics,
        )
        .await;
        if result.is_ok()
            || result
                .as_ref()
                .is_err_and(|error| !error.is_outcome_unknown())
        {
            let mut unresolved = self.unresolved_publication.lock();
            if unresolved.as_ref() == Some(&pending) {
                *unresolved = None;
            }
        }
        result
    }

    async fn committed_cursor(
        &self,
        namespace: &crate::connector::CoordinatedCommitNamespace,
    ) -> Result<Option<crate::connector::CoordinatedCommitCursor>, ConnectorError> {
        let catalog = self
            .catalog
            .as_ref()
            .ok_or_else(|| ConnectorError::InvalidState {
                expected: "open Iceberg sink".into(),
                actual: "catalog is not initialized".into(),
            })?;
        let deadline = tokio::time::Instant::now() + self.config.catalog.request_timeout;
        let pending = self.unresolved_publication.lock().clone();
        let cursor = publication::read_committed_cursor(
            catalog,
            &self.config,
            namespace,
            deadline,
            &self.metrics,
            pending.as_ref(),
        )
        .await?;
        let mut unresolved = self.unresolved_publication.lock();
        if unresolved.as_ref().is_some_and(|pending| {
            pending.external_key == namespace.external_key() && pending.reconciled_by(cursor)
        }) {
            *unresolved = None;
        }
        Ok(cursor)
    }
}

#[cfg(feature = "iceberg-core")]
fn parquet_compression(name: &str) -> Result<parquet::basic::Compression, ConnectorError> {
    match name.trim().to_ascii_lowercase().as_str() {
        "snappy" => Ok(parquet::basic::Compression::SNAPPY),
        "none" | "uncompressed" => Ok(parquet::basic::Compression::UNCOMPRESSED),
        "lz4" => Ok(parquet::basic::Compression::LZ4),
        "zstd" => Ok(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(3).unwrap_or_default(),
        )),
        other => Err(ConnectorError::ConfigurationError(format!(
            "unsupported parquet.compression '{other}'"
        ))),
    }
}

#[cfg(test)]
mod tests;

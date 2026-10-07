//! Prepared lookup projections with native checks around each cache-miss batch.

use super::resolution::{project_batch, SchemaBinding};
use crate::config::ConnectorConfig;
use crate::error::ConnectorError;
use crate::registry::ConnectorRegistry;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use laminar_core::lookup::predicate::Predicate;
use laminar_core::lookup::source::{ColumnId, LookupError, LookupSourceDyn};
use std::sync::Arc;

pub(crate) struct BoundLookup {
    source: Arc<dyn LookupSourceDyn>,
    registry: ConnectorRegistry,
    config: ConnectorConfig,
    binding: SchemaBinding,
    schema: SchemaRef,
    projection: Vec<ColumnId>,
}

impl BoundLookup {
    pub(crate) fn new(
        source: Arc<dyn LookupSourceDyn>,
        registry: ConnectorRegistry,
        config: ConnectorConfig,
        binding: SchemaBinding,
    ) -> Result<Self, ConnectorError> {
        let schema = Arc::new(binding.logical.clone());
        let native = source.schema();
        let projection = schema
            .fields()
            .iter()
            .map(|field| {
                let index = native.index_of(field.name()).map_err(|_| {
                    ConnectorError::SchemaMismatch(
                        "lookup field is absent from the activated reader".into(),
                    )
                })?;
                if native.field(index).data_type() != field.data_type() {
                    return Err(ConnectorError::SchemaMismatch(
                        "lookup reader changed field types at activation".into(),
                    ));
                }
                u32::try_from(index).map_err(|_| {
                    ConnectorError::SchemaMismatch(
                        "lookup projection exceeds column index range".into(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            source,
            registry,
            config,
            binding,
            schema,
            projection,
        })
    }

    async fn check_native(&self) -> Result<(), LookupError> {
        if self.binding.value.is_none() {
            return Ok(());
        }
        let current = self
            .registry
            .resolve_lookup_schema(&self.config, Some(self.schema.clone()))
            .await
            .map_err(|error| match error {
                ConnectorError::Timeout(milliseconds) => {
                    LookupError::Timeout(std::time::Duration::from_millis(milliseconds))
                }
                ConnectorError::ConnectionFailed(reason) => LookupError::Connection(reason),
                other => LookupError::Query(other.to_string()),
            })?;
        if self.binding.value != current.value {
            return Err(LookupError::Query(
                "lookup native identity or layout changed; migrate the committed contract".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl LookupSourceDyn for BoundLookup {
    async fn query_batch(
        &self,
        keys: &[&[u8]],
        predicates: &[Predicate],
        projection: &[ColumnId],
    ) -> Result<Vec<Option<RecordBatch>>, LookupError> {
        let requested = if projection.is_empty() {
            (0..self.projection.len()).collect::<Vec<_>>()
        } else {
            projection
                .iter()
                .map(|index| {
                    usize::try_from(*index)
                        .map_err(|_| LookupError::Query("invalid lookup column index".into()))
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        let native_projection = requested
            .iter()
            .map(|index| {
                self.projection.get(*index).copied().ok_or_else(|| {
                    LookupError::Query("lookup projection is outside the frozen reader".into())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let output = Arc::new(
            self.schema
                .project(&requested)
                .map_err(|error| LookupError::Query(error.to_string()))?,
        );
        self.check_native().await?;
        let rows = self
            .source
            .query_batch(keys, predicates, &native_projection)
            .await?;
        self.check_native().await?;
        rows.into_iter()
            .map(|batch| {
                batch
                    .map(|batch| {
                        project_batch(&batch, &output)
                            .map_err(|error| LookupError::Query(error.to_string()))
                    })
                    .transpose()
            })
            .collect()
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

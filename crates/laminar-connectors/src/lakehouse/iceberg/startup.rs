//! Native target admission and prepared mapping installation.

use super::{metrics, schema_alignment, IcebergSink, SchemaAlignmentPlan};
use crate::config::{ConnectorConfig, ConnectorState};
use crate::error::ConnectorError;
use std::sync::Arc;

impl IcebergSink {
    pub(super) async fn open_target(
        &mut self,
        config: &ConnectorConfig,
    ) -> Result<(), ConnectorError> {
        let built = super::super::iceberg_io::build_catalog_for_access_with_metrics(
            &self.config.catalog,
            &self.config.storage,
            super::super::iceberg_io::CatalogAccess::Write {
                auto_create: self.config.auto_create,
            },
            Some(self.metrics.credential_refresh_failures.clone()),
        )
        .await?;
        let catalog = built.catalog;
        let namespace = &self.config.catalog.namespace;
        let table_name = &self.config.catalog.table_name;
        if self.config.auto_create && config.schema_binding().is_none() {
            if let Some(schema) = config.arrow_schema() {
                tokio::time::timeout(
                        self.config.catalog.request_timeout,
                        super::super::iceberg_io::ensure_table_exists(
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
        let table = super::super::iceberg_io::load_table_with_timeout(
            catalog.as_ref(),
            namespace,
            table_name,
            self.config.catalog.request_timeout,
        )
        .await?;
        super::super::schema_resolution::verify_identity(
            config.schema_binding(),
            &super::super::schema_resolution::iceberg_native(&table),
        )?;
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
        Ok(())
    }
}

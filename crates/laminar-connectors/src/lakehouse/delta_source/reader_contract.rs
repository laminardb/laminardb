//! Reader contract installation for catalog and direct API activation.

use super::{Arc, ConnectorConfig, ConnectorError, DeltaSource, DeltaTable};
use crate::schema::resolution::{SchemaDirection, SchemaOrigin};

impl DeltaSource {
    pub(super) fn install_reader_contract(
        &mut self,
        config: &ConnectorConfig,
        table: &DeltaTable,
    ) -> Result<(), ConnectorError> {
        let native = super::super::schema_resolution::delta_native(table)?;
        super::super::schema_resolution::verify_identity(config.schema_binding(), &native)?;
        let external = super::super::schema_resolution::delta_read_schema(
            &super::super::delta_io::get_table_schema(table)?,
        );
        let mut binding = if let Some(binding) = config.schema_binding() {
            binding.clone()
        } else {
            let explicit = config.arrow_schema();
            let origin = if explicit.is_some() {
                SchemaOrigin::Explicit
            } else {
                SchemaOrigin::Metadata
            };
            let logical = explicit.unwrap_or_else(|| Arc::clone(&external));
            let mut binding = crate::schema::resolution::logical_binding(
                config,
                SchemaDirection::Source,
                origin,
                &logical,
            )?;
            binding.value = Some(native);
            binding
        };
        binding
            .bind_external(external.as_ref().clone())
            .map_err(crate::schema::resolution::binding_error)?;
        binding
            .canonical_bytes()
            .map_err(crate::schema::resolution::binding_error)?;
        self.schema = Some(Arc::new(binding.logical.clone()));
        self.schema_binding = Some(binding);
        Ok(())
    }
}

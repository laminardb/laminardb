//! Admission of bounded participant descriptors for publication or cleanup.

use std::collections::HashSet;

use iceberg::spec::{DataContentType, DataFile};

use crate::connector::{CoordinatedCommitNamespace, CoordinatedCommitPayload};
use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::{
    stable_catalog_identity, IcebergSinkConfig, ICEBERG_MAX_FILES_PER_CHECKPOINT,
};

use super::descriptor::{IcebergCommitDescriptorV1, IcebergTableBindingV1};

pub(super) struct PreparedDescriptorFiles {
    pub(super) binding: IcebergTableBindingV1,
    pub(super) data_files: Vec<DataFile>,
    pub(super) expected_paths: HashSet<String>,
}

pub(super) fn prepare_descriptor_files(
    config: &IcebergSinkConfig,
    namespace: &CoordinatedCommitNamespace,
    entries: &[CoordinatedCommitPayload],
    table: &iceberg::table::Table,
) -> Result<PreparedDescriptorFiles, ConnectorError> {
    let mut binding: Option<IcebergTableBindingV1> = None;
    let mut data_files = Vec::new();
    let mut expected_paths = HashSet::new();
    let descriptor_limit = config
        .max_descriptor_bytes
        .min(crate::connector::MAX_COORDINATED_COMMIT_PAYLOAD_BYTES);
    let file_limit = config
        .max_files_per_checkpoint
        .min(ICEBERG_MAX_FILES_PER_CHECKPOINT);
    for entry in entries {
        let Some(payload) = &entry.payload else {
            continue;
        };
        if payload.len() > descriptor_limit {
            return Err(ConnectorError::TransactionError(format!(
                "Iceberg participant descriptor is {} bytes; configured limit is {descriptor_limit}",
                payload.len()
            )));
        }
        let descriptor = IcebergCommitDescriptorV1::decode(payload)?;
        validate_runtime_identity(&descriptor, namespace, entry)?;
        if descriptor.table.catalog_identity
            != stable_catalog_identity(&config.catalog, &config.storage)
        {
            return Err(ConnectorError::TransactionError(
                "Iceberg descriptor catalog identity differs from the configured catalog".into(),
            ));
        }
        match &binding {
            Some(expected) if !expected.has_same_append_target(&descriptor.table) => {
                return Err(ConnectorError::TransactionError(
                    "Iceberg descriptors bind different tables, refs, schemas, specs, or sort orders"
                        .into(),
                ));
            }
            None => binding = Some(descriptor.table.clone()),
            Some(_) => {}
        }
        let decoded = descriptor.decode_data_files(table)?;
        let projected = data_files.len().checked_add(decoded.len()).ok_or_else(|| {
            ConnectorError::TransactionError("Iceberg aggregate file count overflow".into())
        })?;
        if projected > file_limit {
            return Err(ConnectorError::TransactionError(format!(
                "Iceberg coordinated descriptor set exceeds the {file_limit}-file checkpoint limit"
            )));
        }
        for file in decoded {
            if file.content_type() != DataContentType::Data {
                return Err(ConnectorError::TransactionError(
                    "Iceberg append descriptor contains a delete file".into(),
                ));
            }
            if !expected_paths.insert(file.file_path().to_string()) {
                return Err(ConnectorError::TransactionError(
                    "Iceberg coordinated descriptors repeat a data file".into(),
                ));
            }
            data_files.push(file);
        }
    }
    data_files.sort_by(|left, right| left.file_path().cmp(right.file_path()));
    let binding = match binding {
        Some(binding) => binding,
        None => IcebergTableBindingV1::from_table(table, config)?,
    };
    Ok(PreparedDescriptorFiles {
        binding,
        data_files,
        expected_paths,
    })
}

fn validate_runtime_identity(
    descriptor: &IcebergCommitDescriptorV1,
    namespace: &CoordinatedCommitNamespace,
    entry: &CoordinatedCommitPayload,
) -> Result<(), ConnectorError> {
    if descriptor.deployment_id != namespace.deployment_id
        || descriptor.sink_id != namespace.sink_id
        || descriptor.participant_id != entry.participant_id
        || descriptor.epoch_id != entry.attempt.epoch
    {
        return Err(ConnectorError::TransactionError(
            "Iceberg descriptor runtime identity does not match its coordinated entry".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_table_incarnation(
    config: &IcebergSinkConfig,
    binding: &IcebergTableBindingV1,
    table: &iceberg::table::Table,
) -> Result<(), ConnectorError> {
    let metadata = table.metadata();
    let mismatch = binding.catalog_implementation != config.catalog.catalog_type.to_string()
        || binding.catalog_identity != stable_catalog_identity(&config.catalog, &config.storage)
        || binding.table_uuid != metadata.uuid().to_string()
        || binding.table_identifier != table.identifier().to_string()
        || binding.table_location != metadata.location()
        || binding.table_ref != config.table_ref;
    if mismatch {
        return Err(ConnectorError::TransactionError(
            "Iceberg table UUID, ref, location, or catalog binding changed".into(),
        ));
    }
    Ok(())
}

pub(super) fn validate_table_binding(
    config: &IcebergSinkConfig,
    binding: &IcebergTableBindingV1,
    table: &iceberg::table::Table,
) -> Result<(), ConnectorError> {
    validate_table_incarnation(config, binding, table)?;
    let metadata = table.metadata();
    if binding.schema_id != metadata.current_schema_id()
        || binding.partition_spec_id != metadata.default_partition_spec_id()
        || binding.sort_order_id != metadata.default_sort_order_id()
        || binding.format_version
            != super::descriptor::format_version_number(metadata.format_version())
    {
        return Err(ConnectorError::TransactionError(
            "Iceberg schema, partition spec, sort order, or format changed before publication"
                .into(),
        ));
    }
    Ok(())
}

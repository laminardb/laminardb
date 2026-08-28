use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::{
    IcebergReadMode, IcebergSchemaEvolutionMode, IcebergSinkConfig, IcebergSourceConfig,
    IcebergWriteMode,
};

const MOR_MISSING_ACTIONS: &str =
    "iceberg.write.merge-on-read: iceberg-rust 0.10.1 has no public atomic RowDelta action or position-delete writer";
const COW_MISSING_ACTIONS: &str =
    "iceberg.write.copy-on-write: iceberg-rust 0.10.1 has no public atomic RewriteFiles, OverwriteFiles, ReplacePartitions, or DeleteFiles action";
const CHANGELOG_MISSING_RECONCILIATION: &str =
    "iceberg.read.changelog: iceberg-rust 0.10.1 does not expose complete scan-side delete reconciliation and identifier semantics";

pub(crate) fn validate_sink(config: &IcebergSinkConfig) -> Result<(), ConnectorError> {
    match config.write_mode {
        IcebergWriteMode::Append => {}
        IcebergWriteMode::MergeOnRead => {
            return Err(ConnectorError::FeatureUnsupported(
                MOR_MISSING_ACTIONS.into(),
            ));
        }
        IcebergWriteMode::CopyOnWrite => {
            return Err(ConnectorError::FeatureUnsupported(
                COW_MISSING_ACTIONS.into(),
            ));
        }
    }
    config.validate_writer_limits()?;
    if config.table_ref != "main" {
        return Err(ConnectorError::FeatureUnsupported(format!(
            "iceberg.write.table-ref: FastAppend in iceberg-rust 0.10.1 only publishes the main branch; requested '{}'",
            config.table_ref
        )));
    }
    if config.schema_evolution_mode != IcebergSchemaEvolutionMode::Strict {
        return Err(ConnectorError::FeatureUnsupported(
            "iceberg.schema-evolution.safe: checkpoint-bound schema update actions are not implemented"
                .into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_source(config: &IcebergSourceConfig) -> Result<(), ConnectorError> {
    if config.read_mode == IcebergReadMode::Changelog {
        return Err(ConnectorError::FeatureUnsupported(
            CHANGELOG_MISSING_RECONCILIATION.into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::config::ConnectorConfig;
    use crate::lakehouse::iceberg_config::{IcebergSinkConfig, IcebergSourceConfig};

    use super::*;

    fn connector_config() -> ConnectorConfig {
        let mut config = ConnectorConfig::new("iceberg");
        config.set("catalog.uri", "http://catalog.invalid");
        config.set("catalog.warehouse", "file:///tmp/warehouse");
        config.set("namespace", "test");
        config.set("table.name", "events");
        config
    }

    #[test]
    fn mutation_modes_fail_closed() {
        for mode in ["merge-on-read", "copy-on-write"] {
            let mut config = connector_config();
            config.set("write.mode", mode);
            let error = validate_sink(&IcebergSinkConfig::from_config(&config).unwrap())
                .expect_err("unsupported mutation mode must be rejected");
            assert!(matches!(error, ConnectorError::FeatureUnsupported(_)));
            assert!(error.to_string().contains(mode));
        }
    }

    #[test]
    fn changelog_fails_closed() {
        let mut config = connector_config();
        config.set("read.mode", "changelog");
        let error = validate_source(&IcebergSourceConfig::from_config(&config).unwrap())
            .expect_err("unsupported changelog must be rejected");
        assert!(matches!(error, ConnectorError::FeatureUnsupported(_)));
    }
}

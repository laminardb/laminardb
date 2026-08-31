use crate::connector::DeliveryGuarantee;
use crate::error::ConnectorError;
use crate::lakehouse::iceberg_config::{
    IcebergCatalogAuthType, IcebergCatalogConfig, IcebergCatalogType, IcebergReadMode,
    IcebergSchemaEvolutionMode, IcebergSinkConfig, IcebergSourceConfig, IcebergStorageType,
    IcebergWriteMode,
};

const MOR_MISSING_ACTIONS: &str =
    "[LDB-ICEBERG-MOR-UNSUPPORTED] iceberg.write.merge-on-read: iceberg-rust 0.10.1 has no public atomic RowDelta action or position-delete writer";
const COW_MISSING_ACTIONS: &str =
    "[LDB-ICEBERG-COW-UNSUPPORTED] iceberg.write.copy-on-write: iceberg-rust 0.10.1 has no public atomic RewriteFiles, OverwriteFiles, ReplacePartitions, or DeleteFiles action";
const CHANGELOG_MISSING_RECONCILIATION: &str =
    "[LDB-ICEBERG-CHANGELOG-UNSUPPORTED] iceberg.read.changelog: iceberg-rust 0.10.1 does not expose complete scan-side delete reconciliation and identifier semantics";

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
    validate_catalog_session(&config.catalog)?;
    Ok(())
}

pub(crate) fn cluster_exact_append_certified(config: &IcebergSinkConfig) -> bool {
    cfg!(all(
        feature = "iceberg-catalog-rest",
        feature = "iceberg-storage-s3"
    )) && config.delivery_guarantee == DeliveryGuarantee::ExactlyOnce
        && config.write_mode == IcebergWriteMode::Append
        && config.catalog.catalog_type == IcebergCatalogType::Rest
        && matches!(
            config.catalog.auth_type,
            IcebergCatalogAuthType::None | IcebergCatalogAuthType::Bearer
        )
        && !config.catalog.access_delegation
        && matches!(
            config.storage.storage_type,
            None | Some(IcebergStorageType::S3)
        )
        && direct_s3_warehouse(&config.catalog.warehouse)
}

fn direct_s3_warehouse(warehouse: &str) -> bool {
    warehouse.split_once("://").is_some_and(|(scheme, path)| {
        (scheme.eq_ignore_ascii_case("s3") || scheme.eq_ignore_ascii_case("s3a"))
            && !path.is_empty()
    })
}

pub(crate) fn validate_source(config: &IcebergSourceConfig) -> Result<(), ConnectorError> {
    config.validate_read_limits()?;
    if config.read_mode == IcebergReadMode::Changelog {
        return Err(ConnectorError::FeatureUnsupported(
            CHANGELOG_MISSING_RECONCILIATION.into(),
        ));
    }
    validate_catalog_session(&config.catalog)?;
    Ok(())
}

pub(crate) fn validate_catalog_session(
    catalog: &IcebergCatalogConfig,
) -> Result<(), ConnectorError> {
    if catalog.catalog_type != IcebergCatalogType::Rest {
        return Ok(());
    }
    if catalog.auth_type == IcebergCatalogAuthType::Bearer
        && catalog.properties.get("token").is_none_or(String::is_empty)
    {
        return Err(ConnectorError::ConfigurationError(
            "catalog.auth.type=bearer requires a non-empty resolved catalog.property.token".into(),
        ));
    }
    if catalog.access_delegation {
        return Err(ConnectorError::FeatureUnsupported(
            "iceberg.catalog.rest.access-delegation: iceberg-rust 0.10.1 does not provide refreshable vended credentials or remote signing"
                .into(),
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
        for (mode, code) in [
            ("merge-on-read", "LDB-ICEBERG-MOR-UNSUPPORTED"),
            ("copy-on-write", "LDB-ICEBERG-COW-UNSUPPORTED"),
        ] {
            let mut config = connector_config();
            config.set("write.mode", mode);
            let error = validate_sink(&IcebergSinkConfig::from_config(&config).unwrap())
                .expect_err("unsupported mutation mode must be rejected");
            assert!(matches!(error, ConnectorError::FeatureUnsupported(_)));
            assert!(error.to_string().contains(mode));
            assert!(error.to_string().contains(code));
        }
    }

    #[test]
    fn changelog_fails_closed() {
        let mut config = connector_config();
        config.set("read.mode", "changelog");
        let error = validate_source(&IcebergSourceConfig::from_config(&config).unwrap())
            .expect_err("unsupported changelog must be rejected");
        assert!(matches!(error, ConnectorError::FeatureUnsupported(_)));
        assert!(error
            .to_string()
            .contains("LDB-ICEBERG-CHANGELOG-UNSUPPORTED"));
    }

    #[test]
    fn access_delegation_fails_before_catalog_io() {
        let mut config = connector_config();
        config.set("catalog.access_delegation", "true");
        let sink = IcebergSinkConfig::from_config(&config).unwrap();
        let source = IcebergSourceConfig::from_config(&config).unwrap();
        assert!(matches!(
            validate_sink(&sink),
            Err(ConnectorError::FeatureUnsupported(_))
        ));
        assert!(matches!(
            validate_source(&source),
            Err(ConnectorError::FeatureUnsupported(_))
        ));
    }

    #[test]
    fn cluster_exact_certification_is_rest_s3_only() {
        let mut config = connector_config();
        config.set("catalog.warehouse", "s3://warehouse/root");
        config.set("storage.type", "s3");
        config.set("delivery.guarantee", "exactly-once");
        let parsed = IcebergSinkConfig::from_config(&config).unwrap();
        assert_eq!(
            cluster_exact_append_certified(&parsed),
            cfg!(all(
                feature = "iceberg-catalog-rest",
                feature = "iceberg-storage-s3"
            ))
        );

        for (key, value) in [
            ("catalog.type", "glue"),
            ("storage.type", "gcs"),
            ("catalog.warehouse", "file:///tmp/warehouse"),
            ("catalog.auth.type", "oauth2"),
            ("catalog.access_delegation", "true"),
        ] {
            let mut rejected = config.clone();
            rejected.set(key, value);
            if key == "catalog.auth.type" {
                rejected.set("catalog.property.credential", "resolved-secret");
            }
            assert!(
                !cluster_exact_append_certified(
                    &IcebergSinkConfig::from_config(&rejected).unwrap()
                ),
                "{key}={value} must not be cluster-certified"
            );
        }
    }
}

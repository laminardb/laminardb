use super::*;
use crate::config::ConnectorConfig;

fn storage_config(storage_type: Option<&str>) -> IcebergStorageConfig {
    let mut config = ConnectorConfig::new("iceberg");
    if let Some(storage_type) = storage_type {
        config.set("storage.type", storage_type);
    }
    IcebergStorageConfig::from_config(&config).unwrap()
}

#[cfg(feature = "iceberg-catalog-rest")]
fn catalog_config(configure: impl FnOnce(&mut ConnectorConfig)) -> IcebergCatalogConfig {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("catalog.uri", "https://catalog.test");
    config.set("catalog.warehouse", "s3://bucket/warehouse");
    config.set("namespace", "test");
    config.set("table.name", "events");
    configure(&mut config);
    IcebergCatalogConfig::from_config(&config).unwrap()
}

#[test]
fn catalog_commit_failure_has_unknown_outcome() {
    let source =
        iceberg::Error::new(iceberg::ErrorKind::Unexpected, "response lost").with_retryable(true);
    let error = iceberg_commit_error(&source);
    assert!(error.is_outcome_unknown());
    assert!(error.is_transient());
    assert!(error.to_string().contains("may have applied"));
}

#[test]
fn external_errors_do_not_expose_provider_messages() {
    let source = iceberg::Error::new(
        iceberg::ErrorKind::Unexpected,
        "https://user:secret@objects.test/data?token=credential",
    )
    .with_retryable(true);
    let summary = external_error_summary(&source);
    let commit = iceberg_commit_error(&source).to_string();
    for value in [summary, commit] {
        assert!(value.contains("Unexpected"));
        assert!(!value.contains("secret"));
        assert!(!value.contains("credential"));
    }
}

#[test]
fn credential_bearing_catalog_locations_are_rejected_without_echoing() {
    for location in [
        "https://user:location-secret@objects.test/table",
        "s3://bucket/metadata.json?X-Amz-Signature=location-secret",
    ] {
        let error = validate_credential_free_location("test location", location)
            .unwrap_err()
            .to_string();
        assert!(error.contains("LDB-ICEBERG-CREDENTIAL-LOCATION"));
        assert!(!error.contains("location-secret"));
    }
}

#[test]
fn catalog_commit_conflict_is_a_definite_retryable_rejection() {
    let source = iceberg::Error::new(
        iceberg::ErrorKind::CatalogCommitConflicts,
        "base metadata changed",
    );
    let error = iceberg_commit_error(&source);
    assert!(!error.is_outcome_unknown());
    assert!(error.is_transient());
}

#[test]
#[cfg(feature = "iceberg-storage-s3")]
fn test_storage_factory_infers_s3_from_warehouse_url() {
    let f = storage_factory("s3://bucket/warehouse", &storage_config(None)).unwrap();
    assert!(format!("{f:?}").contains("S3"));
}

#[test]
#[cfg(feature = "iceberg-storage-s3")]
fn test_storage_factory_infers_s3a_from_warehouse_url() {
    let f = storage_factory("s3a://bucket/warehouse", &storage_config(None)).unwrap();
    assert!(format!("{f:?}").contains("S3"));
}

#[test]
#[cfg(feature = "iceberg-storage-fs")]
fn test_storage_factory_infers_fs_from_file_url() {
    let f = storage_factory("file:///tmp/warehouse", &storage_config(None)).unwrap();
    assert!(format!("{f:?}").contains("Fs"));
}

#[test]
fn test_storage_factory_bare_path_requires_explicit_storage_type() {
    // Trimmed `/` and `./` inference: REST catalogs use logical names
    // and we don't want a silent default to local fs.
    let err = storage_factory("/tmp/warehouse", &storage_config(None))
        .unwrap_err()
        .to_string();
    assert!(err.contains("LDB-5100"), "got: {err}");
}

#[test]
#[cfg(feature = "iceberg-storage-s3")]
fn test_storage_factory_explicit_overrides_inference() {
    // Lakekeeper-style: warehouse is a name, storage backend is S3.
    let f = storage_factory("demo", &storage_config(Some("s3"))).unwrap();
    assert!(format!("{f:?}").contains("S3"));
}

#[test]
fn test_storage_factory_unknown_warehouse_without_storage_type_errors() {
    let err = storage_factory("demo", &storage_config(None))
        .unwrap_err()
        .to_string();
    assert!(err.contains("LDB-5100"), "got: {err}");
}

#[test]
fn test_storage_factory_rejects_unknown_storage_type() {
    let mut config = ConnectorConfig::new("iceberg");
    config.set("storage.type", "hdfs");
    let err = IcebergStorageConfig::from_config(&config)
        .unwrap_err()
        .to_string();
    assert!(err.contains("storage.type"), "got: {err}");
}

#[test]
fn storage_scheme_inference_covers_cloud_backends() {
    assert_eq!(
        infer_storage_type("gs://bucket/warehouse"),
        Some(IcebergStorageType::Gcs)
    );
    assert_eq!(
        infer_storage_type("abfss://container@account.dfs.core.windows.net/warehouse"),
        Some(IcebergStorageType::Azure)
    );
}

#[cfg(feature = "iceberg-catalog-rest")]
#[test]
fn rest_auth_fails_closed_when_refresh_or_token_is_unavailable() {
    let oauth = catalog_config(|config| {
        config.set("catalog.auth.type", "oauth2");
        config.set("catalog.property.credential", "client:secret");
    });
    let delegated = catalog_config(|config| {
        config.set("catalog.access_delegation", "true");
    });
    for config in [&oauth, &delegated] {
        assert!(matches!(
            super::super::iceberg::capabilities::validate_catalog_session(config).unwrap_err(),
            ConnectorError::FeatureUnsupported(_)
        ));
    }
}

#[cfg(feature = "iceberg-catalog-rest")]
#[test]
fn rest_properties_keep_catalog_and_storage_configuration_separate() {
    let catalog = catalog_config(|config| {
        config.set("catalog.prefix", "tenant");
        config.set("catalog.auth.type", "bearer");
        config.set("catalog.property.token", "catalog-secret");
    });
    super::super::iceberg::capabilities::validate_catalog_session(&catalog).unwrap();
    let mut raw_storage = ConnectorConfig::new("iceberg");
    raw_storage.set("storage.type", "s3");
    raw_storage.set("storage.endpoint", "https://objects.test");
    raw_storage.set("storage.region", "eu-west-2");
    raw_storage.set("storage.path_style", "true");
    raw_storage.set("storage.encryption", "kms");
    raw_storage.set("storage.kms_key", "storage-secret");
    let storage = IcebergStorageConfig::from_config(&raw_storage).unwrap();

    let properties = rest_properties(&catalog, &storage).unwrap();
    assert_eq!(properties.get("prefix").map(String::as_str), Some("tenant"));
    assert_eq!(
        properties.get("s3.endpoint").map(String::as_str),
        Some("https://objects.test")
    );
    assert_eq!(
        properties.get("s3.path-style-access").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        properties.get("s3.sse.type").map(String::as_str),
        Some("kms")
    );
    assert_eq!(
        properties.get("s3.sse.key").map(String::as_str),
        Some("storage-secret")
    );
}

#[cfg(feature = "iceberg-catalog-rest")]
#[test]
fn storage_properties_cannot_control_catalog_authentication() {
    let catalog = catalog_config(|_| {});
    for key in ["token", "credential", "header.Authorization"] {
        let mut raw_storage = ConnectorConfig::new("iceberg");
        raw_storage.set(format!("storage.property.{key}"), "storage-secret");
        let storage = IcebergStorageConfig::from_config(&raw_storage).unwrap();
        let error = rest_properties(&catalog, &storage).unwrap_err();
        assert!(matches!(error, ConnectorError::ConfigurationError(_)));
        assert!(!error.to_string().contains("storage-secret"));
    }
}

#[cfg(feature = "iceberg-catalog-rest")]
#[test]
fn storage_specific_options_do_not_fall_through_to_s3() {
    let mut gcs_config = ConnectorConfig::new("iceberg");
    gcs_config.set("storage.type", "gcs");
    gcs_config.set("storage.endpoint", "https://storage.googleapis.test");
    let gcs = IcebergStorageConfig::from_config(&gcs_config).unwrap();
    validate_storage_options("gs://bucket/warehouse", &gcs).unwrap();
    let properties = rest_properties(&catalog_config(|_| {}), &gcs).unwrap();
    assert_eq!(
        properties.get("gcs.service.path").map(String::as_str),
        Some("https://storage.googleapis.test")
    );
    assert!(!properties.contains_key("s3.endpoint"));

    let mut azure_config = ConnectorConfig::new("iceberg");
    azure_config.set("storage.type", "azure");
    azure_config.set("storage.endpoint", "https://account.dfs.core.windows.net");
    let azure = IcebergStorageConfig::from_config(&azure_config).unwrap();
    assert!(matches!(
        validate_storage_options("abfss://container@account/warehouse", &azure).unwrap_err(),
        ConnectorError::FeatureUnsupported(_)
    ));
}

#[cfg(all(feature = "iceberg-catalog-rest", feature = "iceberg-storage-fs"))]
#[tokio::test]
async fn rest_catalog_loads_server_config_before_using_server_overrides() {
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/config"))
        .and(query_param("warehouse", "file:///tmp/warehouse"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "defaults": {"prefix": "default-prefix"},
            "overrides": {"prefix": "server-prefix"}
        })))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/server-prefix/namespaces"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"namespaces": []})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let catalog = catalog_config(|config| {
        config.set("catalog.uri", server.uri());
        config.set("catalog.warehouse", "file:///tmp/warehouse");
        config.set("catalog.prefix", "client-prefix");
    });
    let storage = storage_config(Some("fs"));
    let catalog = build_catalog(&catalog, &storage).await.unwrap();
    assert!(catalog.list_namespaces(None).await.unwrap().is_empty());

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].url.path(), "/v1/config");
    assert_eq!(requests[1].url.path(), "/v1/config");
    assert_eq!(requests[2].url.path(), "/v1/server-prefix/namespaces");
}

fn empty_fixture_table() -> Table {
    let schema = iceberg::spec::Schema::builder()
        .with_fields(vec![])
        .build()
        .unwrap();

    let creation = iceberg::TableCreation::builder()
        .name("test_table".to_string())
        .schema(schema)
        .location("s3://test/location".to_string())
        .build();

    let metadata = iceberg::spec::TableMetadataBuilder::from_table_creation(creation)
        .unwrap()
        .build()
        .unwrap();

    Table::builder()
        .metadata(metadata.metadata)
        .identifier(TableIdent::new(
            iceberg::NamespaceIdent::new("test".to_string()),
            "t".to_string(),
        ))
        .file_io(iceberg::io::FileIO::new_with_memory())
        .runtime(iceberg::Runtime::try_current().unwrap())
        .build()
        .unwrap()
}

#[tokio::test]
async fn test_current_snapshot_id_empty_table() {
    assert!(current_snapshot_id(&empty_fixture_table()).is_none());
}

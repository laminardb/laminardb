use super::*;

#[test]
fn test_file_scheme_creates_durable_local_store() {
    let dir = tempfile::tempdir().unwrap();
    let url = url::Url::from_directory_path(dir.path()).unwrap();
    let store = build_object_store(url.as_str(), &HashMap::new()).unwrap();
    assert!(store.to_string().starts_with("DurableLocalObjectStore("));
}

#[test]
fn test_file_scheme_empty_path_errors() {
    let result = build_object_store("file://", &HashMap::new());
    assert!(result.is_err());
}

#[test]
fn file_url_requires_an_absolute_local_path() {
    assert!(file_url_path("file://checkpoint-host/path").is_err());
    assert!(file_url_path("file://./relative").is_err());
    let directory = tempfile::tempdir().unwrap();
    let file_url = url::Url::from_directory_path(directory.path())
        .unwrap()
        .to_string();
    let uppercase_file_url = file_url.replacen("file", "FILE", 1);
    assert_eq!(
        file_url_path(&uppercase_file_url).unwrap(),
        directory.path()
    );
    assert!(file_url_path("file:///tmp/path?version=1").is_err());
    assert!(file_url_path("file:///tmp/path#fragment").is_err());
}

#[test]
fn absolute_local_file_url_classification_is_platform_independent() {
    assert!(is_absolute_local_file_url("file:///tmp/checkpoints"));

    let directory = tempfile::tempdir().unwrap();
    let local_url = url::Url::from_directory_path(directory.path()).unwrap();
    assert!(is_absolute_local_file_url(local_url.as_str()));

    assert!(!is_absolute_local_file_url("file://checkpoint-host/path"));
    assert!(!is_absolute_local_file_url("file://./relative"));
    assert!(is_absolute_local_file_url("FILE:///tmp/path"));
    assert!(!is_absolute_local_file_url("file:///tmp/path?version=1"));
    assert!(!is_absolute_local_file_url("file:///tmp/path#fragment"));
}

#[test]
fn checkpoint_storage_scope_matches_supported_schemes() {
    assert_eq!(
        CheckpointStorageScope::for_url("file:///tmp/checkpoints"),
        CheckpointStorageScope::NodeDurable
    );
    for url in [
        "s3://bucket/prefix",
        "s3a://bucket/prefix",
        "gs://bucket/prefix",
        "gcs://bucket/prefix",
        "az://container/prefix",
        "abfs://container/prefix",
        "abfss://container/prefix",
        "wasb://container/prefix",
        "wasbs://container/prefix",
    ] {
        assert_eq!(
            CheckpointStorageScope::for_url(url),
            CheckpointStorageScope::ClusterShared
        );
    }
    for url in ["memory://", "file://relative", "s3n://bucket", "ftp://host"] {
        assert_eq!(
            CheckpointStorageScope::for_url(url),
            CheckpointStorageScope::Volatile
        );
    }
}

#[test]
fn file_url_decodes_escaped_path_segments() {
    let directory = tempfile::tempdir().unwrap();
    let expected = directory.path().join("laminar checkpoint");
    let encoded = url::Url::from_directory_path(&expected).unwrap();
    assert!(encoded.as_str().contains("laminar%20checkpoint"));
    assert_eq!(file_url_path(encoded.as_str()).unwrap(), expected);
}

#[test]
fn test_unknown_scheme_errors() {
    let result = build_object_store("ftp://bucket/prefix", &HashMap::new());
    assert!(result.is_err());
}

#[test]
fn test_no_scheme_errors() {
    let result = build_object_store("/just/a/path", &HashMap::new());
    assert!(result.is_err());
}

#[test]
fn ambient_options_are_scoped_to_the_selected_provider() {
    assert!(provider_environment_key(
        StorageProvider::AwsS3,
        "AWS_REGION"
    ));
    assert!(!provider_environment_key(StorageProvider::AwsS3, "REGION"));
    assert!(!provider_environment_key(
        StorageProvider::AwsS3,
        "AZURE_STORAGE_TOKEN"
    ));
    assert!(provider_environment_key(
        StorageProvider::Gcs,
        "SERVICE_ACCOUNT"
    ));
    assert!(provider_environment_key(
        StorageProvider::AzureAdls,
        "IDENTITY_ENDPOINT"
    ));
    assert!(!provider_environment_key(
        StorageProvider::AzureAdls,
        "ENDPOINT"
    ));
}

#[test]
fn endpoint_resolution_is_deterministic_and_explicit_first() {
    let options = HashMap::from([
        (
            "aws_endpoint".to_string(),
            "https://explicit.example".to_string(),
        ),
        (
            "aws_endpoint_url_s3".to_string(),
            "https://service-specific.example".to_string(),
        ),
    ]);
    let environment = vec![
        (
            "AWS_ENDPOINT_URL_S3".to_string(),
            "https://ambient.example".to_string(),
        ),
        ("AWS_ALLOW_HTTP".to_string(), "true".to_string()),
    ];
    assert_eq!(
        configured_endpoint(StorageProvider::AwsS3, &options, &environment),
        Some("https://service-specific.example")
    );
    assert!(configured_allow_http(
        StorageProvider::AwsS3,
        &options,
        &environment
    ));

    let options = HashMap::from([("aws_allow_http".to_string(), "false".to_string())]);
    assert!(!configured_allow_http(
        StorageProvider::AwsS3,
        &options,
        &environment
    ));
}

#[test]
fn signed_endpoint_query_is_rejected_without_disclosure() {
    let options = HashMap::from([(
        "aws_endpoint".to_string(),
        "https://compat.example?X-Amz-Signature=do-not-print".to_string(),
    )]);
    let error = build_object_store("s3://bucket/checkpoints", &options)
        .unwrap_err()
        .to_string();
    assert!(error.contains("query parameters"), "{error}");
    assert!(!error.contains("do-not-print"), "{error}");
    assert!(!error.contains("compat.example"), "{error}");
}

#[cfg(feature = "aws")]
#[test]
fn explicit_provider_options_fail_closed() {
    let mut options = HashMap::from([("aws_regoin".to_string(), "us-east-1".to_string())]);
    let error = validate_explicit_options(StorageProvider::AwsS3, &options).unwrap_err();
    assert!(error.to_string().contains("aws_regoin"), "{error}");

    options = HashMap::from([
        ("aws_region".to_string(), "us-east-1".to_string()),
        ("region".to_string(), "us-west-2".to_string()),
    ]);
    assert!(validate_explicit_options(StorageProvider::AwsS3, &options).is_err());
}

#[cfg(feature = "azure")]
#[test]
fn qualified_azure_authority_cannot_be_overridden_by_options() {
    let options = HashMap::from([(
        "azure_storage_account_name".to_string(),
        "different-account".to_string(),
    )]);
    let error = build_object_store(
        "abfss://events@account.dfs.core.windows.net/checkpoints",
        &options,
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("conflicts with the URL authority"));
    assert!(!error.contains("different-account"));
    assert!(!error.contains("events@account"));
}

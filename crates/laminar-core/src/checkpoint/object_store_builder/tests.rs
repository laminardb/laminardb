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
    assert!(file_url_path("FILE:///tmp/path").is_err());
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
    assert!(!is_absolute_local_file_url("FILE:///tmp/path"));
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
        "az://container/prefix",
        "abfs://container/prefix",
        "abfss://container/prefix",
    ] {
        assert_eq!(
            CheckpointStorageScope::for_url(url),
            CheckpointStorageScope::ClusterShared
        );
    }
    for url in ["memory://", "file://relative", "gcs://bucket", "ftp://host"] {
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
    assert!(provider_environment_key("s3", "AWS_REGION"));
    assert!(!provider_environment_key("s3", "REGION"));
    assert!(!provider_environment_key("s3", "AZURE_STORAGE_TOKEN"));
    assert!(provider_environment_key("gs", "SERVICE_ACCOUNT"));
    assert!(provider_environment_key("az", "IDENTITY_ENDPOINT"));
    assert!(!provider_environment_key("az", "ENDPOINT"));
}

#[cfg(feature = "aws")]
#[test]
fn explicit_provider_options_fail_closed() {
    let mut options = HashMap::from([("aws_regoin".to_string(), "us-east-1".to_string())]);
    let error = validate_explicit_options("s3", &options).unwrap_err();
    assert!(error.to_string().contains("aws_regoin"), "{error}");

    options = HashMap::from([
        ("aws_region".to_string(), "us-east-1".to_string()),
        ("region".to_string(), "us-west-2".to_string()),
    ]);
    assert!(validate_explicit_options("s3", &options).is_err());
}

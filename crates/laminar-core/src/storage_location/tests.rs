use super::*;

#[test]
fn provider_aliases_are_table_driven() {
    let cases = [
        ("s3://bucket/path", StorageProvider::AwsS3, "s3"),
        ("S3A://bucket/path", StorageProvider::AwsS3, "s3"),
        ("gs://bucket/path", StorageProvider::Gcs, "gs"),
        ("GCS://bucket/path", StorageProvider::Gcs, "gs"),
        ("az://container/path", StorageProvider::AzureAdls, "az"),
        ("ABFS://container/path", StorageProvider::AzureAdls, "az"),
        ("abfss://container/path", StorageProvider::AzureAdls, "az"),
        ("wasb://container/path", StorageProvider::AzureAdls, "az"),
        ("WASBS://container/path", StorageProvider::AzureAdls, "az"),
        ("FILE:///tmp/checkpoints", StorageProvider::Local, "file"),
    ];
    for (input, provider, canonical) in cases {
        let parsed = StorageLocation::parse(input).unwrap();
        assert_eq!(parsed.provider, provider, "{input}");
        assert_eq!(parsed.canonical_scheme, canonical, "{input}");
    }
}

#[test]
fn consumer_adapters_canonicalize_only_what_the_consumer_requires() {
    let gcs = StorageLocation::parse("gcs://bucket/a%20b/é").unwrap();
    for consumer in [
        StorageConsumer::ObjectStore,
        StorageConsumer::Delta,
        StorageConsumer::Iceberg,
    ] {
        assert_eq!(gcs.adapt(consumer).unwrap().url, "gs://bucket/a%20b/é");
    }

    let s3a = StorageLocation::parse("S3A://bucket/prefix").unwrap();
    assert_eq!(
        s3a.adapt(StorageConsumer::Delta).unwrap().url,
        "s3a://bucket/prefix"
    );

    for input in [
        "az://container/path",
        "abfs://container/path",
        "abfss://container/path",
        "wasb://container/path",
        "wasbs://container/path",
    ] {
        for consumer in [StorageConsumer::ObjectStore, StorageConsumer::Delta] {
            assert_eq!(
                StorageLocation::parse(input)
                    .unwrap()
                    .adapt(consumer)
                    .unwrap()
                    .url,
                "az://container/path",
                "{input} for {consumer:?}"
            );
        }
    }
}

#[test]
fn azure_adapters_preserve_full_authority_in_options_or_url() {
    let input = "abfss://filesystem@account.dfs.core.chinacloudapi.cn/a//b";
    let parsed = StorageLocation::parse(input).unwrap();
    assert_eq!(parsed.bucket_or_container, "filesystem");
    assert_eq!(parsed.account.as_deref(), Some("account"));
    assert_eq!(parsed.filesystem.as_deref(), Some("filesystem"));

    let checkpoint = parsed.adapt(StorageConsumer::ObjectStore).unwrap();
    assert_eq!(checkpoint.url, "az://filesystem/a//b");
    assert!(checkpoint.derived_options.contains(&(
        "azure_endpoint".into(),
        "https://account.dfs.core.chinacloudapi.cn".into()
    )));
    assert!(checkpoint
        .derived_options
        .contains(&("azure_storage_account_name".into(), "account".into())));

    let iceberg = parsed.adapt(StorageConsumer::Iceberg).unwrap();
    assert_eq!(iceberg.url, input);
    assert!(iceberg.derived_options.is_empty());
    assert_eq!(parsed.endpoint_class(), StorageEndpointClass::Native);

    let private =
        StorageLocation::parse("abfss://filesystem@account.dfs.storage.private.example/a//b")
            .unwrap();
    assert_eq!(
        private.adapt(StorageConsumer::Iceberg).unwrap().url,
        "abfss://filesystem@account.dfs.storage.private.example/a//b"
    );
    assert_eq!(
        private.endpoint_class(),
        StorageEndpointClass::CustomOrEmulator
    );
}

#[test]
fn blob_and_adls_schemes_must_match_the_authority_service() {
    assert!(
        StorageLocation::parse("abfss://filesystem@account.blob.core.windows.net/path").is_err()
    );
    assert!(StorageLocation::parse("wasbs://container@account.dfs.core.windows.net/path").is_err());
}

#[test]
fn incomplete_azure_authorities_fail_or_remain_explicitly_unqualified() {
    let short = StorageLocation::parse("abfs://filesystem/path").unwrap();
    assert_eq!(short.account, None);
    assert!(short.adapt(StorageConsumer::Iceberg).is_err());
    assert!(StorageLocation::parse("abfss://account.dfs.example/path").is_err());
    assert!(StorageLocation::parse("abfss://@account.dfs.example/path").is_err());
    assert!(StorageLocation::parse("abfss:///path").is_err());
}

#[test]
fn raw_object_prefix_is_not_normalized() {
    let input = "s3://bucket/a//./../b%20c/literal space/日本語";
    let parsed = StorageLocation::parse(input).unwrap();
    assert_eq!(parsed.prefix, "a//./../b%20c/literal space/日本語");
    assert_eq!(
        parsed.adapt(StorageConsumer::ObjectStore).unwrap().url,
        input
    );
}

#[test]
fn credentials_queries_and_signed_urls_are_rejected_without_echoing_values() {
    let cases = [
        "s3://access:secret@bucket/path",
        "gs://token@bucket/path",
        "az://token@container/path",
        "az://container/path?sig=super-secret",
        "s3://bucket/path?X-Amz-Signature=super-secret",
    ];
    for input in cases {
        let error = StorageLocation::parse(input).unwrap_err();
        let diagnostic = format!("{error:?} {error}");
        assert!(!diagnostic.contains("super-secret"), "{diagnostic}");
        assert!(!diagnostic.contains("access:secret"), "{diagnostic}");
    }
}

#[test]
fn malformed_and_unsupported_locations_fail_closed() {
    for input in [
        "s3://",
        "gs:///prefix",
        "file://relative/path",
        "file:///",
        "relative/path",
        "http://example.test/path",
        "https://example.test/path",
        "s3n://bucket/path",
    ] {
        assert!(StorageLocation::parse(input).is_err(), "{input}");
    }
}

#[test]
fn endpoint_overrides_are_redacted_and_classified() {
    let native = StorageLocation::parse("s3://bucket/path").unwrap();
    assert_eq!(native.endpoint_class(), StorageEndpointClass::Native);
    let compatible = native
        .with_endpoint_override("http://minio.internal:9000/base")
        .unwrap();
    assert_eq!(
        compatible.endpoint_class(),
        StorageEndpointClass::S3Compatible
    );
    let endpoint = compatible.endpoint_override.as_ref().unwrap();
    assert!(endpoint.uses_http());
    assert!(endpoint.has_path());
    assert_eq!(endpoint.to_string(), "<custom-http-endpoint>");
    assert!(!format!("{compatible:?}").contains("minio.internal"));

    let error = StorageLocation::parse("gs://bucket/path")
        .unwrap()
        .with_endpoint_override("https://user:secret@example.test?token=hidden")
        .unwrap_err();
    let diagnostic = format!("{error:?} {error}");
    assert!(!diagnostic.contains("secret"));
    assert!(!diagnostic.contains("hidden"));
}

#[test]
fn debug_and_display_do_not_expose_location_components() {
    let parsed = StorageLocation::parse(
        "abfss://sensitive-fs@sensitive-account.dfs.private.example/private/prefix",
    )
    .unwrap();
    let debug = format!("{parsed:?}");
    let display = parsed.to_string();
    for secret in ["sensitive-fs", "sensitive-account", "private/prefix"] {
        assert!(!debug.contains(secret));
        assert!(!display.contains(secret));
    }
}

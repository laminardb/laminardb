use super::*;

#[test]
fn test_file_format_parse() {
    assert_eq!(FileFormat::parse("csv").unwrap(), FileFormat::Csv);
    assert_eq!(FileFormat::parse("JSON").unwrap(), FileFormat::Json);
    assert_eq!(FileFormat::parse("jsonl").unwrap(), FileFormat::Json);
    assert_eq!(FileFormat::parse("ndjson").unwrap(), FileFormat::Json);
    assert_eq!(FileFormat::parse("text").unwrap(), FileFormat::Text);
    assert_eq!(FileFormat::parse("parquet").unwrap(), FileFormat::Parquet);
    assert_eq!(FileFormat::parse("parq").unwrap(), FileFormat::Parquet);
    assert_eq!(FileFormat::parse("arrow").unwrap(), FileFormat::ArrowIpc);
    assert_eq!(FileFormat::parse("ipc").unwrap(), FileFormat::ArrowIpc);
    assert_eq!(
        FileFormat::parse("arrow_ipc").unwrap(),
        FileFormat::ArrowIpc
    );
    assert!(FileFormat::parse("xml").is_err());
}

#[test]
fn test_file_format_from_extension() {
    assert_eq!(
        FileFormat::from_extension("/data/logs/app.csv"),
        Some(FileFormat::Csv)
    );
    assert_eq!(
        FileFormat::from_extension("events.jsonl"),
        Some(FileFormat::Json)
    );
    assert_eq!(
        FileFormat::from_extension("data.parquet"),
        Some(FileFormat::Parquet)
    );
    assert_eq!(
        FileFormat::from_extension("log.txt"),
        Some(FileFormat::Text)
    );
    assert_eq!(
        FileFormat::from_extension("data.arrow"),
        Some(FileFormat::ArrowIpc)
    );
    assert_eq!(
        FileFormat::from_extension("stream.ipc"),
        Some(FileFormat::ArrowIpc)
    );
    assert_eq!(FileFormat::from_extension("file.bin"), None);
}

#[test]
fn test_file_format_extension() {
    assert_eq!(FileFormat::Csv.extension(), "csv");
    assert_eq!(FileFormat::Json.extension(), "jsonl");
    assert_eq!(FileFormat::Text.extension(), "txt");
    assert_eq!(FileFormat::Parquet.extension(), "parquet");
    assert_eq!(FileFormat::ArrowIpc.extension(), "arrow");
}

#[test]
fn test_file_format_is_bulk() {
    assert!(!FileFormat::Csv.is_bulk_format());
    assert!(!FileFormat::Json.is_bulk_format());
    assert!(!FileFormat::Text.is_bulk_format());
    assert!(FileFormat::ArrowIpc.is_bulk_format());
    assert!(FileFormat::Parquet.is_bulk_format());
}

#[test]
fn test_source_config_from_connector() {
    let mut config = ConnectorConfig::new("files");
    config.set("path", "/data/logs/*.csv");
    config.set("format", "csv");
    config.set("max_files_per_poll", "50");
    config.set("include_metadata", "true");

    let src = FileSourceConfig::from_connector_config(&config).unwrap();
    assert_eq!(src.path, "/data/logs/*.csv");
    assert_eq!(src.format, Some(FileFormat::Csv));
    assert_eq!(src.max_files_per_poll, 50);
    assert!(src.include_metadata);
}

#[test]
fn removed_probabilistic_manifest_options_are_rejected() {
    for option in [
        "allow_overwrites",
        "manifest_retention_count",
        "manifest_retention_age_days",
    ] {
        let mut config = ConnectorConfig::new("files");
        config.set("path", "/data/logs");
        config.set("format", "text");
        config.set(option, "1");
        let error = FileSourceConfig::from_connector_config(&config)
            .expect_err("removed lossy manifest option must fail closed");
        assert!(error.to_string().contains(option));
    }
}

#[test]
fn source_poll_bounds_must_be_nonzero() {
    for option in ["max_files_per_poll", "max_file_bytes"] {
        let mut config = ConnectorConfig::new("files");
        config.set("path", "/data/logs");
        config.set("format", "text");
        config.set(option, "0");
        let error = FileSourceConfig::from_connector_config(&config)
            .expect_err("zero source bound must fail closed");
        assert!(error.to_string().contains(option));
    }
}

#[test]
fn test_source_config_missing_path() {
    let config = ConnectorConfig::new("files");
    assert!(FileSourceConfig::from_connector_config(&config).is_err());
}

#[test]
fn test_sink_config_from_connector() {
    let mut config = ConnectorConfig::new("files");
    config.set("path", "/output");
    config.set("format", "parquet");
    config.set("compression", "zstd");

    let sink = FileSinkConfig::from_connector_config(&config).unwrap();
    assert_eq!(sink.path, "/output");
    assert_eq!(sink.format, FileFormat::Parquet);
    assert_eq!(sink.compression, "zstd");
}

#[test]
fn test_sink_config_missing_format() {
    let mut config = ConnectorConfig::new("files");
    config.set("path", "/output");
    assert!(FileSinkConfig::from_connector_config(&config).is_err());
}

#[test]
fn test_parse_duration_str() {
    assert_eq!(parse_duration_str("10").unwrap(), Duration::from_secs(10));
    assert_eq!(parse_duration_str("10s").unwrap(), Duration::from_secs(10));
    assert_eq!(
        parse_duration_str("500ms").unwrap(),
        Duration::from_millis(500)
    );
}

#[test]
fn test_removed_sink_mode_option_is_rejected() {
    for old_mode in ["append", "rolling"] {
        let mut config = ConnectorConfig::new("files");
        config.set("path", "/output");
        config.set("format", "csv");
        config.set("mode", old_mode);

        let error = FileSinkConfig::from_connector_config(&config).unwrap_err();
        assert!(error.to_string().contains("option 'mode' was removed"));
    }
}

#[test]
fn remote_source_and_sink_urls_fail_before_filesystem_io() {
    for location in [
        "s3://bucket/path",
        "s3a://bucket/path",
        "gs://bucket/path",
        "gcs://bucket/path",
        "az://container/path",
        "abfss://filesystem@account.dfs.core.windows.net/path",
        "wasbs://container@account.blob.core.windows.net/path",
        "https://storage.example/path",
    ] {
        let mut source = ConnectorConfig::new("files");
        source.set("path", location);
        source.set("format", "json");
        let source_error = FileSourceConfig::from_connector_config(&source).unwrap_err();
        assert!(source_error
            .to_string()
            .contains("remote Files connector backends are not enabled"));

        let mut sink = ConnectorConfig::new("files");
        sink.set("path", location);
        sink.set("format", "parquet");
        let sink_error = FileSinkConfig::from_connector_config(&sink).unwrap_err();
        assert!(sink_error
            .to_string()
            .contains("remote Files connector backends are not enabled"));
    }
}

#[test]
fn signed_remote_file_url_is_rejected_without_echoing_query_material() {
    let mut config = ConnectorConfig::new("files");
    config.set(
        "path",
        "az://container/path?sv=1&sig=do-not-echo-signed-secret",
    );
    config.set("format", "json");
    let error = FileSourceConfig::from_connector_config(&config).unwrap_err();
    assert!(!error.to_string().contains("do-not-echo-signed-secret"));
    assert!(error.to_string().contains("query parameters"));
}

#[test]
fn absolute_file_url_is_normalized_to_a_local_path() {
    let directory = tempfile::tempdir().unwrap();
    let file_url = url::Url::from_directory_path(directory.path())
        .unwrap()
        .to_string();
    let mut config = ConnectorConfig::new("files");
    config.set("path", &file_url);
    config.set("format", "csv");
    let parsed = FileSourceConfig::from_connector_config(&config).unwrap();
    assert_eq!(std::path::Path::new(&parsed.path), directory.path());
}

#[cfg(unix)]
#[test]
fn absolute_local_path_with_url_like_component_remains_local() {
    let mut config = ConnectorConfig::new("files");
    config.set("path", "/tmp/url://component/data");
    config.set("format", "json");
    let parsed = FileSourceConfig::from_connector_config(&config).unwrap();
    assert_eq!(parsed.path, "/tmp/url://component/data");
}

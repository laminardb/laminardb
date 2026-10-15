use super::*;

#[test]
fn test_register_file_source() {
    let registry = ConnectorRegistry::new();
    register_file_source(&registry).unwrap();

    let sources = registry.list_sources();
    assert!(sources.contains(&"files".to_string()));

    let info = registry.source_info("files").unwrap();
    assert_eq!(info.name, "files");
    assert!(info.is_source);
    assert!(!info.is_sink);
}

#[test]
fn test_register_file_sink() {
    let registry = ConnectorRegistry::new();
    register_file_sink(&registry).unwrap();

    let sinks = registry.list_sinks();
    assert!(sinks.contains(&"files".to_string()));

    let info = registry.sink_info("files").unwrap();
    assert_eq!(info.name, "files");
    assert!(!info.is_source);
    assert!(info.is_sink);
}

#[test]
fn test_create_source_from_registry() {
    let registry = ConnectorRegistry::new();
    register_file_source(&registry).unwrap();

    let config = crate::config::ConnectorConfig::new("files");
    let source = registry.create_source(&config, None);
    assert!(source.is_ok());
}

#[test]
fn test_create_sink_from_registry() {
    let registry = ConnectorRegistry::new();
    register_file_sink(&registry).unwrap();

    let config = crate::config::ConnectorConfig::new("files");
    let sink = registry.create_sink(&config, None);
    assert!(sink.is_ok());
}

/// `format = 'fpml'` through the public file source path: discovery, decoding, checkpoints.
#[cfg(feature = "fpml")]
mod fpml {
    use std::time::Duration;

    use arrow_array::cast::AsArray;
    use arrow_array::RecordBatch;
    use laminar_core::checkpoint::CheckpointAttempt;

    use crate::config::ConnectorConfig;
    use crate::connector::{DeliveryGuarantee, SourceConnector, SourcePosition, SourceStart};
    use crate::error::ConnectorError;
    use crate::files::FileSource;

    fn example(path: &str) -> Vec<u8> {
        std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/fpml/fpml-5-13-8-rec-2/confirmation")
                .join(path),
        )
        .unwrap()
    }

    fn config(directory: &std::path::Path) -> ConnectorConfig {
        let mut config = ConnectorConfig::new("files");
        config.set("path", directory.to_string_lossy().to_string());
        config.set("format", "fpml");
        config.set("include_metadata", "true");
        config.set("poll_interval", "20ms");
        config.set("stabilisation_delay", "0s");
        config
    }

    async fn start(
        config: ConnectorConfig,
        position: SourcePosition,
    ) -> Result<FileSource, ConnectorError> {
        let mut source = FileSource::new();
        source
            .start(SourceStart::new(config, position, DeliveryGuarantee::AtLeastOnce).unwrap())
            .await?;
        Ok(source)
    }

    /// Poll until `rows` rows arrive or the source fails.
    async fn poll_rows(
        source: &mut FileSource,
        max: usize,
        rows: usize,
    ) -> Result<Vec<RecordBatch>, ConnectorError> {
        let mut batches = Vec::new();
        let mut seen = 0;
        tokio::time::timeout(Duration::from_secs(20), async {
            while seen < rows {
                match source.poll_batch(max).await? {
                    Some(batch) => {
                        seen += batch.records.num_rows();
                        batches.push(batch.records);
                    }
                    None => tokio::time::sleep(Duration::from_millis(10)).await,
                }
            }
            Ok::<_, ConnectorError>(())
        })
        .await
        .map_err(|_| ConnectorError::Timeout(20_000))??;
        Ok(batches)
    }

    fn strings(batch: &RecordBatch, column: &str) -> Vec<String> {
        batch
            .column_by_name(column)
            .unwrap()
            .as_string::<i32>()
            .iter()
            .map(|value| value.unwrap_or_default().to_owned())
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn documents_become_rows_with_their_source_identity() {
        let directory = tempfile::tempdir().unwrap();
        let bytes = example("products/interest-rate-derivatives/ird-ex01-vanilla-swap.xml");
        std::fs::write(directory.path().join("swap.xml"), &bytes).unwrap();
        let mut source = start(config(directory.path()), SourcePosition::Initial)
            .await
            .unwrap();
        let batches = poll_rows(&mut source, 100, 1).await.unwrap();
        let batch = &batches[0];
        assert_eq!(strings(batch, "product_type"), ["swap"]);
        let sha = {
            use sha2::{Digest, Sha256};
            format!("{:x}", Sha256::digest(&bytes))
        };
        assert_eq!(strings(batch, "document_sha256"), [sha]);
        let metadata = batch.column_by_name("_metadata").unwrap().as_struct();
        assert!(metadata
            .column_by_name("file_path")
            .unwrap()
            .as_string::<i32>()
            .value(0)
            .ends_with("swap.xml"));
        assert!(source
            .checkpoint()
            .get_offset("manifest")
            .unwrap()
            .contains("swap.xml"));
        source.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_resumed_document_skips_its_committed_rows() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("package.xml"),
            example(
                "business-processes/clearing/msg-ex10-clearingConfirmed-clearing-performed.xml",
            ),
        )
        .unwrap();
        let mut source = start(config(directory.path()), SourcePosition::Initial)
            .await
            .unwrap();
        let first = poll_rows(&mut source, 1, 1).await.unwrap();
        assert_eq!(first[0].num_rows(), 1);
        let checkpoint = source.checkpoint();
        assert!(checkpoint.get_offset("file_progress").is_some());
        let locators: Vec<String> = strings(&first[0], "row_locator");
        source.close().await.unwrap();

        let mut resumed = start(
            config(directory.path()),
            SourcePosition::Resume {
                attempt: CheckpointAttempt::canonical(3),
                checkpoint,
            },
        )
        .await
        .unwrap();
        let rest = poll_rows(&mut resumed, 10, 2).await.unwrap();
        let mut remaining: Vec<String> = rest
            .iter()
            .flat_map(|batch| strings(batch, "row_locator"))
            .collect();
        remaining.sort();
        assert_eq!(remaining.len(), 2);
        assert!(
            !remaining.contains(&locators[0]),
            "{remaining:?} repeats {locators:?}"
        );
        let ordinals: Vec<u32> = rest
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("trade_ordinal")
                    .unwrap()
                    .as_primitive::<arrow_array::types::UInt32Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ordinals, [1, 2]);
        resumed.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_rejected_document_stops_the_source_without_progress() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("invalid.xml"),
            example("products/equity-swaps/eqs-ex07-long-form-with-stub.xml"),
        )
        .unwrap();
        let mut source = start(config(directory.path()), SourcePosition::Initial)
            .await
            .unwrap();
        let error = poll_rows(&mut source, 10, 1).await.unwrap_err();
        assert!(error.to_string().contains("schema"), "{error}");
        assert!(!error.to_string().contains("http://http://"), "{error}");
        assert!(!source
            .checkpoint()
            .get_offset("manifest")
            .unwrap_or_default()
            .contains("invalid.xml"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_oversized_document_is_rejected_before_it_is_read() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(
            directory.path().join("swap.xml"),
            example("products/interest-rate-derivatives/ird-ex01-vanilla-swap.xml"),
        )
        .unwrap();
        let mut limited = config(directory.path());
        limited.set("fpml.max.document.bytes", "1024");
        let mut source = start(limited, SourcePosition::Initial).await.unwrap();
        let error = poll_rows(&mut source, 10, 1).await.unwrap_err();
        assert!(
            error.to_string().contains("exceeds max_file_bytes 1024"),
            "{error}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_changed_profile_fails_closed_on_restore() {
        let directory = tempfile::tempdir().unwrap();
        let profile_directory = tempfile::tempdir().unwrap();
        let profile = profile_directory.path().join("profile.toml");
        let write_profile = |version: u32| {
            std::fs::write(
                &profile,
                format!(
                    "include = [\"bundled:fpml-5-13/confirmation/common.toml\"]\n\
                     [profile]\nname = \"custom\"\nversion = {version}\n\
                     artifact = \"fpml-5-13-8-rec-2/confirmation\"\nroots = \"documents\"\n\
                     rows = \"**/trade[tradeHeader]\"\n\
                     [[columns]]\nname = \"trade_date\"\ntype = \"date32\"\npath = \"tradeHeader/tradeDate\"\n"
                ),
            )
            .unwrap();
        };
        write_profile(1);
        let mut configured = config(directory.path());
        configured.set("fpml.profile.path", profile.to_string_lossy().to_string());
        let mut source = FileSource::new();
        let binding = source.resolve_schema(&configured, None).await.unwrap();
        configured.set_schema_binding(binding).unwrap();
        let mut source = start(configured.clone(), SourcePosition::Initial)
            .await
            .unwrap();
        let checkpoint = source.checkpoint();
        source.close().await.unwrap();

        write_profile(2);
        let error = start(
            configured,
            SourcePosition::Resume {
                attempt: CheckpointAttempt::canonical(4),
                checkpoint,
            },
        )
        .await
        .expect_err("a changed profile must not reinterpret retained input");
        assert!(
            matches!(error, ConnectorError::SchemaMismatch(_)),
            "{error}"
        );
    }
}

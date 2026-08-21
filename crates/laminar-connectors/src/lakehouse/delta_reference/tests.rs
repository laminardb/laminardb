use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};

use super::*;

fn declared_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
}

#[test]
fn construction_carries_declared_schema() {
    let source = DeltaReferenceTableSource::from_source_config(
        DeltaSourceConfig::new("/tmp/test_delta"),
        declared_schema(),
    );
    assert_eq!(source.declared_schema.field(0).name(), "id");
    assert!(!source.declared_schema.field(0).is_nullable());
}

#[test]
fn missing_table_path_is_rejected() {
    let config = ConnectorConfig::new("delta-lake");
    assert!(DeltaReferenceTableSource::from_connector_config(&config, declared_schema()).is_err());
}

#[tokio::test]
async fn close_is_idempotent_and_prevents_reads() {
    let mut source = DeltaReferenceTableSource::from_source_config(
        DeltaSourceConfig::new("/tmp/test_delta"),
        declared_schema(),
    );
    source.close().await.unwrap();
    source.close().await.unwrap();
    assert!(source.poll_snapshot().await.is_err());
}

mod integration {
    use std::collections::HashMap;

    use arrow_array::{Int64Array, StringArray};
    use deltalake::protocol::SaveMode;
    use tempfile::TempDir;

    use super::*;

    fn physical_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn declared_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    async fn write_table(path: &str) {
        use crate::lakehouse::delta_io;

        let schema = physical_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["one", "two"])),
            ],
        )
        .unwrap();
        let table = delta_io::open_or_create_table(path, HashMap::new(), Some(&schema))
            .await
            .unwrap();
        delta_io::write_batches(
            table,
            vec![batch],
            SaveMode::Append,
            None,
            false,
            None,
            None,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn snapshot_uses_declared_schema_and_exhausts() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().to_str().unwrap();
        write_table(path).await;
        let mut source = DeltaReferenceTableSource::from_source_config(
            DeltaSourceConfig::new(path),
            declared_schema(),
        );

        let batch = source.poll_snapshot().await.unwrap().unwrap();
        assert_eq!(batch.schema(), declared_schema());
        assert_eq!(batch.num_rows(), 2);
        assert!(source.poll_snapshot().await.unwrap().is_none());
        assert!(source.poll_snapshot().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn incompatible_declared_schema_fails_closed() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().to_str().unwrap();
        write_table(path).await;
        let incompatible = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut source = DeltaReferenceTableSource::from_source_config(
            DeltaSourceConfig::new(path),
            incompatible,
        );

        assert!(source.poll_snapshot().await.is_err());
    }
}

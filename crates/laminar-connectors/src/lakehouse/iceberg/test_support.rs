use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{NestedField, PartitionSpec, PrimitiveType, Schema, Transform, Type};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};

use crate::config::ConnectorConfig;
use crate::lakehouse::iceberg_config::IcebergSinkConfig;

use super::epoch_writer::{EpochIdentity, IcebergEpochWriter};
use super::metrics::IcebergMetrics;

pub(crate) struct TestTable {
    pub(crate) catalog: Arc<dyn Catalog>,
    pub(crate) table: iceberg::table::Table,
    pub(crate) config: IcebergSinkConfig,
}

pub(crate) async fn create_test_table(partitioned: bool) -> TestTable {
    let warehouse = format!("memory:///laminardb-test/{}", uuid::Uuid::now_v7());
    let catalog = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                "laminardb-test",
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.to_string(), warehouse.clone())]),
            )
            .await
            .unwrap(),
    );
    let namespace = NamespaceIdent::new("test".into());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .unwrap();
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "category", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap();
    let partition_spec = if partitioned {
        Some(
            PartitionSpec::builder(schema.clone())
                .add_partition_field("category", "category", Transform::Identity)
                .unwrap()
                .build()
                .unwrap(),
        )
    } else {
        None
    };
    let creation = match partition_spec {
        Some(spec) => TableCreation::builder()
            .name("events".to_string())
            .schema(schema)
            .partition_spec(spec)
            .build(),
        None => TableCreation::builder()
            .name("events".to_string())
            .schema(schema)
            .build(),
    };
    let table = catalog.create_table(&namespace, creation).await.unwrap();

    let mut connector = ConnectorConfig::new("iceberg");
    connector.set("catalog.uri", "http://catalog.invalid");
    connector.set("catalog.warehouse", warehouse);
    connector.set("namespace", "test");
    connector.set("table.name", "events");
    connector.set("storage.type", "fs");
    TestTable {
        catalog,
        table,
        config: IcebergSinkConfig::from_config(&connector).unwrap(),
    }
}

pub(crate) fn batch(table: &iceberg::table::Table, values: &[(i64, Option<&str>)]) -> RecordBatch {
    let schema =
        Arc::new(iceberg::arrow::schema_to_arrow_schema(&table.current_schema_ref()).unwrap());
    let ids = Arc::new(Int64Array::from_iter_values(
        values.iter().map(|(id, _)| *id),
    )) as ArrayRef;
    let categories = Arc::new(StringArray::from_iter(
        values.iter().map(|(_, category)| *category),
    )) as ArrayRef;
    RecordBatch::try_new(schema, vec![ids, categories]).unwrap()
}

pub(crate) fn table_ident() -> TableIdent {
    TableIdent::new(NamespaceIdent::new("test".into()), "events".into())
}

pub(crate) async fn append_rows(
    fixture: &TestTable,
    table: &iceberg::table::Table,
    epoch: u64,
    values: &[(i64, Option<&str>)],
) -> (iceberg::table::Table, Vec<String>) {
    append_batch(fixture, table, epoch, batch(table, values)).await
}

pub(crate) async fn append_batch(
    fixture: &TestTable,
    table: &iceberg::table::Table,
    epoch: u64,
    batch: RecordBatch,
) -> (iceberg::table::Table, Vec<String>) {
    let identity = EpochIdentity {
        deployment_id: "018f0000-0000-7000-8000-000000000001".into(),
        sink_id: "source-test".into(),
        participant_id: 1,
        epoch,
    };
    let mut writer =
        IcebergEpochWriter::new(table, &fixture.config, &identity, IcebergMetrics::new(None))
            .unwrap();
    writer.write(batch).await.unwrap();
    let output = writer.close().await.unwrap();
    let paths = output
        .data_files
        .iter()
        .map(|file| file.file_path().to_string())
        .collect();
    let tx = Transaction::new(table);
    let tx = tx
        .fast_append()
        .add_data_files(output.data_files)
        .apply(tx)
        .unwrap();
    let table = tx.commit(fixture.catalog.as_ref()).await.unwrap();
    (table, paths)
}

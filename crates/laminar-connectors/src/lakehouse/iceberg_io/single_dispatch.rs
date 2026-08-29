use std::collections::HashMap;
use std::fmt;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};

/// Prevents iceberg-rust's transaction layer from retrying catalog operations.
///
/// `LaminarDB` owns conflict refresh, compatibility validation, and ambiguous-outcome
/// reconciliation around each coordinated publication attempt. The transaction's mandatory
/// refresh therefore receives the already validated base table.
pub(crate) struct SingleDispatchCatalog<'a> {
    inner: &'a dyn Catalog,
    base: &'a Table,
}

impl<'a> SingleDispatchCatalog<'a> {
    pub(crate) fn new(inner: &'a dyn Catalog, base: &'a Table) -> Self {
        Self { inner, base }
    }
}

impl fmt::Debug for SingleDispatchCatalog<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SingleDispatchCatalog")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Catalog for SingleDispatchCatalog<'_> {
    async fn list_namespaces(
        &self,
        parent: Option<&NamespaceIdent>,
    ) -> iceberg::Result<Vec<NamespaceIdent>> {
        self.inner.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner.create_namespace(namespace, properties).await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner.namespace_exists(namespace).await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner.update_namespace(namespace, properties).await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner.create_table(namespace, creation).await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        if table != self.base.identifier() {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                "coordinated transaction requested an unvalidated table",
            ));
        }
        Ok(self.base.clone())
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.drop_table(table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.purge_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.table_exists(table).await
    }

    async fn rename_table(&self, source: &TableIdent, target: &TableIdent) -> iceberg::Result<()> {
        self.inner.rename_table(source, target).await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner.register_table(table, metadata_location).await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        self.inner
            .update_table(commit)
            .await
            .map_err(|error| error.with_retryable(false))
    }
}

#[cfg(test)]
mod tests {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};

    use super::*;

    #[tokio::test]
    async fn transaction_refresh_uses_the_validated_base() {
        let fixture = crate::lakehouse::iceberg::test_support::create_test_table(false).await;
        let transaction = Transaction::new(&fixture.table);
        let transaction = transaction
            .update_table_properties()
            .set("revision".into(), "new".into())
            .apply(transaction)
            .unwrap();
        let current = transaction.commit(fixture.catalog.as_ref()).await.unwrap();
        assert_eq!(
            current
                .metadata()
                .properties()
                .get("revision")
                .map(String::as_str),
            Some("new")
        );

        let catalog = SingleDispatchCatalog::new(fixture.catalog.as_ref(), &fixture.table);
        let refreshed = catalog
            .load_table(fixture.table.identifier())
            .await
            .unwrap();
        assert!(!refreshed.metadata().properties().contains_key("revision"));
    }
}

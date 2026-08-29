use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use iceberg_catalog_rest::CommitTableRequest;

use crate::lakehouse::iceberg_config::IcebergCatalogAuthType;

pub(super) struct RestCommitTransport {
    client: delta_reqwest::Client,
    auth_type: IcebergCatalogAuthType,
    effective_uri: String,
    properties: HashMap<String, String>,
    idempotency_key: uuid::Uuid,
}

impl RestCommitTransport {
    pub(super) fn new(
        client: delta_reqwest::Client,
        auth_type: IcebergCatalogAuthType,
        effective_uri: String,
        properties: HashMap<String, String>,
        idempotency_key: uuid::Uuid,
    ) -> Self {
        Self {
            client,
            auth_type,
            effective_uri,
            properties,
            idempotency_key,
        }
    }

    async fn update_table(
        &self,
        mut commit: TableCommit,
        catalog: &dyn Catalog,
    ) -> iceberg::Result<Table> {
        let identifier = commit.identifier().clone();
        let body = CommitTableRequest {
            identifier: Some(identifier.clone()),
            requirements: commit.take_requirements(),
            updates: commit.take_updates(),
        };
        let request = self
            .client
            .post(self.table_endpoint(&identifier))
            .header("Idempotency-Key", self.idempotency_key.to_string())
            .json(&body);
        let request = super::apply_request_properties(request, self.auth_type, &self.properties)
            .map_err(|_| Error::new(ErrorKind::DataInvalid, "invalid REST commit request"))?;
        let response = request
            .send()
            .await
            .map_err(|error| classify_dispatch_error(&error))?;
        match response.status() {
            delta_reqwest::StatusCode::OK => catalog.load_table(&identifier).await.map_err(|_| {
                Error::new(
                    ErrorKind::Unexpected,
                    "REST commit succeeded but refreshed table metadata was unavailable",
                )
                .with_retryable(true)
            }),
            delta_reqwest::StatusCode::NOT_FOUND => Err(Error::new(
                ErrorKind::TableNotFound,
                "REST commit target table does not exist",
            )),
            delta_reqwest::StatusCode::CONFLICT => Err(Error::new(
                ErrorKind::CatalogCommitConflicts,
                "REST commit requirements conflict with current table metadata",
            )
            .with_retryable(true)),
            status
                if status.is_server_error()
                    || status == delta_reqwest::StatusCode::REQUEST_TIMEOUT
                    || status == delta_reqwest::StatusCode::TOO_MANY_REQUESTS =>
            {
                Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "REST commit returned HTTP {}; commit outcome may be unknown",
                        status.as_u16()
                    ),
                )
                .with_retryable(true))
            }
            status => Err(Error::new(
                ErrorKind::DataInvalid,
                format!("REST commit was rejected with HTTP {}", status.as_u16()),
            )),
        }
    }

    fn table_endpoint(&self, table: &TableIdent) -> String {
        let prefix = self
            .properties
            .get("prefix")
            .map_or(String::new(), |prefix| format!("/{prefix}"));
        format!(
            "{}/v1{prefix}/namespaces/{}/tables/{}",
            self.effective_uri.trim_end_matches('/'),
            table.namespace.to_url_string(),
            table.name
        )
    }
}

fn classify_dispatch_error(error: &delta_reqwest::Error) -> Error {
    if error.is_builder() || error.is_connect() {
        Error::new(
            ErrorKind::DataInvalid,
            "REST commit could not be dispatched",
        )
    } else {
        Error::new(
            ErrorKind::Unexpected,
            "REST commit transport failed; commit outcome may be unknown",
        )
        .with_retryable(true)
    }
}

pub(super) struct RestCatalogFacade {
    inner: Arc<dyn Catalog>,
    commit: Option<RestCommitTransport>,
}

impl RestCatalogFacade {
    pub(super) fn read(inner: Arc<dyn Catalog>) -> Self {
        Self {
            inner,
            commit: None,
        }
    }

    pub(super) fn publication(inner: Arc<dyn Catalog>, commit: RestCommitTransport) -> Self {
        Self {
            inner,
            commit: Some(commit),
        }
    }
}

impl fmt::Debug for RestCatalogFacade {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RestCatalog")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl Catalog for RestCatalogFacade {
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
        self.inner.load_table(table).await
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
        match &self.commit {
            Some(transport) => transport.update_table(commit, self.inner.as_ref()).await,
            None => self.inner.update_table(commit).await,
        }
    }
}

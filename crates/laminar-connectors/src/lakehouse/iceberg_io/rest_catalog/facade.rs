use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use iceberg::table::Table;
use iceberg::{
    Catalog, Error, ErrorKind, Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use iceberg_catalog_rest::CommitTableRequest;

use super::oauth::{OAuthCatalogState, RestAuthentication};

pub(super) struct RestCommitTransport {
    client: delta_reqwest::Client,
    authentication: RestAuthentication,
    effective_uri: String,
    properties: HashMap<String, String>,
    request_timeout: Duration,
    idempotency_key: uuid::Uuid,
}

impl RestCommitTransport {
    pub(super) fn new(
        client: delta_reqwest::Client,
        authentication: RestAuthentication,
        effective_uri: String,
        properties: HashMap<String, String>,
        request_timeout: Duration,
        idempotency_key: uuid::Uuid,
    ) -> Self {
        Self {
            client,
            authentication,
            effective_uri,
            properties,
            request_timeout,
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
        let request = super::apply_request_properties(
            request,
            &self.authentication,
            &self.properties,
            self.request_timeout,
        )
        .await
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
    inner: RestCatalogInner,
    commit: Option<RestCommitTransport>,
}

enum RestCatalogInner {
    Static(Arc<dyn Catalog>),
    OAuth(Arc<OAuthCatalogState>),
}

impl RestCatalogInner {
    async fn current(&self) -> iceberg::Result<Arc<dyn Catalog>> {
        match self {
            Self::Static(catalog) => Ok(Arc::clone(catalog)),
            Self::OAuth(state) => state.current().await,
        }
    }
}

impl RestCatalogFacade {
    pub(super) fn read(inner: Arc<dyn Catalog>) -> Self {
        Self {
            inner: RestCatalogInner::Static(inner),
            commit: None,
        }
    }

    pub(super) fn oauth(state: OAuthCatalogState) -> Self {
        Self {
            inner: RestCatalogInner::OAuth(Arc::new(state)),
            commit: None,
        }
    }

    pub(super) fn publication(inner: Arc<dyn Catalog>, commit: RestCommitTransport) -> Self {
        Self {
            inner: RestCatalogInner::Static(inner),
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
        self.inner.current().await?.list_namespaces(parent).await
    }

    async fn create_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<Namespace> {
        self.inner
            .current()
            .await?
            .create_namespace(namespace, properties)
            .await
    }

    async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
        self.inner.current().await?.get_namespace(namespace).await
    }

    async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
        self.inner
            .current()
            .await?
            .namespace_exists(namespace)
            .await
    }

    async fn update_namespace(
        &self,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> iceberg::Result<()> {
        self.inner
            .current()
            .await?
            .update_namespace(namespace, properties)
            .await
    }

    async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
        self.inner.current().await?.drop_namespace(namespace).await
    }

    async fn list_tables(&self, namespace: &NamespaceIdent) -> iceberg::Result<Vec<TableIdent>> {
        self.inner.current().await?.list_tables(namespace).await
    }

    async fn create_table(
        &self,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> iceberg::Result<Table> {
        self.inner
            .current()
            .await?
            .create_table(namespace, creation)
            .await
    }

    async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
        self.inner.current().await?.load_table(table).await
    }

    async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.current().await?.drop_table(table).await
    }

    async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
        self.inner.current().await?.purge_table(table).await
    }

    async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
        self.inner.current().await?.table_exists(table).await
    }

    async fn rename_table(&self, source: &TableIdent, target: &TableIdent) -> iceberg::Result<()> {
        self.inner
            .current()
            .await?
            .rename_table(source, target)
            .await
    }

    async fn register_table(
        &self,
        table: &TableIdent,
        metadata_location: String,
    ) -> iceberg::Result<Table> {
        self.inner
            .current()
            .await?
            .register_table(table, metadata_location)
            .await
    }

    async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
        let inner = self.inner.current().await?;
        match &self.commit {
            Some(transport) => transport.update_table(commit, inner.as_ref()).await,
            None => inner.update_table(commit).await,
        }
    }
}

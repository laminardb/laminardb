//! Table resolution through the Unity Catalog API: location, table kind, and table-scoped
//! temporary storage credentials, using the delta-rs Unity client (PAT or OAuth M2M).

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

use deltalake::unity_catalog::models::{
    DataSourceFormat, GetTableResponse, Table, TableTempCredentialsResponse, TableType,
};
use deltalake::unity_catalog::{UnityCatalog, UnityCatalogBuilder};
use tracing::info;

use super::super::delta_config::{UnityAuth, UnityCatalogConfig, UnityStorageCredentials};
use super::super::delta_io::CatalogAccess;
use crate::error::ConnectorError;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// A Unity Catalog table resolved for one access mode.
pub(crate) struct ResolvedUnityTable {
    pub(crate) storage_location: String,
    /// Object-store options carrying vended credentials; empty for configured credentials.
    pub(crate) vended_options: HashMap<String, String>,
    pub(crate) expires_at: Option<SystemTime>,
}

fn client(config: &UnityCatalogConfig) -> Result<UnityCatalog, ConnectorError> {
    let url = url::Url::parse(&config.workspace_url).map_err(|_| {
        ConnectorError::ConfigurationError("catalog.workspace_url is not a valid URL".into())
    })?;
    let loopback = url.host().is_some_and(|host| match host {
        url::Host::Domain(name) => name.eq_ignore_ascii_case("localhost"),
        url::Host::Ipv4(ip) => ip.is_loopback(),
        url::Host::Ipv6(ip) => ip.is_loopback(),
    });
    let allow_http = match url.scheme() {
        "https" => false,
        "http" if loopback => true,
        _ => {
            return Err(ConnectorError::ConfigurationError(
                "catalog.workspace_url must use https (http is accepted only for a loopback \
                 open-source Unity Catalog server)"
                    .into(),
            ))
        }
    };
    let mut options = vec![
        ("workspace_url", config.workspace_url.clone()),
        ("allow_http_url", allow_http.to_string()),
    ];
    match &config.auth {
        UnityAuth::Token(token) => options.push(("access_token", token.clone())),
        UnityAuth::OAuth {
            client_id,
            client_secret,
        } => {
            options.push(("client_id", client_id.clone()));
            options.push(("client_secret", client_secret.clone()));
        }
        UnityAuth::Unset => {
            return Err(ConnectorError::ConfigurationError(
                "Unity Catalog credentials are not configured".into(),
            ))
        }
    }
    UnityCatalogBuilder::builder()
        .build()
        .try_with_options(options)
        .and_then(UnityCatalogBuilder::build)
        .map_err(|_| {
            ConnectorError::ConfigurationError(
                "Unity Catalog client configuration is invalid; verify catalog.workspace_url \
                 and its credentials"
                    .into(),
            )
        })
}

fn table_name(table_path: &str) -> Result<[&str; 3], ConnectorError> {
    let name = table_path.strip_prefix("uc://").unwrap_or(table_path);
    let mut parts = name.split('.');
    match (parts.next(), parts.next(), parts.next(), parts.next()) {
        (Some(catalog), Some(schema), Some(table), None)
            if !catalog.is_empty() && !schema.is_empty() && !table.is_empty() =>
        {
            Ok([catalog, schema, table])
        }
        _ => Err(ConnectorError::ConfigurationError(
            "Unity Catalog tables are addressed as uc://<catalog>.<schema>.<table>".into(),
        )),
    }
}

async fn bounded<T>(
    context: &str,
    request: impl std::future::Future<Output = Result<T, deltalake::unity_catalog::UnityCatalogError>>,
) -> Result<T, ConnectorError> {
    tokio::time::timeout(REQUEST_TIMEOUT, request)
        .await
        .map_err(|_| {
            ConnectorError::ConnectionFailed(format!(
                "Unity Catalog {context} exceeded {REQUEST_TIMEOUT:?}"
            ))
        })?
        .map_err(|_| {
            ConnectorError::ConnectionFailed(format!(
                "Unity Catalog {context} failed; verify catalog.workspace_url and its \
                 credentials"
            ))
        })
}

fn api_error(context: &str, code: &str) -> ConnectorError {
    match code {
        "PERMISSION_DENIED" | "UNAUTHENTICATED" | "INVALID_STATE" => {
            ConnectorError::ConfigurationError(format!(
                "Unity Catalog {context} was refused ({code}); verify the principal's grants"
            ))
        }
        _ => ConnectorError::ConnectionFailed(format!("Unity Catalog {context} failed ({code})")),
    }
}

/// The catalog's table entry, or `None` when it does not exist.
pub(crate) async fn find_table(
    config: &UnityCatalogConfig,
    table_path: &str,
) -> Result<Option<Table>, ConnectorError> {
    let [catalog, schema, table] = table_name(table_path)?;
    let response = bounded(
        "table lookup",
        client(config)?.get_table(catalog, schema, table),
    )
    .await?;
    match response {
        GetTableResponse::Success(table) => Ok(Some(table)),
        GetTableResponse::Error(error)
            if matches!(
                error.error_code.as_str(),
                "TABLE_DOES_NOT_EXIST" | "NOT_FOUND" | "RESOURCE_DOES_NOT_EXIST"
            ) =>
        {
            Ok(None)
        }
        GetTableResponse::Error(error) => Err(api_error("table lookup", &error.error_code)),
    }
}

/// Resolve the table's location and, for vended credentials, temporary storage credentials
/// scoped to the table and to `access`. Unsupported table kinds fail before any storage access.
pub(crate) async fn resolve_table(
    config: &UnityCatalogConfig,
    table_path: &str,
    access: CatalogAccess,
) -> Result<ResolvedUnityTable, ConnectorError> {
    let table = find_table(config, table_path).await?.ok_or_else(|| {
        ConnectorError::ConfigurationError(
            "Unity Catalog table is absent; create it separately or explicitly authorize target \
             preparation"
                .into(),
        )
    })?;
    validate_table(&table, access, config.storage_credentials)?;
    let (vended_options, expires_at) = match config.storage_credentials {
        UnityStorageCredentials::Configured => (HashMap::new(), None),
        UnityStorageCredentials::Vended => vend(config, table_path, access).await?,
    };
    info!(
        access = ?access,
        table_type = ?table.table_type,
        credentials = ?config.storage_credentials,
        "resolved Unity Catalog table"
    );
    Ok(ResolvedUnityTable {
        storage_location: table.storage_location,
        vended_options,
        expires_at,
    })
}

/// External Delta tables are readable and writable. Managed tables are readable through vended
/// credentials only; their commits belong to the catalog (catalog-managed commits), which the
/// pinned delta-rs writer cannot perform.
pub(crate) fn validate_table(
    table: &Table,
    access: CatalogAccess,
    credentials: UnityStorageCredentials,
) -> Result<(), ConnectorError> {
    if !matches!(table.data_source_format, DataSourceFormat::Delta) {
        return Err(ConnectorError::ConfigurationError(format!(
            "Unity Catalog table format {:?} is not Delta",
            table.data_source_format
        )));
    }
    match (&table.table_type, access, credentials) {
        (TableType::External, _, _)
        | (TableType::Managed, CatalogAccess::Read, UnityStorageCredentials::Vended) => Ok(()),
        (TableType::Managed, CatalogAccess::Read, UnityStorageCredentials::Configured) => {
            Err(ConnectorError::ConfigurationError(
                "Unity Catalog managed tables are read only through vended credentials; set \
                 catalog.storage.credentials=vended"
                    .into(),
            ))
        }
        (TableType::Managed, CatalogAccess::Write, _) => Err(ConnectorError::ConfigurationError(
            "Unity Catalog managed tables accept commits only through the catalog \
             (catalog-managed commits), which the pinned delta-rs writer does not implement; \
             write to an external table"
                .into(),
        )),
        (other, _, _) => Err(ConnectorError::ConfigurationError(format!(
            "Unity Catalog {other:?} objects are not Delta tables LaminarDB can read or write"
        ))),
    }
}

async fn vend(
    config: &UnityCatalogConfig,
    table_path: &str,
    access: CatalogAccess,
) -> Result<(HashMap<String, String>, Option<SystemTime>), ConnectorError> {
    let [catalog, schema, table] = table_name(table_path)?;
    let operation = match access {
        CatalogAccess::Read => "READ",
        CatalogAccess::Write => "READ_WRITE",
    };
    let response = bounded(
        "credential vending",
        client(config)?
            .get_temp_table_credentials_with_permission(catalog, schema, table, operation),
    )
    .await?;
    let credentials = match response {
        TableTempCredentialsResponse::Success(credentials) => credentials,
        TableTempCredentialsResponse::Error(error) => {
            return Err(ConnectorError::ConfigurationError(format!(
                "Unity Catalog refused {operation} credential vending ({}); enable external \
                 data access and grant EXTERNAL USE SCHEMA, or set \
                 catalog.storage.credentials=configured to use the connector's own storage \
                 identity",
                error.error_code
            )))
        }
    };
    let expires_at = u64::try_from(credentials.expiration_time.timestamp_millis())
        .ok()
        .and_then(|millis| SystemTime::UNIX_EPOCH.checked_add(Duration::from_millis(millis)))
        .ok_or_else(|| {
            ConnectorError::ConnectionFailed(
                "Unity Catalog vended credentials have an invalid expiration time".into(),
            )
        })?;
    let options = credentials.get_credentials().ok_or_else(|| {
        ConnectorError::ConfigurationError(
            "Unity Catalog vended credentials for a storage provider this build does not \
             include; enable the matching delta-lake-s3/azure/gcs feature"
                .into(),
        )
    })?;
    Ok((options, Some(expires_at)))
}

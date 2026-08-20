//! Catalog-specific table URI and storage-option resolution.

#[cfg(feature = "delta-lake-glue")]
use super::info;
use super::{ConnectorError, HashMap};

/// Resolves catalog-aware table URI and merges catalog-specific storage options.
///
/// - `None`: returns table path and storage options as-is.
/// - `Glue`: calls AWS Glue API to resolve the table's S3 location.
/// - `Unity`: injects workspace URL and access token into storage options.
///
/// # Errors
///
/// Returns `ConnectorError` if catalog resolution fails.
#[cfg(feature = "delta-lake")]
#[allow(clippy::implicit_hasher, clippy::unused_async)]
pub async fn resolve_catalog_options(
    catalog: &super::super::delta_config::DeltaCatalogType,
    #[allow(unused_variables)] catalog_database: Option<&str>,
    #[allow(unused_variables)] catalog_name: Option<&str>,
    _catalog_schema: Option<&str>,
    table_path: &str,
    base_storage_options: &HashMap<String, String>,
) -> Result<(String, HashMap<String, String>), ConnectorError> {
    use super::super::delta_config::DeltaCatalogType;

    match catalog {
        DeltaCatalogType::None => Ok((table_path.to_string(), base_storage_options.clone())),
        #[cfg(feature = "delta-lake-glue")]
        DeltaCatalogType::Glue => {
            use deltalake::DataCatalog;
            let database = catalog_database.ok_or_else(|| {
                ConnectorError::ConfigurationError(
                    "Glue catalog requires 'catalog.database'".into(),
                )
            })?;
            let glue = deltalake_catalog_glue::GlueDataCatalog::from_env()
                .await
                .map_err(|e| {
                    ConnectorError::ConnectionFailed(format!("failed to init Glue catalog: {e}"))
                })?;
            let resolved = glue
                .get_table_storage_location(catalog_name.map(String::from), database, table_path)
                .await
                .map_err(|e| {
                    ConnectorError::ConnectionFailed(format!(
                        "Glue catalog lookup failed for '{database}.{table_path}': {e}"
                    ))
                })?;
            info!(
                glue_database = database,
                table = table_path,
                resolved_path = %resolved,
                "resolved table path via Glue catalog"
            );
            Ok((resolved, base_storage_options.clone()))
        }
        #[cfg(not(feature = "delta-lake-glue"))]
        DeltaCatalogType::Glue => Err(ConnectorError::ConfigurationError(
            "Glue catalog requires the 'delta-lake-glue' feature. \
             Build with: cargo build --features delta-lake-glue"
                .into(),
        )),
        #[cfg(feature = "delta-lake-unity")]
        DeltaCatalogType::Unity {
            workspace_url,
            access_token,
        } => {
            // Resolve the table's actual storage location from Unity Catalog
            // via REST API, then return that direct path (s3://, az://, gs://)
            // instead of the uc:// URI. This bypasses delta-rs's built-in
            // uc:// handling which requires credential vending — a feature
            // that is denied outside Databricks compute environments.
            let full_name = table_path.strip_prefix("uc://").unwrap_or(table_path);

            let storage_location = super::super::unity_catalog::get_table_storage_location(
                workspace_url,
                access_token,
                full_name,
            )
            .await?;

            Ok((storage_location, base_storage_options.clone()))
        }
        #[cfg(not(feature = "delta-lake-unity"))]
        DeltaCatalogType::Unity { .. } => Err(ConnectorError::ConfigurationError(
            "Unity catalog requires the 'delta-lake-unity' feature. \
             Build with: cargo build --features delta-lake-unity"
                .into(),
        )),
    }
}

// ============================================================================
// Integration tests (require delta-lake feature)
// ============================================================================

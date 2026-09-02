//! Catalog-specific table URI and storage-option resolution.

#[cfg(feature = "delta-lake-glue")]
use super::info;
use super::{ConnectorError, HashMap};
#[cfg(feature = "delta-lake-glue")]
use laminar_core::storage_location::StorageProvider;

#[cfg(feature = "delta-lake-glue")]
fn glue_init_error_category(error: &deltalake_catalog_glue::GlueError) -> &'static str {
    match error {
        deltalake_catalog_glue::GlueError::MissingMetadata { .. } => "missing-metadata",
        deltalake_catalog_glue::GlueError::AWSError { source } => aws_glue_error_category(source),
    }
}

#[cfg(feature = "delta-lake-glue")]
fn aws_glue_error_category(error: &aws_sdk_glue::Error) -> &'static str {
    use aws_sdk_glue::error::ProvideErrorMetadata;
    use aws_sdk_glue::Error;

    match error {
        Error::AccessDeniedException(_)
        | Error::KmsKeyNotAccessibleFault(_)
        | Error::PermissionTypeMismatchException(_) => "authorization",
        Error::EntityNotFoundException(_)
        | Error::IntegrationNotFoundFault(_)
        | Error::ResourceNotFoundException(_)
        | Error::TargetResourceNotFound(_) => "not-found",
        Error::ConcurrentRunsExceededException(_)
        | Error::IntegrationQuotaExceededFault(_)
        | Error::ResourceNumberLimitExceededException(_)
        | Error::ThrottlingException(_) => "throttled",
        Error::OperationTimeoutException(_) => "service-timeout",
        Error::FederationSourceRetryableException(_)
        | Error::InternalServerException(_)
        | Error::InternalServiceException(_)
        | Error::ResourceNotReadyException(_) => "service-unavailable",
        Error::InvalidInputException(_) | Error::ValidationException(_) => "invalid-request",
        Error::AlreadyExistsException(_)
        | Error::ConcurrentModificationException(_)
        | Error::ConflictException(_)
        | Error::VersionMismatchException(_) => "catalog-conflict",
        error if error.code().is_none() => "client-or-transport",
        _ => "unclassified-service-error",
    }
}

#[cfg(feature = "delta-lake-glue")]
fn glue_lookup_error_category(error: &deltalake::data_catalog::DataCatalogError) -> &'static str {
    match error {
        deltalake::data_catalog::DataCatalogError::Generic { source, .. } => source
            .downcast_ref::<deltalake_catalog_glue::GlueError>()
            .map_or("catalog-provider", glue_init_error_category),
        deltalake::data_catalog::DataCatalogError::InvalidDataCatalog { .. } => "invalid-catalog",
        deltalake::data_catalog::DataCatalogError::UnknownConfigKey { .. } => {
            "invalid-catalog-configuration"
        }
        deltalake::data_catalog::DataCatalogError::RequestError { .. } => "catalog-request",
    }
}

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
                .map_err(|error| {
                    ConnectorError::ConnectionFailed(
                        format!(
                            "failed to initialize Glue catalog using the downstream AWS credential chain ({}); verify AWS region and credential-chain setup",
                            glue_init_error_category(&error)
                        ),
                    )
                })?;
            let resolved = glue
                .get_table_storage_location(catalog_name.map(String::from), database, table_path)
                .await
                .map_err(|error| {
                    ConnectorError::ConnectionFailed(
                        format!(
                            "Glue catalog lookup failed ({}); verify catalog identifiers, AWS region, credentials, and glue:GetTable permission",
                            glue_lookup_error_category(&error)
                        ),
                    )
                })?;
            info!(
                glue_database = database,
                storage_provider =
                    StorageProvider::detect_uri(&resolved).map_or("unknown", StorageProvider::name),
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

#[cfg(all(test, feature = "delta-lake-glue"))]
mod tests {
    use super::*;

    #[test]
    fn glue_categories_are_actionable_without_provider_details() {
        let error: deltalake::data_catalog::DataCatalogError =
            deltalake_catalog_glue::GlueError::MissingMetadata {
                metadata: "secret-table-location".into(),
            }
            .into();
        assert_eq!(glue_lookup_error_category(&error), "missing-metadata");
        assert!(!glue_lookup_error_category(&error).contains("secret"));

        let error = deltalake_catalog_glue::GlueError::AWSError {
            source: aws_sdk_glue::Error::AccessDeniedException(
                aws_sdk_glue::types::error::AccessDeniedException::builder()
                    .message("private provider detail")
                    .build(),
            ),
        };
        assert_eq!(glue_init_error_category(&error), "authorization");
        assert!(!glue_init_error_category(&error).contains("private"));

        let unhandled = aws_sdk_glue::types::TableInput::builder()
            .build()
            .expect_err("missing table name must produce a client-side build error");
        let error = deltalake_catalog_glue::GlueError::AWSError {
            source: unhandled.into(),
        };
        assert_eq!(glue_init_error_category(&error), "client-or-transport");
    }
}

// ============================================================================
// Integration tests (require delta-lake feature)
// ============================================================================

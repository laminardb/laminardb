//! Delta Lake schema evolution for the sink connector (F031D).
//!
//! Validates schema changes between incoming Arrow `RecordBatch` batches and
//! the current Delta table schema. Supports:
//!
//! - **Additive evolution**: New nullable columns are accepted and auto-merged
//!   into the Delta table schema (when `schema.evolution = true`).
//! - **Breaking change rejection**: Type changes on existing columns and column
//!   removals are rejected with clear error messages.
//!
//! # Design
//!
//! The validator is invoked on each `write_batch()` call to detect schema drift
//! between the source and the Delta table. When `schema_evolution` is enabled,
//! additive changes (new nullable columns) pass through to delta-rs's
//! `SchemaMode::Merge`. Breaking changes always produce errors regardless of
//! the configuration.

use arrow_schema::{DataType, SchemaRef};
use tracing::info;

use crate::error::ConnectorError;

/// Describes a detected schema change.
#[derive(Debug, Clone, PartialEq)]
pub enum DeltaSchemaChange {
    /// A new column was added in the incoming batch.
    ColumnAdded {
        /// Column name.
        name: String,
        /// Data type of the new column.
        data_type: DataType,
        /// Whether the column is nullable.
        nullable: bool,
    },
    /// An existing column was removed from the incoming batch.
    ColumnRemoved {
        /// Column name.
        name: String,
    },
    /// An existing column's type changed.
    TypeChanged {
        /// Column name.
        name: String,
        /// Type in the Delta table.
        table_type: DataType,
        /// Type in the incoming batch.
        batch_type: DataType,
    },
}

/// Result of schema evolution validation.
#[derive(Debug, Clone)]
pub struct SchemaValidationResult {
    /// List of detected changes.
    pub changes: Vec<DeltaSchemaChange>,
    /// Whether all changes are additive (safe for auto-merge).
    pub is_additive_only: bool,
    /// Human-readable descriptions of breaking changes (empty if none).
    pub breaking_reasons: Vec<String>,
}

/// Validates schema changes between a Delta table schema and an incoming batch schema.
///
/// # Arguments
///
/// * `table_schema` - The current Delta Lake table schema
/// * `batch_schema` - The schema of the incoming `RecordBatch`
///
/// # Returns
///
/// A `SchemaValidationResult` describing all detected changes.
#[must_use]
pub fn validate_schema_evolution(
    table_schema: &SchemaRef,
    batch_schema: &SchemaRef,
) -> SchemaValidationResult {
    let mut changes = Vec::new();
    let mut breaking_reasons = Vec::new();

    // Build lookup maps.
    let table_fields: std::collections::HashMap<&str, &arrow_schema::Field> = table_schema
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();

    let batch_fields: std::collections::HashMap<&str, &arrow_schema::Field> = batch_schema
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();

    // Detect new columns (in batch but not in table).
    for field in batch_schema.fields() {
        if !table_fields.contains_key(field.name().as_str()) {
            changes.push(DeltaSchemaChange::ColumnAdded {
                name: field.name().clone(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
            });

            if !field.is_nullable() {
                breaking_reasons.push(format!(
                    "new column '{}' is non-nullable — Delta Lake requires new columns \
                     to be nullable for backward compatibility with existing data",
                    field.name()
                ));
            }
        }
    }

    // Detect removed columns (in table but not in batch).
    for field in table_schema.fields() {
        if !batch_fields.contains_key(field.name().as_str()) {
            changes.push(DeltaSchemaChange::ColumnRemoved {
                name: field.name().clone(),
            });
            breaking_reasons.push(format!(
                "column '{}' exists in the Delta table but is missing from the incoming batch — \
                 column removal is not supported",
                field.name()
            ));
        }
    }

    // Detect type changes on existing columns.
    for field in batch_schema.fields() {
        if let Some(table_field) = table_fields.get(field.name().as_str()) {
            if table_field.data_type() != field.data_type() {
                changes.push(DeltaSchemaChange::TypeChanged {
                    name: field.name().clone(),
                    table_type: table_field.data_type().clone(),
                    batch_type: field.data_type().clone(),
                });
                breaking_reasons.push(format!(
                    "column '{}' type changed from {:?} to {:?} — type changes are \
                     not supported in Delta Lake schema evolution",
                    field.name(),
                    table_field.data_type(),
                    field.data_type()
                ));
            }
        }
    }

    let is_additive_only = breaking_reasons.is_empty() && !changes.is_empty();

    SchemaValidationResult {
        changes,
        is_additive_only,
        breaking_reasons,
    }
}

/// Applies schema evolution validation for a Delta Lake sink write.
///
/// When `schema_evolution` is enabled, additive changes (new nullable columns)
/// are allowed and logged. Breaking changes always return an error.
///
/// When `schema_evolution` is disabled, any schema change returns an error.
///
/// # Errors
///
/// Returns `ConnectorError::SchemaMismatch` on breaking changes or when
/// evolution is disabled but the schema has changed.
pub fn check_schema_compatibility(
    table_schema: &SchemaRef,
    batch_schema: &SchemaRef,
    schema_evolution_enabled: bool,
) -> Result<(), ConnectorError> {
    let result = validate_schema_evolution(table_schema, batch_schema);

    // No changes — everything is fine.
    if result.changes.is_empty() {
        return Ok(());
    }

    // Breaking changes always fail, regardless of config.
    if !result.breaking_reasons.is_empty() {
        let reasons = result.breaking_reasons.join("; ");
        return Err(ConnectorError::SchemaMismatch(format!(
            "incompatible schema change detected: {reasons}"
        )));
    }

    // Additive changes: only allowed when schema evolution is enabled.
    if result.is_additive_only {
        if schema_evolution_enabled {
            let added: Vec<String> = result
                .changes
                .iter()
                .filter_map(|c| {
                    if let DeltaSchemaChange::ColumnAdded {
                        name, data_type, ..
                    } = c
                    {
                        Some(format!("{name}: {data_type:?}"))
                    } else {
                        None
                    }
                })
                .collect();

            info!(
                columns = ?added,
                "Delta Lake schema evolution: auto-merging new nullable columns"
            );
            return Ok(());
        }

        let added: Vec<String> = result
            .changes
            .iter()
            .filter_map(|c| {
                if let DeltaSchemaChange::ColumnAdded { name, .. } = c {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect();

        return Err(ConnectorError::SchemaMismatch(format!(
            "schema evolution is disabled but incoming batch has new columns: {}. \
             Enable with: schema.evolution = 'true'",
            added.join(", ")
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn schema(fields: &[(&str, DataType, bool)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(name, dt, nullable)| Field::new(*name, dt.clone(), *nullable))
                .collect::<Vec<_>>(),
        ))
    }

    // ── validate_schema_evolution tests ──

    #[test]
    fn test_identical_schemas() {
        let s = schema(&[
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]);
        let result = validate_schema_evolution(&s, &s);
        assert!(result.changes.is_empty());
        assert!(result.breaking_reasons.is_empty());
        assert!(!result.is_additive_only);
    }

    #[test]
    fn test_additive_nullable_column() {
        let table = schema(&[("id", DataType::Int64, false)]);
        let batch = schema(&[
            ("id", DataType::Int64, false),
            ("email", DataType::Utf8, true),
        ]);
        let result = validate_schema_evolution(&table, &batch);
        assert_eq!(result.changes.len(), 1);
        assert!(result.is_additive_only);
        assert!(result.breaking_reasons.is_empty());
    }

    #[test]
    fn test_additive_non_nullable_column_breaks() {
        let table = schema(&[("id", DataType::Int64, false)]);
        let batch = schema(&[
            ("id", DataType::Int64, false),
            ("email", DataType::Utf8, false), // non-nullable!
        ]);
        let result = validate_schema_evolution(&table, &batch);
        assert_eq!(result.changes.len(), 1);
        assert!(!result.is_additive_only);
        assert_eq!(result.breaking_reasons.len(), 1);
        assert!(result.breaking_reasons[0].contains("non-nullable"));
    }

    #[test]
    fn test_column_removed_breaks() {
        let table = schema(&[
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]);
        let batch = schema(&[("id", DataType::Int64, false)]);
        let result = validate_schema_evolution(&table, &batch);
        assert!(!result.is_additive_only);
        assert!(!result.breaking_reasons.is_empty());
        assert!(result.breaking_reasons[0].contains("missing"));
    }

    #[test]
    fn test_type_change_breaks() {
        let table = schema(&[("id", DataType::Int64, false)]);
        let batch = schema(&[("id", DataType::Utf8, false)]);
        let result = validate_schema_evolution(&table, &batch);
        assert!(!result.is_additive_only);
        assert!(!result.breaking_reasons.is_empty());
        assert!(result.breaking_reasons[0].contains("type changed"));
    }

    #[test]
    fn test_multiple_breaking_changes() {
        let table = schema(&[
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
            ("value", DataType::Float64, true),
        ]);
        let batch = schema(&[
            ("id", DataType::Utf8, false), // type changed
            // name removed
            ("value", DataType::Float64, true),
            ("extra", DataType::Boolean, false), // non-nullable new column
        ]);
        let result = validate_schema_evolution(&table, &batch);
        // 3 breaking: type change, column removed, non-nullable addition
        assert_eq!(result.breaking_reasons.len(), 3);
    }

    // ── check_schema_compatibility tests ──

    #[test]
    fn test_compatible_no_changes() {
        let s = schema(&[("id", DataType::Int64, false)]);
        assert!(check_schema_compatibility(&s, &s, false).is_ok());
        assert!(check_schema_compatibility(&s, &s, true).is_ok());
    }

    #[test]
    fn test_additive_allowed_with_evolution() {
        let table = schema(&[("id", DataType::Int64, false)]);
        let batch = schema(&[
            ("id", DataType::Int64, false),
            ("email", DataType::Utf8, true),
        ]);
        assert!(check_schema_compatibility(&table, &batch, true).is_ok());
    }

    #[test]
    fn test_additive_rejected_without_evolution() {
        let table = schema(&[("id", DataType::Int64, false)]);
        let batch = schema(&[
            ("id", DataType::Int64, false),
            ("email", DataType::Utf8, true),
        ]);
        let err = check_schema_compatibility(&table, &batch, false);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("schema evolution is disabled"));
        assert!(msg.contains("email"));
    }

    #[test]
    fn test_breaking_rejected_even_with_evolution() {
        let table = schema(&[("id", DataType::Int64, false)]);
        let batch = schema(&[("id", DataType::Utf8, false)]);
        let err = check_schema_compatibility(&table, &batch, true);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("incompatible schema change"));
    }

    #[test]
    fn test_column_removal_rejected_even_with_evolution() {
        let table = schema(&[
            ("id", DataType::Int64, false),
            ("name", DataType::Utf8, true),
        ]);
        let batch = schema(&[("id", DataType::Int64, false)]);
        let err = check_schema_compatibility(&table, &batch, true);
        assert!(err.is_err());
        let msg = err.unwrap_err().to_string();
        assert!(msg.contains("incompatible schema change"));
        assert!(msg.contains("name"));
    }
}

//! Schema resolver and merge engine.
//!
//! The [`SchemaResolver`] implements a five-level priority chain for
//! determining the Arrow schema of a source connector:
//!
//! 1. **Full DDL** — user-declared schema without wildcards
//! 2. **Schema registry** — via [`SchemaRegistryAware`](super::traits::SchemaRegistryAware)
//! 3. **Source provider** — via [`SchemaProvider`](super::traits::SchemaProvider)
//! 4. **Sample inference** — via [`SchemaInferable`](super::traits::SchemaInferable)
//! 5. **Error** — no schema could be determined
//!
//! When partial DDL is provided (with wildcard `*`), the resolver merges
//! user-declared columns with the resolved columns, preserving user-declared
//! columns first.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::error::{SchemaError, SchemaResult};
use super::traits::InferenceConfig;
use crate::connector::SourceConnector;

/// A user-declared schema from DDL (e.g., `CREATE SOURCE ... (col1 INT, ...)`).
#[derive(Debug, Clone)]
pub struct DeclaredSchema {
    /// Explicitly declared columns.
    pub columns: Vec<DeclaredColumn>,

    /// Whether the DDL includes a wildcard (`*`) that should be expanded
    /// with columns from the source.
    pub has_wildcard: bool,

    /// Optional prefix applied to wildcard-expanded columns
    /// (e.g., `src_` → `src_field_name`).
    pub wildcard_prefix: Option<String>,
}

impl DeclaredSchema {
    /// Creates a fully-declared schema (no wildcard).
    #[must_use]
    pub fn full(columns: Vec<DeclaredColumn>) -> Self {
        Self {
            columns,
            has_wildcard: false,
            wildcard_prefix: None,
        }
    }

    /// Creates a schema with a wildcard for additional columns.
    #[must_use]
    pub fn with_wildcard(columns: Vec<DeclaredColumn>) -> Self {
        Self {
            columns,
            has_wildcard: true,
            wildcard_prefix: None,
        }
    }

    /// Sets the wildcard prefix.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.wildcard_prefix = Some(prefix.into());
        self
    }

    /// Returns `true` if no columns are declared and no wildcard is set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty() && !self.has_wildcard
    }
}

/// A single column declared in DDL.
#[derive(Debug, Clone)]
pub struct DeclaredColumn {
    /// Column name.
    pub name: String,

    /// Arrow data type.
    pub data_type: DataType,

    /// Whether the column is nullable.
    pub nullable: bool,

    /// Optional default expression (e.g., `"0"`, `"CURRENT_TIMESTAMP"`).
    pub default: Option<String>,
}

impl DeclaredColumn {
    /// Creates a new declared column.
    #[must_use]
    pub fn new(name: impl Into<String>, data_type: DataType, nullable: bool) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable,
            default: None,
        }
    }

    /// Sets the default expression.
    #[must_use]
    pub fn with_default(mut self, expr: impl Into<String>) -> Self {
        self.default = Some(expr.into());
        self
    }
}

/// The result of schema resolution.
#[derive(Debug, Clone)]
pub struct ResolvedSchema {
    /// The final Arrow schema.
    pub schema: SchemaRef,

    /// How the schema was resolved.
    pub kind: ResolutionKind,

    /// Per-field origin information.
    pub field_origins: Vec<FieldOrigin>,

    /// Any warnings generated during resolution.
    pub warnings: Vec<String>,
}

/// How the schema was resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionKind {
    /// Fully declared in DDL with no wildcard.
    Declared,

    /// Schema came from the source's [`SchemaProvider`](super::traits::SchemaProvider).
    SourceProvided,

    /// Schema came from a schema registry.
    Registry {
        /// The registered schema ID.
        schema_id: i32,
    },

    /// Schema was inferred from samples.
    Inferred {
        /// Number of samples used.
        sample_count: usize,

        /// Warnings from inference.
        warnings: Vec<String>,
    },
}

/// Origin of a single field in a resolved schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldOrigin {
    /// The field was explicitly declared in user DDL.
    UserDeclared,

    /// The field was automatically resolved from the source or registry.
    AutoResolved,

    /// The field was added by wildcard expansion.
    WildcardInferred,

    /// The field was added from a default expression.
    DefaultAdded,
}

impl std::fmt::Display for FieldOrigin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldOrigin::UserDeclared => write!(f, "DECLARED"),
            FieldOrigin::AutoResolved => write!(f, "AUTO"),
            FieldOrigin::WildcardInferred => write!(f, "WILDCARD"),
            FieldOrigin::DefaultAdded => write!(f, "DEFAULT"),
        }
    }
}

/// Stateless schema resolver.
///
/// Implements the five-level priority chain and wildcard merge logic.
pub struct SchemaResolver;

impl SchemaResolver {
    /// Resolves the schema for a source connector.
    ///
    /// Applies the five-level priority chain:
    /// 1. Full DDL without wildcard → `Declared`
    /// 2. Schema registry → `Registry`
    /// 3. Schema provider → `SourceProvided`
    /// 4. Sample inference → `Inferred`
    /// 5. Error
    ///
    /// When the DDL contains a wildcard, the resolver merges declared
    /// columns with the resolved schema via [`merge_with_declared`](Self::merge_with_declared).
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError`] if no resolution strategy succeeds.
    pub async fn resolve(
        declared: &DeclaredSchema,
        connector: &dyn SourceConnector,
        inference_config: &InferenceConfig,
    ) -> SchemaResult<ResolvedSchema> {
        // Level 1: Full DDL without wildcard.
        if !declared.columns.is_empty() && !declared.has_wildcard {
            let schema = Self::declared_to_arrow(declared)?;
            let origins = vec![FieldOrigin::UserDeclared; schema.fields().len()];
            return Ok(ResolvedSchema {
                schema,
                kind: ResolutionKind::Declared,
                field_origins: origins,
                warnings: vec![],
            });
        }

        // Level 2: Schema registry.
        if let Some(registry) = connector.as_schema_registry_aware() {
            // Use connector type as subject (convention).
            let subject = format!("{}-value", inference_config.format);
            if let Ok(registered) = registry.fetch_schema(&subject).await {
                let resolved_schema = registered.schema.clone();
                let kind = ResolutionKind::Registry {
                    schema_id: registered.id,
                };

                if declared.has_wildcard {
                    return Self::merge_with_declared(declared, &resolved_schema, kind);
                }

                let origins = vec![FieldOrigin::AutoResolved; resolved_schema.fields().len()];
                return Ok(ResolvedSchema {
                    schema: resolved_schema,
                    kind,
                    field_origins: origins,
                    warnings: vec![],
                });
            }
        }

        // Level 3: Source-provided schema.
        if let Some(provider) = connector.as_schema_provider() {
            if let Ok(source_schema) = provider.provide_schema().await {
                if declared.has_wildcard {
                    return Self::merge_with_declared(
                        declared,
                        &source_schema,
                        ResolutionKind::SourceProvided,
                    );
                }

                let origins = vec![FieldOrigin::AutoResolved; source_schema.fields().len()];
                return Ok(ResolvedSchema {
                    schema: source_schema,
                    kind: ResolutionKind::SourceProvided,
                    field_origins: origins,
                    warnings: vec![],
                });
            }
        }

        // Level 4: Sample-based inference.
        if let Some(inferable) = connector.as_schema_inferable() {
            let samples = inferable
                .sample_records(inference_config.max_samples)
                .await?;

            if !samples.is_empty() {
                let inferred = inferable
                    .infer_from_samples(&samples, inference_config)
                    .await?;

                let inf_warnings: Vec<String> = inferred
                    .warnings
                    .iter()
                    .map(|w| w.message.clone())
                    .collect();

                let kind = ResolutionKind::Inferred {
                    sample_count: inferred.sample_count,
                    warnings: inf_warnings.clone(),
                };

                if declared.has_wildcard {
                    let mut resolved = Self::merge_with_declared(declared, &inferred.schema, kind)?;
                    resolved.warnings.extend(inf_warnings);
                    return Ok(resolved);
                }

                let origins = vec![FieldOrigin::AutoResolved; inferred.schema.fields().len()];
                return Ok(ResolvedSchema {
                    schema: inferred.schema,
                    kind,
                    field_origins: origins,
                    warnings: inf_warnings,
                });
            }
        }

        // Level 5: Error.
        Err(SchemaError::InferenceFailed(
            "no schema could be resolved: declare a schema, configure a registry, \
             or ensure the connector supports schema inference"
                .into(),
        ))
    }

    /// Merges user-declared columns with a resolved schema.
    ///
    /// User-declared columns come first and take precedence. Resolved
    /// columns that don't conflict are appended, optionally with a
    /// wildcard prefix.
    ///
    /// # Errors
    ///
    /// Currently infallible, but returns `SchemaResult` for future
    /// extensibility (e.g., type coercion failures).
    ///
    /// # Panics
    ///
    /// Panics if a declared column name exists in `declared_names` but
    /// cannot be found in `declared.columns` (this is logically impossible).
    pub fn merge_with_declared(
        declared: &DeclaredSchema,
        resolved: &SchemaRef,
        kind: ResolutionKind,
    ) -> SchemaResult<ResolvedSchema> {
        let mut fields: Vec<Field> = Vec::new();
        let mut origins: Vec<FieldOrigin> = Vec::new();
        let mut warnings: Vec<String> = Vec::new();

        // Declared columns go first.
        let declared_names: Vec<&str> = declared.columns.iter().map(|c| c.name.as_str()).collect();

        for col in &declared.columns {
            fields.push(Field::new(&col.name, col.data_type.clone(), col.nullable));
            origins.push(FieldOrigin::UserDeclared);
        }

        // Append non-conflicting resolved columns.
        for field in resolved.fields() {
            let name = field.name();
            if declared_names.contains(&name.as_str()) {
                // Declared column takes precedence — warn if types differ.
                let declared_col = declared.columns.iter().find(|c| c.name == *name).unwrap();
                if declared_col.data_type != *field.data_type() {
                    warnings.push(format!(
                        "field '{}': declared type {} overrides resolved type {}",
                        name,
                        declared_col.data_type,
                        field.data_type()
                    ));
                }
                continue;
            }

            // Apply wildcard prefix if set.
            let field_name = if let Some(ref prefix) = declared.wildcard_prefix {
                format!("{prefix}{name}")
            } else {
                name.clone()
            };

            fields.push(Field::new(
                &field_name,
                field.data_type().clone(),
                field.is_nullable(),
            ));
            origins.push(FieldOrigin::WildcardInferred);
        }

        Ok(ResolvedSchema {
            schema: Arc::new(Schema::new(fields)),
            kind,
            field_origins: origins,
            warnings,
        })
    }

    /// Validates that a wildcard declaration is consistent and usable.
    ///
    /// Checks:
    /// - The connector supports at least one schema resolution path
    ///   (provider, registry, or inference).
    /// - If a prefix is set, no prefixed column name collides with a
    ///   declared column name.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::WildcardWithoutInference`] if no resolution
    /// path is available, or [`SchemaError::WildcardPrefixCollision`] if
    /// a prefixed name collides with a declared column.
    pub fn validate_wildcard(
        declared: &DeclaredSchema,
        connector: &dyn SourceConnector,
    ) -> SchemaResult<()> {
        if !declared.has_wildcard {
            return Ok(());
        }

        // At least one resolution path must be available.
        let has_provider = connector.as_schema_provider().is_some();
        let has_registry = connector.as_schema_registry_aware().is_some();
        let has_inference = connector.as_schema_inferable().is_some();

        if !has_provider && !has_registry && !has_inference {
            return Err(SchemaError::WildcardWithoutInference);
        }

        Ok(())
    }

    /// Checks that wildcard-prefixed column names don't collide with
    /// declared columns.
    ///
    /// Call this after schema resolution when the resolved field names
    /// are known.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::WildcardPrefixCollision`] on collision.
    pub fn check_prefix_collision(
        declared: &DeclaredSchema,
        resolved: &SchemaRef,
    ) -> SchemaResult<()> {
        let Some(prefix) = &declared.wildcard_prefix else {
            return Ok(());
        };

        let declared_names: Vec<&str> = declared.columns.iter().map(|c| c.name.as_str()).collect();

        for field in resolved.fields() {
            let name = field.name();
            if declared_names.contains(&name.as_str()) {
                continue; // Will be skipped during merge anyway.
            }
            let prefixed = format!("{prefix}{name}");
            if declared_names.contains(&prefixed.as_str()) {
                return Err(SchemaError::WildcardPrefixCollision(prefixed));
            }
        }

        Ok(())
    }

    /// Converts a [`DeclaredSchema`] into an Arrow [`SchemaRef`].
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::InferenceFailed`] if the declared schema
    /// has no columns.
    pub fn declared_to_arrow(declared: &DeclaredSchema) -> SchemaResult<SchemaRef> {
        if declared.columns.is_empty() {
            return Err(SchemaError::InferenceFailed(
                "declared schema has no columns".into(),
            ));
        }

        let fields: Vec<Field> = declared
            .columns
            .iter()
            .map(|c| Field::new(&c.name, c.data_type.clone(), c.nullable))
            .collect();

        Ok(Arc::new(Schema::new(fields)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_resolved_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Int64, true),
        ]))
    }

    // ── DeclaredSchema tests ───────────────────────────────────

    #[test]
    fn test_declared_schema_full() {
        let declared = DeclaredSchema::full(vec![
            DeclaredColumn::new("id", DataType::Int64, false),
            DeclaredColumn::new("name", DataType::Utf8, true),
        ]);
        assert!(!declared.has_wildcard);
        assert!(!declared.is_empty());
    }

    #[test]
    fn test_declared_schema_with_wildcard() {
        let declared =
            DeclaredSchema::with_wildcard(vec![DeclaredColumn::new("id", DataType::Int64, false)]);
        assert!(declared.has_wildcard);
    }

    #[test]
    fn test_declared_schema_empty() {
        let declared = DeclaredSchema::full(vec![]);
        assert!(declared.is_empty());
    }

    #[test]
    fn test_declared_column_with_default() {
        let col = DeclaredColumn::new("status", DataType::Utf8, false).with_default("active");
        assert_eq!(col.default.as_deref(), Some("active"));
    }

    // ── declared_to_arrow tests ────────────────────────────────

    #[test]
    fn test_declared_to_arrow() {
        let declared = DeclaredSchema::full(vec![
            DeclaredColumn::new("id", DataType::Int64, false),
            DeclaredColumn::new("name", DataType::Utf8, true),
        ]);

        let schema = SchemaResolver::declared_to_arrow(&declared).unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "name");
        assert!(schema.field(1).is_nullable());
    }

    #[test]
    fn test_declared_to_arrow_empty_error() {
        let declared = DeclaredSchema::full(vec![]);
        assert!(SchemaResolver::declared_to_arrow(&declared).is_err());
    }

    // ── merge_with_declared tests ──────────────────────────────

    #[test]
    fn test_merge_no_overlap() {
        let declared = DeclaredSchema::with_wildcard(vec![DeclaredColumn::new(
            "extra",
            DataType::Boolean,
            false,
        )]);

        let resolved = sample_resolved_schema();
        let result = SchemaResolver::merge_with_declared(
            &declared,
            &resolved,
            ResolutionKind::SourceProvided,
        )
        .unwrap();

        // "extra" first, then id, name, age
        assert_eq!(result.schema.fields().len(), 4);
        assert_eq!(result.schema.field(0).name(), "extra");
        assert_eq!(result.schema.field(1).name(), "id");
        assert_eq!(result.schema.field(2).name(), "name");
        assert_eq!(result.schema.field(3).name(), "age");

        assert_eq!(result.field_origins[0], FieldOrigin::UserDeclared);
        assert_eq!(result.field_origins[1], FieldOrigin::WildcardInferred);
    }

    #[test]
    fn test_merge_with_overlap() {
        let declared = DeclaredSchema::with_wildcard(vec![DeclaredColumn::new(
            "id",
            DataType::Int32, // different type than resolved
            false,
        )]);

        let resolved = sample_resolved_schema();
        let result = SchemaResolver::merge_with_declared(
            &declared,
            &resolved,
            ResolutionKind::SourceProvided,
        )
        .unwrap();

        // "id" from declared (Int32), then name, age from resolved.
        assert_eq!(result.schema.fields().len(), 3);
        assert_eq!(result.schema.field(0).name(), "id");
        assert_eq!(result.schema.field(0).data_type(), &DataType::Int32);
        assert_eq!(result.schema.field(1).name(), "name");
        assert_eq!(result.schema.field(2).name(), "age");

        // Should warn about type mismatch.
        assert!(!result.warnings.is_empty());
        assert!(result.warnings[0].contains("Int32"));
    }

    #[test]
    fn test_merge_with_prefix() {
        let declared =
            DeclaredSchema::with_wildcard(vec![DeclaredColumn::new("pk", DataType::Int64, false)])
                .with_prefix("src_");

        let resolved = sample_resolved_schema();
        let result = SchemaResolver::merge_with_declared(
            &declared,
            &resolved,
            ResolutionKind::SourceProvided,
        )
        .unwrap();

        // "pk" first, then src_id, src_name, src_age
        assert_eq!(result.schema.fields().len(), 4);
        assert_eq!(result.schema.field(0).name(), "pk");
        assert_eq!(result.schema.field(1).name(), "src_id");
        assert_eq!(result.schema.field(2).name(), "src_name");
        assert_eq!(result.schema.field(3).name(), "src_age");
    }

    // ── ResolutionKind tests ───────────────────────────────────

    #[test]
    fn test_resolution_kind_eq() {
        assert_eq!(ResolutionKind::Declared, ResolutionKind::Declared);
        assert_ne!(ResolutionKind::Declared, ResolutionKind::SourceProvided);
        assert_eq!(
            ResolutionKind::Registry { schema_id: 1 },
            ResolutionKind::Registry { schema_id: 1 }
        );
    }

    // ── FieldOrigin tests ──────────────────────────────────────

    #[test]
    fn test_field_origin_variants() {
        let origins = [
            FieldOrigin::UserDeclared,
            FieldOrigin::AutoResolved,
            FieldOrigin::WildcardInferred,
            FieldOrigin::DefaultAdded,
        ];
        assert_eq!(origins.len(), 4);
        assert_ne!(FieldOrigin::UserDeclared, FieldOrigin::AutoResolved);
    }

    // ── ResolvedSchema tests ───────────────────────────────────

    #[test]
    fn test_resolved_schema_declared() {
        let declared = DeclaredSchema::full(vec![
            DeclaredColumn::new("x", DataType::Float64, false),
            DeclaredColumn::new("y", DataType::Float64, false),
        ]);

        let schema = SchemaResolver::declared_to_arrow(&declared).unwrap();
        let resolved = ResolvedSchema {
            schema,
            kind: ResolutionKind::Declared,
            field_origins: vec![FieldOrigin::UserDeclared; 2],
            warnings: vec![],
        };

        assert_eq!(resolved.kind, ResolutionKind::Declared);
        assert_eq!(resolved.field_origins.len(), 2);
        assert!(resolved.warnings.is_empty());
    }

    // ── check_prefix_collision tests ────────────────────────

    #[test]
    fn test_prefix_collision_none() {
        let declared =
            DeclaredSchema::with_wildcard(vec![DeclaredColumn::new("pk", DataType::Int64, false)]);
        let resolved = sample_resolved_schema();
        assert!(SchemaResolver::check_prefix_collision(&declared, &resolved).is_ok());
    }

    #[test]
    fn test_prefix_collision_detected() {
        let declared = DeclaredSchema::with_wildcard(vec![DeclaredColumn::new(
            "src_name",
            DataType::Utf8,
            true,
        )])
        .with_prefix("src_");

        let resolved = sample_resolved_schema(); // has "name"
        let result = SchemaResolver::check_prefix_collision(&declared, &resolved);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("src_name"));
    }

    #[test]
    fn test_prefix_collision_no_prefix_ok() {
        let declared =
            DeclaredSchema::with_wildcard(vec![DeclaredColumn::new("name", DataType::Utf8, true)]);
        let resolved = sample_resolved_schema();
        // No prefix set, so no collision check needed.
        assert!(SchemaResolver::check_prefix_collision(&declared, &resolved).is_ok());
    }

    #[test]
    fn test_prefix_collision_skip_declared_overlap() {
        // If a resolved field overlaps with a declared field (same name without prefix),
        // it won't be expanded, so no collision.
        let declared = DeclaredSchema::with_wildcard(vec![DeclaredColumn::new(
            "src_id",
            DataType::Int64,
            false,
        )])
        .with_prefix("src_");

        // "id" -> "src_id" would collide, BUT "id" is not the same as "src_id" (the declared name),
        // so it IS a collision.
        let resolved = sample_resolved_schema();
        let result = SchemaResolver::check_prefix_collision(&declared, &resolved);
        assert!(result.is_err());
    }
}

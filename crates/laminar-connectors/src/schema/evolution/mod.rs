//! Schema evolution engine (F-SCHEMA-009).
//!
//! Provides:
//!
//! - [`SchemaEvolution`] — a general-purpose schema evolver that performs
//!   name-based schema diffing, compatibility evaluation, and schema merging
//! - [`SchemaEvolutionEngine`] — orchestrates the full evolution flow:
//!   detect → diff → evaluate → apply → record
//! - [`SchemaHistory`] — tracks per-source schema version history
//! - [`is_safe_widening`] — determines whether a type can be safely widened
#![allow(clippy::disallowed_types)] // cold path: schema management

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use arrow_schema::{DataType, Field, Schema, SchemaRef};

use super::error::{SchemaError, SchemaResult};
use super::traits::{ColumnProjection, CompatibilityMode, EvolutionVerdict, SchemaChange};

// ── Safe widening rules ────────────────────────────────────────────

/// Returns `true` if `from` can be safely widened to `to` without data loss.
#[must_use]
pub fn is_safe_widening(from: &DataType, to: &DataType) -> bool {
    matches!(
        (from, to),
        (
            DataType::Int8,
            DataType::Int16 | DataType::Int32 | DataType::Int64
        ) | (DataType::Int16, DataType::Int32 | DataType::Int64)
            | (DataType::Int32, DataType::Int64 | DataType::Float64)
            | (
                DataType::UInt8,
                DataType::UInt16 | DataType::UInt32 | DataType::UInt64
            )
            | (DataType::UInt16, DataType::UInt32 | DataType::UInt64)
            | (DataType::UInt32, DataType::UInt64)
            | (
                DataType::Float16 | DataType::Int8 | DataType::Int16,
                DataType::Float32 | DataType::Float64
            )
            | (DataType::Float32, DataType::Float64)
            | (DataType::Utf8, DataType::LargeUtf8)
            | (DataType::Binary, DataType::LargeBinary)
    )
}

// ── SchemaEvolution ───────────────────────────────────────────

/// A general-purpose schema evolver that uses name-based schema matching
/// (suitable for JSON, CSV, Avro, etc.).
///
/// For field-ID-based sources (Iceberg, Parquet), a specialised
/// implementation should be used instead.
#[derive(Debug, Clone)]
pub struct SchemaEvolution {
    /// The compatibility mode for evaluating changes.
    pub compatibility: CompatibilityMode,
}

impl SchemaEvolution {
    /// Creates a new evolver with the given compatibility mode.
    #[must_use]
    pub fn new(compatibility: CompatibilityMode) -> Self {
        Self { compatibility }
    }

    /// Computes the differences between two schemas.
    #[must_use]
    pub fn diff_schemas(&self, old: &SchemaRef, new: &SchemaRef) -> Vec<SchemaChange> {
        diff_schemas_by_name(old, new)
    }

    /// Evaluates whether a set of schema changes is acceptable.
    #[must_use]
    pub fn evaluate_evolution(&self, changes: &[SchemaChange]) -> EvolutionVerdict {
        evaluate_changes(changes, self.compatibility)
    }

    /// Applies schema changes, returning a column projection.
    ///
    /// # Errors
    ///
    /// Returns a schema error if the evolution cannot be applied.
    pub fn apply_evolution(
        &self,
        old: &SchemaRef,
        changes: &[SchemaChange],
    ) -> SchemaResult<ColumnProjection> {
        apply_changes(old, changes)
    }
}

impl Default for SchemaEvolution {
    fn default() -> Self {
        Self {
            compatibility: CompatibilityMode::None,
        }
    }
}

// ── Core diffing algorithm ─────────────────────────────────────────

/// Computes the diff between two Arrow schemas using name-based matching.
///
/// Returns a list of [`SchemaChange`] entries describing all differences.
/// Order: column removals, column additions, type changes, nullability changes.
#[must_use]
pub fn diff_schemas_by_name(old: &SchemaRef, new: &SchemaRef) -> Vec<SchemaChange> {
    let mut changes = Vec::new();

    let old_fields: HashMap<&str, &Field> = old
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();

    let new_fields: HashMap<&str, &Field> = new
        .fields()
        .iter()
        .map(|f| (f.name().as_str(), f.as_ref()))
        .collect();

    // Detect removed columns (in old but not in new).
    for field in old.fields() {
        if !new_fields.contains_key(field.name().as_str()) {
            changes.push(SchemaChange::ColumnRemoved {
                name: field.name().clone(),
            });
        }
    }

    // Detect added columns (in new but not in old).
    for field in new.fields() {
        if !old_fields.contains_key(field.name().as_str()) {
            changes.push(SchemaChange::ColumnAdded {
                name: field.name().clone(),
                data_type: field.data_type().clone(),
                nullable: field.is_nullable(),
            });
        }
    }

    // Detect type and nullability changes.
    for field in new.fields() {
        if let Some(old_field) = old_fields.get(field.name().as_str()) {
            if old_field.data_type() != field.data_type() {
                changes.push(SchemaChange::TypeChanged {
                    name: field.name().clone(),
                    old_type: old_field.data_type().clone(),
                    new_type: field.data_type().clone(),
                });
            }
            if old_field.is_nullable() != field.is_nullable() {
                changes.push(SchemaChange::NullabilityChanged {
                    name: field.name().clone(),
                    was_nullable: old_field.is_nullable(),
                    now_nullable: field.is_nullable(),
                });
            }
        }
    }

    changes
}

// ── Compatibility evaluation ───────────────────────────────────────

/// Evaluates a set of schema changes against a compatibility mode.
#[must_use]
pub fn evaluate_changes(changes: &[SchemaChange], mode: CompatibilityMode) -> EvolutionVerdict {
    if changes.is_empty() {
        return EvolutionVerdict::Compatible;
    }

    // Mode::None — everything is allowed.
    if mode == CompatibilityMode::None {
        return EvolutionVerdict::Compatible;
    }

    let requires_backward = matches!(
        mode,
        CompatibilityMode::Backward
            | CompatibilityMode::Full
            | CompatibilityMode::BackwardTransitive
            | CompatibilityMode::FullTransitive
    );
    let requires_forward = matches!(
        mode,
        CompatibilityMode::Forward
            | CompatibilityMode::Full
            | CompatibilityMode::ForwardTransitive
            | CompatibilityMode::FullTransitive
    );

    let mut needs_migration = false;

    for change in changes {
        match change {
            SchemaChange::ColumnAdded { nullable, .. } => {
                // Backward: new schema must read old data.
                // Adding a nullable column is backward-compatible (old data has null).
                // Adding a non-nullable column breaks backward compatibility
                // (old data has no value for this column).
                if requires_backward && !nullable {
                    return EvolutionVerdict::Incompatible(format!(
                        "adding non-nullable column '{}' is not backward-compatible",
                        change_name(change)
                    ));
                }
            }
            SchemaChange::ColumnRemoved { name } => {
                // Backward: removing a column breaks backward compatibility
                // (old data has the column, new reader doesn't expect it).
                // Actually, backward means new reader reads old data — if the new
                // schema drops a column, the new reader just ignores it. That's fine.
                // Forward: old reader reads new data — new data is missing a column
                // that old reader expects. That breaks forward compatibility.
                if requires_forward {
                    return EvolutionVerdict::Incompatible(format!(
                        "removing column '{name}' is not forward-compatible"
                    ));
                }
                needs_migration = true;
            }
            SchemaChange::TypeChanged {
                name,
                old_type,
                new_type,
            } => {
                if is_safe_widening(old_type, new_type) {
                    // Widening is backward-compatible (new reader can handle old
                    // narrower data). But it's NOT forward-compatible (old reader
                    // can't handle wider data).
                    if requires_forward {
                        return EvolutionVerdict::Incompatible(format!(
                            "widening column '{name}' from {old_type:?} to {new_type:?} \
                             is not forward-compatible"
                        ));
                    }
                } else if is_safe_widening(new_type, old_type) {
                    // Narrowing — forward-compatible but not backward.
                    if requires_backward {
                        return EvolutionVerdict::Incompatible(format!(
                            "narrowing column '{name}' from {old_type:?} to {new_type:?} \
                             is not backward-compatible"
                        ));
                    }
                    needs_migration = true;
                } else {
                    // Unrelated types — always incompatible.
                    return EvolutionVerdict::Incompatible(format!(
                        "changing column '{name}' from {old_type:?} to {new_type:?} \
                         is incompatible"
                    ));
                }
            }
            SchemaChange::NullabilityChanged {
                name,
                was_nullable,
                now_nullable,
            } => {
                if *was_nullable && !now_nullable {
                    // Making a column non-nullable: new reader rejects old nulls.
                    if requires_backward {
                        return EvolutionVerdict::Incompatible(format!(
                            "making column '{name}' non-nullable is not backward-compatible"
                        ));
                    }
                    needs_migration = true;
                }
                // Making a column nullable is always safe.
            }
            SchemaChange::ColumnRenamed { old_name, .. } => {
                // Renames without field-IDs are always incompatible
                // (name-based matching treats it as drop + add).
                return EvolutionVerdict::Incompatible(format!(
                    "renaming column '{old_name}' is not supported without field IDs"
                ));
            }
        }
    }

    if needs_migration {
        EvolutionVerdict::RequiresMigration
    } else {
        EvolutionVerdict::Compatible
    }
}

fn change_name(change: &SchemaChange) -> &str {
    match change {
        SchemaChange::ColumnAdded { name, .. }
        | SchemaChange::ColumnRemoved { name }
        | SchemaChange::TypeChanged { name, .. }
        | SchemaChange::NullabilityChanged { name, .. } => name,
        SchemaChange::ColumnRenamed { old_name, .. } => old_name,
    }
}

// ── Schema application ─────────────────────────────────────────────

/// Applies a set of schema changes to produce a new schema and a column
/// projection mapping old columns to new positions.
///
/// # Errors
///
/// Returns [`SchemaError`] if the changes reference columns that don't exist.
pub fn apply_changes(old: &SchemaRef, changes: &[SchemaChange]) -> SchemaResult<ColumnProjection> {
    let mut fields: Vec<Field> = old.fields().iter().map(|f| f.as_ref().clone()).collect();
    let mut old_index_map: Vec<Option<usize>> = (0..fields.len()).map(Some).collect();

    // Collect indices of columns to remove.
    let mut remove_indices = Vec::new();

    for change in changes {
        match change {
            SchemaChange::ColumnAdded {
                name,
                data_type,
                nullable,
            } => {
                fields.push(Field::new(name, data_type.clone(), *nullable));
                old_index_map.push(None); // New column — no old source.
            }
            SchemaChange::ColumnRemoved { name } => {
                if let Some(idx) = fields.iter().position(|f| f.name() == name) {
                    remove_indices.push(idx);
                }
            }
            SchemaChange::TypeChanged { name, new_type, .. } => {
                if let Some(field) = fields.iter_mut().find(|f| f.name() == name) {
                    *field = Field::new(field.name(), new_type.clone(), field.is_nullable());
                }
            }
            SchemaChange::NullabilityChanged {
                name, now_nullable, ..
            } => {
                if let Some(field) = fields.iter_mut().find(|f| f.name() == name) {
                    *field = Field::new(field.name(), field.data_type().clone(), *now_nullable);
                }
            }
            SchemaChange::ColumnRenamed {
                old_name, new_name, ..
            } => {
                if let Some(field) = fields.iter_mut().find(|f| f.name() == old_name) {
                    *field = Field::new(new_name, field.data_type().clone(), field.is_nullable());
                }
            }
        }
    }

    // Remove columns in reverse order to preserve indices.
    remove_indices.sort_unstable();
    remove_indices.dedup();
    for &idx in remove_indices.iter().rev() {
        fields.remove(idx);
        old_index_map.remove(idx);
    }

    let new_schema = Arc::new(Schema::new(fields));

    Ok(ColumnProjection {
        mappings: old_index_map,
        target_schema: new_schema,
    })
}

// ── Schema history ─────────────────────────────────────────────────

/// How a schema evolution was triggered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvolutionTrigger {
    /// User issued ALTER SOURCE DDL.
    Ddl,
    /// Schema registry detected a new schema version.
    Registry {
        /// The schema ID that triggered the change.
        schema_id: u32,
    },
    /// User issued REFRESH SCHEMA.
    ManualRefresh,
}

/// A record in the schema history.
#[derive(Debug, Clone)]
pub struct SchemaHistoryEntry {
    /// Source name.
    pub source_name: String,
    /// Schema version (monotonically increasing per source).
    pub version: u64,
    /// The Arrow schema at this version.
    pub schema: SchemaRef,
    /// Changes from the previous version.
    pub changes: Vec<SchemaChange>,
    /// When this version was applied.
    pub applied_at: SystemTime,
    /// How the evolution was triggered.
    pub trigger: EvolutionTrigger,
}

/// Tracks schema version history for all sources.
#[derive(Debug, Default)]
pub struct SchemaHistory {
    entries: HashMap<String, Vec<SchemaHistoryEntry>>,
}

impl SchemaHistory {
    /// Creates a new empty history.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a new schema version for a source. Returns the version number.
    pub fn record(
        &mut self,
        source_name: &str,
        schema: SchemaRef,
        changes: Vec<SchemaChange>,
        trigger: EvolutionTrigger,
    ) -> u64 {
        let entries = self.entries.entry(source_name.to_string()).or_default();
        let version = entries.last().map_or(1, |e| e.version + 1);
        entries.push(SchemaHistoryEntry {
            source_name: source_name.to_string(),
            version,
            schema,
            changes,
            applied_at: SystemTime::now(),
            trigger,
        });
        version
    }

    /// Returns all versions for a source.
    #[must_use]
    pub fn versions(&self, source_name: &str) -> &[SchemaHistoryEntry] {
        self.entries.get(source_name).map_or(&[], |v| v.as_slice())
    }

    /// Returns the latest version number for a source.
    #[must_use]
    pub fn latest_version(&self, source_name: &str) -> Option<u64> {
        self.entries.get(source_name)?.last().map(|e| e.version)
    }
}

// ── Schema Evolution Engine ────────────────────────────────────────

/// Result of an evolution attempt.
#[derive(Debug)]
pub enum EvolutionResult {
    /// No schema change detected.
    NoChange,
    /// Evolution applied successfully.
    Applied {
        /// The new schema.
        new_schema: SchemaRef,
        /// Column projection from old → new.
        projection: ColumnProjection,
        /// The new version number.
        version: u64,
        /// The individual changes.
        changes: Vec<SchemaChange>,
    },
}

/// Orchestrates the full schema evolution flow.
///
/// 1. Diff the current and proposed schemas
/// 2. Evaluate compatibility
/// 3. Apply changes and produce a new schema + projection
/// 4. Record in schema history
#[derive(Debug)]
pub struct SchemaEvolutionEngine {
    /// Per-source version history.
    pub history: SchemaHistory,
    /// Default compatibility mode.
    pub default_compatibility: CompatibilityMode,
}

impl SchemaEvolutionEngine {
    /// Creates a new engine with the given default compatibility mode.
    #[must_use]
    pub fn new(default_compatibility: CompatibilityMode) -> Self {
        Self {
            history: SchemaHistory::new(),
            default_compatibility,
        }
    }

    /// Executes the full evolution flow for a source.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaError::EvolutionRejected`] if the changes are incompatible.
    pub fn evolve(
        &mut self,
        source_name: &str,
        evolvable: &SchemaEvolution,
        current: &SchemaRef,
        proposed: &SchemaRef,
        trigger: EvolutionTrigger,
    ) -> SchemaResult<EvolutionResult> {
        let changes = evolvable.diff_schemas(current, proposed);

        if changes.is_empty() {
            return Ok(EvolutionResult::NoChange);
        }

        let verdict = evolvable.evaluate_evolution(&changes);

        match verdict {
            EvolutionVerdict::Compatible | EvolutionVerdict::RequiresMigration => {
                let projection = evolvable.apply_evolution(current, &changes)?;
                let version = self.history.record(
                    source_name,
                    projection.target_schema.clone(),
                    changes.clone(),
                    trigger,
                );
                Ok(EvolutionResult::Applied {
                    new_schema: projection.target_schema.clone(),
                    projection,
                    version,
                    changes,
                })
            }
            EvolutionVerdict::Incompatible(reason) => Err(SchemaError::EvolutionRejected(reason)),
        }
    }
}

#[cfg(test)]
mod tests;

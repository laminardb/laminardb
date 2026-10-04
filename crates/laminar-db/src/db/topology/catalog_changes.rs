//! Apply supported DDL to an isolated catalog, retaining an exact request and parent inventory.

use std::collections::BTreeSet;

use laminar_core::cluster::control::{
    CatalogManifest, CatalogManifestStore, CatalogObjectKind, TopologyCatalogState, TopologyError,
};
use laminar_sql::parser::{parse_streaming_sql, StreamingStatement};

use super::{DbError, DbState, LaminarDB};

pub(super) fn parse_one_change(sql: &str) -> Result<StreamingStatement, DbError> {
    let mut parsed = parse_streaming_sql(sql)?;
    if parsed.len() != 1 {
        return Err(TopologyError::Invalid(
            "each validation entry must contain exactly one topology DDL statement".into(),
        )
        .into());
    }
    let statement = parsed
        .pop()
        .ok_or_else(|| TopologyError::Invalid("empty validation entry".into()))?;
    let allowed = match &statement {
        StreamingStatement::CreateSource(create) => !create.or_replace,
        StreamingStatement::CreateSink(create) => !create.or_replace,
        StreamingStatement::CreateStream { or_replace, .. } => !or_replace,
        StreamingStatement::DropSource { cascade, .. }
        | StreamingStatement::DropStream { cascade, .. }
        | StreamingStatement::DropSink { cascade, .. } => !cascade,
        _ => false,
    };
    if !allowed {
        return Err(TopologyError::Unsupported(
            "topology validation supports CREATE/DROP SOURCE/STREAM/SINK without CASCADE; replacement and reset require separate contracts".into(),
        )
        .into());
    }
    Ok(statement)
}

pub(super) fn statement_identity(
    candidate: &LaminarDB,
    sql: &str,
    statement: &StreamingStatement,
) -> Result<(String, CatalogObjectKind, &'static str), DbError> {
    let drop = match statement {
        StreamingStatement::DropSource { name, .. } => {
            Some((name, CatalogObjectKind::Source, "DROP SOURCE"))
        }
        StreamingStatement::DropStream { name, .. } => {
            Some((name, CatalogObjectKind::Stream, "DROP STREAM"))
        }
        StreamingStatement::DropSink { name, .. } => {
            Some((name, CatalogObjectKind::Sink, "DROP SINK"))
        }
        _ => None,
    };
    if let Some((name, kind, operation)) = drop {
        if crate::db::catalog_ddl_contains_comment(sql)? {
            return Err(TopologyError::Unsupported(
                "DROP requires one typed statement without SQL comments".into(),
            )
            .into());
        }
        return Ok((crate::db::canonical_object_name(name)?, kind, operation));
    }
    super::super::validate_cluster_catalog_create(candidate, sql, statement)
}

pub(super) async fn apply_statement(
    candidate: &LaminarDB,
    sql: &str,
    statement: &StreamingStatement,
    name: &str,
) -> Result<(), DbError> {
    // The exception belongs to this unstarted catalog. Active runtime DDL uses admission.
    let result = super::super::CATALOG_MANIFEST_REPLAY
        .scope((), candidate.execute_parsed_single(sql, statement))
        .await?;
    if !matches!(result, crate::handle::ExecuteResult::Ddl(ref info) if info.applied && info.object_name == name)
    {
        return Err(TopologyError::Invalid(format!(
            "candidate DDL for '{name}' did not apply exactly once"
        ))
        .into());
    }
    Ok(())
}

pub(super) async fn apply_catalog_changes(
    candidate: &LaminarDB,
    parent: &CatalogManifest,
    statements: &[String],
    retired_names: &BTreeSet<String>,
) -> Result<CatalogManifest, DbError> {
    let mut entries = parent.entries.clone();
    let mut changed = BTreeSet::new();
    let mut new_objects = 0;
    for sql in statements {
        let statement = parse_one_change(sql)?;
        let (name, kind, operation) = statement_identity(candidate, sql, &statement)?;
        if !changed.insert(name.clone()) {
            return Err(TopologyError::Unsupported(format!(
                "'{name}' changes more than once; replacement and drop/recreate require a new incarnation contract"
            ))
            .into());
        }
        if matches!(
            statement,
            StreamingStatement::DropSource { .. }
                | StreamingStatement::DropStream { .. }
                | StreamingStatement::DropSink { .. }
        ) {
            let index = entries
                .iter()
                .position(|entry| entry.canonical_name == name && entry.kind == kind)
                .ok_or_else(|| {
                    TopologyError::Unsupported(format!(
                        "{operation} '{name}' requires an existing parent object of the same kind; a missing name cannot establish a migration cut"
                    ))
                })?;
            apply_statement(candidate, sql, &statement, &name).await?;
            entries.remove(index);
            continue;
        }
        if parent
            .entries
            .iter()
            .any(|entry| entry.canonical_name == name)
            || retired_names.contains(&name)
        {
            return Err(TopologyError::Unsupported(format!(
                "'{name}' already exists or was retired; replacement and drop/recreate require a new incarnation contract, even with IF NOT EXISTS"
            ))
            .into());
        }
        new_objects += 1;
        if parent.entries.len() + new_objects > super::planning::MAX_VALIDATION_OBJECTS {
            return Err(TopologyError::Unsupported(
                "candidate exceeds the 256 object mapping bound".into(),
            )
            .into());
        }
        apply_statement(candidate, sql, &statement, &name).await?;
        entries.push(laminar_core::cluster::control::CatalogManifestEntry {
            canonical_name: name,
            kind,
            catalog_generation: 1,
            ddl: sql.clone(),
        });
    }
    CatalogManifest::new(entries).map_err(|error| TopologyError::from(error).into())
}

pub(in crate::db) fn validate_manifest_ddl(manifest: &CatalogManifest) -> Result<(), DbError> {
    for entry in &manifest.entries {
        if crate::db::catalog_ddl_contains_comment(&entry.ddl)? {
            return Err(DbError::Pipeline(format!(
                    "[{}] catalog manifest entry '{}' contains SQL comments rather than one canonical typed definition",
                    laminar_core::error_codes::RECOVERY_FAILED,
                    entry.canonical_name
                )));
        }
        let statements = parse_streaming_sql(&entry.ddl).map_err(|error| {
            DbError::Pipeline(format!(
                "[{}] catalog manifest entry '{}' is not valid topology DDL: {error}",
                laminar_core::error_codes::RECOVERY_FAILED,
                entry.canonical_name
            ))
        })?;
        if statements.len() != 1 {
            return Err(DbError::Pipeline(format!(
                "[{}] catalog manifest entry '{}' must contain exactly one typed CREATE statement",
                laminar_core::error_codes::RECOVERY_FAILED,
                entry.canonical_name
            )));
        }
        let Some((name, kind, _)) = crate::db::catalog_create_identity(&statements[0])? else {
            return Err(DbError::Pipeline(format!(
                "[{}] catalog manifest entry '{}' must contain exactly one typed CREATE statement",
                laminar_core::error_codes::RECOVERY_FAILED,
                entry.canonical_name
            )));
        };
        if name != entry.canonical_name || kind != entry.kind {
            return Err(DbError::Pipeline(format!(
                "[{}] catalog manifest entry '{}' does not match its typed DDL identity",
                laminar_core::error_codes::RECOVERY_FAILED,
                entry.canonical_name
            )));
        }
        if crate::db::connector_source_requires_schema_discovery(&statements[0]) {
            return Err(DbError::Pipeline(format!(
                "[{}] catalog manifest source '{}' lacks an explicit durable schema",
                laminar_core::error_codes::RECOVERY_FAILED,
                entry.canonical_name
            )));
        }
        if let Some(key) = crate::db::sensitive_catalog_property(&statements[0]) {
            return Err(DbError::Pipeline(format!(
                "[{}] catalog manifest entry '{}' contains secret property '{key}'",
                laminar_core::error_codes::RECOVERY_FAILED,
                entry.canonical_name
            )));
        }
    }
    Ok(())
}

impl LaminarDB {
    pub(in crate::db) async fn reconcile_retired_catalog_objects(
        &self,
        manifest: &CatalogManifest,
        store: &CatalogManifestStore,
        topology: &TopologyCatalogState,
    ) -> Result<(), DbError> {
        let extras: Vec<_> = self
            .catalog_manifest_inventory()?
            .into_iter()
            .filter(|local| {
                !manifest
                    .entries
                    .iter()
                    .any(|entry| entry.canonical_name == local.canonical_name)
            })
            .collect();
        if extras.is_empty() {
            return Ok(());
        }
        let retired = store.retired_topology_names().await?;
        if extras.iter().any(|entry| {
            !matches!(
                entry.kind,
                CatalogObjectKind::Source | CatalogObjectKind::Stream | CatalogObjectKind::Sink
            ) || !retired.contains(&entry.canonical_name)
        }) {
            return Err(TopologyError::Conflict("local inventory has an object outside the committed catalog without certified retirement".into()).into());
        }
        let (current, current_topology) = store
            .load_with_topology()
            .await?
            .ok_or(TopologyError::Fenced)?;
        if current != *manifest
            || current_topology != *topology
            || !matches!(
                DbState::load(&self.state),
                DbState::Created | DbState::Starting
            )
            || self.runtime_handle.lock().await.is_some()
            || !self.owned_source_tasks.lock().is_empty()
            || !self.owned_sink_handles.lock().is_empty()
            || !self.owned_connector_task_fences.lock().is_empty()
        {
            return Err(TopologyError::Fenced.into());
        }
        // The manifest is in dependency order; retire dependents before their inputs.
        for entry in extras.into_iter().rev() {
            self.require_catalog_kind(&entry.canonical_name, entry.kind, false)?;
            self.rollback_catalog_create(
                &entry.canonical_name,
                entry.kind,
                "committed topology retirement",
            )?;
        }
        Ok(())
    }
}

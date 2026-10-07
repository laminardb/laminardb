//! Apply supported DDL to an isolated catalog, retaining an exact request and parent inventory.

use std::collections::{BTreeMap, BTreeSet};

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
        StreamingStatement::CreateSource(_)
        | StreamingStatement::CreateSink(_)
        | StreamingStatement::CreateStream { .. } => true,
        StreamingStatement::DropSource { cascade, .. }
        | StreamingStatement::DropStream { cascade, .. }
        | StreamingStatement::DropSink { cascade, .. } => !cascade,
        _ => false,
    };
    if !allowed {
        return Err(TopologyError::Unsupported(
            "topology validation supports CREATE, CREATE OR REPLACE and DROP SOURCE/STREAM/SINK without CASCADE".into(),
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
    let ddl = create_definition(sql, statement)?;
    let mut create = statement.clone();
    match &mut create {
        StreamingStatement::CreateSource(create) => create.or_replace = false,
        StreamingStatement::CreateSink(create) => create.or_replace = false,
        StreamingStatement::CreateStream { or_replace, .. } => *or_replace = false,
        _ => {}
    }
    // The exception belongs to this unstarted catalog. Active runtime DDL uses admission.
    let result = super::super::CATALOG_MANIFEST_REPLAY
        .scope(
            (),
            futures::FutureExt::boxed(candidate.execute_parsed_single(&ddl, &create)),
        )
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
    retired_generations: &BTreeMap<String, u64>,
) -> Result<CatalogManifest, DbError> {
    let mut entries = parent.entries.clone();
    let mut changed = BTreeSet::new();
    let mut dropped = BTreeSet::new();
    let mut created = BTreeSet::new();
    let mut new_objects = 0;
    for sql in statements {
        let statement = parse_one_change(sql)?;
        let (name, kind, operation) = statement_identity(candidate, sql, &statement)?;
        if matches!(
            statement,
            StreamingStatement::DropSource { .. }
                | StreamingStatement::DropStream { .. }
                | StreamingStatement::DropSink { .. }
        ) {
            if !changed.insert(name.clone()) {
                return Err(TopologyError::Unsupported(format!(
                    "'{name}' may be dropped only once, before its optional recreation"
                ))
                .into());
            }
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
            dropped.insert(name);
            continue;
        }
        if !created.insert(name.clone()) {
            return Err(TopologyError::Unsupported(format!(
                "'{name}' may be created or replaced only once in one migration"
            ))
            .into());
        }
        let replacing = is_replacement(&statement);
        if let Some(index) = entries
            .iter()
            .position(|entry| entry.canonical_name == name)
        {
            if !replacing || !changed.insert(name.clone()) || entries[index].kind != kind {
                return Err(TopologyError::Unsupported(format!(
                    "'{name}' already exists; use CREATE OR REPLACE to preserve compatible state, or ordered DROP/CREATE for an explicit future-only reset"
                )).into());
            }
            // This private catalog has no actors. The final compiler must prove preservation,
            // including every dependent's unchanged contract, before any admission can occur.
            candidate.rollback_catalog_create(&name, kind, "isolated topology replacement")?;
            apply_statement(candidate, sql, &statement, &name).await?;
            entries[index].ddl = create_definition(sql, &statement)?;
            entries[index].schema_binding = candidate
                .connector_manager
                .lock()
                .schema_binding(&name)
                .cloned();
        } else {
            changed.insert(name.clone());
            let previous_generation = parent
                .entries
                .iter()
                .find(|entry| entry.canonical_name == name)
                .map(|entry| entry.catalog_generation)
                .or_else(|| retired_generations.get(&name).copied());
            let generation = previous_generation.map_or(Ok(1), |generation| {
                generation
                    .checked_add(1)
                    .ok_or_else(|| TopologyError::Invalid("catalog incarnation exhausted".into()))
            })?;
            if dropped.contains(&name)
                && parent
                    .entries
                    .iter()
                    .any(|entry| entry.canonical_name == name && entry.kind != kind)
            {
                return Err(TopologyError::Unsupported(
                    "recreation must retain the catalog kind".into(),
                )
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
                schema_binding: candidate
                    .connector_manager
                    .lock()
                    .schema_binding(&name)
                    .cloned(),
                canonical_name: name,
                kind,
                catalog_generation: generation,
                ddl: create_definition(sql, &statement)?,
            });
        }
    }
    let target = CatalogManifest::new(entries).map_err(TopologyError::from)?;
    candidate.reconcile_catalog_manifest_inventory(&target)?;
    Ok(target)
}

pub(super) fn is_replacement(statement: &StreamingStatement) -> bool {
    match statement {
        StreamingStatement::CreateSource(create) => create.or_replace,
        StreamingStatement::CreateSink(create) => create.or_replace,
        StreamingStatement::CreateStream { or_replace, .. } => *or_replace,
        _ => false,
    }
}

fn create_definition(sql: &str, statement: &StreamingStatement) -> Result<String, DbError> {
    if !is_replacement(statement) {
        return Ok(sql.to_owned());
    }
    // Comments were rejected by typed DDL validation. Retain the exact definition suffix so
    // cold replay executes CREATE without invoking local OR REPLACE mutation semantics.
    let mut suffix = sql.trim_start();
    for keyword in ["CREATE", "OR", "REPLACE"] {
        let boundary = suffix.find(char::is_whitespace).ok_or_else(|| {
            TopologyError::Invalid("replacement has an invalid CREATE prefix".into())
        })?;
        if !suffix[..boundary].eq_ignore_ascii_case(keyword) {
            return Err(
                TopologyError::Invalid("replacement has an invalid CREATE prefix".into()).into(),
            );
        }
        suffix = suffix[boundary..].trim_start();
    }
    Ok(format!("CREATE {suffix}"))
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
        if crate::db::connector_source_requires_schema_discovery(&statements[0])
            && entry.schema_binding.is_none()
        {
            return Err(DbError::Pipeline(format!(
                "[{}] legacy catalog source '{}' lacks an explicit durable schema and a committed schema contract; resolve it through a controlled catalog migration before activation",
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
    pub(in crate::db) async fn reconcile_superseded_catalog_objects(
        &self,
        manifest: &CatalogManifest,
        store: &CatalogManifestStore,
        topology: &TopologyCatalogState,
    ) -> Result<(), DbError> {
        let superseded: Vec<_> = self
            .catalog_manifest_inventory()?
            .into_iter()
            .filter(|local| !manifest.entries.iter().any(|entry| entry == local))
            .collect();
        if superseded.is_empty() {
            return Ok(());
        }
        let retired = store.retired_topology_names().await?;
        let parent = store
            .parent_topology_catalog(&manifest.reference().map_err(TopologyError::from)?)
            .await?;
        if superseded.iter().any(|entry| {
            !matches!(
                entry.kind,
                CatalogObjectKind::Source | CatalogObjectKind::Stream | CatalogObjectKind::Sink
            ) || if manifest
                .entries
                .iter()
                .any(|target| target.canonical_name == entry.canonical_name)
            {
                !parent
                    .as_ref()
                    .is_some_and(|parent| parent.entries.contains(entry))
            } else {
                !retired.contains(&entry.canonical_name)
            }
        }) {
            return Err(TopologyError::Conflict("local inventory conflicts with catalog manifest without a certified parent replacement or retirement".into()).into());
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
        for entry in superseded.into_iter().rev() {
            self.require_catalog_kind(&entry.canonical_name, entry.kind, false)?;
            self.rollback_catalog_create(
                &entry.canonical_name,
                entry.kind,
                "committed topology replacement or retirement",
            )?;
        }
        Ok(())
    }
}

pub(super) fn validate_prepared_targets(
    candidate: &LaminarDB,
    target: &CatalogManifest,
) -> Result<(), DbError> {
    use laminar_connectors::schema::resolution::SchemaPreparation;
    for entry in &target.entries {
        if entry.kind != CatalogObjectKind::Sink {
            continue;
        }
        let Some(binding) = &entry.schema_binding else {
            continue;
        };
        let Some(info) = candidate.connector_registry.sink_info(&binding.connector) else {
            continue;
        };
        let pending = match info.schema_capabilities.preparation {
            SchemaPreparation::None => false,
            SchemaPreparation::ExplicitTableCreation => binding.value.is_none(),
            SchemaPreparation::ExplicitRegistration => binding
                .value
                .as_ref()
                .is_some_and(|native| !native.identity.contains_key("id")),
        };
        if pending {
            return Err(TopologyError::Unsupported(format!(
                "sink '{}' requires external schema preparation; topology validation is read-only. Prepare the table/registry subject separately, then resolve the concrete target",
                entry.canonical_name)).into());
        }
    }
    Ok(())
}

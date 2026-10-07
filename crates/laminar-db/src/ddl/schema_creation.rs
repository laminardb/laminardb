//! Control-plane preparation, persistence, and catalog installation.

use futures::FutureExt;
use std::sync::Arc;

struct SchemaDependencies {
    ddl: Vec<(String, String, u64)>,
    sources: Vec<(String, Arc<crate::catalog::SourceEntry>)>,
}
use laminar_core::schema_binding::{SchemaBinding, SchemaDirection};
use laminar_sql::parser::{SinkFrom, StreamingStatement};

use crate::db::{canonical_object_name, exact_table_reference, LaminarDB};
use crate::error::DbError;
use crate::handle::ExecuteResult;

use super::schema_journal::SchemaJournal;
use super::schema_resolution::{resolution_config, RESOLVED_SCHEMA};
use super::source_sink::ConnectorKind;

impl LaminarDB {
    pub(crate) async fn execute_schema_ddl(
        &self,
        sql: &str,
        statement: &StreamingStatement,
    ) -> Result<ExecuteResult, DbError> {
        // This gate owns DDL preparation. Catalog readers remain available during discovery.
        let _creation = self.schema_creation_lock.lock().await;
        self.ensure_catalog_cleanup_unfenced("database mutation")?;
        #[cfg(feature = "cluster")]
        self.ensure_coordinated_recovery_mutation_unfenced("database mutation")?;
        let operation = match statement {
            StreamingStatement::CreateSource(_) => Some("CREATE SOURCE"),
            StreamingStatement::CreateSink(_) => Some("CREATE SINK"),
            StreamingStatement::CreateLookupTable(_) => Some("CREATE LOOKUP TABLE"),
            StreamingStatement::Standard(sql)
                if matches!(sql.as_ref(), sqlparser::ast::Statement::CreateTable(_)) =>
            {
                Some("CREATE TABLE")
            }
            _ => None,
        };
        if let Some(operation) = operation {
            self.ensure_offline_topology_ddl_allowed(operation)?;
        }
        #[cfg(feature = "cluster")]
        self.preflight_cluster_catalog_mutation(sql, statement)?;
        let mut journal = SchemaJournal::load(self).boxed().await?;
        if journal.is_some() {
            if let Some(key) = crate::db::sensitive_catalog_property(statement) {
                return Err(DbError::Config(format!("durable schema DDL cannot contain secret '{key}'; use an environment reference")));
            }
            if crate::db::catalog_ddl_contains_comment(sql)? {
                return Err(DbError::Config(
                    "durable schema DDL cannot contain comments".into(),
                ));
            }
        }
        let snapshot = {
            let _catalog = self.topology_ddl_lock.read().await;
            SchemaDependencies {
                ddl: self.connector_manager.lock().ordered_ddl(),
                sources: self
                    .catalog
                    .list_sources()
                    .into_iter()
                    .filter_map(|name| self.catalog.get_source(&name).map(|entry| (name, entry)))
                    .collect(),
            }
        };
        let prepared = self
            .prepare_statement_schema(statement, sql, journal.as_ref())
            .boxed()
            .await?;
        {
            let _catalog = self.topology_ddl_lock.read().await;
            self.recheck_schema_dependencies(&snapshot)?;
        }
        if let Some((name, binding)) = &prepared {
            if let Some(journal) = &mut journal {
                journal
                    .publish(name, sql, Some(binding.clone()))
                    .boxed()
                    .await?;
            }
        }
        let catalog_guard = self.topology_ddl_lock.write().await;
        self.recheck_schema_dependencies(&snapshot)?;
        let result = match prepared {
            Some((_, binding)) => {
                RESOLVED_SCHEMA
                    .scope(
                        Some(binding),
                        self.execute_parsed_single(sql, statement).boxed(),
                    )
                    .await?
            }
            None => self.execute_parsed_single(sql, statement).boxed().await?,
        };
        if let Some(journal) = &journal {
            if let ExecuteResult::Ddl(info) = &result {
                if let Some(generation) = journal.generation(&info.object_name) {
                    self.connector_manager
                        .lock()
                        .set_local_schema_generation(&info.object_name, generation);
                }
            }
        }
        let current = self.connector_manager.lock().ordered_ddl();
        let retired = snapshot
            .ddl
            .iter()
            .filter(|(name, _, _)| {
                !current
                    .iter()
                    .any(|(current_name, _, _)| current_name == name)
            })
            .map(|(name, _, _)| name.clone())
            .collect::<Vec<_>>();
        drop(catalog_guard);
        if let Some(journal) = &mut journal {
            if let Err(error) = journal.retire(&retired).boxed().await {
                let name = retired.first().map_or("catalog", String::as_str);
                return Err(self.terminal_catalog_cleanup_error(
                    "durable schema retirement",
                    name,
                    laminar_core::catalog::CatalogObjectKind::Source,
                    &error,
                ));
            }
        }
        Ok(result)
    }

    fn recheck_schema_dependencies(&self, snapshot: &SchemaDependencies) -> Result<(), DbError> {
        if self.shutdown.load(std::sync::atomic::Ordering::Acquire) {
            return Err(DbError::Shutdown);
        }
        self.ensure_catalog_cleanup_unfenced("schema publication")?;
        #[cfg(feature = "cluster")]
        self.ensure_coordinated_recovery_mutation_unfenced("schema publication")?;
        let unchanged_sources = self.catalog.list_sources().len() == snapshot.sources.len()
            && snapshot.sources.iter().all(|(name, entry)| {
                self.catalog
                    .get_source(name)
                    .is_some_and(|current| Arc::ptr_eq(&current, entry))
            });
        if !unchanged_sources || self.connector_manager.lock().ordered_ddl() != snapshot.ddl {
            return Err(DbError::InvalidOperation("catalog dependencies changed during schema resolution; retry against the current generation".into()));
        }
        Ok(())
    }

    fn preflight_schema_creation(&self, statement: &StreamingStatement) -> Result<bool, DbError> {
        let replacement = match statement {
            StreamingStatement::CreateSource(create) if create.or_replace => Some("SOURCE"),
            StreamingStatement::CreateSink(create) if create.or_replace => Some("SINK"),
            StreamingStatement::CreateLookupTable(create) if create.or_replace => {
                Some("LOOKUP TABLE")
            }
            StreamingStatement::Standard(sql) => match sql.as_ref() {
                sqlparser::ast::Statement::CreateTable(create) if create.or_replace => {
                    Some("TABLE")
                }
                _ => None,
            },
            _ => None,
        };
        if let Some(kind) = replacement {
            return Err(DbError::InvalidOperation(format!(
                "CREATE OR REPLACE {kind} is not atomic; use DROP/CREATE"
            )));
        }
        let if_not_exists = match statement {
            StreamingStatement::CreateSource(create) => create.if_not_exists,
            StreamingStatement::CreateSink(create) => create.if_not_exists,
            StreamingStatement::CreateLookupTable(create) => create.if_not_exists,
            StreamingStatement::Standard(statement) => matches!(statement.as_ref(),
                sqlparser::ast::Statement::CreateTable(create) if create.if_not_exists),
            _ => false,
        };
        let schema_create = match statement {
            StreamingStatement::CreateSource(_)
            | StreamingStatement::CreateSink(_)
            | StreamingStatement::CreateLookupTable(_) => {
                crate::db::catalog_create_identity(statement)?
            }
            StreamingStatement::Standard(sql)
                if matches!(sql.as_ref(), sqlparser::ast::Statement::CreateTable(_)) =>
            {
                crate::db::catalog_create_identity(statement)?
            }
            _ => None,
        };
        if let Some((name, kind, _)) = schema_create {
            super::catalog::reject_reserved_namespace(&name)?;
            if let Some(existing) = self.catalog_namespace.lock().get(&name).copied() {
                if existing != kind {
                    return Err(DbError::InvalidOperation(format!(
                        "cannot create {kind} '{name}': the identifier is owned by a {existing}"
                    )));
                }
                if !if_not_exists {
                    return Err(DbError::InvalidOperation(format!(
                        "{kind} '{name}' already exists"
                    )));
                }
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn prepare_statement_schema(
        &self,
        statement: &StreamingStatement,
        sql: &str,
        journal: Option<&SchemaJournal>,
    ) -> Result<Option<(String, SchemaBinding)>, DbError> {
        if !self.preflight_schema_creation(statement)? {
            return Ok(None);
        }
        match statement {
            StreamingStatement::CreateSource(create) => {
                self.prepare_source_definition(create, sql, journal)
                    .boxed()
                    .await
            }
            StreamingStatement::CreateSink(create) => {
                self.prepare_sink_definition(create, sql, journal)
                    .boxed()
                    .await
            }
            StreamingStatement::CreateLookupTable(create) => {
                let name = canonical_object_name(&create.name)?;
                if create.if_not_exists && self.catalog_namespace.lock().contains_key(&name) {
                    return Ok(None);
                }
                let cached = journal
                    .map(|journal| journal.replay(&name, sql))
                    .transpose()?
                    .flatten();
                let binding = match cached {
                    Some(binding) => {
                        RESOLVED_SCHEMA
                            .scope(
                                Some(binding),
                                super::lookup_schema::resolve(self, create).boxed(),
                            )
                            .await?
                    }
                    None => super::lookup_schema::resolve(self, create).boxed().await?,
                };
                Ok(binding.map(|binding| (name, binding)))
            }
            StreamingStatement::Standard(statement) => {
                let sqlparser::ast::Statement::CreateTable(create) = statement.as_ref() else {
                    return Ok(None);
                };
                let name = canonical_object_name(&create.name)?;
                if create.if_not_exists && self.catalog_namespace.lock().contains_key(&name) {
                    return Ok(None);
                }
                let options = super::table_schema::options(self, create)?;
                if options.connector_type.is_none() {
                    return Ok(None);
                }
                let cached = journal
                    .map(|journal| journal.replay(&name, sql))
                    .transpose()?
                    .flatten();
                let (_, _, binding) = match cached {
                    Some(binding) => {
                        RESOLVED_SCHEMA
                            .scope(
                                Some(binding),
                                super::table_schema::resolve(self, create, &options).boxed(),
                            )
                            .await?
                    }
                    None => {
                        super::table_schema::resolve(self, create, &options)
                            .boxed()
                            .await?
                    }
                };
                Ok(binding.map(|binding| (name, binding)))
            }
            _ => Ok(None),
        }
    }
    async fn prepare_source_definition(
        &self,
        create: &laminar_sql::parser::CreateSourceStatement,
        sql: &str,
        journal: Option<&SchemaJournal>,
    ) -> Result<Option<(String, SchemaBinding)>, DbError> {
        let name = canonical_object_name(&create.name)?;
        if create.or_replace {
            return Err(DbError::InvalidOperation(
                "CREATE OR REPLACE SOURCE is not atomic; use DROP/CREATE".into(),
            ));
        }
        if create.if_not_exists && self.catalog.get_source(&name).is_some() {
            return Ok(None);
        }
        let resolved = self.prepare_connector(
            create.connector_type.as_ref(),
            &create.connector_options,
            create.format.as_ref(),
            ConnectorKind::Source,
        )?;
        let Some(resolved) = resolved else {
            return Ok(None);
        };
        let cached = journal
            .map(|journal| journal.replay(&name, sql))
            .transpose()?
            .flatten();
        let (definition, binding) = match cached {
            Some(binding) => {
                RESOLVED_SCHEMA
                    .scope(
                        Some(binding),
                        self.resolve_source_definition(create, Some(&resolved), &name)
                            .boxed(),
                    )
                    .await?
            }
            None => {
                self.resolve_source_definition(create, Some(&resolved), &name)
                    .boxed()
                    .await?
            }
        };
        self.validate_source_input_schema_contract(&name, Some(&resolved), &definition)?;
        Ok(binding.map(|binding| (name, binding)))
    }

    async fn prepare_sink_definition(
        &self,
        create: &laminar_sql::parser::CreateSinkStatement,
        sql: &str,
        journal: Option<&SchemaJournal>,
    ) -> Result<Option<(String, SchemaBinding)>, DbError> {
        let name = canonical_object_name(&create.name)?;
        let resolved = self.prepare_connector(
            create.connector_type.as_ref(),
            &create.connector_options,
            create.format.as_ref(),
            ConnectorKind::Sink,
        )?;
        let Some(resolved) = resolved else {
            return Ok(None);
        };
        let input = match &create.from {
            SinkFrom::Table(input) => canonical_object_name(input)?,
            SinkFrom::Query(_) => {
                return Err(DbError::Unsupported(
                    "sink queries require a named CREATE STREAM".into(),
                ))
            }
        };
        let schema = {
            let _catalog = self.topology_ddl_lock.read().await;
            self.ctx
                .table_provider(exact_table_reference(&input))
                .await?
                .schema()
        };
        let candidate = crate::connector_manager::SinkRegistration {
            name: name.clone(),
            input: input.clone(),
            query_inputs: Vec::new(),
            catalog_generation: 1,
            schema_binding: None,
            connector_type: resolved.connector_type.clone(),
            connector_options: resolved.connector_options.clone(),
            format: resolved.format.clone(),
            format_options: resolved.format_options.clone(),
            filter_expr: create.filter.as_ref().map(ToString::to_string),
        };
        self.validate_sink_dependencies(&candidate)?;
        let mut config = resolution_config(&resolved)?;
        config.set(
            "_arrow_schema",
            crate::pipeline_callback::encode_arrow_schema(&schema),
        );
        config.set(
            "delivery.guarantee",
            self.config.delivery_guarantee.to_string(),
        );
        let mut sink = self.connector_registry.create_sink(&config, None)?;
        let contract = sink.contract(&config)?;
        let carries_changelog = self
            .connector_manager
            .lock()
            .streams()
            .get(&input)
            .is_some_and(|stream| stream.incremental);
        crate::pipeline_lifecycle::admit_sink_contract(
            contract,
            self.config.delivery_guarantee,
            self.runtime_mode(),
            carries_changelog,
        )
        .map_err(|reason| {
            DbError::Config(format!(
                "sink '{name}' schema preparation is not admitted: {reason}"
            ))
        })?;
        let cached = journal
            .map(|journal| journal.replay(&name, sql))
            .transpose()?
            .flatten();
        let mut binding = match cached {
            Some(binding) => {
                RESOLVED_SCHEMA
                    .scope(
                        Some(binding),
                        self.resolve_sink_binding(create, &resolved).boxed(),
                    )
                    .await?
            }
            None => self
                .connector_registry
                .resolve_sink_schema(&config, schema)
                .await
                .map_err(|error| {
                    DbError::Connector(format!(
                        "sink '{name}' ({}) schema resolution: {error}",
                        config.connector_type()
                    ))
                })?,
        };
        if binding.direction != SchemaDirection::Sink {
            return Err(DbError::Config(
                "sink resolver returned a reader contract".into(),
            ));
        }
        let resolved_binding = binding.clone();
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            sink.prepare_schema(&config, &mut binding),
        )
        .await
        .map_err(|_| DbError::Connector("sink schema preparation exceeded 30 seconds".into()))??;
        laminar_connectors::schema::resolution::validate_prepared_writer(
            &config,
            &resolved_binding,
            &binding,
        )
        .map_err(|error| {
            DbError::Connector(format!("sink '{name}' schema preparation: {error}"))
        })?;
        Ok(Some((name, binding)))
    }
}

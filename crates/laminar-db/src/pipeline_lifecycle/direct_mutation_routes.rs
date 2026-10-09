//! Admission of keyed-upsert sources read only by direct keyed-mutation sinks.

use rustc_hash::FxHashSet;

use super::{
    admit_source_recovery_contract, DbError, FxHashMap, HashMap, LaminarDB,
    OrderedIntervalAdmissions, RuntimeMode, TemporalSourceRole,
};
use crate::direct_mutation::{direct_sink_consumers, validate_route_shape};
use laminar_connectors::connector::SourceInputMode;

type Registrations<'a> = (
    &'a HashMap<String, crate::connector_manager::SourceRegistration>,
    &'a HashMap<String, crate::connector_manager::SinkRegistration>,
    &'a HashMap<String, crate::connector_manager::StreamRegistration>,
);

/// Cluster sinks read streams only: a sink reading a source directly is delivered on the
/// coordinator that owns the source, which has no certified cluster placement.
pub(super) fn cluster_source_sink_error(sink: &str, source: &str) -> DbError {
    DbError::Config(format!(
        "cluster sink '{sink}' reads source '{source}' directly; read it through a CREATE STREAM \
         in cluster mode (direct source sinks are supported in embedded and single-node mode)"
    ))
}

impl LaminarDB {
    fn reject_cluster_source_sink(
        &self,
        sink: &crate::connector_manager::SinkRegistration,
    ) -> Result<(), DbError> {
        if sink.connector_type.is_none() || self.catalog.get_source(&sink.input).is_none() {
            return Ok(());
        }
        Err(cluster_source_sink_error(&sink.name, &sink.input))
    }

    /// Require every mutation source to be owned by exactly one admitted route; returns the
    /// sources admitted to the direct keyed-mutation sink route.
    pub(crate) fn validate_mutation_source_routes(
        &self,
        (source_regs, sink_regs, stream_regs): Registrations<'_>,
        temporal_source_roles: &FxHashMap<String, TemporalSourceRole>,
        ordered_interval_admissions: &OrderedIntervalAdmissions,
        runtime: RuntimeMode,
    ) -> Result<FxHashSet<String>, DbError> {
        if runtime == RuntimeMode::Cluster {
            for sink in sink_regs.values() {
                self.reject_cluster_source_sink(sink)?;
            }
        }
        let direct =
            self.validate_direct_mutation_routes(source_regs, sink_regs, stream_regs, runtime)?;
        let mut names = source_regs.keys().collect::<Vec<_>>();
        names.sort_unstable();
        for name in names {
            self.validate_registered_mutation_source_admission(
                name,
                source_regs,
                temporal_source_roles,
                ordered_interval_admissions,
                &direct,
            )?;
        }
        Ok(direct)
    }

    /// The catalog entry of `input` when it is a configured keyed-upsert source, whose direct
    /// sinks receive `_op` rows.
    pub(crate) fn keyed_mutation_source(
        &self,
        input: &str,
    ) -> Result<Option<std::sync::Arc<crate::catalog::SourceEntry>>, DbError> {
        let Some(entry) = self.catalog.get_source(input) else {
            return Ok(None);
        };
        let source_regs = self.connector_manager.lock().sources().clone();
        let keyed = self
            .resolve_registered_source_contract(input, &source_regs)?
            .is_some_and(|(contract, _)| contract.input_mode == SourceInputMode::KeyedUpsert);
        Ok(keyed.then_some(entry))
    }

    /// The schema a sink binds for `input` and whether it is a direct keyed-mutation input,
    /// whose sinks receive the source fields plus `_op`.
    pub(crate) async fn bound_sink_input_schema(
        &self,
        input: &str,
    ) -> Result<(arrow_schema::SchemaRef, bool), DbError> {
        if let Some(source) = self.keyed_mutation_source(input)? {
            return Ok((
                crate::direct_mutation::sink_input_schema(&source.schema),
                true,
            ));
        }
        let _catalog = self.topology_ddl_lock.read().await;
        let schema = self
            .ctx
            .table_provider(crate::db::exact_table_reference(input))
            .await?
            .schema();
        Ok((schema, false))
    }

    /// The per-cycle input owner for every sink that reads a source by name.
    pub(super) fn direct_sink_inputs(
        &self,
        sink_regs: &HashMap<String, crate::connector_manager::SinkRegistration>,
        direct_mutation_sources: &FxHashSet<String>,
    ) -> crate::direct_mutation::DirectSinkInputs {
        use crate::direct_mutation::{sink_input_schema, DirectSinkInput};

        crate::direct_mutation::DirectSinkInputs::new(
            sink_regs
                .values()
                .filter(|sink| sink.connector_type.is_some())
                .filter_map(|sink| {
                    let entry = self.catalog.get_source(&sink.input)?;
                    let input = if direct_mutation_sources.contains(&sink.input) {
                        DirectSinkInput::KeyedMutations(sink_input_schema(&entry.schema))
                    } else {
                        DirectSinkInput::Append
                    };
                    Some((std::sync::Arc::<str>::from(sink.input.as_str()), input))
                })
                .collect(),
        )
    }

    /// Keyed-upsert sources whose only consumers are plain direct sinks, after validating each
    /// route's key, schema, filters, and recovery contract. Mixed consumers are left to the
    /// stateful-route validators, which reject them unless a certified operator owns the source.
    pub(crate) fn validate_direct_mutation_routes(
        &self,
        source_regs: &HashMap<String, crate::connector_manager::SourceRegistration>,
        sink_regs: &HashMap<String, crate::connector_manager::SinkRegistration>,
        stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
        runtime: RuntimeMode,
    ) -> Result<FxHashSet<String>, DbError> {
        let mut names = source_regs.keys().collect::<Vec<_>>();
        names.sort_unstable();
        let mut admitted = FxHashSet::default();
        for name in names {
            let Some((contract, _)) = self.resolve_registered_source_contract(name, source_regs)?
            else {
                continue;
            };
            if contract.input_mode != SourceInputMode::KeyedUpsert {
                continue;
            }
            let Some(sinks) = direct_sink_consumers(name, stream_regs.values(), sink_regs.values())
            else {
                continue;
            };
            let entry = self.catalog.get_source(name).ok_or_else(|| {
                DbError::Config(format!(
                    "direct mutation source '{name}' is absent from the source catalog"
                ))
            })?;
            validate_route_shape(name, &entry.schema, &entry.primary_key, &sinks)
                .map_err(DbError::Config)?;
            admit_source_recovery_contract(
                contract,
                self.config.delivery_guarantee,
                self.config.checkpoint.is_some(),
                runtime,
            )
            .map_err(|reason| {
                DbError::Config(format!(
                    "direct mutation source '{name}' is not admissible in {runtime:?} mode with \
                     {} delivery: {reason} (contract: {contract:?})",
                    self.config.delivery_guarantee
                ))
            })?;
            admitted.insert(name.clone());
        }
        Ok(admitted)
    }
}

use super::{
    admit_append_only_source, Arc, CheckpointStorageScope, ConnectorConfig,
    ConnectorTaskFenceRegistration, DbError, DeliveryGuarantee, FxHashMap, HashMap,
    RecoveredInputChannelProgress, RuntimeMode, SinkConnector, SinkConsistency, SinkContract,
    SinkTopology, SourceConsistency, SourceContract, SourceInputMode, SourceRowPositionCapability,
    SourceTopology, SourceWatermarkState, CLUSTER_BEST_EFFORT, EXACT_SINK_PROTOCOL,
    KEYED_SOURCE_PRIMARY_KEY,
};

/// Validate source durability and placement before the connector performs I/O.
pub(super) fn admit_source_contract(
    contract: SourceContract,
    has_primary_key: bool,
    has_reserved_mutation_columns: bool,
    delivery: DeliveryGuarantee,
    checkpointing_enabled: bool,
    runtime: RuntimeMode,
) -> Result<(), &'static str> {
    if runtime == RuntimeMode::Cluster && delivery == DeliveryGuarantee::BestEffort {
        return Err(CLUSTER_BEST_EFFORT);
    }
    if contract.input_mode == SourceInputMode::KeyedUpsert && !has_primary_key {
        return Err(KEYED_SOURCE_PRIMARY_KEY);
    }
    admit_append_only_source(contract, has_reserved_mutation_columns)?;
    admit_source_recovery_contract(contract, delivery, checkpointing_enabled, runtime)
}

pub(super) fn admit_source_recovery_contract(
    contract: SourceContract,
    delivery: DeliveryGuarantee,
    checkpointing_enabled: bool,
    runtime: RuntimeMode,
) -> Result<(), &'static str> {
    if delivery == DeliveryGuarantee::ExactlyOnce && !contract.is_exact_delivery_certified() {
        return Err(
            "[LDB-5037] exactly-once source delivery is not production-certified for this \
             connector contract",
        );
    }
    if contract.consistency == SourceConsistency::CommitCoupled {
        if delivery == DeliveryGuarantee::ExactlyOnce {
            return Err(
                "exactly-once commit-coupled sources require a certified in-flight \
                 transaction/barrier checkpoint cut, which is not implemented",
            );
        }
        if delivery != DeliveryGuarantee::AtLeastOnce {
            return Err("commit-coupled sources currently support only at-least-once delivery");
        }
        if !checkpointing_enabled {
            return Err(
                "commit-coupled sources require checkpointing so upstream retention can advance",
            );
        }
    }

    if delivery != DeliveryGuarantee::BestEffort
        && contract.consistency == SourceConsistency::Ephemeral
    {
        return Err("at-least-once and exactly-once delivery require replayable sources");
    }

    if runtime == RuntimeMode::Cluster {
        match contract.topology {
            SourceTopology::Splittable => {}
            SourceTopology::NodeLocalIngress => {
                return Err(
                    "cluster node-local ingress has no defined rebalance/state-loss contract",
                );
            }
            SourceTopology::Singleton => {
                return Err(
                    "cluster singleton sources require fenced singleton placement, which is not implemented",
                );
            }
        }
    }

    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TemporalSourceRole {
    Left,
    Right,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct OrderedIntervalAdmissions {
    pub(crate) joins:
        FxHashMap<String, [crate::operator::interval_join_input::BoundedJoinInputMode; 2]>,
    /// Only non-append sources are present. Absence means the ordinary append-only route.
    pub(crate) source_modes: FxHashMap<String, SourceInputMode>,
}

impl TemporalSourceRole {
    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::Left => "left",
            Self::Right => "right",
        }
    }
}

pub(super) fn admit_temporal_source_contract(
    contract: SourceContract,
    role: TemporalSourceRole,
    has_primary_key: bool,
    has_reserved_mutation_columns: bool,
    delivery: DeliveryGuarantee,
    checkpointing_enabled: bool,
    runtime: RuntimeMode,
) -> Result<(), &'static str> {
    if runtime == RuntimeMode::Cluster && delivery == DeliveryGuarantee::BestEffort {
        return Err(CLUSTER_BEST_EFFORT);
    }
    if delivery != DeliveryGuarantee::BestEffort && !checkpointing_enabled {
        return Err(
            "at-least-once and exactly-once temporal joins require checkpointing for state and source-offset recovery",
        );
    }
    if contract.row_positions != SourceRowPositionCapability::OrderedDeterministic {
        return Err("temporal joins require ordered deterministic row positions");
    }
    if has_reserved_mutation_columns {
        return Err("temporal source schemas cannot declare engine-owned mutation columns");
    }
    match (role, contract.input_mode) {
        (TemporalSourceRole::Left, SourceInputMode::AppendOnly)
        | (TemporalSourceRole::Right, SourceInputMode::AppendOnly | SourceInputMode::KeyedUpsert) =>
            {}
        (TemporalSourceRole::Left, _) => {
            return Err("temporal left inputs must be append-only");
        }
        (TemporalSourceRole::Right, SourceInputMode::FullChangelog) => {
            return Err(
                "temporal right inputs support append-only or keyed-upsert mutations, not full changelogs",
            );
        }
    }
    if contract.input_mode == SourceInputMode::KeyedUpsert && !has_primary_key {
        return Err(KEYED_SOURCE_PRIMARY_KEY);
    }
    admit_source_recovery_contract(contract, delivery, checkpointing_enabled, runtime)
}

pub(super) fn has_only_temporal_right_consumers(
    source: &str,
    stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
    sink_regs: &HashMap<String, crate::connector_manager::SinkRegistration>,
) -> bool {
    if sink_regs
        .values()
        .any(|sink| sink.input == source || sink.query_inputs.iter().any(|input| input == source))
    {
        return false;
    }
    let mut consumed = false;
    for stream in stream_regs.values() {
        if let Some([laminar_sql::translator::JoinOperatorConfig::Temporal(config)]) =
            stream.join_config.as_deref()
        {
            if config.right_table == source && config.left_table != source {
                consumed = true;
                continue;
            }
            if config.left_table == source || config.right_table == source {
                return false;
            }
        }
        if crate::sql_analysis::extract_table_references(&stream.query_sql).contains(source) {
            return false;
        }
    }
    consumed
}

pub(super) fn has_only_ordered_interval_consumers(
    source: &str,
    stream_regs: &HashMap<String, crate::connector_manager::StreamRegistration>,
    sink_regs: &HashMap<String, crate::connector_manager::SinkRegistration>,
    admitted_joins: &FxHashMap<
        String,
        [crate::operator::interval_join_input::BoundedJoinInputMode; 2],
    >,
) -> bool {
    if sink_regs
        .values()
        .any(|sink| sink.input == source || sink.query_inputs.iter().any(|input| input == source))
    {
        return false;
    }
    let mut consumed = false;
    for stream in stream_regs.values() {
        let references = crate::sql_analysis::extract_table_references(&stream.query_sql);
        let configured_input = stream.join_config.as_deref().is_some_and(|joins| {
            matches!(
                joins,
                [laminar_sql::translator::JoinOperatorConfig::StreamStream(config)]
                    if config.left_table == source || config.right_table == source
            )
        });
        if !configured_input && !references.contains(source) {
            continue;
        }
        consumed = true;
        if !admitted_joins.contains_key(&stream.name) || !configured_input {
            return false;
        }
    }
    consumed
}

pub(super) fn validate_source_recovery_assignment(
    source: &str,
    assignment_scoped: bool,
    checkpoint: Option<&laminar_core::checkpoint::ConnectorCheckpoint>,
    expected_assignment: Option<std::num::NonZeroU64>,
) -> Result<(), DbError> {
    let captured = checkpoint.and_then(|checkpoint| checkpoint.source_assignment_version);
    match (assignment_scoped, expected_assignment, captured) {
        (true, None, _) => Err(DbError::Checkpoint(format!(
            "[LDB-6055] cluster-assigned source '{source}' recovery has no authoritative assignment fence"
        ))),
        (true, Some(_), None) => Err(DbError::Checkpoint(format!(
            "[LDB-6055] cluster-assigned source '{source}' recovery checkpoint is missing its assignment version"
        ))),
        (true, Some(expected), Some(captured)) if captured != expected => {
            Err(DbError::Checkpoint(format!(
                "[LDB-6055] source '{source}' recovery checkpoint captured assignment version {captured}, committed fence is {expected}"
            )))
        }
        (false, _, Some(captured)) => Err(DbError::Checkpoint(format!(
            "[LDB-6055] non-assigned source '{source}' recovery checkpoint unexpectedly carries assignment version {captured}"
        ))),
        _ => Ok(()),
    }
}

/// Validate sink durability, placement, and changelog semantics before I/O.
pub(super) fn admit_sink_contract(
    contract: SinkContract,
    delivery: DeliveryGuarantee,
    runtime: RuntimeMode,
    carries_changelog: bool,
) -> Result<(), &'static str> {
    if runtime == RuntimeMode::Cluster && delivery == DeliveryGuarantee::BestEffort {
        return Err(CLUSTER_BEST_EFFORT);
    }
    match (delivery, contract.consistency) {
        (DeliveryGuarantee::ExactlyOnce, SinkConsistency::CheckpointCommittable) => {}
        (DeliveryGuarantee::ExactlyOnce, _) => return Err(EXACT_SINK_PROTOCOL),
        (_, SinkConsistency::CheckpointCommittable) => {
            return Err(
                "checkpoint-committable sinks require global exactly-once delivery; running the \
                 coordinated protocol under a weaker label is not supported",
            );
        }
        _ => {}
    }
    if runtime == RuntimeMode::Cluster
        && delivery == DeliveryGuarantee::ExactlyOnce
        && !contract.is_cluster_exact_delivery_certified()
    {
        return Err(
            "cluster exactly-once requires a certified immutable phase-one sink with an atomic, \
             namespaced external checkpoint cursor",
        );
    }

    if delivery == DeliveryGuarantee::AtLeastOnce
        && contract.consistency == SinkConsistency::Ephemeral
    {
        return Err("at-least-once delivery requires a durably acknowledged sink");
    }

    if runtime == RuntimeMode::Cluster {
        match contract.topology {
            SinkTopology::MultiWriter => {}
            SinkTopology::NodeLocalEgress => {
                return Err(
                    "cluster node-local egress has no defined rebalance/state-loss contract",
                );
            }
            SinkTopology::Singleton => {
                return Err(
                    "cluster singleton sinks require fenced singleton placement, which is not implemented",
                );
            }
        }
    }

    if carries_changelog && !contract.accepts_full_changelog() {
        return Err(
            "the input carries deletes/retractions and requires FullChangelog sink semantics; \
             append-only or keyed-upsert alone is insufficient",
        );
    }

    Ok(())
}

/// Immutable facts required to admit one configured sink before external I/O.
#[derive(Clone, Copy)]
pub(super) struct SinkAdmissionContext<'a> {
    pub(super) config: &'a ConnectorConfig,
    pub(super) name: &'a str,
    pub(super) input: &'a str,
    pub(super) delivery: DeliveryGuarantee,
    pub(super) runtime: RuntimeMode,
    pub(super) carries_changelog: bool,
    pub(super) checkpointing_enabled: bool,
    pub(super) checkpoint_storage_scope: CheckpointStorageScope,
}

pub(super) struct PreparedSink {
    pub(super) name: String,
    pub(super) connector: Box<dyn SinkConnector>,
    pub(super) config: ConnectorConfig,
    pub(super) filter_expr: Option<String>,
    pub(super) input: String,
    pub(super) contract: SinkContract,
    pub(super) expects_changelog: bool,
    pub(super) write_timeout: std::time::Duration,
    pub(super) flush_interval: std::time::Duration,
    pub(super) requires_recovery_on_error: bool,
    pub(super) task_fence: ConnectorTaskFenceRegistration,
}

pub(super) type PipelineSink = (
    String,
    crate::sink_task::SinkTaskHandle,
    Option<String>,
    String,
    SinkContract,
    bool,
);

pub(super) struct PipelineSinkSetup {
    pub(super) sinks: Vec<PipelineSink>,
    pub(super) sink_event_rx: laminar_core::streaming::AsyncConsumer<crate::sink_task::SinkEvent>,
    #[cfg(feature = "cluster")]
    pub(super) callback_controller: Option<Arc<laminar_core::cluster::control::ClusterController>>,
}

pub(super) struct PipelineRecoveryState {
    pub(super) graph: crate::operator_graph::OperatorGraph,
    pub(super) recovered_mv_store: crate::mv_store::MvStore,
    pub(super) recovered_channel_progress:
        FxHashMap<String, FxHashMap<Box<[u8]>, RecoveredInputChannelProgress>>,
    pub(super) recovered_input_channels: FxHashMap<String, Arc<[Vec<u8>]>>,
    /// Exact source-keyed cuts owned by the committed checkpoint decision. Unlike physical
    /// channel progress, this map retains a source's cut across an empty current inventory.
    pub(super) recovered_source_watermarks: FxHashMap<String, i64>,
    /// `None` denotes a fresh start. The version distinguishes cumulative v4 source cuts from
    /// legacy v3 indices whose empty current inventory cannot reconstruct an erased prior cut.
    pub(super) recovered_checkpoint_index_version: Option<u32>,
    pub(super) recovered_watermark_frontier: Option<i64>,
    pub(super) restored_reference_tables: bool,
}

pub(super) struct PipelineWatermarks {
    pub(super) stream_entries: Vec<Arc<crate::catalog::StreamEntry>>,
    pub(super) watermark_states: FxHashMap<String, SourceWatermarkState>,
    pub(super) source_entries: FxHashMap<String, Arc<crate::catalog::SourceEntry>>,
    pub(super) source_ids: FxHashMap<String, usize>,
    pub(super) source_names: Vec<String>,
    pub(super) tracker: Option<laminar_core::time::WatermarkTracker>,
}

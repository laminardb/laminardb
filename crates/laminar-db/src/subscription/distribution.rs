//! Planner-owned subscription output distribution before durable runtime binding.

use laminar_core::checkpoint::{
    ChangelogMode, OutputDistribution, OutputPartitionId, SubscriptionDigest,
};
#[cfg(feature = "cluster")]
use laminar_core::checkpoint::{
    OutputDistributionCertificate, PipelineIdentity, StreamGeneration, SubscriptionProtocolVersion,
    OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
};
use laminar_core::state::{KeyGroupCount, PARTITIONING_ABI_VERSION};
use sqlparser::ast::{GroupByExpr, SetExpr, Statement};

use crate::error::DbError;
use crate::operator::capability::SubscriptionOutputDistribution;

/// Planner-certified distribution fields that do not depend on deployment startup.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PlannedSubscriptionOutput {
    distribution: OutputDistribution,
    query_fingerprint: SubscriptionDigest,
    canonical_query: String,
    changelog_mode: ChangelogMode,
}

impl PlannedSubscriptionOutput {
    /// Bind the planner proof to the exact durable runtime identities.
    #[cfg(feature = "cluster")]
    pub(crate) fn bind(
        &self,
        deployment_id: uuid::Uuid,
        catalog_generation: u64,
        stream_name: &str,
        schema_fingerprint: SubscriptionDigest,
        history_retention_bytes: u64,
        pipeline_identity: PipelineIdentity,
        key_group_count: KeyGroupCount,
    ) -> Result<OutputDistributionCertificate, DbError> {
        let certificate = OutputDistributionCertificate {
            version: OUTPUT_DISTRIBUTION_CERTIFICATE_VERSION,
            protocol_version: SubscriptionProtocolVersion::CURRENT,
            stream_id: stream_name.to_owned(),
            catalog_generation,
            stream_generation: StreamGeneration::derive(
                deployment_id,
                catalog_generation,
                stream_name,
                &self.canonical_query,
                &pipeline_identity,
            ),
            final_operator_id: format!("stream:{stream_name}"),
            distribution: self.distribution.clone(),
            schema_fingerprint,
            changelog_mode: self.changelog_mode,
            history_retention_bytes,
            query_fingerprint: self.query_fingerprint,
            pipeline_identity,
        };
        certificate.validate(key_group_count).map_err(|error| {
            DbError::InvalidOperation(format!(
                "subscription output-distribution certificate is invalid: {error}"
            ))
        })?;
        Ok(certificate)
    }

    #[cfg(test)]
    pub(crate) const fn distribution(&self) -> &OutputDistribution {
        &self.distribution
    }

    pub(crate) const fn is_vnode_partitioned(&self) -> bool {
        matches!(
            self.distribution,
            OutputDistribution::VnodePartitioned { .. }
        )
    }

    pub(crate) fn matches_aggregate_grouping(&self, group_columns: usize) -> bool {
        match &self.distribution {
            OutputDistribution::VnodePartitioned { .. } => group_columns != 0,
            OutputDistribution::Singleton { partition } => {
                group_columns == 0 && partition.get() == 0
            }
        }
    }

    #[cfg(feature = "cluster")]
    pub(crate) const fn changelog_mode(&self) -> ChangelogMode {
        self.changelog_mode
    }
}

/// Certify only final SQL aggregate shapes already classified by the physical operator inventory.
pub(crate) fn plan(
    query_sql: &str,
    emit_clause: Option<&laminar_sql::parser::EmitClause>,
    context: &datafusion::prelude::SessionContext,
    key_group_count: KeyGroupCount,
) -> Result<Option<PlannedSubscriptionOutput>, DbError> {
    let capability = crate::operator::sql_query::classify_sql_capability(query_sql, context);
    let Some(distribution) = capability.subscription_output else {
        return Ok(None);
    };
    let canonical_query = crate::pipeline_identity::canonical_sql(query_sql);
    let query_fingerprint = SubscriptionDigest::for_bytes(
        b"laminardb-subscription-query-v1",
        canonical_query.as_bytes(),
    );
    let distribution = match distribution {
        SubscriptionOutputDistribution::VnodePartitioned => OutputDistribution::VnodePartitioned {
            key_expressions_fingerprint: group_expression_fingerprint(query_sql)?,
            partition_abi: PARTITIONING_ABI_VERSION,
            vnode_count: key_group_count.get(),
        },
        SubscriptionOutputDistribution::Singleton => OutputDistribution::Singleton {
            partition: OutputPartitionId::new(0),
        },
    };
    let changelog_mode = if emit_clause
        .is_some_and(|emit| matches!(emit, laminar_sql::parser::EmitClause::Changes))
    {
        ChangelogMode::WeightedRetractInsert
    } else {
        ChangelogMode::FullPartitionSnapshot
    };
    Ok(Some(PlannedSubscriptionOutput {
        distribution,
        query_fingerprint,
        canonical_query,
        changelog_mode,
    }))
}

fn group_expression_fingerprint(query_sql: &str) -> Result<SubscriptionDigest, DbError> {
    let statements = laminar_sql::parse_streaming_sql(query_sql)?;
    let [laminar_sql::parser::StreamingStatement::Standard(statement)] = statements.as_slice()
    else {
        return Err(DbError::InvalidOperation(
            "certified aggregate query did not contain one standard SQL statement".into(),
        ));
    };
    let Statement::Query(query) = statement.as_ref() else {
        return Err(DbError::InvalidOperation(
            "certified aggregate output is not a query".into(),
        ));
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(DbError::InvalidOperation(
            "certified aggregate output is not a direct SELECT".into(),
        ));
    };
    let GroupByExpr::Expressions(expressions, modifiers) = &select.group_by else {
        return Err(DbError::InvalidOperation(
            "GROUP BY ALL is not a certifiable subscription distribution".into(),
        ));
    };
    if expressions.is_empty() || !modifiers.is_empty() {
        return Err(DbError::InvalidOperation(
            "keyed subscription output requires canonical grouping expressions".into(),
        ));
    }
    let expressions = expressions
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>();
    let encoded = serde_json::to_vec(&expressions).map_err(|error| {
        DbError::InvalidOperation(format!(
            "could not encode subscription grouping expressions: {error}"
        ))
    })?;
    Ok(SubscriptionDigest::for_bytes(
        b"laminardb-subscription-group-expressions-v1",
        &encoded,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn groups() -> KeyGroupCount {
        KeyGroupCount::try_from(8_u16).unwrap()
    }

    #[test]
    fn keyed_and_singleton_aggregates_are_explicitly_certified() {
        let context = laminar_sql::create_session_context();
        let keyed = plan(
            "SELECT account_id, SUM(pnl) FROM trades GROUP BY account_id",
            Some(&laminar_sql::parser::EmitClause::Changes),
            &context,
            groups(),
        )
        .unwrap()
        .unwrap();
        assert!(matches!(
            keyed.distribution(),
            OutputDistribution::VnodePartitioned { vnode_count: 8, .. }
        ));

        let singleton = plan("SELECT SUM(pnl) FROM trades", None, &context, groups())
            .unwrap()
            .unwrap();
        assert_eq!(
            singleton.distribution(),
            &OutputDistribution::Singleton {
                partition: OutputPartitionId::new(0)
            }
        );
    }

    #[test]
    fn unsupported_shapes_have_no_distribution_certificate() {
        let context = laminar_sql::create_session_context();
        for sql in [
            "SELECT account_id FROM trades",
            "SELECT account_id FROM trades GROUP BY account_id",
            "SELECT a.id FROM a JOIN b ON a.id = b.id",
            "SELECT TUMBLE(ts, INTERVAL '1' MINUTE), SUM(pnl) FROM trades GROUP BY TUMBLE(ts, INTERVAL '1' MINUTE)",
        ] {
            assert!(plan(sql, None, &context, groups()).unwrap().is_none(), "{sql}");
        }
    }

    #[test]
    fn grouping_expression_order_changes_the_certificate() {
        let context = laminar_sql::create_session_context();
        let first = plan(
            "SELECT a, b, SUM(v) FROM t GROUP BY a, b",
            None,
            &context,
            groups(),
        )
        .unwrap()
        .unwrap();
        let second = plan(
            "SELECT a, b, SUM(v) FROM t GROUP BY b, a",
            None,
            &context,
            groups(),
        )
        .unwrap()
        .unwrap();
        assert_ne!(first.distribution(), second.distribution());
    }
}

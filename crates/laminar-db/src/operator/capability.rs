//! Auditable inventory of the current cluster-state shape of graph operators.
//!
//! This module is deliberately descriptive. Cluster SQL admission remains in
//! `ddl::validate_cluster_query_shape`; no value here is positive proof that a future managed
//! state contract is complete.

/// How retained operator data is owned conceptually.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperatorStateClass {
    /// No retained data influences a later input batch.
    Stateless,
    /// Retained state is intentionally owned by the vnode-zero singleton.
    GlobalSingleton,
    /// Retained state and output are owned by a canonical key's vnode.
    VnodeKeyed,
    /// Read-only state is rebuilt from a versioned replicated snapshot.
    RebuildableReplicated,
    /// Retained state has no distributed ownership contract.
    LocalOnly,
}

/// Durable working-state contract implemented by an operator.
///
/// This is independent of runtime mode. Embedded and single-node execution use the same state
/// implementation and codec; cluster execution additionally projects this contract onto vnode
/// ownership and rebalance. A state class without a managed contract remains descriptive only and
/// must not participate in the managed lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ManagedStateContract {
    /// The existing incremental SQL aggregate checkpoint codec.
    SqlAggregateV1,
    /// Vnode-local event-time windows, sessions, accumulators, and implicit timers.
    CoreWindowV1,
    /// Vnode-local retained rows, relation weights, match support, and event-time join state.
    BoundedIntervalJoinV3,
    /// Vnode-local version history, probes, frontiers, and timers for temporal joins.
    TemporalJoinV1,
    #[cfg(test)]
    TestVnodeStateV1,
}

/// Current relationship between this inventory entry and cluster DDL admission.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ClusterExecutionStatus {
    /// Existing DDL validation independently guards this shape.
    ///
    /// This is not a planner certificate and must never be used by itself to admit a query.
    DdlGuarded,
    /// Existing DDL validation rejects this shape fail-closed.
    Rejected {
        /// Concise inventory reason; user-facing errors remain owned by the DDL validator.
        reason: &'static str,
    },
    /// Runtime scaffolding that is never a user-admissible physical operator.
    InternalOnly,
}

/// Concrete production implementations of [`crate::operator_graph::GraphOperator`].
///
/// Keep one variant per concrete implementation. The exhaustive match in
/// [`OperatorCapability::fixed`] makes additions a compile-time review point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OperatorImplementation {
    SourcePassthrough,
    Tombstoned,
    SqlFilter,
    ChangelogEnrich,
    AiInference,
    EowcQuery,
    IntervalJoin,
    LookupEnrich,
    SqlQuery,
    TemporalFilter,
    Rejecting,
    TemporalJoin,
    WindowFrame,
    #[cfg(test)]
    TestProbe,
}

/// Mandatory, admission-neutral description returned by every graph operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OperatorCapability {
    pub(crate) implementation: OperatorImplementation,
    pub(crate) state_class: OperatorStateClass,
    pub(crate) cluster_status: ClusterExecutionStatus,
    pub(crate) managed_state: Option<ManagedStateContract>,
}

impl OperatorCapability {
    const fn ddl_guarded(
        implementation: OperatorImplementation,
        state_class: OperatorStateClass,
    ) -> Self {
        Self {
            implementation,
            state_class,
            cluster_status: ClusterExecutionStatus::DdlGuarded,
            managed_state: None,
        }
    }

    const fn rejected(
        implementation: OperatorImplementation,
        state_class: OperatorStateClass,
        reason: &'static str,
    ) -> Self {
        Self {
            implementation,
            state_class,
            cluster_status: ClusterExecutionStatus::Rejected { reason },
            managed_state: None,
        }
    }

    const fn internal(
        implementation: OperatorImplementation,
        state_class: OperatorStateClass,
    ) -> Self {
        Self {
            implementation,
            state_class,
            cluster_status: ClusterExecutionStatus::InternalOnly,
            managed_state: None,
        }
    }

    const fn with_managed_state(mut self, contract: ManagedStateContract) -> Self {
        self.managed_state = Some(contract);
        self
    }

    /// Fixed classification for every non-polymorphic production implementation.
    ///
    /// `SqlQuery` is conservatively rejected here. Its implementation supplies a shape-aware
    /// descriptor instead, after classifying the immutable SQL text at construction.
    pub(crate) const fn fixed(implementation: OperatorImplementation) -> Self {
        use OperatorImplementation as Implementation;
        use OperatorStateClass as State;

        match implementation {
            Implementation::SourcePassthrough | Implementation::SqlFilter => {
                Self::ddl_guarded(implementation, State::Stateless)
            }
            Implementation::Tombstoned | Implementation::Rejecting => {
                Self::internal(implementation, State::Stateless)
            }
            Implementation::ChangelogEnrich => Self::rejected(
                implementation,
                State::RebuildableReplicated,
                "replicated lookup state has no cluster snapshot/version contract",
            ),
            Implementation::TemporalJoin => Self::rejected(
                implementation,
                State::VnodeKeyed,
                "temporal joins require the managed temporal operator construction path",
            ),
            Implementation::AiInference => Self::rejected(
                implementation,
                State::LocalOnly,
                "checkpointed in-flight inference has no vnode ownership lifecycle",
            ),
            Implementation::IntervalJoin => Self::rejected(
                implementation,
                State::LocalOnly,
                "join state has no co-partitioned vnode ownership lifecycle",
            ),
            Implementation::EowcQuery => Self::rejected(
                implementation,
                State::VnodeKeyed,
                "window queries require the managed CoreWindow construction path",
            ),
            Implementation::LookupEnrich => Self::rejected(
                implementation,
                State::LocalOnly,
                "checkpointed pending lookup input has no vnode ownership lifecycle",
            ),
            Implementation::TemporalFilter => Self::rejected(
                implementation,
                State::LocalOnly,
                "retracting buffered rows have no vnode ownership lifecycle",
            ),
            Implementation::WindowFrame => Self::rejected(
                implementation,
                State::LocalOnly,
                "retained analytic-frame history has no vnode ownership lifecycle",
            ),
            Implementation::SqlQuery => Self::rejected(
                implementation,
                State::LocalOnly,
                "SQL execution shape has not been classified",
            ),
            #[cfg(test)]
            Implementation::TestProbe => Self::internal(implementation, State::LocalOnly),
        }
    }

    /// Descriptor for a known stateless SQL query shape.
    pub(crate) const fn stateless_sql_query() -> Self {
        Self::ddl_guarded(
            OperatorImplementation::SqlQuery,
            OperatorStateClass::Stateless,
        )
    }

    /// Descriptor for the existing singleton global-aggregate path.
    pub(crate) const fn global_sql_aggregate() -> Self {
        Self::ddl_guarded(
            OperatorImplementation::SqlQuery,
            OperatorStateClass::GlobalSingleton,
        )
        .with_managed_state(ManagedStateContract::SqlAggregateV1)
    }

    /// Managed vnode state for TUMBLE, HOP, and SESSION aggregates.
    pub(crate) const fn managed_core_window() -> Self {
        Self::ddl_guarded(
            OperatorImplementation::EowcQuery,
            OperatorStateClass::VnodeKeyed,
        )
        .with_managed_state(ManagedStateContract::CoreWindowV1)
    }

    /// Descriptor for the one distributed stream-stream join shape admitted by the runtime.
    pub(crate) const fn bounded_interval_join() -> Self {
        Self::ddl_guarded(
            OperatorImplementation::IntervalJoin,
            OperatorStateClass::VnodeKeyed,
        )
        .with_managed_state(ManagedStateContract::BoundedIntervalJoinV3)
    }

    /// Descriptor for the managed temporal operator.
    pub(crate) const fn managed_temporal_join() -> Self {
        Self::ddl_guarded(
            OperatorImplementation::TemporalJoin,
            OperatorStateClass::VnodeKeyed,
        )
        .with_managed_state(ManagedStateContract::TemporalJoinV1)
    }

    /// Descriptor for the DDL-guarded, vnode-managed grouped-aggregate path.
    pub(crate) const fn keyed_sql_aggregate() -> Self {
        Self::ddl_guarded(
            OperatorImplementation::SqlQuery,
            OperatorStateClass::VnodeKeyed,
        )
        .with_managed_state(ManagedStateContract::SqlAggregateV1)
    }

    /// Descriptor for a keyed SQL aggregate with event-time window state.
    pub(crate) const fn windowed_keyed_sql_aggregate() -> Self {
        Self::rejected(
            OperatorImplementation::SqlQuery,
            OperatorStateClass::VnodeKeyed,
            "windowed keyed state has no qualified vnode-scoped timer, watermark eviction, output, and checkpoint/rebalance lifecycle",
        )
    }

    /// Fail-closed descriptor for SQL whose retained-state shape is ambiguous.
    pub(crate) const fn unclassified_sql_query() -> Self {
        Self::fixed(OperatorImplementation::SqlQuery)
    }

    #[cfg(test)]
    pub(crate) const fn test_probe() -> Self {
        Self::fixed(OperatorImplementation::TestProbe)
    }

    #[cfg(test)]
    pub(crate) const fn test_vnode_state() -> Self {
        Self::internal(
            OperatorImplementation::TestProbe,
            OperatorStateClass::VnodeKeyed,
        )
        .with_managed_state(ManagedStateContract::TestVnodeStateV1)
    }

    #[cfg(all(test, feature = "cluster"))]
    pub(crate) const fn test_global_state() -> Self {
        Self::internal(
            OperatorImplementation::TestProbe,
            OperatorStateClass::GlobalSingleton,
        )
        .with_managed_state(ManagedStateContract::TestVnodeStateV1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_production_inventory_is_explicit_and_fail_closed() {
        use ClusterExecutionStatus::{DdlGuarded, InternalOnly, Rejected};
        use OperatorImplementation as Implementation;
        use OperatorStateClass as State;

        let expected = [
            (
                Implementation::SourcePassthrough,
                State::Stateless,
                DdlGuarded,
            ),
            (Implementation::Tombstoned, State::Stateless, InternalOnly),
            (Implementation::SqlFilter, State::Stateless, DdlGuarded),
            (
                Implementation::ChangelogEnrich,
                State::RebuildableReplicated,
                Rejected {
                    reason: "replicated lookup state has no cluster snapshot/version contract",
                },
            ),
            (
                Implementation::AiInference,
                State::LocalOnly,
                Rejected {
                    reason: "checkpointed in-flight inference has no vnode ownership lifecycle",
                },
            ),
            (
                Implementation::EowcQuery,
                State::VnodeKeyed,
                Rejected {
                    reason: "window queries require the managed CoreWindow construction path",
                },
            ),
            (
                Implementation::IntervalJoin,
                State::LocalOnly,
                Rejected {
                    reason: "join state has no co-partitioned vnode ownership lifecycle",
                },
            ),
            (
                Implementation::LookupEnrich,
                State::LocalOnly,
                Rejected {
                    reason: "checkpointed pending lookup input has no vnode ownership lifecycle",
                },
            ),
            (
                Implementation::TemporalFilter,
                State::LocalOnly,
                Rejected {
                    reason: "retracting buffered rows have no vnode ownership lifecycle",
                },
            ),
            (Implementation::Rejecting, State::Stateless, InternalOnly),
            (
                Implementation::TemporalJoin,
                State::VnodeKeyed,
                Rejected {
                    reason:
                        "temporal joins require the managed temporal operator construction path",
                },
            ),
            (
                Implementation::WindowFrame,
                State::LocalOnly,
                Rejected {
                    reason: "retained analytic-frame history has no vnode ownership lifecycle",
                },
            ),
        ];

        for (implementation, state_class, cluster_status) in expected {
            assert_eq!(
                OperatorCapability::fixed(implementation),
                OperatorCapability {
                    implementation,
                    state_class,
                    cluster_status,
                    managed_state: None,
                }
            );
        }

        assert!(matches!(
            OperatorCapability::fixed(Implementation::SqlQuery),
            OperatorCapability {
                state_class: State::LocalOnly,
                cluster_status: Rejected { .. },
                ..
            }
        ));
        assert_eq!(
            OperatorCapability::managed_core_window(),
            OperatorCapability {
                implementation: Implementation::EowcQuery,
                state_class: State::VnodeKeyed,
                cluster_status: DdlGuarded,
                managed_state: Some(ManagedStateContract::CoreWindowV1),
            }
        );
        assert_eq!(
            OperatorCapability::bounded_interval_join(),
            OperatorCapability {
                implementation: Implementation::IntervalJoin,
                state_class: State::VnodeKeyed,
                cluster_status: DdlGuarded,
                managed_state: Some(ManagedStateContract::BoundedIntervalJoinV3),
            }
        );
    }
}

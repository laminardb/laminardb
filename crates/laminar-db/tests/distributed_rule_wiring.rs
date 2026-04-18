//! Verifies `LaminarDbBuilder::physical_optimizer_rule` installs the
//! rule on the real `SessionState`. The rewrite itself is covered by
//! the rule's own unit tests.

#![cfg(feature = "cluster-unstable")]
#![allow(clippy::disallowed_types)]

use std::sync::Arc;

use laminar_core::shuffle::{ShuffleReceiver, ShuffleSender};
use laminar_core::state::{NodeId, VnodeRegistry};
use laminar_db::LaminarDB;
use laminar_sql::datafusion::distributed_aggregate_rule::DistributedAggregateRule;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn distributed_aggregate_rule_installed_on_session_state() {
    // Real shuffle handles — the rule needs them at construction, though
    // this test only compiles a plan (no data flows).
    let recv = Arc::new(
        ShuffleReceiver::bind(1, "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap(),
    );
    let sender = Arc::new(ShuffleSender::new(1));
    let registry = Arc::new(VnodeRegistry::single_owner(4, NodeId(1)));
    let rule = Arc::new(DistributedAggregateRule::new(
        registry,
        sender,
        recv,
        NodeId(1),
    ));

    let db = LaminarDB::builder()
        .physical_optimizer_rule(rule)
        .build()
        .await
        .expect("build");

    // Look up the registered rules via the real SessionState — same
    // one DataFusion uses when compiling queries.
    let ctx = db.session_context();
    let rule_names: Vec<String> = ctx
        .state()
        .physical_optimizers()
        .iter()
        .map(|r| r.name().to_string())
        .collect();
    assert!(
        rule_names.iter().any(|n| n == "DistributedAggregateRule"),
        "DistributedAggregateRule missing from session state; installed: {rule_names:?}",
    );
}

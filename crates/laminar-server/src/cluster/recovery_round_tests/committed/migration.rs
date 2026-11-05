//! Two databases migrate their topology. The migration adds views over the kept keyed view,
//! which each database seeds from the slices it restored at the migration's cut, and drops the
//! view placed by its full row. It keeps the chain view whose owner runs the last join, and
//! drops the one that aggregates over its join stages.

use super::scenario::{
    chain_views, checkpoint, leader, read_views, start_pair, wait_totals, Process,
};
use super::*;

const OPERATION: u128 = 0x5d00_0001;

const STATEMENTS: [&str; 4] = [
    // Placed by its full row, so each database routes its seed to the rows' owners.
    "CREATE MATERIALIZED VIEW migrated_totals AS SELECT total, n FROM recovery_totals",
    // Regroups both accounts onto one owner.
    "CREATE MATERIALIZED VIEW migrated_counts AS SELECT n, COUNT(*) AS accounts FROM recovery_totals GROUP BY n",
    "DROP MATERIALIZED VIEW recovery_amounts",
    "DROP MATERIALIZED VIEW recovery_chain_totals",
];

const VIEWS: [&str; 3] = [
    "SELECT total, n FROM migrated_totals",
    "SELECT n, accounts FROM migrated_counts",
    "SELECT account, left_amount, right_amount FROM recovery_chain_rows",
];

/// The sorted rows of each of [`VIEWS`] once each partition delivered `prefix` records.
fn expected_views(keys: [i64; 2], prefix: i64) -> Vec<Vec<Vec<i64>>> {
    let total = 5 * prefix * (prefix + 1);
    let [_, chain_rows] = chain_views(keys, prefix);
    vec![
        vec![vec![total, prefix], vec![total + prefix, prefix]],
        vec![vec![prefix, 2]],
        chain_rows,
    ]
}

pub(super) async fn qualify(processes: &mut [Process; 2]) -> Result<()> {
    start_pair(processes).await?;
    let keys = connectors::keys()?;
    for process in &mut *processes {
        process.done(&Command::Prefix(2)).await?;
    }
    wait_totals(processes, &BTreeMap::from([(keys[0], 32), (keys[1], 30)])).await?;
    let leader = leader(processes).await?;
    processes[leader]
        .done(&Command::Migrate(
            OPERATION,
            STATEMENTS.map(String::from).to_vec(),
        ))
        .await?;
    let version = wait_active(processes).await?;
    println!("migration {OPERATION:#x} active as topology {version:?} on both databases");
    // The seeds hold the cut's rows, read on both databases.
    let epoch = checkpoint(&mut processes[leader], &[7, 8]).await?;
    for process in &mut *processes {
        assert_eq!(
            read_views(process, &VIEWS, Some(epoch)).await?,
            expected_views(keys, 2)
        );
    }
    for process in &mut *processes {
        process.done(&Command::Prefix(3)).await?;
    }
    wait_totals(processes, &BTreeMap::from([(keys[0], 63), (keys[1], 60)])).await?;
    let epoch = checkpoint(&mut processes[leader], &[7, 8]).await?;
    for process in &mut *processes {
        assert_eq!(
            read_views(process, &VIEWS, Some(epoch)).await?,
            expected_views(keys, 3)
        );
    }
    Ok(())
}

/// Wait until the migration is Active and both databases run its topology.
async fn wait_active(processes: &mut [Process; 2]) -> Result<TopologyVersion> {
    let mut last = Vec::new();
    tokio::time::timeout(DEADLINE, async {
        loop {
            let mut observed = Vec::new();
            for process in processes.iter_mut() {
                match process.request(&Command::Topology(OPERATION)).await? {
                    Response::Topology {
                        phase,
                        committed,
                        active,
                    } => observed.push((phase, committed, active)),
                    response => return Err(anyhow!("expected topology status: {response:?}")),
                }
            }
            last = observed;
            if let Some((Some(phase @ TopologyAdmissionPhase::Aborted { .. }), ..)) = last.first() {
                return Err(anyhow!("migration aborted: {phase:?}"));
            }
            let active = last.iter().all(|(phase, committed, active)| {
                *phase == Some(TopologyAdmissionPhase::Active)
                    && committed.is_some()
                    && committed == active
            });
            if let (true, Some((_, Some(version), _))) = (active, last.first()) {
                return Ok(*version);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .with_context(|| format!("migration did not activate on both databases; last: {last:?}"))?
}

//! A draining database hands its vnodes over live: the other database takes the keyed and
//! row-placed views' slices and the join chains' state, adopts the append views' logs, and reads
//! every view at its next checkpoint. The drained database takes part in no later checkpoint and reads them too.
//! Subscribers on both databases, attached before the drain, fold to the same rows: the moved
//! slices and the adopted log are not changes.

use super::scenario::{
    checkpoint, expected_views, leader, read_views, start_pair, wait_totals, Process, VIEWS,
};
use super::*;

pub(super) async fn qualify(processes: &mut [Process; 2]) -> Result<()> {
    start_pair(processes).await?;
    let keys = connectors::keys()?;
    for process in &mut *processes {
        process.done(&Command::Prefix(2)).await?;
    }
    wait_totals(processes, &BTreeMap::from([(keys[0], 32), (keys[1], 30)])).await?;
    let first = leader(processes).await?;
    let epoch = checkpoint(&mut processes[first], &[7, 8]).await?;
    for process in &mut *processes {
        assert_eq!(
            read_views(process, &VIEWS, Some(epoch)).await?,
            expected_views(keys, 2)
        );
        subscribe(process).await?;
    }
    for process in &mut *processes {
        process.done(&Command::Drain(8)).await?;
    }
    wait_owned_by(processes, 7).await?;
    let fence = processes[1].observe().await?.assignment;
    assert_eq!(
        fence.map(|fence| fence.participant_ids()),
        Some(vec![7]),
        "node 8 is no longer a checkpoint participant"
    );
    // Before any later input, node 7 holds node 8's rows only through the moved slices and log.
    let after = leader(processes).await?;
    let epoch = checkpoint(&mut processes[after], &[7]).await?;
    for process in &mut *processes {
        assert_eq!(
            read_views(process, &VIEWS, Some(epoch)).await?,
            expected_views(keys, 2)
        );
    }
    for process in &mut *processes {
        process.done(&Command::Prefix(3)).await?;
    }
    wait_totals(
        &mut processes[..1],
        &BTreeMap::from([(keys[0], 63), (keys[1], 60)]),
    )
    .await?;
    let after = leader(processes).await?;
    let epoch = checkpoint(&mut processes[after], &[7]).await?;
    // Node 8 learns of node 7's checkpoints from the authority's committed head.
    for process in &mut *processes {
        assert_eq!(
            read_views(process, &VIEWS, Some(epoch)).await?,
            expected_views(keys, 3)
        );
    }
    assert_eq!(
        read_views(&mut processes[1], &VIEWS, None).await?,
        expected_views(keys, 3)
    );
    for process in &mut *processes {
        assert_eq!(
            read_subscriptions(process, epoch).await?,
            expected_views(keys, 3)
        );
    }
    Ok(())
}

/// The columns of each view that [`VIEWS`] reads.
const SUBSCRIBED: [(&str, &[&str]); 5] = [
    ("recovery_totals", &["account", "total", "n"]),
    ("recovery_amounts", &["total"]),
    ("recovery_rows", &["account", "amount"]),
    ("recovery_chain_totals", &["account", "total", "n"]),
    (
        "recovery_chain_rows",
        &["account", "left_amount", "right_amount"],
    ),
];

async fn subscribe(process: &mut Process) -> Result<()> {
    for (view, columns) in SUBSCRIBED {
        let columns = columns.iter().map(|column| (*column).to_owned()).collect();
        process
            .done(&Command::Subscribe(view.to_owned(), columns))
            .await?;
    }
    Ok(())
}

async fn read_subscriptions(process: &mut Process, through: u64) -> Result<Vec<Vec<Vec<i64>>>> {
    let mut read = Vec::new();
    for (view, _) in SUBSCRIBED {
        match process
            .request(&Command::ReadSubscription(view.to_owned(), through))
            .await?
        {
            Response::Rows(rows) => read.push(rows),
            response => return Err(anyhow!("expected subscription rows: {response:?}")),
        }
    }
    Ok(read)
}

/// Wait until both databases run an assignment in which `owner` owns every vnode.
pub(super) async fn wait_owned_by(processes: &mut [Process; 2], owner: u64) -> Result<()> {
    let mut last = Vec::new();
    tokio::time::timeout(DEADLINE, async {
        loop {
            let mut observed = Vec::new();
            for process in processes.iter_mut() {
                let observation = process.observe().await?;
                if observation.fault.is_some() {
                    return Err(anyhow!("fault during the drain: {:?}", observation.fault));
                }
                observed.push(observation.assignment);
            }
            last = observed;
            if last.iter().all(|assignment| {
                assignment
                    .as_ref()
                    .is_some_and(|fence| fence.matches_owner_map(&[owner, owner]))
            }) {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .with_context(|| format!("the drain did not move every vnode to {owner}; last: {last:?}"))?
}

/// A window still open at the handoff cut moves in the cut's state, and only its new owner closes
/// it: the draining database's intake stays fenced from the cut's capture until it adopts the
/// verdict. Every window row is written once, by one database.
pub(super) async fn qualify_windows(processes: &mut [Process; 2]) -> Result<()> {
    start_pair(processes).await?;
    let keys = connectors::keys()?;
    deliver(processes, 2, 4).await?;
    let first = leader(processes).await?;
    checkpoint(&mut processes[first], &[7, 8]).await?;
    wait_windows(processes, 2).await?;
    // Each account's third record opens a window that is still open at the handoff cut: no later
    // record has passed its end.
    deliver(processes, 3, 6).await?;
    for process in &mut *processes {
        process.done(&Command::Drain(8)).await?;
    }
    wait_owned_by(processes, 7).await?;
    deliver(processes, 4, 8).await?;
    let after = leader(processes).await?;
    checkpoint(&mut processes[after], &[7]).await?;
    wait_windows(processes, 6).await?;
    let mut written = Vec::new();
    for process in &mut *processes {
        match process.request(&Command::ReadWindows).await? {
            Response::Rows(rows) => written.push(rows),
            response => return Err(anyhow!("expected window rows: {response:?}")),
        }
    }
    let twice: Vec<_> = written[0]
        .iter()
        .filter(|row| written[1].iter().any(|other| other[..2] == row[..2]))
        .collect();
    assert!(
        twice.is_empty(),
        "both databases wrote these windows (account, start, total): {twice:?}; node 7 {:?}, node 8 {:?}",
        written[0],
        written[1]
    );
    // Account `keys[1]` lives on node 8's vnode, so node 7 closes its third window only from the
    // moved state.
    assert!(
        written[0].contains(&vec![keys[1], 1_003_000, 30]),
        "node 7 did not close the moved window: {:?}",
        written[0]
    );
    let mut all: Vec<_> = written.concat();
    all.sort_unstable();
    let mut expected: Vec<Vec<i64>> = (1..=3_i64)
        .flat_map(|record| {
            (0..2_i64).map(move |partition| {
                vec![
                    keys[usize::try_from(1 - partition).unwrap()],
                    1_000_000 + record * 1_000,
                    record * 10 + partition,
                ]
            })
        })
        .collect();
    expected.sort_unstable();
    assert_eq!(all, expected);
    Ok(())
}

/// Let each database's probe source read `prefix` records per owned partition, and wait until
/// the two have delivered `total` records between them.
async fn deliver(processes: &mut [Process; 2], prefix: u64, total: u64) -> Result<()> {
    for process in &mut *processes {
        process.done(&Command::Prefix(prefix)).await?;
    }
    tokio::time::timeout(DEADLINE, async {
        loop {
            let mut delivered = 0;
            for process in processes.iter_mut() {
                delivered += process.observe().await?.delivered;
            }
            if delivered >= total {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .with_context(|| format!("the probe did not deliver {total} records"))?
}

/// Wait until the two databases' sinks hold at least `rows` window rows between them.
async fn wait_windows(processes: &mut [Process; 2], rows: usize) -> Result<()> {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let mut written = 0;
            for process in processes.iter_mut() {
                match process.request(&Command::ReadWindows).await? {
                    Response::Rows(windows) => written += windows.len(),
                    response => return Err(anyhow!("expected window rows: {response:?}")),
                }
            }
            if written >= rows {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .with_context(|| format!("the sinks did not write {rows} window rows"))?
}

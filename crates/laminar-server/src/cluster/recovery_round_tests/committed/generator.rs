//! The built-in generator split over two databases. A failed database's splits resume on the
//! survivor from the committed cut, and a draining database hands all of its splits over live at
//! the handoff cut. At every committed cut the keyed view holds exactly the rows that the cut's
//! cursor says were read, so no row is lost or counted twice.

use super::rotation::wait_owned_by;
use super::scenario::{leader, read_views, start_pair, wait_open, Process};
use super::*;

const SPLITS: i64 = 4;
const ROWS: i64 = 24_000;
const ROWS_PER_SECOND: i64 = 2_000;
const LANES: i64 = 8;
const VIEW: &str = "SELECT lane, n, total FROM generator_lanes";

/// How node 8's splits move to node 7.
#[derive(Clone, Copy, Debug)]
pub(super) enum Handover {
    /// Node 8 is killed; node 7 recovers its vnodes and splits from the committed cut.
    FailedOwner,
    /// Node 8 drains; node 7 takes its vnodes and splits at the handoff cut.
    Drain,
}

pub(super) fn catalog() -> [String; 2] {
    [
        format!(
            "CREATE SOURCE recovery_input FROM GENERATOR ('splits' = '{SPLITS}', \
             'max.rows' = '{ROWS}', 'rows.per.second' = '{ROWS_PER_SECOND}', \
             'batch.max.size' = '64')"
        ),
        format!(
            "CREATE MATERIALIZED VIEW generator_lanes AS SELECT seq % {LANES} AS lane, \
             COUNT(*) AS n, SUM(seq) AS total FROM recovery_input GROUP BY seq % {LANES}"
        ),
    ]
}

/// A committed cut: its epoch and the next sequence number of every split.
#[derive(Debug)]
struct Cut {
    epoch: u64,
    next: Vec<i64>,
}

impl Cut {
    /// Split `s` read `s, s + SPLITS, …` below its next sequence number.
    fn read(&self) -> impl Iterator<Item = i64> + '_ {
        (0..SPLITS)
            .zip(&self.next)
            .flat_map(|(split, next)| (split..*next).step_by(SPLITS as usize))
    }

    fn rows(&self) -> i64 {
        self.read().map(|_| 1).sum()
    }

    fn inside_the_sequence(&self) -> bool {
        self.next.iter().any(|next| *next < ROWS)
    }

    /// The sorted `(lane, n, total)` rows of the view over the rows this cut read.
    fn lanes(&self) -> Vec<Vec<i64>> {
        let mut lanes = BTreeMap::<i64, (i64, i64)>::new();
        for seq in self.read() {
            let (n, total) = lanes.entry(seq % LANES).or_default();
            *n += 1;
            *total += seq;
        }
        lanes
            .into_iter()
            .map(|(lane, (n, total))| vec![lane, n, total])
            .collect()
    }
}

/// Commit a checkpoint of exactly `participants` whose cursor covers every split.
async fn commit(process: &mut Process, participants: &[u64]) -> Result<Cut> {
    let response = process.request(&Command::Checkpoint).await?;
    let Response::Committed {
        reference,
        offsets,
        participants: committed,
        channels,
        ..
    } = response
    else {
        return Err(anyhow!("expected a committed checkpoint: {response:?}"));
    };
    if committed != participants || channels.len() != SPLITS as usize {
        return Err(anyhow!(
            "cut {reference:?} of {committed:?} with {} channels is not a cut of every split \
             by {participants:?}",
            channels.len()
        ));
    }
    let next = (0..SPLITS)
        .map(|split| {
            offsets
                .get(&format!("seq.{split}"))
                .ok_or_else(|| anyhow!("cut {reference:?} has no cursor of split {split}"))?
                .parse::<i64>()
                .map_err(anyhow::Error::from)
        })
        .collect::<Result<_>>()?;
    Ok(Cut {
        epoch: reference.epoch,
        next,
    })
}

/// Every database reads exactly the rows the cut's cursor read.
async fn check(readers: &mut [Process], cut: &Cut) -> Result<()> {
    let expected = vec![cut.lanes()];
    for reader in readers {
        let read = read_views(reader, &[VIEW], Some(cut.epoch)).await?;
        assert_eq!(
            read, expected,
            "the view at {cut:?} is not the rows it read"
        );
    }
    Ok(())
}

/// Commit and check cuts on `processes[committer]` until one has read at least `rows` rows.
async fn advance(
    processes: &mut [Process],
    committer: usize,
    participants: &[u64],
    rows: i64,
) -> Result<Cut> {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let cut = commit(&mut processes[committer], participants).await?;
            check(processes, &cut).await?;
            if cut.rows() >= rows {
                return Ok(cut);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .with_context(|| format!("no cut of {participants:?} read {rows} rows"))?
}

fn assert_exhausted(cut: &Cut) {
    let ended = (0..SPLITS).map(|split| ROWS + split).collect::<Vec<_>>();
    assert_eq!(cut.next, ended, "every split ends after the last row");
}

pub(super) async fn qualify(processes: &mut [Process; 2], handover: Handover) -> Result<()> {
    start_pair(processes).await?;
    let committer = leader(processes).await?;
    let before = advance(processes, committer, &[7, 8], ROWS / 8).await?;
    println!("cut before the handover: {before:?}");
    assert!(
        before.inside_the_sequence(),
        "the handover must fall inside the sequence: {before:?}"
    );
    match handover {
        Handover::FailedOwner => recover_failed_owner(processes, &before).await,
        Handover::Drain => drain(processes).await,
    }
}

async fn recover_failed_owner(processes: &mut [Process; 2], committed: &Cut) -> Result<()> {
    // Both databases read past the committed cut before node 8 fails, so the survivor replays.
    tokio::time::sleep(Duration::from_millis(500)).await;
    processes[1].kill().await?;
    let survivor = std::slice::from_mut(&mut processes[0]);
    survivor[0].done(&Command::RemoveFailedPeer(8)).await?;
    wait_open(&mut survivor[0], committed.epoch).await?;
    check(survivor, committed).await?;
    assert_exhausted(&advance(survivor, 0, &[7], ROWS).await?);
    Ok(())
}

/// Node 8 loses every split; node 7 reads them from the handoff cut merged from both databases.
async fn drain(processes: &mut [Process; 2]) -> Result<()> {
    for process in &mut *processes {
        process.done(&Command::Drain(8)).await?;
    }
    wait_owned_by(processes, 7).await?;
    let committer = leader(processes).await?;
    let after = commit(&mut processes[committer], &[7]).await?;
    println!("first cut after the drain: {after:?}");
    check(processes, &after).await?;
    assert!(
        after.inside_the_sequence(),
        "node 7 must take node 8's splits inside the sequence: {after:?}"
    );
    assert_exhausted(&advance(processes, committer, &[7], ROWS).await?);
    for process in processes {
        let fault = process.observe().await?.fault;
        assert!(fault.is_none(), "fault after the drain: {fault:?}");
    }
    Ok(())
}

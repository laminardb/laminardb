//! A sequence over two databases: the built-in generator split four ways, or one placed
//! singleton sequence or changelog that only the owner of its home vnode reads. A failed database's share
//! resumes on the survivor from the committed cut, and a draining database hands its share over
//! live at the handoff cut. At every committed cut the keyed view holds exactly the rows that the
//! cut's cursor says were read, so no row is lost or counted twice.

use std::num::NonZeroU32;

use arrow_array::{BinaryArray, Int64Array, RecordBatch, UInt32Array};
use laminar_connectors::config::ConnectorInfo;
use laminar_connectors::connector::{
    SourceBatch, SourceInputMode, SourcePosition, SourceRowPositionCapability, SourceRowPositions,
    SourceStart, SourceTopology,
};
use laminar_core::state::SourceChannelRoute;

use super::rotation::wait_owned_by;
use super::scenario::{leader, read_views, start_pair, wait_open, Process};
use super::*;

const SPLITS: i64 = 4;
const ROWS: i64 = 24_000;
const ROWS_PER_SECOND: i64 = 2_000;
const LANES: i64 = 8;
const VIEW: &str = "SELECT lane, n, total FROM generator_lanes";

/// What the two databases read.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Sequence {
    /// The built-in generator with `SPLITS` splits.
    Splits,
    /// One placed singleton sequence, homed on node 8 so that each handover moves it.
    Singleton,
    /// [`Self::Singleton`] as a full changelog: every fourth change retracts the value inserted
    /// two changes before it.
    Changelog,
}

impl Sequence {
    fn channels(self) -> i64 {
        match self {
            Self::Splits => SPLITS,
            Self::Singleton | Self::Changelog => 1,
        }
    }

    /// Change `index` of the sequence as `(seq, weight)`.
    fn change(self, index: i64) -> (i64, i64) {
        match self {
            Self::Changelog if index % 4 == 3 => (index - 2, -1),
            Self::Splits | Self::Singleton | Self::Changelog => (index, 1),
        }
    }

    /// The source's catalog name. Node 8 owns vnode 1 of the two, so the singleton's name must
    /// route its one channel there.
    pub(super) fn source(self) -> String {
        match self {
            Self::Splits => "recovery_input".into(),
            Self::Singleton | Self::Changelog => (0..)
                .map(|n| format!("placed_input_{n}"))
                .find(|name| {
                    let vnodes = NonZeroU32::new(2).expect("two vnodes");
                    SourceChannelRoute::V1.vnode(name, 0, vnodes) == 1
                })
                .expect("some name routes to vnode 1"),
        }
    }

    pub(super) fn catalog(self) -> [String; 2] {
        let source = self.source();
        let create = match self {
            Self::Splits => format!(
                "CREATE SOURCE {source} FROM GENERATOR ('splits' = '{SPLITS}', \
                 'max.rows' = '{ROWS}', 'rows.per.second' = '{ROWS_PER_SECOND}', \
                 'batch.max.size' = '64')"
            ),
            Self::Singleton => {
                format!("CREATE SOURCE {source} (seq BIGINT NOT NULL) FROM \"{PLACED_SEQUENCE}\"")
            }
            Self::Changelog => format!(
                "CREATE SOURCE {source} (seq BIGINT NOT NULL, __weight BIGINT NOT NULL) \
                 FROM \"{PLACED_CHANGELOG}\""
            ),
        };
        [
            create,
            format!(
                "CREATE MATERIALIZED VIEW generator_lanes AS SELECT seq % {LANES} AS lane, \
                 COUNT(*) AS n, SUM(seq) AS total FROM {source} GROUP BY seq % {LANES}"
            ),
        ]
    }
}

/// How node 8's share moves to node 7.
#[derive(Clone, Copy, Debug)]
pub(super) enum Handover {
    /// Node 8 is killed; node 7 recovers its vnodes and splits from the committed cut.
    FailedOwner,
    /// Node 8 drains; node 7 takes its vnodes and splits at the handoff cut.
    Drain,
}

/// A committed cut: its epoch and the next sequence number of every channel.
#[derive(Debug)]
struct Cut {
    sequence: Sequence,
    epoch: u64,
    next: Vec<i64>,
}

impl Cut {
    /// Channel `s` of `n` read changes `s, s + n, …` below its next sequence number.
    fn read(&self) -> impl Iterator<Item = (i64, i64)> + '_ {
        let channels = self.next.len();
        (0_i64..)
            .zip(&self.next)
            .flat_map(move |(channel, next)| (channel..*next).step_by(channels))
            .map(|index| self.sequence.change(index))
    }

    fn rows(&self) -> i64 {
        self.read().map(|_| 1).sum()
    }

    fn inside_the_sequence(&self) -> bool {
        self.next.iter().any(|next| *next < ROWS)
    }

    /// The sorted `(lane, n, total)` rows of the view over the changes this cut read.
    fn lanes(&self) -> Vec<Vec<i64>> {
        let mut lanes = BTreeMap::<i64, (i64, i64)>::new();
        for (seq, weight) in self.read() {
            let (n, total) = lanes.entry(seq % LANES).or_default();
            *n += weight;
            *total += seq * weight;
        }
        lanes
            .into_iter()
            .filter(|(_, (n, _))| *n != 0)
            .map(|(lane, (n, total))| vec![lane, n, total])
            .collect()
    }
}

/// Commit a checkpoint of exactly `participants` whose cursor covers every channel.
async fn commit(process: &mut Process, sequence: Sequence, participants: &[u64]) -> Result<Cut> {
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
    if committed != participants || channels.len() != sequence.channels() as usize {
        return Err(anyhow!(
            "cut {reference:?} of {committed:?} with {} channels is not a cut of every channel \
             by {participants:?}",
            channels.len()
        ));
    }
    let next = (0..sequence.channels())
        .map(|split| {
            offsets
                .get(&format!("seq.{split}"))
                .ok_or_else(|| anyhow!("cut {reference:?} has no cursor of split {split}"))?
                .parse::<i64>()
                .map_err(anyhow::Error::from)
        })
        .collect::<Result<_>>()?;
    Ok(Cut {
        sequence,
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
    sequence: Sequence,
    committer: usize,
    participants: &[u64],
    rows: i64,
) -> Result<Cut> {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let cut = commit(&mut processes[committer], sequence, participants).await?;
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
    let ended = (0_i64..)
        .take(cut.next.len())
        .map(|channel| ROWS + channel)
        .collect::<Vec<_>>();
    assert_eq!(cut.next, ended, "every channel ends after the last row");
}

pub(super) async fn qualify(
    processes: &mut [Process; 2],
    sequence: Sequence,
    handover: Handover,
) -> Result<()> {
    start_pair(processes).await?;
    let committer = leader(processes).await?;
    let before = advance(processes, sequence, committer, &[7, 8], ROWS / 8).await?;
    println!("cut before the handover: {before:?}");
    assert!(
        before.inside_the_sequence(),
        "the handover must fall inside the sequence: {before:?}"
    );
    match handover {
        Handover::FailedOwner => recover_failed_owner(processes, sequence, &before).await,
        Handover::Drain => drain(processes, sequence).await,
    }
}

async fn recover_failed_owner(
    processes: &mut [Process; 2],
    sequence: Sequence,
    committed: &Cut,
) -> Result<()> {
    // Both databases read past the committed cut before node 8 fails, so the survivor replays.
    tokio::time::sleep(Duration::from_millis(500)).await;
    processes[1].kill().await?;
    let survivor = std::slice::from_mut(&mut processes[0]);
    survivor[0].done(&Command::RemoveFailedPeer(8)).await?;
    wait_open(&mut survivor[0], committed.epoch).await?;
    check(survivor, committed).await?;
    assert_exhausted(&advance(survivor, sequence, 0, &[7], ROWS).await?);
    Ok(())
}

/// Node 8 loses its share; node 7 reads it from the handoff cut merged from both databases.
async fn drain(processes: &mut [Process; 2], sequence: Sequence) -> Result<()> {
    for process in &mut *processes {
        process.done(&Command::Drain(8)).await?;
    }
    wait_owned_by(processes, 7).await?;
    let committer = leader(processes).await?;
    let after = commit(&mut processes[committer], sequence, &[7]).await?;
    println!("first cut after the drain: {after:?}");
    check(processes, &after).await?;
    assert!(
        after.inside_the_sequence(),
        "node 7 must take node 8's share inside the sequence: {after:?}"
    );
    assert_exhausted(&advance(processes, sequence, committer, &[7], ROWS).await?);
    for process in processes {
        let fault = process.observe().await?.fault;
        assert!(fault.is_none(), "fault after the drain: {fault:?}");
    }
    Ok(())
}

const PLACED_SEQUENCE: &str = "placed-sequence";
const PLACED_CHANGELOG: &str = "placed-changelog";

/// Register the placed singleton `sequence` for this database.
pub(super) fn register_placed_sequence(
    registry: &laminar_connectors::registry::ConnectorRegistry,
    sequence: Sequence,
) -> Result<(), ConnectorError> {
    let name = match sequence {
        Sequence::Changelog => PLACED_CHANGELOG,
        Sequence::Splits | Sequence::Singleton => PLACED_SEQUENCE,
    };
    registry.register_source(
        name,
        ConnectorInfo {
            schema_capabilities:
                laminar_connectors::schema::resolution::SchemaCapabilities::declared(false),
            name: name.into(),
            display_name: "Placed singleton sequence".into(),
            version: "1".into(),
            is_source: true,
            is_sink: false,
            config_keys: Vec::new(),
        },
        Arc::new(move |_| {
            Ok(Box::new(PlacedSequence {
                sequence,
                next: 0,
                started: None,
                emitted: 0,
            }))
        }),
    )
}

/// Changes `0..ROWS` at `ROWS_PER_SECOND`, which one node at a time may read. Its cursor `seq.0`
/// is the next change, so a reader started from a cut continues exactly where the cut ended.
struct PlacedSequence {
    sequence: Sequence,
    next: i64,
    /// When this generation started, and how many rows it has emitted since.
    started: Option<tokio::time::Instant>,
    emitted: i64,
}

#[async_trait::async_trait]
impl SourceConnector for PlacedSequence {
    fn contract(&self, _: &ConnectorConfig) -> Result<SourceContract, ConnectorError> {
        let contract = if self.sequence == Sequence::Changelog {
            SourceContract::new(
                SourceConsistency::Replayable,
                SourceTopology::Singleton,
                SourceInputMode::FullChangelog,
            )
            .with_row_positions(SourceRowPositionCapability::OrderedDeterministic)
        } else {
            SourceContract::new(
                SourceConsistency::Replayable,
                SourceTopology::Singleton,
                SourceInputMode::AppendOnly,
            )
        };
        Ok(laminar_connectors::testing::with_cluster_singleton_placement(contract))
    }

    async fn start(&mut self, request: SourceStart) -> Result<(), ConnectorError> {
        self.next = match request.into_parts().1 {
            SourcePosition::Initial => 0,
            SourcePosition::Resume { checkpoint, .. } => checkpoint
                .get_offset("seq.0")
                .and_then(|next| next.parse().ok())
                .ok_or_else(|| {
                    ConnectorError::ConfigurationError("cursor has no `seq.0`".into())
                })?,
            SourcePosition::Initialized { .. } => {
                return Err(ConnectorError::ConfigurationError(
                    "the placed sequence has no sealed start".into(),
                ))
            }
        };
        self.started = Some(tokio::time::Instant::now());
        self.emitted = 0;
        Ok(())
    }

    async fn poll_batch(&mut self, _: usize) -> Result<Option<SourceBatch>, ConnectorError> {
        let Some(started) = self.started else {
            return Ok(None);
        };
        let due = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX)
            * ROWS_PER_SECOND
            / 1_000;
        let rows = (due - self.emitted).min(64).min(ROWS - self.next);
        if rows <= 0 {
            return Ok(None);
        }
        let indexes = self.next..self.next + rows;
        self.next += rows;
        self.emitted += rows;
        let (values, weights): (Vec<_>, Vec<_>) = indexes
            .clone()
            .map(|index| self.sequence.change(index))
            .unzip();
        let mut columns: Vec<arrow_array::ArrayRef> = vec![Arc::new(Int64Array::from(values))];
        if self.sequence != Sequence::Changelog {
            let batch = RecordBatch::try_new(self.schema(), columns)
                .map_err(|error| ConnectorError::Internal(error.to_string()))?;
            return Ok(Some(SourceBatch::new(batch)));
        }
        columns.push(Arc::new(Int64Array::from(weights)));
        let batch = RecordBatch::try_new(self.schema(), columns)
            .map_err(|error| ConnectorError::Internal(error.to_string()))?;
        // Each change keeps its index as its position whichever generation emits it.
        let orders: Vec<_> = indexes.map(|index| index.to_be_bytes()).collect();
        let positions = SourceRowPositions::try_new(
            BinaryArray::from_vec(vec![PLACED_CHANGELOG.as_bytes(); orders.len()]),
            BinaryArray::from_vec(orders.iter().map(<[u8; 8]>::as_slice).collect()),
            UInt32Array::from(vec![0; orders.len()]),
        )?;
        Ok(Some(SourceBatch::positioned(batch, positions)?))
    }

    fn schema(&self) -> SchemaRef {
        let mut fields = vec![Field::new("seq", DataType::Int64, false)];
        if self.sequence == Sequence::Changelog {
            fields.push(Field::new(
                laminar_core::changelog::WEIGHT_COLUMN,
                DataType::Int64,
                false,
            ));
        }
        Arc::new(Schema::new(fields))
    }

    fn checkpoint(&self) -> SourceCheckpoint {
        let mut checkpoint = SourceCheckpoint::new();
        checkpoint.set_offset("seq.0", self.next.to_string());
        checkpoint
    }

    async fn close(&mut self) -> Result<(), ConnectorError> {
        self.started = None;
        Ok(())
    }
}

//! Stream distribution, changelog kind, and the exchange each consumer input port uses.

use std::fmt;
use std::time::Duration;

use arrow::datatypes::DataType;

/// The only vnode that global aggregate state and output occupy; subscription certificates pin
/// global aggregate output to partition zero.
pub(crate) const GLOBAL_VNODE: u32 = 0;

/// One partition-key column. Only a bare input column can match an upstream distribution; an
/// expression the operator evaluates always needs its own exchange.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum KeyColumn {
    Column { name: String, data_type: DataType },
    Computed { expr: String, data_type: DataType },
}

/// Ordered key columns, hashed together by the partition-key codec into the vnode space.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PartitionKey(pub(crate) Vec<KeyColumn>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Distribution {
    /// Rows stay where they were produced; no key guarantee.
    Arbitrary,
    /// Every row is on the owner of the vnode of its key.
    Hash(PartitionKey),
    /// Every row is on the owner of one vnode.
    Singleton { vnode: u32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ChangelogKind {
    AppendOnly,
    /// Rows carry the engine-owned `__weight`.
    Weighted,
    /// An aggregate re-emits touched groups without retracting the values they replace.
    RunningState,
}

/// The column whose source watermark orders a stream's rows: a source's declared event-time
/// column, forwarded unchanged through stateless projections.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct EventTime {
    pub(crate) column: String,
    pub(crate) max_out_of_orderness: Option<Duration>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StreamProperties {
    pub(crate) distribution: Distribution,
    pub(crate) changelog: ChangelogKind,
    pub(crate) event_time: Option<EventTime>,
}

/// A consumer input port, named by its upstream. Join sides are distinct ports even when both
/// read one upstream.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum InputPort {
    Input(String),
    Left(String),
    Right(String),
}

impl InputPort {
    pub(crate) fn upstream(&self) -> &str {
        match self {
            Self::Input(name) | Self::Left(name) | Self::Right(name) => name,
        }
    }
}

/// How one input port receives rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PortExchange {
    /// Rows stay on the node that produced them.
    Forward,
    /// Rows already sit on the owner of the vnode of this key, so none move.
    Colocated(PartitionKey),
    /// Rows move to the owner of the vnode of their key.
    Repartition(PartitionKey),
    /// Rows move to the owner of one vnode. With `partial`, each other node first folds its rows
    /// into one partial row per batch (cluster mode; the owner applies its own rows directly).
    Gather { vnode: u32, partial: bool },
}

impl fmt::Display for KeyColumn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Column { name, data_type } => write!(f, "{name} {data_type}"),
            Self::Computed { expr, data_type } => write!(f, "({expr}) {data_type}"),
        }
    }
}

impl fmt::Display for PartitionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, column) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            write!(f, "{column}")?;
        }
        Ok(())
    }
}

impl fmt::Display for Distribution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Arbitrary => f.write_str("arbitrary"),
            Self::Hash(key) => write!(f, "hash({key})"),
            Self::Singleton { vnode } => write!(f, "singleton(vnode {vnode})"),
        }
    }
}

impl fmt::Display for ChangelogKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::AppendOnly => "append-only",
            Self::Weighted => "weighted changelog",
            Self::RunningState => "running state",
        })
    }
}

impl fmt::Display for StreamProperties {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {}", self.distribution, self.changelog)?;
        match &self.event_time {
            Some(event_time) => write!(f, ", event time {}", event_time.column),
            None => Ok(()),
        }
    }
}

impl fmt::Display for InputPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Input(name) => f.write_str(name),
            Self::Left(name) => write!(f, "left:{name}"),
            Self::Right(name) => write!(f, "right:{name}"),
        }
    }
}

impl fmt::Display for PortExchange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Forward => f.write_str("forward"),
            Self::Colocated(key) => write!(f, "colocated on ({key})"),
            Self::Repartition(key) => write!(f, "repartition on ({key})"),
            Self::Gather {
                vnode,
                partial: true,
            } => write!(f, "gather partials to vnode {vnode}"),
            Self::Gather {
                vnode,
                partial: false,
            } => write!(f, "gather to vnode {vnode}"),
        }
    }
}

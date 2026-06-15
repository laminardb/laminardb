//! FFI-friendly API for language bindings.

mod connection;
mod error;
mod ingestion;
mod query;
mod subscription;

pub use crate::DdlInfo;
pub use connection::{Connection, ExecuteResult};
pub use error::{codes, ApiError};
pub use ingestion::Writer;
pub use query::{QueryResult, QueryStream};
pub use subscription::ArrowSubscription;

pub use crate::LaminarConfig;

pub use crate::{
    PipelineEdge, PipelineNode, PipelineNodeType, PipelineTopology, QueryInfo, SinkInfo,
    SourceInfo, StreamInfo,
};
pub use crate::{PipelineMetrics, PipelineState, SourceMetrics, StreamMetrics};

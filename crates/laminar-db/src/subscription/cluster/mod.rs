//! Checkpoint-committable cluster subscription output.

mod manifest;
mod output_admission;
mod output_pressure;
mod output_state;
mod reader;
mod segment;

#[cfg(feature = "benchmark-internals")]
pub(crate) mod benchmark;

#[cfg(test)]
mod tests;

pub(crate) use output_state::{
    ClusterSubscriptionOutputState, PreparedNodeSubscriptionOutput,
    PreparedPartitionSubscriptionOutput,
};

pub(crate) use reader::{ClusterReaderFrame, ClusterReaderRead, ClusterSubscriptionReader};

pub(crate) use segment::{
    decode_bound_output_segment, decode_output_segment, encode_output_segment,
    EncodedOutputSegment, OutputSegmentBinding, OutputSegmentIdentity, OutputWriterAuthority,
};

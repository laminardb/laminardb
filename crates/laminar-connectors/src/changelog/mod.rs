//! Sink-agnostic changelog collapse for upsert sinks. A commit epoch can carry
//! many changelog events for one merge key (every aggregate value change is a
//! retract + insert), which an upsert writer rejects as a cardinality
//! violation. [`collapse_changelog`] folds an epoch down to one row per key.

mod collapse;

pub use collapse::collapse_changelog;

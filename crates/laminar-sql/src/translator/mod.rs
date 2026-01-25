//! SQL to operator configuration translation
//!
//! This module translates parsed SQL AST into Ring 0 operator configurations
//! that can be instantiated and executed.

mod join_translator;
mod window_translator;

pub use join_translator::{
    JoinOperatorConfig, LookupJoinConfig, LookupJoinType, StreamJoinConfig, StreamJoinType,
};
pub use window_translator::{WindowOperatorConfig, WindowType};

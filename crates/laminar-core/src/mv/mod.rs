//! Cascading materialized view registry with dependency tracking and cycle detection.

mod error;
mod registry;

pub use error::{MvError, MvState};
pub use registry::{MaterializedView, MvRegistry};

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_dependency_chain() {
        let mut registry = MvRegistry::new();
        registry.register_base_table("trades");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let mv = |n: &str, s: Vec<&str>| {
            MaterializedView::new(
                n,
                "",
                s.into_iter().map(String::from).collect(),
                schema.clone(),
            )
        };

        registry.register(mv("a", vec!["trades"])).unwrap();
        registry.register(mv("b", vec!["a"])).unwrap();
        registry.register(mv("c", vec!["b"])).unwrap();

        let chain = registry.dependency_chain("c");
        assert_eq!(chain, vec!["a", "b", "c"]);
    }

    #[test]
    fn test_show_dependencies_equivalent() {
        let mut registry = MvRegistry::new();
        registry.register_base_table("trades");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let mv = |n: &str, s: Vec<&str>| {
            MaterializedView::new(
                n,
                "",
                s.into_iter().map(String::from).collect(),
                schema.clone(),
            )
        };

        registry.register(mv("ohlc_1s", vec!["trades"])).unwrap();
        registry.register(mv("ohlc_1m", vec!["ohlc_1s"])).unwrap();
        registry.register(mv("ohlc_1h", vec!["ohlc_1m"])).unwrap();

        let chain = registry.dependency_chain("ohlc_1h");
        assert_eq!(chain, vec!["ohlc_1s", "ohlc_1m", "ohlc_1h"]);

        let direct: Vec<_> = registry.get_dependencies("ohlc_1h").collect();
        assert_eq!(direct, vec!["ohlc_1m"]);
    }
}

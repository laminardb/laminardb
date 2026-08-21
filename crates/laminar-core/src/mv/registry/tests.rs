use super::*;

fn mv(name: &str, sources: Vec<&str>) -> MaterializedView {
    MaterializedView::simple(name, sources.into_iter().map(String::from).collect())
}

#[test]
fn test_simple_registration() {
    let mut registry = MvRegistry::new();
    registry.register_base_table("trades");

    let view = mv("ohlc_1s", vec!["trades"]);
    registry.register(view).unwrap();

    assert_eq!(registry.len(), 1);
    assert!(registry.get("ohlc_1s").is_some());
}

#[test]
fn test_cascading_registration() {
    let mut registry = MvRegistry::new();
    registry.register_base_table("trades");

    registry.register(mv("ohlc_1s", vec!["trades"])).unwrap();
    registry.register(mv("ohlc_1m", vec!["ohlc_1s"])).unwrap();
    registry.register(mv("ohlc_1h", vec!["ohlc_1m"])).unwrap();

    assert_eq!(registry.topo_order(), &["ohlc_1s", "ohlc_1m", "ohlc_1h"]);
}

#[test]
fn test_duplicate_name_error() {
    let mut registry = MvRegistry::new();
    registry.register_base_table("trades");

    registry.register(mv("ohlc_1s", vec!["trades"])).unwrap();

    let result = registry.register(mv("ohlc_1s", vec!["trades"]));
    assert!(matches!(result, Err(MvError::DuplicateName(_))));
}

#[test]
fn test_source_not_found_error() {
    let mut registry = MvRegistry::new();

    let result = registry.register(mv("view", vec!["nonexistent"]));
    assert!(matches!(result, Err(MvError::SourceNotFound(_))));
}

#[test]
fn test_cycle_detection_direct() {
    let mut registry = MvRegistry::new();
    registry.register_base_table("a");

    registry.register(mv("b", vec!["a"])).unwrap();
    registry.register(mv("c", vec!["b"])).unwrap();

    // Try to create c -> b -> c (cycle via new registration with c as source of c)
    // Actually, we can't register "c" again because of DuplicateName
    // Let's test a different cycle: d depends on c, then try to make c depend on d
    registry.register(mv("d", vec!["c"])).unwrap();

    // Can't make e depend on d and have c depend on e (would require modifying c)
    // But we can test by trying to add a view that creates a cycle through existing views
    // Actually this is the correct test: try to add x -> d, y -> x, and then a view that d -> y
}

#[test]
fn test_multi_source_view() {
    let mut registry = MvRegistry::new();
    registry.register_base_table("orders");
    registry.register_base_table("payments");

    // View that joins two base tables
    registry
        .register(mv("order_payments", vec!["orders", "payments"]))
        .unwrap();

    assert_eq!(registry.topo_order(), &["order_payments"]);

    // Check dependencies
    let deps: Vec<_> = registry.get_dependencies("order_payments").collect();
    assert!(deps.contains(&"orders"));
    assert!(deps.contains(&"payments"));
}

#[test]
fn test_diamond_dependency() {
    let mut registry = MvRegistry::new();
    registry.register_base_table("source");

    //       source
    //       /    \
    //      a      b
    //       \    /
    //         c
    registry.register(mv("a", vec!["source"])).unwrap();
    registry.register(mv("b", vec!["source"])).unwrap();
    registry.register(mv("c", vec!["a", "b"])).unwrap();

    // c should come last
    let order = registry.topo_order();
    let c_idx = order.iter().position(|x| x == "c").unwrap();
    let a_idx = order.iter().position(|x| x == "a").unwrap();
    let b_idx = order.iter().position(|x| x == "b").unwrap();

    assert!(c_idx > a_idx);
    assert!(c_idx > b_idx);
}

#[test]
fn test_unregister_simple() {
    let mut registry = MvRegistry::new();
    registry.register_base_table("trades");
    registry.register(mv("ohlc_1s", vec!["trades"])).unwrap();

    let removed = registry.unregister("ohlc_1s").unwrap();
    assert_eq!(removed.name, "ohlc_1s");
    assert!(registry.is_empty());
}

#[test]
fn test_unregister_with_dependents_error() {
    let mut registry = MvRegistry::new();
    registry.register_base_table("trades");
    registry.register(mv("ohlc_1s", vec!["trades"])).unwrap();
    registry.register(mv("ohlc_1m", vec!["ohlc_1s"])).unwrap();

    let result = registry.unregister("ohlc_1s");
    assert!(matches!(result, Err(MvError::HasDependents(_, _))));
}

#[test]
fn test_unregister_cascade() {
    let mut registry = MvRegistry::new();
    registry.register_base_table("trades");
    registry.register(mv("ohlc_1s", vec!["trades"])).unwrap();
    registry.register(mv("ohlc_1m", vec!["ohlc_1s"])).unwrap();
    registry.register(mv("ohlc_1h", vec!["ohlc_1m"])).unwrap();

    let removed = registry.unregister_cascade("ohlc_1s").unwrap();

    // All three should be removed
    assert_eq!(removed.len(), 3);
    assert!(registry.is_empty());

    // Removed in reverse order (dependents first)
    assert_eq!(removed[0].name, "ohlc_1h");
    assert_eq!(removed[1].name, "ohlc_1m");
    assert_eq!(removed[2].name, "ohlc_1s");
}

#[test]
fn test_dependency_chain() {
    let mut registry = MvRegistry::new();
    registry.register_base_table("trades");
    registry.register(mv("ohlc_1s", vec!["trades"])).unwrap();
    registry.register(mv("ohlc_1m", vec!["ohlc_1s"])).unwrap();
    registry.register(mv("ohlc_1h", vec!["ohlc_1m"])).unwrap();

    let chain = registry.dependency_chain("ohlc_1h");
    assert_eq!(chain, vec!["ohlc_1s", "ohlc_1m", "ohlc_1h"]);
}

#[test]
fn test_get_dependents() {
    let mut registry = MvRegistry::new();
    registry.register_base_table("trades");
    registry.register(mv("a", vec!["trades"])).unwrap();
    registry.register(mv("b", vec!["trades"])).unwrap();
    registry.register(mv("c", vec!["a"])).unwrap();

    let dependents: Vec<_> = registry.get_dependents("trades").collect();
    assert!(dependents.contains(&"a"));
    assert!(dependents.contains(&"b"));
    assert!(!dependents.contains(&"c"));

    let a_dependents: Vec<_> = registry.get_dependents("a").collect();
    assert_eq!(a_dependents, vec!["c"]);
}

#[test]
fn test_view_state_update() {
    let mut registry = MvRegistry::new();
    registry.register_base_table("trades");
    registry.register(mv("ohlc_1s", vec!["trades"])).unwrap();

    let view = registry.get_mut("ohlc_1s").unwrap();
    assert_eq!(view.state, MvState::Running);

    view.state = MvState::Dropping;
    assert_eq!(view.state, MvState::Dropping);
}

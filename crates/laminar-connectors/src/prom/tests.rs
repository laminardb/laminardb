use super::*;

#[test]
fn counter_registers_and_counts() {
    let r = Registry::new();
    let h = RegHandle {
        registry: &r,
        _local: None,
    };
    let c = h.counter("test_total", "Test events");
    c.inc();
    c.inc();
    assert_eq!(c.get(), 2);
    // Registry should also see the counter.
    let mfs = r.gather();
    assert!(mfs.iter().any(|m| m.name() == "test_total"));
}

#[test]
fn reg_or_local_uses_provided() {
    let provided = Registry::new();
    let mut local = None;
    let h = reg_or_local(Some(&provided), &mut local);
    let c = h.counter("provided_total", "Counter on provided registry");
    c.inc();
    assert!(local.is_none());
    assert!(provided
        .gather()
        .iter()
        .any(|m| m.name() == "provided_total"));
}

#[test]
fn reg_or_local_falls_back_to_local() {
    let mut local = None;
    let h = reg_or_local(None, &mut local);
    let c = h.counter("local_total", "Counter on local registry");
    c.inc();
    assert!(local.is_some());
}

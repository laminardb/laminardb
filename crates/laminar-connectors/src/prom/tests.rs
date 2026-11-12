use super::*;

#[test]
fn counter_registers_and_counts() {
    let r = Registry::new();
    let h = RegHandle {
        registry: &r,
        _local: None,
        registered: RefCell::default(),
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

fn exported(registry: &Registry, name: &str) -> Option<f64> {
    registry
        .gather()
        .iter()
        .find(|family| family.name() == name)
        .map(|family| family.get_metric()[0].get_counter().value())
}

#[test]
fn a_recreated_instance_exports_its_own_counter() {
    let registry = Registry::new();
    let mut local = None;
    let first = reg_or_local(Some(&registry), &mut local);
    let old = first.counter("recreated_total", "Re-created instance counter");
    let old_registration = first.into_registration();
    old.inc_by(3);
    drop(old_registration);

    let mut local = None;
    let second = reg_or_local(Some(&registry), &mut local);
    let new = second.counter("recreated_total", "Re-created instance counter");
    let _new_registration = second.into_registration();
    new.inc_by(5);
    old.inc();

    assert_eq!(exported(&registry, "recreated_total"), Some(5.0));
}

#[test]
fn an_instance_that_lost_the_name_does_not_release_it() {
    let registry = Registry::new();
    let mut local = None;
    let first = reg_or_local(Some(&registry), &mut local);
    let live = first.counter("contended_total", "Contended counter");
    let _live_registration = first.into_registration();

    let mut local = None;
    let second = reg_or_local(Some(&registry), &mut local);
    let _ = second.counter("contended_total", "Contended counter");
    drop(second.into_registration());
    live.inc_by(7);

    assert_eq!(exported(&registry, "contended_total"), Some(7.0));
}

#[test]
fn a_registration_holds_until_its_last_clone_drops() {
    let registry = Registry::new();
    let mut local = None;
    let handle = reg_or_local(Some(&registry), &mut local);
    let _counter = handle.counter("cloned_total", "Cloned registration counter");
    let registration = handle.into_registration();
    let clone = registration.clone();

    drop(registration);
    assert!(exported(&registry, "cloned_total").is_some());
    drop(clone);
    assert!(exported(&registry, "cloned_total").is_none());
}

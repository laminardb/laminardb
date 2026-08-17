use super::*;

#[test]
fn absolute_extension_preserves_the_original_deadline() {
    let deadline = LeaseDeadline::uninitialized();
    let valid_for = Duration::from_millis(37);
    let valid_until = deadline.origin.checked_add(valid_for).unwrap();

    deadline.extend_until(valid_until);

    assert_eq!(
        deadline.valid_until_ns.load(Ordering::Acquire),
        u64::try_from(valid_for.as_nanos()).unwrap()
    );
}

#[test]
fn naturally_expired_deadline_cannot_be_resurrected() {
    let deadline = LeaseDeadline::live_for(Duration::from_nanos(1));
    while deadline.is_live() {
        std::hint::spin_loop();
    }

    assert!(!deadline.is_live());
    deadline.extend(Duration::from_secs(60));

    assert!(!deadline.is_live());
    assert!(deadline.terminal.load(Ordering::Acquire));
    assert_eq!(deadline.valid_until_ns.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn renewal_before_expiry_extends_an_existing_waiter() {
    let deadline = std::sync::Arc::new(LeaseDeadline::live_for(Duration::from_millis(100)));
    let mut waiting = {
        let deadline = std::sync::Arc::clone(&deadline);
        tokio::spawn(async move { deadline.wait_until_expired().await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;

    deadline.extend(Duration::from_secs(60));

    assert!(
        tokio::time::timeout(Duration::from_millis(120), &mut waiting)
            .await
            .is_err(),
        "the waiter used the superseded pre-renewal deadline"
    );
    assert!(deadline.is_live());
    deadline.fence();
    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .expect("fencing did not stop the renewed waiter")
        .unwrap();
}

#[test]
fn terminal_fence_rejects_every_later_extension() {
    let deadline = LeaseDeadline::live_for(Duration::from_secs(60));
    deadline.fence();

    deadline.extend(Duration::from_secs(60));
    deadline.extend_until(Instant::now() + Duration::from_secs(60));

    assert!(deadline.terminal.load(Ordering::Acquire));
    assert!(!deadline.is_live());
    assert_eq!(deadline.valid_until_ns.load(Ordering::Acquire), 0);
}

#[test]
fn terminal_fence_wins_concurrent_extension() {
    for _ in 0..64 {
        let deadline = std::sync::Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(3));
        std::thread::scope(|scope| {
            let extending = std::sync::Arc::clone(&deadline);
            let extending_barrier = std::sync::Arc::clone(&barrier);
            scope.spawn(move || {
                extending_barrier.wait();
                extending.extend(Duration::from_secs(120));
                extending.extend_until(Instant::now() + Duration::from_secs(120));
            });

            let fencing = std::sync::Arc::clone(&deadline);
            let fencing_barrier = std::sync::Arc::clone(&barrier);
            scope.spawn(move || {
                fencing_barrier.wait();
                fencing.fence();
            });

            barrier.wait();
        });

        assert!(deadline.terminal.load(Ordering::Acquire));
        assert!(!deadline.is_live());
        assert_eq!(deadline.valid_until_ns.load(Ordering::Acquire), 0);
    }
}

#[tokio::test]
async fn terminal_fence_wakes_expiry_waiter() {
    let deadline = std::sync::Arc::new(LeaseDeadline::live_for(Duration::from_secs(60)));
    let waiting = {
        let deadline = std::sync::Arc::clone(&deadline);
        tokio::spawn(async move { deadline.wait_until_expired().await })
    };
    tokio::task::yield_now().await;

    deadline.fence();

    tokio::time::timeout(Duration::from_secs(1), waiting)
        .await
        .expect("terminal fence did not wake the expiry waiter")
        .unwrap();
}

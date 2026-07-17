use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Process-local monotonic deadline for a renewable durable lease.
pub struct LeaseDeadline {
    origin: Instant,
    valid_until_ns: AtomicU64,
    terminal: AtomicBool,
    transition: parking_lot::Mutex<()>,
    changed: tokio::sync::Notify,
}

impl LeaseDeadline {
    /// Create an inactive, renewable deadline for a lease manager before acquisition.
    #[must_use]
    pub(crate) fn uninitialized() -> Self {
        Self {
            origin: Instant::now(),
            valid_until_ns: AtomicU64::new(0),
            terminal: AtomicBool::new(false),
            transition: parking_lot::Mutex::new(()),
            changed: tokio::sync::Notify::new(),
        }
    }

    /// Create an irreversibly fenced deadline.
    #[must_use]
    pub fn fenced() -> Self {
        let deadline = Self::uninitialized();
        deadline.fence();
        deadline
    }

    /// Create a deadline live for `remaining`.
    #[must_use]
    pub fn live_for(remaining: Duration) -> Self {
        let deadline = Self::uninitialized();
        deadline.extend(remaining);
        deadline
    }

    pub(crate) fn extend(&self, remaining: Duration) {
        let until = self.origin.elapsed().saturating_add(remaining).as_nanos();
        let until = u64::try_from(until).unwrap_or(u64::MAX).max(1);
        self.update_valid_until(until);
    }

    pub(crate) fn extend_until(&self, valid_until: Instant) {
        let Some(remaining_from_origin) = valid_until.checked_duration_since(self.origin) else {
            self.fence();
            return;
        };
        if remaining_from_origin.is_zero() {
            self.fence();
            return;
        }
        let until = u64::try_from(remaining_from_origin.as_nanos()).unwrap_or(u64::MAX);
        self.update_valid_until(until);
    }

    fn update_valid_until(&self, valid_until_ns: u64) {
        let _transition = self.transition.lock();
        if self.terminal.load(Ordering::Acquire) {
            return;
        }
        let current = self.valid_until_ns.load(Ordering::Acquire);
        if current != 0 && self.origin.elapsed().as_nanos() >= u128::from(current) {
            self.terminal.store(true, Ordering::Release);
            self.valid_until_ns.store(0, Ordering::Release);
            self.changed.notify_waiters();
            return;
        }
        self.valid_until_ns.store(valid_until_ns, Ordering::Release);
        self.changed.notify_waiters();
    }

    /// Irreversibly revoke the lease deadline and wake all waiters.
    pub fn fence(&self) {
        let _transition = self.transition.lock();
        self.terminal.store(true, Ordering::Release);
        self.valid_until_ns.store(0, Ordering::Release);
        self.changed.notify_waiters();
    }

    /// Withdraw a renewable grant without terminalizing its manager.
    pub(crate) fn withdraw(&self) {
        let _transition = self.transition.lock();
        if self.terminal.load(Ordering::Acquire) {
            return;
        }
        self.valid_until_ns.store(0, Ordering::Release);
        self.changed.notify_waiters();
    }

    /// Whether the holder remains inside its last successful renewal deadline.
    #[must_use]
    pub fn is_live(&self) -> bool {
        if self.terminal.load(Ordering::Acquire) {
            return false;
        }
        let deadline = self.valid_until_ns.load(Ordering::Acquire);
        deadline != 0 && self.origin.elapsed().as_nanos() < u128::from(deadline)
    }

    /// Wait until the deadline expires naturally or is terminally fenced.
    pub async fn wait_until_expired(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();

            if self.terminal.load(Ordering::Acquire) {
                return;
            }
            let valid_until_ns = self.valid_until_ns.load(Ordering::Acquire);
            let elapsed_ns = self.origin.elapsed().as_nanos();
            if valid_until_ns == 0 {
                return;
            }
            if elapsed_ns >= u128::from(valid_until_ns) {
                let _transition = self.transition.lock();
                if self.terminal.load(Ordering::Acquire) {
                    return;
                }
                let current = self.valid_until_ns.load(Ordering::Acquire);
                if current != 0 && self.origin.elapsed().as_nanos() < u128::from(current) {
                    continue;
                }
                self.terminal.store(true, Ordering::Release);
                self.valid_until_ns.store(0, Ordering::Release);
                self.changed.notify_waiters();
                return;
            }
            let remaining_ns = u128::from(valid_until_ns).saturating_sub(elapsed_ns);
            let remaining = Duration::from_nanos(u64::try_from(remaining_ns).unwrap_or(u64::MAX));
            tokio::select! {
                () = tokio::time::sleep(remaining) => {}
                () = &mut changed => {}
            }
        }
    }
}

impl std::fmt::Debug for LeaseDeadline {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("LeaseDeadline")
            .field("terminal", &self.terminal.load(Ordering::Acquire))
            .field("live", &self.is_live())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
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
    fn withdrawn_renewable_grant_can_be_reacquired() {
        let deadline = LeaseDeadline::live_for(Duration::from_secs(60));

        deadline.withdraw();
        assert!(!deadline.is_live());
        assert!(!deadline.terminal.load(Ordering::Acquire));

        deadline.extend(Duration::from_secs(60));
        assert!(deadline.is_live());
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
}

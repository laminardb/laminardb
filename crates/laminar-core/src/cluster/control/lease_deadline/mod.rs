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
mod tests;

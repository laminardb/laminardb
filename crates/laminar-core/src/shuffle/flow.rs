//! Per-connection bytes-denominated credit flow control.
//!
//! The sender acquires credit equal to a message's payload size before
//! sending; the receiver grants credit (via a `Credit` message) as it
//! drains its per-connection queue. When credit is exhausted, the
//! sender blocks on the semaphore until more arrives.
//!
//! See `docs/plans/shuffle-protocol.md` §"Flow control".

use std::sync::Arc;

use tokio::sync::Semaphore;

/// Bytes-denominated semaphore backing a single shuffle connection's
/// flow control. Internally a `tokio::sync::Semaphore` with permits =
/// bytes of available credit.
#[derive(Clone, Debug)]
pub struct CreditSemaphore {
    inner: Arc<Semaphore>,
}

impl CreditSemaphore {
    /// Create with an initial credit of `initial_bytes`. The value from
    /// the design doc is 16 MiB per connection.
    #[must_use]
    pub fn new(initial_bytes: u64) -> Self {
        Self {
            inner: Arc::new(Semaphore::new(usize::try_from(initial_bytes).unwrap_or(usize::MAX))),
        }
    }

    /// Acquire `need_bytes` of credit. Awaits until enough credit is
    /// available.
    ///
    /// # Errors
    /// Returns `()` error if the underlying semaphore has been closed
    /// (signaling the connection is shutting down).
    pub async fn acquire(&self, need_bytes: u64) -> Result<(), ()> {
        let n = u32::try_from(need_bytes).map_err(|_| ())?;
        let permit = self.inner.acquire_many(n).await.map_err(|_| ())?;
        // Credit is consumed; forget the permit so it isn't returned
        // when dropped. Credits are replenished explicitly via
        // `grant()`.
        permit.forget();
        Ok(())
    }

    /// Try to acquire without waiting. Returns `true` on success.
    #[must_use]
    pub fn try_acquire(&self, need_bytes: u64) -> bool {
        let Ok(n) = u32::try_from(need_bytes) else {
            return false;
        };
        match self.inner.try_acquire_many(n) {
            Ok(permit) => {
                permit.forget();
                true
            }
            Err(_) => false,
        }
    }

    /// Grant `delta_bytes` of additional credit. Called on receipt of a
    /// `Credit` message from the receiver.
    pub fn grant(&self, delta_bytes: u64) {
        let delta = usize::try_from(delta_bytes).unwrap_or(usize::MAX);
        self.inner.add_permits(delta);
    }

    /// Close the semaphore, unblocking any waiters with an error.
    /// Called when the connection terminates.
    pub fn close(&self) {
        self.inner.close();
    }

    /// Current available credit in bytes.
    #[must_use]
    pub fn available(&self) -> u64 {
        self.inner.available_permits() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn acquire_then_grant() {
        let credit = CreditSemaphore::new(100);
        credit.acquire(40).await.unwrap();
        assert_eq!(credit.available(), 60);
        credit.grant(40);
        assert_eq!(credit.available(), 100);
    }

    #[tokio::test]
    async fn acquire_blocks_until_grant() {
        let credit = CreditSemaphore::new(50);
        // Exhaust initial credit.
        credit.acquire(50).await.unwrap();
        assert_eq!(credit.available(), 0);

        // Spawn a task that waits on more credit.
        let c2 = credit.clone();
        let waiter = tokio::spawn(async move { c2.acquire(20).await });

        // Waiter must be blocked before the grant.
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        assert!(!waiter.is_finished());

        // Grant, then waiter unblocks.
        credit.grant(20);
        waiter.await.unwrap().unwrap();
        assert_eq!(credit.available(), 0); // acquired on unblock
    }

    #[tokio::test]
    async fn try_acquire_nonblocking() {
        let credit = CreditSemaphore::new(30);
        assert!(credit.try_acquire(10));
        assert_eq!(credit.available(), 20);
        assert!(!credit.try_acquire(21));
        assert_eq!(credit.available(), 20);
    }

    #[tokio::test]
    async fn close_unblocks_waiters() {
        let credit = CreditSemaphore::new(0);
        let c2 = credit.clone();
        let waiter = tokio::spawn(async move { c2.acquire(10).await });
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        credit.close();
        assert!(waiter.await.unwrap().is_err());
    }
}

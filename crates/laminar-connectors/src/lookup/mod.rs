//! Lookup tables for enrichment joins.
//!
//! Production code uses `laminar_core::lookup::LookupSource` for the
//! actual on-demand lookups; this module also hosts finite startup snapshot
//! adapters for replicated reference tables.

#[cfg(feature = "postgres-cdc")]
use std::future::Future;

/// Delta Lake reference table source for lookup/enrichment joins.
pub mod delta_reference;

/// Delta Lake on-demand lookup source for cache-miss fallback.
#[cfg(feature = "delta-lake")]
pub mod delta_lookup;

/// Iceberg on-demand lookup source for cache-miss fallback.
#[cfg(feature = "iceberg")]
pub mod iceberg_lookup;

/// PostgreSQL startup snapshot source (no CDC required).
#[cfg(feature = "postgres-cdc")]
pub mod postgres_reference;

/// PostgreSQL on-demand lookup source (pooled, `WHERE pk = ANY($1)`).
#[cfg(feature = "postgres-cdc")]
pub mod postgres_lookup;

/// Poll a driver future from a task whose lifetime is independent of its caller.
/// Dropping the waiter detaches the task; it does not cancel the driver operation.
#[cfg(feature = "postgres-cdc")]
async fn await_owned_driver<T, E>(
    future: impl Future<Output = Result<T, E>> + Send + 'static,
    join_error: impl FnOnce(tokio::task::JoinError) -> E + Send + 'static,
) -> Result<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
{
    match tokio::spawn(future).await {
        Ok(result) => result,
        Err(error) => Err(join_error(error)),
    }
}

// Re-export the canonical lookup types from laminar-core.
pub use laminar_core::lookup::{LookupError, LookupResult};

#[cfg(all(test, feature = "postgres-cdc"))]
mod tests {
    use super::await_owned_driver;

    #[tokio::test]
    async fn owned_driver_outlives_a_cancelled_waiter() {
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let (completed_tx, completed_rx) = tokio::sync::oneshot::channel();

        let waiter = tokio::spawn(async move {
            await_owned_driver(
                async move {
                    let _ = started_tx.send(());
                    let _ = release_rx.await;
                    let _ = completed_tx.send(());
                    Ok::<(), ()>(())
                },
                |_| (),
            )
            .await
        });

        started_rx.await.expect("owned task started");
        waiter.abort();
        assert!(waiter
            .await
            .expect_err("waiter must be cancelled")
            .is_cancelled());
        release_tx
            .send(())
            .expect("owned task still receives release");
        tokio::time::timeout(std::time::Duration::from_secs(1), completed_rx)
            .await
            .expect("owned task must finish after waiter cancellation")
            .expect("completion signal");
    }
}

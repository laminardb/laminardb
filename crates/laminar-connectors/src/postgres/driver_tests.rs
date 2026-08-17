#[cfg(test)]
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

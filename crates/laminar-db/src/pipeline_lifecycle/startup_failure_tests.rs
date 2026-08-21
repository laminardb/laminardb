use super::*;

async fn roundtrip(error: DbError) -> DbError {
    let attempt = StartupAttempt::new();
    attempt.complete(Err(error));
    attempt.wait().await.expect_err("startup must fail")
}

#[tokio::test]
async fn startup_attempt_preserves_every_terminal_error_variant() {
    assert!(matches!(
        roundtrip(DbError::PipelineTerminal("pipeline poison".into())).await,
        DbError::PipelineTerminal(reason) if reason == "pipeline poison"
    ));
    assert!(matches!(
        roundtrip(DbError::BackpressureFail("bounded queue".into())).await,
        DbError::BackpressureFail(reason) if reason == "bounded queue"
    ));
    assert!(matches!(
        roundtrip(DbError::ShuffleTerminal("invalid owner".into())).await,
        DbError::ShuffleTerminal(reason) if reason == "invalid owner"
    ));
    assert!(matches!(
        roundtrip(DbError::ManagedStateBudgetExceeded {
            context: "restore budget".into(),
            accounted_bytes: 17,
            limit_bytes: 16,
        })
        .await,
        DbError::ManagedStateBudgetExceeded {
            context,
            accounted_bytes: 17,
            limit_bytes: 16,
        } if context == "restore budget"
    ));
}

#[tokio::test]
async fn failed_start_cleanup_cancels_the_prepared_runtime_generation() {
    let db = LaminarDB::open().unwrap();
    let prepared = tokio_util::sync::CancellationToken::new();
    *db.runtime_shutdown.write() = prepared.clone();

    db.cleanup_failed_start().await.unwrap();

    assert!(prepared.is_cancelled());
    db.shutdown().await.unwrap();
}

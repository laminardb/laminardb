use super::*;

#[test]
fn test_send_recv() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (tx, mut rx) = channel::<i32>(16);
    tx.push(42).unwrap();
    let val = rt.block_on(rx.recv()).unwrap();
    assert_eq!(val, 42);
}

#[test]
fn test_try_push_full() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (tx, mut rx) = channel::<i32>(2);
    assert!(tx.try_push(1).is_ok());
    assert!(tx.try_push(2).is_ok());
    let err = tx.try_push(3);
    assert!(err.is_err());
    assert_eq!(err.unwrap_err().into_inner(), 3);
    assert_eq!(rt.block_on(rx.recv()).unwrap(), 1);
    assert!(tx.try_push(3).is_ok());
}

#[tokio::test]
async fn test_disconnected_on_drop() {
    let (tx, rx) = channel::<i32>(16);
    assert!(!rx.is_disconnected());
    drop(tx);
    assert!(rx.is_disconnected());
}

#[test]
fn test_closed_on_drop() {
    let (tx, rx) = channel::<i32>(16);
    assert!(!tx.is_closed());
    drop(rx);
    assert!(tx.is_closed());
}

#[test]
fn test_clone_multi_producer() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (tx, mut rx) = channel::<i32>(16);
    let tx2 = tx.clone();
    tx.push(1).unwrap();
    tx2.push(2).unwrap();
    let a = rt.block_on(rx.recv()).unwrap();
    let b = rt.block_on(rx.recv()).unwrap();
    let mut items = vec![a, b];
    items.sort_unstable();
    assert_eq!(items, vec![1, 2]);
}

#[tokio::test]
async fn test_async_recv() {
    let (tx, mut rx) = channel::<i32>(64);
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        tx.push(42).unwrap();
    });
    let val = rx.recv().await.unwrap();
    assert_eq!(val, 42);
}

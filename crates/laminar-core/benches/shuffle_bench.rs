//! Shuffle transport benchmarks over loopback gRPC.
//!
//! `send_to_caller`: time spent on the calling (Ring 0) thread per send, with
//! the queue kept un-full so enqueue cost isn't conflated with backpressure.
//! `end_to_end`: sustained throughput from `send_to` to receiver delivery.
//!
//! Run with: cargo bench -p laminar-core --features cluster --bench shuffle_bench

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use laminar_core::shuffle::{ShuffleMessage, ShuffleReceiver, ShuffleSender};
use tokio::runtime::Runtime;

const ROWS: usize = 1024;
/// Per-burst send count; kept well under the transport's 1024-slot queue so
/// caller-side timing never includes a full-queue park.
const BURST: u64 = 256;

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Float64, false),
        Field::new("w", DataType::Float64, false),
    ]));
    let v: Vec<f64> = (0..ROWS).map(|i| i as f64 * 0.5).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from((0..ROWS as i64).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(v.clone())),
            Arc::new(Float64Array::from(v)),
        ],
    )
    .unwrap()
}

struct Harness {
    sender: ShuffleSender,
    received: Arc<AtomicU64>,
}

async fn harness() -> Harness {
    let recv = ShuffleReceiver::bind(2, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let sender = ShuffleSender::new(1);
    sender.register_peer(2, recv.local_addr()).await;
    let received = Arc::new(AtomicU64::new(0));
    let counter = Arc::clone(&received);
    tokio::spawn(async move {
        while recv.recv().await.is_some() {
            counter.fetch_add(1, Ordering::Release);
        }
    });
    Harness { sender, received }
}

async fn drain_to(h: &Harness, target: u64) {
    while h.received.load(Ordering::Acquire) < target {
        tokio::task::yield_now().await;
    }
}

fn bench_shuffle(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let h = rt.block_on(harness());
    let msg = ShuffleMessage::VnodeData("s".into(), 0, batch());
    rt.block_on(async {
        h.sender.send_to(2, &msg).await.unwrap();
        drain_to(&h, 1).await;
    });

    let mut group = c.benchmark_group("shuffle");
    group.throughput(Throughput::Elements(ROWS as u64));

    group.bench_function("send_to_caller", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let mut total = Duration::ZERO;
                let mut remaining = iters;
                while remaining > 0 {
                    let burst = remaining.min(BURST);
                    let target = h.received.load(Ordering::Acquire) + burst;
                    let start = Instant::now();
                    for _ in 0..burst {
                        h.sender.send_to(2, &msg).await.unwrap();
                    }
                    total += start.elapsed();
                    drain_to(&h, target).await;
                    remaining -= burst;
                }
                total
            })
        });
    });

    group.bench_function("end_to_end", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async {
                let target = h.received.load(Ordering::Acquire) + iters;
                let start = Instant::now();
                for _ in 0..iters {
                    h.sender.send_to(2, &msg).await.unwrap();
                }
                drain_to(&h, target).await;
                start.elapsed()
            })
        });
    });

    group.finish();
}

criterion_group!(benches, bench_shuffle);
criterion_main!(benches);

//! End-to-end committed subscription gateway and slow-reader benchmarks.

use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use laminar_db::ClusterSubscriptionGatewayBenchmark;

fn bench_gateway_replay_merge(c: &mut Criterion) {
    const ROWS_PER_FRAME: u32 = 64;
    const TOTAL_FRAMES: u64 = 256;
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let single = runtime
        .block_on(ClusterSubscriptionGatewayBenchmark::new(
            1,
            u16::try_from(TOTAL_FRAMES).unwrap(),
            ROWS_PER_FRAME,
        ))
        .unwrap();
    let partitioned = runtime
        .block_on(ClusterSubscriptionGatewayBenchmark::new(
            64,
            4,
            ROWS_PER_FRAME,
        ))
        .unwrap();
    let mut group = c.benchmark_group("cluster_subscription_gateway_replay");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Elements(
        TOTAL_FRAMES * u64::from(ROWS_PER_FRAME),
    ));
    group.bench_function("one_partition_256_frames", |bencher| {
        bencher.iter(|| black_box(runtime.block_on(single.replay_once()).unwrap()));
    });
    group.bench_function("64_partitions_4_frames", |bencher| {
        bencher.iter(|| black_box(runtime.block_on(partitioned.replay_once()).unwrap()));
    });
    group.finish();
}

fn bench_slow_reader_memory_bound(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let fixture = runtime
        .block_on(ClusterSubscriptionGatewayBenchmark::new(64, 2, 65_536))
        .unwrap();
    let footprint = runtime.block_on(fixture.slow_reader_footprint()).unwrap();
    let mut group = c.benchmark_group("cluster_subscription_slow_reader");
    group.sample_size(10);
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(10));
    group.throughput(Throughput::Bytes(
        u64::try_from(footprint.queued_arrow_bytes).unwrap(),
    ));
    group.bench_function(
        BenchmarkId::new(
            "bounded_gateway_64_partitions",
            format!(
                "{}frames_{}bytes",
                footprint.queued_frames, footprint.queued_arrow_bytes
            ),
        ),
        |bencher| {
            bencher.iter(|| black_box(runtime.block_on(fixture.slow_reader_footprint()).unwrap()));
        },
    );
    group.finish();
}

criterion_group!(
    benches,
    bench_gateway_replay_merge,
    bench_slow_reader_memory_bound
);
criterion_main!(benches);

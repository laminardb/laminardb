//! Benchmarks for io_uring operations.
//!
//! These benchmarks only run on Linux with io_uring support.

use criterion::{criterion_group, criterion_main, Criterion};

#[cfg(all(target_os = "linux", feature = "io-uring"))]
use criterion::{BenchmarkId, Throughput};
#[cfg(all(target_os = "linux", feature = "io-uring"))]
use std::hint::black_box;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod linux_benchmarks {
    use super::*;
    use laminar_core::io_uring::{CoreRingManager, IoUringConfig, RingMode};
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::os::unix::io::AsRawFd;
    use tempfile::tempdir;

    fn make_config(mode: RingMode) -> IoUringConfig {
        IoUringConfig {
            ring_entries: 256,
            mode,
            sqpoll_idle_ms: 1000,
            sqpoll_cpu: None,
            buffer_size: 64 * 1024,
            buffer_count: 256,
            coop_taskrun: true,
            single_issuer: true,
            direct_table: false,
            direct_table_size: 256,
        }
    }

    pub fn bench_buffer_acquire_release(c: &mut Criterion) {
        let config = make_config(RingMode::Standard);
        let mut manager = match CoreRingManager::new(0, &config) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("io_uring not available: {e}");
                return;
            }
        };

        c.bench_function("io_uring/buffer_acquire_release", |b| {
            b.iter(|| {
                let (idx, buf) = manager.acquire_buffer().unwrap();
                black_box(buf[0]);
                manager.release_buffer(idx);
            });
        });
    }

    pub fn bench_write_latency(c: &mut Criterion) {
        let config = make_config(RingMode::Standard);
        let mut manager = match CoreRingManager::new(0, &config) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("io_uring not available: {e}");
                return;
            }
        };

        // Create temp file
        let dir = tempdir().unwrap();
        let path = dir.path().join("bench.dat");

        // Pre-allocate file
        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .unwrap();
            file.write_all(&vec![0u8; 64 * 1024 * 256]).unwrap();
            file.flush().unwrap();
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let fd = file.as_raw_fd();

        let mut group = c.benchmark_group("io_uring/write_latency");

        for size in [4096, 16384, 65536].iter() {
            group.throughput(Throughput::Bytes(*size as u64));
            group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
                b.iter(|| {
                    let (idx, buf) = manager.acquire_buffer().unwrap();
                    // Fill buffer with pattern
                    for i in 0..size.min(buf.len()) {
                        buf[i] = (i & 0xff) as u8;
                    }

                    let user_data = manager.submit_write(fd, idx, 0, size as u32).unwrap();
                    manager.submit().unwrap();
                    let completion = manager.wait_for(user_data).unwrap();
                    black_box(completion.result);
                    manager.release_buffer(idx);
                });
            });
        }
        group.finish();
    }

    pub fn bench_batch_writes(c: &mut Criterion) {
        let config = make_config(RingMode::Standard);
        let mut manager = match CoreRingManager::new(0, &config) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("io_uring not available: {e}");
                return;
            }
        };

        // Create temp file
        let dir = tempdir().unwrap();
        let path = dir.path().join("bench_batch.dat");

        {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)
                .unwrap();
            file.write_all(&vec![0u8; 64 * 1024 * 256]).unwrap();
            file.flush().unwrap();
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let fd = file.as_raw_fd();

        let mut group = c.benchmark_group("io_uring/batch_writes");

        for batch_size in [1, 4, 16, 64].iter() {
            group.throughput(Throughput::Elements(*batch_size as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(batch_size),
                batch_size,
                |b, &batch_size| {
                    b.iter(|| {
                        let mut user_datas = Vec::with_capacity(batch_size);
                        let mut buf_indices = Vec::with_capacity(batch_size);

                        // Submit batch
                        for i in 0..batch_size {
                            let (idx, buf) = manager.acquire_buffer().unwrap();
                            buf[0] = i as u8;
                            let user_data = manager
                                .submit_write(fd, idx, (i * 4096) as u64, 4096)
                                .unwrap();
                            user_datas.push(user_data);
                            buf_indices.push(idx);
                        }

                        manager.submit().unwrap();

                        // Wait for all
                        for _ in 0..batch_size {
                            let completions = manager.poll_completions();
                            for c in completions {
                                black_box(c.result);
                            }
                        }

                        // Release buffers
                        for idx in buf_indices {
                            manager.release_buffer(idx);
                        }
                    });
                },
            );
        }
        group.finish();
    }

    pub fn run_benchmarks(c: &mut Criterion) {
        bench_buffer_acquire_release(c);
        bench_write_latency(c);
        bench_batch_writes(c);
    }
}

#[cfg(not(all(target_os = "linux", feature = "io-uring")))]
mod linux_benchmarks {
    use super::*;

    pub fn run_benchmarks(_c: &mut Criterion) {
        eprintln!("io_uring benchmarks require Linux with io-uring feature enabled");
    }
}

fn io_uring_benchmarks(c: &mut Criterion) {
    linux_benchmarks::run_benchmarks(c);
}

criterion_group!(benches, io_uring_benchmarks);
criterion_main!(benches);

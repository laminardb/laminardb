use std::collections::VecDeque;

pub struct LatencyTracker {
    samples: VecDeque<u64>,
    capacity: usize,
    max: u64,
}

impl LatencyTracker {
    pub fn new() -> Self {
        Self {
            samples: VecDeque::with_capacity(1000),
            capacity: 1000,
            max: 0,
        }
    }

    pub fn record(&mut self, ns: u64) {
        if ns == 0 {
            return;
        }
        if self.samples.len() >= self.capacity {
            self.samples.pop_front();
        }
        self.samples.push_back(ns);
        if ns > self.max {
            self.max = ns;
        }
    }

    pub fn p50(&self) -> u64 {
        self.percentile(50)
    }

    pub fn p99(&self) -> u64 {
        self.percentile(99)
    }

    pub fn max(&self) -> u64 {
        self.max
    }

    fn percentile(&self, pct: usize) -> u64 {
        if self.samples.is_empty() {
            return 0;
        }
        let mut sorted: Vec<u64> = self.samples.iter().copied().collect();
        sorted.sort_unstable();
        let idx = (pct * sorted.len() / 100).min(sorted.len() - 1);
        sorted[idx]
    }
}

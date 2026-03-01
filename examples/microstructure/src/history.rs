use std::collections::VecDeque;

pub struct RingBuf<T> {
    data: VecDeque<T>,
    capacity: usize,
}

impl<T> RingBuf<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, value: T) {
        if self.data.len() >= self.capacity {
            self.data.pop_front();
        }
        self.data.push_back(value);
    }

    pub fn as_slice(&self) -> &VecDeque<T> {
        &self.data
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

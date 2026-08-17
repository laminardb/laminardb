use super::KafkaBlockingTasks;

impl KafkaBlockingTasks {
    pub(crate) async fn tracked_count(&self) -> usize {
        self.handles.lock().await.len()
    }
}

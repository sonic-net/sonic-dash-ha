use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

/// Swbus message IDs are defined as the startup time of the service
/// in Epoch nanoseconds plus the number of messages sent since it started.
/// This generator struct generates IDs following that scheme.
pub struct MessageIdGenerator {
    next_id: AtomicU64,
}

impl MessageIdGenerator {
    pub fn new() -> Self {
        let startup_epoch_nanos: u64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        Self {
            next_id: AtomicU64::new(startup_epoch_nanos),
        }
    }

    pub fn generate(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}
impl Default for MessageIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

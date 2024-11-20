use crate::swbus::SwbusMessageId;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

pub struct SwbusMessageIdGenerator {
    startup_epoch_nanos: u64,
    count: AtomicU64,
}

impl SwbusMessageIdGenerator {
    pub fn new() -> Self {
        let startup_epoch_nanos: u64 = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        Self {
            startup_epoch_nanos,
            count: AtomicU64::new(0),
        }
    }

    pub fn generate(&self) -> SwbusMessageId {
        SwbusMessageId {
            startup_epoch_nanos: self.startup_epoch_nanos,
            count: self.count.fetch_add(1, Ordering::SeqCst),
        }
    }
}

use super::*;
use crate::*;
use std::{os::fd::BorrowedFd, ptr::null, sync::Arc, time::Duration};

/// Rust wrapper around `swss::ZmqConsumerStateTable`.
#[derive(Debug)]
pub struct ZmqConsumerStateTable {
    ptr: SWSSZmqConsumerStateTable,
    _db: DbConnector,

    /// See [`DropGuard`] and [`ZmqServer`].
    _drop_guard: Arc<DropGuard>,
}

impl ZmqConsumerStateTable {
    pub fn new(
        db: DbConnector,
        table_name: &str,
        zmqs: &mut ZmqServer,
        pop_batch_size: Option<i32>,
        pri: Option<i32>,
    ) -> Self {
        let table_name = cstr(table_name);
        let pop_batch_size = pop_batch_size.as_ref().map(|n| n as *const i32).unwrap_or(null());
        let pri = pri.as_ref().map(|n| n as *const i32).unwrap_or(null());
        let ptr = unsafe { SWSSZmqConsumerStateTable_new(db.ptr, table_name.as_ptr(), zmqs.ptr, pop_batch_size, pri) };
        let drop_guard = Arc::new(DropGuard(ptr));
        zmqs.register_consumer_state_table(drop_guard.clone());
        Self {
            ptr,
            _db: db,
            _drop_guard: drop_guard,
        }
    }

    pub fn pops(&self) -> Vec<KeyOpFieldValues> {
        unsafe {
            let ans = SWSSZmqConsumerStateTable_pops(self.ptr);
            take_key_op_field_values_array(ans)
        }
    }

    pub fn get_fd(&self) -> BorrowedFd {
        let fd = unsafe { SWSSZmqConsumerStateTable_getFd(self.ptr) };

        // SAFETY: This fd represents the underlying zmq socket, which should remain alive as long as there
        // is a listener (i.e. a ZmqConsumerStateTable)
        unsafe { BorrowedFd::borrow_raw(fd.try_into().unwrap()) }
    }

    pub fn read_data(&self, timeout: Duration, interrupt_on_signal: bool) -> SelectResult {
        let timeout_ms = timeout.as_millis().try_into().unwrap();
        let res = unsafe { SWSSZmqConsumerStateTable_readData(self.ptr, timeout_ms, interrupt_on_signal as u8) };
        SelectResult::from_raw(res)
    }
}

impl_read_data_async!(ZmqConsumerStateTable);

unsafe impl Send for ZmqConsumerStateTable {}

/// A type that will free the underlying `ZmqConsumerStateTable` when it is dropped.
/// This is shared with `ZmqServer`
#[derive(Debug)]
pub(crate) struct DropGuard(SWSSZmqConsumerStateTable);

impl Drop for DropGuard {
    fn drop(&mut self) {
        unsafe { SWSSZmqConsumerStateTable_free(self.0) };
    }
}

unsafe impl Send for DropGuard {}

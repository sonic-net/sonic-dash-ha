use super::*;
use crate::*;
use std::{os::fd::BorrowedFd, ptr::null, time::Duration};

/// Rust wrapper around `swss::ConsumerStateTable`.
#[derive(Debug)]
pub struct ConsumerStateTable {
    ptr: SWSSConsumerStateTable,
    _db: DbConnector,
}

impl ConsumerStateTable {
    pub fn new(db: DbConnector, table_name: &str, pop_batch_size: Option<i32>, pri: Option<i32>) -> Self {
        let table_name = cstr(table_name);
        let pop_batch_size = pop_batch_size.as_ref().map(|n| n as *const i32).unwrap_or(null());
        let pri = pri.as_ref().map(|n| n as *const i32).unwrap_or(null());
        let ptr = unsafe { SWSSConsumerStateTable_new(db.ptr, table_name.as_ptr(), pop_batch_size, pri) };
        Self { ptr, _db: db }
    }

    pub fn pops(&self) -> Vec<KeyOpFieldValues> {
        unsafe {
            let ans = SWSSConsumerStateTable_pops(self.ptr);
            take_key_op_field_values_array(ans)
        }
    }

    pub fn get_fd(&self) -> BorrowedFd {
        let fd = unsafe { SWSSConsumerStateTable_getFd(self.ptr) };

        // SAFETY: This fd represents the underlying redis connection, which should stay alive
        // as long as the DbConnector does.
        unsafe { BorrowedFd::borrow_raw(fd.try_into().unwrap()) }
    }

    pub fn read_data(&self, timeout: Duration, interrupt_on_signal: bool) -> SelectResult {
        let timeout_ms = timeout.as_millis().try_into().unwrap();
        let res = unsafe { SWSSConsumerStateTable_readData(self.ptr, timeout_ms, interrupt_on_signal as u8) };
        SelectResult::from_raw(res)
    }
}

impl Drop for ConsumerStateTable {
    fn drop(&mut self) {
        unsafe { SWSSConsumerStateTable_free(self.ptr) };
    }
}

unsafe impl Send for ConsumerStateTable {}

/// Async versions of methods
#[cfg(feature = "async")]
impl ConsumerStateTable {
    async_util::impl_read_data_async!();
}

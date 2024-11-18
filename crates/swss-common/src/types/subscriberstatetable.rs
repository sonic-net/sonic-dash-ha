use super::*;
use crate::*;
use std::{os::fd::BorrowedFd, ptr::null, time::Duration};

/// Rust wrapper around `swss::SubscriberStateTable`.
#[derive(Debug)]
pub struct SubscriberStateTable {
    ptr: SWSSSubscriberStateTable,
    _db: DbConnector,
}

impl SubscriberStateTable {
    pub fn new(db: DbConnector, table_name: &str, pop_batch_size: Option<i32>, pri: Option<i32>) -> Self {
        let table_name = cstr(table_name);
        let pop_batch_size = pop_batch_size.as_ref().map(|n| n as *const i32).unwrap_or(null());
        let pri = pri.as_ref().map(|n| n as *const i32).unwrap_or(null());
        let ptr = unsafe { SWSSSubscriberStateTable_new(db.ptr, table_name.as_ptr(), pop_batch_size, pri).into() };
        Self { ptr, _db: db }
    }

    pub fn pops(&self) -> Vec<KeyOpFieldValues> {
        unsafe {
            let ans = SWSSSubscriberStateTable_pops(self.ptr);
            take_key_op_field_values_array(ans)
        }
    }

    pub fn read_data(&self, timeout: Duration, interrupt_on_signal: bool) -> SelectResult {
        let timeout_ms = timeout.as_millis().try_into().unwrap();
        let res = unsafe { SWSSSubscriberStateTable_readData(self.ptr, timeout_ms, interrupt_on_signal as u8) };
        SelectResult::from_raw(res)
    }

    pub fn get_fd(&self) -> BorrowedFd {
        let fd = unsafe { SWSSSubscriberStateTable_getFd(self.ptr) };

        // SAFETY: This fd represents the underlying redis connection, which should remain open as
        // long as the DbConnector is alive
        unsafe { BorrowedFd::borrow_raw(fd.try_into().unwrap()) }
    }
}

impl_read_data_async!(SubscriberStateTable);

impl Drop for SubscriberStateTable {
    fn drop(&mut self) {
        unsafe { SWSSSubscriberStateTable_free(self.ptr) };
    }
}

unsafe impl Send for SubscriberStateTable {}

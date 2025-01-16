use super::*;
use crate::bindings::*;
use std::{os::fd::BorrowedFd, ptr::null, time::Duration};

/// Rust wrapper around `swss::ConsumerStateTable`.
#[derive(Debug)]
pub struct ConsumerStateTable {
    ptr: SWSSConsumerStateTable,
    _db: DbConnector,
}

impl ConsumerStateTable {
    pub fn new(db: DbConnector, table_name: &str, pop_batch_size: Option<i32>, pri: Option<i32>) -> Result<Self> {
        let table_name = cstr(table_name);
        let pop_batch_size = pop_batch_size.as_ref().map(|n| n as *const i32).unwrap_or(null());
        let pri = pri.as_ref().map(|n| n as *const i32).unwrap_or(null());
        let ptr = unsafe {
            Exception::try1(|p_cst| SWSSConsumerStateTable_new(db.ptr, table_name.as_ptr(), pop_batch_size, pri, p_cst))
        }?;
        Ok(Self { ptr, _db: db })
    }

    pub fn pops(&self) -> Result<Vec<KeyOpFieldValues>> {
        unsafe {
            let arr = Exception::try1(|p_arr| SWSSConsumerStateTable_pops(self.ptr, p_arr))?;
            Ok(take_key_op_field_values_array(arr))
        }
    }

    pub fn get_fd(&self) -> Result<BorrowedFd> {
        // SAFETY: This fd represents the underlying redis connection, which should stay alive
        // as long as the DbConnector does.
        unsafe {
            let fd = Exception::try1(|p_fd| SWSSConsumerStateTable_getFd(self.ptr, p_fd))?;
            let fd = BorrowedFd::borrow_raw(fd.try_into().unwrap());
            Ok(fd)
        }
    }

    pub fn read_data(&self, timeout: Duration, interrupt_on_signal: bool) -> Result<SelectResult> {
        let timeout_ms = timeout.as_millis().try_into().unwrap();
        let res = unsafe {
            Exception::try1(|p_res| {
                SWSSConsumerStateTable_readData(self.ptr, timeout_ms, interrupt_on_signal as u8, p_res)
            })?
        };
        Ok(SelectResult::from_raw(res))
    }
}

impl Drop for ConsumerStateTable {
    fn drop(&mut self) {
        unsafe { Exception::try0(SWSSConsumerStateTable_free(self.ptr)).expect("Dropping ConsumerStateTable") };
    }
}

unsafe impl Send for ConsumerStateTable {}

/// Async versions of methods
#[cfg(feature = "async")]
impl ConsumerStateTable {
    async_util::impl_read_data_async!();
}

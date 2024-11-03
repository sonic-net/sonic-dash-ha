use super::*;
use crate::*;
use std::{ptr::null, rc::Rc, time::Duration};

obj_wrapper! {
    struct SubscriberStateTableObj { ptr: SWSSSubscriberStateTable } SWSSSubscriberStateTable_free
}

/// Rust wrapper around `swss::SubscriberStateTable`.
#[derive(Clone, Debug)]
pub struct SubscriberStateTable {
    obj: Rc<SubscriberStateTableObj>,
    _db: DbConnector,
}

impl SubscriberStateTable {
    pub fn new(db: DbConnector, table_name: &str, pop_batch_size: Option<i32>, pri: Option<i32>) -> Self {
        let table_name = cstr(table_name);
        let pop_batch_size = pop_batch_size.map(|n| &n as *const i32).unwrap_or(null());
        let pri = pri.map(|n| &n as *const i32).unwrap_or(null());
        let obj = unsafe {
            Rc::new(SWSSSubscriberStateTable_new(db.obj.ptr, table_name.as_ptr(), pop_batch_size, pri).into())
        };
        Self { obj, _db: db }
    }

    pub fn pops(&self) -> Vec<KeyOpFieldValues> {
        unsafe {
            let ans = SWSSSubscriberStateTable_pops(self.obj.ptr);
            take_key_op_field_values_array(ans)
        }
    }

    pub fn read_data(&self, timeout: Duration) -> SelectResult {
        let timeout_ms = timeout.as_millis().try_into().unwrap();
        let res = unsafe { SWSSSubscriberStateTable_readData(self.obj.ptr, timeout_ms) };
        SelectResult::from_raw(res)
    }

    pub fn has_data(&self) -> bool {
        unsafe { SWSSSubscriberStateTable_hasData(self.obj.ptr) == 1 }
    }

    pub fn has_cached_data(&self) -> bool {
        unsafe { SWSSSubscriberStateTable_hasCachedData(self.obj.ptr) == 1 }
    }

    pub fn initialized_with_data(&self) -> bool {
        unsafe { SWSSSubscriberStateTable_initializedWithData(self.obj.ptr) == 1 }
    }
}

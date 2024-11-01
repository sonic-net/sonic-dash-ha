use super::*;
use crate::*;
use std::{ptr::null, rc::Rc, time::Duration};

obj_wrapper! {
    struct ZmqConsumerStateTableObj { ptr: SWSSZmqConsumerStateTable } SWSSZmqConsumerStateTable_free
}

#[derive(Clone, Debug)]
pub struct ZmqConsumerStateTable {
    obj: Rc<ZmqConsumerStateTableObj>,
    _db: DbConnector,
    // ZmqConsumerStateTable does not own a copy of the ZmqServer because the ZmqServer must be
    // destroyed first (otherwise its worker thread might call a destroyed ZmqMessageHandler).
    // Instead, the ZmqServer owns a copy of all handlers registered to it, so they can be kept
    // alive until the ZmqServer is destroyed.
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
        let pop_batch_size = pop_batch_size.map(|n| &n as *const i32).unwrap_or(null());
        let pri = pri.map(|n| &n as *const i32).unwrap_or(null());
        let obj = unsafe {
            let p = SWSSZmqConsumerStateTable_new(db.obj.ptr, table_name.as_ptr(), zmqs.obj.ptr, pop_batch_size, pri);
            Rc::new(p.into())
        };
        let self_ = Self { obj, _db: db };
        zmqs.register_consumer_state_table(self_.clone());
        self_
    }

    pub fn pops(&self) -> Vec<KeyOpFieldValues> {
        unsafe {
            let ans = SWSSZmqConsumerStateTable_pops(self.obj.ptr);
            take_key_op_field_values_array(ans)
        }
    }

    pub fn read_data(&self, timeout: Duration) -> SelectResult {
        let timeout_ms = timeout.as_millis().try_into().unwrap();
        let res = unsafe { SWSSZmqConsumerStateTable_readData(self.obj.ptr, timeout_ms) };
        SelectResult::from_raw(res)
    }

    pub fn has_data(&self) -> bool {
        unsafe { SWSSZmqConsumerStateTable_hasData(self.obj.ptr) == 1 }
    }

    pub fn has_cached_data(&self) -> bool {
        unsafe { SWSSZmqConsumerStateTable_hasCachedData(self.obj.ptr) == 1 }
    }

    pub fn initialized_with_data(&self) -> bool {
        unsafe { SWSSZmqConsumerStateTable_initializedWithData(self.obj.ptr) == 1 }
    }
}

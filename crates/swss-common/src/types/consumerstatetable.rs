use super::*;
use crate::*;
use std::{ptr::null, rc::Rc};

obj_wrapper! {
    struct ConsumerStateTableObj { ptr: SWSSConsumerStateTable } SWSSConsumerStateTable_free
}

#[derive(Clone, Debug)]
pub struct ConsumerStateTable {
    obj: Rc<ConsumerStateTableObj>,
    _db: DbConnector,
}

impl ConsumerStateTable {
    pub fn new(db: DbConnector, table_name: &str, pop_batch_size: Option<i32>, pri: Option<i32>) -> Self {
        let table_name = cstr(table_name);
        let pop_batch_size = pop_batch_size.map(|n| &n as *const i32).unwrap_or(null());
        let pri = pri.map(|n| &n as *const i32).unwrap_or(null());
        let obj =
            unsafe { Rc::new(SWSSConsumerStateTable_new(db.obj.ptr, table_name.as_ptr(), pop_batch_size, pri).into()) };
        Self { obj, _db: db }
    }

    pub fn pops(&self) -> Vec<KeyOpFieldValues> {
        unsafe {
            let ans = SWSSConsumerStateTable_pops(self.obj.ptr);
            take_key_op_field_values_array(ans)
        }
    }
}

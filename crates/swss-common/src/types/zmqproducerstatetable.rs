use super::*;
use crate::*;
use std::rc::Rc;

obj_wrapper! {
    struct ZmqProducerStateTableObj { ptr: SWSSZmqProducerStateTable } SWSSZmqProducerStateTable_free
}

/// Rust wrapper around `swss::ZmqProducerStateTable`.
#[derive(Clone, Debug)]
pub struct ZmqProducerStateTable {
    obj: Rc<ZmqProducerStateTableObj>,
    _db: DbConnector,
    _zmqc: ZmqClient,
}

impl ZmqProducerStateTable {
    pub fn new(db: DbConnector, table_name: &str, zmqc: ZmqClient, db_persistence: bool) -> Self {
        let table_name = cstr(table_name);
        let obj = unsafe {
            Rc::new(
                SWSSZmqProducerStateTable_new(db.obj.ptr, table_name.as_ptr(), zmqc.obj.ptr, db_persistence as u8)
                    .into(),
            )
        };
        Self {
            obj,
            _db: db,
            _zmqc: zmqc,
        }
    }

    pub fn set<I, F, V>(&self, key: &str, fvs: I)
    where
        I: IntoIterator<Item = (F, V)>,
        F: AsRef<[u8]>,
        V: Into<CxxString>,
    {
        let key = cstr(key);
        let (arr, _k) = make_field_value_array(fvs);
        unsafe { SWSSZmqProducerStateTable_set(self.obj.ptr, key.as_ptr(), arr) };
    }

    pub fn del(&self, key: &str) {
        let key = cstr(key);
        unsafe { SWSSZmqProducerStateTable_del(self.obj.ptr, key.as_ptr()) };
    }

    pub fn db_updater_queue_size(&self) -> u64 {
        unsafe { SWSSZmqProducerStateTable_dbUpdaterQueueSize(self.obj.ptr) }
    }
}

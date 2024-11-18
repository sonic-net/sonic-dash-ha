use super::*;
use crate::*;

/// Rust wrapper around `swss::ZmqProducerStateTable`.
#[derive(Debug)]
pub struct ZmqProducerStateTable {
    ptr: SWSSZmqProducerStateTable,
    _db: DbConnector,
    _zmqc: ZmqClient,
}

impl ZmqProducerStateTable {
    pub fn new(db: DbConnector, table_name: &str, zmqc: ZmqClient, db_persistence: bool) -> Self {
        let table_name = cstr(table_name);
        let db_persistence = db_persistence as u8;
        let ptr = unsafe { SWSSZmqProducerStateTable_new(db.ptr, table_name.as_ptr(), zmqc.ptr, db_persistence) };
        Self {
            ptr,
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
        unsafe { SWSSZmqProducerStateTable_set(self.ptr, key.as_ptr(), arr) };
    }

    pub fn del(&self, key: &str) {
        let key = cstr(key);
        unsafe { SWSSZmqProducerStateTable_del(self.ptr, key.as_ptr()) };
    }

    pub fn db_updater_queue_size(&self) -> u64 {
        unsafe { SWSSZmqProducerStateTable_dbUpdaterQueueSize(self.ptr) }
    }
}

impl Drop for ZmqProducerStateTable {
    fn drop(&mut self) {
        unsafe { SWSSZmqProducerStateTable_free(self.ptr) };
    }
}

unsafe impl Send for ZmqProducerStateTable {}

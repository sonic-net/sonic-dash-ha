use super::*;
use crate::*;
use std::rc::Rc;

obj_wrapper! {
    struct ProducerStateTableObj { ptr: SWSSProducerStateTable } SWSSProducerStateTable_free
}

/// Rust wrapper around `swss::ProducerStateTable`.
#[derive(Clone, Debug)]
pub struct ProducerStateTable {
    obj: Rc<ProducerStateTableObj>,
    _db: DbConnector,
}

impl ProducerStateTable {
    pub fn new(db: DbConnector, table_name: &str) -> Self {
        let table_name = cstr(table_name);
        let obj = Rc::new(unsafe { SWSSProducerStateTable_new(db.obj.ptr, table_name.as_ptr()).into() });
        Self { obj, _db: db }
    }

    pub fn set_buffered(&self, buffered: bool) {
        unsafe { SWSSProducerStateTable_setBuffered(self.obj.ptr, buffered as u8) };
    }

    pub fn set<I, F, V>(&self, key: &str, fvs: I)
    where
        I: IntoIterator<Item = (F, V)>,
        F: AsRef<[u8]>,
        V: Into<CxxString>,
    {
        let key = cstr(key);
        let (arr, _k) = make_field_value_array(fvs);
        unsafe { SWSSProducerStateTable_set(self.obj.ptr, key.as_ptr(), arr) };
    }

    pub fn del(&self, key: &str) {
        let key = cstr(key);
        unsafe { SWSSProducerStateTable_del(self.obj.ptr, key.as_ptr()) };
    }

    pub fn flush(&self) {
        unsafe { SWSSProducerStateTable_flush(self.obj.ptr) };
    }

    pub fn count(&self) -> i64 {
        unsafe { SWSSProducerStateTable_count(self.obj.ptr) }
    }

    pub fn clear(&self) {
        unsafe { SWSSProducerStateTable_clear(self.obj.ptr) };
    }

    pub fn create_temp_view(&self) {
        unsafe { SWSSProducerStateTable_create_temp_view(self.obj.ptr) };
    }

    pub fn apply_temp_view(&self) {
        unsafe { SWSSProducerStateTable_apply_temp_view(self.obj.ptr) };
    }
}

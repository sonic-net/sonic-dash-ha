use super::*;
use crate::bindings::*;

/// Rust wrapper around `swss::ProducerStateTable`.
#[derive(Debug)]
pub struct ProducerStateTable {
    ptr: SWSSProducerStateTable,
    _db: DbConnector,
}

impl ProducerStateTable {
    pub fn new(db: DbConnector, table_name: &str) -> Self {
        let table_name = cstr(table_name);
        let ptr = unsafe { SWSSProducerStateTable_new(db.ptr, table_name.as_ptr()) };
        Self { ptr, _db: db }
    }

    pub fn set_buffered(&self, buffered: bool) {
        unsafe { SWSSProducerStateTable_setBuffered(self.ptr, buffered as u8) };
    }

    pub fn set<I, F, V>(&self, key: &str, fvs: I)
    where
        I: IntoIterator<Item = (F, V)>,
        F: AsRef<[u8]>,
        V: Into<CxxString>,
    {
        let key = cstr(key);
        let (arr, _k) = make_field_value_array(fvs);
        unsafe { SWSSProducerStateTable_set(self.ptr, key.as_ptr(), arr) };
    }

    pub fn del(&self, key: &str) {
        let key = cstr(key);
        unsafe { SWSSProducerStateTable_del(self.ptr, key.as_ptr()) };
    }

    pub fn flush(&self) {
        unsafe { SWSSProducerStateTable_flush(self.ptr) };
    }

    pub fn count(&self) -> i64 {
        unsafe { SWSSProducerStateTable_count(self.ptr) }
    }

    pub fn clear(&self) {
        unsafe { SWSSProducerStateTable_clear(self.ptr) };
    }

    pub fn create_temp_view(&self) {
        unsafe { SWSSProducerStateTable_create_temp_view(self.ptr) };
    }

    pub fn apply_temp_view(&self) {
        unsafe { SWSSProducerStateTable_apply_temp_view(self.ptr) };
    }
}

impl Drop for ProducerStateTable {
    fn drop(&mut self) {
        unsafe { SWSSProducerStateTable_free(self.ptr) };
    }
}

unsafe impl Send for ProducerStateTable {}

#[cfg(feature = "async")]
impl ProducerStateTable {
    async_util::impl_basic_async_method!(
        set_async <= set<I, F, V>(&self, key: &str, fvs: I)
                     where
                         I: IntoIterator<Item = (F, V)> + Send,
                         F: AsRef<[u8]>,
                         V: Into<CxxString>,
    );
    async_util::impl_basic_async_method!(del_async <= del(&self, key: &str));
    async_util::impl_basic_async_method!(flush_async <= flush(&self));
}

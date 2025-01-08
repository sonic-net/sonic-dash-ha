use std::ptr;

use super::*;
use crate::*;

#[derive(Debug)]
pub struct Table {
    ptr: SWSSTable,
    _db: DbConnector,
}

impl Table {
    pub fn new(db: DbConnector, table_name: &str) -> Self {
        let table_name = cstr(table_name);
        let ptr = unsafe { SWSSTable_new(db.ptr, table_name.as_ptr()) };
        Self { ptr, _db: db }
    }

    pub fn get(&self, key: &str) -> Option<FieldValues> {
        let key = cstr(key);
        let mut arr = SWSSFieldValueArray {
            len: 0,
            data: ptr::null_mut(),
        };
        let exists = unsafe { SWSSTable_get(self.ptr, key.as_ptr(), &mut arr) };
        if exists == 1 {
            Some(unsafe { take_field_value_array(arr) })
        } else {
            None
        }
    }

    pub fn hget(&self, key: &str, field: &str) -> Option<CxxString> {
        let key = cstr(key);
        let field = cstr(field);
        let mut val: SWSSString = ptr::null_mut();
        let exists = unsafe { SWSSTable_hget(self.ptr, key.as_ptr(), field.as_ptr(), &mut val) };
        if exists == 1 {
            Some(unsafe { CxxString::take_raw(&mut val).unwrap() })
        } else {
            None
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
        unsafe { SWSSTable_set(self.ptr, key.as_ptr(), arr) };
    }

    pub fn hset(&self, key: &str, field: &str, val: &CxxStr) {
        let key = cstr(key);
        let field = cstr(field);
        unsafe { SWSSTable_hset(self.ptr, key.as_ptr(), field.as_ptr(), val.as_raw()) };
    }

    pub fn del(&self, key: &str) {
        let key = cstr(key);
        unsafe { SWSSTable_del(self.ptr, key.as_ptr()) };
    }

    pub fn hdel(&self, key: &str, field: &str) {
        let key = cstr(key);
        let field = cstr(field);
        unsafe { SWSSTable_hdel(self.ptr, key.as_ptr(), field.as_ptr()) };
    }

    pub fn get_keys(&self) -> Vec<String> {
        unsafe { take_string_array(SWSSTable_getKeys(self.ptr)) }
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        unsafe { SWSSTable_free(self.ptr) };
    }
}

unsafe impl Send for Table {}

#[cfg(feature = "async")]
impl Table {
    async_util::impl_basic_async_method!(new_async <= new(db: DbConnector, table_name: &str) -> Self);
    async_util::impl_basic_async_method!(get_async <= get(&self, key: &str) -> Option<FieldValues>);
    async_util::impl_basic_async_method!(hget_async <= hget(&self, key: &str, field: &str) -> Option<CxxString>);
    async_util::impl_basic_async_method!(
        set_async <= set<I, F, V>(&self, key: &str, fvs: I)
                     where
                         I: IntoIterator<Item = (F, V)> + Send,
                         F: AsRef<[u8]>,
                         V: Into<CxxString>,
    );
    async_util::impl_basic_async_method!(hset_async <= hset(&self, key: &str, field: &str, value: &CxxStr));
    async_util::impl_basic_async_method!(del_async <= del(&self, key: &str));
    async_util::impl_basic_async_method!(hdel_async <= hdel(&self, key: &str, field: &str));
    async_util::impl_basic_async_method!(get_keys_async <= get_keys(&self) -> Vec<String>);
}

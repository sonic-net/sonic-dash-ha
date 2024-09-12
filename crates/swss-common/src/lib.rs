mod bindings {
    #![allow(unused, non_snake_case, non_upper_case_globals, non_camel_case_types)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use std::{
    collections::HashMap,
    ffi::{CStr, CString},
    ptr::null,
    slice,
};

use crate::bindings::*;

unsafe fn free<T>(ptr: *mut T) {
    libc::free(ptr as *mut libc::c_void);
}

unsafe fn str(ptr: *mut i8) -> String {
    let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
    free(ptr);
    s
}

unsafe fn field_value_array(map: SWSSFieldValueArray) -> HashMap<String, String> {
    let mut out = HashMap::with_capacity(map.len as usize);
    if !map.data.is_null() {
        let entries = slice::from_raw_parts(map.data, map.len as usize);
        for fv in entries {
            let field = str(fv.field);
            let value = str(fv.value);
            out.insert(field, value);
        }
        free(map.data);
    }
    out
}

unsafe fn key_op_field_values_array(arr: SWSSKeyOpFieldValuesArray) -> Vec<KeyOpFieldValues> {
    let mut out = Vec::with_capacity(arr.len as usize);
    if !arr.data.is_null() {
        unsafe {
            let kfvs = slice::from_raw_parts(arr.data, arr.len as usize);
            for kfv in kfvs {
                out.push(KeyOpFieldValues {
                    key: str(kfv.key),
                    operation: str(kfv.operation),
                    field_values: field_value_array(kfv.fieldValues),
                });
            }
            free(arr.data)
        };
    }
    out
}

fn cstr(s: &str) -> CString {
    CString::new(s).expect("str must not contain null bytes")
}

pub struct KeyOpFieldValues {
    pub key: String,
    pub operation: String,
    pub field_values: HashMap<String, String>,
}

pub struct DBConnector {
    db: SWSSDBConnector,
}

impl DBConnector {
    pub fn new_tcp(db_id: i32, hostname: &str, port: u16, timeout: u32) -> DBConnector {
        let hostname = cstr(hostname);
        Self {
            db: unsafe { SWSSDBConnector_new_tcp(db_id, hostname.as_ptr(), port, timeout) },
        }
    }

    pub fn new_unix(db_id: i32, sock_path: &str, timeout: u32) -> DBConnector {
        let sock_path = cstr(sock_path);
        Self {
            db: unsafe { SWSSDBConnector_new_unix(db_id, sock_path.as_ptr(), timeout) },
        }
    }

    pub fn del(&self, key: &str) -> bool {
        let key = cstr(key);
        unsafe { SWSSDBConnector_del(self.db, key.as_ptr()) == 1 }
    }

    pub fn set(&self, key: &str, val: &str) {
        let key = cstr(key);
        let val = cstr(val);
        unsafe { SWSSDBConnector_set(self.db, key.as_ptr(), val.as_ptr()) };
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let key = cstr(key);
        unsafe {
            let ans = SWSSDBConnector_get(self.db, key.as_ptr());
            if ans.is_null() {
                None
            } else {
                Some(str(ans))
            }
        }
    }

    pub fn exists(&self, key: &str) -> bool {
        let key = cstr(key);
        unsafe { SWSSDBConnector_exists(self.db, key.as_ptr()) == 1 }
    }

    pub fn hdel(&self, key: &str, field: &str) -> bool {
        let key = cstr(key);
        let field = cstr(field);
        unsafe { SWSSDBConnector_hdel(self.db, key.as_ptr(), field.as_ptr()) == 1 }
    }

    pub fn hset(&self, key: &str, field: &str, val: &str) {
        let key = cstr(key);
        let field = cstr(field);
        let val = cstr(val);
        unsafe { SWSSDBConnector_hset(self.db, key.as_ptr(), field.as_ptr(), val.as_ptr()) };
    }

    pub fn hget(&self, key: &str, field: &str) -> Option<String> {
        let key = cstr(key);
        let field = cstr(field);
        unsafe {
            let ans = SWSSDBConnector_hget(self.db, key.as_ptr(), field.as_ptr());
            if ans.is_null() {
                None
            } else {
                Some(str(ans))
            }
        }
    }

    pub fn hgetall(&self, key: &str) -> HashMap<String, String> {
        let key = cstr(key);
        unsafe {
            let ans = SWSSDBConnector_hgetall(self.db, key.as_ptr());
            field_value_array(ans)
        }
    }

    pub fn hexists(&self, key: &str, field: &str) -> bool {
        let key = cstr(key);
        let field = cstr(field);
        unsafe { SWSSDBConnector_hexists(self.db, key.as_ptr(), field.as_ptr()) == 1 }
    }

    pub fn flush_db(&self) -> bool {
        unsafe { SWSSDBConnector_flushdb(self.db) == 1 }
    }
}

impl Drop for DBConnector {
    fn drop(&mut self) {
        unsafe { SWSSDBConnector_free(self.db) }
    }
}

pub struct SubscriberStateTable<'a> {
    tbl: SWSSSubscriberStateTable,
    _db: &'a DBConnector,
}

impl<'a> SubscriberStateTable<'a> {
    pub fn new(db: &'a DBConnector, table_name: &str, pop_batch_size: Option<i32>, pri: Option<i32>) -> Self {
        let table_name = cstr(table_name);
        let pop_batch_size = pop_batch_size.map(|n| &n as *const i32).unwrap_or(null());
        let pri = pri.map(|n| &n as *const i32).unwrap_or(null());
        let tbl = unsafe { SWSSSubscriberStateTable_new(db.db, table_name.as_ptr(), pop_batch_size, pri) };
        Self { tbl, _db: db }
    }

    pub fn pops(&self) -> Vec<KeyOpFieldValues> {
        unsafe {
            let ans = SWSSSubscriberStateTable_pops(self.tbl);
            key_op_field_values_array(ans)
        }
    }

    pub fn has_data(&self) -> bool {
        unsafe { SWSSSubscriberStateTable_hasData(self.tbl) == 1 }
    }

    pub fn has_cached_data(&self) -> bool {
        unsafe { SWSSSubscriberStateTable_hasCachedData(self.tbl) == 1 }
    }

    pub fn initialized_with_data(&self) -> bool {
        unsafe { SWSSSubscriberStateTable_initializedWithData(self.tbl) == 1 }
    }

    pub fn read_data(&self) {
        unsafe { SWSSSubscriberStateTable_readData(self.tbl) };
    }
}

impl Drop for SubscriberStateTable<'_> {
    fn drop(&mut self) {
        unsafe { SWSSSubscriberStateTable_free(self.tbl) };
    }
}

pub struct ConsumerStateTable<'a> {
    tbl: SWSSConsumerStateTable,
    _db: &'a DBConnector,
}

impl<'a> ConsumerStateTable<'a> {
    pub fn new(db: &'a DBConnector, table_name: &str, pop_batch_size: Option<i32>, pri: Option<i32>) -> Self {
        let table_name = cstr(table_name);
        let pop_batch_size = pop_batch_size.map(|n| &n as *const i32).unwrap_or(null());
        let pri = pri.map(|n| &n as *const i32).unwrap_or(null());
        let tbl = unsafe { SWSSConsumerStateTable_new(db.db, table_name.as_ptr(), pop_batch_size, pri) };
        Self { tbl, _db: db }
    }

    pub fn pops(&self) -> Vec<KeyOpFieldValues> {
        unsafe {
            let ans = SWSSConsumerStateTable_pops(self.tbl);
            key_op_field_values_array(ans)
        }
    }
}

impl Drop for ConsumerStateTable<'_> {
    fn drop(&mut self) {
        unsafe { SWSSConsumerStateTable_free(self.tbl) };
    }
}

pub struct ProducerStateTable<'a> {
    tbl: SWSSProducerStateTable,
    _db: &'a DBConnector,
}

impl<'a> ProducerStateTable<'a> {
    pub fn new(db: &'a DBConnector, table_name: &str) -> Self {
        let table_name = cstr(table_name);
        let tbl = unsafe { SWSSProducerStateTable_new(db.db, table_name.as_ptr()) };
        Self { tbl, _db: db }
    }

    pub fn set_buffered(&self, buffered: bool) {
        unsafe { SWSSProducerStateTable_setBuffered(self.tbl, buffered as u8) };
    }

    pub fn set<I, S>(&self, key: &str, fv_iter: I)
    where
        I: IntoIterator<Item = (S, S)>,
        S: for<'s> Into<&'s str>,
    {
        let mut pairs: Vec<SWSSFieldValuePair> = fv_iter
            .into_iter()
            .map(|(f, v)| SWSSFieldValuePair {
                field: cstr(f.into()).into_raw(),
                value: cstr(v.into()).into_raw(),
            })
            .collect();

        let key = cstr(key);
        let arr = SWSSFieldValueArray {
            len: pairs.len() as u64,
            data: pairs.as_mut_ptr(),
        };
        unsafe { SWSSProducerStateTable_set(self.tbl, key.as_ptr(), arr) };

        // Free those CStrings that we made with into_raw()
        for SWSSFieldValuePair { field, value } in pairs {
            unsafe {
                drop(CString::from_raw(field));
                drop(CString::from_raw(value));
            }
        }
    }

    pub fn del(&self, key: &str) {
        let key = cstr(key);
        unsafe { SWSSProducerStateTable_del(self.tbl, key.as_ptr()) };
    }

    pub fn flush(&self) {
        unsafe { SWSSProducerStateTable_flush(self.tbl) };
    }

    pub fn count(&self) -> i64 {
        unsafe { SWSSProducerStateTable_count(self.tbl) }
    }

    pub fn clear(&self) {
        unsafe { SWSSProducerStateTable_clear(self.tbl) };
    }

    pub fn create_temp_view(&self) {
        unsafe { SWSSProducerStateTable_create_temp_view(self.tbl) };
    }

    pub fn apply_temp_view(&self) {
        unsafe { SWSSProducerStateTable_apply_temp_view(self.tbl) };
    }
}

impl Drop for ProducerStateTable<'_> {
    fn drop(&mut self) {
        unsafe { SWSSProducerStateTable_free(self.tbl) };
    }
}

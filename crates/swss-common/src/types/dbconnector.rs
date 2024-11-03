use super::*;
use crate::*;
use std::{collections::HashMap, rc::Rc};

obj_wrapper! {
    struct DBConnectorObj { ptr: SWSSDBConnector } SWSSDBConnector_free
}

/// Rust wrapper around `swss::DBConnector`.
#[derive(Clone, Debug)]
pub struct DbConnector {
    pub(crate) obj: Rc<DBConnectorObj>,
}

impl DbConnector {
    pub fn new_tcp(db_id: i32, hostname: &str, port: u16, timeout_ms: u32) -> DbConnector {
        let hostname = cstr(hostname);
        let obj = unsafe { SWSSDBConnector_new_tcp(db_id, hostname.as_ptr(), port, timeout_ms).into() };
        Self { obj: Rc::new(obj) }
    }

    pub fn new_unix(db_id: i32, sock_path: &str, timeout_ms: u32) -> DbConnector {
        let sock_path = cstr(sock_path);
        let obj = unsafe { SWSSDBConnector_new_unix(db_id, sock_path.as_ptr(), timeout_ms).into() };
        Self { obj: Rc::new(obj) }
    }

    pub fn del(&self, key: &str) -> bool {
        let key = cstr(key);
        unsafe { SWSSDBConnector_del(self.obj.ptr, key.as_ptr()) == 1 }
    }

    pub fn set(&self, key: &str, val: &CxxString) {
        let key = cstr(key);
        unsafe { SWSSDBConnector_set(self.obj.ptr, key.as_ptr(), val.as_raw_ref()) };
    }

    pub fn get(&self, key: &str) -> Option<CxxString> {
        let key = cstr(key);
        unsafe {
            let mut ans = SWSSDBConnector_get(self.obj.ptr, key.as_ptr());
            CxxString::take_raw(&mut ans)
        }
    }

    pub fn exists(&self, key: &str) -> bool {
        let key = cstr(key);
        unsafe { SWSSDBConnector_exists(self.obj.ptr, key.as_ptr()) == 1 }
    }

    pub fn hdel(&self, key: &str, field: &str) -> bool {
        let key = cstr(key);
        let field = cstr(field);
        unsafe { SWSSDBConnector_hdel(self.obj.ptr, key.as_ptr(), field.as_ptr()) == 1 }
    }

    pub fn hset(&self, key: &str, field: &str, val: &CxxString) {
        let key = cstr(key);
        let field = cstr(field);
        unsafe { SWSSDBConnector_hset(self.obj.ptr, key.as_ptr(), field.as_ptr(), val.as_raw_ref()) };
    }

    pub fn hget(&self, key: &str, field: &str) -> Option<CxxString> {
        let key = cstr(key);
        let field = cstr(field);
        unsafe {
            let mut ans = SWSSDBConnector_hget(self.obj.ptr, key.as_ptr(), field.as_ptr());
            CxxString::take_raw(&mut ans)
        }
    }

    pub fn hgetall(&self, key: &str) -> HashMap<String, CxxString> {
        let key = cstr(key);
        unsafe {
            let ans = SWSSDBConnector_hgetall(self.obj.ptr, key.as_ptr());
            take_field_value_array(ans)
        }
    }

    pub fn hexists(&self, key: &str, field: &str) -> bool {
        let key = cstr(key);
        let field = cstr(field);
        unsafe { SWSSDBConnector_hexists(self.obj.ptr, key.as_ptr(), field.as_ptr()) == 1 }
    }

    pub fn flush_db(&self) -> bool {
        unsafe { SWSSDBConnector_flushdb(self.obj.ptr) == 1 }
    }
}

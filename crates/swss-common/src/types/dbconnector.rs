use super::*;
use crate::bindings::*;
use std::collections::HashMap;

/// Rust wrapper around `swss::DBConnector`.
#[derive(Debug)]
pub struct DbConnector {
    pub(crate) ptr: SWSSDBConnector,

    db_id: i32,
    connection: DbConnectionInfo,
}

/// Details about how a DbConnector is connected to Redis
#[derive(Debug, Clone)]
pub enum DbConnectionInfo {
    Tcp { hostname: String, port: u16 },
    Unix { sock_path: String },
}

impl DbConnector {
    /// Create a new DbConnector from [`DbConnectionInfo`].
    ///
    /// Timeout of 0 means block indefinitely.
    fn new(db_id: i32, connection: DbConnectionInfo, timeout_ms: u32) -> Result<DbConnector> {
        let ptr = match &connection {
            DbConnectionInfo::Tcp { hostname, port } => {
                let hostname = cstr(hostname);
                unsafe {
                    swss_try!(p_db => SWSSDBConnector_new_tcp(db_id, hostname.as_ptr(), *port, timeout_ms, p_db))?
                }
            }
            DbConnectionInfo::Unix { sock_path } => {
                let sock_path = cstr(sock_path);
                unsafe { swss_try!(p_db => SWSSDBConnector_new_unix(db_id, sock_path.as_ptr(), timeout_ms, p_db))? }
            }
        };

        Ok(Self { ptr, db_id, connection })
    }

    /// Create a DbConnector over a tcp socket.
    ///
    /// Timeout of 0 means block indefinitely.
    pub fn new_tcp(db_id: i32, hostname: impl Into<String>, port: u16, timeout_ms: u32) -> Result<DbConnector> {
        let hostname = hostname.into();
        Self::new(db_id, DbConnectionInfo::Tcp { hostname, port }, timeout_ms)
    }

    /// Create a DbConnector over a unix socket.
    ///
    /// Timeout of 0 means block indefinitely.
    pub fn new_unix(db_id: i32, sock_path: impl Into<String>, timeout_ms: u32) -> Result<DbConnector> {
        let sock_path = sock_path.into();
        Self::new(db_id, DbConnectionInfo::Unix { sock_path }, timeout_ms)
    }

    /// Clone a DbConnector with a timeout.
    ///
    /// Timeout of 0 means block indefinitely.
    pub fn clone_timeout(&self, timeout_ms: u32) -> Result<DbConnector> {
        Self::new(self.db_id, self.connection.clone(), timeout_ms)
    }

    pub fn db_id(&self) -> i32 {
        self.db_id
    }

    pub fn connection(&self) -> &DbConnectionInfo {
        &self.connection
    }

    pub fn del(&self, key: &str) -> Result<bool> {
        let key = cstr(key);
        let status = unsafe { swss_try!(p_status => SWSSDBConnector_del(self.ptr, key.as_ptr(), p_status))? };
        Ok(status == 1)
    }

    pub fn set(&self, key: &str, val: &CxxStr) -> Result<()> {
        let key = cstr(key);
        unsafe { swss_try!(SWSSDBConnector_set(self.ptr, key.as_ptr(), val.as_raw())) }
    }

    pub fn get(&self, key: &str) -> Result<Option<CxxString>> {
        let key = cstr(key);
        unsafe {
            let ans = swss_try!(p_ans => SWSSDBConnector_get(self.ptr, key.as_ptr(), p_ans))?;
            Ok(CxxString::take(ans))
        }
    }

    pub fn exists(&self, key: &str) -> Result<bool> {
        let key = cstr(key);
        let status = unsafe { swss_try!(p_status => SWSSDBConnector_exists(self.ptr, key.as_ptr(), p_status))? };
        Ok(status == 1)
    }

    pub fn hdel(&self, key: &str, field: &str) -> Result<bool> {
        let key = cstr(key);
        let field = cstr(field);
        let status =
            unsafe { swss_try!(p_status => SWSSDBConnector_hdel(self.ptr, key.as_ptr(), field.as_ptr(), p_status))? };
        Ok(status == 1)
    }

    pub fn hset(&self, key: &str, field: &str, val: &CxxStr) -> Result<()> {
        let key = cstr(key);
        let field = cstr(field);
        unsafe {
            swss_try!(SWSSDBConnector_hset(
                self.ptr,
                key.as_ptr(),
                field.as_ptr(),
                val.as_raw(),
            ))
        }
    }

    pub fn hget(&self, key: &str, field: &str) -> Result<Option<CxxString>> {
        let key = cstr(key);
        let field = cstr(field);
        unsafe {
            let ans = swss_try!(p_ans => SWSSDBConnector_hget(self.ptr, key.as_ptr(), field.as_ptr(), p_ans))?;
            Ok(CxxString::take(ans))
        }
    }

    pub fn hgetall(&self, key: &str) -> Result<HashMap<String, CxxString>> {
        let key = cstr(key);
        unsafe {
            let arr = swss_try!(p_arr => SWSSDBConnector_hgetall(self.ptr, key.as_ptr(), p_arr))?;
            Ok(take_field_value_array(arr))
        }
    }

    pub fn hexists(&self, key: &str, field: &str) -> Result<bool> {
        let key = cstr(key);
        let field = cstr(field);
        let status = unsafe {
            swss_try!(p_status => SWSSDBConnector_hexists(self.ptr, key.as_ptr(), field.as_ptr(), p_status))?
        };
        Ok(status == 1)
    }

    pub fn flush_db(&self) -> Result<bool> {
        let status = unsafe { swss_try!(p_status => SWSSDBConnector_flushdb(self.ptr, p_status))? };
        Ok(status == 1)
    }
}

impl Drop for DbConnector {
    fn drop(&mut self) {
        unsafe { swss_try!(SWSSDBConnector_free(self.ptr)).expect("Dropping DbConnector") };
    }
}

unsafe impl Send for DbConnector {}

#[cfg(feature = "async")]
impl DbConnector {
    async_util::impl_basic_async_method!(new_tcp_async <= new_tcp(db_id: i32, hostname: &str, port: u16, timeout_ms: u32) -> Result<DbConnector>);
    async_util::impl_basic_async_method!(new_unix_async <= new_unix(db_id: i32, sock_path: &str, timeout_ms: u32) -> Result<DbConnector>);
    async_util::impl_basic_async_method!(clone_timeout_async <= clone_timeout(&self, timeout_ms: u32) -> Result<DbConnector>);
    async_util::impl_basic_async_method!(del_async <= del(&self, key: &str) -> Result<bool>);
    async_util::impl_basic_async_method!(set_async <= set(&self, key: &str, value: &CxxStr) -> Result<()>);
    async_util::impl_basic_async_method!(get_async <= get(&self, key: &str) -> Result<Option<CxxString>>);
    async_util::impl_basic_async_method!(exists_async <= exists(&self, key: &str) -> Result<bool>);
    async_util::impl_basic_async_method!(hdel_async <= hdel(&self, key: &str, field: &str) -> Result<bool>);
    async_util::impl_basic_async_method!(hset_async <= hset(&self, key: &str, field: &str, value: &CxxStr) -> Result<()>);
    async_util::impl_basic_async_method!(hget_async <= hget(&self, key: &str, field: &str) -> Result<Option<CxxString>>);
    async_util::impl_basic_async_method!(hgetall_async <= hgetall(&self, key: &str) -> Result<HashMap<String, CxxString>>);
    async_util::impl_basic_async_method!(hexists_async <= hexists(&self, key: &str, field: &str) -> Result<bool>);
    async_util::impl_basic_async_method!(flush_db_async <= flush_db(&self) -> Result<bool>);
}

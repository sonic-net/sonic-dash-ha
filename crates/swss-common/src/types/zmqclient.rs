use super::*;
use crate::*;

/// Rust wrapper around `swss::ZmqClient`.
#[derive(Debug)]
pub struct ZmqClient {
    pub(crate) ptr: SWSSZmqClient,
}

impl ZmqClient {
    pub fn new(endpoint: &str) -> Self {
        let endpoint = cstr(endpoint);
        let ptr = unsafe { SWSSZmqClient_new(endpoint.as_ptr()).into() };
        Self { ptr }
    }

    pub fn is_connected(&self) -> bool {
        unsafe { SWSSZmqClient_isConnected(self.ptr) == 1 }
    }

    pub fn connect(&self) {
        unsafe { SWSSZmqClient_connect(self.ptr) }
    }

    pub fn send_msg<I>(&self, db_name: &str, table_name: &str, kfvs: I)
    where
        I: IntoIterator<Item = KeyOpFieldValues>,
    {
        let db_name = cstr(db_name);
        let table_name = cstr(table_name);
        let (kfvs, _k) = make_key_op_field_values_array(kfvs);
        unsafe { SWSSZmqClient_sendMsg(self.ptr, db_name.as_ptr(), table_name.as_ptr(), kfvs) };
    }
}

impl Drop for ZmqClient {
    fn drop(&mut self) {
        unsafe { SWSSZmqClient_free(self.ptr) };
    }
}

unsafe impl Send for ZmqClient {}

#[cfg(feature = "async")]
impl ZmqClient {
    async_util::impl_basic_async_method!(new_async <= new(endpoint: &str) -> Self);
    async_util::impl_basic_async_method!(connect_async <= connect(&self));
    async_util::impl_basic_async_method!(
        send_msg_async <= send_msg<I>(&self, db_name: &str, table_name: &str, kfvs: I)
                          where
                              I: IntoIterator<Item = KeyOpFieldValues> + Send,
    );
}

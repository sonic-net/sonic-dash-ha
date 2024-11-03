use super::*;
use crate::*;
use std::rc::Rc;

obj_wrapper! {
    struct ZmqClientObj { ptr: SWSSZmqClient } SWSSZmqClient_free
}

/// Rust wrapper around `swss::ZmqClient`.
#[derive(Clone, Debug)]
pub struct ZmqClient {
    pub(crate) obj: Rc<ZmqClientObj>,
}

impl ZmqClient {
    pub fn new(endpoint: &str) -> Self {
        let endpoint = cstr(endpoint);
        let obj = unsafe { Rc::new(SWSSZmqClient_new(endpoint.as_ptr()).into()) };
        Self { obj }
    }

    pub fn is_connected(&self) -> bool {
        unsafe { SWSSZmqClient_isConnected(self.obj.ptr) == 1 }
    }

    pub fn connect(&self) {
        unsafe { SWSSZmqClient_connect(self.obj.ptr) }
    }

    pub fn send_msg<'a, I>(&self, db_name: &str, table_name: &str, kfvs: I)
    where
        I: IntoIterator<Item = KeyOpFieldValues>,
    {
        let db_name = cstr(db_name);
        let table_name = cstr(table_name);
        let (kfvs, _k) = make_key_op_field_values_array(kfvs);
        unsafe { SWSSZmqClient_sendMsg(self.obj.ptr, db_name.as_ptr(), table_name.as_ptr(), kfvs) };
    }
}

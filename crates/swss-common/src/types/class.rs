use crate::*;
use std::{collections::HashMap, ptr::null, rc::Rc, time::Duration};

macro_rules! obj_wrapper {
    (struct $obj:ident { ptr: $ptr:ty } $freefn:expr) => {
        #[derive(Debug)]
        struct $obj {
            ptr: $ptr,
        }

        impl Drop for $obj {
            fn drop(&mut self) {
                unsafe {
                    $freefn(self.ptr);
                }
            }
        }

        impl From<$ptr> for $obj {
            fn from(ptr: $ptr) -> Self {
                Self { ptr }
            }
        }
    };
}

obj_wrapper! {
    struct DBConnectorObj { ptr: SWSSDBConnector } SWSSDBConnector_free
}

#[derive(Clone, Debug)]
pub struct DbConnector {
    obj: Rc<DBConnectorObj>,
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

obj_wrapper! {
    struct SubscriberStateTableObj { ptr: SWSSSubscriberStateTable } SWSSSubscriberStateTable_free
}

#[derive(Clone, Debug)]
pub struct SubscriberStateTable {
    obj: Rc<SubscriberStateTableObj>,
    _db: DbConnector,
}

impl SubscriberStateTable {
    pub fn new(db: DbConnector, table_name: &str, pop_batch_size: Option<i32>, pri: Option<i32>) -> Self {
        let table_name = cstr(table_name);
        let pop_batch_size = pop_batch_size.map(|n| &n as *const i32).unwrap_or(null());
        let pri = pri.map(|n| &n as *const i32).unwrap_or(null());
        let obj = unsafe {
            Rc::new(SWSSSubscriberStateTable_new(db.obj.ptr, table_name.as_ptr(), pop_batch_size, pri).into())
        };
        Self { obj, _db: db }
    }

    pub fn pops(&self) -> Vec<KeyOpFieldValues> {
        unsafe {
            let ans = SWSSSubscriberStateTable_pops(self.obj.ptr);
            take_key_op_field_values_array(ans)
        }
    }

    pub fn read_data(&self, timeout: Duration) -> SelectResult {
        let timeout_ms = timeout.as_millis().try_into().unwrap();
        let res = unsafe { SWSSSubscriberStateTable_readData(self.obj.ptr, timeout_ms) };
        SelectResult::from_raw(res)
    }

    pub fn has_data(&self) -> bool {
        unsafe { SWSSSubscriberStateTable_hasData(self.obj.ptr) == 1 }
    }

    pub fn has_cached_data(&self) -> bool {
        unsafe { SWSSSubscriberStateTable_hasCachedData(self.obj.ptr) == 1 }
    }

    pub fn initialized_with_data(&self) -> bool {
        unsafe { SWSSSubscriberStateTable_initializedWithData(self.obj.ptr) == 1 }
    }
}

obj_wrapper! {
    struct ConsumerStateTableObj { ptr: SWSSConsumerStateTable } SWSSConsumerStateTable_free
}

#[derive(Clone, Debug)]
pub struct ConsumerStateTable {
    obj: Rc<ConsumerStateTableObj>,
    _db: DbConnector,
}

impl ConsumerStateTable {
    pub fn new(db: DbConnector, table_name: &str, pop_batch_size: Option<i32>, pri: Option<i32>) -> Self {
        let table_name = cstr(table_name);
        let pop_batch_size = pop_batch_size.map(|n| &n as *const i32).unwrap_or(null());
        let pri = pri.map(|n| &n as *const i32).unwrap_or(null());
        let obj =
            unsafe { Rc::new(SWSSConsumerStateTable_new(db.obj.ptr, table_name.as_ptr(), pop_batch_size, pri).into()) };
        Self { obj, _db: db }
    }

    pub fn pops(&self) -> Vec<KeyOpFieldValues> {
        unsafe {
            let ans = SWSSConsumerStateTable_pops(self.obj.ptr);
            take_key_op_field_values_array(ans)
        }
    }
}

obj_wrapper! {
    struct ProducerStateTableObj { ptr: SWSSProducerStateTable } SWSSProducerStateTable_free
}

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

obj_wrapper! {
    struct ZmqServerObj { ptr: SWSSZmqServer } SWSSZmqServer_free
}

#[derive(Clone, Debug)]
pub struct ZmqServer {
    obj: Rc<ZmqServerObj>,

    // The types that register message handlers with a ZmqServer must be kept alive until
    // the server thread dies, otherwise we risk the server thread calling methods on deleted objects.
    // Currently this is just ZmqConsumerStateTable, but in the future there may be other types added
    // and this vec will need to hold an enum of the possible message handlers.
    handlers: Vec<ZmqConsumerStateTable>,
}

impl ZmqServer {
    pub fn new(endpoint: &str) -> Self {
        let endpoint = cstr(endpoint);
        let obj = unsafe { Rc::new(SWSSZmqServer_new(endpoint.as_ptr()).into()) };
        Self {
            obj,
            handlers: Vec::new(),
        }
    }

    fn register_consumer_state_table(&mut self, tbl: ZmqConsumerStateTable) {
        self.handlers.push(tbl);
    }
}

obj_wrapper! {
    struct ZmqClientObj { ptr: SWSSZmqClient } SWSSZmqClient_free
}

#[derive(Clone, Debug)]
pub struct ZmqClient {
    obj: Rc<ZmqClientObj>,
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

obj_wrapper! {
    struct ZmqConsumerStateTableObj { ptr: SWSSZmqConsumerStateTable } SWSSZmqConsumerStateTable_free
}

#[derive(Clone, Debug)]
pub struct ZmqConsumerStateTable {
    obj: Rc<ZmqConsumerStateTableObj>,
    _db: DbConnector,
    // ZmqConsumerStateTable does not own a copy of the ZmqServer because the ZmqServer must be
    // destroyed first (otherwise its worker thread might call a destroyed ZmqMessageHandler).
    // Instead, the ZmqServer owns a copy of all handlers registered to it, so they can be kept
    // alive until the ZmqServer is destroyed.
}

impl ZmqConsumerStateTable {
    pub fn new(
        db: DbConnector,
        table_name: &str,
        zmqs: &mut ZmqServer,
        pop_batch_size: Option<i32>,
        pri: Option<i32>,
    ) -> Self {
        let table_name = cstr(table_name);
        let pop_batch_size = pop_batch_size.map(|n| &n as *const i32).unwrap_or(null());
        let pri = pri.map(|n| &n as *const i32).unwrap_or(null());
        let obj = unsafe {
            let p = SWSSZmqConsumerStateTable_new(db.obj.ptr, table_name.as_ptr(), zmqs.obj.ptr, pop_batch_size, pri);
            Rc::new(p.into())
        };
        let self_ = Self { obj, _db: db };
        zmqs.register_consumer_state_table(self_.clone());
        self_
    }

    pub fn pops(&self) -> Vec<KeyOpFieldValues> {
        unsafe {
            let ans = SWSSZmqConsumerStateTable_pops(self.obj.ptr);
            take_key_op_field_values_array(ans)
        }
    }

    pub fn read_data(&self, timeout: Duration) -> SelectResult {
        let timeout_ms = timeout.as_millis().try_into().unwrap();
        let res = unsafe { SWSSZmqConsumerStateTable_readData(self.obj.ptr, timeout_ms) };
        SelectResult::from_raw(res)
    }

    pub fn has_data(&self) -> bool {
        unsafe { SWSSZmqConsumerStateTable_hasData(self.obj.ptr) == 1 }
    }

    pub fn has_cached_data(&self) -> bool {
        unsafe { SWSSZmqConsumerStateTable_hasCachedData(self.obj.ptr) == 1 }
    }

    pub fn initialized_with_data(&self) -> bool {
        unsafe { SWSSZmqConsumerStateTable_initializedWithData(self.obj.ptr) == 1 }
    }
}

obj_wrapper! {
    struct ZmqProducerStateTableObj { ptr: SWSSZmqProducerStateTable } SWSSZmqProducerStateTable_free
}

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

mod bindings {
    #![allow(unused, non_snake_case, non_upper_case_globals, non_camel_case_types)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use std::{
    any::Any,
    collections::HashMap,
    ffi::{CStr, CString},
    ptr::null,
    slice,
    sync::Arc,
};

use crate::bindings::*;

unsafe fn free<T>(ptr: *const T) {
    libc::free(ptr as *mut libc::c_void);
}

unsafe fn str(ptr: *const i8) -> String {
    let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
    free(ptr);
    s
}

unsafe fn take_field_value_array(arr: SWSSFieldValueArray) -> HashMap<String, String> {
    let mut out = HashMap::with_capacity(arr.len as usize);
    if !arr.data.is_null() {
        let entries = slice::from_raw_parts(arr.data, arr.len as usize);
        for fv in entries {
            let field = str(fv.field);
            let value = str(fv.value);
            out.insert(field, value);
        }
        free(arr.data);
    }
    out
}

fn make_field_value_array<I, S>(fvs: I) -> (SWSSFieldValueArray, Vec<Box<dyn Any>>)
where
    I: IntoIterator<Item = (S, S)>,
    S: AsRef<str>,
{
    let mut droppables: Vec<Box<dyn Any>> = Vec::new();
    let mut data = Vec::new();

    for (field, value) in fvs {
        let field = cstr(field.as_ref());
        let value = cstr(value.as_ref());
        data.push(SWSSFieldValuePair {
            field: field.as_ptr(),
            value: value.as_ptr(),
        });
        droppables.push(Box::new(field));
        droppables.push(Box::new(value));
    }

    let arr = SWSSFieldValueArray {
        data: data.as_ptr(),
        len: data.len().try_into().unwrap(),
    };
    droppables.push(Box::new(data));

    (arr, droppables)
}

unsafe fn take_key_op_field_values_array(arr: SWSSKeyOpFieldValuesArray) -> Vec<KeyOpFieldValues> {
    let mut out = Vec::with_capacity(arr.len as usize);
    if !arr.data.is_null() {
        unsafe {
            let kfvs = slice::from_raw_parts(arr.data, arr.len as usize);
            for kfv in kfvs {
                out.push(KeyOpFieldValues {
                    key: str(kfv.key),
                    operation: str(kfv.operation),
                    field_values: take_field_value_array(kfv.fieldValues),
                });
            }
            free(arr.data)
        };
    }
    out
}

fn make_key_op_field_values_array<'a, I>(kfvs: I) -> (SWSSKeyOpFieldValuesArray, Vec<Box<dyn Any>>)
where
    I: IntoIterator<Item = &'a KeyOpFieldValues>,
{
    let mut droppables: Vec<Box<dyn Any>> = Vec::new();
    let mut data = Vec::new();

    for kfv in kfvs {
        let key = cstr(&kfv.key);
        let operation = cstr(&kfv.operation);
        let (fv_arr, arr_droppables) = make_field_value_array(&kfv.field_values);
        data.push(SWSSKeyOpFieldValues {
            key: key.as_ptr(),
            operation: operation.as_ptr(),
            fieldValues: fv_arr,
        });
        droppables.push(Box::new(key));
        droppables.push(Box::new(operation));
        droppables.extend(arr_droppables);
    }

    let arr = SWSSKeyOpFieldValuesArray {
        data: data.as_ptr(),
        len: data.len().try_into().unwrap(),
    };
    droppables.push(Box::new(data));

    (arr, droppables)
}

fn cstr(s: &str) -> CString {
    CString::new(s).expect("str must not contain null bytes")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyOpFieldValues {
    pub key: String,
    pub operation: String,
    pub field_values: HashMap<String, String>,
}

macro_rules! obj_wrapper {
    ($(struct $obj:ident { ptr: $inner:ident } $freefn:ident)*) => {
        $(
        #[derive(Debug)]
        struct $obj { ptr: $inner }

        impl Drop for $obj {
            fn drop(&mut self) {
                unsafe {
                    $freefn(self.ptr);
                }
            }
        }

        impl From<$inner> for $obj {
            fn from(ptr: $inner) -> Self {
                Self { ptr }
            }
        }
        )*
    };
}

obj_wrapper! {
    struct DBConnectorObj { ptr: SWSSDBConnector } SWSSDBConnector_free
}

#[derive(Clone, Debug)]
pub struct DbConnector {
    obj: Arc<DBConnectorObj>,
}

impl DbConnector {
    pub fn new_tcp(db_id: i32, hostname: &str, port: u16, timeout: u32) -> DbConnector {
        let hostname = cstr(hostname);
        let obj = unsafe { SWSSDBConnector_new_tcp(db_id, hostname.as_ptr(), port, timeout).into() };
        Self { obj: Arc::new(obj) }
    }

    pub fn new_unix(db_id: i32, sock_path: &str, timeout: u32) -> DbConnector {
        let sock_path = cstr(sock_path);
        let obj = unsafe { SWSSDBConnector_new_unix(db_id, sock_path.as_ptr(), timeout).into() };
        Self { obj: Arc::new(obj) }
    }

    pub fn del(&self, key: &str) -> bool {
        let key = cstr(key);
        unsafe { SWSSDBConnector_del(self.obj.ptr, key.as_ptr()) == 1 }
    }

    pub fn set(&self, key: &str, val: &str) {
        let key = cstr(key);
        let val = cstr(val);
        unsafe { SWSSDBConnector_set(self.obj.ptr, key.as_ptr(), val.as_ptr()) };
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let key = cstr(key);
        unsafe {
            let ans = SWSSDBConnector_get(self.obj.ptr, key.as_ptr());
            if ans.is_null() {
                None
            } else {
                Some(str(ans))
            }
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

    pub fn hset(&self, key: &str, field: &str, val: &str) {
        let key = cstr(key);
        let field = cstr(field);
        let val = cstr(val);
        unsafe { SWSSDBConnector_hset(self.obj.ptr, key.as_ptr(), field.as_ptr(), val.as_ptr()) };
    }

    pub fn hget(&self, key: &str, field: &str) -> Option<String> {
        let key = cstr(key);
        let field = cstr(field);
        unsafe {
            let ans = SWSSDBConnector_hget(self.obj.ptr, key.as_ptr(), field.as_ptr());
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
    obj: Arc<SubscriberStateTableObj>,
    _db: DbConnector,
}

impl SubscriberStateTable {
    pub fn new(db: DbConnector, table_name: &str, pop_batch_size: Option<i32>, pri: Option<i32>) -> Self {
        let table_name = cstr(table_name);
        let pop_batch_size = pop_batch_size.map(|n| &n as *const i32).unwrap_or(null());
        let pri = pri.map(|n| &n as *const i32).unwrap_or(null());
        let obj = unsafe {
            Arc::new(SWSSSubscriberStateTable_new(db.obj.ptr, table_name.as_ptr(), pop_batch_size, pri).into())
        };
        Self { obj, _db: db }
    }

    pub fn pops(&self) -> Vec<KeyOpFieldValues> {
        unsafe {
            let ans = SWSSSubscriberStateTable_pops(self.obj.ptr);
            take_key_op_field_values_array(ans)
        }
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

    pub fn read_data(&self) {
        unsafe { SWSSSubscriberStateTable_readData(self.obj.ptr) };
    }
}

obj_wrapper! {
    struct ConsumerStateTableObj { ptr: SWSSConsumerStateTable } SWSSConsumerStateTable_free
}

#[derive(Clone, Debug)]
pub struct ConsumerStateTable {
    obj: Arc<ConsumerStateTableObj>,
    _db: DbConnector,
}

impl ConsumerStateTable {
    pub fn new(db: DbConnector, table_name: &str, pop_batch_size: Option<i32>, pri: Option<i32>) -> Self {
        let table_name = cstr(table_name);
        let pop_batch_size = pop_batch_size.map(|n| &n as *const i32).unwrap_or(null());
        let pri = pri.map(|n| &n as *const i32).unwrap_or(null());
        let obj = unsafe {
            Arc::new(SWSSConsumerStateTable_new(db.obj.ptr, table_name.as_ptr(), pop_batch_size, pri).into())
        };
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
    obj: Arc<ProducerStateTableObj>,
    _db: DbConnector,
}

impl ProducerStateTable {
    pub fn new(db: DbConnector, table_name: &str) -> Self {
        let table_name = cstr(table_name);
        let obj = Arc::new(unsafe { SWSSProducerStateTable_new(db.obj.ptr, table_name.as_ptr()).into() });
        Self { obj, _db: db }
    }

    pub fn set_buffered(&self, buffered: bool) {
        unsafe { SWSSProducerStateTable_setBuffered(self.obj.ptr, buffered as u8) };
    }

    pub fn set<I, S>(&self, key: &str, fv_iter: I)
    where
        I: IntoIterator<Item = (S, S)>,
        S: AsRef<str>,
    {
        let key = cstr(key);
        let (arr, _droppables) = make_field_value_array(fv_iter);
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

// libswsscommon handles zmq messages in another thread, so Send + Sync are required
// 'static is a simplification and could probably be reduced to the lifetime of ZmqServer, but it probably won't matter
pub trait ZmqMessageHandlerFn: FnMut(&[KeyOpFieldValues]) + Send + Sync + 'static {}
impl<T: FnMut(&[KeyOpFieldValues]) + Send + Sync + 'static> ZmqMessageHandlerFn for T {}

#[derive(Clone, Debug)]
struct ClosureZmqMessageHandler {
    callback: *mut Box<dyn ZmqMessageHandlerFn>,
    handler: SWSSZmqMessageHandler,
}

impl ClosureZmqMessageHandler {
    fn new<F>(callback: F) -> Self
    where
        F: FnMut(&[KeyOpFieldValues]) + Send + Sync + 'static,
    {
        unsafe extern "C" fn real_handler(callback_ptr: *mut libc::c_void, arr: *const SWSSKeyOpFieldValuesArray) {
            let res = std::panic::catch_unwind(|| {
                let kfvs = take_key_op_field_values_array(*arr);
                let callback = (callback_ptr as *mut Box<dyn ZmqMessageHandlerFn>).as_mut().unwrap();
                callback(&kfvs);
            });

            if res.is_err() {
                eprintln!("Aborting to avoid unwinding a Rust panic into C++ code");
                eprintln!("Backtrace:\n{}", std::backtrace::Backtrace::force_capture());
                std::process::abort();
            }
        }

        let callback: *mut Box<dyn ZmqMessageHandlerFn> = Box::into_raw(Box::new(Box::new(callback)));
        let handler = unsafe { SWSSZmqMessageHandler_new(callback as _, Some(real_handler)) };

        Self { callback, handler }
    }
}

impl Drop for ClosureZmqMessageHandler {
    fn drop(&mut self) {
        unsafe {
            SWSSZmqMessageHandler_free(self.handler);
            drop(Box::from_raw(self.callback));
        }
    }
}

// The types that register message handlers with a ZmqServer and are owned on the rust side
#[derive(Clone, Debug)]
enum ZmqMessageHandler {
    Closure { _h: ClosureZmqMessageHandler },
    ConsumerStateTable { _t: ZmqConsumerStateTable },
}

obj_wrapper! {
    struct ZmqServerObj { ptr: SWSSZmqServer } SWSSZmqServer_free
}

#[derive(Clone, Debug)]
pub struct ZmqServer {
    obj: Arc<ZmqServerObj>,

    // The types that register message handlers with a ZmqServer must be kept alive until
    // the server thread dies, otherwise we risk the server thread calling methods on deleted objects
    handlers: Vec<ZmqMessageHandler>,
}

impl ZmqServer {
    pub fn new(endpoint: &str) -> Self {
        let endpoint = cstr(endpoint);
        let obj = unsafe { Arc::new(SWSSZmqServer_new(endpoint.as_ptr()).into()) };
        Self {
            obj,
            handlers: Vec::new(),
        }
    }

    pub fn register_message_handler<F>(&mut self, db_name: &str, table_name: &str, handler: F)
    where
        F: ZmqMessageHandlerFn,
    {
        let db_name = cstr(db_name);
        let table_name = cstr(table_name);
        let handler = ClosureZmqMessageHandler::new(handler);
        unsafe {
            SWSSZmqServer_registerMessageHandler(self.obj.ptr, db_name.as_ptr(), table_name.as_ptr(), handler.handler);
        }
        self.handlers.push(ZmqMessageHandler::Closure { _h: handler });
    }

    fn register_consumer_state_table(&mut self, tbl: ZmqConsumerStateTable) {
        self.handlers.push(ZmqMessageHandler::ConsumerStateTable { _t: tbl })
    }
}

obj_wrapper! {
    struct ZmqClientObj { ptr: SWSSZmqClient } SWSSZmqClient_free
}

#[derive(Clone, Debug)]
pub struct ZmqClient {
    obj: Arc<ZmqClientObj>,
}

impl ZmqClient {
    pub fn new(endpoint: &str) -> Self {
        let endpoint = cstr(endpoint);
        let obj = unsafe { Arc::new(SWSSZmqClient_new(endpoint.as_ptr()).into()) };
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
        I: IntoIterator<Item = &'a KeyOpFieldValues>,
    {
        let db_name = cstr(db_name);
        let table_name = cstr(table_name);
        let (kfvs, _droppables) = make_key_op_field_values_array(kfvs);
        unsafe { SWSSZmqClient_sendMsg(self.obj.ptr, db_name.as_ptr(), table_name.as_ptr(), &kfvs as *const _) };
    }
}

obj_wrapper! {
    struct ZmqConsumerStateTableObj { ptr: SWSSZmqConsumerStateTable } SWSSZmqConsumerStateTable_free
}

#[derive(Clone, Debug)]
pub struct ZmqConsumerStateTable {
    obj: Arc<ZmqConsumerStateTableObj>,
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
            Arc::new(p.into())
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

    pub fn get_fd(&self) -> i32 {
        unsafe { SWSSZmqConsumerStateTable_getFd(self.obj.ptr) }
    }

    pub fn read_data(&self) -> u64 {
        unsafe { SWSSZmqConsumerStateTable_readData(self.obj.ptr) }
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

    pub fn db_updater_queue_size(&self) -> u64 {
        unsafe { SWSSZmqConsumerStateTable_dbUpdaterQueueSize(self.obj.ptr) }
    }
}

obj_wrapper! {
    struct ZmqProducerStateTableObj { ptr: SWSSZmqProducerStateTable } SWSSZmqProducerStateTable_free
}

#[derive(Clone, Debug)]
pub struct ZmqProducerStateTable {
    obj: Arc<ZmqProducerStateTableObj>,
    _db: DbConnector,
    _zmqc: ZmqClient,
}

impl ZmqProducerStateTable {
    pub fn new(db: DbConnector, table_name: &str, zmqc: ZmqClient, db_persistence: bool) -> Self {
        let table_name = cstr(table_name);
        let obj = unsafe {
            Arc::new(
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

    pub fn set<I, S>(&self, key: &str, fv_iter: I)
    where
        I: IntoIterator<Item = (S, S)>,
        S: AsRef<str>,
    {
        let key = cstr(key);
        let (arr, _droppables) = make_field_value_array(fv_iter);
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

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        fs::File,
        io::{BufRead, BufReader, Read},
        process::{Child, Command, Stdio},
        sync::{Arc, Mutex},
        thread::sleep,
        time::{Duration, Instant},
    };

    use super::*;

    #[test]
    fn dbconnector() {
        let _redis = Redis::start("/tmp/dbconnector_test.sock");
        let db = DbConnector::new_unix(0, "/tmp/dbconnector_test.sock", 0);

        assert!(db.flush_db());

        let random = {
            let mut buf = [0u8; 8];
            File::open("/dev/urandom")
                .unwrap()
                .take(8)
                .read_exact(&mut buf)
                .unwrap();
            format!("{:0X}", u64::from_be_bytes(buf))
        };

        db.set("hello", "hello, world!");
        db.set("random", &random);
        assert_eq!(db.get("hello"), Some("hello, world!".to_owned()));
        assert_eq!(db.get("random"), Some(random.clone()));
        assert_eq!(db.get("noexist"), None);

        assert!(db.exists("hello"));
        assert!(!db.exists("noexist"));
        assert!(db.del("hello"));
        assert!(!db.del("hello"));
        assert!(db.del("random"));
        assert!(!db.del("random"));
        assert!(!db.del("noexist"));

        db.hset("a", "hello", "hello, world!");
        db.hset("a", "random", &random);
        assert_eq!(db.hget("a", "hello"), Some("hello, world!".to_owned()));
        assert_eq!(db.hget("a", "random"), Some(random));
        assert_eq!(db.hget("a", "noexist"), None);
        assert_eq!(db.hget("noexist", "noexist"), None);
        assert!(db.hexists("a", "hello"));
        assert!(!db.hexists("a", "noexist"));
        assert!(!db.hexists("noexist", "hello"));
        assert!(db.hdel("a", "hello"));
        assert!(!db.hdel("a", "hello"));
        assert!(db.hdel("a", "random"));
        assert!(!db.hdel("a", "random"));
        assert!(!db.hdel("a", "noexist"));
        assert!(!db.hdel("noexist", "noexist"));
        assert!(!db.del("a"));

        assert!(db.hgetall("a").is_empty());
        db.hset("a", "a", "1");
        db.hset("a", "b", "2");
        db.hset("a", "c", "3");
        assert_eq!(
            db.hgetall("a"),
            HashMap::from_iter([
                ("a".to_owned(), "1".to_owned()),
                ("b".to_owned(), "2".to_owned()),
                ("c".to_owned(), "3".to_owned())
            ])
        );

        assert!(db.flush_db());
    }

    #[test]
    fn zmq() {
        let ep = "ipc:///tmp/zmq_test.sock";
        let mut s = ZmqServer::new(ep);

        let kvfs_seen: Arc<Mutex<Option<KeyOpFieldValues>>> = Arc::new(Mutex::new(None));
        let kvfs_seen_ = kvfs_seen.clone();

        s.register_message_handler("a", "b", move |kvfs| {
            *kvfs_seen_.lock().unwrap() = Some(kvfs[0].clone());
        });

        let kvfs = KeyOpFieldValues {
            key: "akey".into(),
            operation: "SET".into(),
            field_values: HashMap::from_iter([("afield".into(), "avalue".into())]),
        };
        let c = ZmqClient::new(ep);
        c.send_msg("a", "b", [&kvfs]);

        let start_time = Instant::now();
        loop {
            let kvfs_seen_lock = kvfs_seen.lock().unwrap();
            match &*kvfs_seen_lock {
                Some(kvfs_seen) => {
                    assert_eq!(&kvfs, kvfs_seen);
                    break;
                }
                None => {
                    drop(kvfs_seen_lock);
                    sleep(Duration::from_secs_f32(0.1));
                    assert!(
                        Instant::now().duration_since(start_time) < Duration::from_secs(5),
                        "timeout waiting for kvfs_seen"
                    )
                }
            }
        }
    }

    struct Redis(Child);

    impl Redis {
        fn start(sock_path: impl AsRef<str>) -> Self {
            #[rustfmt::skip]
            let mut child = Command::new("redis-server")
                .args([
                    "--appendonly", "no",
                    "--save", "",
                    "--notify-keyspace-events", "AKE",
                    "--port", "0",
                    "--unixsocket", sock_path.as_ref(),
                ])
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let mut stdout = BufReader::new(child.stdout.take().unwrap());
            let mut buf = String::new();
            loop {
                buf.clear();
                if stdout.read_line(&mut buf).unwrap() == 0 {
                    panic!("Redis didn't start");
                }
                if buf.contains("ready to accept connections") {
                    break Self(child);
                }
            }
        }
    }

    impl Drop for Redis {
        fn drop(&mut self) {
            Command::new("kill")
                .args(["-s", "TERM", &self.0.id().to_string()])
                .status()
                .unwrap();
            self.0.wait().unwrap();
        }
    }
}

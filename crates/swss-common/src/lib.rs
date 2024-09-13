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

fn make_key_op_field_values_array(kfvs: &[KeyOpFieldValues]) -> (SWSSKeyOpFieldValuesArray, Vec<Box<dyn Any>>) {
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

#[derive(Debug, Clone)]
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
            take_field_value_array(ans)
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
            take_key_op_field_values_array(ans)
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
            take_key_op_field_values_array(ans)
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
        S: AsRef<str>,
    {
        let key = cstr(key);
        let (arr, _droppables) = make_field_value_array(fv_iter);
        unsafe { SWSSProducerStateTable_set(self.tbl, key.as_ptr(), arr) };
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

struct ZmqMessageHandler {
    callback: *mut Box<dyn FnMut(&[KeyOpFieldValues]) + 'static>,
    handler: SWSSZmqMessageHandler,
}

impl ZmqMessageHandler {
    fn new<F: FnMut(&[KeyOpFieldValues]) + 'static>(callback: F) -> Self {
        unsafe extern "C" fn real_handler(callback_ptr: *mut libc::c_void, arr: *const SWSSKeyOpFieldValuesArray) {
            let kfvs = take_key_op_field_values_array(*arr);
            let callback = (callback_ptr as *mut Box<dyn FnMut(&[KeyOpFieldValues])>)
                .as_mut()
                .unwrap();

            callback(&kfvs);
        }

        let callback: *mut Box<dyn FnMut(&[KeyOpFieldValues]) + 'static> = Box::into_raw(Box::new(Box::new(callback)));
        let handler = unsafe { SWSSZmqMessageHandler_new(callback as _, Some(real_handler)) };

        Self { callback, handler }
    }
}

impl Drop for ZmqMessageHandler {
    fn drop(&mut self) {
        unsafe {
            SWSSZmqMessageHandler_free(self.handler);
            drop(Box::from_raw(self.callback));
        }
    }
}

pub struct ZmqServer {
    zmqs: SWSSZmqServer,
    handlers: Vec<ZmqMessageHandler>,
}

impl ZmqServer {
    pub fn new(endpoint: &str) -> Self {
        let endpoint = cstr(endpoint);
        let zmqs = unsafe { SWSSZmqServer_new(endpoint.as_ptr()) };
        Self {
            zmqs,
            handlers: Vec::new(),
        }
    }

    pub fn register_message_handler<F>(&mut self, db_name: &str, table_name: &str, handler: F)
    where
        F: FnMut(&[KeyOpFieldValues]) + 'static,
    {
        let db_name = cstr(db_name);
        let table_name = cstr(table_name);
        let handler = ZmqMessageHandler::new(handler);
        unsafe {
            SWSSZmqServer_registerMessageHandler(self.zmqs, db_name.as_ptr(), table_name.as_ptr(), handler.handler);
        }
        self.handlers.push(handler);
    }
}

impl Drop for ZmqServer {
    fn drop(&mut self) {
        unsafe { SWSSZmqServer_free(self.zmqs) };
    }
}

pub struct ZmqClient {
    zmqc: SWSSZmqClient,
}

impl ZmqClient {
    pub fn new(endpoint: &str) -> Self {
        let endpoint = cstr(endpoint);
        let zmqc = unsafe { SWSSZmqClient_new(endpoint.as_ptr()) };
        Self { zmqc }
    }

    pub fn is_connected(&self) -> bool {
        unsafe { SWSSZmqClient_isConnected(self.zmqc) == 1 }
    }

    pub fn connect(&self) {
        unsafe { SWSSZmqClient_connect(self.zmqc) }
    }

    pub fn send_msg(&self, db_name: &str, table_name: &str, kfvs: &[KeyOpFieldValues]) {
        let db_name = cstr(db_name);
        let table_name = cstr(table_name);
        let (kfvs, _droppables) = make_key_op_field_values_array(kfvs);
        unsafe { SWSSZmqClient_sendMsg(self.zmqc, db_name.as_ptr(), table_name.as_ptr(), &kfvs as *const _) };
    }
}

impl Drop for ZmqClient {
    fn drop(&mut self) {
        unsafe { SWSSZmqClient_free(self.zmqc) };
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        fs::File,
        io::{BufRead, BufReader, Read},
        process::{Child, Command, Stdio},
    };

    use super::*;

    #[test]
    fn dbconnector() {
        let _redis = Redis::start("/tmp/dbconnector_test.sock");
        let db = DBConnector::new_unix(0, "/tmp/dbconnector_test.sock", 0);

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
        s.register_message_handler("a", "b", |kvfs| {
            println!("{kvfs:?}");
        });

        let c = ZmqClient::new(ep);
        c.send_msg(
            "a",
            "b",
            &[KeyOpFieldValues {
                key: "akey".into(),
                operation: "HSET".into(),
                field_values: HashMap::from_iter([("afield".into(), "avalue".into())]),
            }],
        );
    }

    struct Redis(Child);

    impl Redis {
        fn start(sock_path: impl AsRef<str>) -> Self {
            let mut child = Command::new("redis-server")
                .args([
                    "--appendonly",
                    "no",
                    "--save",
                    "",
                    "--notify-keyspace-events",
                    "AKE",
                    "--port",
                    "0",
                    "--unixsocket",
                    sock_path.as_ref(),
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

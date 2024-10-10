mod bindings {
    #![allow(unused, non_snake_case, non_upper_case_globals, non_camel_case_types)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

use std::{
    any::Any,
    collections::HashMap,
    error::Error,
    ffi::{CStr, CString},
    fmt::Display,
    ptr::null,
    slice,
    str::FromStr,
    sync::Arc,
    time::Duration,
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
                    operation: KeyOperation::from_str(&str(kfv.operation)).unwrap(),
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
        let (fv_arr, arr_droppables) = make_field_value_array(&kfv.field_values);
        data.push(SWSSKeyOpFieldValues {
            key: key.as_ptr(),
            operation: kfv.operation.as_c_str(),
            fieldValues: fv_arr,
        });
        droppables.push(Box::new(key));
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

pub fn sonic_db_config_initialize(path: &str) {
    let path = cstr(path);
    unsafe { SWSSSonicDBConfig_initialize(path.as_ptr()) }
}

pub fn sonic_db_config_initialize_global(path: &str) {
    let path = cstr(path);
    unsafe { SWSSSonicDBConfig_initializeGlobalConfig(path.as_ptr()) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SelectResult {
    /// Data is now available.
    Data,
    /// Waiting was interrupted by a signal.
    Signal,
    /// Timed out.
    Timeout,
}

impl SelectResult {
    fn from_raw(raw: SWSSSelectResult) -> Self {
        if raw == SWSSSelectResult_SWSSSelectResult_DATA {
            SelectResult::Data
        } else if raw == SWSSSelectResult_SWSSSelectResult_SIGNAL {
            SelectResult::Signal
        } else if raw == SWSSSelectResult_SWSSSelectResult_TIMEOUT {
            SelectResult::Timeout
        } else {
            panic!("unhandled SWSSSelectResult: {raw}")
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum KeyOperation {
    Set,
    Del,
}

impl KeyOperation {
    fn as_c_str(self) -> *const i8 {
        static SET: &'static CStr = c"SET";
        static DEL: &'static CStr = c"DEL";
        match self {
            KeyOperation::Set => SET.as_ptr(),
            KeyOperation::Del => DEL.as_ptr(),
        }
    }
}

impl FromStr for KeyOperation {
    type Err = InvalidKeyOperationString;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "SET" => Ok(Self::Set),
            "DEL" => Ok(Self::Del),
            _ => Err(InvalidKeyOperationString(s.to_string())),
        }
    }
}

#[derive(Debug)]
pub struct InvalidKeyOperationString(String);

impl Display for InvalidKeyOperationString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, r#"A KeyOperation String must be "SET" or "DEL", but was {}"#, self.0)
    }
}

impl Error for InvalidKeyOperationString {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyOpFieldValues {
    pub key: String,
    pub operation: KeyOperation,
    pub field_values: HashMap<String, String>,
}

/// Intended for testing, ordered by key
impl PartialOrd for KeyOpFieldValues {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Intended for testing, ordered by key
impl Ord for KeyOpFieldValues {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

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
    obj: Arc<DBConnectorObj>,
}

impl DbConnector {
    pub fn new_tcp(db_id: i32, hostname: &str, port: u16, timeout_ms: u32) -> DbConnector {
        let hostname = cstr(hostname);
        let obj = unsafe { SWSSDBConnector_new_tcp(db_id, hostname.as_ptr(), port, timeout_ms).into() };
        Self { obj: Arc::new(obj) }
    }

    pub fn new_unix(db_id: i32, sock_path: &str, timeout_ms: u32) -> DbConnector {
        let sock_path = cstr(sock_path);
        let obj = unsafe { SWSSDBConnector_new_unix(db_id, sock_path.as_ptr(), timeout_ms).into() };
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

    /// Panics if `timeout.as_millis()` would overflow a `u32`
    pub fn read_data(&self, timeout: Duration) -> SelectResult {
        let timeout_ms = timeout.as_millis().try_into().unwrap();
        let res = unsafe { SWSSSubscriberStateTable_readData(self.obj.ptr, timeout_ms) };
        SelectResult::from_raw(res)
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

    pub fn set<I, S>(&self, key: &str, fvs: I)
    where
        I: IntoIterator<Item = (S, S)>,
        S: AsRef<str>,
    {
        let key = cstr(key);
        let (arr, _droppables) = make_field_value_array(fvs);
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
    obj: Arc<ZmqServerObj>,

    // The types that register message handlers with a ZmqServer must be kept alive until
    // the server thread dies, otherwise we risk the server thread calling methods on deleted objects.
    // Currently this is just ZmqConsumerStateTable, but in the future there may be other types added
    // and this vec will need to hold an enum of the possible message handlers.
    handlers: Vec<ZmqConsumerStateTable>,
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

    fn register_consumer_state_table(&mut self, tbl: ZmqConsumerStateTable) {
        self.handlers.push(tbl);
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

    pub fn set<I, S>(&self, key: &str, fvs: I)
    where
        I: IntoIterator<Item = (S, S)>,
        S: AsRef<str>,
    {
        let key = cstr(key);
        let (arr, _droppables) = make_field_value_array(fvs);
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
        fs::{self, remove_file},
        io::{BufRead, BufReader},
        iter,
        process::{Child, Command, Stdio},
        sync::Mutex,
        thread::{self},
        time::Duration,
    };

    use rand::{random, Rng};

    use super::*;

    struct Redis {
        proc: Child,
        sock: String,
    }

    impl Redis {
        fn start() -> Self {
            let sock = random_unix_sock();
            #[rustfmt::skip]
            let mut child = Command::new("redis-server")
                .args([
                    "--appendonly", "no",
                    "--save", "",
                    "--notify-keyspace-events", "AKE",
                    "--port", "0",
                    "--unixsocket", &sock,
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
                    break Self { proc: child, sock };
                }
            }
        }
    }

    impl Drop for Redis {
        fn drop(&mut self) {
            Command::new("kill")
                .args(["-s", "TERM", &self.proc.id().to_string()])
                .status()
                .unwrap();
            self.proc.wait().unwrap();
        }
    }

    struct Defer<F: FnOnce()>(Option<F>);

    impl<F: FnOnce()> Defer<F> {
        fn new(f: F) -> Self {
            Self(Some(f))
        }
    }

    impl<F: FnOnce()> Drop for Defer<F> {
        fn drop(&mut self) {
            self.0.take().unwrap()()
        }
    }

    const DB_CONFIG_JSON: &str = r#"
        {
            "DATABASES": {
                "db name doesn't matter": {
                    "id": 0,
                    "separator": ":",
                    "instance": "redis"
                }
            }
        }
    "#;

    const DB_GLOBAL_CONFIG_JSON: &str = "{}";

    fn sonic_db_config_init_for_test() {
        // HACK
        // We need to do our own locking here because locking is not correctly implemented in
        // swss::SonicDBConfig :/
        static INITIALIZED: Mutex<bool> = Mutex::new(false);
        let mut is_init = INITIALIZED.lock().unwrap();
        if !*is_init {
            fs::write("/tmp/db_config_test.json", DB_CONFIG_JSON).unwrap();
            fs::write("/tmp/db_global_config_test.json", DB_GLOBAL_CONFIG_JSON).unwrap();
            sonic_db_config_initialize("/tmp/db_config_test.json");
            sonic_db_config_initialize_global("/tmp/db_global_config_test.json");
            fs::remove_file("/tmp/db_config_test.json").unwrap();
            fs::remove_file("/tmp/db_global_config_test.json").unwrap();
            *is_init = true;
        }
    }

    fn random_string() -> String {
        format!("{:0X}", random::<u64>())
    }

    fn random_kfv() -> KeyOpFieldValues {
        let key = random_string();
        let operation = if random() { KeyOperation::Set } else { KeyOperation::Del };
        let mut field_values = HashMap::new();

        if operation == KeyOperation::Set {
            // We need at least one field-value pair, otherwise swss::BinarySerializer infers that
            // the operation is DEL even if the .operation field is SET
            for _ in 0..rand::thread_rng().gen_range(100..1000) {
                field_values.insert(random_string(), random_string());
            }
        }

        KeyOpFieldValues {
            key,
            operation,
            field_values,
        }
    }

    fn random_kfvs() -> Vec<KeyOpFieldValues> {
        iter::repeat_with(random_kfv).take(100).collect()
    }

    fn random_unix_sock() -> String {
        format!("/tmp/{}.sock", random_string())
    }

    // zmq doesn't clean up its own ipc sockets, so we include a deferred operation for that
    fn random_zmq_endpoint() -> (String, impl Drop) {
        let sock = random_unix_sock();
        let endpoint = format!("ipc://{sock}");
        (endpoint, Defer::new(|| remove_file(sock).unwrap()))
    }

    // swss::ZmqServer spawns a thread which polls for messages every second. When we want to test
    // the receipt of a message, we need to wait one second plus a little extra wiggle room.
    fn sleep_zmq_poll() {
        thread::sleep(Duration::from_millis(1100));
    }

    #[test]
    fn dbconnector() {
        let redis = Redis::start();
        let db = DbConnector::new_unix(0, &redis.sock, 0);

        assert!(db.flush_db());

        let random = random_string();

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
    fn consumer_producer_state_tables() {
        sonic_db_config_init_for_test();
        let redis = Redis::start();
        let db = DbConnector::new_unix(0, &redis.sock, 0);

        let pst = ProducerStateTable::new(db.clone(), "table_a");
        let cst = ConsumerStateTable::new(db.clone(), "table_a", None, None);

        assert!(cst.pops().is_empty());

        let mut kfvs = random_kfvs();
        for (i, kfv) in kfvs.iter().enumerate() {
            assert_eq!(pst.count(), i as i64);
            match kfv.operation {
                KeyOperation::Set => pst.set(&kfv.key, &kfv.field_values),
                KeyOperation::Del => pst.del(&kfv.key),
            }
        }

        let mut kfvs_cst = cst.pops();
        assert!(cst.pops().is_empty());

        kfvs.sort_unstable();
        kfvs_cst.sort_unstable();
        assert_eq!(kfvs_cst.len(), kfvs.len());
        assert_eq!(kfvs_cst, kfvs);
    }

    #[test]
    fn subscriber_state_table() {
        sonic_db_config_init_for_test();
        let redis = Redis::start();
        let db = DbConnector::new_unix(0, &redis.sock, 0);

        let sst = SubscriberStateTable::new(db.clone(), "table_a", None, None);
        assert!(!sst.has_data());
        assert!(sst.pops().is_empty());

        db.hset("table_a:key_a", "field_a", "value_a");
        db.hset("table_a:key_a", "field_b", "value_b");
        assert_eq!(sst.read_data(Duration::from_millis(1000)), SelectResult::Data);
        assert!(sst.has_data());
        let mut kfvs = sst.pops();

        // SubscriberStateTable will pick up duplicate KeyOpFieldValues' after two SETs on the same
        // key. I'm not actually sure if this is intended.
        assert_eq!(kfvs.len(), 2);
        assert_eq!(kfvs[0], kfvs[1]);

        assert!(!sst.has_data());
        assert!(sst.pops().is_empty());

        let KeyOpFieldValues {
            key,
            operation,
            field_values,
        } = kfvs.pop().unwrap();

        assert_eq!(key, "key_a");
        assert_eq!(operation, KeyOperation::Set);
        assert_eq!(
            field_values,
            HashMap::from_iter([
                ("field_a".into(), "value_a".into()),
                ("field_b".into(), "value_b".into())
            ])
        );
    }

    #[test]
    fn zmq_consumer_state_table() {
        use SelectResult::*;

        let (endpoint, _delete) = random_zmq_endpoint();
        let mut zmqs = ZmqServer::new(&endpoint);
        let zmqc = ZmqClient::new(&endpoint);
        assert!(zmqc.is_connected());

        let redis = Redis::start();
        let db = DbConnector::new_unix(0, &redis.sock, 0);

        let kfvs = random_kfvs();
        let zcst_table_a = ZmqConsumerStateTable::new(db.clone(), "table_a", &mut zmqs, None, None);
        let zcst_table_b = ZmqConsumerStateTable::new(db.clone(), "table_b", &mut zmqs, None, None);
        assert!(!zcst_table_a.has_data());
        assert!(!zcst_table_b.has_data());

        zmqc.send_msg("", "table_a", &kfvs); // db name is empty because we are using DbConnector::new_unix
        assert_eq!(zcst_table_a.read_data(Duration::from_millis(1500)), Data);
        assert_eq!(zcst_table_b.read_data(Duration::from_millis(1500)), Timeout);
        assert!(zcst_table_a.has_data());
        assert!(!zcst_table_b.has_data());

        zmqc.send_msg("", "table_b", &kfvs);
        assert_eq!(zcst_table_a.read_data(Duration::from_millis(1500)), Timeout);
        assert_eq!(zcst_table_b.read_data(Duration::from_millis(1500)), Data);
        assert!(zcst_table_a.has_data());
        assert!(zcst_table_b.has_data());

        let kfvs_a = zcst_table_a.pops();
        let kvfs_b = zcst_table_b.pops();
        assert_eq!(kfvs_a, kvfs_b);
        assert_eq!(kfvs, kfvs_a);
        assert!(!zcst_table_a.has_data());
        assert!(!zcst_table_b.has_data());
    }

    #[test]
    fn zmq_consumer_producer_state_tables() {
        let (endpoint, _delete) = random_zmq_endpoint();
        let mut zmqs = ZmqServer::new(&endpoint);
        let zmqc = ZmqClient::new(&endpoint);

        let redis = Redis::start();
        let db = DbConnector::new_unix(0, &redis.sock, 0);

        let zpst = ZmqProducerStateTable::new(db.clone(), "table_a", zmqc.clone(), false);
        let zcst = ZmqConsumerStateTable::new(db.clone(), "table_a", &mut zmqs, None, None);
        assert!(!zcst.has_data());

        let kfvs = random_kfvs();
        for kfv in &kfvs {
            match kfv.operation {
                KeyOperation::Set => zpst.set(&kfv.key, &kfv.field_values),
                KeyOperation::Del => zpst.del(&kfv.key),
            }
        }

        sleep_zmq_poll();
        assert!(zcst.has_data());
        let kfvs_seen = zcst.pops();
        assert_eq!(kfvs.len(), kfvs_seen.len());
        assert_eq!(kfvs, kfvs_seen);
    }
}

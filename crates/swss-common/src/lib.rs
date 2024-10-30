mod bindings {
    #![allow(unused, non_snake_case, non_upper_case_globals, non_camel_case_types)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}
mod types;

use bindings::*;
pub use types::*;

pub fn sonic_db_config_initialize(path: &str) {
    let path = cstr(path);
    unsafe { bindings::SWSSSonicDBConfig_initialize(path.as_ptr()) }
}

pub fn sonic_db_config_initialize_global(path: &str) {
    let path = cstr(path);
    unsafe { bindings::SWSSSonicDBConfig_initializeGlobalConfig(path.as_ptr()) }
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
            let mut child = Command::new("timeout")
                .args([
                    "--signal=KILL",
                    "15s",
                    "redis-server",
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

    fn random_cxx_string() -> CxxString {
        CxxString::new(random_string())
    }

    fn random_kfv() -> KeyOpFieldValues {
        let key = random_string();
        let operation = if random() { KeyOperation::Set } else { KeyOperation::Del };
        let mut field_values = HashMap::new();

        if operation == KeyOperation::Set {
            // We need at least one field-value pair, otherwise swss::BinarySerializer infers that
            // the operation is DEL even if the .operation field is SET
            for _ in 0..rand::thread_rng().gen_range(100..1000) {
                field_values.insert(random_string(), random_cxx_string());
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

        let random = random_cxx_string();

        db.set("hello", &"hello, world!".into());
        db.set("random", &random);
        assert_eq!(db.get("hello").unwrap(), "hello, world!");
        assert_eq!(db.get("random").unwrap(), random);
        assert_eq!(db.get("noexist"), None);

        assert!(db.exists("hello"));
        assert!(!db.exists("noexist"));
        assert!(db.del("hello"));
        assert!(!db.del("hello"));
        assert!(db.del("random"));
        assert!(!db.del("random"));
        assert!(!db.del("noexist"));

        db.hset("a", "hello", &"hello, world!".into());
        db.hset("a", "random", &random);
        assert_eq!(db.hget("a", "hello").unwrap(), "hello, world!");
        assert_eq!(db.hget("a", "random").unwrap(), random);
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
        db.hset("a", "a", &"1".into());
        db.hset("a", "b", &"2".into());
        db.hset("a", "c", &"3".into());
        assert_eq!(
            db.hgetall("a"),
            HashMap::from_iter([
                ("a".into(), "1".into()),
                ("b".into(), "2".into()),
                ("c".into(), "3".into())
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
                KeyOperation::Set => pst.set(&kfv.key, kfv.field_values.clone()),
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

        db.hset("table_a:key_a", "field_a", &"value_a".into());
        db.hset("table_a:key_a", "field_b", &"value_b".into());
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

        zmqc.send_msg("", "table_a", kfvs.clone()); // db name is empty because we are using DbConnector::new_unix
        assert_eq!(zcst_table_a.read_data(Duration::from_millis(1500)), Data);
        assert_eq!(zcst_table_b.read_data(Duration::from_millis(1500)), Timeout);
        assert!(zcst_table_a.has_data());
        assert!(!zcst_table_b.has_data());

        zmqc.send_msg("", "table_b", kfvs.clone());
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
                KeyOperation::Set => zpst.set(&kfv.key, kfv.field_values.clone()),
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

mod bindings {
    #![allow(unused, non_snake_case, non_upper_case_globals, non_camel_case_types)]
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}
mod types;

use bindings::*;
pub use types::*;

/// Rust wrapper around `swss::SonicDBConfig::initialize`.
pub fn sonic_db_config_initialize(path: &str) {
    let path = cstr(path);
    unsafe { bindings::SWSSSonicDBConfig_initialize(path.as_ptr()) }
}

/// Rust wrapper around `swss::SonicDBConfig::initializeGlobalConfig`.
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

    #[test]
    fn dbconnector() {
        let redis = Redis::start();
        let db = DbConnector::new_unix(0, &redis.sock, 0);

        assert!(db.flush_db());

        let random = random_cxx_string();

        db.set("hello", &CxxString::new("hello, world!"));
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

        db.hset("a", "hello", &CxxString::new("hello, world!"));
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
        db.hset("a", "a", &CxxString::new("1"));
        db.hset("a", "b", &CxxString::new("2"));
        db.hset("a", "c", &CxxString::new("3"));
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

        assert_eq!(cst.read_data(Duration::from_millis(2000), true), SelectResult::Data);
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
        assert!(sst.pops().is_empty());

        db.hset("table_a:key_a", "field_a", &CxxString::new("value_a"));
        db.hset("table_a:key_a", "field_b", &CxxString::new("value_b"));
        assert_eq!(sst.read_data(Duration::from_millis(300), true), SelectResult::Data);
        let mut kfvs = sst.pops();

        // SubscriberStateTable will pick up duplicate KeyOpFieldValues' after two SETs on the same
        // key. I'm not actually sure if this is intended.
        assert_eq!(kfvs.len(), 2);
        assert_eq!(kfvs[0], kfvs[1]);

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

        zmqc.send_msg("", "table_a", kfvs.clone()); // db name is empty because we are using DbConnector::new_unix
        assert_eq!(zcst_table_a.read_data(Duration::from_millis(1500), true), Data);

        zmqc.send_msg("", "table_b", kfvs.clone());
        assert_eq!(zcst_table_b.read_data(Duration::from_millis(1500), true), Data);

        let kfvs_a = zcst_table_a.pops();
        let kvfs_b = zcst_table_b.pops();
        assert_eq!(kfvs_a, kvfs_b);
        assert_eq!(kfvs, kfvs_a);
    }

    #[test]
    fn zmq_consumer_producer_state_tables() {
        use SelectResult::*;

        sonic_db_config_init_for_test();

        let (endpoint, _delete) = random_zmq_endpoint();
        let mut zmqs = ZmqServer::new(&endpoint);
        let zmqc = ZmqClient::new(&endpoint);

        let redis = Redis::start();
        let db = DbConnector::new_unix(0, &redis.sock, 0);

        let zpst = ZmqProducerStateTable::new(db.clone(), "table_a", zmqc.clone(), false);
        let zcst = ZmqConsumerStateTable::new(db.clone(), "table_a", &mut zmqs, None, None);

        let kfvs = random_kfvs();
        for kfv in &kfvs {
            match kfv.operation {
                KeyOperation::Set => zpst.set(&kfv.key, kfv.field_values.clone()),
                KeyOperation::Del => zpst.del(&kfv.key),
            }
        }

        let mut kfvs_seen = Vec::new();
        while kfvs_seen.len() != kfvs.len() {
            assert_eq!(zcst.read_data(Duration::from_millis(2000), true), Data);
            kfvs_seen.extend(zcst.pops());
        }
        assert_eq!(kfvs, kfvs_seen);
    }

    #[cfg(feature = "async")]
    mod async_tests {
        use super::*;

        #[cfg(feature = "async")]
        async fn timeout<F: std::future::Future>(timeout_ms: u32, fut: F) -> F::Output {
            tokio::time::timeout(Duration::from_millis(timeout_ms.into()), fut)
                .await
                .expect("timed out")
        }

        #[tokio::test]
        async fn consumer_producer_state_tables_async() {
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

            timeout(2000, cst.read_data_async()).await.unwrap();
            let mut kfvs_cst = cst.pops();
            assert!(cst.pops().is_empty());

            kfvs.sort_unstable();
            kfvs_cst.sort_unstable();
            assert_eq!(kfvs_cst.len(), kfvs.len());
            assert_eq!(kfvs_cst, kfvs);
        }

        #[tokio::test]
        async fn subscriber_state_table_async() {
            sonic_db_config_init_for_test();
            let redis = Redis::start();
            let db = DbConnector::new_unix(0, &redis.sock, 0);

            let sst = SubscriberStateTable::new(db.clone(), "table_a", None, None);
            assert!(sst.pops().is_empty());

            db.hset("table_a:key_a", "field_a", &CxxString::new("value_a"));
            db.hset("table_a:key_a", "field_b", &CxxString::new("value_b"));
            timeout(300, sst.read_data_async()).await.unwrap();
            let mut kfvs = sst.pops();

            // SubscriberStateTable will pick up duplicate KeyOpFieldValues' after two SETs on the same
            // key. I'm not actually sure if this is intended.
            assert_eq!(kfvs.len(), 2);
            assert_eq!(kfvs[0], kfvs[1]);

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

        #[tokio::test]
        async fn zmq_consumer_producer_state_table_async() {
            sonic_db_config_init_for_test();

            let (endpoint, _delete) = random_zmq_endpoint();
            let mut zmqs = ZmqServer::new(&endpoint);
            let zmqc = ZmqClient::new(&endpoint);

            let redis = Redis::start();
            let db = DbConnector::new_unix(0, &redis.sock, 0);

            let zpst = ZmqProducerStateTable::new(db.clone(), "table_a", zmqc.clone(), false);
            let zcst = ZmqConsumerStateTable::new(db.clone(), "table_a", &mut zmqs, None, None);

            let kfvs = random_kfvs();
            for kfv in &kfvs {
                match kfv.operation {
                    KeyOperation::Set => zpst.set(&kfv.key, kfv.field_values.clone()),
                    KeyOperation::Del => zpst.del(&kfv.key),
                }
            }

            let mut kfvs_seen = Vec::new();
            while kfvs_seen.len() != kfvs.len() {
                timeout(2000, zcst.read_data_async()).await.unwrap();
                kfvs_seen.extend(zcst.pops());
            }
            assert_eq!(kfvs, kfvs_seen);
        }
    }
}

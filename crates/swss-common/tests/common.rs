#![allow(unused)]

use std::{
    collections::HashMap,
    fs::{self, remove_file},
    io::{BufRead, BufReader},
    iter,
    process::{Child, Command, Stdio},
    sync::Mutex,
};

use rand::{random, Rng};

use swss_common::*;

pub struct Redis {
    pub proc: Child,
    pub sock: String,
}

impl Redis {
    pub fn start() -> Self {
        sonic_db_config_init_for_test();

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
            if buf.contains("eady to accept connections") {
                break Self { proc: child, sock };
            }
        }
    }

    pub fn db_connector(&self) -> DbConnector {
        DbConnector::new_unix(0, &self.sock, 0).unwrap()
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

pub struct Defer<F: FnOnce()>(Option<F>);

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

pub fn sonic_db_config_init_for_test() {
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

pub fn random_string() -> String {
    format!("{:0X}", random::<u64>())
}

pub fn random_cxx_string() -> CxxString {
    CxxString::new(random_string())
}

pub fn random_fvs() -> FieldValues {
    let mut field_values = HashMap::new();
    for _ in 0..rand::thread_rng().gen_range(100..1000) {
        field_values.insert(random_string(), random_cxx_string());
    }
    field_values
}

pub fn random_kfv() -> KeyOpFieldValues {
    let key = random_string();
    let operation = if random() { KeyOperation::Set } else { KeyOperation::Del };
    let field_values = if operation == KeyOperation::Set {
        // We need at least one field-value pair, otherwise swss::BinarySerializer infers that
        // the operation is DEL even if the .operation field is SET
        random_fvs()
    } else {
        HashMap::new()
    };

    KeyOpFieldValues {
        key,
        operation,
        field_values,
    }
}

pub fn random_kfvs() -> Vec<KeyOpFieldValues> {
    iter::repeat_with(random_kfv).take(100).collect()
}

pub fn random_unix_sock() -> String {
    format!("/tmp/{}.sock", random_string())
}

// zmq doesn't clean up its own ipc sockets, so we include a deferred operation for that
pub fn random_zmq_endpoint() -> (String, impl Drop) {
    let sock = random_unix_sock();
    let endpoint = format!("ipc://{sock}");
    (endpoint, Defer::new(|| remove_file(sock).unwrap()))
}

#![allow(unused)]
use sonicdb_macro::SerdeSonicDb;
use sonicdb_serde::*;
use swss_common::*;

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

pub fn random_unix_sock() -> String {
    format!("/tmp/{}.sock", random_string())
}

#[test]
fn test_default() {
    #[derive(SerdeSonicDb)]
    struct MyStruct {
        #[serde_sonicdb(key)]
        id: String,
        attr1: Option<String>,
        attr2: Option<String>,
    }
    let obj = MyStruct {
        id: "id".to_string(),
        attr1: Some("attr1".to_string()),
        attr2: Some("attr2".to_string()),
    };

    assert!(MyStruct::get_key_separator() == "|");
    assert!(MyStruct::get_table_name() == "MYSTRUCT");
    assert!(obj.get_key() == "MYSTRUCT|id");
}
#[test]
fn test_attributes() {
    #[derive(SerdeSonicDb)]
    #[serde_sonicdb(table_name = "MY_STRUCT", key_separator = ":")]
    struct MyStruct {
        #[serde_sonicdb(key)]
        id1: String,
        #[serde_sonicdb(key)]
        id2: String,

        attr1: Option<String>,
        attr2: Option<String>,
    }
    impl MyStruct {
        fn from_key(key: &str) -> Self {
            let mut parts = key.split("|");
            // skip table name
            parts.next();
            Self {
                id1: parts.next().unwrap().to_string(),
                id2: parts.next().unwrap().to_string(),
                attr1: None,
                attr2: None,
            }
        }
    }
    let obj = MyStruct {
        id1: "id1".to_string(),
        id2: "id2".to_string(),
        attr1: Some("attr1".to_string()),
        attr2: Some("attr2".to_string()),
    };

    assert!(MyStruct::get_key_separator() == ":");
    assert!(MyStruct::get_table_name() == "MY_STRUCT");
    assert!(obj.get_key() == "MY_STRUCT:id1:id2");
}

#[test]
fn test_to_sonicdb() -> Result<()> {
    #[derive(SerdeSonicDb)]
    struct MyStruct {
        #[serde_sonicdb(key)]
        id: String,
        attr1: Option<String>,
        attr2: Option<String>,
    }

    let mut obj = MyStruct {
        id: "id".to_string(),
        attr1: Some("attr1".to_string()),
        attr2: Some("attr2".to_string()),
    };

    let redis = Redis::start();
    let db = redis.db_connector();
    obj.to_sonicdb(&db);
    assert_eq!(db.hget("MYSTRUCT|id", "attr1")?.unwrap(), "attr1");
    assert_eq!(db.hget("MYSTRUCT|id", "attr2")?.unwrap(), "attr2");

    obj.attr1 = None;
    obj.attr2 = Some("attr2_new".to_string());
    obj.to_sonicdb(&db);
    assert_eq!(db.hget("MYSTRUCT|id", "attr1")?, None);
    assert_eq!(db.hget("MYSTRUCT|id", "attr2")?.unwrap(), "attr2_new");

    Ok(())
}

#[test]
fn test_from_sonicdb() -> Result<()> {
    #[derive(SerdeSonicDb)]
    struct MyStruct {
        #[serde_sonicdb(key)]
        id: String,
        attr1: Option<String>,
        attr2: Option<String>,
    }
    let mut obj = MyStruct {
        id: "id".to_string(),
        attr1: None,
        attr2: None,
    };

    let redis = Redis::start();
    let db = redis.db_connector();
    db.hset("MYSTRUCT|id", "attr1", &CxxString::new("abc"))?;
    db.hset("MYSTRUCT|id", "attr2", &CxxString::new("def"))?;
    obj.from_sonicdb(&db)?;
    assert_eq!(obj.attr1, Some("abc".to_string()));
    assert_eq!(obj.attr2, Some("def".to_string()));

    Ok(())
}

#[test]
fn test_load_all() -> Result<()> {
    #[derive(SerdeSonicDb)]
    struct MyStruct {
        #[serde_sonicdb(key)]
        id: String,
        attr1: Option<String>,
        attr2: Option<String>,
    }
    let mut obj1 = MyStruct {
        id: "obj1".to_string(),
        attr1: "obj1_attr1".to_string().into(),
        attr2: "obj1_attr2".to_string().into(),
    };
    let mut obj2 = MyStruct {
        id: "obj2".to_string(),
        attr1: "obj2_attr1".to_string().into(),
        attr2: "obj2_attr2".to_string().into(),
    };
    let redis = Redis::start();
    let db = redis.db_connector();
    obj1.to_sonicdb(&db)?;
    obj2.to_sonicdb(&db)?;

    let entries: HashMap<String, MyStruct> = load_all(&db)?;
    assert_eq!(entries.len(), 2);
    let obj1 = &entries["MYSTRUCT|obj1"];
    assert_eq!(obj1.attr1, Some("obj1_attr1".to_string()));
    assert_eq!(obj1.attr2, Some("obj1_attr2".to_string()));

    let obj2 = &entries["MYSTRUCT|obj2"];
    assert_eq!(obj2.attr1, Some("obj2_attr1".to_string()));
    assert_eq!(obj2.attr2, Some("obj2_attr2".to_string()));

    Ok(())
}

use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader, Read},
    process::{Child, Command, Stdio},
};

use swss_common::DBConnector;

#[test]
fn dbconnector_test() {
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

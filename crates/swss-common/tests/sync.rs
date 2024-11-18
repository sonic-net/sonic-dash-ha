mod common;

use common::*;
use std::{collections::HashMap, time::Duration};
use swss_common::*;

#[test]
fn dbconnector() {
    let redis = Redis::start();
    let db = redis.db_connector();

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
    let pst = ProducerStateTable::new(redis.db_connector(), "table_a");
    let cst = ConsumerStateTable::new(redis.db_connector(), "table_a", None, None);

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
    let db = redis.db_connector();
    let sst = SubscriberStateTable::new(redis.db_connector(), "table_a", None, None);
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
    let zcst_table_a = ZmqConsumerStateTable::new(redis.db_connector(), "table_a", &mut zmqs, None, None);
    let zcst_table_b = ZmqConsumerStateTable::new(redis.db_connector(), "table_b", &mut zmqs, None, None);

    let kfvs = random_kfvs();

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
    let zpst = ZmqProducerStateTable::new(redis.db_connector(), "table_a", zmqc, false);
    let zcst = ZmqConsumerStateTable::new(redis.db_connector(), "table_a", &mut zmqs, None, None);

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

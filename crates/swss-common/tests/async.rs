#![cfg(feature = "async")]
mod common;

use common::*;
use paste::paste;
use std::{collections::HashMap, time::Duration};
use swss_common::*;

async fn timeout<F: std::future::Future>(timeout_ms: u32, fut: F) -> F::Output {
    tokio::time::timeout(Duration::from_millis(timeout_ms.into()), fut)
        .await
        .expect("timed out")
}

// This macro verifies that test function futures are Send. tokio::test is a bit misleading
// because a tokio runtime's root future can be !Send. This takes a test function and defines
// two tests from it - one that is the root future, one that is a separately spawned future.
macro_rules! define_tokio_test_fns {
    ($f:ident) => {
        paste! {
            #[tokio::test]
            async fn [< $f _root_future >]() {
                $f().await;
            }

            #[tokio::test]
            async fn [< $f _spawned_future >]() {
                tokio::task::spawn($f()).await;
            }
        }
    };
}

define_tokio_test_fns!(consumer_producer_state_tables_async);
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

define_tokio_test_fns!(subscriber_state_table_async);
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

define_tokio_test_fns!(zmq_consumer_producer_state_table_async);
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

use serde::{Deserialize, Serialize};
use std::{mem, time::Duration};
use swbus_actor::{Actor, ActorMessage, ActorRuntime, Result, State};
use swbus_edge::{swbus_proto::swbus::ServicePath, SwbusEdgeRuntime};
use swss_common::Table;
use swss_common_testing::Redis;
use tokio::{
    sync::mpsc,
    sync::oneshot::{channel, Sender},
    time::timeout,
};

use std::sync::Arc;
use swbus_cli_data::hamgr::actor_state::ActorState;
use swbus_edge::swbus_proto::swbus::{
    request_response::ResponseBody, swbus_message::Body, ManagementRequest, ManagementRequestType, SwbusErrorCode,
    SwbusMessage, SwbusMessageHeader,
};

fn sp(name: &str) -> ServicePath {
    ServicePath::from_string(&format!("test.test.test/test/test/test/{name}")).unwrap()
}

#[tokio::test]
async fn echo() {
    // Add a handler to the runtime to receive management response
    let (mgmt_resp_queue_tx, mut mgmt_resp_queue_rx) = mpsc::channel::<SwbusMessage>(1);

    let mut swbus_edge = SwbusEdgeRuntime::new("none".to_string(), sp("none"));
    swbus_edge.add_handler(sp("mgmt_resp"), mgmt_resp_queue_tx);

    swbus_edge.start().await.unwrap();
    let swbus_edge = Arc::new(swbus_edge);
    let actor_runtime = ActorRuntime::new(swbus_edge.clone());

    swbus_actor::set_global_runtime(actor_runtime);

    let (notify_done, is_done) = channel();

    swbus_actor::spawn(KVStore(Redis::start()), sp("kv"));
    swbus_actor::spawn(KVClient(notify_done), sp("client"));

    timeout(Duration::from_secs(3), is_done)
        .await
        .expect("timeout")
        .unwrap();
    verify_actor_state(swbus_edge, &mut mgmt_resp_queue_rx).await;
}

async fn verify_actor_state(swbus_edge: Arc<SwbusEdgeRuntime>, mgmt_resp_queue_rx: &mut mpsc::Receiver<SwbusMessage>) {
    // send a request to get actor state
    let mgmt_request = ManagementRequest::new(ManagementRequestType::HamgrdGetActorState);

    let header = SwbusMessageHeader::new(sp("mgmt_resp"), sp("kv"), 1);

    let request_msg = SwbusMessage {
        header: Some(header),
        body: Some(Body::ManagementRequest(mgmt_request)),
    };
    swbus_edge.send(request_msg).await.unwrap();
    let expected_json = r#"
    {
        "incoming_state": [
          {
            "key": "",
            "source": "test.test.test/test/test/test/client",
            "request_id": 1,
            "version": 2001,
            "message": {
              "key": "",
              "data": "{\"Get\":{\"key\":\"count\"}}"
            }
          }
        ],
        "internal_state": [
          {
            "key": "data",
            "swss_table": "kv-actor-data",
            "swss_key": "kv-actor-data",
            "fvs": [
              {
                "key": "count",
                "value": "1000"
              }
            ],
            "mutated": false,
            "backup_fvs": [
              {
                "key": "count",
                "value": "999"
              }
            ]
          }
        ]
      }
      "#;

    let expected = serde_json::from_str(&expected_json).unwrap();
    match timeout(Duration::from_secs(3), mgmt_resp_queue_rx.recv()).await {
        Ok(Some(msg)) => match msg.body {
            Some(Body::Response(ref response)) => {
                assert_eq!(response.request_id, 1);
                assert_eq!(response.error_code, SwbusErrorCode::Ok as i32);
                match response.response_body {
                    Some(ResponseBody::ManagementQueryResult(ref result)) => {
                        println!("{}", &result.value);
                        let state: ActorState = serde_json::from_str(&result.value).unwrap();

                        assert_eq!(state, expected);
                    }
                    _ => assert!(false, "message body is not a ManagementQueryResult"),
                }
            }
            _ => assert!(false, "message body is not a Response"),
        },
        Ok(None) => {
            assert!(false, "channel broken");
        }
        Err(_) => {
            assert!(false, "request timeout: didn't receive response");
        }
    }
}

#[derive(Serialize, Deserialize)]
enum KVMessage {
    Get { key: String },
    Set { key: String, val: String },
}

impl KVMessage {
    fn get(k: impl Into<String>) -> Self {
        KVMessage::Get { key: k.into() }
    }

    fn set(k: impl Into<String>, v: impl Into<String>) -> Self {
        KVMessage::Set {
            key: k.into(),
            val: v.into(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct KVGetResult {
    key: String,
    val: String,
}

struct KVClient(Sender<()>);

impl KVClient {
    fn notify_done(&mut self) {
        mem::replace(&mut self.0, channel().0).send(()).unwrap();
    }
}

impl Actor for KVClient {
    async fn init(&mut self, state: &mut State) -> Result<()> {
        state
            .outgoing()
            .send(sp("kv"), ActorMessage::new("", &KVMessage::get("count"))?);
        Ok(())
    }

    async fn handle_message(&mut self, state: &mut State, key: &str) -> Result<()> {
        assert_eq!(key, "kv-get");
        let KVGetResult { key, val } = state.incoming().get(key)?.deserialize_data::<KVGetResult>()?;

        match &*key {
            "count" => {
                let n = if val.is_empty() { 0 } else { val.parse::<u32>()? };
                if n == 1000 {
                    self.notify_done();
                } else {
                    state.outgoing().send(
                        sp("kv"),
                        ActorMessage::new("", &KVMessage::set("count", format!("{}", n + 1)))?,
                    );

                    state
                        .outgoing()
                        .send(sp("kv"), ActorMessage::new("", &KVMessage::get("count"))?);
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}

struct KVStore(Redis);

impl Actor for KVStore {
    async fn init(&mut self, state: &mut State) -> Result<()> {
        let table = Table::new_async(self.0.db_connector(), "kv-actor-data").await?;
        state.internal().add("data", table, "kv-actor-data").await;
        Ok(())
    }

    async fn handle_message(&mut self, state: &mut State, key: &str) -> Result<()> {
        let (internal, incoming, outgoing) = state.get_all();
        let entry = incoming.get_entry(key).unwrap();
        match entry.msg.deserialize_data::<KVMessage>()? {
            KVMessage::Get { key } => {
                let val = internal
                    .get("data")
                    .get(&key)
                    .map(|cxx_string| cxx_string.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let source = entry.source.clone();
                outgoing.send(
                    source,
                    ActorMessage::new(
                        "kv-get",
                        &KVGetResult {
                            key: key.to_owned(),
                            val,
                        },
                    )?,
                );
            }
            KVMessage::Set { key, val } => {
                state.internal().get_mut("data").insert(key, val.into());
            }
        }
        Ok(())
    }
}

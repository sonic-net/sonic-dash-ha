use serde::{Deserialize, Serialize};
use std::{mem, time::Duration};
use swbus_actor::{Actor, ActorMessage, ActorRuntime, Context, Result, State};
use swbus_edge::{swbus_proto::swbus::ConnectionType, swbus_proto::swbus::ServicePath, SwbusEdgeRuntime};
use swss_common::Table;
use swss_common_testing::Redis;
use tokio::{
    sync::mpsc,
    sync::oneshot::{channel, Sender},
    time::timeout,
};

use std::sync::Arc;
use swbus_actor::state::ActorStateDump;
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

    let mut swbus_edge = SwbusEdgeRuntime::new("none".to_string(), sp("none"), ConnectionType::InNode);
    swbus_edge.add_handler(sp("mgmt_resp"), mgmt_resp_queue_tx);

    swbus_edge.start().await.unwrap();
    let swbus_edge = Arc::new(swbus_edge);
    let actor_runtime = ActorRuntime::new(swbus_edge.clone());

    swbus_actor::set_global_runtime(actor_runtime);

    let (notify_done, is_done) = channel();

    swbus_actor::spawn(KVStore(Redis::start()), "test", "kv");
    swbus_actor::spawn(KVClient(notify_done, false), "test", "client");

    timeout(Duration::from_secs(3), is_done)
        .await
        .expect("timeout")
        .unwrap();
    verify_actor_state(swbus_edge.clone(), &mut mgmt_resp_queue_rx).await;

    // Test delete operation
    let (notify_done_delete, is_done_delete) = channel();
    swbus_actor::spawn(KVClient(notify_done_delete, true), "test", "client");

    timeout(Duration::from_secs(3), is_done_delete)
        .await
        .expect("timeout")
        .unwrap();
    verify_actor_state_after_delete(swbus_edge, &mut mgmt_resp_queue_rx).await;
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
        "incoming": {
            "": {
            "msg": {
                "key": "",
                "data": {
                "Get": {
                    "key": "count"
                }
                }
            },
            "source": {
                "region_id": "test",
                "cluster_id": "test",
                "node_id": "test",
                "service_type": "test",
                "service_id": "test",
                "resource_type": "test",
                "resource_id": "client"
            },
            "request_id": 0,
            "version": 2001,
            "created_time": 0,
            "last_updated_time": 0,
            "response": "Ok",
            "acked": true
            }
        },
        "internal": {
            "data": {
            "swss_table_name": "kv-actor-data",
            "swss_key": "kv-actor-data",
            "fvs": {
                "count": "1000"
            },
            "mutated": false,
            "to_delete": false,
            "backup_fvs": {
                "count": "999"
            },
            "last_updated_time": 0
            }
        },
        "outgoing":{
            "outgoing_queued":[],
            "outgoing_sent":{
                "kv-get":{
                    "msg":{
                        "key":"kv-get",
                        "data":{
                            "key":"count",
                            "val":"1000"
                        }
                    },
                    "id":0,
                    "created_time":0,
                    "last_updated_time":0,
                    "last_sent_time":0,
                    "version":1001,
                    "acked":true,
                    "response":"Ok",
                    "response_source":{
                        "region_id":"test",
                        "cluster_id":"test",
                        "node_id":"test",
                        "service_type":"test",
                        "service_id":"test",
                        "resource_type":"test",
                        "resource_id":"client"
                    }
                }
            }
        }
    }
    "#;

    let expected: ActorStateDump = serde_json::from_str(expected_json).unwrap();
    match timeout(Duration::from_secs(3), mgmt_resp_queue_rx.recv()).await {
        Ok(Some(msg)) => match msg.body {
            Some(Body::Response(ref response)) => {
                assert_eq!(response.request_id, 1);
                assert_eq!(response.error_code, SwbusErrorCode::Ok as i32);
                match response.response_body {
                    Some(ResponseBody::ManagementQueryResult(ref result)) => {
                        println!("{}", &result.value);
                        let mut state: ActorStateDump = serde_json::from_str(&result.value).unwrap();
                        let inner_fields = state.outgoing.outgoing_sent.get_mut("kv-get").unwrap();
                        inner_fields.id = 0;
                        inner_fields.created_time = 0;
                        inner_fields.last_updated_time = 0;
                        inner_fields.last_sent_time = 0;

                        assert_eq!(state, expected);
                    }
                    _ => panic!("message body is not a ManagementQueryResult"),
                }
            }
            _ => panic!("message body is not a Response"),
        },
        Ok(None) => {
            panic!("channel broken");
        }
        Err(_) => {
            panic!("request timeout: didn't receive response");
        }
    }
}

async fn verify_actor_state_after_delete(
    swbus_edge: Arc<SwbusEdgeRuntime>,
    mgmt_resp_queue_rx: &mut mpsc::Receiver<SwbusMessage>,
) {
    // send a request to get actor state
    let mgmt_request = ManagementRequest::new(ManagementRequestType::HamgrdGetActorState);

    let header = SwbusMessageHeader::new(sp("mgmt_resp"), sp("kv"), 2);

    let request_msg = SwbusMessage {
        header: Some(header),
        body: Some(Body::ManagementRequest(mgmt_request)),
    };
    swbus_edge.send(request_msg).await.unwrap();
    let expected_json = r#"
    {
        "incoming": {
            "": {
            "msg": {
                "key": "",
                "data": {
                "Del": {
                    "key": "count"
                }
                }
            },
            "source": {
                "region_id": "test",
                "cluster_id": "test",
                "node_id": "test",
                "service_type": "test",
                "service_id": "test",
                "resource_type": "test",
                "resource_id": "client"
            },
            "request_id": 0,
            "version": 2002,
            "created_time": 0,
            "last_updated_time": 0,
            "response": "Ok",
            "acked": true
            }
        },
        "internal": {},
        "outgoing":{
            "outgoing_queued":[],
            "outgoing_sent":{
                "kv-get":{
                    "msg":{
                        "key":"kv-get",
                        "data":{
                            "key":"count",
                            "val":"1000"
                        }
                    },
                    "id":0,
                    "created_time":0,
                    "last_updated_time":0,
                    "last_sent_time":0,
                    "version":1001,
                    "acked":true,
                    "response":"Ok",
                    "response_source":{
                        "region_id":"test",
                        "cluster_id":"test",
                        "node_id":"test",
                        "service_type":"test",
                        "service_id":"test",
                        "resource_type":"test",
                        "resource_id":"client"
                    }
                }
            }
        }
    }
    "#;

    let expected: ActorStateDump = serde_json::from_str(expected_json).unwrap();
    match timeout(Duration::from_secs(3), mgmt_resp_queue_rx.recv()).await {
        Ok(Some(msg)) => match msg.body {
            Some(Body::Response(ref response)) => {
                assert_eq!(response.request_id, 2);
                assert_eq!(response.error_code, SwbusErrorCode::Ok as i32);
                match response.response_body {
                    Some(ResponseBody::ManagementQueryResult(ref result)) => {
                        println!("{}", &result.value);
                        let mut state: ActorStateDump = serde_json::from_str(&result.value).unwrap();
                        if let Some(inner_fields) = state.outgoing.outgoing_sent.get_mut("kv-get") {
                            inner_fields.id = 0;
                            inner_fields.created_time = 0;
                            inner_fields.last_updated_time = 0;
                            inner_fields.last_sent_time = 0;
                        }
                        // Reset timestamps for incoming message
                        if let Some(incoming_entry) = state.incoming.get_mut("") {
                            incoming_entry.request_id = 0;
                            incoming_entry.created_time = 0;
                            incoming_entry.last_updated_time = 0;
                        }

                        assert_eq!(state, expected);
                    }
                    _ => panic!("message body is not a ManagementQueryResult"),
                }
            }
            _ => panic!("message body is not a Response"),
        },
        Ok(None) => {
            panic!("channel broken");
        }
        Err(_) => {
            panic!("request timeout: didn't receive response");
        }
    }
}

#[derive(Serialize, Deserialize)]
enum KVMessage {
    Get { key: String },
    Set { key: String, val: String },
    Del { key: String },
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

    fn del(k: impl Into<String>) -> Self {
        KVMessage::Del { key: k.into() }
    }
}

#[derive(Serialize, Deserialize)]
struct KVGetResult {
    key: String,
    val: String,
}

struct KVClient(Sender<()>, bool);

impl KVClient {
    fn notify_done(&mut self) {
        mem::replace(&mut self.0, channel().0).send(()).unwrap();
    }
}

impl Actor for KVClient {
    async fn init(&mut self, state: &mut State) -> Result<()> {
        if self.1 {
            // to_delete is true, send delete message and immediately notify done
            state
                .outgoing()
                .send(sp("kv"), ActorMessage::new("", &KVMessage::del("count"))?);
            self.notify_done();
        } else {
            state
                .outgoing()
                .send(sp("kv"), ActorMessage::new("", &KVMessage::get("count"))?);
        }
        Ok(())
    }

    async fn handle_message(&mut self, state: &mut State, key: &str, _context: &mut Context) -> Result<()> {
        if self.1 {
            // For delete operations, we shouldn't receive any messages
            return Ok(());
        }

        assert_eq!(key, "kv-get");
        let KVGetResult { key, val } = state.incoming().get_or_fail(key)?.deserialize_data::<KVGetResult>()?;

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

    async fn handle_message(&mut self, state: &mut State, key: &str, _context: &mut Context) -> Result<()> {
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
            KVMessage::Del { key: _ } => {
                state.internal().delete("data");
            }
        }
        Ok(())
    }
}

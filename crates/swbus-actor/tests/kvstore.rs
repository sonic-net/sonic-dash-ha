use serde::{Deserialize, Serialize};
use std::{mem, time::Duration};
use swbus_actor::{Actor, ActorMessage, ActorRuntime, Result, State};
use swbus_edge::{swbus_proto::swbus::ServicePath, SwbusEdgeRuntime};
use swss_common::Table;
use swss_common_testing::Redis;
use tokio::{
    sync::oneshot::{channel, Sender},
    time::timeout,
};

fn sp(name: &str) -> ServicePath {
    ServicePath::from_string(&format!("test.test.test/test/test/test/{name}")).unwrap()
}

#[tokio::test]
async fn echo() {
    let mut swbus_edge = SwbusEdgeRuntime::new("none".to_string(), sp("none"));
    swbus_edge.start().await.unwrap();
    let actor_runtime = ActorRuntime::new(swbus_edge.into());
    swbus_actor::set_global_runtime(actor_runtime);

    let (notify_done, is_done) = channel();

    swbus_actor::spawn(KVStore(Redis::start()), sp("kv"));
    swbus_actor::spawn(KVClient(notify_done), sp("client"));

    timeout(Duration::from_secs(3), is_done)
        .await
        .expect("timeout")
        .unwrap();
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

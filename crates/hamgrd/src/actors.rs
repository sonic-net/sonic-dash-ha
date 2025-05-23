//! Actors
//!
//! <https://github.com/r12f/SONiC/blob/user/r12f/hamgrd/doc/smart-switch/high-availability/smart-switch-ha-hamgrd.md#2-key-actors>

pub mod dpu;
pub mod vdpu;

pub mod ha_scope;
pub mod ha_set;

#[cfg(test)]
pub mod test;
use anyhow::Result as AnyhowResult;
use std::sync::Arc;
use swbus_actor::{spawn, Actor, ActorMessage};
use swbus_edge::swbus_proto::message_id_generator::MessageIdGenerator;
use swbus_edge::swbus_proto::result::*;
use swbus_edge::swbus_proto::swbus::{swbus_message::Body, DataRequest, ServicePath, SwbusErrorCode, SwbusMessage};
use swbus_edge::SwbusEdgeRuntime;
use swss_common::{KeyOpFieldValues, KeyOperation, SubscriberStateTable, ZmqClient, ZmqProducerStateTable};
use swss_common_bridge::{consumer::ConsumerBridge, producer::spawn_producer_bridge};
use tokio::sync::mpsc::{channel, Receiver};
use tracing::error;
pub struct ActorCreator<F, T>
where
    F: Fn(String) -> AnyhowResult<T>,
    T: Actor,
{
    sp: ServicePath,
    rt: Arc<SwbusEdgeRuntime>,
    handler_rx: Receiver<SwbusMessage>,
    create_fn: F,
    id_generator: MessageIdGenerator,
}
// Connection worker facade
impl<F, T> ActorCreator<F, T>
where
    F: Fn(String) -> AnyhowResult<T>,
    T: Actor,
{
    pub fn new(sp: ServicePath, rt: Arc<SwbusEdgeRuntime>, public: bool, create_fn: F) -> Self {
        let (handler_tx, handler_rx) = channel::<SwbusMessage>(10);

        if public {
            rt.add_handler(sp.clone(), handler_tx);
        } else {
            rt.add_private_handler(sp.clone(), handler_tx);
        }

        Self {
            sp,
            rt,
            handler_rx,
            create_fn,
            id_generator: MessageIdGenerator::new(),
        }
    }

    pub async fn run(mut self) {
        loop {
            let msg = self.handler_rx.recv().await.unwrap();
            if let Err(swbus_err) = self.handle_received_message(&msg).await {
                let (code, err_msg) = match swbus_err {
                    SwbusError::ConnectionError { code, detail } => (code, detail.to_string()),
                    SwbusError::InternalError { code, detail } => (code, detail),
                    SwbusError::InputError { code, detail } => (code, detail),
                    SwbusError::RouteError { code, detail } => (code, detail),
                };
                let response = SwbusMessage::new_response(
                    &msg,
                    Some(&self.sp),
                    code,
                    &err_msg,
                    self.id_generator.generate(),
                    None,
                );
                if self.rt.send(response).await.is_err() {
                    error!("Failed to send response to swbus");
                }
            } else {
                // forward the message to the actor that is just spawned
                if self.rt.send(msg).await.is_err() {
                    error!("Failed to send response to swbus");
                }
            }
        }
    }

    async fn handle_received_message(&self, msg: &SwbusMessage) -> Result<()> {
        let header = msg.header.as_ref().unwrap();
        let source = header.source.as_ref().unwrap();
        let destination = header.destination.as_ref().unwrap();
        // todo: alternatively, check message type
        if source.resource_type != "swss-common-bridge" {
            // todo: log a message
            return Err(SwbusError::route(
                SwbusErrorCode::NoRoute,
                "actor doesn't exist: not from common-bridge".to_string(),
            ));
        }
        if let Some(Body::DataRequest(DataRequest { payload })) = &msg.body {
            match ActorMessage::deserialize(payload) {
                Ok(actor_msg) => {
                    let kfv: KeyOpFieldValues = actor_msg.deserialize_data().map_err(|_| {
                        SwbusError::input(
                            SwbusErrorCode::InvalidPayload,
                            "cannot decode as ActorMessage".to_string(),
                        )
                    })?;

                    if kfv.operation == KeyOperation::Del {
                        return Err(SwbusError::input(
                            SwbusErrorCode::NoRoute,
                            "actor doesn't exist: won't create actor for DEL kfv".to_string(),
                        ));
                    }
                    let actor = (self.create_fn)(kfv.key.clone()).map_err(|e| {
                        let mut sp = self.sp.clone();
                        sp.resource_id = kfv.key.clone();
                        SwbusError::input(
                            SwbusErrorCode::Fail,
                            format!("Failed to create actor {}. Error: {}", sp.to_swbusd_service_path(), e),
                        )
                    })?;
                    spawn(actor, &destination.resource_type, &destination.resource_id);
                }
                Err(_) => {
                    // log a message
                    return Err(SwbusError::input(
                        SwbusErrorCode::InvalidPayload,
                        "cannot decode as ActorMessage".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

pub async fn spawn_consumer_bridge_for_actor(
    edge_runtime: Arc<SwbusEdgeRuntime>,
    db_name: &'static str,
    table_name: &'static str,
    actor_name: &'static str,
    actor_id: Option<&str>,
    single_entry: bool,
) -> AnyhowResult<ConsumerBridge> {
    spawn_consumer_bridge_for_actor_with_selector(
        edge_runtime,
        db_name,
        table_name,
        actor_name,
        actor_id,
        single_entry,
        |_| true,
    )
    .await
}

/// Spawn a consumer bridge for an actor. The bridge subscribes to the given db and table.
/// The bridge will send messages to the actor when there is update on the table. If actor_id is
/// provided, the message destination is `sp(actor_name, actor_id)`. Otherwise, the
/// service path is constructed as `sp(actor_name, key)`, where key is the key of the table entry.
/// The ActorMessage payload is the KeyOpFieldValues of the table entry. If single_entry is set,
/// the key of the ActorMessage is the table_name. Otherwise, the key is table_name|key.
/// selector is a function that takes a KeyOpFieldValues and returns a boolean. If the function
/// returns true, the message is sent to the actor. Otherwise, the message is ignored.
///
pub async fn spawn_consumer_bridge_for_actor_with_selector<F>(
    edge_runtime: Arc<SwbusEdgeRuntime>,
    db_name: &'static str,
    table_name: &'static str,
    actor_name: &'static str,
    actor_id: Option<&str>,
    single_entry: bool,
    selector: F,
) -> AnyhowResult<ConsumerBridge>
where
    F: Fn(&KeyOpFieldValues) -> bool + Sync + Send + 'static,
{
    let db = crate::db_named(db_name).await?;

    let sst = SubscriberStateTable::new_async(db, table_name, None, None).await?;

    let addr = crate::sp("swss-common-bridge", table_name);

    if actor_id.is_some() {
        let actor_id = actor_id.unwrap().to_owned();
        Ok(ConsumerBridge::spawn(
            edge_runtime,
            addr,
            sst,
            move |kfv: &KeyOpFieldValues| {
                let sp = crate::sp(actor_name, &actor_id);
                let key = match single_entry {
                    true => table_name.to_owned(),
                    false => format!("{}|{}", table_name, kfv.key),
                };
                (sp, key)
            },
            selector,
        ))
    } else {
        Ok(ConsumerBridge::spawn(
            edge_runtime,
            addr,
            sst,
            move |kfv: &KeyOpFieldValues| {
                (
                    crate::sp(actor_name, &kfv.key),
                    match single_entry {
                        true => table_name.to_owned(),
                        false => format!("{}|{}", table_name, kfv.key),
                    },
                )
            },
            selector,
        ))
    }
}

pub async fn spawn_zmq_producer_bridge(
    edge_runtime: Arc<SwbusEdgeRuntime>,
    db_name: &str,
    table_name: &str,
    zmq_endpoint: &str,
) -> AnyhowResult<()> {
    let zmqc = ZmqClient::new(zmq_endpoint)?;
    let dpu_appl_db = crate::db_named(db_name).await?;
    let zpst = ZmqProducerStateTable::new(dpu_appl_db, table_name, zmqc, false).unwrap();

    let sp = edge_runtime.new_sp("swss-common-bridge", table_name);
    spawn_producer_bridge(edge_runtime.clone(), sp, zpst).await?;
    Ok(())
}

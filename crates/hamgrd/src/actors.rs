//! Actors
//!
//! <https://github.com/r12f/SONiC/blob/user/r12f/hamgrd/doc/smart-switch/high-availability/smart-switch-ha-hamgrd.md#2-key-actors>
pub mod dpu;
pub mod ha_scope;
pub mod ha_set;
pub mod vdpu;

#[cfg(test)]
pub mod test;
use anyhow::Result as AnyhowResult;
use futures_util::FutureExt;
use sonic_common::SonicDbTable;
use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use swbus_actor::{spawn, Actor, ActorMessage};
use swbus_edge::swbus_proto::message_id_generator::MessageIdGenerator;
use swbus_edge::swbus_proto::result::*;
use swbus_edge::swbus_proto::swbus::{swbus_message::Body, DataRequest, ServicePath, SwbusErrorCode, SwbusMessage};
use swbus_edge::SwbusEdgeRuntime;
use swss_common::{
    KeyOpFieldValues, KeyOperation, ProducerStateTable, SubscriberStateTable, ZmqClient, ZmqProducerStateTable,
};
use swss_common_bridge::{consumer::ConsumerBridge, producer::spawn_producer_bridge};
use tokio::sync::mpsc::{channel, Receiver};
use tokio::task::JoinHandle;
use tracing::{error, info};

/// Best-effort extraction of a panic message from `catch_unwind`'s payload.
fn panic_payload_to_string(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

pub trait DbBasedActor: Actor {
    fn name() -> &'static str;
    fn table_name() -> &'static str;
    fn new(key: String) -> AnyhowResult<Self>
    where
        Self: Sized;

    async fn start_actor_creator<T>(edge_runtime: Arc<SwbusEdgeRuntime>) -> AnyhowResult<Vec<ConsumerBridge>>
    where
        Self: Sized,
        T: SonicDbTable + 'static,
    {
        let ac = ActorCreator::new(
            edge_runtime.new_sp(Self::name(), ""),
            edge_runtime.clone(),
            false,
            |key: String| -> AnyhowResult<Self> { Self::new(key) },
        );

        tokio::task::spawn(ac.run());

        let config_db = crate::db_for_table::<T>().await?;
        let sst = SubscriberStateTable::new_async(config_db, T::table_name(), None, None).await?;
        let addr = crate::common_bridge_sp::<T>(&edge_runtime);
        let base_addr = edge_runtime.get_base_sp();
        Ok(vec![ConsumerBridge::spawn::<T, _, _, _>(
            edge_runtime.clone(),
            addr,
            sst,
            move |kfv: &KeyOpFieldValues| {
                let mut addr = base_addr.clone();
                addr.resource_type = Self::name().to_owned();
                addr.resource_id = kfv.key.clone();
                (addr, T::table_name().to_owned())
            },
            |_| true,
        )])
    }
}

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
            let msg = match self.handler_rx.recv().await {
                Some(m) => m,
                None => {
                    // All senders dropped; the route is gone, so we can safely exit.
                    error!(
                        "ActorCreator {}: handler channel closed, exiting",
                        self.sp.to_swbusd_service_path()
                    );
                    return;
                }
            };

            // Per-message handling must never panic, otherwise the run task
            // dies, the receiver is dropped, and the still-registered route
            // returns "channel closed" forever after.
            let result = AssertUnwindSafe(self.handle_received_message(&msg))
                .catch_unwind()
                .await;

            match result {
                Ok(Ok(())) => {
                    // forward the message to the actor that is just spawned
                    if self.rt.send(msg).await.is_err() {
                        error!("Failed to send response to swbus");
                    }
                }
                Ok(Err(swbus_err)) => {
                    let (code, err_msg) = match swbus_err {
                        SwbusError::ConnectionError { code, detail } => (code, detail.to_string()),
                        SwbusError::InternalError { code, detail } => (code, detail),
                        SwbusError::InputError { code, detail } => (code, detail),
                        SwbusError::RouteError { code, detail } => (code, detail),
                    };
                    let Some(req_header) = msg.header.as_ref() else {
                        error!("Cannot send error response: incoming message has no header");
                        continue;
                    };
                    let response = SwbusMessage::new_response(
                        req_header,
                        Some(&self.sp),
                        code,
                        &err_msg,
                        self.id_generator.generate(),
                        None,
                    );
                    if self.rt.send(response).await.is_err() {
                        error!("Failed to send response to swbus");
                    }
                }
                Err(panic) => {
                    let panic_msg = panic_payload_to_string(&panic);
                    error!(
                        "ActorCreator {}: panic while handling message: {}",
                        self.sp.to_swbusd_service_path(),
                        panic_msg
                    );
                    // Best-effort: tell the sender we failed so it doesn't hang.
                    if let Some(req_header) = msg.header.as_ref() {
                        let response = SwbusMessage::new_response(
                            req_header,
                            Some(&self.sp),
                            SwbusErrorCode::Fail,
                            &format!("ActorCreator panicked: {panic_msg}"),
                            self.id_generator.generate(),
                            None,
                        );
                        if self.rt.send(response).await.is_err() {
                            error!("Failed to send panic response to swbus");
                        }
                    }
                    // Continue the loop; the route stays valid.
                }
            }
        }
    }

    async fn handle_received_message(&self, msg: &SwbusMessage) -> Result<()> {
        let header = msg
            .header
            .as_ref()
            .ok_or_else(|| SwbusError::input(SwbusErrorCode::InvalidPayload, "missing message header".to_string()))?;
        let source = header
            .source
            .as_ref()
            .ok_or_else(|| SwbusError::input(SwbusErrorCode::InvalidPayload, "missing message source".to_string()))?;
        let destination = header.destination.as_ref().ok_or_else(|| {
            SwbusError::input(
                SwbusErrorCode::InvalidPayload,
                "missing message destination".to_string(),
            )
        })?;
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
                        return Err(SwbusError::route(
                            SwbusErrorCode::NoRoute,
                            "actor doesn't exist: won't create actor for DEL kfv".to_string(),
                        ));
                    }
                    // create_fn / spawn are user-supplied and must not be allowed to
                    // panic the run loop. Catch any panic and turn it into a Fail.
                    let key = kfv.key.clone();
                    let create_result = std::panic::catch_unwind(AssertUnwindSafe(|| (self.create_fn)(key)));
                    let actor = match create_result {
                        Ok(Ok(actor)) => actor,
                        Ok(Err(e)) => {
                            let mut sp = self.sp.clone();
                            sp.resource_id = kfv.key.clone();
                            return Err(SwbusError::internal(
                                SwbusErrorCode::Fail,
                                format!("Failed to create actor {}. Error: {}", sp.to_swbusd_service_path(), e),
                            ));
                        }
                        Err(panic) => {
                            let mut sp = self.sp.clone();
                            sp.resource_id = kfv.key.clone();
                            return Err(SwbusError::internal(
                                SwbusErrorCode::Fail,
                                format!(
                                    "Panic while creating actor {}: {}",
                                    sp.to_swbusd_service_path(),
                                    panic_payload_to_string(&panic)
                                ),
                            ));
                        }
                    };
                    let res_type = destination.resource_type.clone();
                    let res_id = destination.resource_id.clone();
                    if let Err(panic) = std::panic::catch_unwind(AssertUnwindSafe(|| spawn(actor, &res_type, &res_id)))
                    {
                        return Err(SwbusError::internal(
                            SwbusErrorCode::Fail,
                            format!(
                                "Panic while spawning actor {}/{}: {}",
                                res_type,
                                res_id,
                                panic_payload_to_string(&panic)
                            ),
                        ));
                    }
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

pub async fn spawn_consumer_bridge_for_actor<T>(
    edge_runtime: Arc<SwbusEdgeRuntime>,
    actor_name: &'static str,
    actor_id: Option<&str>,
    single_entry: bool,
) -> AnyhowResult<ConsumerBridge>
where
    T: SonicDbTable + 'static,
{
    spawn_consumer_bridge_for_actor_with_selector::<T, _>(edge_runtime, actor_name, actor_id, single_entry, |_| true)
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
pub async fn spawn_consumer_bridge_for_actor_with_selector<T, F>(
    edge_runtime: Arc<SwbusEdgeRuntime>,
    actor_name: &'static str,
    actor_id: Option<&str>,
    single_entry: bool,
    selector: F,
) -> AnyhowResult<ConsumerBridge>
where
    T: SonicDbTable + 'static,
    F: Fn(&KeyOpFieldValues) -> bool + Sync + Send + 'static,
{
    let db = crate::db_for_table::<T>().await?;

    let sst = SubscriberStateTable::new_async(db, T::table_name(), None, None).await?;

    let addr = crate::common_bridge_sp::<T>(&edge_runtime);

    if let Some(actor_id) = actor_id {
        let sp = edge_runtime.new_sp(actor_name, actor_id);
        Ok(ConsumerBridge::spawn::<T, _, _, _>(
            edge_runtime,
            addr,
            sst,
            move |kfv: &KeyOpFieldValues| {
                let key = match single_entry {
                    true => T::table_name().to_owned(),
                    false => format!("{}|{}", T::table_name(), kfv.key),
                };
                (sp.clone(), key)
            },
            selector,
        ))
    } else {
        let base_addr = edge_runtime.get_base_sp();
        Ok(ConsumerBridge::spawn::<T, _, _, _>(
            edge_runtime,
            addr,
            sst,
            move |kfv: &KeyOpFieldValues| {
                let mut addr = base_addr.clone();
                addr.resource_type = actor_name.to_owned();
                addr.resource_id = kfv.key.clone();
                (
                    addr,
                    match single_entry {
                        true => T::table_name().to_owned(),
                        false => format!("{}|{}", T::table_name(), kfv.key),
                    },
                )
            },
            selector,
        ))
    }
}

pub async fn spawn_zmq_producer_bridge<T>(
    edge_runtime: Arc<SwbusEdgeRuntime>,
    zmq_endpoint: &str,
) -> AnyhowResult<JoinHandle<()>>
where
    T: SonicDbTable + 'static,
{
    if let Ok(zmqc) = ZmqClient::new(zmq_endpoint) {
        let dpu_appl_db = crate::db_for_table::<T>().await?;
        let zpst = ZmqProducerStateTable::new(dpu_appl_db, T::table_name(), zmqc, true).unwrap();

        let sp = crate::common_bridge_sp::<T>(&edge_runtime);
        info!(
            "spawned ZMQ producer bridge for {} at {}",
            T::table_name(),
            sp.to_longest_path()
        );
        Ok(spawn_producer_bridge(edge_runtime.clone(), sp, zpst))
    } else {
        anyhow::bail!("Failed to connect to ZMQ server at {}", zmq_endpoint);
    }
}

pub async fn spawn_vanilla_producer_bridge<T>(edge_runtime: Arc<SwbusEdgeRuntime>) -> AnyhowResult<JoinHandle<()>>
where
    T: SonicDbTable + 'static,
{
    let db = crate::db_for_table::<T>().await?;
    let pst = ProducerStateTable::new(db, T::table_name()).unwrap();

    let sp = crate::common_bridge_sp::<T>(&edge_runtime);
    info!(
        "spawned producer bridge for {} at {}",
        T::table_name(),
        sp.to_longest_path()
    );
    Ok(spawn_producer_bridge(edge_runtime.clone(), sp, pst))
}

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
use sonic_common::SonicDbTable;
use std::sync::Arc;
use swbus_actor::{spawn, Actor, ActorMessage};
use swbus_edge::swbus_proto::message_id_generator::MessageIdGenerator;
use swbus_edge::swbus_proto::result::*;
use swbus_edge::swbus_proto::swbus::{swbus_message::Body, DataRequest, ServicePath, SwbusErrorCode, SwbusMessage};
use swbus_edge::SwbusEdgeRuntime;
use swss_common::{
    ConsumerStateTable, KeyOpFieldValues, KeyOperation, ProducerStateTable, SubscriberStateTable, ZmqClient,
    ZmqProducerStateTable,
};
use swss_common_bridge::{consumer::ConsumerBridge, producer::spawn_producer_bridge};
use tokio::sync::mpsc::{channel, Receiver};
use tokio::task::JoinHandle;
use tracing::{error, info};

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

        let db = crate::db_for_table::<T>().await?;

        let addr = crate::common_bridge_sp::<T>(&edge_runtime);
        let base_addr = edge_runtime.get_base_sp();
        let dest_generator = move |kfv: &KeyOpFieldValues| {
            let mut addr = base_addr.clone();
            addr.resource_type = Self::name().to_owned();
            addr.resource_id = kfv.key.clone();
            (addr, T::table_name().to_owned())
        };

        // Use ConsumerStateTable for APPL_DB, SubscriberStateTable for other DBs (sonic conventions)
        if T::db_name() == "APPL_DB" {
            let cst = ConsumerStateTable::new(db, T::table_name(), None, None)?;
            Ok(vec![ConsumerBridge::spawn::<T, _, _, _>(
                edge_runtime.clone(),
                addr,
                cst,
                dest_generator,
                |_| true,
            )])
        } else {
            let sst = SubscriberStateTable::new_async(db, T::table_name(), None, None).await?;
            Ok(vec![ConsumerBridge::spawn::<T, _, _, _>(
                edge_runtime.clone(),
                addr,
                sst,
                dest_generator,
                |_| true,
            )])
        }
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
            let msg = self.handler_rx.recv().await.unwrap();
            if let Err(swbus_err) = self.handle_received_message(&msg).await {
                let (code, err_msg) = match swbus_err {
                    SwbusError::ConnectionError { code, detail } => (code, detail.to_string()),
                    SwbusError::InternalError { code, detail } => (code, detail),
                    SwbusError::InputError { code, detail } => (code, detail),
                    SwbusError::RouteError { code, detail } => (code, detail),
                };
                let response = SwbusMessage::new_response(
                    msg.header.as_ref().unwrap(),
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

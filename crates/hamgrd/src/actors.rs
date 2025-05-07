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
use swbus_actor::{spawn, Actor, ActorMessage, State};
use swbus_edge::swbus_proto::message_id_generator::MessageIdGenerator;
use swbus_edge::swbus_proto::result::*;
use swbus_edge::swbus_proto::swbus::{swbus_message::Body, DataRequest, ServicePath, SwbusErrorCode, SwbusMessage};
use swbus_edge::SwbusEdgeRuntime;
use swss_common::{FieldValues, KeyOpFieldValues, KeyOperation};
use tokio::sync::mpsc::{channel, Receiver};

pub struct ActorCreator<F, T>
where
    F: Fn(String, &FieldValues) -> AnyhowResult<T>,
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
    F: Fn(String, &FieldValues) -> AnyhowResult<T>,
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
                self.rt.send(response).await;
            } else {
                // forward the message to the actor that is just spawned
                self.rt.send(msg).await;
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
                    let actor = (self.create_fn)(kfv.key, &kfv.field_values).map_err(|e| {
                        SwbusError::input(
                            SwbusErrorCode::InvalidPayload,
                            "cannot decode as ActorMessage".to_string(),
                        )
                    })?;
                    spawn(actor, destination.clone());
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

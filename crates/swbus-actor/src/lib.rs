pub mod resend_queue;
pub mod runtime;

use std::{future::Future, sync::Arc};
use swbus_edge::{simple_client::*, swbus_proto::swbus::SwbusMessage};
use tokio::sync::mpsc::Sender;

/// Module containing all the imports needed to write an `Actor`.
pub mod prelude {
    pub use crate::{resend_queue::ResendQueueConfig, runtime::ActorRuntime, Actor, Outbox};
    pub use swbus_edge::{
        simple_client::{IncomingMessage, MessageBody, MessageId, OutgoingMessage},
        swbus_proto::swbus::{DataRequest, RequestResponse, ServicePath, SwbusErrorCode},
        SwbusEdgeRuntime,
    };
}

/// An actor that can be run on an [`ActorRuntime`](runtime::ActorRuntime).
pub trait Actor: Send + 'static {
    /// The actor just started.
    fn init(&mut self, outbox: Outbox) -> impl Future<Output = ()> + Send;

    /// A new message came in.
    fn handle_message(&mut self, message: IncomingMessage, outbox: Outbox) -> impl Future<Output = ()> + Send;
}

/// An actor's outbox, used to send messages on Swbus.
#[derive(Clone)]
pub struct Outbox {
    /// Outgoing message sender. The receiver lives in [`runtime::MessageBridge`].
    message_tx: Sender<SwbusMessage>,

    /// We need a copy of the SimpleSwbusEdgeClient so that we can get `MessageId`s
    /// from sent messages and return them to the actor.
    swbus_client: Arc<SimpleSwbusEdgeClient>,
}

impl Outbox {
    fn new(message_tx: Sender<SwbusMessage>, swbus_client: Arc<SimpleSwbusEdgeClient>) -> Self {
        Self {
            message_tx,
            swbus_client,
        }
    }

    pub async fn send(&self, msg: OutgoingMessage) -> MessageId {
        let (id, msg) = self.swbus_client.outgoing_message_to_swbus_message(msg);
        // we ignore this result, because if the MessageBridge was shut down and this returns Err, we
        // will be notified anyway by the inbox receiver.
        _ = self.message_tx.send(msg).await;
        id
    }
}

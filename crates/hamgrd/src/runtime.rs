mod resend_queue;

use futures::future::Either;
use resend_queue::{ResendMessage, ResendQueue};
use serde::{Deserialize, Serialize};
use std::{future::Future, sync::Arc};
use swbus_edge::{SwbusEdgeRuntime, SwbusMessage, SwbusMessageHeader};
use swbus_proto::swbus::{
    swbus_message::Body as SwbusMessageBody, DataRequest, RequestResponse, ServicePath, SwbusErrorCode,
};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinSet,
};

pub use resend_queue::ResendQueueConfig;

pub trait Actor: Send + 'static {
    fn init<'a>(&'a mut self, outbox: &Outbox) -> impl Future<Output = ()> + Send + 'a;
    fn handle_message<'a>(
        &'a mut self,
        message: IncomingMessage,
        outbox: &Outbox,
    ) -> impl Future<Output = ()> + Send + 'a;
}

// A macro to translate "async fn foo()" into "fn foo() -> impl Future<Output = ()> + Send + 'static".
macro_rules! impl_actor {
    (
        impl Actor for $t:ty {
            async fn init(
                &mut self,
                $init_outbox_ident:ident : $init_outbox_ty:ty
            ) $init_body:tt

            async fn handle_message(
                &mut self,
                $handle_message_msg_ident:ident : $handle_message_msg_ty:ty,
                $handle_message_outbox_ident:ident : $handle_message_outbox_ty:ty
            ) $handle_message_body:tt
        }
    ) => {
        impl Actor for $t {
            fn init<'a>(
                &'a mut self,
                $init_outbox_ident: $init_outbox_ty,
            ) -> impl ::std::future::Future<Output = ()> + Send + 'a {
                async move { $init_body }
            }

            fn handle_message<'a>(
                &'a mut self,
                $handle_message_msg_ident: $handle_message_msg_ty,
                $handle_message_outbox_ident: $handle_message_outbox_ty,
            ) -> impl ::std::future::Future<Output = ()> + Send + 'a {
                async move { $handle_message_body }
            }
        }
    };
}
pub(crate) use impl_actor;

pub struct ActorRuntime {
    swbus_edge: Arc<SwbusEdgeRuntime>,
    resend_config: ResendQueueConfig,
    tasks: JoinSet<()>,
}

impl ActorRuntime {
    pub async fn new(swbus_uri: String, resend_config: ResendQueueConfig) -> Self {
        let mut swbus_edge = SwbusEdgeRuntime::new(swbus_uri);
        swbus_edge.start().await.expect("Starting swbus edge runtime");
        ActorRuntime {
            swbus_edge: Arc::new(swbus_edge),
            resend_config,
            tasks: JoinSet::new(),
        }
    }

    /// Spawn an actor that listens for messages on the given service_path
    pub async fn spawn<A: Actor>(&mut self, service_path: Arc<ServicePath>, actor: A) {
        const CHANNEL_SIZE: usize = 1024;
        let (inbox_tx, inbox_rx) = channel(CHANNEL_SIZE);
        let (outbox_tx, outbox_rx) = channel(CHANNEL_SIZE);
        let (swbus_tx, swbus_rx) = channel(CHANNEL_SIZE);
        self.swbus_edge
            .add_handler((*service_path).clone(), swbus_tx)
            .await
            .expect("adding handler to swbus-edge");
        let message_bridge = MessageBridge::new(
            self.resend_config,
            self.swbus_edge.clone(),
            inbox_tx,
            outbox_rx,
            swbus_rx,
        );
        let outbox = Outbox::new(service_path, outbox_tx);
        self.tasks.spawn(actor_main(actor, outbox, inbox_rx));
        self.tasks.spawn(message_bridge.run());
    }

    /// Block on all actors
    pub async fn join(self) {
        self.tasks.join_all().await;
    }
}

async fn actor_main(mut actor: impl Actor, outbox: Outbox, mut inbox: Receiver<IncomingMessage>) {
    actor.init(&outbox).await;

    // If inbox.recv() returns None, the MessageBridge died
    while let Some(msg) = inbox.recv().await {
        actor.handle_message(msg, &outbox).await;
    }
}

#[derive(Debug, Clone)]
pub struct IncomingMessage {
    id: u64,
    source: Arc<ServicePath>,
    body: MessageBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageBody {
    X,
}

/// A bridge between Swbus and actor messages, for a single actor.
struct MessageBridge {
    resend_queue: ResendQueue,

    /// Our interface to Swbus.
    swbus_edge: Arc<SwbusEdgeRuntime>,

    /// Receiver for MessageBridge to receive incoming messages from Swbus.
    /// The sender end exists in SwbusEdgeRuntime.
    swbus_rx: Receiver<SwbusMessage>,

    /// Sender for MessageBridge to send incoming messages to its actor.
    /// The receiver end is managed by the caller of `new()`.
    inbox_tx: Sender<IncomingMessage>,

    /// Receiver for MessageBridge to receive outgoing messages actor.
    /// The sender end exists in Outbox.
    outbox_rx: Receiver<SwbusMessage>,
}

impl MessageBridge {
    fn new(
        config: ResendQueueConfig,
        swbus_edge: Arc<SwbusEdgeRuntime>,
        inbox_tx: Sender<IncomingMessage>,
        outbox_rx: Receiver<SwbusMessage>,
        swbus_rx: Receiver<SwbusMessage>,
    ) -> Self {
        Self {
            resend_queue: ResendQueue::new(config),
            swbus_edge,
            outbox_rx,
            inbox_tx,
            swbus_rx,
        }
    }

    /// Message bridge main loop
    async fn run(mut self) {
        loop {
            let resend_timeout = match self.resend_queue.next_resend_instant() {
                Some(instant) => Either::Left(tokio::time::sleep_until(instant)), // Sleep until next resend time
                None => Either::Right(futures::future::pending()), // Future that never finishes and takes no resources
            };

            tokio::select! {
                maybe_msg = self.swbus_rx.recv() => {
                    // if maybe_msg is None, swbus has died
                    let Some(msg) = maybe_msg else { break };
                    self.handle_incoming_message(msg).await;
                }

                maybe_msg = self.outbox_rx.recv() => {
                    // If maybe_msg is None, the actor has died (its Outbox was dropped)
                    let Some(msg) = maybe_msg else { break };
                    self.handle_outgoing_message(msg).await;
                }

                () = resend_timeout => {
                    self.resend_pending_messages().await;
                }
            }
        }
    }

    async fn handle_incoming_message(&mut self, msg: SwbusMessage) {
        match msg.body {
            SwbusMessageBody::DataRequest(DataRequest { payload }) => {
                match serde_json::from_slice(&payload) {
                    Ok(body) => {
                        let id = msg.header.id;
                        let source = Arc::new(msg.header.source);
                        let incoming_message = IncomingMessage { id, source, body };
                        _ = self.inbox_tx.send(incoming_message).await;
                    }

                    // Payload could not be decoded - respond with invalid payload
                    Err(e) => {
                        _ = self
                            .swbus_edge
                            .send(&SwbusMessage {
                                header: SwbusMessageHeader::new(msg.header.destination, msg.header.source),
                                body: SwbusMessageBody::Response(RequestResponse {
                                    request_id: msg.header.id,
                                    error_code: SwbusErrorCode::InvalidPayload as i32,
                                    error_message: e.to_string(),
                                }),
                            })
                            .await;
                    }
                }
            }
            SwbusMessageBody::Response(RequestResponse {
                request_id,
                error_code,
                error_message: _,
            }) => {
                self.resend_queue.message_acknowledged(request_id);
                let _error_code = SwbusErrorCode::try_from(error_code).expect("unknown error code");
                todo!();
            }
            _ => { /* I don't know what to do with the other messages */ }
        }
    }

    async fn handle_outgoing_message(&mut self, msg: SwbusMessage) {
        self.swbus_edge
            .send(&msg)
            .await
            .expect("sending message with swbus-edge");
        self.resend_queue.enqueue(msg);
    }

    async fn resend_pending_messages(&mut self) {
        use ResendMessage::*;

        for resend in self.resend_queue.iter_resend() {
            match resend {
                Resend(_swbus_message) => todo!(),
                TooManyTries(_id) => todo!(),
            }
        }
    }
}

/// An actor's outbox. This is passed to actor methods for it to send messages to its `MessageBridge`.
pub struct Outbox {
    /// The id of the actor who is sending messages
    source: Arc<ServicePath>,

    /// The sender that passes messages to the MessageBridge
    tx: Sender<SwbusMessage>,
}

impl Outbox {
    fn new(source: Arc<ServicePath>, tx: Sender<SwbusMessage>) -> Self {
        Self { source, tx }
    }

    async fn send(&self, destination: Arc<ServicePath>, body: MessageBody) {
        // Convert the message data into an SwbusMessage
        let source = (*self.source).clone();
        let dest = Arc::unwrap_or_clone(destination);
        let payload = serde_json::to_vec(&body).expect("serializing message");
        let body = SwbusMessageBody::DataRequest(DataRequest { payload });
        let swbus_message = SwbusMessage {
            header: SwbusMessageHeader::new(source, dest),
            body,
        };

        // Send it to the MessageBridge.
        _ = self.tx.send(swbus_message).await;
    }
}

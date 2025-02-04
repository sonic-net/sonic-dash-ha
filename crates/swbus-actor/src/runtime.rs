use crate::{
    resend_queue::{ResendMessage, ResendQueue, ResendQueueConfig},
    Actor, Outbox,
};
use std::sync::Arc;
use swbus_edge::{
    simple_client::*,
    swbus_proto::swbus::{RequestResponse, ServicePath, SwbusMessage},
    SwbusEdgeRuntime,
};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinSet,
};

/// Struct that spawns and drives actor tasks.
pub struct ActorRuntime {
    swbus_edge: Arc<SwbusEdgeRuntime>,
    resend_config: ResendQueueConfig,
    tasks: JoinSet<()>,
}

impl ActorRuntime {
    /// Create a new Actor runtime on top of an (already started) SwbusEdgeRuntime
    pub fn new(swbus_edge: Arc<SwbusEdgeRuntime>, resend_config: ResendQueueConfig) -> Self {
        ActorRuntime {
            swbus_edge,
            resend_config,
            tasks: JoinSet::new(),
        }
    }

    /// Spawn an actor that listens for messages on the given service_path
    pub async fn spawn<A: Actor>(&mut self, service_path: ServicePath, actor: A) {
        let swbus_client = Arc::new(SimpleSwbusEdgeClient::new(self.swbus_edge.clone(), service_path).await);
        let (inbox_tx, inbox_rx) = channel(1024);
        let (outbox_tx, outbox_rx) = channel(1024);
        let message_bridge = MessageBridge::new(self.resend_config, swbus_client.clone(), inbox_tx, outbox_rx);
        let outbox = Outbox::new(outbox_tx, swbus_client);
        self.tasks.spawn(message_bridge.run());
        self.tasks.spawn(actor_main(actor, inbox_rx, outbox));
    }

    /// Block on all actors
    pub async fn join(self) {
        self.tasks.join_all().await;
    }
}

/// Main loop for an actor task
async fn actor_main(mut actor: impl Actor, mut inbox_rx: Receiver<IncomingMessage>, outbox: Outbox) {
    actor.init(outbox.clone()).await;

    // If inbox.recv() returns None, the MessageBridge died
    while let Some(msg) = inbox_rx.recv().await {
        actor.handle_message(msg, outbox.clone()).await;
    }
}

/// A bridge between Swbus and an actor, providing middleware (currently just the resend queue).
pub(crate) struct MessageBridge {
    resend_queue: ResendQueue,

    swbus_client: Arc<SimpleSwbusEdgeClient>,

    /// Sender for MessageBridge to send incoming messages or message failure signals to its actor.
    /// The receiver exists in actor_main.
    inbox_tx: Sender<IncomingMessage>,

    /// Receiver for MessageBridge to receive outgoing messages actor.
    /// The sender end exists in Outbox.
    outbox_rx: Receiver<SwbusMessage>,
}

impl MessageBridge {
    fn new(
        resend_queue_config: ResendQueueConfig,
        swbus_client: Arc<SimpleSwbusEdgeClient>,
        inbox_tx: Sender<IncomingMessage>,
        outbox_rx: Receiver<SwbusMessage>,
    ) -> Self {
        Self {
            resend_queue: ResendQueue::new(resend_queue_config),
            swbus_client,
            inbox_tx,
            outbox_rx,
        }
    }

    /// Message bridge main loop
    async fn run(mut self) {
        loop {
            tokio::select! {
                maybe_msg = self.swbus_client.recv() => {
                    // if maybe_msg is None, swbus has died
                    let Some(msg) = maybe_msg else { break };
                    self.handle_incoming_message(msg).await;
                }

                maybe_msg = self.outbox_rx.recv() => {
                    // If maybe_msg is None, the actor has died (its Outbox was dropped)
                    let Some(msg) = maybe_msg else { break };
                    self.handle_outgoing_message(msg).await;
                }

                () = self.resend_queue.wait() => {
                    self.resend_pending_messages().await;
                }
            }
        }
    }

    async fn handle_incoming_message(&mut self, msg: IncomingMessage) {
        if let MessageBody::Response(RequestResponse { request_id, .. }) = &msg.body {
            self.resend_queue.message_acknowledged(*request_id);
        }
        self.inbox_tx.send(msg).await.unwrap();
    }

    async fn handle_outgoing_message(&mut self, msg: SwbusMessage) {
        self.resend_queue.enqueue(msg.clone());
        self.swbus_client.send_raw(msg).await.unwrap();
    }

    async fn resend_pending_messages(&mut self) {
        use ResendMessage::*;

        for resend in self.resend_queue.iter_resend() {
            match resend {
                Resend(swbus_msg) => self.swbus_client.send_raw((*swbus_msg).clone()).await.unwrap(),
                TooManyTries { id, destination } => {
                    eprintln!("Message {id} to {destination} was dropped");
                }
            }
        }
    }
}

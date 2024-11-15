mod resend_queue;

use resend_queue::{ResendMessage, ResendQueue};
use std::{future::Future, sync::Arc};
use swbus_edge::{simple_client::*, SwbusEdgeRuntime};
use tokio::{
    sync::mpsc::{channel, Receiver, Sender},
    task::JoinSet,
};

pub use resend_queue::ResendQueueConfig;

pub trait Actor: Send + 'static {
    /// The actor just started.
    fn init(&mut self, outbox: Outbox) -> impl Future<Output = ()> + Send;

    /// A new message came in.
    fn handle_message(&mut self, message: IncomingMessage, outbox: Outbox) -> impl Future<Output = ()> + Send;

    /// The message with this id was never acknowledged by the receiving party.
    fn handle_message_failure(&mut self, id: MessageId, outbox: Outbox) -> impl Future<Output = ()> + Send;
}

/// An actor's outbox. This is passed to actor methods for it to send messages to its `MessageBridge`.
#[derive(Clone)]
pub struct Outbox {
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

async fn actor_main(mut actor: impl Actor, mut inbox_rx: Receiver<InboxMessage>, outbox: Outbox) {
    actor.init(outbox.clone()).await;

    // If inbox.recv() returns None, the MessageBridge died
    while let Some(msg) = inbox_rx.recv().await {
        match msg {
            InboxMessage::Message(msg) => actor.handle_message(msg, outbox.clone()).await,
            InboxMessage::MessageFailure(id) => actor.handle_message_failure(id, outbox.clone()).await,
        }
    }
}

/// Messages sent from MessageBridge to an actor.
/// These messages trigger callbacks from the Actor trait.
enum InboxMessage {
    Message(IncomingMessage),
    MessageFailure(MessageId),
}

/// A bridge between Swbus and actor messages, for a single actor.
struct MessageBridge {
    resend_queue: ResendQueue,
    swbus_client: Arc<SimpleSwbusEdgeClient>,

    /// Sender for MessageBridge to send incoming messages or message failure signals to its actor.
    /// The receiver exists in actor_main.
    inbox_tx: Sender<InboxMessage>,

    /// Receiver for MessageBridge to receive outgoing messages actor.
    /// The sender end exists in Outbox.
    outbox_rx: Receiver<SwbusMessage>,
}

impl MessageBridge {
    fn new(
        resend_queue_config: ResendQueueConfig,
        swbus_client: Arc<SimpleSwbusEdgeClient>,
        inbox_tx: Sender<InboxMessage>,
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
        self.inbox_tx.send(InboxMessage::Message(msg)).await.unwrap();
    }

    async fn handle_outgoing_message(&mut self, msg: SwbusMessage) {
        self.resend_queue.enqueue(msg.clone());
        self.swbus_client.send_raw(msg).await.unwrap();
    }

    async fn resend_pending_messages(&mut self) {
        use ResendMessage::*;

        for resend in self.resend_queue.iter_resend() {
            match resend {
                Resend(swbus_msg) => self.swbus_client.send_raw((**swbus_msg).clone()).await.unwrap(),
                TooManyTries(id) => self.inbox_tx.send(InboxMessage::MessageFailure(id)).await.unwrap(),
            }
        }
    }
}

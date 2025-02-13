#![allow(unused)]
use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use swbus_edge::{
    simple_client::{IncomingMessage, MessageBody, MessageId, OutgoingMessage, SimpleSwbusEdgeClient},
    swbus_proto::swbus::{ServicePath, SwbusErrorCode, SwbusMessage},
    SwbusEdgeRuntime,
};
use tokio::{
    task::JoinHandle,
    time::{interval, Interval, MissedTickBehavior},
};

/// The core trait that must be implemented to spawn an actor.
pub trait Actor: Send + 'static {
    fn init(&mut self, outbox: &mut Outbox) -> impl Future<Output = Result<ControlFlow, Error>> + Send + 'static;

    fn handle_reqest(
        &mut self,
        outbox: &mut Outbox,
        payload: &[u8],
        source: &ServicePath,
    ) -> impl Future<Output = Result<ControlFlow, Error>> + Send + 'static;
}

/// Queue of outgoing messages that are sent if the function finishes successfully.
pub struct Outbox {
    msgs: Vec<OutgoingMessage>,
}

impl Outbox {
    /// Enqueue a message for sending, if `handle_request`/`init` succeed.
    pub fn send(&mut self, payload: Vec<u8>, destination: ServicePath) {
        self.msgs.push(OutgoingMessage {
            destination,
            body: MessageBody::Request { payload },
        })
    }
}

/// If an actor should `Continue` or `Stop` running.
#[derive(PartialEq, Eq, Clone, Copy)]
pub enum ControlFlow {
    /// Continue receiving requests.
    Continue,

    /// Stop receiving requests. Unacked messages will continue to be retried.
    Stop,
}

/// `Box<dyn std::error::Error + Send + 'static>`
pub type Error = Box<dyn std::error::Error + Send + 'static>;

/// An actor and the support structures needed to run it.
struct ActorDriver<A> {
    actor: A,
    swbus: SimpleSwbusEdgeClient,
    maintenence_interval: Interval,
    unacked_messages: HashMap<MessageId, UnackedMessage>,
    stopped: bool,
    outbox: Outbox,
}

impl<A: Actor> ActorDriver<A> {
    fn new(actor: A, swbus: SimpleSwbusEdgeClient) -> Self {
        let mut maintenence_interval = interval(Duration::from_secs(60));
        maintenence_interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ActorDriver {
            actor,
            swbus,
            maintenence_interval,
            unacked_messages: HashMap::new(),
            stopped: false,
            outbox: Outbox { msgs: Vec::new() },
        }
    }

    /// Run the actor's main loop
    async fn run(mut self) {
        while !(self.stopped && self.unacked_messages.is_empty()) {
            tokio::select! {
                maybe_msg = self.swbus.recv(), if !self.stopped => {
                    let msg = maybe_msg.expect("Swbus is down, killing actor");
                    self.handle_message(msg).await
                }
                _ = self.maintenence_interval.tick() => self.maintenence().await,
            }
        }
    }

    /// Handle an incoming message - request or response
    async fn handle_message(&mut self, msg: IncomingMessage) {
        match msg.body {
            MessageBody::Request { payload } => {
                self.handle_request(msg.id, msg.source, payload).await;
            }
            MessageBody::Response {
                request_id,
                error_code,
                error_message,
            } => {
                if error_code == SwbusErrorCode::Ok {
                    self.unacked_messages.remove(&request_id);
                } else {
                    eprintln!(
                        "Request {} to {} error: swbus error code {}: {}",
                        request_id, msg.source, error_code, error_message
                    );
                }
            }
        }
    }

    /// Handle an incoming request, triggering `Actor::handle_request`.
    async fn handle_request(&mut self, request_id: MessageId, source: ServicePath, payload: Vec<u8>) {
        // Call actor callback
        let res = self.actor.handle_reqest(&mut self.outbox, &payload, &source).await;

        // Generate response based on callback result
        let (error_code, error_message) = match res {
            Ok(cf) => {
                if cf == ControlFlow::Stop {
                    self.stopped = true;
                    // TODO: Unsubscribe from swbus address
                }
                (SwbusErrorCode::Ok, String::new())
            }
            Err(e) => (SwbusErrorCode::Fail, e.to_string()),
        };

        if error_code == SwbusErrorCode::Ok {
            // Send requests queued in the outbox
            for msg in self.outbox.msgs.drain(..) {
                let (id, msg) = self.swbus.outgoing_message_to_swbus_message(msg);
                let unacked_message = UnackedMessage {
                    msg: msg.clone(),
                    sent_at: Instant::now(),
                };
                self.unacked_messages.insert(id, unacked_message);
                self.swbus.send_raw(msg).await.expect("Sending message failed");
            }
        } else {
            // Just drop the messages
            self.outbox.msgs.clear();
        }

        // Send the response
        self.swbus
            .send(OutgoingMessage {
                destination: source,
                body: MessageBody::Response {
                    request_id,
                    error_code,
                    error_message,
                },
            })
            .await
            .expect("Sending message failed");
    }

    /// Run a maintenence pass
    async fn maintenence(&mut self) {
        // Drop messages that have been unacked for over an hour, as a memory leak failsafe
        self.unacked_messages
            .retain(|_, msg| msg.sent_at.elapsed() < Duration::from_secs(3600));

        // Resend unacked messages
        for UnackedMessage { msg, .. } in self.unacked_messages.values() {
            self.swbus.send_raw(msg.clone()).await.expect("Sending message failed");
        }
    }
}

/// A sent message that has not received an Ok response.
///
/// Both an error response and no response at all are considered unacked.
struct UnackedMessage {
    msg: SwbusMessage,
    sent_at: Instant,
}

/// Global structures shared by all actors.
pub struct ActorRuntime {
    swbus_edge: Arc<SwbusEdgeRuntime>,
}

impl ActorRuntime {
    pub fn new(swbus_edge: Arc<SwbusEdgeRuntime>) -> Self {
        Self { swbus_edge }
    }

    /// Spawn an actor on this runtime, reachable by sending Swbus requests to `addr`.
    pub fn spawn<A: Actor>(&self, actor: A, addr: ServicePath) {
        let swbus_client = SimpleSwbusEdgeClient::new(self.swbus_edge.clone(), addr);
        let actor_driver = ActorDriver::new(actor, swbus_client);
        tokio::task::spawn(actor_driver.run());
    }
}

// Global actor runtime for using `actor::spawn`, similar to `tokio::spawn`.
static GLOBAL_RUNTIME: RwLock<Option<ActorRuntime>> = RwLock::new(None);

/// Set the global [`ActorRuntime`] for [`spawn`].
pub fn set_global_runtime(rt: ActorRuntime) {
    *GLOBAL_RUNTIME.write().unwrap() = Some(rt);
}

/// Spawn an actor on the global runtime.
///
/// Panics if called before [`set_global_runtime`] is called.
pub fn spawn<A: Actor>(actor: A, addr: ServicePath) {
    GLOBAL_RUNTIME
        .read()
        .unwrap()
        .as_ref()
        .expect("You must call actor::set_global_runtime() before calling actor::spawn()")
        .spawn(actor, addr);
}

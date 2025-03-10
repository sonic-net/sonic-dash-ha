#![allow(unused)]
pub mod actor_message;
pub mod state;

use std::{
    future::Future,
    sync::{Arc, RwLock},
    time::Instant,
};
use swbus_edge::{
    simple_client::{IncomingMessage, MessageBody, OutgoingMessage, SimpleSwbusEdgeClient},
    swbus_proto::swbus::{ServicePath, SwbusErrorCode, SwbusMessage},
    SwbusEdgeRuntime,
};

pub use actor_message::ActorMessage;
pub use anyhow::{Error, Result};
pub use serde_json as json;
pub use state::State;

/// An actor that can be run.
pub trait Actor: Send + 'static {
    /// Callback run upon spawn. Allows actors to setup the internal state table and send initial messages to get started.
    ///
    /// If this returns `Err(..)`, the actor dies immediately and cannot receive messages.
    ///
    /// The default implementation does nothing.
    fn init(&mut self, state: &mut State) -> impl Future<Output = Result<()>> + Send {
        _ = state;
        async { Ok(()) }
    }

    /// Callback run upon receipt of an [`ActorMessage`].
    ///
    /// If this returns `Err(..)`, state changes are not committed.
    /// Outgoing state messages are not sent, and internal state changes are rolled back.
    fn handle_message(&mut self, state: &mut State, key: &str) -> impl Future<Output = Result<()>> + Send;
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

/// An actor and the support structures needed to run it.
struct ActorDriver<A> {
    actor: A,
    state: State,
    swbus_edge: Arc<SimpleSwbusEdgeClient>,
}

impl<A: Actor> ActorDriver<A> {
    fn new(actor: A, swbus_edge: SimpleSwbusEdgeClient) -> Self {
        let swbus_edge = Arc::new(swbus_edge);
        ActorDriver {
            actor,
            state: State::new(swbus_edge.clone()),
            swbus_edge,
        }
    }

    /// Run the actor's main loop
    async fn run(mut self) {
        self.actor.init(&mut self.state).await.unwrap();
        self.state.internal.commit_changes().await;
        self.state.outgoing.send_queued_messages().await;

        loop {
            tokio::select! {
                _ = self.state.outgoing.drive_resend_loop() => unreachable!("drive_resend_loop never returns"),
                // received = self.state.incoming.recv() => match received {
                //     Received::ActorMessage { key } => self.handle_message(&key).await,
                //     Received::Ack { id } => self.state.outgoing.ack_message(id),
                // }
                maybe_msg = self.swbus_edge.recv() => self.handle_swbus_message(maybe_msg.expect("swbus-edge died")).await,
            }
        }
    }

    async fn handle_swbus_message(&mut self, msg: IncomingMessage) {
        let IncomingMessage { id, source, body } = msg;
        match body {
            MessageBody::Request { payload } => {
                match self.state.incoming.handle_request(id, source, &payload).await {
                    Ok(key) => self.handle_actor_message(&key).await,
                    // TODO: This is an obscure error scenario, do we handle it?
                    Err(e) => eprintln!("Incoming state table failed to handle request: {e:#}"),
                }
            }
            MessageBody::Response {
                request_id,
                error_code,
                error_message,
            } => {
                if error_code == SwbusErrorCode::Ok {
                    self.state.outgoing.ack_message(request_id);
                } else {
                    // TODO: What to do with error messages?
                    eprintln!("error response to {request_id}: ({error_code:?}) {error_message}");
                }
            }
        }
    }

    /// Handle an actor message in the incoming state table, triggering `Actor::handle_message`.
    async fn handle_actor_message(&mut self, key: &str) {
        let res = self.actor.handle_message(&mut self.state, key).await;
        let (error_code, error_message) = match res {
            Ok(()) => {
                self.state.internal.commit_changes().await;
                self.state.outgoing.send_queued_messages().await;
                (SwbusErrorCode::Ok, String::new())
            }
            Err(e) => {
                self.state.internal.drop_changes();
                self.state.outgoing.drop_queued_messages();
                (SwbusErrorCode::Fail, format!("{e:#}"))
            }
        };

        let entry = self.state.incoming.get_entry(key).unwrap();

        self.swbus_edge
            .send(OutgoingMessage {
                destination: entry.source.clone(),
                body: MessageBody::Response {
                    request_id: entry.request_id,
                    error_code,
                    error_message,
                },
            })
            .await
            .expect("failed to send swbus message");
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

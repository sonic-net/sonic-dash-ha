use crate::{Actor, Context, State};
use std::sync::Arc;
use swbus_edge::{
    simple_client::{IncomingMessage, MessageBody, OutgoingMessage, SimpleSwbusEdgeClient},
    swbus_proto::swbus::{ServicePath, SwbusErrorCode},
};
use tracing::info;

/// An actor and the support structures needed to run it.
pub(crate) struct ActorDriver<A> {
    actor: A,
    state: State,
    swbus_edge: Arc<SimpleSwbusEdgeClient>,
    context: Context,
}

impl<A: Actor> ActorDriver<A> {
    pub(crate) fn new(actor: A, swbus_edge: SimpleSwbusEdgeClient) -> Self {
        let swbus_edge = Arc::new(swbus_edge);
        ActorDriver {
            actor,
            state: State::new(swbus_edge.clone()),
            swbus_edge,
            context: Context::new(),
        }
    }

    /// Run the actor's main loop
    pub(crate) async fn run(mut self) {
        self.actor.init(&mut self.state).await.unwrap();
        self.state.internal.commit_changes().await;
        self.state.outgoing.send_queued_messages().await;

        loop {
            tokio::select! {
                _ = self.state.outgoing.drive_resend_loop() => unreachable!("drive_resend_loop never returns"),
                maybe_msg = self.swbus_edge.recv() => self.handle_swbus_message(maybe_msg.expect("swbus-edge died")).await,
            }
            if self.context.stopped {
                info!(
                    "actor {} terminated",
                    self.swbus_edge.get_service_path().to_longest_path()
                );
                break;
            }
        }
    }

    async fn handle_swbus_message(&mut self, msg: IncomingMessage) {
        let IncomingMessage { id, source, body, .. } = msg;
        match body {
            MessageBody::Request { payload } => {
                match self.state.incoming.handle_request(id, source.clone(), &payload).await {
                    Ok(key) => self.handle_actor_message(&key).await,
                    Err(e) => eprintln!("Incoming state table failed to handle request: {e:#}"),
                }
            }
            MessageBody::Response {
                request_id,
                error_code,
                error_message,
            } => self
                .state
                .outgoing
                .handle_response(request_id, error_code, &error_message, source),
        }
    }

    /// Handle an actor message in the incoming state table, triggering `Actor::handle_message`.
    async fn handle_actor_message(&mut self, key: &str) {
        let res = self.actor.handle_message(&mut self.state, key, &mut self.context).await;

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

        let (request_id, destination) = self.state.incoming.request_handled(key, error_code, &error_message);

        self.swbus_edge
            .send(OutgoingMessage {
                destination,
                body: MessageBody::Response {
                    request_id,
                    error_code,
                    error_message,
                },
            })
            .await
            .expect("failed to send swbus message");
    }
}

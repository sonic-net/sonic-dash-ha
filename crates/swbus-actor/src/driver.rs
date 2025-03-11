use crate::{Actor, State};
use std::sync::Arc;
use swbus_edge::{
    simple_client::{IncomingMessage, MessageBody, OutgoingMessage, SimpleSwbusEdgeClient},
    swbus_proto::swbus::SwbusErrorCode,
};

/// An actor and the support structures needed to run it.
pub(crate) struct ActorDriver<A> {
    actor: A,
    state: State,
    swbus_edge: Arc<SimpleSwbusEdgeClient>,
}

impl<A: Actor> ActorDriver<A> {
    pub(crate) fn new(actor: A, swbus_edge: SimpleSwbusEdgeClient) -> Self {
        let swbus_edge = Arc::new(swbus_edge);
        ActorDriver {
            actor,
            state: State::new(swbus_edge.clone()),
            swbus_edge,
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

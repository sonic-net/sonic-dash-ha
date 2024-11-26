//! This module contains a simplified wrapper around [`SwbusEdgeRuntime`] that does not expose
//! infra messages, message id generation, and other internal details to clients.

use crate::SwbusEdgeRuntime;
use std::sync::Arc;
use swbus_proto::{
    message_id_generator::MessageIdGenerator,
    result::Result,
    swbus::{swbus_message::Body, SwbusMessageHeader, TraceRouteRequest, TraceRouteResponse},
};
use tokio::sync::{
    mpsc::{channel, Receiver},
    Mutex,
};

pub use swbus_proto::swbus::{DataRequest, RequestResponse, ServicePath, SwbusErrorCode, SwbusMessage};

pub type MessageId = u64;

pub struct SimpleSwbusEdgeClient {
    rt: Arc<SwbusEdgeRuntime>,
    handler_rx: Mutex<Receiver<SwbusMessage>>,
    source: ServicePath,
    id_generator: MessageIdGenerator,
}

impl SimpleSwbusEdgeClient {
    pub async fn new(rt: Arc<SwbusEdgeRuntime>, source: ServicePath) -> Self {
        let (handler_tx, handler_rx) = channel::<SwbusMessage>(crate::edge_runtime::SWBUS_RECV_QUEUE_SIZE);
        rt.add_handler(source.clone(), handler_tx)
            .await
            .expect("failed to add handler to SwbusEdgeRuntime");
        Self {
            rt,
            handler_rx: Mutex::new(handler_rx),
            source,
            id_generator: MessageIdGenerator::new(),
        }
    }

    /// Receive the next incoming message from Swbus.
    ///
    /// Returns None when no more messages will ever be received.
    pub async fn recv(&self) -> Option<IncomingMessage> {
        loop {
            let msg = self.handler_rx.lock().await.recv().await?;
            match self.handle_received_message(msg) {
                HandleReceivedMessage::PassToActor(msg) => break Some(msg),
                HandleReceivedMessage::Respond(msg) => self.rt.send(msg).await.unwrap(),
                HandleReceivedMessage::Ignore => {}
            }
        }
    }

    fn handle_received_message(&self, msg: SwbusMessage) -> HandleReceivedMessage {
        let header = msg.header.unwrap();
        let id = header.id;
        let source = header.source.unwrap();
        let destination = header.destination.unwrap();
        let body = msg.body.unwrap();

        match body {
            Body::DataRequest(req) => HandleReceivedMessage::PassToActor(IncomingMessage {
                id,
                source,
                body: MessageBody::Request(req),
            }),
            Body::Response(resp) => HandleReceivedMessage::PassToActor(IncomingMessage {
                id,
                source,
                body: MessageBody::Response(resp),
            }),
            Body::PingRequest(_) => HandleReceivedMessage::Respond(SwbusMessage::new(
                SwbusMessageHeader::new(destination, source, self.id_generator.generate()),
                Body::Response(RequestResponse::ok(id)),
            )),
            Body::TraceRouteRequest(TraceRouteRequest { trace_id }) => {
                HandleReceivedMessage::Respond(SwbusMessage::new(
                    SwbusMessageHeader::new(destination, source, self.id_generator.generate()),
                    Body::TraceRouteResponse(TraceRouteResponse { trace_id }),
                ))
            }
            _ => HandleReceivedMessage::Ignore,
        }
    }

    /// Send an [`OutgoingMessage`].
    ///
    /// Shortcut for [`outgoing_message_to_swbus_message`] followed by [`send_raw`].
    pub async fn send(&self, msg: OutgoingMessage) -> Result<MessageId> {
        let (id, msg) = self.outgoing_message_to_swbus_message(msg);
        self.rt.send(msg).await?;
        Ok(id)
    }

    /// Send a raw [`SwbusMessage`] created with [`outgoing_message_to_swbus_message`].
    ///
    /// This is used to implement message resending - repeating a message with the same id as the last time it was sent.
    pub async fn send_raw(&self, msg: SwbusMessage) -> Result<()> {
        self.rt.send(msg).await
    }

    /// Compile an [`OutgoingMessage`] into an [`SwbusMessage`] for use with [`send_raw`].
    pub fn outgoing_message_to_swbus_message(&self, msg: OutgoingMessage) -> (MessageId, SwbusMessage) {
        let id = self.id_generator.generate();
        let msg = SwbusMessage {
            header: Some(SwbusMessageHeader::new(self.source.clone(), msg.destination, id)),
            body: Some(match msg.body {
                MessageBody::Request(req) => Body::DataRequest(req),
                MessageBody::Response(resp) => Body::Response(resp),
            }),
        };
        (id, msg)
    }
}

enum HandleReceivedMessage {
    PassToActor(IncomingMessage),
    Respond(SwbusMessage),
    Ignore,
}

/// A simplified version of [`swbus_message::Body`], only representing a `DataRequest` or `Response`.
#[derive(Debug, Clone)]
pub enum MessageBody {
    Request(DataRequest),
    Response(RequestResponse),
}

#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub id: MessageId,
    pub source: ServicePath,
    pub body: MessageBody,
}

#[derive(Debug, Clone)]
pub struct OutgoingMessage {
    pub destination: ServicePath,
    pub body: MessageBody,
}

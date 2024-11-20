//! This module contains a simplified wrapper around [`SwbusEdgeRuntime`] that does not expose
//! infra messages, message id generation, and other internal details to clients.

use crate::SwbusEdgeRuntime;
use std::sync::Arc;
use swbus_proto::{
    result::Result,
    swbus::{swbus_message::Body, SwbusMessage, SwbusMessageHeader, TraceRouteRequest, TraceRouteResponse},
    util::SwbusMessageIdGenerator,
};
use tokio::sync::mpsc::{channel, Receiver};

pub use swbus_proto::swbus::{DataRequest, RequestResponse, ServicePath, SwbusErrorCode, SwbusMessageId};

pub struct SimpleSwbusClient {
    rt: Arc<SwbusEdgeRuntime>,
    handler_rx: Receiver<SwbusMessage>,
    source: ServicePath,
    id_generator: SwbusMessageIdGenerator,
}

impl SimpleSwbusClient {
    pub async fn new(rt: Arc<SwbusEdgeRuntime>, source: ServicePath) -> Self {
        let (handler_tx, handler_rx) = channel::<SwbusMessage>(crate::edge_runtime::SWBUS_RECV_QUEUE_SIZE);
        rt.add_handler(source.clone(), handler_tx)
            .await
            .expect("failed to add handler to SwbusEdgeRuntime");
        Self {
            rt,
            handler_rx,
            source,
            id_generator: SwbusMessageIdGenerator::new(),
        }
    }

    /// Returns None when no more messages will ever be received.
    pub async fn recv(&mut self) -> Option<IncomingMessage> {
        loop {
            let msg = self.handler_rx.recv().await?;
            let header = msg.header.unwrap();
            let id = header.id.unwrap();
            let source = header.source.unwrap();
            let destination = header.destination.unwrap();
            let body = msg.body.unwrap();

            match body {
                Body::DataRequest(req) => {
                    return Some(IncomingMessage {
                        id,
                        source,
                        body: MessageBody::Request(req),
                    });
                }
                Body::Response(resp) => {
                    return Some(IncomingMessage {
                        id,
                        source,
                        body: MessageBody::Response(resp),
                    });
                }
                Body::PingRequest(_) => {
                    _ = self
                        .rt
                        .send(SwbusMessage {
                            header: Some(SwbusMessageHeader::new(
                                destination,
                                source,
                                self.id_generator.generate(),
                            )),
                            body: Some(Body::Response(RequestResponse {
                                request_id: Some(id),
                                error_code: SwbusErrorCode::Ok as i32,
                                error_message: "".into(),
                            })),
                        })
                        .await;
                }
                Body::TraceRouteRequest(TraceRouteRequest { trace_id }) => {
                    _ = self
                        .rt
                        .send(SwbusMessage {
                            header: Some(SwbusMessageHeader::new(
                                destination,
                                source,
                                self.id_generator.generate(),
                            )),
                            body: Some(Body::TraceRouteResponse(TraceRouteResponse { trace_id })),
                        })
                        .await;
                }
                Body::TraceRouteResponse(_) => { /* We should never receive this message */ }
                Body::RegistrationQueryRequest(_) => { /* What does this message mean */ }
                Body::RegistrationQueryResponse(_) => { /* We should never receive this message */ }
            }
        }
    }

    pub async fn send(&self, destination: ServicePath, body: MessageBody) -> Result<SwbusMessageId> {
        let id = self.id_generator.generate();
        let msg = SwbusMessage {
            header: Some(SwbusMessageHeader::new(self.source.clone(), destination, id)),
            body: Some(match body {
                MessageBody::Request(req) => Body::DataRequest(req),
                MessageBody::Response(resp) => Body::Response(resp),
            }),
        };
        self.rt.send(msg).await?;
        Ok(id)
    }
}

/// A simplified version of [`swbus_message::Body`], only representing a `DataRequest` or `Response`.
pub enum MessageBody {
    Request(DataRequest),
    Response(RequestResponse),
}

pub struct IncomingMessage {
    pub id: SwbusMessageId,
    pub source: ServicePath,
    pub body: MessageBody,
}

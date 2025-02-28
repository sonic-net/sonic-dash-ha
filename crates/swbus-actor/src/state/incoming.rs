use crate::actor_message::{ActorMessage, Value};
use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    sync::Arc,
};
use swbus_edge::{
    simple_client::{IncomingMessage, MessageBody, MessageId, OutgoingMessage, SimpleSwbusEdgeClient},
    swbus_proto::swbus::{ServicePath, SwbusErrorCode},
};

pub struct Incoming {
    swbus_edge: Arc<SimpleSwbusEdgeClient>,
    table: HashMap<String, IncomingTableEntry>,
}

impl Incoming {
    pub fn get(&self, key: &str) -> Option<&ActorMessage> {
        self.get_entry(key).map(|entry| &entry.msg)
    }

    pub fn get_entry(&self, key: &str) -> Option<&IncomingTableEntry> {
        self.table.get(key)
    }

    pub(crate) fn new(swbus_edge: Arc<SimpleSwbusEdgeClient>) -> Self {
        Self {
            swbus_edge,
            table: HashMap::new(),
        }
    }

    fn insert(&mut self, msg: ActorMessage, source: ServicePath, request_id: MessageId) {
        match self.table.get_mut(&msg.key) {
            Some(entry) => {
                entry.version += 1;
                entry.msg = msg;
                entry.source = source;
                entry.request_id = request_id;
            }
            None => {
                self.table.insert(
                    msg.key.clone(),
                    IncomingTableEntry {
                        version: 1,
                        msg,
                        source,
                        request_id,
                    },
                );
            }
        }
    }

    pub(crate) async fn recv(&mut self) -> Received {
        loop {
            let IncomingMessage { id, source, body } = self.swbus_edge.recv().await.expect("swbus-edge died");

            match body {
                MessageBody::Request { payload } => match ActorMessage::deserialize(&payload) {
                    Ok(actor_msg) => {
                        let key = actor_msg.key.clone();
                        self.insert(actor_msg, source.clone(), id);
                        break Received::ActorMessage { key };
                    }
                    Err(e) => {
                        self.swbus_edge
                            .send(OutgoingMessage {
                                destination: source,
                                body: MessageBody::Response {
                                    request_id: id,
                                    error_code: SwbusErrorCode::InvalidPayload,
                                    error_message: format!("{e:#}"),
                                },
                            })
                            .await
                            .expect("failed to send swbus message");
                    }
                },
                MessageBody::Response {
                    request_id,
                    error_code,
                    error_message,
                } => {
                    if error_code == SwbusErrorCode::Ok {
                        break Received::Ack { id: request_id };
                    } else {
                        // TODO: what to do with error responses?
                        eprintln!("error response to {request_id}: ({error_code:?}) {error_message}");
                    }
                }
            }
        }
    }
}

pub(crate) enum Received {
    /// Received an ActorMessage, which is now available in `Incoming` at this `key`.
    ActorMessage { key: String },

    /// Received a successful response to a previous outgoing request. This should be passed to `Outgoing`.
    Ack { id: MessageId },
}

pub struct IncomingTableEntry {
    pub msg: ActorMessage,
    pub source: ServicePath,
    pub request_id: MessageId,
    pub version: u64,
}

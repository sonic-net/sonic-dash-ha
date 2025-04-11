use crate::actor_message::ActorMessage;
use anyhow::{anyhow, Result};
use std::{collections::HashMap, sync::Arc};
use swbus_cli_data::hamgr::actor_state::ActorMessage as ActorMessageDump;
use swbus_cli_data::hamgr::actor_state::IncomingStateEntry;
use swbus_edge::{
    simple_client::{MessageBody, MessageId, OutgoingMessage, SimpleSwbusEdgeClient},
    swbus_proto::swbus::{ServicePath, SwbusErrorCode},
};

/// Incoming state table - messages from other actors identified by a string key.
pub struct Incoming {
    swbus_edge: Arc<SimpleSwbusEdgeClient>,
    table: HashMap<String, IncomingTableEntry>,
}

impl Incoming {
    pub fn get(&self, key: &str) -> Result<&ActorMessage> {
        self.get_entry(key).map(|entry| &entry.msg)
    }

    pub fn get_entry(&self, key: &str) -> Result<&IncomingTableEntry> {
        self.table
            .get(key)
            .ok_or_else(|| anyhow!("Incoming state table has no key '{key}'"))
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

    /// Extracts the ActorMessage from a request and inserts it into the table,
    /// and returns a clone of the key to pass to the actor callback.
    pub(crate) async fn handle_request(
        &mut self,
        id: MessageId,
        source: ServicePath,
        payload: &[u8],
    ) -> Result<String> {
        match ActorMessage::deserialize(payload) {
            Ok(actor_msg) => {
                let key = actor_msg.key.clone();
                self.insert(actor_msg, source.clone(), id);
                Ok(key)
            }
            Err(e) => {
                self.swbus_edge
                    .send(OutgoingMessage {
                        destination: source,
                        body: MessageBody::Response {
                            request_id: id,
                            error_code: SwbusErrorCode::InvalidPayload,
                            error_message: format!("{e:#}"),
                            response_body: None,
                        },
                    })
                    .await
                    .expect("invalid ActorMessage received, but failed to send error response swbus message");

                Err(e.context("invalid ActorMessage received"))
            }
        }
    }

    pub(crate) fn dump_state(&self) -> Vec<IncomingStateEntry> {
        self.table
            .iter()
            .map(|(key, entry)| IncomingStateEntry {
                key: key.clone(),
                version: entry.version,
                message: ActorMessageDump {
                    key: entry.msg.key.clone(),
                    data: format!("{:#}", entry.msg.data),
                },
                source: entry.source.to_longest_path(),
                request_id: entry.request_id,
            })
            .collect()
    }
}

pub struct IncomingTableEntry {
    pub msg: ActorMessage,
    pub source: ServicePath,
    pub request_id: MessageId,
    pub version: u64,
}

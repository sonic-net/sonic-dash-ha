use crate::actor_message::ActorMessage;
use anyhow::{anyhow, Result};
use std::{collections::HashMap, sync::Arc};
use swbus_cli_data::hamgr::actor_state::ActorMessage as ActorMessageDump;
use swbus_cli_data::hamgr::actor_state::IncomingStateEntry;
use swbus_edge::{
    simple_client::{MessageBody, MessageId, OutgoingMessage, SimpleSwbusEdgeClient},
    swbus_proto::swbus::{ServicePath, SwbusErrorCode},
};

use super::get_unix_time;

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

    /// Inserts an actor message (and associated metadata) into the incoming table.
    fn insert(&mut self, msg: ActorMessage, source: ServicePath, request_id: MessageId) {
        match self.table.get_mut(&msg.key) {
            Some(entry) => entry.update_received(msg, source, request_id),
            None => {
                let key = msg.key.clone();
                self.table.insert(key, IncomingTableEntry::new(msg, source, request_id));
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

    /// Updates the incoming table with data about the response to the most recent request.
    /// Called by `ActorDriver` after the actor has handled the message.
    /// Returns data for the actor driver to route the response.
    pub(crate) fn request_handled(
        &mut self,
        key: &str,
        error_code: SwbusErrorCode,
        error_message: &str,
    ) -> (MessageId, ServicePath) {
        let entry = self.table.get_mut(key).unwrap();
        entry.update_handled(error_code, error_message);
        (entry.request_id, entry.source.clone())
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
                created_time: entry.created_time,
                last_updated_time: entry.last_updated_time,
                response: entry.response.clone(),
                acked: entry.acked,
            })
            .collect()
    }
}

pub struct IncomingTableEntry {
    /// The latest request to this key.
    pub msg: ActorMessage,
    /// Who sent the latest message to this key
    pub source: ServicePath,
    /// The id of the latest request to this key.
    pub request_id: MessageId,
    /// How many times this key has been updated.
    pub version: u64,
    /// Time this key was created, in unix seconds.
    pub created_time: u64,
    /// Time this key last received a request, in unix seconds.
    pub last_updated_time: u64,
    /// Informational string about the response to the latest request.
    pub response: String,
    /// Whether the latest request was successful or not.
    pub acked: bool,
}

impl IncomingTableEntry {
    /// A request created a new key in the table.
    fn new(msg: ActorMessage, source: ServicePath, request_id: MessageId) -> Self {
        Self {
            msg,
            source,
            request_id,
            version: 1,
            created_time: get_unix_time(),
            last_updated_time: get_unix_time(),
            response: String::new(),
            acked: false,
        }
    }

    /// Update this entry with a newly received request.
    fn update_received(&mut self, msg: ActorMessage, source: ServicePath, request_id: MessageId) {
        self.msg = msg;
        self.source = source;
        self.request_id = request_id;
        self.version += 1;
        self.last_updated_time = get_unix_time();
    }

    /// Update this entry after a newly received request has been handled by the actor.
    fn update_handled(&mut self, error_code: SwbusErrorCode, error_message: &str) {
        if error_code == SwbusErrorCode::Ok {
            self.acked = true;
            self.response = String::from("Ok");
        } else {
            self.acked = false;
            self.response = format!("{error_code:?} ({error_message})");
        }
    }
}

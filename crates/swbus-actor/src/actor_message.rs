use std::any::type_name;

use crate::Result;
use anyhow::Context;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use swbus_edge::{
    simple_client::{MessageBody, MessageId, OutgoingMessage, SimpleSwbusEdgeClient},
    swbus_proto::swbus::{ServicePath, SwbusMessage},
};

pub use serde_json::Value;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ActorMessage {
    pub key: String,
    pub data: Value,
}

impl ActorMessage {
    pub fn new<K: Into<String>, T: Serialize>(key: K, data: &T) -> Result<Self> {
        let data = serde_json::to_value(data).context("serializing actor message data")?;
        Ok(Self { key: key.into(), data })
    }

    pub fn deserialize_data<T: DeserializeOwned>(&self) -> Result<T> {
        serde_json::from_value(self.data.clone())
            .with_context(|| format!("deserializing ActorMessage::data into {}", type_name::<T>()))
    }

    pub(crate) fn serialize(&self) -> Vec<u8> {
        // this should never fail, we know serde_json can handle ActorMessage fine.
        serde_json::to_vec(self).unwrap()
    }

    pub(crate) fn deserialize(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data).context("deserializing ActorMessage")
    }
}

pub(crate) fn actor_msg_to_swbus_msg(
    actor_message: &ActorMessage,
    destination: ServicePath,
    swbus_client: &SimpleSwbusEdgeClient,
) -> SwbusMessage {
    swbus_client
        .outgoing_message_to_swbus_message(OutgoingMessage {
            destination,
            body: MessageBody::Request {
                payload: actor_message.serialize(),
            },
        })
        .1
}

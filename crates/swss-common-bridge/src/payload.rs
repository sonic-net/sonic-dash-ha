use crate::table_watcher::TableWatcherMessage;
use std::error::Error;
use swbus_actor::prelude::ServicePath;
use swss_common::KeyOpFieldValues;

pub fn encode_subscribe(subscriber: ServicePath) -> Vec<u8> {
    serde_json::to_vec(&TableWatcherMessage::Subscribe { subscriber }).unwrap()
}

pub fn encode_unsubscribe(subscriber: ServicePath) -> Vec<u8> {
    serde_json::to_vec(&TableWatcherMessage::Unsubscribe { subscriber }).unwrap()
}

pub(crate) fn decode_table_watcher_message(payload: &[u8]) -> serde_json::Result<TableWatcherMessage> {
    serde_json::from_slice(payload)
}

pub(crate) fn encode_kfvs(kfvs: &KeyOpFieldValues) -> Vec<u8> {
    serde_json::to_vec(kfvs).unwrap()
}

pub fn decode_kfvs(payload: &[u8]) -> Result<KeyOpFieldValues, Box<dyn Error>> {
    Ok(serde_json::from_slice(payload)?)
}

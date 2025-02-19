mod consumer;
mod producer;

use std::error::Error;
use swss_common::KeyOpFieldValues;

pub use consumer::ConsumerTableBridge;
pub use producer::ProducerTableBridge;

pub fn encode_kfvs(kfvs: &KeyOpFieldValues) -> Vec<u8> {
    serde_json::to_vec(kfvs).unwrap()
}

pub fn decode_kfvs(payload: &[u8]) -> Result<KeyOpFieldValues, Box<dyn Error>> {
    Ok(serde_json::from_slice(payload)?)
}

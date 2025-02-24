use std::error::Error;
use swss_common::KeyOpFieldValues;

pub fn encode_kfv(kfv: &KeyOpFieldValues) -> Vec<u8> {
    bincode::serialize(kfv).expect("Error serializing KeyOpFieldValues")
}

pub fn decode_kfv(data: &[u8]) -> Result<KeyOpFieldValues, Box<dyn Error + Send + Sync + 'static>> {
    bincode::deserialize(data).map_err(|e| e.into())
}

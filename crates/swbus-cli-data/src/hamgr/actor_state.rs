use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ActorState {
    pub incoming_state: Vec<IncomingStateEntry>,
    pub internal_state: Vec<InternalStateEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IncomingStateEntry {
    pub key: String,
    pub source: String,
    pub request_id: u64,
    pub version: u64,
    pub message: ActorMessage,
}

impl PartialEq for IncomingStateEntry {
    // Skip request_id in comparison during test
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.source == other.source
            && self.version == other.version
            && self.message == other.message
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ActorMessage {
    pub key: String,
    pub data: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct InternalStateEntry {
    pub key: String,
    pub swss_table: String,
    pub swss_key: String,
    pub fvs: Vec<KeyValue>,
    pub mutated: bool,
    pub backup_fvs: Vec<KeyValue>,
}

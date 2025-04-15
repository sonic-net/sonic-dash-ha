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
    pub created_time: u64,
    pub last_updated_time: u64,
    pub response: String,
    pub acked: bool,
}

impl PartialEq for IncomingStateEntry {
    // Skip request_id, create_time and last_update_time in comparison during test
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.source == other.source
            && self.version == other.version
            && self.message == other.message
            && self.response == other.response
            && self.acked == other.acked
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InternalStateEntry {
    pub key: String,
    pub swss_table: String,
    pub swss_key: String,
    pub fvs: Vec<KeyValue>,
    pub mutated: bool,
    pub backup_fvs: Vec<KeyValue>,
    pub last_updated_time: Option<u64>,
}

impl PartialEq for InternalStateEntry {
    // Skip last_update_time in comparison during test
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
            && self.swss_table == other.swss_table
            && self.swss_key == other.swss_key
            && self.fvs == other.fvs
            && self.backup_fvs == other.backup_fvs
            && self.mutated == other.mutated
    }
}

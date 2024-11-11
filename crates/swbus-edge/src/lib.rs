mod core_client;
mod edge_runtime;
mod message_handler_proxy;
mod message_router;

pub use edge_runtime::SwbusEdgeRuntime;

use swbus_proto::swbus::{
    swbus_message::Body, ServicePath, SwbusMessage as RawSwbusMessage, SwbusMessageHeader as RawSwbusMessageHeader,
};

/// A clone of protobuf's SwbusMessage, but with required fields.
#[derive(Debug, Clone)]
pub struct SwbusMessage {
    pub header: SwbusMessageHeader,
    pub body: Body,
}

impl Into<RawSwbusMessage> for SwbusMessage {
    fn into(self) -> RawSwbusMessage {
        RawSwbusMessage {
            header: Some(self.header.into()),
            body: Some(self.body),
        }
    }
}

impl From<RawSwbusMessage> for SwbusMessage {
    fn from(value: RawSwbusMessage) -> Self {
        SwbusMessage {
            header: value.header.expect("no header").into(),
            body: value.body.expect("no body"),
        }
    }
}

/// A clone of protobuf's SwbusMessageHeader, but with required fields.
#[derive(Debug, Clone)]
pub struct SwbusMessageHeader {
    pub version: u32,
    pub id: u64,
    pub flag: u32,
    pub ttl: u32,
    pub source: ServicePath,
    pub destination: ServicePath,
}

impl SwbusMessageHeader {
    pub fn new(source: ServicePath, destination: ServicePath) -> Self {
        // use the regular constructor so we can get id generation
        RawSwbusMessageHeader::new(source, destination).into()
    }
}

impl Into<RawSwbusMessageHeader> for SwbusMessageHeader {
    fn into(self) -> RawSwbusMessageHeader {
        RawSwbusMessageHeader {
            version: self.version,
            id: self.id,
            flag: self.flag,
            ttl: self.ttl,
            source: Some(self.source),
            destination: Some(self.destination),
        }
    }
}

impl From<RawSwbusMessageHeader> for SwbusMessageHeader {
    fn from(value: RawSwbusMessageHeader) -> Self {
        SwbusMessageHeader {
            version: value.version,
            id: value.id,
            flag: value.flag,
            ttl: value.ttl,
            source: value.source.expect("no source"),
            destination: value.destination.expect("no destination"),
        }
    }
}

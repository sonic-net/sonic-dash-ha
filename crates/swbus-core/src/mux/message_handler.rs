use std::io::Result;
use swbus_proto::swbus::*;

pub trait MessageHandler {
    fn send_queue_tx(&self) -> Result<tokio::sync::mpsc::Sender<SwbusMessage>>;
}

use crate::contracts::swbus::*;
use std::io::Result;

pub trait MessageHandler {
    fn message_queue_tx(&self) -> Result<tokio::sync::mpsc::Sender<SwbusMessage>>;
}

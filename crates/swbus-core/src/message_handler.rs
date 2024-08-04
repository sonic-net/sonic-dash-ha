use crate::contracts::swbus::*;
use std::io::Result;

pub trait MessageHandler {
    fn handle_message(&self, message: SwbusMessage) -> Result<()>;
}

use std::io;

use swbus_proto::result::*;
use swbus_proto::swbus::*;
use tokio::sync::mpsc::Sender;

pub struct SwbusMessageHandlerProxy {
    scope: Scope,
    tx: Sender<SwbusMessage>,
}

impl SwbusMessageHandlerProxy {
    pub fn new(tx: Sender<SwbusMessage>, scope: Scope) -> Self {
        Self { tx, scope }
    }

    pub fn scope(&self) -> Scope {
        self.scope
    }

    pub async fn send(&self, message: SwbusMessage) -> Result<()> {
        match self.tx.send(message).await {
            Ok(_) => Ok(()),
            Err(e) => Err(SwbusError::connection(
                SwbusErrorCode::ConnectionError,
                io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    format!("Message handler channel is broken: {}", e),
                ),
            )),
        }
    }
}

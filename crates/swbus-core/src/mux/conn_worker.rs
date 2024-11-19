use super::SwbusConnInfo;
use super::SwbusMultiplexer;
use std::io;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::Streaming;
use tracing::{error, info};

pub(crate) enum SwbusConnControlMessage {
    Shutdown,
}

pub struct SwbusConnWorker {
    info: Arc<SwbusConnInfo>,
    control_queue_rx: mpsc::Receiver<SwbusConnControlMessage>,
    // incoming message stream
    message_stream: Streaming<SwbusMessage>,
    mux: Arc<SwbusMultiplexer>,
}

impl SwbusConnWorker {
    pub fn new(
        info: Arc<SwbusConnInfo>,
        control_queue_rx: mpsc::Receiver<SwbusConnControlMessage>,
        message_stream: Streaming<SwbusMessage>,
        mux: Arc<SwbusMultiplexer>,
    ) -> Self {
        Self {
            info,
            control_queue_rx,
            message_stream,
            mux,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        self.register_to_mux()?;
        let result = self.run_worker_loop().await;
        //unregister from mux
        self.unregister_from_mux()?;
        result
    }

    pub fn register_to_mux(&self) -> Result<()> {
        Ok(())
    }

    pub fn unregister_from_mux(&self) -> Result<()> {
        self.mux.unregister(self.info.clone());
        Ok(())
    }

    pub async fn run_worker_loop(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                control_message = self.control_queue_rx.recv() => {
                    match control_message {
                        Some(SwbusConnControlMessage::Shutdown) => {
                            info!("Shutting down connection worker.");
                            break;
                        }
                        None => {
                            unreachable!("Control queue closed unexpectedly.");
                        }
                    }
                }

                data_message = self.message_stream.next() => {
                    match data_message {
                        Some(Ok(message)) => {
                            match self.process_data_message(message).await {
                                Ok(_) => {}
                                Err(err) => {
                                    error!("Failed to process the incoming message: {}", err);
                                }
                            }
                        }
                        Some(Err(err)) => {
                            error!("Failed to receive message: {}.", err);
                            return Err(SwbusError::connection(
                                SwbusErrorCode::ConnectionError,
                                io::Error::new(io::ErrorKind::ConnectionReset, err.to_string()),
                            ));
                        }
                        None => {
                            info!("Message stream closed.");
                            return Err(SwbusError::connection(
                                SwbusErrorCode::ConnectionError,
                                io::Error::new(io::ErrorKind::ConnectionReset, "Message stream closed.".to_string()),
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn process_data_message(&mut self, message: SwbusMessage) -> Result<()> {
        self.validate_message_common(&message)?;
        match message.body {
            Some(swbus_message::Body::TraceRouteRequest(_)) => {
                println!("Received traceroute request: {:?}", message);
                //self.process_ping_request(&message);
            }
            _ => {
                self.mux.route_message(message).await?;
            }
        }
        Ok(())
    }

    fn validate_message_common(&mut self, message: &SwbusMessage) -> Result<()> {
        if message.header.is_none() {
            return Err(SwbusError::input(
                SwbusErrorCode::InvalidHeader,
                "Message missing header".to_string(),
            ));
        }

        let message_header = message.header.as_ref().unwrap();
        if message_header.version < 1 {
            return Err(SwbusError::input(
                SwbusErrorCode::InvalidHeader,
                "Message version too low".to_string(),
            ));
        }

        if message_header.source.is_none() {
            return Err(SwbusError::input(
                SwbusErrorCode::InvalidHeader,
                "Message missing source".to_string(),
            ));
        }

        if message_header.destination.is_none() {
            return Err(SwbusError::input(
                SwbusErrorCode::InvalidHeader,
                "Message missing destination".to_string(),
            ));
        }

        if message.body.is_none() {
            return Err(SwbusError::input(
                SwbusErrorCode::InvalidHeader,
                "Message missing body".to_string(),
            ));
        }

        Ok(())
    }
}

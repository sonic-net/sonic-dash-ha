use super::SwbusConnInfo;
use super::SwbusMultiplexer;
use crate::mux::conn_store::SwbusConnStore;
use futures_core::stream::Stream;
use std::io;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::SwbusMessage;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tonic::Status;
use tracing::{error, info};

pub(crate) enum SwbusConnControlMessage {
    Shutdown,
}

pub struct SwbusConnWorker<T>
where
    T: Stream<Item = Result<SwbusMessage, Status>> + Unpin,
{
    info: Arc<SwbusConnInfo>,
    control_queue_rx: mpsc::Receiver<SwbusConnControlMessage>,
    // incoming message stream
    message_stream: T,
    mux: Arc<SwbusMultiplexer>,
    conn_store: Arc<SwbusConnStore>,
}

impl<T> SwbusConnWorker<T>
where
    T: Stream<Item = Result<SwbusMessage, Status>> + Unpin,
{
    pub(crate) fn new(
        info: Arc<SwbusConnInfo>,
        control_queue_rx: mpsc::Receiver<SwbusConnControlMessage>,
        message_stream: T,
        mux: Arc<SwbusMultiplexer>,
        conn_store: Arc<SwbusConnStore>,
    ) -> Self {
        Self {
            info,
            control_queue_rx,
            message_stream,
            mux,
            conn_store,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        self.register_to_mux()?;
        let result = self.run_worker_loop().await;
        // unregister from mux
        self.unregister_from_mux()?;
        if result.is_err() {
            self.conn_store.conn_lost(self.info.clone());
        }
        result
    }

    pub fn register_to_mux(&self) -> Result<()> {
        Ok(())
    }

    pub fn unregister_from_mux(&self) -> Result<()> {
        self.mux.unregister(self.info.clone());
        Ok(())
    }

    async fn run_worker_loop(&mut self) -> Result<()> {
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
                info!("Received traceroute request: {:?}", message);
                // self.process_ping_request(&message);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mux::RouteConfig;
    use tokio_stream::{self as stream};

    #[tokio::test]
    async fn test_worker_shutdown() {
        let (tx, rx) = mpsc::channel(1);
        let message_stream = stream::iter(vec![]);
        let mux = Arc::new(SwbusMultiplexer::new());
        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));

        let conn_info = Arc::new(SwbusConnInfo::new_client(
            ConnectionType::Cluster,
            "127.0.0.1:8080".parse().unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.1-dpu0").unwrap(),
        ));

        let mut worker = SwbusConnWorker::new(conn_info, rx, message_stream, mux, conn_store);
        let worker_task = tokio::spawn(async move { worker.run().await });

        tx.send(SwbusConnControlMessage::Shutdown).await.unwrap();
        let result = worker_task.await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_worker_process_data_message() {
        let (tx, rx) = mpsc::channel(1);

        let header = SwbusMessageHeader::new(
            ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap(),
            1,
        );
        let ping_msg = SwbusMessage {
            header: Some(header),
            body: Some(swbus_message::Body::PingRequest(PingRequest::new())),
        };
        let message_stream = stream::iter(vec![Ok(ping_msg)]);

        let mux = Arc::new(SwbusMultiplexer::new());
        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));
        let route_config = RouteConfig {
            key: ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap(),
            scope: RouteScope::Cluster,
        };
        mux.set_my_routes(vec![route_config.clone()]);

        let conn_info = Arc::new(SwbusConnInfo::new_client(
            ConnectionType::Cluster,
            "127.0.0.1:8080".parse().unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.1-dpu0").unwrap(),
        ));

        let mut worker = SwbusConnWorker::new(conn_info, rx, message_stream, mux, conn_store);
        let worker_task = tokio::spawn(async move { worker.run().await });

        tx.send(SwbusConnControlMessage::Shutdown).await.unwrap();
        let result = worker_task.await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_worker_invalid_message() {
        let (_, rx) = mpsc::channel(1);
        let message_stream = stream::iter(vec![]);
        let mux = Arc::new(SwbusMultiplexer::new());
        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));

        let conn_info = Arc::new(SwbusConnInfo::new_client(
            ConnectionType::Cluster,
            "127.0.0.1:8080".parse().unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap(),
            ServicePath::from_string("regiona.clustera.10.0.0.1-dpu0").unwrap(),
        ));

        let mut worker = SwbusConnWorker::new(conn_info, rx, message_stream, mux, conn_store);

        // verify message without header
        let message = SwbusMessage {
            header: None,
            body: Some(swbus_message::Body::TraceRouteRequest(Default::default())),
        };
        assert!(worker.validate_message_common(&message).is_err());

        // verify message with wrong version
        let header = SwbusMessageHeader {
            version: 0,
            id: 1,
            flag: 0,
            ttl: 64,
            source: Some(ServicePath::from_string("regiona.clustera.10.0.0.1-dpu0").unwrap()),
            destination: Some(ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap()),
        };
        let message = SwbusMessage {
            header: Some(header),
            body: Some(swbus_message::Body::TraceRouteRequest(Default::default())),
        };
        assert!(worker.validate_message_common(&message).is_err());

        // verify message without source
        let header = SwbusMessageHeader {
            version: 1,
            id: 1,
            flag: 0,
            ttl: 64,
            source: None,
            destination: Some(ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap()),
        };
        let message = SwbusMessage {
            header: Some(header),
            body: Some(swbus_message::Body::TraceRouteRequest(Default::default())),
        };
        assert!(worker.validate_message_common(&message).is_err());

        // verify message without dest
        let header = SwbusMessageHeader {
            version: 1,
            id: 1,
            flag: 0,
            ttl: 64,
            source: Some(ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap()),
            destination: None,
        };
        let message = SwbusMessage {
            header: Some(header),
            body: Some(swbus_message::Body::TraceRouteRequest(Default::default())),
        };
        assert!(worker.validate_message_common(&message).is_err());

        // verify message without body
        let header = SwbusMessageHeader {
            version: 1,
            id: 1,
            flag: 0,
            ttl: 64,
            source: Some(ServicePath::from_string("regiona.clustera.10.0.0.1-dpu0").unwrap()),
            destination: Some(ServicePath::from_string("regiona.clustera.10.0.0.2-dpu0").unwrap()),
        };
        let message = SwbusMessage {
            header: Some(header),
            body: None,
        };
        assert!(worker.validate_message_common(&message).is_err());
    }
}

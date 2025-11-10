use super::route_annoucer::{RouteAnnounceTask, RouteAnnouncer};
use super::SwbusConn;
use super::SwbusMultiplexer;
use crate::mux::conn_store::SwbusConnStore;
use crate::mux::SwbusConnInfo;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use swbus_config::SwbusConfig;
use swbus_proto::result::*;
use swbus_proto::swbus::swbus_service_server::{SwbusService, SwbusServiceServer};
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::*;

pub struct SwbusServiceHost {
    swbus_server_addrs: Vec<SocketAddr>,
    mux: Option<Arc<SwbusMultiplexer>>,
    conn_store: Option<Arc<SwbusConnStore>>,
    shutdown_ct: CancellationToken,
}

type SwbusMessageResult<T> = Result<Response<T>, Status>;
type SwbusMessageStream = Pin<Box<dyn Stream<Item = Result<SwbusMessage, Status>> + Send>>;

// Separate implementation struct to allow cloning for multiple servers
struct SwbusServiceImpl {
    mux: Arc<SwbusMultiplexer>,
    conn_store: Arc<SwbusConnStore>,
}

impl SwbusServiceHost {
    pub fn new(swbus_server_addrs: Vec<SocketAddr>) -> Self {
        Self {
            swbus_server_addrs,
            mux: None,
            conn_store: None,
            shutdown_ct: CancellationToken::new(),
        }
    }

    pub fn get_shutdown_token(&self) -> CancellationToken {
        self.shutdown_ct.clone()
    }

    pub async fn shutdown(&self) {
        info!("SwbusServiceServer shutting down");
        self.shutdown_ct.cancel();
    }

    pub async fn start(mut self, config: SwbusConfig) -> Result<()> {
        if self.swbus_server_addrs.is_empty() {
            return Err(SwbusError::input(
                SwbusErrorCode::InvalidArgs,
                "No server addresses provided.".to_string(),
            ));
        }

        debug!("SwbusServiceServer starting at {:?}", self.swbus_server_addrs);

        if config.routes.is_empty() {
            return Err(SwbusError::input(
                SwbusErrorCode::InvalidArgs,
                "No routes found in the configuration.".to_string(),
            ));
        }

        // create mux and set route announce task queue
        let mut mux = SwbusMultiplexer::new(config.routes);
        let (route_annouce_task_tx, route_annouce_task_rx) = mpsc::channel::<RouteAnnounceTask>(100);
        let route_announcer_ct = CancellationToken::new();
        mux.set_route_announcer(route_annouce_task_tx, route_announcer_ct.clone());

        let mux = Arc::new(mux);
        let mux_clone = mux.clone();

        // start the route announcer
        tokio::spawn(async move {
            let mut route_announcer = RouteAnnouncer::new(route_annouce_task_rx, route_announcer_ct, mux_clone);
            route_announcer.run().await;
        });

        let conn_store = Arc::new(SwbusConnStore::new(mux.clone()));

        // add peers to the connection store
        for peer in config.peers {
            conn_store.add_peer(peer);
        }

        self.mux = Some(mux);
        let conn_store_clone = conn_store.clone();
        self.conn_store = Some(conn_store);

        // Start multiple servers, one for each address
        let mut server_handles = Vec::new();
        let addrs = self.swbus_server_addrs.clone();

        for addr in addrs.into_iter() {
            let service = SwbusServiceServer::new(SwbusServiceImpl {
                mux: self.mux.clone().unwrap(),
                conn_store: self.conn_store.clone().unwrap(),
            });

            let shutdown_ct_for_server = self.shutdown_ct.clone();
            let conn_store_clone = conn_store_clone.clone();
            let server_handle = tokio::spawn(async move {
                info!("Starting SwbusServiceServer on {}", addr);
                Server::builder()
                    .add_service(service)
                    .serve_with_shutdown(addr, async move {
                        shutdown_ct_for_server.cancelled().await;
                        info!("SwbusServiceServer on {} shutting down", addr);
                        conn_store_clone.shutdown().await;
                    })
                    .await
                    .map_err(|e| {
                        SwbusError::connection(
                            SwbusErrorCode::ConnectionError,
                            io::Error::other(format!("Failed to listen at {addr}: {e}")),
                        )
                    })
            });

            server_handles.push(server_handle);
        }

        // Wait for all servers to complete
        for handle in server_handles {
            handle
                .await
                .map_err(|e| SwbusError::internal(SwbusErrorCode::Fail, format!("Server task panicked: {e}")))??;
        }
        debug!("All SwbusServiceServers terminated");

        Ok(())
    }
}

#[tonic::async_trait]
impl SwbusService for SwbusServiceImpl {
    type StreamMessagesStream = SwbusMessageStream;

    #[instrument(name="connection_received", level="info", skip_all, fields(addr=%request.remote_addr().unwrap()))]
    async fn stream_messages(
        &self,
        request: Request<Streaming<SwbusMessage>>,
    ) -> SwbusMessageResult<SwbusMessageStream> {
        let client_addr = request.remote_addr().unwrap();

        info!("SwbusServiceServer::connection from {} accepted", client_addr);
        let service_path = match request.metadata().get(SWBUS_CLIENT_SERVICE_PATH) {
            Some(path) => match ServicePath::from_string(path.to_str().unwrap()) {
                Ok(service_path) => service_path,
                Err(e) => {
                    error!("SwbusServiceServer::error parsing client service path: {:?}", e);
                    return Err(Status::invalid_argument("Invalid client service path"));
                }
            },
            None => {
                error!("SwbusServiceServer::client service path not found");
                return Err(Status::invalid_argument("Client service path not found"));
            }
        };

        let conn_type = match request.metadata().get(SWBUS_CONNECTION_TYPE) {
            Some(conn_type_meta) => match ConnectionType::from_str_name(conn_type_meta.to_str().unwrap()) {
                Some(conn_type) => conn_type,
                None => {
                    return Err(Status::invalid_argument(format!(
                        "Invalid connection_type: {}",
                        conn_type_meta.to_str().unwrap()
                    )));
                }
            },
            None => {
                error!("SwbusServiceServer::service path scope not set");
                return Err(Status::invalid_argument("service path scope not set"));
            }
        };

        let in_stream = request.into_inner();
        info!(
            conn_type = conn_type as i32,
            service_path = service_path.to_longest_path(),
            "Creating SwbusConn"
        );
        // outgoing message queue
        let (out_tx, out_rx) = mpsc::channel(16);

        let conn_info = Arc::new(SwbusConnInfo::new_server(conn_type, client_addr, service_path));
        let conn =
            SwbusConn::from_incoming_stream(conn_info, in_stream, out_tx, self.mux.clone(), self.conn_store.clone())
                .await;
        self.conn_store.conn_established(conn);
        let out_stream = ReceiverStream::new(out_rx);

        // Send server service path in response metadata
        let mut response = Response::new(Box::pin(out_stream) as Self::StreamMessagesStream);
        let server_service_path = self.mux.get_my_service_path().to_string();
        response
            .metadata_mut()
            .insert(SWBUS_SERVER_SERVICE_PATH, server_service_path.parse().unwrap());

        Ok(response)
    }
}

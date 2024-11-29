use super::route_config::RoutesConfig;
use crate::mux::multiplexer::SwbusMultiplexer;
use crate::mux::service::SwbusServiceServerImpl;
use std::io;
use std::sync::Arc;
use swbus_proto::result::*;
use swbus_proto::swbus::swbus_service_server::SwbusServiceServer;
use swbus_proto::swbus::*;
use tokio::task::JoinHandle;
use tonic::transport::Server;

pub struct SwbusCoreRuntime {
    swbus_server_addr: String,
    multiplexer: Arc<SwbusMultiplexer>,
}

impl SwbusCoreRuntime {
    pub fn new(swbus_server_addr: String) -> Self {
        let multiplexer = Arc::new(SwbusMultiplexer::new());
        // populate the multiplexer with the routes
        Self {
            swbus_server_addr,
            multiplexer,
        }
    }

    pub async fn start(&mut self, routes_config: RoutesConfig) -> Result<()> {
        let addr = self.swbus_server_addr.parse().map_err(|e| {
            SwbusError::input(
                SwbusErrorCode::InvalidArgs,
                format!("Failed to parse server address: {}.", e),
            )
        })?;
        match routes_config.routes.len() {
            0 => {
                return Err(SwbusError::input(
                    SwbusErrorCode::InvalidArgs,
                    "No routes found in the configuration.".to_string(),
                ));
            }
            _ => {
                self.multiplexer.set_my_routes(routes_config.routes);
            }
        }

        // Start the grpc server
        let server = SwbusServiceServerImpl::new(self.multiplexer.clone());
        let server_handle: JoinHandle<Result<()>> = tokio::spawn(async move {
            Server::builder()
                .add_service(SwbusServiceServer::new(server))
                .serve(addr)
                .await
                .map_err(|e| {
                    SwbusError::connection(
                        SwbusErrorCode::ConnectionError,
                        io::Error::new(io::ErrorKind::Other, format!("Failed to listen at {}: {}", addr, e)),
                    )
                })
        });

        // Start connections to the neighbours
        for peer in routes_config.peers {
            self.multiplexer.add_peer(peer);
        }

        //Wait for server to finish
        match server_handle.await {
            Ok(Ok(_)) => Ok(()),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(SwbusError::internal(SwbusErrorCode::Fail, e.to_string())),
        }
    }
}

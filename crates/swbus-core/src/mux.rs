use crate::contracts::swbus::*;
use crate::MessageHandler;
use dashmap::DashMap;

pub struct Mux {
    /// Route table. Each entry is a registered prefix to message handler mapping.
    routes: DashMap<String, Box<dyn MessageHandler>>,
}

impl Mux {
    pub fn new() -> Self {
        Mux { routes: DashMap::new() }
    }

    pub fn register(&self, scope: ConnectionType, path: &ServicePath, handler: Box<dyn MessageHandler>) {
        let route_key = match scope {
            ConnectionType::Global => path.to_regional_prefix(),
            ConnectionType::Regional => path.to_cluster_prefix(),
            ConnectionType::Cluster => path.to_node_prefix(),
            ConnectionType::Node => path.to_service_prefix(),
            ConnectionType::Client => path.to_string(),
        };

        self.routes.insert(route_key.to_string(), handler);
    }
}

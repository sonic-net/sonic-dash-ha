use super::conn::*;
use super::conn_proxy::SwbusConnProxy;
use crate::contracts::swbus::*;
use dashmap::DashMap;

#[derive(Debug)]
struct SwbusMultiplexerNextHop {
    conn_proxy: SwbusConnProxy,
    hop_count: u32,
}

impl SwbusMultiplexerNextHop {
    pub fn new(conn_proxy: SwbusConnProxy, hop_count: u32) -> Self {
        SwbusMultiplexerNextHop { conn_proxy, hop_count }
    }

    pub async fn queue_message(&self, message: SwbusMessage) -> crate::Result<()> {
        self.conn_proxy.queue_message(message).await
    }
}

pub struct SwbusMultiplexer {
    /// Route table. Each entry is a registered prefix to a next hop, which points to a connection.
    routes: DashMap<String, SwbusMultiplexerNextHop>,

    /// Connection table. Each entry is a connection id to a connection info.
    connections: DashMap<String, SwbusConn>,
}

impl SwbusMultiplexer {
    pub fn new() -> Self {
        SwbusMultiplexer {
            routes: DashMap::new(),
            connections: DashMap::new(),
        }
    }

    pub fn register(&self, path: &ServicePath, conn: SwbusConn) {
        // First, we insert the connection to connection table.
        let conn_info = conn.info();
        self.connections.insert(conn_id.clone(), conn);

        // Next, we update the route table.
        let route_key = match conn_type {
            ConnectionType::Global => path.to_regional_prefix(),
            ConnectionType::Regional => path.to_cluster_prefix(),
            ConnectionType::Cluster => path.to_node_prefix(),
            ConnectionType::Node => path.to_service_prefix(),
            ConnectionType::Client => path.to_string(),
        };
        let nexthop = SwbusMultiplexerNextHop::new(conn_id, 1);
        self.update_route(route_key, nexthop);
    }

    fn update_route(&self, route_key: String, nexthop: SwbusMultiplexerNextHop) {
        // If route entry doesn't exist, we insert the next hop as a new one.
        let mut route_entry = self.routes.entry(route_key).or_insert(nexthop.clone());

        // If we already have one, then we update the entry only when we have a smaller hop count.
        // The dashmap RefMut reference will hold a lock to the entry, which makes this function atomic.
        if route_entry.hop_count > nexthop.hop_count {
            *route_entry.value_mut() = nexthop;
        }
    }
}

pub mod core_client;
pub mod edge_runtime;
mod message_handler_proxy;
mod message_router;
pub mod simple_client;

pub use edge_runtime::SwbusEdgeRuntime;

use std::any::Any;
pub use swbus_proto;
// Marker trait for types that can be used as a runtime environment for swbus-edge.
// This is used by client to attach data to the runtime and shared by all handlers in the same runtime.
// The data is opagque to swbus-edge and is only used by the client.
pub trait RuntimeEnv: Send + Sync + Any {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

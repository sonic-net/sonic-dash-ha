use crate::{driver::ActorDriver, Actor};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use swbus_edge::{simple_client::SimpleSwbusEdgeClient, swbus_proto::swbus::ServicePath, SwbusEdgeRuntime};

/// Global structures shared by all actors.
pub struct ActorRuntime {
    swbus_edge: Arc<SwbusEdgeRuntime>,
}

impl ActorRuntime {
    pub fn new(swbus_edge: Arc<SwbusEdgeRuntime>) -> Self {
        Self { swbus_edge }
    }

    /// Spawn an actor on this runtime, reachable by sending Swbus requests to `addr`.
    pub fn spawn<A: Actor>(&self, actor: A, addr: ServicePath) {
        // TODO: Add privacy option
        let swbus_client = SimpleSwbusEdgeClient::new(self.swbus_edge.clone(), addr, true);
        let actor_driver = ActorDriver::new(actor, swbus_client);
        tokio::task::spawn(actor_driver.run());
    }

    pub fn get_swbus_edge(&self) -> Arc<SwbusEdgeRuntime> {
        self.swbus_edge.clone()
    }
}

// Global actor runtime for using `actor::spawn`, similar to `tokio::spawn`.
static GLOBAL_RUNTIME: RwLock<Option<ActorRuntime>> = RwLock::new(None);

/// Set the global [`ActorRuntime`] for [`spawn`].
pub fn set_global_runtime(rt: ActorRuntime) {
    *GLOBAL_RUNTIME.write().unwrap() = Some(rt);
}

/// Set the global [`ActorRuntime`] for [`spawn`], only if not previously set.
///
/// This is useful for test environments where every test may attempt to set the global runtime.
pub fn set_global_runtime_if_unset(rt: ActorRuntime) {
    let mut guard = GLOBAL_RUNTIME.write().unwrap();
    if guard.is_none() {
        *guard = Some(rt);
    }
}

/// Get the global [`ActorRuntime`].
pub fn get_global_runtime() -> RwLockReadGuard<'static, Option<ActorRuntime>> {
    GLOBAL_RUNTIME.read().unwrap()
}

/// Spawn an actor on the global runtime.
///
/// Panics if called before [`set_global_runtime`] is called.
pub fn spawn<A: Actor>(actor: A, addr: ServicePath) {
    GLOBAL_RUNTIME
        .read()
        .unwrap()
        .as_ref()
        .expect("You must call actor::set_global_runtime() before calling actor::spawn()")
        .spawn(actor, addr);
}

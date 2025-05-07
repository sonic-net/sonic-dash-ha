mod driver;

pub mod actor_message;
pub mod runtime;
pub mod state;

use std::future::Future;

pub use actor_message::ActorMessage;
pub use anyhow::{Error, Result};
pub use runtime::{get_global_runtime, set_global_runtime, set_global_runtime_if_unset, spawn, ActorRuntime};
pub use serde_json as json;
pub use state::State;

/// An actor that can be run.
pub trait Actor: Send + 'static {
    /// Callback run upon spawn. Allows actors to setup the internal state table and send initial messages to get started.
    ///
    /// If this returns `Err(..)`, the actor dies immediately and cannot receive messages.
    ///
    /// The default implementation does nothing.
    fn init(&mut self, state: &mut State) -> impl Future<Output = Result<()>> + Send {
        _ = state;
        async { Ok(()) }
    }

    /// Callback run upon receipt of an [`ActorMessage`].
    ///
    /// If this returns `Err(..)`, state changes are not committed.
    /// Outgoing state messages are not sent, and internal state changes are rolled back.
    fn handle_message(
        &mut self,
        state: &mut State,
        key: &str,
        context: &mut Context,
    ) -> impl Future<Output = Result<()>> + Send;
}

pub struct Context {
    stopped: bool,
}

impl Context {
    pub fn new() -> Self {
        Context { stopped: false }
    }

    pub fn stop(&mut self) {
        self.stopped = true;
    }
}

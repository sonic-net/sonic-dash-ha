pub mod incoming;
pub mod internal;
pub mod outgoing;

use incoming::Incoming;
use internal::Internal;
use outgoing::Outgoing;
use std::sync::Arc;
use swbus_edge::simple_client::SimpleSwbusEdgeClient;

/// Actor state tables.
pub struct State {
    pub(crate) internal: Internal,
    pub(crate) incoming: Incoming,
    pub(crate) outgoing: Outgoing,
}

impl State {
    pub(crate) fn new(swbus_edge: Arc<SimpleSwbusEdgeClient>) -> Self {
        Self {
            internal: Internal::new(),
            incoming: Incoming::new(swbus_edge.clone()),
            outgoing: Outgoing::new(swbus_edge),
        }
    }

    /// Helper to access all three state tables without mutably borrowing the entire state struct.
    ///
    /// Example of how this helps:
    /// ```compile_fail
    /// let x = state.incoming().get("x")?;
    /// let tbl = state.internal().get_mut("tbl");
    /// tbl["x"] = x.deserialize_data::<String>()?.into();
    /// // ERROR: state is mutably borrowed twice
    /// ```
    ///
    /// ```no_run
    /// # fn foo() -> anyhow::Result<()> {
    /// # let state: swbus_actor::State = todo!();
    /// let (internal, incoming, outgoing) = state.get_all();
    /// let x = incoming.get("x")?;
    /// let tbl = internal.get_mut("tbl");
    /// tbl["x"] = x.deserialize_data::<String>()?.into();
    /// // Ok
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_all(&mut self) -> (&mut Internal, &mut Incoming, &mut Outgoing) {
        (&mut self.internal, &mut self.incoming, &mut self.outgoing)
    }

    pub fn internal(&mut self) -> &mut Internal {
        &mut self.internal
    }

    pub fn incoming(&mut self) -> &mut Incoming {
        &mut self.incoming
    }

    pub fn outgoing(&mut self) -> &mut Outgoing {
        &mut self.outgoing
    }
}

fn get_unix_time() -> u64 {
    use std::time::SystemTime;

    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

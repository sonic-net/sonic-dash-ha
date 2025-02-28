pub mod incoming;
pub mod internal;
pub mod outgoing;

use incoming::Incoming;
use internal::Internal;
use outgoing::Outgoing;
use std::sync::Arc;
use swbus_edge::simple_client::SimpleSwbusEdgeClient;

pub struct State {
    pub internal: Internal,
    pub incoming: Incoming,
    pub outgoing: Outgoing,
}

impl State {
    pub(crate) fn new(swbus_edge: Arc<SimpleSwbusEdgeClient>) -> Self {
        Self {
            internal: Internal::new(),
            incoming: Incoming::new(swbus_edge.clone()),
            outgoing: Outgoing::new(swbus_edge),
        }
    }
}

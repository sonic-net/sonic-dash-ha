use super::*;
use crate::*;
use std::rc::Rc;

obj_wrapper! {
    struct ZmqServerObj { ptr: SWSSZmqServer } SWSSZmqServer_free
}

/// Rust wrapper around `swss::ZmqServer`.
#[derive(Clone, Debug)]
pub struct ZmqServer {
    pub(crate) obj: Rc<ZmqServerObj>,

    // The types that register message handlers with a ZmqServer must be kept alive until
    // the server thread dies, otherwise we risk the server thread calling methods on deleted objects.
    // Currently this is just ZmqConsumerStateTable, but in the future there may be other types added
    // and this vec will need to hold an enum of the possible message handlers.
    handlers: Vec<ZmqConsumerStateTable>,
}

impl ZmqServer {
    pub fn new(endpoint: &str) -> Self {
        let endpoint = cstr(endpoint);
        let obj = unsafe { Rc::new(SWSSZmqServer_new(endpoint.as_ptr()).into()) };
        Self {
            obj,
            handlers: Vec::new(),
        }
    }

    pub(crate) fn register_consumer_state_table(&mut self, tbl: ZmqConsumerStateTable) {
        self.handlers.push(tbl);
    }
}

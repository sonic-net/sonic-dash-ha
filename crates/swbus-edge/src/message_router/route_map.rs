use super::Privacy;
use crate::message_handler_proxy::SwbusMessageHandlerProxy;
use dashmap::DashMap;
use std::ops::Deref;
use swbus_proto::swbus::ServicePath;

#[derive(Default)]
pub(super) struct RouteMap(DashMap<ServicePath, (SwbusMessageHandlerProxy, Privacy)>);

impl RouteMap {
    pub(super) fn insert(&self, svc_path: ServicePath, handler: SwbusMessageHandlerProxy, privacy: Privacy) {
        self.0.insert(svc_path, (handler, privacy));
    }

    pub(super) fn get(&self, svc_path: &ServicePath, message_privacy: Privacy) -> Option<SwbusMessageHandlerProxy> {
        self.0.get(svc_path).and_then(|pair| {
            let (handler, route_privacy) = pair.deref();
            if *route_privacy == Privacy::Private && message_privacy == Privacy::Public {
                None
            } else {
                Some(handler.clone())
            }
        })
    }
}

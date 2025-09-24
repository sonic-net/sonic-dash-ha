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

    pub(super) fn remove(&self, svc_path: &ServicePath) -> Option<(SwbusMessageHandlerProxy, Privacy)> {
        self.0.remove(svc_path).map(|(_, value)| value)
    }

    pub(super) fn contains(&self, svc_path: &ServicePath) -> bool {
        self.0.contains_key(svc_path)
    }
}

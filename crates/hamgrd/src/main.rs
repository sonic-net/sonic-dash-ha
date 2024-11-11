mod runtime;

use runtime::{impl_actor, Actor, ActorRuntime, IncomingMessage, Outbox, ResendQueueConfig};
use std::{sync::Arc, time::Duration};
use swbus_proto::swbus::ServicePath;

#[tokio::main]
async fn main() {
    let bind_addr = Arc::new(ServicePath {
        region_id: "region_a".into(),
        cluster_id: "switch-cluster-a".into(),
        node_id: "10.0.0.1".into(),
        service_type: "hamgrd".into(),
        service_id: "dpu0".into(),
        resource_type: "hascope".into(),
        resource_id: "dpu".into(),
    });

    let resend_config = ResendQueueConfig {
        resend_time: Duration::from_millis(500),
        max_tries: 120,
    };

    let runtime = ActorRuntime::new(resend_config);
    runtime.spawn(bind_addr, TestActor).await;
}

struct TestActor;

impl_actor! {
    impl Actor for TestActor {
        async fn init(&mut self, _outbox: &Outbox) {}

        async fn handle_message(&mut self, msg: IncomingMessage, _outbox: &Outbox) {
            println!("Received message {msg:?}")
        }
    }
}

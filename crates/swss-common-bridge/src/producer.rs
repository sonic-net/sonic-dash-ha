use std::{future::Future, sync::Arc};
use swbus_actor::ActorMessage;
use swbus_edge::{
    simple_client::{MessageBody, OutgoingMessage, SimpleSwbusEdgeClient},
    swbus_proto::swbus::{ServicePath, SwbusErrorCode},
    SwbusEdgeRuntime,
};
use swss_common::{FieldValues, KeyOpFieldValues, KeyOperation, ProducerStateTable, Table, ZmqProducerStateTable};
use tokio::task::JoinHandle;
use tokio_util::task::AbortOnDropHandle;

pub struct ProducerBridge {
    _task: AbortOnDropHandle<()>,
}

impl ProducerBridge {
    pub fn spawn<T>(rt: Arc<SwbusEdgeRuntime>, addr: ServicePath, table: T) -> Self
    where
        T: ProducerTable,
    {
        let task = spawn_producer_bridge(rt, addr, table);
        ProducerBridge {
            _task: AbortOnDropHandle::new(task),
        }
    }
}

pub fn spawn_producer_bridge<T>(rt: Arc<SwbusEdgeRuntime>, addr: ServicePath, mut table: T) -> JoinHandle<()>
where
    T: ProducerTable,
{
    let swbus = SimpleSwbusEdgeClient::new(rt, addr, false);
    tokio::task::spawn(async move {
        loop {
            let Some(msg) = swbus.recv().await else {
                // Swbus shut down, we might as well quit.
                break;
            };

            let MessageBody::Request { payload } = msg.body else {
                // There is no reason we'd receive a response, but ignore them anyway.
                continue;
            };

            let (error_code, error_message) = match ActorMessage::deserialize(&payload) {
                Ok(actor_msg) => match actor_msg.deserialize_data::<KeyOpFieldValues>() {
                    Ok(kfv) => {
                        table.apply_kfv(kfv).await;
                        (SwbusErrorCode::Ok, String::new())
                    }
                    Err(e) => (
                        SwbusErrorCode::InvalidPayload,
                        format!("Invalid KeyOpFieldValues: {e:#}"),
                    ),
                },
                Err(e) => (SwbusErrorCode::InvalidPayload, format!("Invalid ActorMessage: {e:#}")),
            };

            swbus
                .send(OutgoingMessage {
                    destination: msg.source,
                    body: MessageBody::Response {
                        request_id: msg.id,
                        error_code,
                        error_message,
                        response_body: None,
                    },
                })
                .await
                .expect("Sending swbus message");
        }
    })
}

pub trait ProducerTable: Send + 'static {
    fn set(&mut self, key: &str, fvs: FieldValues) -> impl Future<Output = ()> + Send;
    fn del(&mut self, key: &str) -> impl Future<Output = ()> + Send;
    fn apply_kfv(&mut self, kfv: KeyOpFieldValues) -> impl Future<Output = ()> + Send {
        async move {
            match kfv.operation {
                KeyOperation::Set => self.set(&kfv.key, kfv.field_values).await,
                KeyOperation::Del => self.del(&kfv.key).await,
            }
        }
    }
}

macro_rules! impl_producertable {
    ($($t:ty)*) => {
        $(impl ProducerTable for $t {
            async fn set(&mut self, key: &str, fvs: FieldValues) {
                <$t>::set_async(self, key, fvs)
                    .await
                    .expect(concat!(stringify!($t::set_async), " threw an exception"))
            }

            async fn del(&mut self, key: &str) {
                <$t>::del_async(self, key)
                    .await
                    .expect(concat!(stringify!($t::del_async), " threw an exception"))
            }
        })*
    }
}

impl_producertable! { ProducerStateTable ZmqProducerStateTable Table }

#[cfg(test)]
mod test {
    use crate::{
        consumer::ConsumerTable,
        producer::{ProducerBridge, ProducerTable},
    };
    use std::{sync::Arc, time::Duration};
    use swbus_actor::ActorMessage;
    use swbus_edge::{
        simple_client::{MessageBody, OutgoingMessage, SimpleSwbusEdgeClient},
        swbus_proto::swbus::ServicePath,
        SwbusEdgeRuntime,
    };
    use swss_common::{
        ConsumerStateTable, KeyOpFieldValues, ProducerStateTable, ZmqClient, ZmqConsumerStateTable,
        ZmqProducerStateTable, ZmqServer,
    };
    use swss_common_testing::{random_kfvs, random_zmq_endpoint, Redis};
    use tokio::time::timeout;

    #[tokio::test]
    async fn producer_state_table_bridge() {
        let redis = Redis::start();
        let pst = ProducerStateTable::new(redis.db_connector(), "mytable").unwrap();
        let cst = ConsumerStateTable::new(redis.db_connector(), "mytable", None, None).unwrap();
        timeout(Duration::from_secs(5), run_test(cst, pst)).await.unwrap();
    }

    #[tokio::test]
    async fn zmq_ponsumer_state_table_bridge() {
        let (zmq_endpoint, _deleter) = random_zmq_endpoint();
        let mut zmqs = ZmqServer::new(&zmq_endpoint).unwrap();
        let zmqc = ZmqClient::new(&zmq_endpoint).unwrap();

        let redis = Redis::start();
        let zpst = ZmqProducerStateTable::new(redis.db_connector(), "mytable", zmqc, false).unwrap();
        let zcst = ZmqConsumerStateTable::new(redis.db_connector(), "mytable", &mut zmqs, None, None).unwrap();
        timeout(Duration::from_secs(5), run_test(zcst, zpst)).await.unwrap();
    }

    async fn run_test<C: ConsumerTable, P: ProducerTable>(mut consumer_table: C, producer_table: P) {
        // Setup swbus
        let mut swbus_edge = SwbusEdgeRuntime::new("<none>".to_string(), sp("edge"));
        swbus_edge.start().await.unwrap();
        let rt = Arc::new(swbus_edge);

        // Create edge client to send updates to the bridge
        let swbus = SimpleSwbusEdgeClient::new(rt.clone(), sp("receiver"), true);

        // Spawn the bridge
        let _bridge = ProducerBridge::spawn(rt, sp("mytable-bridge"), producer_table);

        // Send some updates to the bridge
        let mut kfvs = random_kfvs();
        for kfv in &kfvs {
            let msg = OutgoingMessage {
                destination: sp("mytable-bridge"),
                body: MessageBody::Request {
                    payload: encode_kfv(kfv),
                },
            };
            swbus.send(msg).await.unwrap();
        }

        // Receive the updates directly
        let mut kfvs_received = Vec::new();
        while kfvs_received.len() < kfvs.len() {
            consumer_table.read_data().await;
            kfvs_received.extend(consumer_table.pops().await);
        }

        // Assert we got all the same updates
        kfvs.sort_unstable();
        kfvs_received.sort_unstable();
        assert_eq!(kfvs, kfvs_received);
    }

    fn encode_kfv(kfv: &KeyOpFieldValues) -> Vec<u8> {
        ActorMessage::new("", kfv).unwrap().serialize()
    }

    fn sp(s: &str) -> ServicePath {
        ServicePath::from_string(&format!("test.test.test/test/test/test/{s}")).unwrap()
    }
}

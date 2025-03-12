use std::{future::Future, sync::Arc};
use swbus_actor::ActorMessage;
use swbus_edge::{
    simple_client::{MessageBody, OutgoingMessage, SimpleSwbusEdgeClient},
    swbus_proto::swbus::ServicePath,
    SwbusEdgeRuntime,
};
use swss_common::{ConsumerStateTable, KeyOpFieldValues, SubscriberStateTable, ZmqConsumerStateTable};
use tokio::task::JoinHandle;

/// Spawn a consumer table to actor bridge task.
///
/// `dest_generator` is a function that takes a `&KeyOpFieldValues` read from `table`
/// and generates the `ServicePath` address and `String` input table key that
/// the data will be sent to.
pub fn spawn_consumer_bridge<T, F>(
    rt: Arc<SwbusEdgeRuntime>,
    addr: ServicePath,
    mut table: T,
    mut dest_generator: F,
) -> JoinHandle<()>
where
    T: ConsumerTable,
    F: FnMut(&KeyOpFieldValues) -> (ServicePath, String) + Send + 'static,
{
    let swbus = SimpleSwbusEdgeClient::new(rt, addr, false);
    tokio::task::spawn(async move {
        loop {
            tokio::select! {
                _ = table.read_data() => {
                    for kfvs in table.pops().await {
                        let (destination, key) = dest_generator(&kfvs);
                        let payload = ActorMessage::new(key, &kfvs).expect("encoding ActorMessage").serialize();
                        swbus
                            .send(OutgoingMessage { destination, body: MessageBody::Request { payload } })
                            .await
                            .expect("Sending swbus message");
                    }
                }

                // Ignore all messages received.
                // It is a programming error to send a request to a consumer table.
                // Responses are ignored because we don't resend updates if the receiver fails.
                maybe_msg = swbus.recv() => {
                    if maybe_msg.is_none() {
                        // Swbus shut down, we might as well quit.
                        break;
                    }
                }
            }
        }
    })
}

pub trait ConsumerTable: Send + 'static {
    fn read_data(&mut self) -> impl Future<Output = ()> + Send;
    fn pops(&mut self) -> impl Future<Output = Vec<KeyOpFieldValues>> + Send;
}

macro_rules! impl_consumertable {
    ($($t:ty)*) => {
        $(impl ConsumerTable for $t {
            async fn read_data(&mut self) {
                <$t>::read_data_async(self)
                    .await
                    .expect(concat!(stringify!($t::read_data_async), " io error"));
            }

            async fn pops(&mut self) -> Vec<KeyOpFieldValues> {
                <$t>::pops_async(self)
                    .await
                    .expect(concat!(stringify!($t::pops_async), " threw an exception"))
            }
        })*
    };
}

impl_consumertable! { ConsumerStateTable SubscriberStateTable ZmqConsumerStateTable }

#[cfg(test)]
mod test {
    use super::{spawn_consumer_bridge, ConsumerTable};
    use crate::producer::ProducerTable;
    use std::{sync::Arc, time::Duration};
    use swbus_actor::ActorMessage;
    use swbus_edge::{
        simple_client::{IncomingMessage, MessageBody, SimpleSwbusEdgeClient},
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
    async fn consumer_state_table_bridge() {
        let redis = Redis::start();
        let pst = ProducerStateTable::new(redis.db_connector(), "mytable").unwrap();
        let cst = ConsumerStateTable::new(redis.db_connector(), "mytable", None, None).unwrap();
        timeout(Duration::from_secs(5), run_test(cst, pst)).await.unwrap();
    }

    /*
    TODO: I do not know why this doesn't work
    #[tokio::test]
    async fn subscriber_state_table_bridge() {
        let redis = Redis::start();
        let tbl = Table::new(redis.db_connector(), "mytable").unwrap();
        let sst = SubscriberStateTable::new(redis.db_connector(), "mytable", None, None).unwrap();
        timeout(Duration::from_secs(5), run_test(sst, tbl)).await.unwrap();
    }
    */

    #[tokio::test]
    async fn zmq_consumer_state_table_bridge() {
        let (zmq_endpoint, _deleter) = random_zmq_endpoint();
        let mut zmqs = ZmqServer::new(&zmq_endpoint).unwrap();
        let zmqc = ZmqClient::new(&zmq_endpoint).unwrap();

        let redis = Redis::start();
        let zpst = ZmqProducerStateTable::new(redis.db_connector(), "mytable", zmqc, false).unwrap();
        let zcst = ZmqConsumerStateTable::new(redis.db_connector(), "mytable", &mut zmqs, None, None).unwrap();
        timeout(Duration::from_secs(5), run_test(zcst, zpst)).await.unwrap();
    }

    async fn run_test<C: ConsumerTable, P: ProducerTable>(consumer_table: C, mut producer_table: P) {
        // Setup swbus
        let mut swbus_edge = SwbusEdgeRuntime::new("<none>".to_string(), sp("edge"));
        swbus_edge.start().await.unwrap();
        let rt = Arc::new(swbus_edge);

        // Create edge client to receive updates from the bridge
        let swbus = SimpleSwbusEdgeClient::new(rt.clone(), sp("receiver"), true);

        // Spawn the bridge
        spawn_consumer_bridge(rt, sp("mytable-bridge"), consumer_table, |_| {
            (sp("receiver"), "".into())
        });

        // Send some updates we should receive
        let mut kfvs = random_kfvs();
        for kfv in kfvs.clone() {
            producer_table.apply_kfv(kfv).await;
        }

        // Receive the updates
        let mut kfvs_received = Vec::new();
        for _ in 0..kfvs.len() {
            let msg = swbus.recv().await;
            let Some(IncomingMessage {
                body: MessageBody::Request { payload },
                ..
            }) = msg
            else {
                panic!("Did not receive proper message from bridge")
            };

            let kfv_received = decode_kfv(&payload);
            kfvs_received.push(kfv_received);
        }

        // Assert we got all the same updates
        kfvs.sort_unstable();
        kfvs_received.sort_unstable();
        assert_eq!(kfvs, kfvs_received);
    }

    fn decode_kfv(payload: &[u8]) -> KeyOpFieldValues {
        ActorMessage::deserialize(payload).unwrap().deserialize_data().unwrap()
    }

    fn sp(s: &str) -> ServicePath {
        ServicePath::from_string(&format!("test.test.test/test/test/test/{s}")).unwrap()
    }
}

use crate::encoding::encode_kfvs;
use std::{future::Future, sync::Arc};
use swbus_edge::{
    simple_client::{MessageBody, OutgoingMessage, SimpleSwbusEdgeClient},
    swbus_proto::swbus::ServicePath,
    SwbusEdgeRuntime,
};
use swss_common::{ConsumerStateTable, KeyOpFieldValues, SubscriberStateTable, ZmqConsumerStateTable};
use tokio::task::JoinHandle;

pub fn spawn_consumer_bridge<T, F>(
    rt: Arc<SwbusEdgeRuntime>,
    source: ServicePath,
    mut table: T,
    mut dest_generator: F,
) -> JoinHandle<()>
where
    T: ConsumerTable,
    F: FnMut(&KeyOpFieldValues) -> ServicePath + Send + 'static,
{
    let simple_client = SimpleSwbusEdgeClient::new(rt, source, false);
    tokio::task::spawn(async move {
        loop {
            tokio::select! {
                _ = table.read_data() => {
                    for kfvs in table.pops().await {
                        let destination = dest_generator(&kfvs);
                        let payload = encode_kfvs(&kfvs);
                        simple_client
                            .send(OutgoingMessage { destination, body: MessageBody::Request { payload } })
                            .await
                            .expect("Sending swbus message");
                    }
                }

                // Ignore all messages received.
                // It is a programming error to send a request to a consumer table.
                // Responses are ignored because we don't resend updates if the receiver fails.
                maybe_msg = simple_client.recv() => {
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
    use super::spawn_consumer_bridge;
    use crate::encoding::decode_kfv;
    use std::{sync::Arc, time::Duration};
    use swbus_edge::{
        simple_client::{IncomingMessage, MessageBody, SimpleSwbusEdgeClient},
        swbus_proto::swbus::ServicePath,
        SwbusEdgeRuntime,
    };
    use swss_common::{ConsumerStateTable, KeyOperation, ProducerStateTable};
    use swss_common_testing::{random_kfvs, Redis};
    use tokio::time::timeout;

    fn sp(s: &str) -> ServicePath {
        ServicePath::from_string(&format!("test.test.test/test/test/test/{s}")).unwrap()
    }

    #[tokio::test]
    async fn consumer_state_table_bridge() {
        // Setup swbus
        let mut swbus_edge = SwbusEdgeRuntime::new("<none>".to_string(), sp("edge"));
        swbus_edge.start().await.unwrap();
        let rt = Arc::new(swbus_edge);

        // Setup swss-common
        let redis = Redis::start();
        let mut pst = ProducerStateTable::new(redis.db_connector(), "mytable").unwrap();
        let cst = ConsumerStateTable::new(redis.db_connector(), "mytable", None, None).unwrap();

        // Create handler to receive updates from the bridge
        let receiver = SimpleSwbusEdgeClient::new(rt.clone(), sp("receiver"), true);

        // Spawn the bridge
        spawn_consumer_bridge(rt, sp("mytable-bridge"), cst, |_| sp("receiver"));

        // Send some updates we should receive
        let mut kfvs = random_kfvs();
        for kfv in kfvs.clone() {
            match kfv.operation {
                KeyOperation::Set => {
                    pst.set_async(&kfv.key, kfv.field_values).await.unwrap();
                }
                KeyOperation::Del => {
                    pst.del_async(&kfv.key).await.unwrap();
                }
            }
        }

        // Receive the updates
        let mut kfvs_received = Vec::new();
        for _ in 0..kfvs.len() {
            let msg = timeout(Duration::from_secs(3), receiver.recv()).await;
            let Ok(Some(IncomingMessage {
                body: MessageBody::Request { payload },
                ..
            })) = msg
            else {
                panic!("Did not receive proper message from bridge")
            };

            let kfv_received = decode_kfv(&payload).unwrap();
            kfvs_received.push(kfv_received);
        }

        // Assert we got all the same updates
        kfvs.sort_unstable();
        kfvs_received.sort_unstable();
        assert_eq!(kfvs, kfvs_received);
    }
}

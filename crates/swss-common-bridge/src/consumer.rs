use std::{collections::HashMap, future::Future, sync::Arc};
use swbus_actor::ActorMessage;
use swbus_edge::{
    simple_client::{MessageBody, OutgoingMessage, SimpleSwbusEdgeClient},
    swbus_proto::swbus::ServicePath,
    SwbusEdgeRuntime,
};
use swss_common::{
    ConsumerStateTable, FieldValues, KeyOpFieldValues, KeyOperation, SubscriberStateTable, Table, ZmqConsumerStateTable,
};
use tokio::task::JoinHandle;
use tokio_util::task::AbortOnDropHandle;

pub struct ConsumerBridge {
    _task: AbortOnDropHandle<()>,
}

impl ConsumerBridge {
    /// Spawn a consumer table to actor bridge task.
    ///
    /// `dest_generator` is a function that takes a `&KeyOpFieldValues` read from `table`
    /// and generates the `ServicePath` address and `String` input table key that
    /// the data will be sent to.
    pub fn spawn<T, F, S>(
        rt: Arc<SwbusEdgeRuntime>,
        addr: ServicePath,
        table: T,
        dest_generator: F,
        selector: S,
    ) -> Self
    where
        T: ConsumerTable,
        F: FnMut(&KeyOpFieldValues) -> (ServicePath, String) + Send + 'static,
        S: Fn(&KeyOpFieldValues) -> bool + Sync + Send + 'static,
    {
        let task = spawn_consumer_bridge(rt, addr, table, dest_generator, selector);
        ConsumerBridge {
            _task: AbortOnDropHandle::new(task),
        }
    }
}

pub fn spawn_consumer_bridge<T, F, S>(
    rt: Arc<SwbusEdgeRuntime>,
    addr: ServicePath,
    mut table: T,
    mut dest_generator: F,
    selector: S,
) -> JoinHandle<()>
where
    T: ConsumerTable,
    F: FnMut(&KeyOpFieldValues) -> (ServicePath, String) + Send + 'static,
    S: Fn(&KeyOpFieldValues) -> bool + Sync + Send + 'static,
{
    let swbus = SimpleSwbusEdgeClient::new(rt, addr, false, false);
    tokio::task::spawn(async move {
        let mut table_cache = TableCache::default();
        let mut send_kfv = async |kfv: KeyOpFieldValues| {
            // Merge the kfv to get the whole table as an update
            let kfv = table_cache.merge_kfv(kfv);
            if !selector(&kfv) {
                return;
            }

            // Use user-provided function to generate Actor's ServicePath and input table key
            let (destination, key) = dest_generator(&kfv);

            // Encode the KeyOpFieldValues as an ActorMessage
            let payload = ActorMessage::new(key, &kfv).expect("encoding ActorMessage").serialize();

            // Send the message
            swbus
                .send(OutgoingMessage {
                    destination,
                    body: MessageBody::Request { payload },
                })
                .await
                .expect("Sending swbus message");
        };

        // Send initial/rehydration updates
        for kfv in table.rehydrate().await {
            send_kfv(kfv).await;
        }

        loop {
            tokio::select! {
                // Send all received updates
                _ = table.read_data() => {
                    for kfv in table.pops().await {
                        send_kfv(kfv).await;
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

/// An in-memory copy of a table.
/// We keep a copy so that we can send the entire table for each update, rather than just the updated fields.
/// This relieves the need for actors to handle partial updates by caching their own copy.
#[derive(Default)]
struct TableCache(HashMap<String, FieldValues>);

impl TableCache {
    /// Merge the update and return a `KeyOpFieldValues` that contains the state of the entire table.
    fn merge_kfv(&mut self, kfv: KeyOpFieldValues) -> KeyOpFieldValues {
        match kfv.operation {
            KeyOperation::Set => {
                let field_values = self.0.entry(kfv.key.clone()).or_default();
                field_values.extend(kfv.field_values);
                KeyOpFieldValues {
                    key: kfv.key,
                    operation: KeyOperation::Set,
                    field_values: field_values.clone(),
                }
            }
            KeyOperation::Del => {
                self.0.remove(&kfv.key);
                kfv
            }
        }
    }
}

pub trait ConsumerTable: Send + 'static {
    /// Wait for updates
    fn read_data(&mut self) -> impl Future<Output = ()> + Send;

    /// Get updates
    fn pops(&mut self) -> impl Future<Output = Vec<KeyOpFieldValues>> + Send;

    /// Dump the table, as if `pops()` returned everything again, for rehydration after a restart
    fn rehydrate(&mut self) -> impl Future<Output = Vec<KeyOpFieldValues>> + Send;
}

macro_rules! rehydrate_body {
    (true, $self:ident) => {{
        let db = $self.db_connector_mut().clone_async().await;
        let mut tbl = Table::new_async(db, $self.table_name()).await.expect("Table::new");
        let keys = tbl.get_keys_async().await.expect("Table::get_keys");

        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let field_values = tbl.get_async(&key).await.expect("Table::get").unwrap_or_default();
            out.push(KeyOpFieldValues {
                key,
                operation: KeyOperation::Set,
                field_values,
            });
        }
        out
    }};

    (false, self) => {
        // This table does not support rehydration.
        // Eg, ZmqConsumerStateTable does not write updates down anywhere,
        // so it's impossible to rehydrate.
        vec![]
    };
}

macro_rules! impl_consumertable {
    ($($t:ty [$can_rehydrate:tt])*) => {
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

            async fn rehydrate(&mut self) -> Vec<KeyOpFieldValues> {
                rehydrate_body!($can_rehydrate, self)
            }
        })*
    };
}

impl_consumertable! { ConsumerStateTable[true] SubscriberStateTable[true] ZmqConsumerStateTable[false] }

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
        ConsumerStateTable, KeyOpFieldValues, KeyOperation, ProducerStateTable, ZmqClient, ZmqConsumerStateTable,
        ZmqProducerStateTable, ZmqServer,
    };
    use swss_common_testing::{random_kfvs, random_zmq_endpoint, Redis};
    use tokio::time::timeout;

    #[tokio::test]
    async fn consumer_state_table_bridge() {
        let redis = Redis::start();
        let pst = ProducerStateTable::new(redis.db_connector(), "mytable").unwrap();
        let cst = ConsumerStateTable::new(redis.db_connector(), "mytable", None, None).unwrap();
        let cst2 = ConsumerStateTable::new(redis.db_connector(), "mytable", None, None).unwrap();
        timeout(Duration::from_secs(5), run_test(cst, Some(cst2), pst))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn zmq_consumer_state_table_bridge() {
        let (zmq_endpoint, _deleter) = random_zmq_endpoint();
        let mut zmqs = ZmqServer::new(&zmq_endpoint).unwrap();
        let zmqc = ZmqClient::new(&zmq_endpoint).unwrap();

        let redis = Redis::start();
        let zpst = ZmqProducerStateTable::new(redis.db_connector(), "mytable", zmqc, false).unwrap();
        let zcst = ZmqConsumerStateTable::new(redis.db_connector(), "mytable", &mut zmqs, None, None).unwrap();
        timeout(Duration::from_secs(5), run_test(zcst, None, zpst))
            .await
            .unwrap();
    }

    async fn run_test<C: ConsumerTable, P: ProducerTable>(
        consumer_table: C,
        rehydrate_table: Option<C>,
        mut producer_table: P,
    ) {
        // Setup swbus
        let mut swbus_edge = SwbusEdgeRuntime::new("<none>".to_string(), sp("edge"));
        swbus_edge.start().await.unwrap();
        let rt = Arc::new(swbus_edge);

        // Create edge client to receive updates from the bridge
        let swbus = SimpleSwbusEdgeClient::new(rt.clone(), sp("receiver"), true, false);

        // Spawn the bridge
        let bridge = spawn_consumer_bridge(
            rt.clone(),
            sp("mytable-bridge"),
            consumer_table,
            |_| (sp("receiver"), "".into()),
            |_| true,
        );

        // Send some updates we should receive
        let mut kfvs = random_kfvs();
        for kfv in kfvs.clone() {
            producer_table.apply_kfv(kfv).await;
        }

        // Receive the updates
        let mut kfvs_received = receive_n_messages(kfvs.len(), &swbus).await;

        // Assert we got all the same updates
        kfvs.sort_unstable();
        kfvs_received.sort_unstable();
        assert_eq!(kfvs, kfvs_received);
        bridge.abort();

        // Test rehydration
        if let Some(rehydrate_table) = rehydrate_table {
            // Spawn new bridge to rehydrate with
            let _bridge_rehydrate = spawn_consumer_bridge(
                rt,
                sp("mytable-bridge"),
                rehydrate_table,
                |_| (sp("receiver"), "".into()),
                |_| true,
            );

            // Receive all updates that are in the database (no DELs because those aren't saved)
            let n_set_kfvs = kfvs.iter().filter(|kfv| kfv.operation == KeyOperation::Set).count();
            let kfvs_received = receive_n_messages(n_set_kfvs, &swbus).await;

            for kfv in kfvs_received {
                assert!(kfvs.contains(&kfv));
            }
        }
    }

    async fn receive_n_messages(n: usize, swbus: &SimpleSwbusEdgeClient) -> Vec<KeyOpFieldValues> {
        let mut received = Vec::new();
        for _ in 0..n {
            let msg = swbus.recv().await.unwrap();
            let IncomingMessage {
                body: MessageBody::Request { payload },
                ..
            } = msg
            else {
                panic!("Did not receive proper message from bridge")
            };
            let kfv = ActorMessage::deserialize(&payload).unwrap().deserialize_data().unwrap();
            received.push(kfv);
        }
        received
    }

    fn sp(s: &str) -> ServicePath {
        ServicePath::from_string(&format!("test.test.test/test/test/test/{s}")).unwrap()
    }
}

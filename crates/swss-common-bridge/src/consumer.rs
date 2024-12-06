use std::future::Future;
use swbus_actor::prelude::*;
use swss_common::{ConsumerStateTable, KeyOpFieldValues, SubscriberStateTable, ZmqConsumerStateTable};
use tokio_util::task::AbortOnDropHandle;

/// A bridge that converts swss consumer table updates to Swbus messages.
///
/// That is, a [`ConsumerStateTable`], [`SubscriberStateTable`], or [`ZmqConsumerStateTable`].
pub struct ConsumerTableBridge<T>(State<T>);

enum State<T> {
    WaitingForInit { table: T, destination: ServicePath },
    Running { _watcher_task: AbortOnDropHandle<()> },
}

impl<T: ConsumerTable + 'static> ConsumerTableBridge<T> {
    pub fn new(table: T, destination: ServicePath) -> Self {
        ConsumerTableBridge(State::WaitingForInit { table, destination })
    }
}

impl<T: ConsumerTable + 'static> Actor for ConsumerTableBridge<T> {
    async fn init(&mut self, outbox: Outbox) {
        replace_with::replace_with_or_abort(&mut self.0, |state| match state {
            State::WaitingForInit { table, destination } => State::Running {
                _watcher_task: AbortOnDropHandle::new(tokio::spawn(watch_table(table, outbox, destination))),
            },
            _ => unreachable!(),
        });
    }

    async fn handle_message(&mut self, message: IncomingMessage, outbox: Outbox) {
        match message.body {
            MessageBody::Request(_) => {
                let err = "ConsumerTableBridge doesn't expect any messages".to_string();
                let msg = OutgoingMessage::error_response(message, SwbusErrorCode::InvalidPayload, err);
                outbox.send(msg).await;
            }
            MessageBody::Response(_) => { /* ignore all responses */ }
        }
    }
}

async fn watch_table<T: ConsumerTable>(mut table: T, outbox: Outbox, destination: ServicePath) {
    loop {
        table.read_data_async().await;
        for kfvs in table.pops() {
            let payload = crate::encode_kfvs(&kfvs);
            outbox
                .send(OutgoingMessage::request(destination.clone(), payload))
                .await;
        }
    }
}

pub trait ConsumerTable: Send {
    fn read_data_async(&mut self) -> impl Future<Output = ()> + Send;
    fn pops(&mut self) -> Vec<KeyOpFieldValues>;
}

macro_rules! impl_consumertable {
    ($t:ty) => {
        impl ConsumerTable for $t {
            async fn read_data_async(&mut self) {
                <$t>::read_data_async(self)
                    .await
                    .expect("read_data_async io error");
            }

            fn pops(&mut self) -> Vec<KeyOpFieldValues> {
                <$t>::pops(self).expect(concat!(stringify!($t), "::pops threw an exception"))
            }
        }
    };
}

impl_consumertable!(ConsumerStateTable);
impl_consumertable!(SubscriberStateTable);
impl_consumertable!(ZmqConsumerStateTable);

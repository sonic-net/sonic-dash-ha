use std::future::Future;
use swss_common::{ConsumerStateTable, KeyOpFieldValues, SubscriberStateTable, ZmqConsumerStateTable};

pub(crate) trait Table: Send {
    fn read_data_async(&mut self) -> impl Future<Output = ()> + Send;
    fn pops(&mut self) -> Vec<KeyOpFieldValues>;
}

impl Table for ConsumerStateTable {
    async fn read_data_async(&mut self) {
        self.read_data_async().await.expect("read_data_async io error");
    }

    fn pops(&mut self) -> Vec<KeyOpFieldValues> {
        ConsumerStateTable::pops(self)
    }
}

impl Table for SubscriberStateTable {
    async fn read_data_async(&mut self) {
        self.read_data_async().await.expect("read_data_async io error");
    }

    fn pops(&mut self) -> Vec<KeyOpFieldValues> {
        SubscriberStateTable::pops(self)
    }
}

impl Table for ZmqConsumerStateTable {
    async fn read_data_async(&mut self) {
        self.read_data_async().await.expect("read_data_async io error");
    }

    fn pops(&mut self) -> Vec<KeyOpFieldValues> {
        ZmqConsumerStateTable::pops(self)
    }
}

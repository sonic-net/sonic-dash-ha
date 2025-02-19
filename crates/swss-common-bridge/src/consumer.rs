use std::future::Future;
use swss_common::{ConsumerStateTable, KeyOpFieldValues, SubscriberStateTable, ZmqConsumerStateTable};

pub trait ConsumerTable: Send {
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

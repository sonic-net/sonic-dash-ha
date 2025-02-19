use std::future::Future;
use swss_common::{FieldValues, ProducerStateTable, Table, ZmqProducerStateTable};

pub trait ProducerTable {
    fn set(&mut self, key: &str, fvs: FieldValues) -> impl Future<Output = ()> + Send;
    fn del(&mut self, key: &str) -> impl Future<Output = ()> + Send;
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

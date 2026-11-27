pub mod consumer;
pub mod producer;

use std::collections::HashMap;
use swss_common::{FieldValues, KeyOpFieldValues, KeyOperation};

/// An in-memory copy of a table.
/// The consumer bridge uses this to merge incremental updates into complete entries and skip duplicate notifications.
/// The cache is established from updates read from the consumer table and is empty after the bridge restarts.
#[derive(Default)]
pub(crate) struct TableCache(HashMap<String, FieldValues>);

impl TableCache {
    /// Merge the update and return a `KeyOpFieldValues` that contains the state of the entire table.
    /// Returns None if the update doesn't change the existing data.
    fn merge_kfv(&mut self, kfv: KeyOpFieldValues) -> Option<KeyOpFieldValues> {
        match kfv.operation {
            KeyOperation::Set => {
                let field_values = self.0.entry(kfv.key.clone()).or_default();

                // Check if the new field_values would be the same as the existing ones
                let mut new_field_values = field_values.clone();
                new_field_values.extend(kfv.field_values);

                if new_field_values == *field_values {
                    return None;
                }

                *field_values = new_field_values;
                Some(KeyOpFieldValues {
                    key: kfv.key,
                    operation: KeyOperation::Set,
                    field_values: field_values.clone(),
                })
            }
            KeyOperation::Del => {
                self.0.remove(&kfv.key);
                Some(kfv)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use swss_common::{CxxString, DbConnector, FieldValues, KeyOpFieldValues};
    use swss_common_testing::{random_fvs, random_kfv, Redis};

    #[test]
    fn async_helper_shares_the_consumers_types_and_serde() {
        let _: fn(&Redis) -> DbConnector = Redis::db_connector;
        let _: FieldValues = random_fvs();
        let _: KeyOpFieldValues = random_kfv();

        let value = CxxString::new("shared async value");
        let encoded = serde_json::to_string(&value).unwrap();
        assert_eq!(serde_json::from_str::<CxxString>(&encoded).unwrap(), value);
    }

    #[tokio::test]
    async fn async_connector_uses_the_shared_tokio_runtime() {
        let redis = Redis::start();
        let mut db = DbConnector::new_unix_async(0, &redis.sock, 1_000).await.unwrap();
        let value = CxxString::new("hello, world!");
        db.set_async("greeting", &value).await.unwrap();
        assert_eq!(db.get_async("greeting").await.unwrap().unwrap(), value);
    }
}

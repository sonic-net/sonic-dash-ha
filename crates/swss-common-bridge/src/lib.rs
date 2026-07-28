pub mod consumer;
pub mod producer;

use std::collections::HashMap;
use swss_common::{FieldValues, KeyOpFieldValues, KeyOperation};

/// An in-memory copy of a table.
/// We keep a copy so that we can skip update if there is no change. The cache is established from the previous
/// request sent to the Producer. If the Producer is restarted, the cache will be empty.
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

    /// Replace the cached entry with the provided kfv.
    /// Returns false if operation is SET and field values are the same as cached ones.
    /// For Del it always return true because the local cache is cleared after restart.
    fn replace_kfv(&mut self, kfv: KeyOpFieldValues) -> bool {
        match kfv.operation {
            KeyOperation::Set => {
                let field_values = self.0.entry(kfv.key.clone()).or_default();

                if kfv.field_values == *field_values {
                    return false;
                }

                *field_values = kfv.field_values;
                true
            }
            KeyOperation::Del => {
                self.0.remove(&kfv.key);
                true
            }
        }
    }
}

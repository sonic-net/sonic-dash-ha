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

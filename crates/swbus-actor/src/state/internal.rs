use super::get_unix_time;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use swss_common::{FieldValues, Table};

/// Internal state table - SWSS `Table`s.
#[derive(Default, Debug)]
pub struct Internal {
    table: HashMap<String, InternalTableEntry>,
}

impl Internal {
    pub fn get(&self, key: &str) -> &FieldValues {
        self.table
            .get(key)
            .unwrap_or_else(|| panic!("Invalid internal table key '{key}'"))
            .fvs()
    }

    pub fn get_mut(&mut self, key: &str) -> &mut FieldValues {
        self.table
            .get_mut(key)
            .unwrap_or_else(|| panic!("Invalid internal table key '{key}'"))
            .fvs_mut()
    }

    pub async fn add(&mut self, key: impl Into<String>, swss_table: Table, swss_key: impl Into<String>) {
        let entry = InternalTableEntry::new(swss_table, swss_key.into()).await;
        self.table.insert(key.into(), entry);
    }

    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn drop_changes(&mut self) {
        for entry in self.table.values_mut() {
            entry.drop_changes();
        }
    }

    pub(crate) async fn commit_changes(&mut self) {
        for entry in self.table.values_mut() {
            entry.commit_changes().await;
        }
    }

    pub(crate) fn dump_state(&self) -> HashMap<String, InternalTableData> {
        self.table
            .iter()
            .map(|(key, entry)| (key.clone(), entry.data.clone()))
            .collect()
    }
}

#[derive(Debug)]
struct InternalTableEntry {
    swss_table: Table,
    data: InternalTableData,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InternalTableData {
    pub swss_table_name: String,
    pub swss_key: String,
    // Local cache/copy of the table's FVs
    pub fvs: FieldValues,
    pub mutated: bool,

    // FVs that will be restored if an actor callback fails
    pub backup_fvs: FieldValues,

    /// Last time changes were written to the table, in unix seconds.
    /// `None` if the table was never written to.
    pub last_updated_time: Option<u64>,
}

impl PartialEq for InternalTableData {
    // Skip last_update_time in comparison during test
    fn eq(&self, other: &Self) -> bool {
        self.swss_table_name == other.swss_table_name
            && self.swss_key == other.swss_key
            && self.fvs == other.fvs
            && self.backup_fvs == other.backup_fvs
            && self.mutated == other.mutated
    }
}

impl InternalTableEntry {
    async fn new(mut swss_table: Table, swss_key: String) -> Self {
        // (re)hydrate from the table
        let fvs = swss_table
            .get_async(&swss_key)
            .await
            .expect("Table::get threw an exception")
            .unwrap_or_default();
        let backup_fvs = fvs.clone();

        Self {
            data: InternalTableData {
                swss_table_name: swss_table.get_name().to_string(),
                swss_key,
                fvs,
                mutated: false,
                backup_fvs,
                last_updated_time: None,
            },
            swss_table,
        }
    }

    fn fvs(&self) -> &FieldValues {
        &self.data.fvs
    }

    fn fvs_mut(&mut self) -> &mut FieldValues {
        if !self.data.mutated {
            self.data.backup_fvs.clone_from(&self.data.fvs);
            self.data.mutated = true;
        }
        &mut self.data.fvs
    }

    async fn commit_changes(&mut self) {
        self.data.mutated = false;
        self.swss_table
            .set_async(&self.data.swss_key, self.data.fvs.clone())
            .await
            .expect("Table::set threw an exception");
        self.data.last_updated_time = Some(get_unix_time());
    }

    fn drop_changes(&mut self) {
        self.data.mutated = false;
        self.data.fvs.clone_from(&self.data.backup_fvs);
    }
}

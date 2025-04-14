use std::collections::HashMap;
use swss_common::{FieldValues, Table};

use super::get_unix_time;

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
}

#[derive(Debug)]
struct InternalTableEntry {
    swss_table: Table,
    swss_key: String,

    // Local cache/copy of the table's FVs
    fvs: FieldValues,
    mutated: bool,

    // FVs that will be restored if an actor callback fails
    backup_fvs: FieldValues,

    /// Last time changes were written to the table, in unix seconds.
    /// `None` if the table was never written to.
    last_updated_time: Option<u64>,
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
            swss_table,
            swss_key,
            fvs,
            mutated: false,
            backup_fvs,
            last_updated_time: None,
        }
    }

    fn fvs(&self) -> &FieldValues {
        &self.fvs
    }

    fn fvs_mut(&mut self) -> &mut FieldValues {
        if !self.mutated {
            self.backup_fvs.clone_from(&self.fvs);
            self.mutated = true;
        }
        &mut self.fvs
    }

    async fn commit_changes(&mut self) {
        self.mutated = false;
        self.swss_table
            .set_async(&self.swss_key, self.fvs.clone())
            .await
            .expect("Table::set threw an exception");
        self.last_updated_time = Some(get_unix_time());
    }

    fn drop_changes(&mut self) {
        self.mutated = false;
        self.fvs.clone_from(&self.backup_fvs);
    }
}

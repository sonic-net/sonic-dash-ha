use std::collections::HashMap;
use swss_common::{DbConnector, Result};

pub trait SonicDbObject {
    fn get_key_separator() -> String;
    fn get_key(&self) -> String;
    fn from_keys(key_str: &str) -> Self;
    fn get_table_name() -> String;

    fn to_sonicdb(&self, db: &DbConnector) -> Result<()>;
    fn from_sonicdb(&mut self, db: &DbConnector) -> Result<()>;
}

pub fn load_all<T: SonicDbObject>(db: &DbConnector) -> Result<HashMap<String, T>> {
    let mut map = HashMap::new();
    let key_prefix = format!("{}{}*", T::get_table_name(), T::get_key_separator());
    let keys = db.keys(&key_prefix)?;
    for key in keys {
        let mut obj = T::from_keys(&key);
        obj.from_sonicdb(db)?;
        map.insert(key, obj);
    }
    Ok(map)
}

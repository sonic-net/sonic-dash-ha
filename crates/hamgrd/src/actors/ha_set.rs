use anyhow::Result;

#[allow(unused)]
pub struct HaSetActor {
    id: String,
}
impl HaSetActor {
    #[allow(unused)]
    pub fn new(key: String) -> Result<Self> {
        let actor = HaSetActor { id: key };
        Ok(actor)
    }

    #[allow(unused)]
    pub fn table_name() -> &'static str {
        "DASH_HA_SET_CONFIG_TABLE"
    }

    #[allow(unused)]
    pub fn name() -> &'static str {
        "ha-set"
    }
}

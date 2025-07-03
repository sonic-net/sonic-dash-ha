// temporarily disable unused warning until vdpu/ha-set actors are implemented
//#![allow(unused)]
use anyhow::{Context, Result};
use chrono::DateTime;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{formats::CommaSeparator, serde_as, skip_serializing_none, StringWithSeparator};
use sonicdb_derive::SonicDb;
use swss_common::{DbConnector, Table};
use swss_serde::from_table;

/// Format: "Tue Jun 04 09:00:00 PM UTC 2024"
const TIMESTAMP_FORMAT: &str = "%a %b %d %I:%M:%S %p UTC %Y";

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2112-ha-global-configurations>
#[derive(Serialize, Deserialize, Default, Debug, SonicDb)]
#[sonicdb(table_name = "DASH_HA_GLOBAL_CONFIG", key_separator = "|", db_name = "CONFIG_DB")]
pub struct DashHaGlobalConfig {
    // The port of control plane data channel, used for bulk sync.
    pub cp_data_channel_port: Option<u16>,
    // The destination port used when tunneling packetse via DPU-to-DPU data plane channel.
    pub dp_channel_dst_port: Option<u16>,
    // The min source port used when tunneling packetse via DPU-to-DPU data plane channel.
    pub dp_channel_src_port_min: Option<u16>,
    // The max source port used when tunneling packetse via DPU-to-DPU data plane channel.
    pub dp_channel_src_port_max: Option<u16>,
    // The interval of sending each DPU-to-DPU data path probe.
    pub dp_channel_probe_interval_ms: Option<u32>,
    // The number of probe failure needed to consider data plane channel is dead.
    pub dp_channel_probe_fail_threshold: Option<u32>,
    // The interval of DPU BFD probe in milliseconds.
    pub dpu_bfd_probe_interval_in_ms: Option<u32>,
    // The number of DPU BFD probe failure before probe down.
    pub dpu_bfd_probe_multiplier: Option<u32>,
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2111-dpu--vdpu-definitions>
#[skip_serializing_none]
#[derive(Deserialize, Serialize, Clone, PartialEq, Eq, Debug, SonicDb)]
#[sonicdb(table_name = "DPU", key_separator = "|", db_name = "CONFIG_DB")]
pub struct Dpu {
    pub state: Option<String>,
    pub vip_ipv4: Option<String>,
    pub vip_ipv6: Option<String>,
    pub pa_ipv4: String,
    pub pa_ipv6: Option<String>,
    pub dpu_id: u32,
    pub vdpu_id: Option<String>,
    pub orchagent_zmq_port: u16,
    pub swbus_port: u16,
    pub midplane_ipv4: String,
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2111-dpu--vdpu-definitions>
#[derive(Serialize, Deserialize, Clone, SonicDb)]
#[sonicdb(table_name = "REMOTE_DPU", key_separator = "|", db_name = "CONFIG_DB")]
pub struct RemoteDpu {
    pub pa_ipv4: String,
    pub pa_ipv6: Option<String>,
    pub npu_ipv4: String,
    pub npu_ipv6: Option<String>,
    pub dpu_id: u32,
    pub swbus_port: u16,
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2111-dpu--vdpu-definitions>
#[serde_as]
#[derive(Deserialize, Clone, SonicDb)]
#[sonicdb(table_name = "VDPU", key_separator = "|", db_name = "CONFIG_DB")]
pub struct VDpu {
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub main_dpu_ids: Vec<String>,
}

#[derive(Serialize, Deserialize, SonicDb)]
#[sonicdb(table_name = "BFD_SESSION_TABLE", key_separator = ":", db_name = "DPU_APPL_DB")]
pub struct BfdSessionTable {
    pub tx_interval: Option<u32>,
    pub rx_interval: Option<u32>,
    pub multiplier: Option<u32>,
    pub multihop: bool,
    pub shutdown: bool,
    pub local_addr: String,
    #[serde(rename = "type")]
    pub session_type: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Default, Debug)]
#[serde(rename_all = "lowercase")]
pub enum DpuPmonStateType {
    Up,
    Down,
    #[default]
    Unknown,
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/pmon/smartswitch-pmon.md#dpu_state-definition>
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, SonicDb)]
#[sonicdb(table_name = "DPU_STATE", key_separator = "|", db_name = "CHASSIS_STATE_DB")]
pub struct DpuState {
    #[serde(default)]
    pub dpu_midplane_link_state: DpuPmonStateType,
    #[serde(
        default = "now_in_millis",
        deserialize_with = "timestamp_deserialize",
        serialize_with = "timestamp_serialize"
    )]
    pub dpu_midplane_link_time: i64,
    #[serde(default)]
    pub dpu_control_plane_state: DpuPmonStateType,
    #[serde(
        default = "now_in_millis",
        deserialize_with = "timestamp_deserialize",
        serialize_with = "timestamp_serialize"
    )]
    pub dpu_control_plane_time: i64,
    #[serde(default)]
    pub dpu_data_plane_state: DpuPmonStateType,
    #[serde(
        default = "now_in_millis",
        deserialize_with = "timestamp_deserialize",
        serialize_with = "timestamp_serialize"
    )]
    pub dpu_data_plane_time: i64,
}

impl Default for DpuState {
    fn default() -> Self {
        Self {
            dpu_midplane_link_state: DpuPmonStateType::Unknown,
            dpu_midplane_link_time: now_in_millis(),
            dpu_control_plane_state: DpuPmonStateType::Unknown,
            dpu_control_plane_time: now_in_millis(),
            dpu_data_plane_state: DpuPmonStateType::Unknown,
            dpu_data_plane_time: now_in_millis(),
        }
    }
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/BFD/SmartSwitchDpuLivenessUsingBfd.md#27-dpu-bfd-session-state-updates>
#[serde_as]
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Debug, SonicDb)]
#[sonicdb(table_name = "DASH_BFD_PROBE_STATE", key_separator = "|", db_name = "DPU_STATE_DB")]
pub struct DashBfdProbeState {
    #[serde(default)]
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub v4_bfd_up_sessions: Vec<String>,
    #[serde(
        default = "now_in_millis",
        deserialize_with = "timestamp_deserialize",
        serialize_with = "timestamp_serialize"
    )]
    pub v4_bfd_up_sessions_timestamp: i64,
    #[serde(default)]
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub v6_bfd_up_sessions: Vec<String>,
    #[serde(
        default = "now_in_millis",
        deserialize_with = "timestamp_deserialize",
        serialize_with = "timestamp_serialize"
    )]
    pub v6_bfd_up_sessions_timestamp: i64,
}

impl Default for DashBfdProbeState {
    fn default() -> Self {
        Self {
            v4_bfd_up_sessions: Vec::new(),
            v4_bfd_up_sessions_timestamp: now_in_millis(),
            v6_bfd_up_sessions: Vec::new(),
            v6_bfd_up_sessions_timestamp: now_in_millis(),
        }
    }
}

fn timestamp_serialize<S>(ts: &i64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let datetime =
        DateTime::from_timestamp_millis(*ts).ok_or_else(|| serde::ser::Error::custom("invalid timestamp"))?;
    let formatted = datetime.format(TIMESTAMP_FORMAT).to_string();
    serializer.serialize_str(&formatted)
}

fn timestamp_deserialize<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    // Handle both missing fields and present fields
    let opt: Option<String> = Option::deserialize(deserializer)?;
    match opt {
        Some(s) => {
            let naive = chrono::NaiveDateTime::parse_from_str(&s, TIMESTAMP_FORMAT).map_err(de::Error::custom)?;
            Ok(naive.and_utc().timestamp_millis())
        }
        None => Ok(now_in_millis()), // Use default when field is missing
    }
}

pub fn now_in_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub fn get_dpu_config_from_db(dpu_id: u32) -> Result<Dpu> {
    let db = DbConnector::new_named("CONFIG_DB", false, 0).context("connecting config_db")?;
    let table = Table::new(db, "DPU").context("opening DPU table")?;

    let keys = table.get_keys().context("Failed to get keys from DPU table")?;

    for key in keys {
        let dpu: Dpu = from_table(&table, &key).context(format!("reading DPU entry {key}"))?;

        // find the DPU entry for the slot
        if dpu.dpu_id == dpu_id {
            return Ok(dpu);
        } else {
            continue;
        }
    }
    Err(anyhow::anyhow!("DPU entry not found for slot {}", dpu_id))
}

#[cfg(test)]
mod test {
    use super::*;
    use std::net::Ipv4Addr;
    use swss_common::{FieldValues, KeyOpFieldValues};
    use swss_common_testing::*;
    #[test]
    fn test_deserialize_dpu() {
        let json = r#"
        {
            "key": "DPU",
            "operation": "Set",
            "field_values": {
                "pa_ipv4": "1.2.3.4",
                "dpu_id": "1",
                "orchagent_zmq_port": "8100",
                "swbus_port": "23606",
                "midplane_ipv4": "127.0.0.1"
            }
        }"#;
        let kfv: KeyOpFieldValues = serde_json::from_str(json).unwrap();
        let dpu: Dpu = swss_serde::from_field_values(&kfv.field_values).unwrap();
        assert!(dpu.pa_ipv4 == "1.2.3.4");
        assert!(dpu.dpu_id == 1);
    }

    #[test]
    fn test_get_dpu_config_from_db() {
        let _ = Redis::start_config_db();

        // Populate the CONFIG_DB for testing
        populate_configdb_for_test();

        let config_fromdb = get_dpu_config_from_db(6).unwrap();

        let expected = Dpu {
            state: None,
            vip_ipv6: None,
            pa_ipv4: "1.2.3.6".to_string(),
            vip_ipv4: Some("4.5.6.6".to_string()),
            pa_ipv6: None,
            dpu_id: 6,
            orchagent_zmq_port: 8100,
            swbus_port: 23612,
            midplane_ipv4: "169.254.1.6".to_string(),
            vdpu_id: Some("vpdu6".to_string()),
        };

        assert_eq!(config_fromdb, expected);
    }

    fn populate_configdb_for_test() {
        let db: DbConnector = DbConnector::new_named("CONFIG_DB", false, 0).unwrap();
        let table = Table::new(db, "DPU").unwrap();

        // create local dpu table first
        for d in 6..8 {
            let dpu_fvs = vec![
                ("pa_ipv4".to_string(), Ipv4Addr::new(1, 2, 3, d).to_string()),
                ("vip_ipv4".to_string(), Ipv4Addr::new(4, 5, 6, d).to_string()),
                ("dpu_id".to_string(), d.to_string()),
                ("orchagent_zmq_port".to_string(), "8100".to_string()),
                ("swbus_port".to_string(), (23606 + d as u16).to_string()),
                ("midplane_ipv4".to_string(), Ipv4Addr::new(169, 254, 1, d).to_string()),
                ("vdpu_id".to_string(), format!("vpdu{d}")),
            ];
            table.set(&d.to_string(), dpu_fvs).unwrap();
        }
    }

    #[test]
    fn test_deserialize_dpu_state() {
        let json = r#"
        {
            "dpu_midplane_link_state": "up",
            "dpu_midplane_link_time": "Mon Jun 10 03:15:42 PM UTC 2024",
            "dpu_control_plane_state": "down",
            "dpu_control_plane_time": "Tue Jun 11 09:30:15 AM UTC 2024",
            "dpu_data_plane_state": "unknown",
            "dpu_data_plane_time": "Wed Jun 12 11:45:30 PM UTC 2024"
        }"#;

        let fvs: FieldValues = serde_json::from_str(json).unwrap();
        let dpu_state: DpuState = swss_serde::from_field_values(&fvs).unwrap();

        assert_eq!(dpu_state.dpu_midplane_link_state, DpuPmonStateType::Up);
        assert_eq!(dpu_state.dpu_control_plane_state, DpuPmonStateType::Down);
        assert_eq!(dpu_state.dpu_data_plane_state, DpuPmonStateType::Unknown);

        // Verify timestamps are parsed correctly (approximate values)
        assert!(dpu_state.dpu_midplane_link_time > 0);
        assert!(dpu_state.dpu_control_plane_time > 0);
        assert!(dpu_state.dpu_data_plane_time > 0);
    }

    #[test]
    fn test_serialize_dpu_state() {
        let dpu_state = DpuState {
            dpu_midplane_link_state: DpuPmonStateType::Up,
            dpu_midplane_link_time: 1718053542000, // Mon Jun 10 03:15:42 PM UTC 2024
            dpu_control_plane_state: DpuPmonStateType::Down,
            dpu_control_plane_time: 1718096215000, // Tue Jun 11 09:30:15 AM UTC 2024
            dpu_data_plane_state: DpuPmonStateType::Unknown,
            dpu_data_plane_time: 1718231130000, // Wed Jun 12 11:45:30 PM UTC 2024
        };

        let serialized = serde_json::to_string(&dpu_state).unwrap();

        // Verify the JSON contains expected timestamp format
        assert!(serialized.contains("Mon Jun 10"));
        assert!(serialized.contains("PM UTC 2024"));
        assert!(serialized.contains("\"dpu_midplane_link_state\":\"up\""));
        assert!(serialized.contains("\"dpu_control_plane_state\":\"down\""));
        assert!(serialized.contains("\"dpu_data_plane_state\":\"unknown\""));
    }

    #[test]
    fn test_dpu_state_default_values() {
        let json = r#"
        {
            "dpu_midplane_link_state": "up",
            "dpu_midplane_link_time": "Mon Jun 10 03:15:42 PM UTC 2024"
        }"#;

        let fvs: FieldValues = serde_json::from_str(json).unwrap();
        let dpu_state: DpuState = swss_serde::from_field_values(&fvs).unwrap();

        // Test default values
        assert_eq!(dpu_state.dpu_midplane_link_state, DpuPmonStateType::Up);
        assert_eq!(dpu_state.dpu_control_plane_state, DpuPmonStateType::Unknown);
        assert_eq!(dpu_state.dpu_data_plane_state, DpuPmonStateType::Unknown);

        // Default timestamps should be current time (approximately)
        let now = chrono::Utc::now().timestamp_millis();
        assert!(dpu_state.dpu_midplane_link_time > 0);
        assert!((dpu_state.dpu_control_plane_time - now).abs() < 1000); // within 1 second
        assert!((dpu_state.dpu_data_plane_time - now).abs() < 1000); // within 1 second
    }

    #[test]
    fn test_timestamp_roundtrip() {
        let original_timestamp = 1718053542000i64; // Mon Jun 10 03:15:42 PM UTC 2024

        // Serialize timestamp to string
        let datetime = DateTime::from_timestamp_millis(original_timestamp).unwrap();
        let formatted = datetime.format(TIMESTAMP_FORMAT).to_string();

        // Deserialize back to timestamp
        let parsed = chrono::NaiveDateTime::parse_from_str(&formatted, TIMESTAMP_FORMAT).unwrap();
        let roundtrip_timestamp = parsed.and_utc().timestamp_millis();

        assert_eq!(original_timestamp, roundtrip_timestamp);
    }

    #[test]
    fn test_deserialize_dash_bfd_probe_state() {
        let json = r#"
        {
            "v4_bfd_up_sessions": "10.0.1.1,10.0.1.2,10.0.1.3",
            "v4_bfd_up_sessions_timestamp": "Mon Jun 10 03:15:42 PM UTC 2024",
            "v6_bfd_up_sessions": "2001:db8::1,2001:db8::2",
            "v6_bfd_up_sessions_timestamp": "Tue Jun 11 09:30:15 AM UTC 2024"
        }"#;

        let fvs: FieldValues = serde_json::from_str(json).unwrap();
        let bfd_state: DashBfdProbeState = swss_serde::from_field_values(&fvs).unwrap();

        assert_eq!(bfd_state.v4_bfd_up_sessions.len(), 3);
        assert_eq!(bfd_state.v4_bfd_up_sessions[0], "10.0.1.1");
        assert_eq!(bfd_state.v4_bfd_up_sessions[1], "10.0.1.2");
        assert_eq!(bfd_state.v4_bfd_up_sessions[2], "10.0.1.3");

        assert_eq!(bfd_state.v6_bfd_up_sessions.len(), 2);
        assert_eq!(bfd_state.v6_bfd_up_sessions[0], "2001:db8::1");
        assert_eq!(bfd_state.v6_bfd_up_sessions[1], "2001:db8::2");

        // Verify timestamps are parsed correctly
        assert!(bfd_state.v4_bfd_up_sessions_timestamp > 0);
        assert!(bfd_state.v6_bfd_up_sessions_timestamp > 0);
    }

    #[test]
    fn test_serialize_dash_bfd_probe_state() {
        let bfd_state = DashBfdProbeState {
            v4_bfd_up_sessions: vec!["10.0.1.1".to_string(), "10.0.1.2".to_string()],
            v4_bfd_up_sessions_timestamp: 1718053542000, // Mon Jun 10 03:15:42 PM UTC 2024
            v6_bfd_up_sessions: vec!["2001:db8::1".to_string()],
            v6_bfd_up_sessions_timestamp: 1718096215000, // Tue Jun 11 09:30:15 AM UTC 2024
        };

        let serialized = serde_json::to_string(&bfd_state).unwrap();

        // Verify comma-separated serialization and timestamp format
        assert!(serialized.contains("10.0.1.1,10.0.1.2"));
        assert!(serialized.contains("2001:db8::1"));
        assert!(serialized.contains("Mon Jun 10"));
        assert!(serialized.contains("Tue Jun 11"));
        assert!(serialized.contains("PM UTC 2024"));
        assert!(serialized.contains("AM UTC 2024"));
    }

    #[test]
    fn test_dash_bfd_probe_state_empty_sessions() {
        let json = r#"
        {
            "v4_bfd_up_sessions": "",
            "v6_bfd_up_sessions": ""
        }"#;

        let fvs: FieldValues = serde_json::from_str(json).unwrap();
        let bfd_state: DashBfdProbeState = swss_serde::from_field_values(&fvs).unwrap();

        assert_eq!(bfd_state.v4_bfd_up_sessions.len(), 0);
        assert_eq!(bfd_state.v6_bfd_up_sessions.len(), 0);

        // Default timestamps should be current time (approximately)
        let now = chrono::Utc::now().timestamp_millis();
        assert!((bfd_state.v4_bfd_up_sessions_timestamp - now).abs() < 1000);
        assert!((bfd_state.v6_bfd_up_sessions_timestamp - now).abs() < 1000);
    }

    #[test]
    fn test_dash_bfd_probe_state_partial_data() {
        let json = r#"
        {
            "v4_bfd_up_sessions": "10.1.1.1,10.1.1.2",
            "v4_bfd_up_sessions_timestamp": "Mon Jun 10 03:15:42 PM UTC 2024"
        }"#;

        let fvs: FieldValues = serde_json::from_str(json).unwrap();
        let bfd_state: DashBfdProbeState = swss_serde::from_field_values(&fvs).unwrap();

        assert_eq!(bfd_state.v4_bfd_up_sessions.len(), 2);
        assert_eq!(bfd_state.v4_bfd_up_sessions[0], "10.1.1.1");
        assert_eq!(bfd_state.v4_bfd_up_sessions[1], "10.1.1.2");

        // v6 should use defaults
        assert_eq!(bfd_state.v6_bfd_up_sessions.len(), 0);

        assert!(bfd_state.v4_bfd_up_sessions_timestamp > 0);

        // v6 timestamp should use default (current time)
        let now = chrono::Utc::now().timestamp_millis();
        assert!((bfd_state.v6_bfd_up_sessions_timestamp - now).abs() < 1000);
    }
}

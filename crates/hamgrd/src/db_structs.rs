use anyhow::{Context, Result};
use chrono::DateTime;
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use serde_with::{formats::CommaSeparator, serde_as, skip_serializing_none, StringWithSeparator};
use sonic_dash_api_proto::{ha_scope_config::HaScopeConfig, ha_set_config::HaSetConfig, types::*};
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
#[skip_serializing_none]
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

#[skip_serializing_none]
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

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2121-ha-set-configurations>
#[serde_as]
#[skip_serializing_none]
#[derive(Serialize, Deserialize, SonicDb)]
#[sonicdb(table_name = "DASH_HA_SET_CONFIG_TABLE", key_separator = ":", db_name = "APPL_DB")]
pub struct DashHaSetConfigTable {
    pub ha_set_config: HaSetConfig,
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2311-ha-set-configurations>
#[skip_serializing_none]
#[derive(Serialize, Deserialize, Default, PartialEq, Eq, SonicDb)]
#[sonicdb(table_name = "DASH_HA_SET_TABLE", key_separator = ":", db_name = "DPU_APPL_DB")]
pub struct DashHaSetTable {
    // Config version.
    pub version: String,
    // IPv4 Data path VIP.
    pub vip_v4: String,
    // IPv4 Data path VIP.
    pub vip_v6: Option<String>,
    // Owner of HA state machine. It can be controller, switch.
    pub owner: Option<String>,
    // Scope of HA set. It can be dpu, eni.
    pub scope: Option<String>,
    // The IP address of local NPU. It can be IPv4 or IPv6. Used for setting up the BFD session.
    pub local_npu_ip: String,
    // The IP address of local DPU.
    pub local_ip: String,
    // The IP address of peer DPU.
    pub peer_ip: String,
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
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/vxlan/Overlay%20ECMP%20ehancements.md#22-app-db>
#[skip_serializing_none]
#[serde_as]
#[derive(Debug, Deserialize, Serialize, PartialEq, SonicDb)]
#[sonicdb(table_name = "VNET_ROUTE_TUNNEL_TABLE", key_separator = ":", db_name = "APPL_DB")]
pub struct VnetRouteTunnelTable {
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub endpoint: Vec<String>,
    #[serde_as(as = "Option<StringWithSeparator::<CommaSeparator, String>>")]
    pub endpoint_monitor: Option<Vec<String>>,
    pub monitoring: Option<String>,
    #[serde_as(as = "Option<StringWithSeparator::<CommaSeparator, String>>")]
    pub primary: Option<Vec<String>>,
    pub rx_monitor_timer: Option<u32>,
    pub tx_monitor_timer: Option<u32>,
    pub check_directly_connected: Option<bool>,
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-dpu-scope-dpu-driven-setup.md#2122-ha-scope-configurations>
#[skip_serializing_none]
#[serde_as]
#[derive(Debug, Deserialize, Serialize, PartialEq, SonicDb)]
#[sonicdb(table_name = "DASH_HA_SCOPE_CONFIG_TABLE", key_separator = ":", db_name = "APPL_DB")]
pub struct DashHaScopeConfigTable {
    pub ha_scope_config: HaScopeConfig,
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2312-ha-scope-configurations>
#[skip_serializing_none]
#[serde_as]
#[derive(Debug, Deserialize, Serialize, PartialEq, SonicDb)]
#[sonicdb(table_name = "DASH_HA_SCOPE_TABLE", key_separator = ":", db_name = "DPU_APPL_DB")]
pub struct DashHaScopeTable {
    pub version: u32,
    pub disable: bool,
    pub ha_role: String,
    pub flow_reconcile_requested: bool,
    pub activate_role_requested: bool,
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2342-ha-scope-state>
#[derive(Debug, Deserialize, Serialize, PartialEq, Default, Clone, SonicDb)]
#[sonicdb(table_name = "DASH_HA_SCOPE_STATE", key_separator = "|", db_name = "DPU_STATE_DB")]
pub struct DpuDashHaScopeState {
    // The last update time of this state in milliseconds.
    pub last_updated_time: i64,
    // The current HA role confirmed by ASIC. Please refer to the HA states defined in HA HLD.
    pub ha_role: String,
    // The time when HA role is moved into current one in milliseconds.
    pub ha_role_start_time: i64,
    // The current term confirmed by ASIC.
    pub ha_term: String,
    // DPU is pending on role activation.
    pub activate_role_pending: bool,
    // Flow reconcile is requested and pending approval.
    pub flow_reconcile_pending: bool,
    // Brainsplit is detected, and DPU is pending on recovery.
    pub brainsplit_recover_pending: bool,
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2342-ha-scope-state>
#[skip_serializing_none]
#[serde_as]
#[derive(Debug, Deserialize, Serialize, PartialEq, Default, Clone, SonicDb)]
#[sonicdb(table_name = "DASH_HA_SCOPE_STATE", key_separator = "|", db_name = "STATE_DB")]
pub struct NpuDashHaScopeState {
    // HA scope creation time in milliseconds.
    pub creation_time_in_ms: i64, /*todo: where is this from */
    // Last heartbeat time in milliseconds. This is used for leak detection.
    // Heartbeat time happens once per minute and will not change the last state updated time.
    pub last_heartbeat_time_in_ms: i64, /*todo: what is heartbeat */
    // Data path VIP of the DPU or ENI
    pub vip_v4: String,
    // Data path VIP of the DPU or ENI
    pub vip_v6: Option<String>,
    // The IP address of the DPU.
    pub local_ip: String,
    // The IP address of the peer DPU.
    pub peer_ip: String,

    // The state of the HA state machine. This is the state in NPU hamgrd.
    // The state of the HA state machine. This is the state in NPU hamgrd.
    pub local_ha_state: Option<String>,
    // The time when local target HA state is set.
    pub local_ha_state_last_updated_time_in_ms: Option<i64>,
    // The reason of the last HA state change.
    pub local_ha_state_last_updated_reason: Option<String>,
    // The target HA state in ASIC. This is the state that hamgrd generates and asking DPU to move to.
    pub local_target_asic_ha_state: Option<String>,
    // The HA state that ASIC acked.
    pub local_acked_asic_ha_state: Option<String>,
    // The current target term of the HA state machine.
    pub local_target_term: Option<String>,
    // The current term that acked by ASIC.
    pub local_acked_term: Option<String>,
    // The state of the HA state machine in peer DPU.
    pub peer_ha_state: Option<String>, /*todo: we don't know peer dpu state */
    // The current term in peer DPU.
    pub peer_term: Option<String>,

    // The state of local vDPU midplane. The value can be "unknown", "up", "down".
    pub local_vdpu_midplane_state: DpuPmonStateType,
    // Local vDPU midplane state last updated time in milliseconds.
    pub local_vdpu_midplane_state_last_updated_time_in_ms: i64,
    // The state of local vDPU control plane, which includes DPU OS and certain required firmware. The value can be "unknown", "up", "down".
    pub local_vdpu_control_plane_state: DpuPmonStateType,
    // Local vDPU control plane state last updated time in milliseconds.
    pub local_vdpu_control_plane_state_last_updated_time_in_ms: i64,
    // The state of local vDPU data plane, which includes DPU hardware / ASIC and certain required firmware. The value can be "unknown", "up", "down".
    pub local_vdpu_data_plane_state: DpuPmonStateType,
    // Local vDPU data plane state last updated time in milliseconds.
    pub local_vdpu_data_plane_state_last_updated_time_in_ms: i64,
    // The list of IPv4 peer IPs (NPU IP) of the BFD sessions in up state.
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub local_vdpu_up_bfd_sessions_v4: Vec<String>,
    // Local vDPU BFD sessions v4 last updated time in milliseconds.
    pub local_vdpu_up_bfd_sessions_v4_update_time_in_ms: i64,
    // The list of IPv6 peer IPs (NPU IP) of the BFD sessions in up state.
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub local_vdpu_up_bfd_sessions_v6: Vec<String>,
    // Local vDPU BFD sessions v6 last updated time in milliseconds.
    pub local_vdpu_up_bfd_sessions_v6_update_time_in_ms: i64,

    // GUIDs of pending operation IDs, connected by ","
    #[serde_as(as = "Option<StringWithSeparator::<CommaSeparator, String>>")]
    pub pending_operation_ids: Option<Vec<String>>,
    // Type of pending operations, e.g. "switchover", "activate_role", "flow_reconcile", "brainsplit_recover". Connected by ","
    #[serde_as(as = "Option<StringWithSeparator::<CommaSeparator, String>>")]
    pub pending_operation_types: Option<Vec<String>>,
    // Last updated time of the pending operation list.
    pub pending_operation_list_last_updated_time_in_ms: Option<i64>,
    // Switchover ID (GUID).
    pub switchover_id: Option<String>,
    // Switchover state. It can be "pending_approval", "approved", "in_progress", "completed", "failed"
    pub switchover_state: Option<String>,
    // The time when operation is created.
    pub switchover_start_time_in_ms: Option<i64>,
    // The time when operation is ended.
    pub switchover_end_time_in_ms: Option<i64>,
    // The time when operation is approved.
    pub switchover_approved_time_in_ms: Option<i64>,
    // Flow sync session ID.
    pub flow_sync_session_id: Option<String>,
    // Flow sync session state. It can be "in_progress", "completed", "failed"
    pub flow_sync_session_state: Option<String>,
    // Flow sync start time in milliseconds.
    pub flow_sync_session_start_time_in_ms: Option<i64>,
    // The IP endpoint of the server that flow records are sent to.
    pub flow_sync_session_target_server: Option<String>,
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

pub fn ip_to_string(ip: &IpAddress) -> String {
    match &ip.ip {
        Some(sonic_dash_api_proto::types::ip_address::Ip::Ipv4(addr)) => std::net::Ipv4Addr::from(*addr).to_string(),
        Some(sonic_dash_api_proto::types::ip_address::Ip::Ipv6(addr)) => {
            use std::net::Ipv6Addr;
            let bytes: [u8; 16] = addr.clone().try_into().unwrap_or([0; 16]);
            Ipv6Addr::from(bytes).to_string()
        }
        _ => "".to_string(),
    }
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
    fn test_serde_vnet_route_tunnel() {
        let json = r#"
        {
            "key": "default|3.2.1.0/32",
            "operation": "Set",
            "field_values": {
                "endpoint": "1.2.3.4,2.2.3.4",
                "endpoint_monitor": "1.2.3.5,2.2.3.5"
            }
        }"#;
        let kfv: KeyOpFieldValues = serde_json::from_str(json).unwrap();
        let vnet: VnetRouteTunnelTable = swss_serde::from_field_values(&kfv.field_values).unwrap();
        println!("{vnet:?}");
        assert!(vnet.endpoint == vec!["1.2.3.4", "2.2.3.4"]);
        assert!(vnet.endpoint_monitor == Some(vec!["1.2.3.5".into(), "2.2.3.5".into()]));
        assert!(vnet.monitoring.is_none());
        let fvs = swss_serde::to_field_values(&vnet).unwrap();
        println!("{fvs:?}");
        assert!(fvs["endpoint"] == "1.2.3.4,2.2.3.4");
        assert!(fvs["endpoint_monitor"] == "1.2.3.5,2.2.3.5");
        assert!(!fvs.contains_key("monitoring"));
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

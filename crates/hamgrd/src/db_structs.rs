// temporarily disable unused warning until vdpu/ha-set actors are implemented
#![allow(unused)]
use anyhow::{Context, Result};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::formats::CommaSeparator;
use serde_with::{serde_as, skip_serializing_none, StringWithSeparator};
use std::fmt;
use swss_common::{DbConnector, Table};
use swss_serde::from_table;

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2112-ha-global-configurations>
#[skip_serializing_none]
#[derive(Serialize, Deserialize, Default, Debug)]
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
#[derive(Deserialize, Serialize, Clone, PartialEq, Eq, Debug)]
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
#[derive(Serialize, Deserialize, Clone)]
pub struct RemoteDpu {
    pub pa_ipv4: String,
    pub pa_ipv6: Option<String>,
    pub npu_ipv4: String,
    pub npu_ipv6: Option<String>,
    pub dpu_id: u32,
    pub swbus_port: u16,
}

#[skip_serializing_none]
#[derive(Serialize, Deserialize)]
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

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2121-ha-set-configurations>
/// in APPL_DB
#[serde_as]
#[skip_serializing_none]
#[derive(Serialize, Deserialize)]
pub struct DashHaSetConfigTable {
    pub version: String,
    pub vip_v4: String,
    pub vip_v6: Option<String>,
    // dpu or switch
    pub owner: Option<String>,
    // dpu or eni
    pub scope: Option<String>,
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub vdpu_ids: Vec<String>,
    pub pinned_vdpu_bfd_probe_states: Option<String>,
    #[serde_as(as = "Option<StringWithSeparator::<CommaSeparator, String>>")]
    pub preferred_vdpu_ids: Option<Vec<String>>,
    pub preferred_standalone_vdpu_index: Option<u32>,
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2311-ha-set-configurations>
/// In DPU_APPL_DB
#[skip_serializing_none]
#[derive(Serialize, Deserialize, Default, PartialEq, Eq)]
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

/// https://github.com/sonic-net/SONiC/blob/master/doc/vxlan/Overlay%20ECMP%20ehancements.md#22-app-db
/// In APPL_DB
#[skip_serializing_none]
#[serde_as]
#[derive(Debug, Deserialize, Serialize, PartialEq)]
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

/// https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-dpu-scope-dpu-driven-setup.md#2122-ha-scope-configurations
/// In APPL_DB
#[skip_serializing_none]
#[serde_as]
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct DashHaScopeConfigTable {
    pub version: String,
    pub disable: bool,
    pub desired_ha_state: String,
    #[serde_as(as = "Option<StringWithSeparator::<CommaSeparator, String>>")]
    pub approved_pending_operation_ids: Option<Vec<String>>,
}

/// https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-dpu-scope-dpu-driven-setup.md#2312-ha-scope-configurations
/// In DPU_APPL_DB
#[skip_serializing_none]
#[serde_as]
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct DashHaScopeTable {
    pub version: String,
    pub disable: bool,
    pub ha_role: String,
    pub flow_reconcile_requested: bool,
    pub activate_role_requested: bool
}

/// https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2342-ha-scope-state
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct DpuDashHaScopeState {
    // The last update time of this state in milliseconds.
    pub last_updated_time: u64,
    // The current HA role confirmed by ASIC. Please refer to the HA states defined in HA HLD.
    pub ha_role: String,
    // The time when HA role is moved into current one in milliseconds.	
    pub ha_role_start_time: u64,
    // The current term confirmed by ASIC.
    pub ha_term: String,
    // DPU is pending on role activation.
    pub activate_role_pending: bool,
    // Flow reconcile is requested and pending approval.
    pub flow_reconcile_pending: bool,
    // Brainsplit is detected, and DPU is pending on recovery.
    pub brainsplit_recover_pending: bool
}

/// https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2342-ha-scope-state
#[skip_serializing_none]
#[serde_as]
#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct NpuDashHaScopeState {
    // HA scope creation time in milliseconds.
    pub creation_time_in_ms: u64, /*todo: where is this from */
    // Last heartbeat time in milliseconds. This is used for leak detection. 
    // Heartbeat time happens once per minute and will not change the last state updated time.
    pub last_heartbeat_time_in_ms: u64, /*todo: what is heartbeat */
    // Data path VIP of the DPU or ENI
    pub vip_v4: String,
    // Data path VIP of the DPU or ENI
    pub vip_v6: String,
    // The IP address of the DPU.
    pub local_ip: String,
    // The IP address of the peer DPU.
    pub peer_ip: String,

    // The state of the HA state machine. This is the state in NPU hamgrd.
    pub local_ha_state: String,
    // The time when local target HA state is set.
    pub local_ha_state_last_updated_time_in_ms: u64,
    // The reason of the last HA state change.
    pub local_ha_state_last_updated_reason: String,
    // The target HA state in ASIC. This is the state that hamgrd generates and asking DPU to move to.
    pub local_target_asic_ha_state: String,
    // The HA state that ASIC acked.
    pub local_acked_asic_ha_state: String,
    // The current target term of the HA state machine.
    pub local_target_term: String,
    // The current term that acked by ASIC.
    pub local_acked_term: String,
    // The state of the HA state machine in peer DPU.
    pub peer_ha_state: Option<String>, /*todo: we don't know peer dpu state */
    // The current term in peer DPU.
    pub peer_term: Option<String>,

    // The state of local vDPU midplane. The value can be "unknown", "up", "down".
    pub local_vdpu_midplane_state: String,
    // Local vDPU midplane state last updated time in milliseconds.
    pub local_vdpu_midplane_state_last_updated_time_in_ms: u64,
    // The state of local vDPU control plane, which includes DPU OS and certain required firmware. The value can be "unknown", "up", "down".
    pub local_vdpu_control_plane_state: String,
    // Local vDPU control plane state last updated time in milliseconds.
    pub local_vdpu_control_plane_state_last_updated_time_in_ms: u64,
    // The state of local vDPU data plane, which includes DPU hardware / ASIC and certain required firmware. The value can be "unknown", "up", "down".
    pub local_vdpu_data_plane_state: String,
    // Local vDPU data plane state last updated time in milliseconds.
    pub local_vdpu_data_plane_state_last_updated_time_in_ms: u64,
    // The list of IPv4 peer IPs (NPU IP) of the BFD sessions in up state.
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub local_vdpu_up_bfd_sessions_v4: Vec<String>,
    // Local vDPU BFD sessions v4 last updated time in milliseconds.
    pub local_vdpu_up_bfd_sessions_v4_update_time_in_ms: u64,
    // The list of IPv6 peer IPs (NPU IP) of the BFD sessions in up state.
    #[serde_as(as = "StringWithSeparator::<CommaSeparator, String>")]
    pub local_vdpu_up_bfd_sessions_v6: Vec<String>,
    // Local vDPU BFD sessions v6 last updated time in milliseconds.
    pub local_vdpu_up_bfd_sessions_v6_update_time_in_ms: u64,

    // GUIDs of pending operation IDs, connected by ","
    #[serde_as(as = "Option<StringWithSeparator::<CommaSeparator, String>>")]
    pub pending_operation_ids: Option<Vec<String>>,
    // Type of pending operations, e.g. "switchover", "activate_role", "flow_reconcile", "brainsplit_recover". Connected by ","
    #[serde_as(as = "Option<StringWithSeparator::<CommaSeparator, String>>")]
    pub pending_operation_types: Option<Vec<String>>,
    // Last updated time of the pending operation list.
    pub pending_operation_list_last_updated_time_in_ms: Option<u64>,
    // Switchover ID (GUID).
    pub switchover_id: Option<String>,
    // Switchover state. It can be "pending_approval", "approved", "in_progress", "completed", "failed"
    pub switchover_state: Option<String>,
    // The time when operation is created.
    pub switchover_start_time_in_ms: Option<u64>,
    // The time when operation is ended.
    pub switchover_end_time_in_ms: Option<u64>,
    // The time when operation is approved.
    pub switchover_approved_time_in_ms: Option<u64>,
    // Flow sync session ID.
    pub flow_sync_session_id: Option<String>,
    // Flow sync session state. It can be "in_progress", "completed", "failed"
    pub flow_sync_session_state: Option<String>,
    // Flow sync start time in milliseconds.
    pub flow_sync_session_start_time_in_ms: Option<u64>,
    // The IP endpoint of the server that flow records are sent to.
    pub flow_sync_session_target_server: Option<String>,
}

pub fn get_dpu_config_from_db(dpu_id: u32) -> Result<Dpu> {
    let db = DbConnector::new_named("CONFIG_DB", false, 0).context("connecting config_db")?;
    let table = Table::new(db, "DPU").context("opening DPU table")?;

    let keys = table.get_keys().context("Failed to get keys from DPU table")?;

    for key in keys {
        let dpu: Dpu = from_table(&table, &key).context(format!("reading DPU entry {}", key))?;

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
    use swss_common::KeyOpFieldValues;
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
        println!("{:?}", vnet);
        assert!(vnet.endpoint == vec!["1.2.3.4", "2.2.3.4"]);
        assert!(vnet.endpoint_monitor == Some(vec!["1.2.3.5".into(), "2.2.3.5".into()]));
        assert!(vnet.monitoring.is_none());
        let fvs = swss_serde::to_field_values(&vnet).unwrap();
        println!("{:?}", fvs);
        assert!(fvs["endpoint"] == "1.2.3.4,2.2.3.4");
        assert!(fvs["endpoint_monitor"] == "1.2.3.5,2.2.3.5");
        assert!(!fvs.contains_key("monitoring"));
    }

    #[test]
    fn test_get_dpu_config_from_db() {
        let _ = Redis::start_config_db();

        // Populate the CONFIG_DB for testing
        populate_configdb_for_test();

        let config_fromdb = get_dpu_config_from_db(0).unwrap();

        let expected = Dpu {
            state: None,
            vip_ipv6: None,
            pa_ipv4: "1.2.3.0".to_string(),
            vip_ipv4: Some("4.5.6.0".to_string()),
            pa_ipv6: None,
            dpu_id: 0,
            orchagent_zmq_port: 8100,
            swbus_port: 23606,
            midplane_ipv4: "169.254.1.0".to_string(),
            vdpu_id: Some("vpdu0".to_string()),
        };

        assert_eq!(config_fromdb, expected);
    }

    fn populate_configdb_for_test() {
        let db: DbConnector = DbConnector::new_named("CONFIG_DB", false, 0).unwrap();
        let table = Table::new(db, "DPU").unwrap();

        // create local dpu table first
        for d in 0..2 {
            let dpu_fvs = vec![
                ("pa_ipv4".to_string(), Ipv4Addr::new(1, 2, 3, d).to_string()),
                ("vip_ipv4".to_string(), Ipv4Addr::new(4, 5, 6, d).to_string()),
                ("dpu_id".to_string(), d.to_string()),
                ("orchagent_zmq_port".to_string(), "8100".to_string()),
                ("swbus_port".to_string(), (23606 + d as u16).to_string()),
                ("midplane_ipv4".to_string(), Ipv4Addr::new(169, 254, 1, d).to_string()),
                ("vdpu_id".to_string(), format!("vpdu{}", d)),
            ];
            table.set(&d.to_string(), dpu_fvs).unwrap();
        }
    }
}

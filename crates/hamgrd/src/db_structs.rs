// temporarily disable unused warning until vdpu/ha-set actors are implemented
#![allow(unused)]
//use serde::{Deserialize, Serialize};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_with::formats::CommaSeparator;
use serde_with::{serde_as, skip_serializing_none, StringWithSeparator};
use std::fmt;

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2112-ha-global-configurations>
#[skip_serializing_none]
#[derive(Serialize, Deserialize, Default)]
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
#[derive(Deserialize, Serialize, Clone, PartialEq, Eq)]
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
#[skip_serializing_none]
#[derive(Serialize, Deserialize)]
pub struct DashHaSetConfigTable {
    pub version: Option<String>,
    pub vip_v4: Option<String>,
    pub vip_v6: Option<String>,
    // dpu or switch
    pub owner: Option<String>,
    // dpu or eni
    pub scope: Option<String>,
    pub vdpu_ids: Option<String>,
    pub pinned_vdpu_bfd_probe_states: Option<String>,
    pub preferred_vdpu_ids: Option<String>,
    pub preferred_standalone_vdpu_index: Option<u32>,
}

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2311-ha-set-configurations>
#[skip_serializing_none]
#[derive(Serialize, Deserialize, Default)]
pub struct DashHaSetTable {
    // Config version.
    pub version: Option<String>,
    // IPv4 Data path VIP.
    pub vip_v4: Option<String>,
    // IPv4 Data path VIP.
    pub vip_v6: Option<String>,
    // Owner of HA state machine. It can be controller, switch.
    pub owner: Option<String>,
    // Scope of HA set. It can be dpu, eni.
    pub scope: Option<String>,
    // The IP address of local NPU. It can be IPv4 or IPv6. Used for setting up the BFD session.
    pub local_npu_ip: Option<String>,
    // The IP address of local DPU.
    pub local_ip: Option<String>,
    // The IP address of peer DPU.
    pub peer_ip: Option<String>,
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
#[skip_serializing_none]
#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
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

#[cfg(test)]
mod test {
    use super::*;
    use swss_common::KeyOpFieldValues;
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
        assert!(vnet.monitoring == None);
        let fvs = swss_serde::to_field_values(&vnet).unwrap();
        println!("{:?}", fvs);
        assert!(fvs["endpoint"] == "1.2.3.4,2.2.3.4");
        assert!(fvs["endpoint_monitor"] == "1.2.3.5,2.2.3.5");
        assert!(fvs.get("monitoring").is_none());

    }
}

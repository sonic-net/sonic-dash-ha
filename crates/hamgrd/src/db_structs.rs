// temporarily disable unused warning until vdpu/ha-set actors are implemented
#![allow(unused)]
use serde::{Deserialize, Serialize};

/// <https://github.com/sonic-net/SONiC/blob/master/doc/smart-switch/high-availability/smart-switch-ha-detailed-design.md#2112-ha-global-configurations>
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
#[derive(Deserialize, Clone)]
pub struct RemoteDpu {
    pub pa_ipv4: Option<String>,
    pub pa_ipv6: Option<String>,
    pub npu_ipv4: String,
    pub npu_ipv6: Option<String>,
    pub dpu_id: u32,
    pub swbus_port: Option<u16>,
}

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
}

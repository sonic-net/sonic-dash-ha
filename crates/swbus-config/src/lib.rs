use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use swbus_proto::swbus::*;
use swss_common::{DbConnector, Table};
use swss_serde::from_table;
use thiserror::Error;
use tracing::*;

const CONFIG_DB: &str = "CONFIG_DB";
pub const SWBUSD_PORT: u16 = 23606;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct SwbusConfig {
    pub endpoint: SocketAddr,
    pub routes: Vec<RouteConfig>,
    pub peers: Vec<PeerConfig>,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, Deserialize)]
pub struct RouteConfig {
    #[serde(deserialize_with = "deserialize_service_path")]
    pub key: ServicePath,
    pub scope: RouteScope,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PeerConfig {
    #[serde(deserialize_with = "deserialize_service_path")]
    pub id: ServicePath,
    pub endpoint: SocketAddr,
    pub conn_type: ConnectionType,
}

#[derive(Error, Debug)]
pub enum SwbusConfigError {
    #[error("InvalidConfig: {detail}")]
    InvalidConfig { detail: String },

    #[error("IOError: {detail}")]
    IOError { detail: String },

    #[error("InternalError:{detail}")]
    Internal { detail: String },
}

impl SwbusConfigError {
    pub fn invalid_config(detail: String) -> Self {
        SwbusConfigError::InvalidConfig { detail }
    }

    pub fn io_error(detail: String) -> Self {
        SwbusConfigError::IOError { detail }
    }

    pub fn internal(detail: String) -> Self {
        SwbusConfigError::Internal { detail }
    }
}

pub type Result<T, E = SwbusConfigError> = core::result::Result<T, E>;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ConfigDBDPUEntry {
    #[serde(rename = "type")]
    pub dpu_type: Option<String>,
    pub state: Option<String>,
    pub slot_id: u32,
    pub pa_ipv4: Option<String>,
    pub pa_ipv6: Option<String>,
    pub npu_ipv4: Option<String>,
    pub npu_ipv6: Option<String>,
    pub probe_ip: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ConfigDBDeviceMetadataEntry {
    pub region: Option<String>,
    pub cluster: Option<String>,
    #[serde(rename = "type")]
    pub device_type: Option<String>,
    pub sub_type: Option<String>,
    pub hostname: Option<String>,
    pub platform: Option<String>,
    pub hwsku: Option<String>,
}

#[instrument]
fn route_config_from_dpu_entry(dpu_entry: &ConfigDBDPUEntry, region: &str, cluster: &str) -> Result<Vec<RouteConfig>> {
    let mut routes = Vec::new();
    let slot_id = dpu_entry.slot_id;

    debug!("Collecting routes for local dpu{}", slot_id);
    let dpu_type = dpu_entry
        .dpu_type
        .as_ref()
        .ok_or(SwbusConfigError::invalid_config("DPU type not found".to_string()))?;
    if dpu_type != "local" && dpu_type != "cluster" {
        // ignore external DPUs, not for DASH smartswitch
        return Err(SwbusConfigError::invalid_config(format!(
            "Unsupported DPU type: {}",
            dpu_type
        )));
    }

    if let Some(npu_ipv4) = dpu_entry.npu_ipv4.as_ref() {
        if npu_ipv4.parse::<Ipv4Addr>().is_err() {
            return Err(SwbusConfigError::invalid_config(format!(
                "Invalid IPv4 address: {}",
                npu_ipv4
            )));
        }

        let sp = ServicePath::with_node(region, cluster, &format!("{}-dpu{}", npu_ipv4, slot_id), "", "", "", "");
        routes.push(RouteConfig {
            key: sp,
            scope: RouteScope::Cluster,
        });
    }

    if let Some(npu_ipv6) = dpu_entry.npu_ipv6.as_ref() {
        if npu_ipv6.parse::<Ipv6Addr>().is_err() {
            return Err(SwbusConfigError::invalid_config(format!(
                "Invalid IPv6 address: {}",
                npu_ipv6
            )));
        }

        let sp = ServicePath::with_node(region, cluster, &format!("{}-dpu{}", npu_ipv6, slot_id), "", "", "", "");
        routes.push(RouteConfig {
            key: sp,
            scope: RouteScope::Cluster,
        });
    }

    if routes.is_empty() {
        SwbusConfigError::invalid_config(format!("No valid routes found in local dpu{}", slot_id));
    }

    debug!("Routes collected: {:?}", &routes);
    Ok(routes)
}

#[instrument]
fn peer_config_from_dpu_entry(
    dpu_id: &str,
    dpu_entry: ConfigDBDPUEntry,
    region: &str,
    cluster: &str,
) -> Result<Vec<PeerConfig>> {
    let mut peers = Vec::new();

    debug!("Collecting peer info for DPU: {}", dpu_id);
    let dpu_type = dpu_entry
        .dpu_type
        .as_ref()
        .ok_or(SwbusConfigError::invalid_config("DPU type not found".to_string()))?;
    if dpu_type != "local" && dpu_type != "cluster" {
        // ignore external DPUs, not for DASH smartswitch
        return Err(SwbusConfigError::invalid_config(format!(
            "Unsupported DPU type: {}",
            dpu_type
        )));
    }

    let slot_id = dpu_entry.slot_id;

    if let Some(npu_ipv4) = dpu_entry.npu_ipv4 {
        let npu_ipv4 = npu_ipv4
            .parse::<Ipv4Addr>()
            .map_err(|_| SwbusConfigError::invalid_config(format!("Invalid IPv4 address: {}", npu_ipv4)))?;

        let sp = ServicePath::with_node(region, cluster, &format!("{}-dpu{}", npu_ipv4, slot_id), "", "", "", "");
        peers.push(PeerConfig {
            id: sp,
            endpoint: SocketAddr::new(IpAddr::V4(npu_ipv4), SWBUSD_PORT + slot_id as u16),
            conn_type: ConnectionType::Cluster,
        });
    }

    if let Some(npu_ipv6) = dpu_entry.npu_ipv6 {
        let npu_ipv6 = npu_ipv6
            .parse::<Ipv6Addr>()
            .map_err(|_| SwbusConfigError::invalid_config(format!("Invalid IPv6 address: {}", npu_ipv6)))?;

        let sp = ServicePath::with_node(region, cluster, &format!("{}-dpu{}", npu_ipv6, slot_id), "", "", "", "");
        peers.push(PeerConfig {
            id: sp,
            endpoint: SocketAddr::new(IpAddr::V6(npu_ipv6), SWBUSD_PORT + slot_id as u16),
            conn_type: ConnectionType::Cluster,
        });
    }

    if peers.is_empty() {
        error!("No valid peers found in DPU: {}", dpu_id);
    }

    debug!("Peer info collected: {:?}", &peers);
    Ok(peers)
}

#[instrument]
fn get_device_info() -> Result<(String, String)> {
    let db = DbConnector::new_named(CONFIG_DB, false, 0)
        .map_err(|e| SwbusConfigError::io_error(format!("Failed to connect config_db: {}", e)))?;
    let table = Table::new(db, "DEVICE_METADATA")
        .map_err(|e| SwbusConfigError::internal(format!("Failed to open DEVICE_METADATA table: {}", e)))?;

    let metadata: ConfigDBDeviceMetadataEntry = from_table(&table, "localhost")
        .map_err(|e| SwbusConfigError::internal(format!("Failed to read DEVICE_METADATA:localhost entry: {}", e)))?;

    let region = metadata.region.ok_or(SwbusConfigError::invalid_config(
        "region not found in DEVICE_METADATA table".to_string(),
    ))?;
    let cluster = metadata.cluster.ok_or(SwbusConfigError::invalid_config(
        "cluster not found in DEVICE_METADATA table".to_string(),
    ))?;

    debug!("Region: {}, Cluster: {}", region, cluster);

    Ok((region, cluster))
}

#[instrument]
pub fn swbus_config_from_db(slot_id: u32) -> Result<SwbusConfig> {
    let mut peers = Vec::new();
    let mut myroutes: Option<Vec<RouteConfig>> = None;
    let mut myendpoint: Option<SocketAddr> = None;

    let (region, cluster) = get_device_info()?;

    let db = DbConnector::new_named(CONFIG_DB, false, 0)
        .map_err(|e| SwbusConfigError::io_error(format!("Failed to connect config_db: {}", e)))?;
    let table =
        Table::new(db, "DPU").map_err(|e| SwbusConfigError::internal(format!("Failed to open DPU table: {}", e)))?;

    let keys = table
        .get_keys()
        .map_err(|e| SwbusConfigError::internal(format!("Failed to get keys from DPU table: {}", e)))?;
    for key in keys {
        let dpu: ConfigDBDPUEntry = from_table(&table, &key)
            .map_err(|e| SwbusConfigError::internal(format!("Failed to read DPU entry {}: {}", key, e)))?;

        if dpu.dpu_type.as_ref().is_none() {
            error!("Missing type in DPU entry {}", key);
            continue;
        }
        let dpu_type = dpu.dpu_type.as_ref().unwrap();
        if dpu_type != "local" && dpu_type != "cluster" {
            // ignore external DPUs, not for DASH smartswitch
            debug!("Unsupported DPU type: {} in {}", dpu_type, key);
            continue;
        }

        // find the DPU entry for the slot
        if dpu_type == "local" && dpu.slot_id == slot_id {
            myroutes = Some(route_config_from_dpu_entry(&dpu, &region, &cluster).map_err(|e| {
                error!("Failed to collect routes for {}: {}", slot_id, e);
                e
            })?);

            if let Some(npu_ipv4) = dpu.npu_ipv4 {
                myendpoint = Some(SocketAddr::new(
                    IpAddr::V4(npu_ipv4.parse().expect("not expecting error")),
                    SWBUSD_PORT + slot_id as u16,
                ));
            } else if let Some(npu_ipv6) = dpu.npu_ipv6 {
                myendpoint = Some(SocketAddr::new(
                    IpAddr::V6(npu_ipv6.parse().expect("not expecting error")),
                    SWBUSD_PORT + slot_id as u16,
                ));
            }
            continue;
        }

        let dpu: ConfigDBDPUEntry = from_table(&table, &key)
            .map_err(|e| SwbusConfigError::internal(format!("Failed to read DPU entry {}: {}", key, e)))?;
        let peer = peer_config_from_dpu_entry(&key, dpu, &region, &cluster).map_err(|e| {
            error!("Failed to collect peers from {}: {}", key, e);
            e
        })?;
        peers.extend(peer);
    }
    if myroutes.is_none() {
        return Err(SwbusConfigError::invalid_config(format!(
            "DPU at slot {} is not found",
            slot_id
        )));
    }

    info!("successfully load swbus config from configdb for dpu {}", slot_id);

    Ok(SwbusConfig {
        endpoint: myendpoint.unwrap(),
        routes: myroutes.unwrap(),
        peers,
    })
}

pub fn swbus_config_from_yaml(yaml_file: &str) -> Result<SwbusConfig> {
    let file =
        File::open(yaml_file).map_err(|e| SwbusConfigError::io_error(format!("Failed to open YAML file: {}", e)))?;
    let reader = BufReader::new(file);

    // Parse the YAML data
    let swbus_config: SwbusConfig = serde_yaml::from_reader(reader)
        .map_err(|e| SwbusConfigError::invalid_config(format!("Failed to parse YAML file: {}", e)))?;
    Ok(swbus_config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use swss_common::{DbConnector, Table};
    use swss_common_testing::*;
    use swss_serde::to_table;
    use tempfile::tempdir;

    #[test]
    fn test_load_from_configdb() {
        let _ = Redis::start_config_db();

        let db = DbConnector::new_named(CONFIG_DB, false, 0).unwrap();
        let table = Table::new(db, "DEVICE_METADATA").unwrap();

        let metadata = ConfigDBDeviceMetadataEntry {
            region: Some("region-a".to_string()),
            cluster: Some("cluster-a".to_string()),
            device_type: Some("SpineRouter".to_string()),
            sub_type: Some("SmartSwitch".to_string()),
            hostname: Some("smartswitch1".to_string()),
            platform: Some("x86_64".to_string()),
            hwsku: Some("hwsku1".to_string()),
        };
        to_table(&metadata, &table, "localhost").unwrap();

        let db = DbConnector::new_named(CONFIG_DB, false, 0).unwrap();
        let table = Table::new(db, "DPU").unwrap();
        for s in 0..2 {
            for d in 0..2 {
                let dpu = ConfigDBDPUEntry {
                    dpu_type: if s == 0 {
                        Some("local".to_string())
                    } else {
                        Some("cluster".to_string())
                    },
                    state: Some("active".to_string()),
                    slot_id: d,
                    pa_ipv4: Some(format!("10.0.0.{}", s * 8 + d)),
                    pa_ipv6: Some(format!("2001:db8::{}", s * 8 + d)),
                    npu_ipv4: Some(format!("10.0.1.{}", s)),
                    npu_ipv6: Some(format!("2001:db8:1::{}", s)),
                    probe_ip: None,
                };
                to_table(&dpu, &table, &format!("dpu{}", s * 8 + d)).unwrap();

                assert_eq!(
                    table
                        .hget(&format!("dpu{}", s * 8 + d), "type")
                        .unwrap()
                        .as_ref()
                        .unwrap(),
                    if s == 0 { "local" } else { "cluster" }
                );
            }
        }

        let mut config_fromdb = swbus_config_from_db(0).unwrap();

        assert_eq!(config_fromdb.routes.len(), 2);
        assert_eq!(config_fromdb.peers.len(), 6);

        // create equivalent config in yaml
        let yaml_content = r#"
        endpoint: "10.0.1.0:23606"
        routes:
          - key: "region-a.cluster-a.10.0.1.0-dpu0"
            scope: "Cluster"
          - key: "region-a.cluster-a.2001:db8:1::0-dpu0"
            scope: "Cluster"
        peers:
          - id: "region-a.cluster-a.10.0.1.0-dpu1"
            endpoint: "10.0.1.0:23607"
            conn_type: "Cluster"
          - id: "region-a.cluster-a.2001:db8:1::-dpu1"
            endpoint: "[2001:db8:1::]:23607"
            conn_type: "Cluster"            
          - id: "region-a.cluster-a.10.0.1.1-dpu0"
            endpoint: "10.0.1.1:23606"
            conn_type: "Cluster"
          - id: "region-a.cluster-a.2001:db8:1::1-dpu0"
            endpoint: "[2001:db8:1::1]:23606"
            conn_type: "Cluster"   
          - id: "region-a.cluster-a.10.0.1.1-dpu1"
            endpoint: "10.0.1.1:23607"
            conn_type: "Cluster"
          - id: "region-a.cluster-a.2001:db8:1::1-dpu1"
            endpoint: "[2001:db8:1::1]:23607"
            conn_type: "Cluster"   
        "#;

        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_config.yaml");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(yaml_content.as_bytes()).unwrap();

        let mut expected = swbus_config_from_yaml(file_path.to_str().unwrap()).unwrap();

        // sort before compare
        config_fromdb.routes.sort_by(|a, b| a.key.cmp(&b.key));
        config_fromdb.peers.sort_by(|a, b| a.id.cmp(&b.id));
        expected.routes.sort_by(|a, b| a.key.cmp(&b.key));
        expected.peers.sort_by(|a, b| a.id.cmp(&b.id));
        assert_eq!(config_fromdb, expected);

        // clean up
        let db = DbConnector::new_named(CONFIG_DB, false, 0).unwrap();
        let table = Table::new(db, "DPU").unwrap();
        for s in 0..2 {
            for d in 0..2 {
                table.del(&format!("dpu{}", s * 8 + d)).unwrap();
            }
        }
    }

    #[test]
    fn test_load_from_yaml() {
        let yaml_content = r#"
        endpoint: 10.0.0.1:8000
        routes:
          - key: "region-a.cluster-a.10.0.0.1-dpu0"
            scope: "Cluster"
        peers:
          - id: "region-a.cluster-a.10.0.0.2-dpu0"
            endpoint: "10.0.0.2:8000"
            conn_type: "Cluster"
          - id: "region-a.cluster-a.10.0.0.3-dpu0"
            endpoint: "10.0.0.3:8000"
            conn_type: "Cluster"
        "#;

        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_config.yaml");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(yaml_content.as_bytes()).unwrap();

        let result = swbus_config_from_yaml(file_path.to_str().unwrap());
        match result {
            Ok(_) => {}
            Err(e) => {
                panic!("Failed to load config from yaml: {}", e);
            }
        }

        let config = result.unwrap();
        assert_eq!(config.routes.len(), 1);
        assert_eq!(config.peers.len(), 2);

        assert_eq!(
            config.routes[0].key,
            ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap()
        );
        assert_eq!(config.routes[0].scope, RouteScope::Cluster);

        assert_eq!(
            config.peers[0].id,
            ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap()
        );
        assert_eq!(
            config.peers[0].endpoint,
            "10.0.0.2:8000".parse().expect("not expecting error")
        );
        assert_eq!(config.peers[0].conn_type, ConnectionType::Cluster);
        assert_eq!(
            config.peers[1].id,
            ServicePath::from_string("region-a.cluster-a.10.0.0.3-dpu0").unwrap()
        );
        assert_eq!(
            config.peers[1].endpoint,
            "10.0.0.3:8000".parse().expect("not expecting error")
        );
        assert_eq!(config.peers[1].conn_type, ConnectionType::Cluster);
    }
}

use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io;
use std::io::BufReader;
use std::net::SocketAddr;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use swbus_proto::swbus::*;
use swss_common::{DbConnector, Table};
use swss_serde::from_table;
use thiserror::Error;
use tracing::*;

const CONFIG_DB: &str = "CONFIG_DB";

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
    #[error("Invalid config: {0}")]
    InvalidConfig(String),

    #[error("Failed to connect to the data source")]
    IOError(#[from] io::Error),

    #[error("swss-common error occurred while {action}: {original}")]
    SWSSError {
        action: String,
        #[source]
        original: swss_common::Exception, // Preserve the original error
    },

    #[error("swss-serde error occurred while {action}: {original}")]
    SWSSSerdeError {
        action: String,
        #[source]
        original: swss_serde::Error,
    },
}

impl From<(String, swss_common::Exception)> for SwbusConfigError {
    fn from((action, original): (String, swss_common::Exception)) -> Self {
        SwbusConfigError::SWSSError { action, original }
    }
}

impl From<(String, swss_serde::Error)> for SwbusConfigError {
    fn from((action, original): (String, swss_serde::Error)) -> Self {
        SwbusConfigError::SWSSSerdeError { action, original }
    }
}

pub type Result<T, E = SwbusConfigError> = core::result::Result<T, E>;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ConfigDBDPUEntry {
    pub state: Option<String>,
    pub swbus_port: Option<u16>, // Optional field for local port
    pub dpu_id: u32,
    #[serde(skip)]
    pub npu_ipv4: Option<Ipv4Addr>,
    #[serde(skip)]
    pub npu_ipv6: Option<Ipv6Addr>,
}

impl ConfigDBDPUEntry {
    // DPU entry in other slots are considered remote
    pub fn to_remote_dpu(&self) -> ConfigDBRemoteDPUEntry {
        ConfigDBRemoteDPUEntry {
            swbus_port: self.swbus_port,
            dpu_type: Some("cluster".to_string()),
            dpu_id: self.dpu_id,
            npu_ipv4: self.npu_ipv4.map(|ip| ip.to_string()),
            npu_ipv6: self.npu_ipv6.map(|ip| ip.to_string()),
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ConfigDBRemoteDPUEntry {
    #[serde(rename = "type")]
    pub dpu_type: Option<String>,
    pub swbus_port: Option<u16>, // Optional field for local port
    pub dpu_id: u32,
    pub npu_ipv4: Option<String>,
    pub npu_ipv6: Option<String>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ConfigDBDeviceMetadataEntry {
    pub region: Option<String>,
    pub cluster: Option<String>,
    #[serde(rename = "type")]
    pub device_type: Option<String>,
    pub sub_type: Option<String>,
}

#[instrument]
fn route_config_from_dpu_entry(dpu_entry: &ConfigDBDPUEntry, region: &str, cluster: &str) -> Result<Vec<RouteConfig>> {
    let mut routes = Vec::new();
    let dpu_id = dpu_entry.dpu_id;

    debug!("Collecting routes for local dpu{}", dpu_id);

    if let Some(npu_ipv4) = dpu_entry.npu_ipv4.as_ref() {
        let sp = ServicePath::with_node(region, cluster, &format!("{npu_ipv4}-dpu{dpu_id}"), "", "", "", "");
        routes.push(RouteConfig {
            key: sp,
            scope: RouteScope::Cluster,
        });
    }

    if let Some(npu_ipv6) = dpu_entry.npu_ipv6.as_ref() {
        let sp = ServicePath::with_node(region, cluster, &format!("{npu_ipv6}-dpu{dpu_id}"), "", "", "", "");
        routes.push(RouteConfig {
            key: sp,
            scope: RouteScope::Cluster,
        });
    }

    if routes.is_empty() {
        SwbusConfigError::InvalidConfig(format!("No valid routes found in local dpu{dpu_id}"));
    }

    debug!("Routes collected: {:?}", &routes);
    Ok(routes)
}

#[instrument]
fn peer_config_from_dpu_entry(
    key: &str,
    dpu_entry: ConfigDBRemoteDPUEntry,
    region: &str,
    cluster: &str,
) -> Result<Vec<PeerConfig>> {
    let mut peers = Vec::new();

    debug!("Collecting peer info for DPU: {}", key);
    let dpu_type = dpu_entry
        .dpu_type
        .as_ref()
        .ok_or(SwbusConfigError::InvalidConfig("DPU type not found".to_string()))?;
    if dpu_type != "cluster" {
        // ignore external DPUs, not for DASH smartswitch
        return Err(SwbusConfigError::InvalidConfig(format!(
            "Unsupported DPU type: {dpu_type}"
        )));
    }

    let swbusd_port = dpu_entry.swbus_port.ok_or(SwbusConfigError::InvalidConfig(format!(
        "swbusd_port is not found in dpu {key} is not found"
    )))?;

    let dpu_id = dpu_entry.dpu_id;

    if let Some(npu_ipv4) = dpu_entry.npu_ipv4 {
        let npu_ipv4 = npu_ipv4
            .parse::<Ipv4Addr>()
            .map_err(|_| SwbusConfigError::InvalidConfig(format!("Invalid IPv4 address: {npu_ipv4}")))?;

        let sp = ServicePath::with_node(region, cluster, &format!("{npu_ipv4}-dpu{dpu_id}"), "", "", "", "");
        peers.push(PeerConfig {
            id: sp,
            endpoint: SocketAddr::new(IpAddr::V4(npu_ipv4), swbusd_port),
            conn_type: ConnectionType::Cluster,
        });
    }

    if let Some(npu_ipv6) = dpu_entry.npu_ipv6 {
        let npu_ipv6 = npu_ipv6
            .parse::<Ipv6Addr>()
            .map_err(|_| SwbusConfigError::InvalidConfig(format!("Invalid IPv6 address: {npu_ipv6}")))?;

        let sp = ServicePath::with_node(region, cluster, &format!("{npu_ipv6}-dpu{dpu_id}"), "", "", "", "");
        peers.push(PeerConfig {
            id: sp,
            endpoint: SocketAddr::new(IpAddr::V6(npu_ipv6), swbusd_port),
            conn_type: ConnectionType::Cluster,
        });
    }

    if peers.is_empty() {
        error!("No valid peers found in DPU: {}", key);
    }

    debug!("Peer info collected: {:?}", &peers);
    Ok(peers)
}

#[instrument]
fn get_device_info() -> Result<(String, String)> {
    let db = DbConnector::new_named(CONFIG_DB, false, 0).map_err(|e| ("connecting to config_db".into(), e))?;
    let table = Table::new(db, "DEVICE_METADATA").map_err(|e| ("opening DEVICE_METADATA table".into(), e))?;

    let metadata: ConfigDBDeviceMetadataEntry =
        from_table(&table, "localhost").map_err(|e| ("reading DEVICE_METADATA:localhost entry".into(), e))?;

    let region = metadata.region.ok_or(SwbusConfigError::InvalidConfig(
        "region not found in DEVICE_METADATA table".into(),
    ))?;
    let cluster = metadata.cluster.ok_or(SwbusConfigError::InvalidConfig(
        "cluster not found in DEVICE_METADATA table".into(),
    ))?;

    debug!("Region: {}, Cluster: {}", region, cluster);

    Ok((region, cluster))
}

#[instrument]
fn get_loopback_address(lb_index: u32) -> Result<(Option<Ipv4Addr>, Option<Ipv6Addr>)> {
    let mut my_ipv4 = None;
    let mut my_ipv6 = None;
    let db = DbConnector::new_named(CONFIG_DB, false, 0).map_err(|e| ("connecting to config_db".into(), e))?;
    let table = Table::new(db, "LOOPBACK_INTERFACE").map_err(|e| ("opening LOOPBACK_INTERFACE table".into(), e))?;
    let keys = table
        .get_keys()
        .map_err(|e| ("Failed to get keys from LOOPBACK_INTERFACE table".into(), e))?;

    let lb_prefix = format!("Loopback{lb_index}|");
    for mut key in keys {
        if !key.starts_with(lb_prefix.as_str()) {
            continue;
        }
        key = key.replace(lb_prefix.as_str(), "");
        let addr = key.split("/").next().unwrap_or("");
        match IpAddr::from_str(addr) {
            Ok(IpAddr::V4(v4_addr)) => my_ipv4 = Some(v4_addr),
            Ok(IpAddr::V6(v6_addr)) => my_ipv6 = Some(v6_addr),
            Err(_) => continue,
        }
    }
    if my_ipv4.is_none() && my_ipv6.is_none() {
        return Err(SwbusConfigError::InvalidConfig(
            "No valid loopback address found".into(),
        ));
    }

    debug!(
        "Loopback{} ipv4 address: {}",
        lb_index,
        my_ipv4.map_or("none".to_string(), |ip| ip.to_string())
    );
    debug!(
        "Loopback{} ipv6 address: {}",
        lb_index,
        my_ipv6.map_or("none".to_string(), |ip| ip.to_string())
    );
    Ok((my_ipv4, my_ipv6))
}

#[instrument]
pub fn swbus_config_from_db(dpu_id: u32) -> Result<SwbusConfig> {
    let mut peers = Vec::new();
    let mut myroutes: Option<Vec<RouteConfig>> = None;
    let mut myendpoint: Option<SocketAddr> = None;

    let (region, cluster) = get_device_info()?;

    // Get the Loopback0 address
    let (my_ipv4, my_ipv6) = get_loopback_address(0)?;

    let db = DbConnector::new_named(CONFIG_DB, false, 0).map_err(|e| ("connecting config_db".into(), e))?;
    let table = Table::new(db, "DPU").map_err(|e| ("opening DPU table".into(), e))?;

    let keys = table
        .get_keys()
        .map_err(|e| ("Failed to get keys from DPU table".into(), e))?;

    for key in keys {
        let mut dpu: ConfigDBDPUEntry =
            from_table(&table, &key).map_err(|e| (format!("reading DPU entry {key}"), e))?;

        dpu.npu_ipv4 = my_ipv4;
        dpu.npu_ipv6 = my_ipv6;

        // find the DPU entry for the slot
        if dpu.dpu_id == dpu_id {
            // Check if the DPU entry is valid and has the required fields
            let swbusd_port = dpu.swbus_port.ok_or(SwbusConfigError::InvalidConfig(format!(
                "swbusd_port is not found in dpu{dpu_id} is not found"
            )))?;

            myroutes = Some(route_config_from_dpu_entry(&dpu, &region, &cluster).map_err(|e| {
                error!("Failed to collect routes for dpu{dpu_id}: {e}");
                e
            })?);

            if let Some(npu_ipv4) = dpu.npu_ipv4 {
                myendpoint = Some(SocketAddr::new(std::net::IpAddr::V4(npu_ipv4), swbusd_port));
            } else if let Some(npu_ipv6) = dpu.npu_ipv6 {
                myendpoint = Some(SocketAddr::new(std::net::IpAddr::V6(npu_ipv6), swbusd_port));
            }
            continue;
        }
        let remote_dpu = dpu.to_remote_dpu();
        let peer = peer_config_from_dpu_entry(&key, remote_dpu, &region, &cluster).map_err(|e| {
            error!("Failed to collect peers from {}: {}", key, e);
            e
        })?;
        peers.extend(peer);
    }
    if myroutes.is_none() {
        return Err(SwbusConfigError::InvalidConfig(format!(
            "DPU at slot {dpu_id} is not found"
        )));
    }

    let db = DbConnector::new_named(CONFIG_DB, false, 0).unwrap();
    let table = Table::new(db, "REMOTE_DPU").map_err(|e| ("opening REMOTE_DPU table".into(), e))?;

    let keys = table
        .get_keys()
        .map_err(|e| ("Failed to get keys from REMOTE_DPU table".into(), e))?;

    for key in keys {
        let remote_dpu: ConfigDBRemoteDPUEntry =
            from_table(&table, &key).map_err(|e| (format!("reading REMOTE_DPU entry {key}"), e))?;

        let peer = peer_config_from_dpu_entry(&key, remote_dpu, &region, &cluster).map_err(|e| {
            error!("Failed to collect peers from {}: {}", key, e);
            e
        })?;

        peers.extend(peer);
    }

    info!("successfully load swbus config from configdb for dpu {}", dpu_id);

    Ok(SwbusConfig {
        endpoint: myendpoint.unwrap(),
        routes: myroutes.unwrap(),
        peers,
    })
}

pub fn swbus_config_from_yaml(yaml_file: &str) -> Result<SwbusConfig> {
    let file = File::open(yaml_file)?;
    let reader = BufReader::new(file);

    // Parse the YAML data
    let swbus_config: SwbusConfig = serde_yaml::from_reader(reader)
        .map_err(|e| SwbusConfigError::InvalidConfig(format!("Failed to parse YAML file: {e}")))?;
    Ok(swbus_config)
}

pub mod test_utils {
    use super::*;
    use swss_common::CxxString;
    use swss_common::{DbConnector, Table};
    use swss_serde::to_table;

    pub fn populate_configdb_for_test() {
        let db = DbConnector::new_named(CONFIG_DB, false, 0).unwrap();
        let table = Table::new(db, "DEVICE_METADATA").unwrap();

        let metadata = ConfigDBDeviceMetadataEntry {
            region: Some("region-a".to_string()),
            cluster: Some("cluster-a".to_string()),
            device_type: Some("SpineRouter".to_string()),
            sub_type: Some("SmartSwitch".to_string()),
        };
        to_table(&metadata, &table, "localhost").unwrap();

        let db = DbConnector::new_named(CONFIG_DB, false, 0).unwrap();

        let table = Table::new(db, "LOOPBACK_INTERFACE").unwrap();
        table
            .hset("Loopback0|10.0.1.0/32", "NULL", &CxxString::new("NULL"))
            .unwrap();
        table
            .hset("Loopback0|2001:db8:1::/128", "NULL", &CxxString::new("NULL"))
            .unwrap();
        table
            .hset("Loopback1|11.0.1.0/32", "NULL", &CxxString::new("NULL"))
            .unwrap();
        table
            .hset("Loopback1|2001:dd8:1::/128", "NULL", &CxxString::new("NULL"))
            .unwrap();
        let db: DbConnector = DbConnector::new_named(CONFIG_DB, false, 0).unwrap();
        let table = Table::new(db, "DPU").unwrap();
        // create local dpu table first
        for d in 0..2 {
            let dpu = ConfigDBDPUEntry {
                state: Some("active".to_string()),
                dpu_id: d,
                npu_ipv4: None,
                npu_ipv6: None,
                swbus_port: Some(23606 + d as u16),
            };
            to_table(&dpu, &table, &format!("dpu{d}")).unwrap();
        }

        let db = DbConnector::new_named(CONFIG_DB, false, 0).unwrap();
        // create remote dpu entries
        let table = Table::new(db, "REMOTE_DPU").unwrap();
        for s in 1..3 {
            for d in 0..2 {
                let dpu = ConfigDBRemoteDPUEntry {
                    dpu_type: Some("cluster".to_string()),
                    dpu_id: d,
                    npu_ipv4: Some(format!("10.0.1.{s}")),
                    npu_ipv6: Some(format!("2001:db8:1::{s}")),
                    swbus_port: Some(23606 + d as u16),
                };
                let key = format!("dpu{s}_{d}");
                to_table(&dpu, &table, &key).unwrap();
                assert_eq!(table.hget(&key, "type").unwrap().as_ref().unwrap(), "cluster");
            }
        }
    }

    // This function can be used to clean up the test database after tests
    pub fn cleanup_configdb_for_test() {
        // clean up
        let db = DbConnector::new_named(CONFIG_DB, false, 0).unwrap();
        let table = Table::new(db, "DPU").unwrap();
        for d in 0..2 {
            table.del(&format!("dpu{d}")).unwrap();
        }
        for s in 1..3 {
            for d in 0..2 {
                table.del(&format!("dpu{s}_{d}")).unwrap();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;
    use std::io::Write;
    use swss_common_testing::*;
    use tempfile::tempdir;

    #[test]
    fn test_load_from_configdb() {
        let _ = Redis::start_config_db();

        // Populate the CONFIG_DB for testing
        populate_configdb_for_test();

        let mut config_fromdb = swbus_config_from_db(0).unwrap();

        assert_eq!(config_fromdb.routes.len(), 2);
        assert_eq!(config_fromdb.peers.len(), 10);

        // create equivalent config in yaml
        let yaml_content = r#"
        endpoint: "10.0.1.0:23606"
        routes:
          - key: "region-a.cluster-a.10.0.1.0-dpu0"
            scope: "Cluster"
          - key: "region-a.cluster-a.2001:db8:1::-dpu0"
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
          - id: "region-a.cluster-a.10.0.1.2-dpu0"
            endpoint: "10.0.1.2:23606"
            conn_type: "Cluster"
          - id: "region-a.cluster-a.2001:db8:1::2-dpu0"
            endpoint: "[2001:db8:1::2]:23606"
            conn_type: "Cluster"
          - id: "region-a.cluster-a.10.0.1.2-dpu1"
            endpoint: "10.0.1.2:23607"
            conn_type: "Cluster"
          - id: "region-a.cluster-a.2001:db8:1::2-dpu1"
            endpoint: "[2001:db8:1::2]:23607"
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

        cleanup_configdb_for_test();
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
                panic!("Failed to load config from yaml: {e}");
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

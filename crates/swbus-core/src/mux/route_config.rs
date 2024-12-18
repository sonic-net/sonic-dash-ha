use serde::Deserialize;
use std::fs::File;
use std::io::BufReader;
use std::net::SocketAddr;
use swbus_proto::swbus::*;

#[derive(Debug, Clone, Deserialize)]
pub struct RoutesConfig {
    pub routes: Vec<RouteConfig>,
    pub peers: Vec<PeerConfig>,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone, Deserialize)]
pub struct RouteConfig {
    #[serde(deserialize_with = "deserialize_service_path")]
    pub key: ServicePath,
    pub scope: RouteScope,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PeerConfig {
    #[serde(deserialize_with = "deserialize_service_path")]
    pub id: ServicePath,
    pub endpoint: SocketAddr,
    pub conn_type: ConnectionType,
}

impl RoutesConfig {
    pub fn load_from_yaml(yaml_file: String) -> Result<RoutesConfig, Box<dyn std::error::Error>> {
        let file = File::open(yaml_file)?;
        let reader = BufReader::new(file);

        // Parse the YAML data
        let routes_config: RoutesConfig = serde_yaml::from_reader(reader)?;
        //let routes_config = RoutesConfig::from_routes_config_serde(&route_config_serde);
        Ok(routes_config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_load_from_yaml() {
        let yaml_content = r#"
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

        let result = RoutesConfig::load_from_yaml(file_path.to_str().unwrap().to_string());
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

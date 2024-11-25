use serde::{Deserialize, Deserializer};
use serde_yaml::Error;
use std::fs::File;
use std::io::BufReader;
use std::io::Write;
use std::net::SocketAddr;
use swbus_proto::swbus::*;
use tempfile::tempdir;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ScopeYaml {
    Client,
    Local,
    Region,
    Cluster,
    Global,
}

#[derive(Deserialize, Debug)]
pub struct RoutesConfigYaml {
    routes: Vec<RouteConfigYaml>,
    peers: Vec<PeerConfigYaml>,
}

#[derive(Deserialize, Debug)]
pub struct RouteConfigYaml {
    pub key: String,
    pub scope: ScopeYaml,
}

#[derive(Deserialize, Debug)]
pub struct PeerConfigYaml {
    pub id: String,
    pub endpoint: String,
    pub scope: ScopeYaml,
}

#[derive(Debug, Clone)]
pub struct RoutesConfig {
    pub routes: Vec<RouteConfig>,
    pub peers: Vec<PeerConfig>,
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct RouteConfig {
    pub key: ServicePath,
    pub scope: Scope,
}

#[derive(Debug, Clone)]
pub struct PeerConfig {
    pub id: ServicePath,
    pub endpoint: SocketAddr,
    pub scope: Scope,
}

impl ScopeYaml {
    fn to_scope(&self) -> Scope {
        match self {
            ScopeYaml::Client => Scope::Client,
            ScopeYaml::Local => Scope::Local,
            ScopeYaml::Region => Scope::Region,
            ScopeYaml::Cluster => Scope::Cluster,
            ScopeYaml::Global => Scope::Global,
        }
    }
}

impl RoutesConfig {
    pub fn load_from_yaml(yaml_file: String) -> Result<RoutesConfig, Box<dyn std::error::Error>> {
        let file = File::open(yaml_file)?;
        let reader = BufReader::new(file);

        // Parse the YAML data
        let route_config_yaml = serde_yaml::from_reader(reader)?;
        let routes_config = RoutesConfig::from_routes_config_yaml(route_config_yaml);
        Ok(routes_config)
    }

    fn from_routes_config_yaml(config: RoutesConfigYaml) -> RoutesConfig {
        let routes = config
            .routes
            .iter()
            .map(|route| RouteConfig {
                key: ServicePath::from_string(&route.key).expect("Failed to parse service path"),
                scope: route.scope.to_scope(),
            })
            .collect();

        let peers = config
            .peers
            .iter()
            .map(|peer| PeerConfig {
                id: ServicePath::from_string(&peer.id).expect("Failed to parse service path"),
                endpoint: peer
                    .endpoint
                    .parse()
                    .expect(&format!("Failed to parse endpoint:{}", peer.endpoint)),
                scope: peer.scope.to_scope(),
            })
            .collect();

        RoutesConfig { routes, peers }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_from_yaml() {
        let yaml_content = r#"
        routes:
          - key: "region-a.cluster-a.10.0.0.1-dpu0"
            scope: "cluster"
        peers:
          - id: "region-a.cluster-a.10.0.0.2-dpu0"
            endpoint: "10.0.0.2:8000"
            scope: "cluster"
          - id: "region-a.cluster-a.10.0.0.3-dpu0"
            endpoint: "10.0.0.3:8000"
            scope: "cluster"
        "#;

        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_config.yaml");
        let mut file = File::create(&file_path).unwrap();
        file.write_all(yaml_content.as_bytes()).unwrap();

        let result = RoutesConfig::load_from_yaml(file_path.to_str().unwrap().to_string());
        assert!(result.is_ok());

        let config = result.unwrap();
        assert_eq!(config.routes.len(), 1);
        assert_eq!(config.peers.len(), 2);

        assert_eq!(
            config.routes[0].key,
            ServicePath::from_string("region-a.cluster-a.10.0.0.1-dpu0").unwrap()
        );
        assert_eq!(config.routes[0].scope, Scope::Cluster);

        assert_eq!(
            config.peers[0].id,
            ServicePath::from_string("region-a.cluster-a.10.0.0.2-dpu0").unwrap()
        );
        assert_eq!(
            config.peers[0].endpoint,
            "10.0.0.2:8000".parse().expect("not expecting error")
        );
        assert_eq!(config.peers[0].scope, Scope::Cluster);
        assert_eq!(
            config.peers[1].id,
            ServicePath::from_string("region-a.cluster-a.10.0.0.3-dpu0").unwrap()
        );
        assert_eq!(
            config.peers[1].endpoint,
            "10.0.0.3:8000".parse().expect("not expecting error")
        );
        assert_eq!(config.peers[1].scope, Scope::Cluster);
    }
}

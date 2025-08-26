use bollard::container::*;
use bollard::Docker;
use chrono::Local;
use futures_util::stream::TryStreamExt;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;
use swss_common::{CxxString, DbConnector};

// DB field names
const DB_FEATURE_TABLE: &str = "FEATURE";
const DB_FIELD_SET_OWNER: &str = "set_owner";
const DB_FIELD_NO_FALLBACK: &str = "no_fallback_to_local";

const DB_FIELD_CURRENT_OWNER: &str = "current_owner";
const DB_FIELD_UPD_TIMESTAMP: &str = "update_time";
const DB_FIELD_CONTAINER_ID: &str = "container_id";
const DB_FIELD_REMOTE_STATE: &str = "remote_state";
const DB_FIELD_SYSTEM_STATE: &str = "system_state";
const DB_FIELD_STATE: &str = "state";
const DB_FIELD_ST_FEAT_CTR_STABLE_VER: &str = "container_stable_version";

const DB_KUBE_LABEL_TABLE: &str = "KUBE_LABELS";
const DB_KUBE_LABEL_SET_KEY: &str = "SET";
const DB_SERVER_TABLE: &str = "KUBERNETES_MASTER";
const DB_SERVER_KEY: &str = "SERVER";
const DB_FIELD_ST_SER_CONNECTED: &str = "connected";
const DB_FIELD_ST_SER_UPDATE_TS: &str = "update_time";

// Get seconds to wait for remote docker to start.
// If not, revert to local
//
const SONIC_CTR_CONFIG: &str = "/etc/sonic/remote_ctr.config.json";
const SONIC_CTR_CONFIG_PEND_SECS: &str = "revert_to_local_on_wait_seconds";
const DEFAULT_PEND_SECS: u32 = 5 * 60;
const WAIT_POLL_SECS: u32 = 2;

struct DbConnections {
    config_db: DbConnector,
    state_db: DbConnector,
    remote_ctr_enabled: bool,
}

impl DbConnections {
    fn initialize_connection() -> DbConnections {
        DbConnections {
            config_db: DbConnector::new_tcp(4, "localhost", 6379, 0).expect("Unable to connect to Redis DB"),
            state_db: DbConnector::new_tcp(6, "localhost", 6379, 0).expect("Unable to connect to Redis DB"),
            remote_ctr_enabled: Path::new("/lib/systemd/system/ctrmgrd.service").exists(),
        }
    }
}

pub struct Container<'a> {
    feature: &'a str,
    db_connections: DbConnections,
}

#[derive(enumset::EnumSetType, Debug)]
enum StartFlags {
    StartLocal,
    StartKube,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Unable to parse JSON file")]
    JSONParse(#[from] serde_json::Error),
    #[error("Unexpected structure of JSON file")]
    JSONData(String),
    #[error("Docker error")]
    Docker(#[from] bollard::errors::Error),
    #[error("Unable to open file")]
    IO(#[from] std::io::Error),
    #[error("Unable to get/set data to swsscommon")]
    Swsscommon(#[from] swss_common::Exception),
    #[error("UTF-8 parsing error")]
    UTF8(#[from] std::str::Utf8Error),
}

impl<'a> Container<'a> {
    fn get_config_data(field: &str) -> Result<serde_json::Value, Error> {
        let mut file = File::open(SONIC_CTR_CONFIG)?;
        let mut file_contents = String::new();
        file.read_to_string(&mut file_contents)?;
        let data: serde_json::Value = serde_json::from_str(&file_contents)?;
        data.as_object()
            .ok_or(Error::JSONData(
                "config file isn't an object at the top level".to_string(),
            ))?
            .get(field)
            .ok_or(Error::JSONData(format!("field {field} not found")))
            .cloned()
    }

    fn read_data(db_connector: &DbConnector, key: &str, fields: &mut HashMap<&str, String>) -> Result<(), Error> {
        let table_name = if key == DB_SERVER_KEY {
            DB_SERVER_TABLE
        } else {
            DB_FEATURE_TABLE
        };

        let data = db_connector.hgetall(&format!("{table_name}|{key}"))?;
        for (field, default) in fields.iter_mut() {
            if let Some(value) = data.get(field as &str) {
                *default = value.to_str()?.to_string()
            }
        }
        Ok(())
    }

    pub fn new(feature: &str) -> Container<'_> {
        Container {
            feature,
            db_connections: DbConnections::initialize_connection(),
        }
    }

    fn read_config(&self) -> Result<HashMap<&'static str, String>, Error> {
        let mut fields: HashMap<&str, String> = HashMap::from([
            (DB_FIELD_SET_OWNER, "local".to_string()),
            (DB_FIELD_NO_FALLBACK, "False".to_string()),
            (DB_FIELD_STATE, "disabled".to_string()),
        ]);
        Self::read_data(&self.db_connections.config_db, self.feature, &mut fields)?;
        Ok(fields)
    }

    fn read_state(&self) -> Result<HashMap<&'static str, String>, Error> {
        let mut fields: HashMap<&str, String> = HashMap::from([
            (DB_FIELD_CURRENT_OWNER, "local".to_string()),
            (DB_FIELD_REMOTE_STATE, "none".to_string()),
            (DB_FIELD_CONTAINER_ID, "".to_string()),
        ]);
        Self::read_data(&self.db_connections.state_db, self.feature, &mut fields)?;
        Ok(fields)
    }

    fn read_server_state(&self) -> Result<HashMap<&'static str, String>, Error> {
        let mut fields: HashMap<&str, String> = HashMap::from([
            (DB_FIELD_ST_SER_CONNECTED, "false".to_string()),
            (DB_FIELD_ST_SER_UPDATE_TS, "".to_string()),
        ]);
        Self::read_data(&self.db_connections.state_db, DB_SERVER_KEY, &mut fields)?;
        Ok(fields)
    }

    fn set_label(&self, feature: &str, create: bool) -> Result<(), Error> {
        if self.db_connections.remote_ctr_enabled {
            self.db_connections.state_db.hset(
                DB_KUBE_LABEL_TABLE,
                &format!("{DB_KUBE_LABEL_SET_KEY}|{feature}_enabled"),
                &CxxString::new(if create { "true" } else { "false" }),
            )?;
        }
        Ok(())
    }

    fn update_data(&self, data: &HashMap<&str, &str>) {
        if self.db_connections.remote_ctr_enabled {
            for (key, value) in data.iter() {
                let _ = self.db_connections.state_db.hset(
                    DB_FEATURE_TABLE,
                    &format!("{}|{}", self.feature, key),
                    &CxxString::new(value),
                );
            }
        }
    }

    async fn container_version(&self, docker: &Docker) -> Option<String> {
        let container_options = docker
            .inspect_container(self.feature, None)
            .await
            .expect("Error getting the version of container");
        if let Some(config) = container_options.config {
            if let Some(envs) = config.env {
                for env in envs {
                    if env.starts_with("IMAGE_VERSION=") {
                        return Some(env.split('=').collect::<Vec<&str>>()[1].to_string());
                    }
                }
            }
        }
        None
    }

    pub fn container_id(&self) -> Cow<'a, str> {
        let data = self
            .db_connections
            .state_db
            .hgetall(&format!("FEATURE|{}", self.feature))
            .expect("Unable to get data");
        if data
            .get(DB_FIELD_CURRENT_OWNER)
            .map_or("", |value| value.to_str().unwrap())
            == "local"
        {
            Cow::Borrowed(self.feature)
        } else {
            data.get(DB_FIELD_CONTAINER_ID)
                .map_or(Cow::Borrowed(self.feature), |value| {
                    Cow::Owned(value.to_str().unwrap().to_string())
                })
        }
    }

    pub async fn start(&self) -> Result<(), Error> {
        let feature_config = self.read_config()?;
        let feature_state = self.read_state()?;
        let server_state = self.read_server_state()?;

        let timestamp = format!("{}", Local::now().format("%Y-%m-%d %H:%M:%S"));
        let mut data: HashMap<&str, &str> =
            HashMap::from([(DB_FIELD_SYSTEM_STATE, "up"), (DB_FIELD_UPD_TIMESTAMP, &timestamp)]);

        let mut start_val = enumset::EnumSet::new();
        if feature_config.get(DB_FIELD_SET_OWNER).unwrap() == "local" {
            start_val |= StartFlags::StartLocal;
        } else {
            start_val |= StartFlags::StartKube;
            if feature_config.get(DB_FIELD_NO_FALLBACK).unwrap() == "False"
                && (feature_state.get(DB_FIELD_REMOTE_STATE).unwrap() == "none"
                    || server_state.get(DB_FIELD_ST_SER_CONNECTED).unwrap() == "false")
            {
                start_val |= StartFlags::StartLocal;
                data.insert(DB_FIELD_REMOTE_STATE, "none");
            }
        }

        if start_val & StartFlags::StartLocal != enumset::EnumSet::empty() {
            data.insert(DB_FIELD_CURRENT_OWNER, "local");
            data.insert(DB_FIELD_CONTAINER_ID, self.feature);
            if start_val == StartFlags::StartLocal {
                self.set_label(self.feature, false)?;
                data.insert(DB_FIELD_REMOTE_STATE, "none");
            }
        }

        self.update_data(&data);

        if start_val & StartFlags::StartLocal != enumset::EnumSet::empty() {
            let docker = Docker::connect_with_local_defaults()?;
            docker
                .start_container(self.feature, None::<StartContainerOptions<String>>)
                .await?;
        }

        if start_val & StartFlags::StartKube != enumset::EnumSet::empty() {
            self.set_label(self.feature, true)?;
        }

        Ok(())
    }

    pub async fn stop(&self, timeout: Option<i64>) -> Result<(), Error> {
        let feature_config = self.read_config()?;
        let feature_state = self.read_state()?;
        let docker_id = self.container_id();
        let remove_label = feature_config.get(DB_FIELD_SET_OWNER).unwrap() != "local";

        let docker = Docker::connect_with_local_defaults()?;

        if remove_label {
            self.set_label(self.feature, false)?;
        }

        if !docker_id.is_empty() {
            let stop_options = timeout.map(|timeout| StopContainerOptions { t: timeout });
            docker.stop_container(self.feature, stop_options).await?;
        }

        let timestamp = format!("{}", Local::now().format("%Y-%m-%d %H:%M:%S"));
        let mut data: HashMap<&str, &str> = HashMap::from([
            (DB_FIELD_CURRENT_OWNER, "none"),
            (DB_FIELD_SYSTEM_STATE, "down"),
            (DB_FIELD_CONTAINER_ID, ""),
            (DB_FIELD_UPD_TIMESTAMP, &timestamp),
        ]);
        if feature_state.get(DB_FIELD_REMOTE_STATE).unwrap() == "running" {
            data.insert(DB_FIELD_REMOTE_STATE, "stopped");
        }

        self.update_data(&data);

        Ok(())
    }

    pub async fn kill(&self) -> Result<(), Error> {
        let feature_config = self.read_config()?;
        let feature_state = self.read_state()?;
        let docker_id = self.container_id();
        let remove_label = (feature_config.get(DB_FIELD_SET_OWNER).unwrap() != "local")
            || (feature_state.get(DB_FIELD_CURRENT_OWNER).unwrap() != "local");

        if remove_label {
            self.set_label(self.feature, false)?;
        }

        if feature_config.get(DB_FIELD_SET_OWNER).unwrap() == "local" {
            let current_state = feature_state.get(DB_FIELD_STATE).unwrap();
            if current_state != "enabled" && current_state != "always_enabled" {
                println!("{} is not enabled", self.feature);
                return Ok(());
            }
        }

        let docker = Docker::connect_with_local_defaults()?;

        if !docker_id.is_empty() {
            docker
                .kill_container(self.feature, Some(KillContainerOptions { signal: "SIGINT" }))
                .await?;
        }

        Ok(())
    }

    pub async fn wait(&self) -> Result<(), Error> {
        let feature_config = self.read_config()?;
        //let mut feature_state = read_state(&db_connections, feature);
        let mut feature_state;
        let mut docker_id = self.container_id();
        let mut pend_wait_seconds: u32 = 0;

        let docker = Docker::connect_with_local_defaults()?;

        if *docker_id == *self.feature {
            let version_option = self.container_version(&docker).await;
            if let Some(version) = version_option {
                self.update_data(&HashMap::from([(DB_FIELD_ST_FEAT_CTR_STABLE_VER, version.as_str())]));
            }
        }

        if docker_id.is_empty() && feature_config.get(DB_FIELD_NO_FALLBACK).unwrap() == "False" {
            pend_wait_seconds = match Self::get_config_data(SONIC_CTR_CONFIG_PEND_SECS) {
                Ok(value) => value.as_u64().map(|value| value as u32).unwrap_or(DEFAULT_PEND_SECS),
                Err(_) => DEFAULT_PEND_SECS,
            }
        }

        while docker_id.is_empty() {
            if feature_config.get(DB_FIELD_NO_FALLBACK).unwrap() == "False" {
                if pend_wait_seconds < WAIT_POLL_SECS {
                    break;
                }
                pend_wait_seconds -= WAIT_POLL_SECS;
            }

            sleep(Duration::from_secs(WAIT_POLL_SECS as u64));
            feature_state = self.read_state()?;

            docker_id = Cow::Borrowed(feature_state.get(DB_FIELD_CONTAINER_ID).unwrap());

            if feature_state.get(DB_FIELD_REMOTE_STATE).unwrap() == "pending" {
                self.update_data(&HashMap::from([(DB_FIELD_REMOTE_STATE, "ready")]));
            }
        }

        docker
            .wait_container(
                self.feature,
                Some(WaitContainerOptions {
                    condition: "not-running",
                }),
            )
            .try_collect::<Vec<_>>()
            .await?;

        Ok(())
    }
}

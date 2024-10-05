use swss_common::DbConnector;
use std::collections::HashMap;
use clap::{Parser, ValueEnum};
use chrono::Local;
use bollard::Docker;
use bollard::container::*;
use futures_util::stream::TryStreamExt;
use enumset;
use std::fs::File;
use std::io::Read;
use std::time::Duration;
use std::thread::sleep;

struct DbConnections {
    config_db : DbConnector,
    state_db : DbConnector,
    remote_ctr_enabled : bool,
}

// DB field names
const FEATURE_TABLE: &str = "FEATURE";
const SET_OWNER: &str = "set_owner";
const NO_FALLBACK: &str = "no_fallback_to_local";

const CURRENT_OWNER: &str = "current_owner";
const UPD_TIMESTAMP: &str = "update_time";
const CONTAINER_ID: &str = "container_id";
const REMOTE_STATE: &str = "remote_state";
const VERSION: &str = "container_version";
const SYSTEM_STATE: &str = "system_state";
const STATE: &str = "state";
const ST_FEAT_CTR_STABLE_VER: &str = "container_stable_version";

const KUBE_LABEL_TABLE: &str = "KUBE_LABELS";
const KUBE_LABEL_SET_KEY: &str = "SET";
const SERVER_TABLE: &str = "KUBERNETES_MASTER";
const SERVER_KEY: &str = "SERVER";
const ST_SER_CONNECTED: &str = "connected";
const ST_SER_UPDATE_TS: &str = "update_time";

// Get seconds to wait for remote docker to start.
// If not, revert to local
//
const SONIC_CTR_CONFIG: &str = "/etc/sonic/remote_ctr.config.json";
const SONIC_CTR_CONFIG_PEND_SECS: &str = "revert_to_local_on_wait_seconds";
const DEFAULT_PEND_SECS: u32 = 5 * 60;
const WAIT_POLL_SECS: u32 = 2;

fn get_config_data(field: &str) -> Option<serde_json::Value> {
    let mut file = match File::open(SONIC_CTR_CONFIG) {
        Ok(f) => f,
        Err(_e) => return None,
    };
    let mut file_contents = String::new();
    file.read_to_string(&mut file_contents).unwrap();
    let data : serde_json::Value = serde_json::from_str(&file_contents).unwrap();
    data.as_object().unwrap().get(field).and_then(|value| Some(value.clone()))
}

fn read_data(db_connector: &DbConnector, feature: &String, fields: &mut HashMap<&str, String>) {
    let table_name : &str;
    if feature == SERVER_KEY {
        table_name = "KUBERNETES_MASTER";
    } else {
        table_name = "FEATURE";
    }

    let data = db_connector.hgetall(&format!("{}|{}", table_name, feature));
    for (field, default) in fields.iter_mut() {
        match data.get(field as &str) {
            Some(value) => *default = value.to_string(),
            None => {},
        }
    }
}

fn read_config(db_connections: &DbConnections, feature: &String) -> HashMap<&'static str, String> {
    let mut fields : HashMap<&str, String> = HashMap::from(
        [
        (SET_OWNER, "local".to_string()),
        (NO_FALLBACK, "False".to_string()),
        (STATE, "disabled".to_string())
        ]);
    read_data(&db_connections.config_db, feature, &mut fields);
    fields
}

fn read_state(db_connections: &DbConnections, feature: &String) -> HashMap<&'static str, String> {
    let mut fields : HashMap<&str, String> = HashMap::from(
        [
        (CURRENT_OWNER, "local".to_string()),
        (REMOTE_STATE, "none".to_string()),
        (CONTAINER_ID, "".to_string())
        ]);
    read_data(&db_connections.state_db, feature, &mut fields);
    fields
}

fn set_label(db_connections: &DbConnections, feature: &String, create: bool) {
    if db_connections.remote_ctr_enabled {
        todo!();
    }
}

fn update_data(db_connections: &DbConnections, feature: &String, data: &HashMap<&str, String>) {
    if db_connections.remote_ctr_enabled {
        todo!();
    }
}

fn container_version(docker: &Docker, feature: &String) -> String {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let container_options = rt.block_on(docker.inspect_container(&feature, None)).expect("Unable to communicate with Docker");
    match container_options.config {
        Some(config) => {
            match config.env {
                Some(envs) => {
                    for env in envs {
                        if env.starts_with("IMAGE_VERSION=") {
                            return env.split('=').collect::<Vec<&str>>()[1].to_string();
                        }
                    }
                    return "".to_string();
                },
                None => {
                    return "".to_string();
                }
            }
        },
        None => {
            return "".to_string();
        }
    };
}

fn initialize_connection() -> DbConnections {
    DbConnections {
        config_db: DbConnector::new_tcp(4, "localhost", 6379, 0),
        state_db: DbConnector::new_tcp(6, "localhost", 6379, 0),
        remote_ctr_enabled: false,
    }
}

fn get_container_id(feature: &String, db_connections: &DbConnections) -> String {
    let data = db_connections.state_db.hgetall(&format!("FEATURE|{}", feature));
    if data.get(CURRENT_OWNER).unwrap_or(&String::new()) == "local" {
        return feature.clone();
    } else {
        return data.get(CONTAINER_ID).unwrap_or(feature).clone();
    }
}

fn container_id(feature: &String) {
    let db_connections = initialize_connection();
    println!("{}\n", get_container_id(feature, &db_connections));
}

#[derive(enumset::EnumSetType, Debug)]
enum StartFlags {
    StartLocal,
    StartKube,
}

fn container_start(feature: &String) {
    let db_connections = initialize_connection();

    let feature_config = read_config(&db_connections, &feature);
    let feature_state = read_state(&db_connections, &feature);

    let timestamp = format!("{}", Local::now().format("%Y-%m-%d %H:%M:%S"));
    let mut data : HashMap<&str, String> = HashMap::from(
        [
        (SYSTEM_STATE, "up".to_string()),
        (UPD_TIMESTAMP, timestamp),
        ]);

    let mut start_val = enumset::EnumSet::new();
    if feature_config.get(SET_OWNER).unwrap() == "local" {
        start_val |= StartFlags::StartLocal;
    } else {
        start_val |= StartFlags::StartKube;
        if feature_config.get(NO_FALLBACK).unwrap() == "False" && feature_state.get(REMOTE_STATE).unwrap() == "none" {
            start_val |= StartFlags::StartLocal;
            data.insert(REMOTE_STATE, "none".to_string());
        }
    }

    if start_val & StartFlags::StartLocal != enumset::EnumSet::empty() {
        data.insert(CURRENT_OWNER, "local".to_string());
        data.insert(CONTAINER_ID, feature.clone());
        if start_val == StartFlags::StartLocal {
            set_label(&db_connections, &feature, false);
            data.insert(REMOTE_STATE, "none".to_string());
        }
    }

    update_data(&db_connections, &feature, &data);

    if start_val & StartFlags::StartLocal != enumset::EnumSet::empty() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(docker.start_container(&feature, None::<StartContainerOptions<String>>)).expect("Unable to communicate with Docker");
    }

    if start_val & StartFlags::StartKube != enumset::EnumSet::empty() {
        set_label(&db_connections, &feature, true);
    }
}

fn container_stop(feature: &String) {
    let db_connections = initialize_connection();

    let feature_config = read_config(&db_connections, &feature);
    let feature_state = read_state(&db_connections, &feature);
    let docker_id = get_container_id(feature, &db_connections);

    let docker = Docker::connect_with_local_defaults().unwrap();

    if !docker_id.is_empty() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(docker.stop_container(&feature, None)).expect("Unable to communicate with Docker");
    }

    let timestamp = format!("{}", Local::now().format("%Y-%m-%d %H:%M:%S"));
    let mut data : HashMap<&str, String> = HashMap::from(
        [
        (CURRENT_OWNER, "none".to_string()),
        (SYSTEM_STATE, "down".to_string()),
        (CONTAINER_ID, "".to_string()),
        (UPD_TIMESTAMP, timestamp),
        ]);
    if feature_state.get(REMOTE_STATE).unwrap() == "running" {
        data.insert(REMOTE_STATE, "stopped".to_string());
    }

    update_data(&db_connections, &feature, &data);
}

fn container_kill(feature: &String) {
    let db_connections = initialize_connection();

    let feature_config = read_config(&db_connections, &feature);
    let feature_state = read_state(&db_connections, &feature);
    let docker_id = get_container_id(feature, &db_connections);
    let remove_label = (feature_config.get(SET_OWNER).unwrap() != "local") || (feature_state.get(CURRENT_OWNER).unwrap() != "local");

    if remove_label {
        set_label(&db_connections, &feature, false);
    }

    if feature_config.get(SET_OWNER).unwrap() == "local" {
        let current_state = feature_state.get(STATE).unwrap();
        if current_state != "enabled" && current_state != "always_enabled" {
            println!("{} is not enabled", feature);
            return;
        }
    }

    let docker = Docker::connect_with_local_defaults().unwrap();

    if !docker_id.is_empty() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(docker.kill_container(&feature, Some(KillContainerOptions{
                signal: "SIGINT",
        }))).expect("Unable to communicate with Docker");
    }
}

fn container_wait(feature: &String) {
    let db_connections = initialize_connection();

    let feature_config = read_config(&db_connections, &feature);
    let mut feature_state = read_state(&db_connections, &feature);
    let mut docker_id = get_container_id(feature, &db_connections);
    let mut pend_wait_seconds : u32 = 0;

    let docker = Docker::connect_with_local_defaults().unwrap();

    if docker_id == *feature {
        let version = container_version(&docker, &feature);
        if !version.is_empty() {
            update_data(&db_connections, &feature, &HashMap::from([(ST_FEAT_CTR_STABLE_VER, version)]));
        }
    }

    if docker_id.is_empty() && feature_config.get(NO_FALLBACK).unwrap() == "False" {
        pend_wait_seconds = get_config_data(SONIC_CTR_CONFIG_PEND_SECS).and_then(|value| value.as_u64()).and_then(|value| Some(value as u32)).unwrap_or(DEFAULT_PEND_SECS);
    }

    while docker_id.is_empty() {
        if feature_config.get(NO_FALLBACK).unwrap() == "False" {
            if pend_wait_seconds < WAIT_POLL_SECS {
                break;
            }
            pend_wait_seconds = pend_wait_seconds - WAIT_POLL_SECS;
        }

        sleep(Duration::from_secs(WAIT_POLL_SECS as u64));
        feature_state = read_state(&db_connections, &feature);

        docker_id = feature_state.get(CONTAINER_ID).unwrap().clone();

        if feature_state.get(REMOTE_STATE).unwrap() == "pending" {
            update_data(&db_connections, &feature, &HashMap::from([(REMOTE_STATE, "ready".to_string())]));
        }
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(docker.wait_container(&feature, Some(WaitContainerOptions{
        condition: "not-running",
    })).try_collect::<Vec<_>>()).expect("Unable to communicate with Docker");
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Action {
    Start,
    Stop,
    Kill,
    Wait,
    Id,
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(value_enum)]
    /// The action to take for the container
    action: Action,

    /// The name of the container
    name: String,

    /// Timeout for the action to occur
    #[arg(short, long)]
    timeout: Option<u32>,
}

fn main() {
    let cli = Cli::parse();

    match cli.action {
        Action::Start => container_start(&cli.name),
        Action::Wait => container_wait(&cli.name),
        Action::Stop => container_stop(&cli.name),
        Action::Kill => container_kill(&cli.name),
        Action::Id => container_id(&cli.name),
    };
}
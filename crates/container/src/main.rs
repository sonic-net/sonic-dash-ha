use bollard::container::*;
use bollard::Docker;
use chrono::Local;
use clap::{Parser, ValueEnum};
use futures_util::stream::TryStreamExt;
use std::borrow::Cow;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::thread::sleep;
use std::time::Duration;
use swss_common::{CxxString, DbConnector};

struct DbConnections {
    config_db: DbConnector,
    state_db: DbConnector,
    remote_ctr_enabled: bool,
}

// DB field names
const FEATURE_TABLE: &str = "FEATURE";
const SET_OWNER: &str = "set_owner";
const NO_FALLBACK: &str = "no_fallback_to_local";

const CURRENT_OWNER: &str = "current_owner";
const UPD_TIMESTAMP: &str = "update_time";
const CONTAINER_ID: &str = "container_id";
const REMOTE_STATE: &str = "remote_state";
const SYSTEM_STATE: &str = "system_state";
const STATE: &str = "state";
const ST_FEAT_CTR_STABLE_VER: &str = "container_stable_version";

const KUBE_LABEL_TABLE: &str = "KUBE_LABELS";
const KUBE_LABEL_SET_KEY: &str = "SET";
const SERVER_TABLE: &str = "KUBERNETES_MASTER";
const SERVER_KEY: &str = "SERVER";
//const ST_SER_CONNECTED: &str = "connected";
//const ST_SER_UPDATE_TS: &str = "update_time";

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
    let data: serde_json::Value = serde_json::from_str(&file_contents).unwrap();
    data.as_object().unwrap().get(field).cloned()
}

fn read_data(db_connector: &DbConnector, feature: &str, fields: &mut HashMap<&str, String>) {
    let table_name = if feature == SERVER_KEY {
        SERVER_TABLE
    } else {
        FEATURE_TABLE
    };

    let data = db_connector
        .hgetall(&format!("{}|{}", table_name, feature))
        .expect("Unable to get data");
    for (field, default) in fields.iter_mut() {
        if let Some(value) = data.get(field as &str) {
            *default = value.to_str().unwrap().to_string()
        }
    }
}

fn read_config(db_connections: &DbConnections, feature: &str) -> HashMap<&'static str, String> {
    let mut fields: HashMap<&str, String> = HashMap::from([
        (SET_OWNER, "local".to_string()),
        (NO_FALLBACK, "False".to_string()),
        (STATE, "disabled".to_string()),
    ]);
    read_data(&db_connections.config_db, feature, &mut fields);
    fields
}

fn read_state(db_connections: &DbConnections, feature: &str) -> HashMap<&'static str, String> {
    let mut fields: HashMap<&str, String> = HashMap::from([
        (CURRENT_OWNER, "local".to_string()),
        (REMOTE_STATE, "none".to_string()),
        (CONTAINER_ID, "".to_string()),
    ]);
    read_data(&db_connections.state_db, feature, &mut fields);
    fields
}

fn set_label(db_connections: &DbConnections, feature: &str, create: bool) {
    if db_connections.remote_ctr_enabled {
        let _ = db_connections.state_db.hset(
            KUBE_LABEL_TABLE,
            &format!("{}|{}_enabled", KUBE_LABEL_SET_KEY, feature),
            &CxxString::new(if create { "true" } else { "false" }),
        );
    }
}

fn update_data(db_connections: &DbConnections, feature: &str, data: &HashMap<&str, &str>) {
    if db_connections.remote_ctr_enabled {
        for (key, value) in data.iter() {
            let _ = db_connections
                .state_db
                .hset(FEATURE_TABLE, &format!("{}|{}", feature, key), &CxxString::new(value));
        }
    }
}

fn container_version(docker: &Docker, feature: &str) -> String {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let container_options = rt
        .block_on(docker.inspect_container(feature, None))
        .expect("Unable to communicate with Docker");
    match container_options.config {
        Some(config) => match config.env {
            Some(envs) => {
                for env in envs {
                    if env.starts_with("IMAGE_VERSION=") {
                        return env.split('=').collect::<Vec<&str>>()[1].to_string();
                    }
                }
                String::new()
            }
            None => String::new(),
        },
        None => String::new(),
    }
}

fn initialize_connection() -> DbConnections {
    DbConnections {
        config_db: DbConnector::new_tcp(4, "localhost", 6379, 0).expect("Unable to connect to Redis DB"),
        state_db: DbConnector::new_tcp(6, "localhost", 6379, 0).expect("Unable to connect to Redis DB"),
        remote_ctr_enabled: false,
    }
}

fn get_container_id<'a>(feature: &'a str, db_connections: &DbConnections) -> Cow<'a, str> {
    let data = db_connections
        .state_db
        .hgetall(&format!("FEATURE|{}", feature))
        .expect("Unable to get data");
    if data.get(CURRENT_OWNER).map_or("", |value| value.to_str().unwrap()) == "local" {
        Cow::Borrowed(feature)
    } else {
        data.get(CONTAINER_ID).map_or(Cow::Borrowed(feature), |value| {
            Cow::Owned(value.to_str().unwrap().to_string())
        })
    }
}

fn container_id(feature: &str) {
    let db_connections = initialize_connection();
    println!("{}", get_container_id(feature, &db_connections));
}

#[derive(enumset::EnumSetType, Debug)]
enum StartFlags {
    StartLocal,
    StartKube,
}

fn container_start(feature: &str) {
    let db_connections = initialize_connection();

    let feature_config = read_config(&db_connections, feature);
    let feature_state = read_state(&db_connections, feature);

    let timestamp = format!("{}", Local::now().format("%Y-%m-%d %H:%M:%S"));
    let mut data: HashMap<&str, &str> = HashMap::from([(SYSTEM_STATE, "up"), (UPD_TIMESTAMP, &timestamp)]);

    let mut start_val = enumset::EnumSet::new();
    if feature_config.get(SET_OWNER).unwrap() == "local" {
        start_val |= StartFlags::StartLocal;
    } else {
        start_val |= StartFlags::StartKube;
        if feature_config.get(NO_FALLBACK).unwrap() == "False" && feature_state.get(REMOTE_STATE).unwrap() == "none" {
            start_val |= StartFlags::StartLocal;
            data.insert(REMOTE_STATE, "none");
        }
    }

    if start_val & StartFlags::StartLocal != enumset::EnumSet::empty() {
        data.insert(CURRENT_OWNER, "local");
        data.insert(CONTAINER_ID, feature);
        if start_val == StartFlags::StartLocal {
            set_label(&db_connections, feature, false);
            data.insert(REMOTE_STATE, "none");
        }
    }

    update_data(&db_connections, feature, &data);

    if start_val & StartFlags::StartLocal != enumset::EnumSet::empty() {
        let docker = Docker::connect_with_local_defaults().unwrap();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(docker.start_container(feature, None::<StartContainerOptions<String>>))
            .expect("Unable to communicate with Docker");
    }

    if start_val & StartFlags::StartKube != enumset::EnumSet::empty() {
        set_label(&db_connections, feature, true);
    }
}

fn container_stop(feature: &str, timeout: Option<i64>) {
    let db_connections = initialize_connection();

    //let feature_config = read_config(&db_connections, feature);
    let feature_state = read_state(&db_connections, feature);
    let docker_id = get_container_id(feature, &db_connections);

    let docker = Docker::connect_with_local_defaults().unwrap();

    if !docker_id.is_empty() {
        let stop_options = timeout.map(|timeout| StopContainerOptions { t: timeout });
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(docker.stop_container(feature, stop_options))
            .expect("Unable to communicate with Docker");
    }

    let timestamp = format!("{}", Local::now().format("%Y-%m-%d %H:%M:%S"));
    let mut data: HashMap<&str, &str> = HashMap::from([
        (CURRENT_OWNER, "none"),
        (SYSTEM_STATE, "down"),
        (CONTAINER_ID, ""),
        (UPD_TIMESTAMP, &timestamp),
    ]);
    if feature_state.get(REMOTE_STATE).unwrap() == "running" {
        data.insert(REMOTE_STATE, "stopped");
    }

    update_data(&db_connections, feature, &data);
}

fn container_kill(feature: &str) {
    let db_connections = initialize_connection();

    let feature_config = read_config(&db_connections, feature);
    let feature_state = read_state(&db_connections, feature);
    let docker_id = get_container_id(feature, &db_connections);
    let remove_label =
        (feature_config.get(SET_OWNER).unwrap() != "local") || (feature_state.get(CURRENT_OWNER).unwrap() != "local");

    if remove_label {
        set_label(&db_connections, feature, false);
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
        rt.block_on(docker.kill_container(feature, Some(KillContainerOptions { signal: "SIGINT" })))
            .expect("Unable to communicate with Docker");
    }
}

fn container_wait(feature: &str) {
    let db_connections = initialize_connection();

    let feature_config = read_config(&db_connections, feature);
    //let mut feature_state = read_state(&db_connections, feature);
    let mut feature_state;
    let mut docker_id = get_container_id(feature, &db_connections);
    let mut pend_wait_seconds: u32 = 0;

    let docker = Docker::connect_with_local_defaults().unwrap();

    if *docker_id == *feature {
        let version = container_version(&docker, feature);
        if !version.is_empty() {
            update_data(
                &db_connections,
                feature,
                &HashMap::from([(ST_FEAT_CTR_STABLE_VER, version.as_str())]),
            );
        }
    }

    if docker_id.is_empty() && feature_config.get(NO_FALLBACK).unwrap() == "False" {
        pend_wait_seconds = get_config_data(SONIC_CTR_CONFIG_PEND_SECS)
            .and_then(|value| value.as_u64())
            .map(|value| value as u32)
            .unwrap_or(DEFAULT_PEND_SECS);
    }

    while docker_id.is_empty() {
        if feature_config.get(NO_FALLBACK).unwrap() == "False" {
            if pend_wait_seconds < WAIT_POLL_SECS {
                break;
            }
            pend_wait_seconds -= WAIT_POLL_SECS;
        }

        sleep(Duration::from_secs(WAIT_POLL_SECS as u64));
        feature_state = read_state(&db_connections, feature);

        docker_id = Cow::Borrowed(feature_state.get(CONTAINER_ID).unwrap());

        if feature_state.get(REMOTE_STATE).unwrap() == "pending" {
            update_data(&db_connections, feature, &HashMap::from([(REMOTE_STATE, "ready")]));
        }
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(
        docker
            .wait_container(
                feature,
                Some(WaitContainerOptions {
                    condition: "not-running",
                }),
            )
            .try_collect::<Vec<_>>(),
    )
    .expect("Unable to communicate with Docker");
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
    timeout: Option<i64>,
}

fn main() {
    let cli = Cli::parse();

    match cli.action {
        Action::Start => container_start(&cli.name),
        Action::Wait => container_wait(&cli.name),
        Action::Stop => container_stop(&cli.name, cli.timeout),
        Action::Kill => container_kill(&cli.name),
        Action::Id => container_id(&cli.name),
    };
}

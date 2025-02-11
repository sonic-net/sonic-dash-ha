use clap::Parser;
use sonic_common::log;
use swbus_core::mux::service::SwbusServiceHost;
use tracing::info;
mod config_reader;
use config_reader::{swbus_config_from_db, swbus_config_from_yaml};

#[derive(Parser, Debug)]
#[command(name = "swbusd")]
struct Args {
    // The slot id of the DPU. If this is set, it will read configuration from DPU table in config_db.
    // Otherwise, it will read configuration from the yaml file and bind to the specified address.
    #[arg(short = 's', long)]
    slot_id: Option<u32>,
    /// swbusd config in yaml file, including routes and peer information.
    #[arg(short = 'c', long)]
    config: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if let Err(e) = log::init("swbusd") {
        eprintln!("Failed to initialize logging: {}", e);
    }
    info!("Starting swbusd");
    let swbusd_config = match args.slot_id {
        Some(slot_id) => swbus_config_from_db(slot_id).unwrap(),
        None => {
            let config_path = args.config.expect("route_config is required when slot_id is not set");

            swbus_config_from_yaml(config_path).unwrap()
        }
    };

    let server = SwbusServiceHost::new(&swbusd_config.endpoint);
    server.start(swbusd_config).await.unwrap();
}

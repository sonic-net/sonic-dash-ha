use clap::Parser;
use sonic_common::log;
use swbus_core::mux::route_config::RoutesConfig;
use swbus_core::mux::service::SwbusServiceHost;
use tracing::info;
#[derive(Parser, Debug)]
#[command(name = "swbusd")]
struct Args {
    /// The address to connect to
    #[arg(short = 'a', long)]
    address: String,
    /// The initial routes of swbusd in yaml file
    #[arg(short = 'r', long)]
    route_config: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if let Err(e) = log::init("swbusd") {
        eprintln!("Failed to initialize logging: {}", e);
    }
    info!("Starting swbusd");
    let route_config = RoutesConfig::load_from_yaml(args.route_config).unwrap();
    let server = SwbusServiceHost::new(args.address);
    server.start(route_config).await.unwrap();
}

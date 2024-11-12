use clap::Parser;
use swbus_core::mux::core_runtime::SwbusCoreRuntime;
use swbus_core::mux::route_config::RoutesConfig;

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
    let route_config = RoutesConfig::load_from_yaml(args.route_config).unwrap();
    let mut runtime = SwbusCoreRuntime::new(args.address);
    runtime.start(route_config).await.unwrap();
}

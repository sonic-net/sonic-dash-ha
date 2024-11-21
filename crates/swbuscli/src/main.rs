mod ping;
mod show;
use clap::Parser;
use std::sync::Arc;
use swbus_edge::edge_runtime::SwbusEdgeRuntime;
use swbus_proto::swbus::*;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

#[derive(Parser, Debug)]
#[command(name = "swbuscli")]
struct Command {
    /// swbusd address
    #[arg()]
    address: String,

    /// service path of the client (todo: remove this)
    #[arg(value_parser = ServicePath::from_string)]
    service_path: ServicePath,
    /// Enable debug output
    #[arg(short = 'd', long)]
    debug: bool,

    #[command(subcommand)]
    subcommand: CliSub,
}

#[derive(Parser, Debug)]
enum CliSub {
    Ping(ping::PingCmd),
    Show(show::ShowCmd),
}

trait CmdHandler {
    async fn handle(&self, ctx: &CommandContext);
}

struct CommandContext {
    debug: bool,
    sp: ServicePath,
    runtime: Arc<Mutex<SwbusEdgeRuntime>>,
}

#[tokio::main]
async fn main() {
    let args = Command::parse();

    let runtime = Arc::new(Mutex::new(SwbusEdgeRuntime::new(
        format!("http://{}", args.address),
        args.service_path.clone(),
    )));
    let runtime_clone = runtime.clone();
    let runtime_task: JoinHandle<()> = tokio::spawn(async move {
        runtime_clone.lock().await.start().await.unwrap();
    });

    let ctx = CommandContext {
        debug: args.debug,
        sp: args.service_path,
        runtime: runtime.clone(),
    };
    match args.subcommand {
        CliSub::Ping(ping_args) => ping_args.handle(&ctx).await,
        CliSub::Show(show_args) => show_args.handle(&ctx).await,
    };
}

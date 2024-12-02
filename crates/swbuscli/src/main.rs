mod ping;
mod show;
use clap::Parser;
use std::sync::Arc;
use swbus_edge::edge_runtime::SwbusEdgeRuntime;
use swbus_proto::message_id_generator::MessageIdGenerator;
use swbus_proto::result::SwbusError;
use swbus_proto::swbus::*;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, Instant};

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
    id_generator: MessageIdGenerator,
}

pub struct ResponseResult {
    pub error_code: SwbusErrorCode,
    pub error_message: String,
    pub msg: Option<SwbusMessage>,
}

impl ResponseResult {
    pub fn from_code(error_code: i32, error_message: String, msg: Option<SwbusMessage>) -> Self {
        ResponseResult {
            error_code: SwbusErrorCode::try_from(error_code).unwrap_or(SwbusErrorCode::Fail),
            error_message,
            msg,
        }
    }
}
pub(crate) async fn wait_for_response(
    recv_queue_rx: &mut mpsc::Receiver<SwbusMessage>,
    request_id: u64,
    timeout: u32,
) -> ResponseResult {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(timeout as u64) {
        match time::timeout(Duration::from_secs(timeout as u64), recv_queue_rx.recv()).await {
            Ok(Some(msg)) => match msg.body {
                Some(swbus_message::Body::Response(ref response)) => {
                    if response.request_id != request_id {
                        // Not my response
                        continue;
                    }
                    return ResponseResult::from_code(response.error_code, response.error_message.clone(), Some(msg));
                }
                _ => continue,
            },
            Ok(None) => {
                return ResponseResult::from_code(SwbusErrorCode::Fail as i32, "channel broken".to_string(), None);
            }
            Err(_) => {
                return ResponseResult::from_code(SwbusErrorCode::Timeout as i32, "request timeout".to_string(), None);
            }
        }
    }
    ResponseResult::from_code(SwbusErrorCode::Timeout as i32, "request timeout".to_string(), None)
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
        id_generator: MessageIdGenerator::new(),
    };
    match args.subcommand {
        CliSub::Ping(ping_args) => ping_args.handle(&ctx).await,
        CliSub::Show(show_args) => show_args.handle(&ctx).await,
    };
}
